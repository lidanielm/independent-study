"""
Base agent class for financial AI agents.
"""

import os
import json
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
from abc import ABC, abstractmethod

# Load environment variables from .env file
from dotenv import load_dotenv

# Load .env file - tries current directory and parent directories
# This will find .env in the project root
load_dotenv()

project_root = Path(__file__).parent.parent.parent.parent
env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(env_path)

# Make OpenAI optional
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False


class BaseAgent(ABC):
    """
    Base class for financial analysis agents.
    
    Provides LLM integration and tool execution capabilities.
    """
    
    def __init__(
        self,
        name: str,
        system_prompt: str,
        tools: Optional[List[Callable]] = None,
        model: str = "gpt-4o-mini",
        temperature: float = 0.7
    ):
        """
        Initialize the agent.
        """
        self.name = name
        self.system_prompt = system_prompt
        self.tools = tools or []
        self.model = model
        self.temperature = temperature
        self.memory: List[Dict[str, str]] = []
        
        # Initialize LLM client if available
        if OPENAI_AVAILABLE:
            api_key = os.getenv("OPENAI_API_KEY")
            if api_key:
                self.client = OpenAI(api_key=api_key)
            else:
                self.client = None
                print(f"Warning: OPENAI_API_KEY not found in environment.")
        else:
            self.client = None
            print(f"Warning: OpenAI package not installed.")
    
    def _get_tool_definitions(self) -> List[Dict[str, Any]]:
        """Get tool definitions in OpenAI function calling format."""
        tool_defs = []
        
        for tool in self.tools:
            # Extract function signature and docstring
            import inspect
            sig = inspect.signature(tool)
            doc = inspect.getdoc(tool) or ""
            
            # Parse parameters
            properties = {}
            required = []
            for param_name, param in sig.parameters.items():
                if param_name == 'self':
                    continue
                
                param_type = "string"
                if param.annotation != inspect.Parameter.empty:
                    if param.annotation == int:
                        param_type = "integer"
                    elif param.annotation == float:
                        param_type = "number"
                    elif param.annotation == bool:
                        param_type = "boolean"
                
                properties[param_name] = {
                    "type": param_type,
                    "description": f"Parameter {param_name}"
                }
                
                if param.default == inspect.Parameter.empty:
                    required.append(param_name)
            
            tool_defs.append({
                "type": "function",
                "function": {
                    "name": tool.__name__,
                    "description": doc.split('\n')[0] if doc else f"Tool: {tool.__name__}",
                    "parameters": {
                        "type": "object",
                        "properties": properties,
                        "required": required
                    }
                }
            })
        
        return tool_defs
    
    def _execute_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Any:
        """Execute a tool by name."""
        for tool in self.tools:
            if tool.__name__ == tool_name:
                try:
                    return tool(**arguments)
                except Exception as e:
                    return {"error": f"Tool execution failed: {str(e)}"}
        
        return {"error": f"Tool {tool_name} not found"}
    
    def _call_llm(
        self,
        messages: List[Dict[str, str]],
        tools: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Call the LLM with messages and optional tools.
        """
        if not self.client:
            # Mock response for testing without API key
            return {
                "choices": [{
                    "message": {
                        "content": "Mock response: LLM not configured. Please set OPENAI_API_KEY environment variable.",
                        "role": "assistant"
                    }
                }]
            }
        
        try:
            kwargs = {
                "model": self.model,
                "messages": messages,
                "temperature": self.temperature
            }
            
            if tools:
                kwargs["tools"] = tools
                kwargs["tool_choice"] = "auto"
            
            response = self.client.chat.completions.create(**kwargs)
            
            message = response.choices[0].message
            tool_calls = []
            if hasattr(message, 'tool_calls') and message.tool_calls:
                tool_calls = [
                    {
                        "id": tc.id,
                        "type": tc.type,
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments
                        }
                    } for tc in message.tool_calls
                ]
            
            return {
                "choices": [{
                    "message": {
                        "content": message.content,
                        "role": message.role,
                        "tool_calls": tool_calls
                    }
                }]
            }
        except Exception as e:
            return {
                "error": f"LLM call failed: {str(e)}",
                "choices": [{
                    "message": {
                        "content": f"Error: {str(e)}",
                        "role": "assistant"
                    }
                }]
            }
    
    async def process_query(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None,
        max_iterations: int = 5
    ) -> Dict[str, Any]:
        """
        Process a user query using the agent's capabilities.
        """
        # Build messages
        messages = [
            {"role": "system", "content": self.system_prompt}
        ]
        
        # Add memory (recent conversation history)
        for msg in self.memory[-10:]:  # Keep last 10 messages
            messages.append(msg)
        
        # Add current query
        user_message = query
        if context:
            user_message += f"\n\nContext: {json.dumps(context, indent=2)}"
        
        messages.append({"role": "user", "content": user_message})
        
        # Get tool definitions
        tool_defs = self._get_tool_definitions() if self.tools else None
        
        # Call LLM with tool support
        iteration = 0
        tool_results = []
        
        while iteration < max_iterations:
            response = self._call_llm(messages, tool_defs)
            
            if "error" in response:
                return {
                    "answer": response["error"],
                    "sources": [],
                    "tool_calls": tool_results,
                    "error": True
                }
            
            message = response["choices"][0]["message"]
            messages.append(message)
            
            # Check if LLM wants to call tools
            tool_calls = message.get("tool_calls") or []
            
            if not tool_calls:
                # No more tool calls, return the final answer
                answer = message.get("content", "")
                
                # Update memory
                self.memory.append({"role": "user", "content": query})
                self.memory.append({"role": "assistant", "content": answer})
                
                return {
                    "answer": answer,
                    "sources": self._extract_sources(tool_results),
                    "tool_calls": tool_results,
                    "agent": self.name
                }
            
            # Execute tool calls
            for tool_call in tool_calls:
                func_name = tool_call["function"]["name"]
                func_args = json.loads(tool_call["function"]["arguments"])
                
                result = self._execute_tool(func_name, func_args)
                tool_results.append({
                    "tool": func_name,
                    "arguments": func_args,
                    "result": result
                })
                
                # Add tool result to messages
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call["id"],
                    "content": json.dumps(result, default=str)
                })
            
            iteration += 1
        
        # Max iterations reached
        final_message = messages[-1] if messages else {"content": "Max iterations reached"}
        return {
            "answer": final_message.get("content", "Processing incomplete"),
            "sources": self._extract_sources(tool_results),
            "tool_calls": tool_results,
            "agent": self.name,
            "warning": "Max iterations reached"
        }
    
    def _extract_sources(self, tool_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract source citations from tool results."""
        sources = []
        
        for tool_result in tool_results:
            if tool_result["tool"] in ["search_documents", "search_news", "search_filings", "search_transcripts"]:
                result = tool_result.get("result", [])
                if isinstance(result, list):
                    for item in result[:5]:  # Limit to top 5 sources
                        source = {
                            "type": item.get("doc_type", "unknown"),
                            "ticker": item.get("ticker"),
                            "title": item.get("title") or item.get("section"),
                            "score": item.get("similarity_score"),
                            "text_preview": item.get("text", "")[:200] if item.get("text") else None
                        }
                        sources.append(source)
        
        return sources
    
    def clear_memory(self):
        """Clear conversation memory."""
        self.memory = []

