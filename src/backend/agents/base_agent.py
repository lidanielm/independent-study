"""
Base agent class for financial AI agents.
"""

import os
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable, Tuple
import re
from abc import ABC, abstractmethod

# Load environment variables from .env file
from dotenv import load_dotenv

# Load .env file - tries current directory and parent directories
# This will find .env in the project root
try:
    load_dotenv()
except Exception:
    pass

project_root = Path(__file__).parent.parent.parent.parent
env_path = project_root / ".env"
if env_path.exists():
    try:
        load_dotenv(env_path)
    except Exception:
        pass

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
        temperature: float = 0.7,
        react_enabled: bool = False,
        react_text_mode: bool = False,
        react_expose_trace: bool = True
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
        self.react_enabled = react_enabled
        self.react_text_mode = react_text_mode
        self.react_expose_trace = react_expose_trace
        self._react_system_guidance = (
            "Follow a ReAct loop (Thought -> Action -> Observation -> Thought -> Final Answer). "
            "Think step-by-step about what information is needed before calling tools. "
            "Use tools only when they add value. After each observation, summarize what was learned, "
            "decide the next best action, and stop when you have enough evidence to answer."
        )
        self._react_reflection_prompt = (
            "Reflect on the observations above. Decide if another action is needed. "
            "If you have enough information, provide a concise final answer with sources."
        )
        self._react_format_prompt = (
            "Use the exact schema for every step:\n"
            "Thought: <your reasoning about what to do next>\n"
            "Action: <tool name or 'None' if you can answer now>\n"
            "Action Input: <JSON object with arguments when a tool is used; use {} when no tool>\n"
            "Observation: <leave blank; will be filled after the tool runs>\n"
            "Always emit one Action block per turn. Do not include extra text outside this schema."
        )
        
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
        if self.react_text_mode:
            return await self._process_query_react_text(query, context=context, max_iterations=max_iterations)
        
        # Build messages
        messages = [
            {"role": "system", "content": self.system_prompt}
        ]
        
        # Add ReAct guidance to encourage reasoning before actions
        if self.react_enabled:
            messages.append({"role": "system", "content": self._react_system_guidance})
        
        # Add memory (recent conversation history)
        for msg in self.memory[-10:]:  # Keep last 10 messages
            messages.append(msg)
        
        # Add current query
        user_message = query
        if context:
            # Make ticker very prominent if present
            ticker = context.get("ticker")
            if ticker:
                user_message += f"\n\nIMPORTANT: The ticker symbol is {ticker}. You MUST use ticker='{ticker}' in all search tool calls."
            user_message += f"\n\nAdditional Context: {json.dumps(context, indent=2)}"
        
        messages.append({"role": "user", "content": user_message})
        
        # Nudge the model to plan before acting
        if self.react_enabled:
            messages.append({
                "role": "system",
                "content": (
                    "Before taking any action, briefly reason about what is needed. "
                    "If you can answer without tools, do so. Otherwise, select the best tool and explain why."
                )
            })
        
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
                
                # Add tool result to messages as an observation
                observation_content = json.dumps(result, default=str)
                if self.react_enabled:
                    observation_content = f"Observation ({func_name}): {observation_content}"
                
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call["id"],
                    "content": observation_content
                })
            
            # Prompt the model to reflect on observations before next action
            if self.react_enabled and tool_calls:
                messages.append({
                    "role": "system",
                    "content": self._react_reflection_prompt
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
    
    def _parse_react_response(self, content: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Parse Thought, Action, Action Input from the assistant text using regex."""
        thought = None
        action = None
        action_input = None
        
        thought_match = re.search(r"^Thought:\s*(.*?)(?=^Action:|\Z)", content, re.DOTALL | re.MULTILINE)
        if thought_match:
            thought = thought_match.group(1).strip()
        
        action_match = re.search(r"^Action:\s*(.*)", content, re.MULTILINE)
        if action_match:
            action = action_match.group(1).strip()
            if action.lower() == "none":
                action = None
        
        input_match = re.search(r"^Action Input:\s*(\{.*\})", content, re.DOTALL | re.MULTILINE)
        if input_match:
            action_input = input_match.group(1).strip()
        
        return thought, action, action_input
    
    async def _process_query_react_text(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None,
        max_iterations: int = 5
    ) -> Dict[str, Any]:
        """Process a query using text-based ReAct parsing (Thought/Action/Action Input/Observation)."""
        messages = [{"role": "system", "content": self.system_prompt}]
        messages.append({"role": "system", "content": self._react_system_guidance})
        messages.append({"role": "system", "content": self._react_format_prompt})
        
        for msg in self.memory[-10:]:
            messages.append(msg)
        
        user_message = query
        if context:
            ticker = context.get("ticker")
            if ticker:
                user_message += f"\n\nIMPORTANT: The ticker symbol is {ticker}. Use ticker='{ticker}' in relevant tools."
            user_message += f"\n\nAdditional Context: {json.dumps(context, indent=2)}"
        messages.append({"role": "user", "content": user_message})
        
        iteration = 0
        tool_results = []
        trace = []
        
        while iteration < max_iterations:
            response = self._call_llm(messages, tools=None)
            if "error" in response:
                error_payload = {
                    "answer": response["error"],
                    "sources": [],
                    "tool_calls": tool_results,
                    "error": True
                }
                if self.react_expose_trace:
                    error_payload["trace"] = trace
                return error_payload
            
            message = response["choices"][0]["message"]
            assistant_content = message.get("content", "") or ""
            messages.append({"role": "assistant", "content": assistant_content})
            
            thought, action, action_input = self._parse_react_response(assistant_content)
            step = {"thought": thought, "action": action, "action_input": action_input, "observation": None}
            
            if not action:
                # Guardrail: models sometimes emit "Action: None" but forget to write a final answer.
                # Only stop if a "Final Answer:" section is present; otherwise nudge and continue.
                if re.search(r"^Final Answer:\s*", assistant_content, re.MULTILINE):
                    answer = assistant_content
                    self.memory.append({"role": "user", "content": query})
                    self.memory.append({"role": "assistant", "content": answer})
                    trace.append(step)
                    success_payload = {
                        "answer": answer,
                        "sources": self._extract_sources(tool_results),
                        "tool_calls": tool_results,
                        "agent": self.name
                    }
                    if self.react_expose_trace:
                        success_payload["trace"] = trace
                    return success_payload

                # Nudge: provide a real final answer (even if limited by missing data).
                trace.append(step)
                messages.append({
                    "role": "system",
                    "content": (
                        "You set Action: None but did not provide a Final Answer. "
                        "Write `Final Answer:` now. If relevant sources were not found, "
                        "clearly state what is missing and what additional data is needed."
                    )
                })
                iteration += 1
                continue
            
            # Parse JSON arguments
            parsed_args = {}
            parse_error = None
            if action_input:
                try:
                    parsed_args = json.loads(action_input)
                except Exception as e:
                    parse_error = f"Failed to parse Action Input JSON: {e}"
            else:
                parse_error = "Missing Action Input for the specified Action."
            
            if parse_error:
                observation_text = parse_error
                step["observation"] = observation_text
                messages.append({"role": "assistant", "content": f"Observation: {observation_text}"})
                trace.append(step)
                iteration += 1
                continue
            
            # Execute tool
            result = self._execute_tool(action, parsed_args)
            tool_results.append({"tool": action, "arguments": parsed_args, "result": result})
            
            # region agent log
            try:
                with open("/Users/danielli/Documents/penn/fa25/is/.cursor/debug.log", "a") as _f:
                    _f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "run2",
                        "hypothesisId": "H2",
                        "location": "base_agent.py:_process_query_react_text:tool_exec",
                        "message": "tool executed",
                        "data": {
                            "action": action,
                            "arguments": parsed_args,
                            "result_preview": str(result)[:500],
                        },
                        "timestamp": int(datetime.now().timestamp() * 1000),
                    }) + "\n")
            except Exception:
                pass
            # endregion
            
            observation_text = json.dumps(result, default=str)
            step["observation"] = observation_text
            trace.append(step)
            
            messages.append({"role": "assistant", "content": f"Observation: {observation_text}"})
            messages.append({"role": "system", "content": self._react_reflection_prompt})
            
            iteration += 1
        
        # Max iterations reached: force a final answer.
        messages.append({
            "role": "system",
            "content": (
                "Max tool iterations reached. Provide a `Final Answer:` now using the observations so far. "
                "If key documents are missing, explicitly say what is missing and what would be needed."
            )
        })
        final_response = self._call_llm(messages, tools=None)
        if "error" in final_response:
            # Fall back to last assistant message (avoid returning a system prompt)
            last_assistant = next((m for m in reversed(messages) if m.get("role") == "assistant"), {"content": ""})
            final_content = last_assistant.get("content") or "Max iterations reached; unable to produce final answer."
        else:
            final_content = final_response["choices"][0]["message"].get("content", "") or ""

        response_payload = {
            "answer": final_content,
            "sources": self._extract_sources(tool_results),
            "tool_calls": tool_results,
            "agent": self.name,
            "warning": "Max iterations reached"
        }
        if self.react_expose_trace:
            response_payload["trace"] = trace
        return response_payload
    
    def clear_memory(self):
        """Clear conversation memory."""
        self.memory = []

