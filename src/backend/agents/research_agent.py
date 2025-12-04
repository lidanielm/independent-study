"""
Research Agent for searching and synthesizing financial information.
"""

from typing import Optional, List, Callable
import sys
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from .base_agent import BaseAgent
from .tools.search_tools import search_documents, search_news, search_filings, search_transcripts
from .tools.data_tools import get_price_data, get_fundamentals


class ResearchAgent(BaseAgent):
    """
    Research Agent specialized in searching financial documents and synthesizing findings.
    
    Capabilities:
    - Search across news, filings, and transcripts
    - Synthesize information from multiple sources
    - Answer questions about company performance, risks, and strategies
    - Provide citations and source references
    """
    
    def __init__(
        self,
        model: str = "gpt-4o-mini",
        temperature: float = 0.7
    ):
        """Initialize the Research Agent."""
        
        system_prompt = """You are a financial research analyst AI assistant. Your role is to help users understand 
companies by searching through financial documents including:
- News articles about the company
- SEC filings (10-K, 10-Q) containing financial statements and risk factors
- Earnings call transcripts with management commentary

When answering questions:
1. Use the search tools to find relevant information from multiple sources
2. Synthesize findings from different documents to provide comprehensive answers
3. Always cite your sources (mention document type, ticker, and key details)
4. If information is not found, clearly state that
5. Provide balanced perspectives when multiple viewpoints exist
6. Focus on factual information from the documents rather than speculation

Be thorough but concise. Structure your responses clearly with:
- A direct answer to the question
- Supporting evidence from the documents
- Source citations
- Any relevant context or caveats"""

        tools: List[Callable] = [
            search_documents,
            search_news,
            search_filings,
            search_transcripts,
            get_price_data,
            get_fundamentals
        ]
        
        super().__init__(
            name="Research Agent",
            system_prompt=system_prompt,
            tools=tools,
            model=model,
            temperature=temperature
        )
    
    async def research_topic(
        self,
        topic: str,
        ticker: Optional[str] = None,
        doc_types: Optional[List[str]] = None
    ) -> dict:
        """
        Research a specific topic across financial documents.
        """
        query = f"Research the following topic: {topic}"
        if ticker:
            query += f" for ticker {ticker}"
        
        context = {}
        if ticker:
            context["ticker"] = ticker
        if doc_types:
            context["doc_types"] = doc_types
        
        return await self.process_query(query, context=context)
    
    async def answer_question(
        self,
        question: str,
        ticker: Optional[str] = None
    ) -> dict:
        """
        Answer a specific question about a company or financial topic.
        """
        context = {}
        if ticker:
            context["ticker"] = ticker
        
        return await self.process_query(question, context=context)

