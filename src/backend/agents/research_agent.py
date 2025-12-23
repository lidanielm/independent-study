"""
Research Agent for searching and synthesizing financial information.
"""

from typing import Optional, List, Callable, Dict, Any, Set
import sys
import json
from pathlib import Path
from datetime import datetime

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from .base_agent import BaseAgent
from .tools.search_tools import search_documents, search_news, search_filings, search_transcripts
from .tools.data_tools import get_price_data, get_fundamentals
from .query_intent import parse_intent
from etl.config import ETLConfig


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
        temperature: float = 0.7,
        max_tickers: int = 3,
        ticker_universe_path: Optional[Path] = None
    ):
        """Initialize the Research Agent."""
        
        system_prompt = """You are a financial research analyst AI assistant. Your role is to help users understand 
companies by searching through financial documents including:
- News articles about the company
- SEC filings (10-K, 10-Q) containing financial statements and risk factors
- Earnings call transcripts with management commentary

Follow a ReAct loop (Thought -> Action -> Observation -> Thought -> ... -> Final Answer) using structured reasoning:
- Think through the problem and what data you need before calling any tool.
- Choose the minimal set of tools to gather the needed evidence.
- After each tool result, summarize the key observations and decide the next best action.
- Stop gathering data once you have enough to answer; then provide the final answer.

Format every turn exactly as:
Thought: <your reasoning about next steps>
Action: <tool name or None>
Action Input: <JSON object with arguments; use {} when no tool>
Observation: <leave blank; system will fill after tool run>

When answering questions:
1. ALWAYS use the ticker symbol from the context when calling search tools (search_filings, search_transcripts, search_news)
2. Use the search tools to find relevant information from multiple sources
3. Synthesize findings from different documents to provide comprehensive answers
4. Always cite your sources (mention document type, ticker, and key details)
5. If information is not found after searching, clearly state that
6. Provide balanced perspectives when multiple viewpoints exist
7. Focus on factual information from the documents rather than speculation

IMPORTANT: If a ticker is provided in the context, you MUST pass it as the 'ticker' parameter to all search tool calls. 
For example, if context shows ticker='TSLA', call search_filings(query="...", ticker="TSLA").

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
            temperature=temperature,
            react_enabled=True,
            react_text_mode=True,
            react_expose_trace=False
        )
        self.max_tickers = max_tickers
        self.ticker_universe = self._load_ticker_universe(ticker_universe_path)

    def _load_ticker_universe(self, override_path: Optional[Path]) -> Set[str]:
        """Load ticker universe from JSON file, return uppercase set."""
        try:
            data_path = override_path or (Path(__file__).parent.parent / "data" / "stock_tickers.json")
            with open(data_path, "r") as f:
                tickers = json.load(f)
            if isinstance(tickers, list):
                return {t.upper() for t in tickers if isinstance(t, str) and t.strip()}
        except Exception as e:
            print(f"Warning: failed to load ticker universe: {e}")
        return set()
    
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

    async def process_query(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None,
        max_iterations: int = 5
    ) -> dict:
        """
        Override to infer tickers and sources, then run per-ticker ReAct flows and aggregate.
        """
        base_context = context.copy() if context else {}
        provided_ticker = base_context.get("ticker")
        
        intent = parse_intent(query, ticker_hint=provided_ticker)
        
        # Build ticker candidate list (dedup preserving order)
        candidates: List[str] = []
        if provided_ticker:
            candidates.append(provided_ticker.upper())
        candidates.extend([t for t in intent.raw_ticker_candidates if len(t) >= 2])
        if intent.ticker and intent.ticker not in candidates and len(intent.ticker) >= 2:
            candidates.append(intent.ticker)
        dedup = []
        for t in candidates:
            if t not in dedup:
                dedup.append(t)
        stoplist = {
            "US", "USA",
            "Q1", "Q2", "Q3", "Q4",
            "H1", "H2", "FY", "CY", "LY", "PY",
            "EPS", "EBIT", "EBITDA", "EARNINGS",
            "GAAP", "NON-GAAP", "GUIDE", "GUIDANCE",
            "SALES", "REVENUE", "REV", "MARGIN", "MARGINS",
            "GM", "OM", "OPM", "GPM",
            "YOY", "QOQ", "LTM", "TTM",
            "USD", "EUR", "GBP",
            "SPX", "NDX", "DJI", "RUT",
            "ETF", "ETFS", "ADR", "ADRs",
            "STAPLES", "TECH", "SECTOR",
            "INDEX", "INDICES"
        }
        universe = self.ticker_universe
        filtered = []
        for t in dedup:
            tu = t.upper()
            if tu in stoplist:
                continue
            if universe and tu not in universe:
                continue
            filtered.append(tu)
        tickers = filtered[: self.max_tickers]
        
        # Determine sources inferred
        doc_types: List[str] = []
        if intent.needs_news:
            doc_types.append("news")
        if intent.needs_filings:
            doc_types.append("filings")
        if intent.needs_transcripts:
            doc_types.append("transcripts")
        
        # If no ticker inferred, fallback to single run with sources
        if not tickers:
            if doc_types:
                base_context["doc_types"] = doc_types
            return await super().process_query(
                query=query,
                context=base_context if base_context else None,
                max_iterations=max_iterations
            )
        
        # Run per-ticker and aggregate
        answers = []
        all_sources = []
        all_tool_calls = []
        for t in tickers:
            ctx = base_context.copy()
            ctx["ticker"] = t
            if doc_types:
                ctx["doc_types"] = doc_types

            # Opportunistic: if user needs filings and they're missing locally, try to fetch/process/index them.
            # This keeps the agent from returning empty/partial results for common comparative questions.
            try:
                cfg = ETLConfig()
                auto_enabled = getattr(cfg, "AUTO_ENABLED", False)
                disable_auto = bool(ctx.get("disable_auto_fetch"))
                if auto_enabled and not disable_auto and ("filings" in (doc_types or [])):
                    # Fast check: do we have any processed filings parquet for this ticker?
                    import glob
                    has_processed = bool(glob.glob(str(cfg.PROCESSED_FILINGS_DIR / f"{t}_*.parquet")))
                    if not has_processed:
                        from etl.auto_orchestrator import ensure_filings
                        status = ensure_filings(t, cfg)
                        # Rebuild indices for this ticker so subsequent searches see the new docs
                        if status.get("processed"):
                            from retrieval.retrieval_service import get_retrieval_service
                            get_retrieval_service().rebuild_indices(ticker=t, doc_types={"filing"})
                        # Surface ETL status to the LLM via context so it can explain limitations
                        ctx["auto_fetch_status"] = ctx.get("auto_fetch_status", {})
                        ctx["auto_fetch_status"]["filings"] = status
            except Exception as _exc:
                ctx["auto_fetch_status"] = ctx.get("auto_fetch_status", {})
                ctx["auto_fetch_status"]["filings"] = {"ticker": t, "error": str(_exc)}

            result = await super().process_query(
                query=query,
                context=ctx,
                max_iterations=max_iterations
            )
            answers.append((t, result.get("answer", "")))
            all_sources.extend(result.get("sources", []) or [])
            all_tool_calls.extend(result.get("tool_calls", []) or [])
        
        # Combine answers into a single response
        combined_answer = "\n\n".join([f"{t}: {ans}" for t, ans in answers])
        return {
            "answer": combined_answer,
            "sources": all_sources,
            "tool_calls": all_tool_calls,
            "agent": self.name
        }

