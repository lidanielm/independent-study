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
from .tools.search_tools import suggest_tickers
from .tools.data_tools import get_price_data, get_fundamentals, screen_rising_operational_risk
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
1. If context includes a single 'ticker', use it in all relevant tool calls.
   If context includes a list 'tickers', you must choose the appropriate ticker for each tool call (e.g., search_filings(..., ticker="NVDA") then search_filings(..., ticker="AMD")).
2. Use the search tools to find relevant information from multiple sources
3. Synthesize findings from different documents to provide comprehensive answers
4. Always cite your sources (mention document type, ticker, and key details)
5. If information is not found after searching, clearly state that
6. Provide balanced perspectives when multiple viewpoints exist
7. Focus on factual information from the documents rather than speculation

IMPORTANT: If a ticker is provided in the context, you MUST pass it as the 'ticker' parameter to all search tool calls. 
For example, if context shows ticker='TSLA', call search_filings(query="...", ticker="TSLA").

If no ticker is provided, use `suggest_tickers(query=..., doc_type=...)` to identify which tickers are most relevant,
then search those tickers' documents to produce the answer.

For competitive-position questions about AI chips, specifically look for 10-K evidence such as:
- Software ecosystem / lock-in: CUDA, developer ecosystem, software stack, platform compatibility, proprietary APIs, switching costs.
- Supply chain dominance: capacity constraints, foundry dependencies, advanced packaging, HBM/memory supply, single-source risk, long-term supply agreements.

For broad screening queries that ask for "firms with ..." (no tickers provided), you should consider using:
- `screen_rising_operational_risk(...)` to generate candidate tickers from filings before doing deeper searches.

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
            suggest_tickers,
            get_price_data,
            get_fundamentals,
            screen_rising_operational_risk,
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
        Override to infer tickers and sources.
        - Single ticker: run one ReAct flow.
        - Multiple tickers: run a single ReAct flow with `tickers=[...]` so the agent produces ONE Thought + ONE Final Answer.
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
        
        # region agent log
        try:
            with open("/Users/danielli/Documents/penn/fa25/is/.cursor/debug.log", "a") as _f:
                _f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "run1",
                    "hypothesisId": "H1",
                    "location": "research_agent.py:process_query:pre_run",
                    "message": "ticker intent processed",
                    "data": {
                        "query": query,
                        "context": base_context,
                        "intent": intent.__dict__,
                        "candidates": dedup,
                        "filtered": tickers,
                        "stoplist_hit": [t for t in dedup if t.upper() in stoplist],
                        "universe_checked": bool(self.ticker_universe),
                        "universe_size": len(self.ticker_universe),
                    },
                    "timestamp": int(datetime.now().timestamp() * 1000),
                }) + "\n")
        except Exception:
            pass
        # endregion
        
        # Determine sources inferred
        doc_types: List[str] = []
        if intent.needs_news:
            doc_types.append("news")
        if intent.needs_filings:
            doc_types.append("filings")
        if intent.needs_transcripts:
            doc_types.append("transcripts")
        
        # If no ticker inferred, pick candidate tickers based on corpus search for the query.
        # This keeps "firms with ..." queries from collapsing to an empty run.
        if not tickers:
            try:
                suggestion = suggest_tickers(query=query, doc_type=None, k=self.max_tickers, candidate_k=80, min_score=0.0)
                suggested = [t["ticker"] for t in (suggestion.get("tickers") or []) if t.get("ticker")]
            except Exception:
                suggested = []
                suggestion = None

            # If we found candidates, run one multi-ticker loop; otherwise fall back to single run.
            if suggested:
                ctx = base_context.copy()
                ctx.pop("ticker", None)
                ctx["tickers"] = suggested
                if doc_types:
                    ctx["doc_types"] = doc_types
                if suggestion:
                    ctx["ticker_suggestions"] = suggestion
                return await super().process_query(query=query, context=ctx, max_iterations=max_iterations)

            if doc_types:
                base_context["doc_types"] = doc_types
            return await super().process_query(query=query, context=base_context if base_context else None, max_iterations=max_iterations)

        # If multiple tickers, run ONE flow with tickers list so we only produce one Thought + one Final Answer.
        if len(tickers) > 1:
            ctx = base_context.copy()
            ctx.pop("ticker", None)  # avoid forcing single-ticker behavior
            ctx["tickers"] = tickers
            if doc_types:
                ctx["doc_types"] = doc_types

            # Opportunistic filings prefetch/index per ticker (best-effort)
            try:
                cfg = ETLConfig()
                auto_enabled = getattr(cfg, "AUTO_ENABLED", False)
                disable_auto = bool(ctx.get("disable_auto_fetch"))
                if auto_enabled and not disable_auto and ("filings" in (doc_types or [])):
                    import glob
                    from etl.auto_orchestrator import ensure_filings
                    from retrieval.retrieval_service import get_retrieval_service
                    ctx["auto_fetch_status"] = ctx.get("auto_fetch_status", {})
                    for t in tickers:
                        has_processed = bool(glob.glob(str(cfg.PROCESSED_FILINGS_DIR / f"{t}_*.parquet")))
                        if not has_processed:
                            status = ensure_filings(t, cfg)
                            ctx["auto_fetch_status"].setdefault("filings", {})[t] = status
                            if status.get("processed"):
                                get_retrieval_service().rebuild_indices(ticker=t, doc_types={"filing"})
            except Exception as _exc:
                ctx["auto_fetch_status"] = ctx.get("auto_fetch_status", {})
                ctx["auto_fetch_status"]["filings_error"] = str(_exc)

            return await super().process_query(query=query, context=ctx, max_iterations=max_iterations)

        # Single ticker: run as before
        t = tickers[0]
        ctx = base_context.copy()
        ctx["ticker"] = t
        if doc_types:
            ctx["doc_types"] = doc_types

        # Opportunistic: if user needs filings and they're missing locally, try to fetch/process/index them.
        try:
            cfg = ETLConfig()
            auto_enabled = getattr(cfg, "AUTO_ENABLED", False)
            disable_auto = bool(ctx.get("disable_auto_fetch"))
            if auto_enabled and not disable_auto and ("filings" in (doc_types or [])):
                import glob
                has_processed = bool(glob.glob(str(cfg.PROCESSED_FILINGS_DIR / f"{t}_*.parquet")))
                if not has_processed:
                    from etl.auto_orchestrator import ensure_filings
                    status = ensure_filings(t, cfg)
                    if status.get("processed"):
                        from retrieval.retrieval_service import get_retrieval_service
                        get_retrieval_service().rebuild_indices(ticker=t, doc_types={"filing"})
                    ctx["auto_fetch_status"] = ctx.get("auto_fetch_status", {})
                    ctx["auto_fetch_status"]["filings"] = status
        except Exception as _exc:
            ctx["auto_fetch_status"] = ctx.get("auto_fetch_status", {})
            ctx["auto_fetch_status"]["filings"] = {"ticker": t, "error": str(_exc)}

        return await super().process_query(query=query, context=ctx, max_iterations=max_iterations)

