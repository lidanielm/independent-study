"""
Lightweight intent parser to infer ticker/topic and needed sources from a user query.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Set


TICKER_PATTERN = re.compile(r"\b([A-Z]{1,5})\b")


@dataclass
class IntentResult:
    ticker: Optional[str]
    needs_news: bool
    needs_filings: bool
    needs_transcripts: bool
    raw_ticker_candidates: List[str]


def infer_ticker(query: str, provided: Optional[str] = None) -> Optional[str]:
    """Infer a ticker from the query or provided hint."""
    if provided:
        return provided.upper()
    matches = TICKER_PATTERN.findall(query or "")
    if matches:
        return matches[0].upper()
    return None


def infer_sources(query: str) -> Set[str]:
    """Infer which sources are likely needed based on query terms."""
    q = (query or "").lower()
    sources = set()
    if any(term in q for term in ["10-k", "10q", "10-q", "filing", "sec", "risk factor", "md&a"]):
        sources.add("filings")
    if any(term in q for term in ["call", "transcript", "earnings", "guidance", "qa", "q&a"]):
        sources.add("transcripts")
    if any(term in q for term in ["news", "headline", "article", "press"]):
        sources.add("news")
    # default to all if none detected
    if not sources:
        sources = {"news", "filings", "transcripts"}
    return sources


def parse_intent(query: str, ticker_hint: Optional[str] = None) -> IntentResult:
    """Parse query to infer ticker and which sources to fetch/search."""
    ticker_candidates = []
    if ticker_hint:
        ticker_candidates.append(ticker_hint.upper())
    q = query or ""

    # Add explicit ticker-like tokens
    ticker_candidates.extend([m.upper() for m in TICKER_PATTERN.findall(q)])
    # Pick the first candidate that looks like a real ticker (filters common false positives like "AI", "10K", etc.)
    stop = {
        "AI",
        "US", "USA",
        "Q1", "Q2", "Q3", "Q4",
        "H1", "H2", "FY", "CY", "LY", "PY",
        "EPS", "EBIT", "EBITDA", "EARNINGS",
        "GAAP", "NON", "NON-GAAP",
        "SEC", "ETF", "ETFS", "ADR", "ADRS",
        "K", "10K", "10Q",
    }
    ticker = next((c for c in ticker_candidates if len(c) >= 2 and c not in stop), None)
    if ticker is None and ticker_candidates:
        ticker = ticker_candidates[0]

    sources = infer_sources(query)
    return IntentResult(
        ticker=ticker,
        needs_news="news" in sources,
        needs_filings="filings" in sources,
        needs_transcripts="transcripts" in sources,
        raw_ticker_candidates=ticker_candidates,
    )


__all__ = ["IntentResult", "parse_intent", "infer_ticker", "infer_sources"]
