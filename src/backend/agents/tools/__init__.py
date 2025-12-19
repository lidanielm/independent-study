"""
Tools available to agents for interacting with the financial data system.
"""

from typing import TYPE_CHECKING, Any

# NOTE: Avoid importing retrieval/embedding stacks at package import time.
# `search_tools` pulls in RetrievalService which imports the embedding model (torch/transformers).
# Use lazy attribute access so lightweight tools (like screeners) can be imported without heavy deps.

if TYPE_CHECKING:
    from .search_tools import search_documents, search_news, search_filings, search_transcripts, suggest_tickers
    from .data_tools import get_price_data, get_fundamentals, get_features, screen_rising_operational_risk


def __getattr__(name: str) -> Any:
    if name in {"search_documents", "search_news", "search_filings", "search_transcripts", "suggest_tickers"}:
        from . import search_tools as _st
        return getattr(_st, name)
    if name in {"get_price_data", "get_fundamentals", "get_features", "screen_rising_operational_risk"}:
        from . import data_tools as _dt
        return getattr(_dt, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    'search_documents',
    'search_news', 
    'search_filings',
    'search_transcripts',
    'suggest_tickers',
    'get_price_data',
    'get_fundamentals',
    'get_features',
    'screen_rising_operational_risk',
]

