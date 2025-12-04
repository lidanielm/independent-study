"""
Search tools for agents to query financial documents.
"""

from typing import List, Dict, Any, Optional
import sys
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from retrieval.retrieval_service import get_retrieval_service


def search_documents(
    query: str,
    doc_type: Optional[str] = None,
    ticker: Optional[str] = None,
    k: int = 10,
    min_score: float = 0.0
) -> List[Dict[str, Any]]:
    """Search across all financial documents (news, filings, transcripts)."""
    service = get_retrieval_service()
    results = service.search(
        query=query,
        doc_type=doc_type,
        ticker=ticker,
        k=k,
        min_score=min_score
    )
    return results


def search_news(
    query: str,
    ticker: Optional[str] = None,
    k: int = 10
) -> List[Dict[str, Any]]:
    """
    Search news articles.
    """
    service = get_retrieval_service()
    results = service.search_news(query, ticker=ticker, k=k)
    return results


def search_filings(
    query: str,
    ticker: Optional[str] = None,
    k: int = 10
) -> List[Dict[str, Any]]:
    """
    Search SEC filings (10-K, 10-Q).
    """
    service = get_retrieval_service()
    results = service.search_filings(query, ticker=ticker, k=k)
    return results


def search_transcripts(
    query: str,
    ticker: Optional[str] = None,
    k: int = 10
) -> List[Dict[str, Any]]:
    """
    Search earnings call transcripts.
    """
    service = get_retrieval_service()
    results = service.search_transcripts(query, ticker=ticker, k=k)
    return results

