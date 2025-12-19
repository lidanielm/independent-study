"""
Search tools for agents to query financial documents.
"""

from typing import List, Dict, Any, Optional
import sys
from pathlib import Path
import json
from datetime import datetime

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from retrieval.retrieval_service import get_retrieval_service


def suggest_tickers(
    query: str,
    doc_type: Optional[str] = None,
    k: int = 10,
    candidate_k: int = 60,
    min_score: float = 0.0,
) -> Dict[str, Any]:
    """
    Suggest likely tickers to explore for a query when the user did not provide any ticker.

    This is intentionally generic:
    - We search the corpus for the query (optionally constrained to a doc_type)
    - We aggregate retrieved results by their `ticker` metadata
    - We rank tickers by a weighted score (sum of similarity scores)

    Args:
        query: Natural language query
        doc_type: Optional document type constraint (e.g., 'news', 'filing', 'transcript')
        k: Number of tickers to return
        candidate_k: Number of documents to retrieve before aggregating tickers
        min_score: Minimum similarity score threshold when retrieving documents
    """
    service = get_retrieval_service()
    results = service.search(
        query=query,
        doc_type=doc_type,
        ticker=None,
        k=candidate_k,
        min_score=min_score,
    ) or []

    scores: Dict[str, float] = {}
    counts: Dict[str, int] = {}
    samples: Dict[str, List[Dict[str, Any]]] = {}

    for r in results:
        t = (r.get("ticker") or "").upper().strip()
        if not t:
            continue
        s = float(r.get("similarity_score", 0.0) or 0.0)
        scores[t] = scores.get(t, 0.0) + s
        counts[t] = counts.get(t, 0) + 1
        if t not in samples:
            samples[t] = []
        if len(samples[t]) < 3:
            samples[t].append({
                "doc_type": r.get("doc_type"),
                "section": r.get("section"),
                "title": r.get("title"),
                "filing_file": r.get("filing_file"),
                "published": r.get("published"),
                "similarity_score": r.get("similarity_score"),
            })

    ranked = sorted(scores.keys(), key=lambda t: scores[t], reverse=True)
    top = ranked[:k]

    return {
        "query": query,
        "doc_type": doc_type,
        "tickers": [
            {
                "ticker": t,
                "score": scores.get(t, 0.0),
                "count": counts.get(t, 0),
                "samples": samples.get(t, []),
            }
            for t in top
        ],
        "candidates_considered": len(results),
        "unique_tickers": len(scores),
    }


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
    # region agent log
    try:
        with open("/Users/danielli/Documents/penn/fa25/is/.cursor/debug.log", "a") as _f:
            _f.write(json.dumps({
                "sessionId": "debug-session",
                "runId": "run2",
                "hypothesisId": "H3",
                "location": "search_tools.py:search_transcripts",
                "message": "search_transcripts called",
                "data": {
                    "query": query,
                    "ticker": ticker,
                    "k": k,
                    "result_count": len(results) if isinstance(results, list) else None
                },
                "timestamp": int(datetime.now().timestamp() * 1000),
            }) + "\n")
    except Exception:
        pass
    # endregion
    return results

