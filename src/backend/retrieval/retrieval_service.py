"""
Retrieval service for semantic search over financial documents.
"""

from pathlib import Path
from typing import List, Dict, Any, Optional
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from retrieval.vector_store import FinancialVectorStore
from utils.nlp import get_embedding
from etl.config import ETLConfig


class RetrievalService:
    """
    Service for retrieving relevant documents using semantic search.
    """
    
    def __init__(self, config: Optional[ETLConfig] = None):
        """
        Initialize retrieval service.
        
        Args:
            config: ETLConfig instance (uses default if None)
        """
        if config is None:
            config = ETLConfig()
        
        self.config = config
        self.indices_dir = config.PROCESSED_DIR / "indices"
        self.indices_dir.mkdir(parents=True, exist_ok=True)
        self._combined_store = None
        self._news_store = None
        self._filings_store = None
        self._transcripts_store = None
    
    def _load_combined_index(self) -> FinancialVectorStore:
        """Lazy load combined index."""
        if self._combined_store is None:
            combined_path = self.indices_dir / "combined.index"
            if combined_path.with_suffix('.index').exists():
                self._combined_store = FinancialVectorStore()
                self._combined_store.load(combined_path)
            else:
                # Build index if it doesn't exist
                from retrieval.index_builder import build_combined_index
                self._combined_store = build_combined_index(self.config)
        return self._combined_store
    
    def search(
        self,
        query: str,
        doc_type: Optional[str] = None,
        ticker: Optional[str] = None,
        k: int = 10,
        min_score: float = 0.0
    ) -> List[Dict[str, Any]]:
        """
        Search for documents similar to query.
        
        Args:
            query: Natural language query string
            doc_type: Optional filter by document type ('news', 'filing', 'transcript')
            ticker: Optional filter by ticker symbol
            k: Number of results to return
            min_score: Minimum similarity score threshold
        
        Returns:
            List of relevant documents with metadata
        """
        # Get query embedding
        query_embedding = get_embedding(query)
        
        # Load appropriate store
        if doc_type:
            # Load specific document type store
            store_path = self.indices_dir / f"{doc_type}.index"
            if store_path.with_suffix('.index').exists():
                store = FinancialVectorStore()
                store.load(store_path)
            else:
                # Use combined store if specific doesn't exist
                store = self._load_combined_index()
        else:
            # Use combined store
            store = self._load_combined_index()
        
        # Search
        results = store.search(
            query_embedding,
            k=k,
            doc_type=doc_type,
            ticker=ticker,
            min_score=min_score
        )
        
        return results
    
    def search_news(
        self,
        query: str,
        ticker: Optional[str] = None,
        k: int = 10
    ) -> List[Dict[str, Any]]:
        """Search news articles."""
        return self.search(query, doc_type='news', ticker=ticker, k=k)
    
    def search_filings(
        self,
        query: str,
        ticker: Optional[str] = None,
        k: int = 10
    ) -> List[Dict[str, Any]]:
        """Search SEC filings."""
        return self.search(query, doc_type='filing', ticker=ticker, k=k)
    
    def search_transcripts(
        self,
        query: str,
        ticker: Optional[str] = None,
        k: int = 10
    ) -> List[Dict[str, Any]]:
        """Search earnings call transcripts."""
        return self.search(query, doc_type='transcript', ticker=ticker, k=k)
    
    def rebuild_indices(self, ticker: Optional[str] = None):
        """Rebuild all vector indices."""
        from retrieval.index_builder import build_combined_index
        self._combined_store = build_combined_index(self.config, ticker)
        self._news_store = None
        self._filings_store = None
        self._transcripts_store = None


# Global service instance
_retrieval_service = None

def get_retrieval_service() -> RetrievalService:
    """Get or create global retrieval service instance."""
    global _retrieval_service
    if _retrieval_service is None:
        _retrieval_service = RetrievalService()
    return _retrieval_service

