"""
Retrieval service for search over financial documents.
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
    Service for retrieving relevant documents using search.
    """
    
    def __init__(self, config: Optional[ETLConfig] = None):
        """Initialize retrieval service."""
        if config is None:
            config = ETLConfig()
        
        self.config = config
        self.indices_dir = config.PROCESSED_DIR / "indices"
        self.indices_dir.mkdir(parents=True, exist_ok=True)
        self._combined_store = None
        self._news_store = None
        self._filings_store = None
        self._transcripts_store = None
    
    def _load_combined_index(self, force_reload: bool = False) -> FinancialVectorStore:
        """Lazy load combined index."""
        if self._combined_store is None or force_reload:
            combined_path = self.indices_dir / "combined.index"
            # Try loading from Supabase first if enabled, then local
            self._combined_store = FinancialVectorStore()
            try:
                self._combined_store.load(
                    combined_path, 
                    use_storage_adapter=self.config.USE_SUPABASE_STORAGE, 
                    config=self.config
                )
            except FileNotFoundError:
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
        """Search for documents similar to query."""
        # Get query embedding
        query_embedding = get_embedding(query)
        
        # Load appropriate store
        if doc_type:
            # Try plural form first (filings, transcripts, news), then singular, then combined.
            plural_map = {"filing": "filings", "transcript": "transcripts", "news": "news"}
            store = FinancialVectorStore()

            tried = []
            for candidate in (plural_map.get(doc_type, doc_type), doc_type):
                store_path = self.indices_dir / f"{candidate}.index"
                tried.append(str(store_path))
                try:
                    store.load(
                        store_path,
                        use_storage_adapter=self.config.USE_SUPABASE_STORAGE,
                        config=self.config
                    )
                    break
                except FileNotFoundError:
                    continue
            else:
                # Use combined store if no specific index exists
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
        # Try filings.index first (plural), then fall back to combined index
        query_embedding = get_embedding(query)
        store_path = self.indices_dir / "filings.index"
        store = FinancialVectorStore()
        try:
            store.load(
                store_path,
                use_storage_adapter=self.config.USE_SUPABASE_STORAGE,
                config=self.config
            )
            return store.search(query_embedding, k=k, doc_type='filing', ticker=ticker)
        except FileNotFoundError:
            pass
        # Fall back to combined index
        return self.search(query, doc_type='filing', ticker=ticker, k=k)
    
    def search_transcripts(
        self,
        query: str,
        ticker: Optional[str] = None,
        k: int = 10
    ) -> List[Dict[str, Any]]:
        """Search earnings call transcripts."""
        return self.search(query, doc_type='transcript', ticker=ticker, k=k)
    
    def rebuild_indices(self, ticker: Optional[str] = None, doc_types: Optional[set] = None):
        """Rebuild vector indices (optionally limited to doc types)."""
        from retrieval.index_builder import build_combined_index
        self._combined_store = build_combined_index(self.config, ticker, doc_types)
        # Clear all cached stores to force reload
        self._news_store = None
        self._filings_store = None
        self._transcripts_store = None
        # Force reload combined index on next access
        if self._combined_store is not None:
            # Reload from disk to ensure fresh data
            combined_path = self.indices_dir / "combined.index"
            self._combined_store = FinancialVectorStore()
            try:
                self._combined_store.load(
                    combined_path,
                    use_storage_adapter=self.config.USE_SUPABASE_STORAGE,
                    config=self.config
                )
            except FileNotFoundError:
                pass


# Global service instance
_retrieval_service = None

def get_retrieval_service() -> RetrievalService:
    """Get or create global retrieval service instance."""
    global _retrieval_service
    if _retrieval_service is None:
        _retrieval_service = RetrievalService()
    return _retrieval_service

