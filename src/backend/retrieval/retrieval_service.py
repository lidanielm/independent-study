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
        """
        Rebuild vector indices.

        IMPORTANT: If `doc_types` is provided, we rebuild those *specific* indices AND then rebuild the
        combined index with ALL available doc types so search remains consistent (e.g., rebuilding only
        filings should not wipe news from `combined.index`).
        """
        from retrieval import index_builder
        indices_dir = self.indices_dir

        if doc_types:
            # Rebuild requested per-type indices first
            if "news" in doc_types and self.config.PROCESSED_NEWS_FILE.exists():
                index_builder.build_news_index(self.config.PROCESSED_NEWS_FILE, indices_dir / "news.index", ticker)
            if "news_insight" in doc_types and self.config.PROCESSED_NEWS_INSIGHTS_FILE.exists():
                index_builder.build_news_insights_index(self.config.PROCESSED_NEWS_INSIGHTS_FILE, indices_dir / "news_insights.index", ticker)
            if "filing" in doc_types and self.config.PROCESSED_FILINGS_DIR.exists():
                index_builder.build_filings_index(self.config.PROCESSED_FILINGS_DIR, indices_dir / "filings.index", ticker)
            if "filing_insight" in doc_types and self.config.PROCESSED_FILINGS_INSIGHTS_DIR.exists():
                index_builder.build_filings_insights_index(self.config.PROCESSED_FILINGS_INSIGHTS_DIR, indices_dir / "filings_insights.index", ticker)
            if "transcript" in doc_types and self.config.PROCESSED_TRANSCRIPTS_DIR.exists():
                index_builder.build_transcripts_index(self.config.PROCESSED_TRANSCRIPTS_DIR, indices_dir / "transcripts.index", ticker)
            if "transcript_qa" in doc_types and self.config.PROCESSED_TRANSCRIPTS_QA_DIR.exists():
                index_builder.build_transcript_qa_index(self.config.PROCESSED_TRANSCRIPTS_QA_DIR, indices_dir / "transcripts_qa.index", ticker)
            if "transcript_guidance" in doc_types and self.config.PROCESSED_TRANSCRIPTS_GUIDANCE_DIR.exists():
                index_builder.build_transcript_guidance_index(self.config.PROCESSED_TRANSCRIPTS_GUIDANCE_DIR, indices_dir / "transcripts_guidance.index", ticker)

        # Always rebuild combined index with ALL doc types so it stays complete.
        self._combined_store = index_builder.build_combined_index(self.config, ticker, doc_types=None)

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

