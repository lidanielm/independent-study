"""
Vector store for search over financial documents.
Uses FAISS for efficient similarity search.
"""

import faiss
import numpy as np
import pickle
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from io import BytesIO


class FinancialVectorStore:
    """
    Vector store for financial documents (news, filings, transcripts).
    Supports multiple document types and efficient similarity search.
    """
    
    def __init__(self, dimension: int = 384, index_path: Optional[Path] = None):
        """Initialize vector store."""
        self.dimension = dimension
        self.index = faiss.IndexFlatL2(self.dimension)
        self.metadata: List[Dict[str, Any]] = []
        self.doc_type_map: Dict[str, List[Tuple[int, int]]] = {}

        if index_path:
            self.load(index_path)

    def add_documents(self, embeddings: np.ndarray, metadata: List[Dict[str, Any]], doc_type: str):
        """Add documents and their embeddings to the store."""
        if embeddings.size == 0:
            return
        if embeddings.ndim == 1:
            embeddings = embeddings.reshape(1, -1)
        embeddings = embeddings.astype('float32')
        if embeddings.shape[1] != self.dimension:
            raise ValueError(f"Embedding dimension mismatch: expected {self.dimension}, got {embeddings.shape[1]}")

        start_idx = self.index.ntotal
        self.index.add(embeddings)
        self.metadata.extend(metadata)
        end_idx = self.index.ntotal
        ranges = self.doc_type_map.get(doc_type, [])
        ranges.append((start_idx, end_idx))
        self.doc_type_map[doc_type] = ranges

    def search(
        self,
        query_embedding: np.ndarray,
        k: int = 5,
        doc_type: Optional[str] = None,
        ticker: Optional[str] = None,
        min_score: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Search for nearest neighbors and return metadata with similarity scores.
        
        When ticker is provided, results matching that ticker are prioritized (sorted first)
        but other relevant results are still included if there aren't enough ticker matches.
        """
        if self.index.ntotal == 0:
            return []
        
        if query_embedding.ndim == 1:
            query_embedding = query_embedding.reshape(1, -1)
        
        query_embedding = query_embedding.astype('float32')
        
        # Search for more results if ticker prioritization is enabled
        # This ensures we have enough results to prioritize properly
        search_k = k * 3 if ticker else k * 2
        distances, indices = self.index.search(query_embedding, min(search_k, self.index.ntotal))
        
        ticker_matches = []
        other_results = []
        
        for i, idx in enumerate(indices[0]):
            if idx < 0 or idx >= len(self.metadata):
                continue
            
            metadata = self.metadata[idx].copy()
            distance = float(distances[0][i])
            
            # Convert distance to similarity score (lower distance = higher similarity)
            similarity_score = 1.0 / (1.0 + distance)
            
            # Apply doc_type filter
            if doc_type and metadata.get('doc_type') != doc_type:
                continue
            
            # Apply min_score filter
            if min_score and similarity_score < min_score:
                continue
            
            metadata['similarity_score'] = similarity_score
            metadata['distance'] = distance
            
            # If ticker is provided, separate results by ticker match
            if ticker:
                result_ticker = metadata.get('ticker', '').upper()
                if result_ticker == ticker.upper():
                    ticker_matches.append(metadata)
                else:
                    other_results.append(metadata)
            else:
                # No ticker filter, just add to results
                ticker_matches.append(metadata)
        
        # Sort both lists by similarity score (descending)
        ticker_matches.sort(key=lambda x: x['similarity_score'], reverse=True)
        other_results.sort(key=lambda x: x['similarity_score'], reverse=True)
        
        # Combine: ticker matches first, then other results
        if ticker:
            results = ticker_matches + other_results
        else:
            results = ticker_matches
        
        # Return top k results
        final = results[:k]

        # Compute ticker presence diagnostics (lightweight scan)
        ticker_present_total = 0
        ticker_present_doc_type = 0
        if ticker:
            target = ticker.upper()
            ticker_present_total = sum(1 for m in self.metadata if m.get("ticker", "").upper() == target)
            if doc_type:
                ticker_present_doc_type = sum(
                    1
                    for m in self.metadata
                    if m.get("ticker", "").upper() == target and m.get("doc_type") == doc_type
                )

        # region agent log
        try:
            with open("/Users/danielli/Documents/penn/fa25/is/.cursor/debug.log", "a") as _f:
                _f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "run3",
                    "hypothesisId": "H4",
                    "location": "vector_store.py:search",
                    "message": "vector search results",
                    "data": {
                        "doc_type": doc_type,
                        "ticker_param": ticker,
                        "k": k,
                        "search_k": search_k,
                        "ticker_matches": len(ticker_matches),
                        "other_results": len(other_results),
                        "returned": len(final),
                        "sample_returned_tickers": list({m.get('ticker') for m in final if m.get('ticker')})[:5],
                        "ticker_present_total": ticker_present_total,
                        "ticker_present_doc_type": ticker_present_doc_type,
                    },
                    "timestamp": int(datetime.now().timestamp() * 1000),
                }) + "\n")
        except Exception:
            pass
        # endregion

        return final
    
    def save(self, save_path: Path, use_storage_adapter: bool = False, config=None):
        """Save index and metadata to disk, optionally to Supabase."""
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save FAISS index
        index_file = save_path.with_suffix('.index')
        faiss.write_index(self.index, str(index_file))
        
        # Save metadata and mappings
        data = {
            'metadata': self.metadata,
            'doc_type_map': self.doc_type_map,
            'dimension': self.dimension
        }
        pkl_file = save_path.with_suffix('.pkl')
        with open(pkl_file, 'wb') as f:
            pickle.dump(data, f)
        
        # Also save to Supabase if enabled
        if use_storage_adapter and config:
            from utils.storage import StorageAdapter
            storage = StorageAdapter(config)
            # Save index file
            index_remote = f"indices/{index_file.name}"
            with open(index_file, 'rb') as f:
                index_data = f.read()
            storage.save_bytes(index_data, index_file, index_remote, "application/octet-stream")
            # Save pkl file
            pkl_remote = f"indices/{pkl_file.name}"
            with open(pkl_file, 'rb') as f:
                pkl_data = f.read()
            storage.save_bytes(pkl_data, pkl_file, pkl_remote, "application/octet-stream")
    
    def load(self, load_path: Path, use_storage_adapter: bool = False, config=None):
        """Load index and metadata from disk, optionally from Supabase."""
        index_file = load_path.with_suffix('.index')
        pkl_file = load_path.with_suffix('.pkl')
        
        # Try to load from Supabase if enabled
        if use_storage_adapter and config:
            from utils.storage import StorageAdapter
            storage = StorageAdapter(config)
            index_remote = f"indices/{index_file.name}"
            pkl_remote = f"indices/{pkl_file.name}"
            
            # Try loading from Supabase
            index_data = storage.load_bytes(index_file, index_remote)
            pkl_data = storage.load_bytes(pkl_file, pkl_remote)
            
            if index_data and pkl_data:
                # Load from downloaded data
                self.index = faiss.read_index(BytesIO(index_data))
                data = pickle.loads(pkl_data)
                self.metadata = data['metadata']
                self.doc_type_map = data.get('doc_type_map', {})
                self.dimension = data.get('dimension', 384)
                return
        
        # Fall back to local files
        if not index_file.exists() or not pkl_file.exists():
            raise FileNotFoundError(f"Index files not found at {load_path}")
        
        # Load FAISS index
        self.index = faiss.read_index(str(index_file))
        
        # Load metadata
        with open(pkl_file, 'rb') as f:
            data = pickle.load(f)
            self.metadata = data['metadata']
            self.doc_type_map = data.get('doc_type_map', {})
            self.dimension = data.get('dimension', 384)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the vector store."""
        return {
            'total_documents': self.index.ntotal,
            'dimension': self.dimension,
            'doc_types': list(self.doc_type_map.keys()),
            'doc_type_counts': {
                doc_type: sum(end - start for start, end in ranges)
                for doc_type, ranges in self.doc_type_map.items()
            }
        }

