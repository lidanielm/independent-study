"""
Vector store for semantic search over financial documents.
Uses FAISS for efficient similarity search.
"""

import faiss
import numpy as np
import pickle
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import json


class FinancialVectorStore:
    """
    Vector store for financial documents (news, filings, transcripts).
    Supports multiple document types and efficient similarity search.
    """
    
    def __init__(self, dimension: int = 384, index_path: Optional[Path] = None):
        """
        Initialize vector store.
        
        Args:
            dimension: Embedding dimension (384 for all-MiniLM-L6-v2)
            index_path: Optional path to load existing index
        """
        self.dimension = dimension
        self.index = faiss.IndexFlatL2(dimension)  # L2 distance for similarity
        self.metadata = []  # Store document metadata
        self.doc_type_map = {}  # Map document type to index ranges
        
        if index_path and index_path.exists():
            self.load(index_path)
    
    def add_documents(
        self, 
        embeddings: np.ndarray, 
        metadata: List[Dict[str, Any]],
        doc_type: Optional[str] = None
    ):
        """
        Add document embeddings with metadata.
        
        Args:
            embeddings: numpy array of shape (n_docs, dimension)
            metadata: List of metadata dicts (one per document)
            doc_type: Optional document type label (e.g., 'news', 'filings')
        """
        if len(embeddings) != len(metadata):
            raise ValueError("Number of embeddings must match number of metadata entries")
        
        if embeddings.shape[1] != self.dimension:
            raise ValueError(f"Embedding dimension {embeddings.shape[1]} doesn't match store dimension {self.dimension}")
        
        start_idx = len(self.metadata)
        end_idx = start_idx + len(embeddings)
        
        # Add to index
        self.index.add(embeddings.astype('float32'))
        
        # Store metadata
        self.metadata.extend(metadata)
        
        # Track document type ranges
        if doc_type:
            if doc_type not in self.doc_type_map:
                self.doc_type_map[doc_type] = []
            self.doc_type_map[doc_type].append((start_idx, end_idx))
    
    def search(
        self, 
        query_embedding: np.ndarray, 
        k: int = 10,
        doc_type: Optional[str] = None,
        ticker: Optional[str] = None,
        min_score: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """
        Semantic search returning top-k similar documents.
        
        Args:
            query_embedding: Query embedding vector (1D array)
            k: Number of results to return
            doc_type: Optional filter by document type
            ticker: Optional filter by ticker symbol
            min_score: Optional minimum similarity score threshold
        
        Returns:
            List of result dictionaries with metadata and similarity score
        """
        if self.index.ntotal == 0:
            return []
        
        # Reshape query if needed
        if query_embedding.ndim == 1:
            query_embedding = query_embedding.reshape(1, -1)
        
        query_embedding = query_embedding.astype('float32')
        
        # Search
        distances, indices = self.index.search(query_embedding, min(k * 2, self.index.ntotal))
        
        results = []
        for i, idx in enumerate(indices[0]):
            if idx < 0 or idx >= len(self.metadata):
                continue
            
            metadata = self.metadata[idx].copy()
            distance = float(distances[0][i])
            
            # Convert distance to similarity score (lower distance = higher similarity)
            # Using inverse distance normalized to [0, 1] range
            similarity_score = 1.0 / (1.0 + distance)
            
            # Apply filters
            if doc_type and metadata.get('doc_type') != doc_type:
                continue
            
            if ticker and metadata.get('ticker', '').upper() != ticker.upper():
                continue
            
            if min_score and similarity_score < min_score:
                continue
            
            metadata['similarity_score'] = similarity_score
            metadata['distance'] = distance
            results.append(metadata)
            
            if len(results) >= k:
                break
        
        return results
    
    def save(self, save_path: Path):
        """Save index and metadata to disk."""
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
        with open(save_path.with_suffix('.pkl'), 'wb') as f:
            pickle.dump(data, f)
    
    def load(self, load_path: Path):
        """Load index and metadata from disk."""
        index_file = load_path.with_suffix('.index')
        pkl_file = load_path.with_suffix('.pkl')
        
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

