"""
Build vector indices from processed financial documents.
Integrates with ETL pipeline to create searchable indices.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Optional
import sys
import os

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from retrieval.vector_store import FinancialVectorStore
from utils.nlp import get_embedding
from etl.config import ETLConfig


def build_news_index(
    news_path: Path,
    output_path: Path,
    ticker: Optional[str] = None
) -> FinancialVectorStore:
        """Build vector index from processed news data."""
    if not news_path.exists():
        print(f"Warning: News file not found at {news_path}")
        return FinancialVectorStore()
    
    df = pd.read_parquet(news_path)
    
    # Filter by ticker if specified
    if ticker and 'ticker' in df.columns:
        df = df[df['ticker'].str.upper() == ticker.upper()]
    
    if df.empty:
        print(f"Warning: No news data found")
        return FinancialVectorStore()
    
    # Extract embeddings
    embeddings_list = []
    metadata_list = []
    
    for idx, row in df.iterrows():
        # Get embedding if it exists, otherwise compute it
        if 'embedding' in row and row['embedding'] is not None:
            if isinstance(row['embedding'], np.ndarray):
                embedding = row['embedding']
            else:
                # If stored as list, convert to array
                embedding = np.array(row['embedding'])
        else:
            # Compute embedding from title
            text = row.get('clean_title', row.get('title', ''))
            embedding = get_embedding(text)
        
        embeddings_list.append(embedding)
        
        # Build metadata
        metadata = {
            'doc_type': 'news',
            'ticker': row.get('ticker', ''),
            'title': row.get('title', row.get('clean_title', '')),
            'description': row.get('description', ''),
            'link': row.get('link', ''),
            'published': str(row.get('published', '')) if pd.notna(row.get('published')) else '',
            'publisher': row.get('publisher', ''),
            'sentiment': float(row.get('sentiment', 0.0)) if pd.notna(row.get('sentiment')) else 0.0,
            'index': int(idx)
        }
        metadata_list.append(metadata)
    
    # Convert to numpy array
    if not embeddings_list:
        print(f"Warning: No embeddings found in news data")
        return FinancialVectorStore()
    
    embeddings = np.array(embeddings_list)
    
    # Create and populate vector store
    store = FinancialVectorStore(dimension=embeddings.shape[1])
    store.add_documents(embeddings, metadata_list, doc_type='news')
    
    # Save index
    store.save(output_path)
    print(f"Built news index with {len(metadata_list)} documents at {output_path}")
    
    return store


def build_filings_index(
    filings_dir: Path,
    output_path: Path,
    ticker: Optional[str] = None
) -> FinancialVectorStore:
        """Build vector index from processed filings data."""
    import glob
    
    if not filings_dir.exists():
        # Directory doesn't exist, no filings to process
        return FinancialVectorStore()
    
    filing_files = glob.glob(str(filings_dir / "*.parquet"))
    
    if not filing_files:
        # No filing files found, but this is okay - not all tickers have filings processed
        return FinancialVectorStore()
    
    embeddings_list = []
    metadata_list = []
    
    for filing_file in filing_files:
        # Extract ticker from filename if not provided
        filename = Path(filing_file).stem
        file_ticker = filename.split('_')[0].upper() if '_' in filename else None
        
        if ticker and file_ticker and file_ticker != ticker.upper():
            continue
        
        df = pd.read_parquet(filing_file)
        
        for idx, row in df.iterrows():
            # Get embedding
            if 'embedding' in row and row['embedding'] is not None:
                if isinstance(row['embedding'], np.ndarray):
                    embedding = row['embedding']
                else:
                    embedding = np.array(row['embedding'])
            else:
                # Compute from text
                text = row.get('text', '')
                embedding = get_embedding(text)
            
            embeddings_list.append(embedding)
            
            metadata = {
                'doc_type': 'filing',
                'ticker': file_ticker or ticker or '',
                'section': row.get('section', ''),
                'text': row.get('text', '')[:500],  # Truncate for metadata
                'sentiment_score': float(row.get('sentiment_score', 0.0)) if pd.notna(row.get('sentiment_score')) else 0.0,
                'filing_file': filename,
                'index': int(idx)
            }
            metadata_list.append(metadata)
    
    if not embeddings_list:
        # No filing data to index (empty files or no valid data)
        return FinancialVectorStore()
    
    embeddings = np.array(embeddings_list)
    store = FinancialVectorStore(dimension=embeddings.shape[1])
    store.add_documents(embeddings, metadata_list, doc_type='filing')
    
    store.save(output_path)
    print(f"Built filings index with {len(metadata_list)} documents at {output_path}")
    
    return store


def build_transcripts_index(
    transcripts_dir: Path,
    output_path: Path,
    ticker: Optional[str] = None
) -> FinancialVectorStore:
        """Build vector index from processed transcripts data."""
    import glob
    
    if not transcripts_dir.exists():
        # Directory doesn't exist, no transcripts to process
        return FinancialVectorStore()
    
    transcript_files = glob.glob(str(transcripts_dir / "*.parquet"))
    
    if not transcript_files:
        # No transcript files found, but this is okay - not all tickers have transcripts processed
        return FinancialVectorStore()
    
    embeddings_list = []
    metadata_list = []
    
    for transcript_file in transcript_files:
        filename = Path(transcript_file).stem
        file_ticker = filename.split('_')[0].upper() if '_' in filename else None
        
        if ticker and file_ticker and file_ticker != ticker.upper():
            continue
        
        df = pd.read_parquet(transcript_file)
        
        for idx, row in df.iterrows():
            if 'embedding' in row and row['embedding'] is not None:
                if isinstance(row['embedding'], np.ndarray):
                    embedding = row['embedding']
                else:
                    embedding = np.array(row['embedding'])
            else:
                text = row.get('text', '')
                embedding = get_embedding(text)
            
            embeddings_list.append(embedding)
            
            metadata = {
                'doc_type': 'transcript',
                'ticker': file_ticker or ticker or '',
                'speaker': row.get('speaker', ''),
                'text': row.get('text', '')[:500],
                'sentiment': float(row.get('sentiment', 0.0)) if pd.notna(row.get('sentiment')) else 0.0,
                'transcript_file': filename,
                'index': int(idx)
            }
            metadata_list.append(metadata)
    
    if not embeddings_list:
        # No transcript data to index (empty files or no valid data)
        return FinancialVectorStore()
    
    embeddings = np.array(embeddings_list)
    store = FinancialVectorStore(dimension=embeddings.shape[1])
    store.add_documents(embeddings, metadata_list, doc_type='transcript')
    
    store.save(output_path)
    print(f"Built transcripts index with {len(metadata_list)} documents at {output_path}")
    
    return store


def build_combined_index(
    config: Optional[ETLConfig] = None,
    ticker: Optional[str] = None
) -> FinancialVectorStore:
        """Build combined vector index from all document types."""
    if config is None:
        config = ETLConfig()
    
    store = FinancialVectorStore()
    
    # Build indices for each document type
    indices_dir = config.PROCESSED_DIR / "indices"
    indices_dir.mkdir(parents=True, exist_ok=True)
    
    # News index
    if config.PROCESSED_NEWS_FILE.exists():
        news_store = build_news_index(
            config.PROCESSED_NEWS_FILE,
            indices_dir / "news.index",
            ticker
        )
        if news_store.index.ntotal > 0:
            # Merge into combined store
            for i in range(news_store.index.ntotal):
                embedding = news_store.index.reconstruct(i).reshape(1, -1)
                store.index.add(embedding.astype('float32'))
                store.metadata.append(news_store.metadata[i])
            store.doc_type_map['news'] = [(0, news_store.index.ntotal)]
    
    # Filings index
    if config.PROCESSED_FILINGS_DIR.exists():
        filings_store = build_filings_index(
            config.PROCESSED_FILINGS_DIR,
            indices_dir / "filings.index",
            ticker
        )
        if filings_store.index.ntotal > 0:
            start_idx = store.index.ntotal
            for i in range(filings_store.index.ntotal):
                embedding = filings_store.index.reconstruct(i).reshape(1, -1)
                store.index.add(embedding.astype('float32'))
                store.metadata.append(filings_store.metadata[i])
            store.doc_type_map['filing'] = [(start_idx, store.index.ntotal)]
    
    # Transcripts index
    if config.PROCESSED_TRANSCRIPTS_DIR.exists():
        transcripts_store = build_transcripts_index(
            config.PROCESSED_TRANSCRIPTS_DIR,
            indices_dir / "transcripts.index",
            ticker
        )
        if transcripts_store.index.ntotal > 0:
            start_idx = store.index.ntotal
            for i in range(transcripts_store.index.ntotal):
                embedding = transcripts_store.index.reconstruct(i).reshape(1, -1)
                store.index.add(embedding.astype('float32'))
                store.metadata.append(transcripts_store.metadata[i])
            store.doc_type_map['transcript'] = [(start_idx, store.index.ntotal)]
    
    # Save combined index
    combined_path = indices_dir / "combined.index"
    if store.index.ntotal > 0:
        store.save(combined_path)
        print(f"Built combined index with {store.index.ntotal} total documents")
    else:
        print(f"Warning: No documents to index")
    
    return store


if __name__ == "__main__":
    # Test index building
    config = ETLConfig()
    config.ensure_directories()
    
    store = build_combined_index(config)
    print(f"\nIndex stats: {store.get_stats()}")

