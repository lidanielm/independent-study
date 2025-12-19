"""
Build vector indices from processed financial documents.
Integrates with ETL pipeline to create searchable indices.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Optional, Set
import sys
import os
import json

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
    config_obj = ETLConfig()
    store.save(output_path, use_storage_adapter=config_obj.USE_SUPABASE_STORAGE, config=config_obj)
    print(f"Built news index with {len(metadata_list)} documents at {output_path}")
    
    return store


def build_news_insights_index(
    insights_path: Path,
    output_path: Path,
    ticker: Optional[str] = None,
) -> FinancialVectorStore:
    """Index DocETL-derived news events/entities."""
    if not insights_path.exists():
        return FinancialVectorStore()

    df = pd.read_parquet(insights_path)
    if ticker and 'ticker' in df.columns:
        df = df[df['ticker'].str.upper() == ticker.upper()]
    if df.empty:
        return FinancialVectorStore()

    embeddings_list = []
    metadata_list = []
    for idx, row in df.iterrows():
        text_parts = []
        events = row.get('events', [])
        if isinstance(events, str):
            try:
                events = json.loads(events)
            except Exception:
                events = []
        for event in events or []:
            if isinstance(event, dict):
                text_parts.append(f"{event.get('event_type', '')}: {event.get('rationale', '')}")
        text_parts.append(str(row.get('sentiment_with_rationale', '')))
        text = " ".join([part for part in text_parts if part]).strip()
        if not text:
            continue
        embedding = get_embedding(text)
        embeddings_list.append(embedding)
        metadata_list.append({
            'doc_type': 'news_insight',
            'ticker': row.get('ticker', ''),
            'link': row.get('link', ''),
            'published': str(row.get('published', '')),
            'index': int(idx),
        })

    if not embeddings_list:
        return FinancialVectorStore()

    embeddings = np.array(embeddings_list)
    store = FinancialVectorStore(dimension=embeddings.shape[1])
    store.add_documents(embeddings, metadata_list, doc_type='news_insight')
    config_obj = ETLConfig()
    store.save(output_path, use_storage_adapter=config_obj.USE_SUPABASE_STORAGE, config=config_obj)
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
        print(f"[INDEX_BUILDER] No filing parquet files found in {filings_dir}")
        return FinancialVectorStore()
    
    print(f"[INDEX_BUILDER] Found {len(filing_files)} filing parquet files to index")
    embeddings_list = []
    metadata_list = []
    
    for filing_file in filing_files:
        # Extract ticker from filename if not provided
        filename = Path(filing_file).stem
        file_ticker = filename.split('_')[0].upper() if '_' in filename else None
        
        # Only filter by ticker if explicitly requested AND we have a file_ticker to compare
        # This allows building comprehensive indices while still supporting ticker-specific builds
        if ticker and file_ticker and file_ticker != ticker.upper():
            print(f"[INDEX_BUILDER] Skipping {filename} (ticker filter: {file_ticker} != {ticker})")
            continue
        
        try:
            df = pd.read_parquet(filing_file)
            print(f"[INDEX_BUILDER] Loaded {filename}: {len(df)} rows, columns: {list(df.columns)}")
            
            if df.empty:
                print(f"[INDEX_BUILDER] Warning: {filename} is empty")
                continue
            
            rows_processed = 0
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
                    if not text or (isinstance(text, str) and len(text.strip()) == 0):
                        print(f"[INDEX_BUILDER] Warning: Empty text in row {idx} of {filename}, skipping")
                        continue
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
                rows_processed += 1
            
            print(f"[INDEX_BUILDER] Processed {rows_processed} rows from {filename}")
        except Exception as e:
            print(f"[INDEX_BUILDER] Error processing {filename}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    if not embeddings_list:
        # No filing data to index (empty files or no valid data)
        print(f"[INDEX_BUILDER] No embeddings extracted from filing files")
        return FinancialVectorStore()
    
    embeddings = np.array(embeddings_list)
    store = FinancialVectorStore(dimension=embeddings.shape[1])
    store.add_documents(embeddings, metadata_list, doc_type='filing')
    
    config_obj = ETLConfig()
    store.save(output_path, use_storage_adapter=config_obj.USE_SUPABASE_STORAGE, config=config_obj)
    print(f"[INDEX_BUILDER] Built filings index with {len(metadata_list)} documents at {output_path}")
    
    return store


def build_filings_insights_index(
    filings_insights_dir: Path,
    output_path: Path,
    ticker: Optional[str] = None,
) -> FinancialVectorStore:
    """Index DocETL-derived filing insights (summaries, risks, guidance)."""
    import glob

    if not filings_insights_dir.exists():
        return FinancialVectorStore()

    filing_files = glob.glob(str(filings_insights_dir / "*.parquet"))
    if not filing_files:
        return FinancialVectorStore()

    embeddings_list = []
    metadata_list = []

    for filing_file in filing_files:
        filename = Path(filing_file).stem
        file_ticker = filename.split('_')[0].upper() if '_' in filename else None
        if ticker and file_ticker and file_ticker != ticker.upper():
            continue
        df = pd.read_parquet(filing_file)
        if df.empty:
            continue
        for idx, row in df.iterrows():
            summary = row.get('mdna_summary', '')
            risks = row.get('risk_factors', [])
            risk_text = " ".join([r.get('risk', '') for r in risks]) if isinstance(risks, list) else ""
            text = f"{summary} {risk_text}".strip()
            if not text:
                continue
            embedding = get_embedding(text)
            embeddings_list.append(embedding)
            metadata_list.append({
                'doc_type': 'filing_insight',
                'ticker': row.get('ticker', file_ticker or ''),
                'filing_type': row.get('filing_type', ''),
                'filing_date': row.get('filing_date', ''),
                'index': int(idx),
            })

    if not embeddings_list:
        return FinancialVectorStore()

    embeddings = np.array(embeddings_list)
    store = FinancialVectorStore(dimension=embeddings.shape[1])
    store.add_documents(embeddings, metadata_list, doc_type='filing_insight')
    config_obj = ETLConfig()
    store.save(output_path, use_storage_adapter=config_obj.USE_SUPABASE_STORAGE, config=config_obj)
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
        
        # Only filter by ticker if explicitly requested
        # This allows building comprehensive indices while still supporting ticker-specific builds
        if ticker and file_ticker and file_ticker != ticker.upper():
            print(f"[INDEX_BUILDER] Skipping transcript {filename} (ticker filter: {file_ticker} != {ticker})")
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
    
    config_obj = ETLConfig()
    store.save(output_path, use_storage_adapter=config_obj.USE_SUPABASE_STORAGE, config=config_obj)
    print(f"Built transcripts index with {len(metadata_list)} documents at {output_path}")
    
    return store


def build_transcript_qa_index(
    transcripts_qa_dir: Path,
    output_path: Path,
    ticker: Optional[str] = None,
) -> FinancialVectorStore:
    """Index DocETL-derived transcript Q&A snippets."""
    import glob

    if not transcripts_qa_dir.exists():
        return FinancialVectorStore()

    qa_files = glob.glob(str(transcripts_qa_dir / "*.parquet"))
    if not qa_files:
        return FinancialVectorStore()

    embeddings_list = []
    metadata_list = []

    for qa_file in qa_files:
        filename = Path(qa_file).stem
        file_ticker = filename.split('_')[0].upper() if '_' in filename else None
        if ticker and file_ticker and file_ticker != ticker.upper():
            continue
        df = pd.read_parquet(qa_file)
        for idx, row in df.iterrows():
            question = row.get('question', '')
            answer = row.get('answer', '')
            text = f"{question}\n{answer}".strip()
            if not text:
                continue
            embedding = get_embedding(text)
            embeddings_list.append(embedding)
            metadata_list.append({
                'doc_type': 'transcript_qa',
                'ticker': file_ticker or ticker or '',
                'asked_by': row.get('asked_by', ''),
                'answered_by': row.get('answered_by', ''),
                'index': int(idx),
            })

    if not embeddings_list:
        return FinancialVectorStore()

    embeddings = np.array(embeddings_list)
    store = FinancialVectorStore(dimension=embeddings.shape[1])
    store.add_documents(embeddings, metadata_list, doc_type='transcript_qa')
    config_obj = ETLConfig()
    store.save(output_path, use_storage_adapter=config_obj.USE_SUPABASE_STORAGE, config=config_obj)
    return store


def build_transcript_guidance_index(
    transcripts_guidance_dir: Path,
    output_path: Path,
    ticker: Optional[str] = None,
) -> FinancialVectorStore:
    """Index DocETL-derived guidance statements from transcripts."""
    import glob

    if not transcripts_guidance_dir.exists():
        return FinancialVectorStore()

    guidance_files = glob.glob(str(transcripts_guidance_dir / "*.parquet"))
    if not guidance_files:
        return FinancialVectorStore()

    embeddings_list = []
    metadata_list = []

    for g_file in guidance_files:
        filename = Path(g_file).stem
        file_ticker = filename.split('_')[0].upper() if '_' in filename else None
        if ticker and file_ticker and file_ticker != ticker.upper():
            continue
        df = pd.read_parquet(g_file)
        for idx, row in df.iterrows():
            metric = row.get('metric', '')
            value = row.get('value', '')
            period = row.get('period', '')
            text = f"{metric}: {value} ({period})"
            if not text.strip():
                continue
            embedding = get_embedding(text)
            embeddings_list.append(embedding)
            metadata_list.append({
                'doc_type': 'transcript_guidance',
                'ticker': file_ticker or ticker or '',
                'period': period,
                'index': int(idx),
            })

    if not embeddings_list:
        return FinancialVectorStore()

    embeddings = np.array(embeddings_list)
    store = FinancialVectorStore(dimension=embeddings.shape[1])
    store.add_documents(embeddings, metadata_list, doc_type='transcript_guidance')
    config_obj = ETLConfig()
    store.save(output_path, use_storage_adapter=config_obj.USE_SUPABASE_STORAGE, config=config_obj)
    return store


def build_combined_index(
    config: Optional[ETLConfig] = None,
    ticker: Optional[str] = None,
    doc_types: Optional[Set[str]] = None,
) -> FinancialVectorStore:
    """Build combined vector index from all document types."""
    if config is None:
        config = ETLConfig()
    
    store = FinancialVectorStore()
    
    # Build indices for each document type
    indices_dir = config.PROCESSED_DIR / "indices"
    indices_dir.mkdir(parents=True, exist_ok=True)
    
    # News index
    if (doc_types is None or "news" in doc_types) and config.PROCESSED_NEWS_FILE.exists():
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

    # News insights index
    if (doc_types is None or "news_insight" in doc_types) and config.PROCESSED_NEWS_INSIGHTS_FILE.exists():
        news_insights_store = build_news_insights_index(
            config.PROCESSED_NEWS_INSIGHTS_FILE,
            indices_dir / "news_insights.index",
            ticker,
        )
        if news_insights_store.index.ntotal > 0:
            start_idx = store.index.ntotal
            for i in range(news_insights_store.index.ntotal):
                embedding = news_insights_store.index.reconstruct(i).reshape(1, -1)
                store.index.add(embedding.astype('float32'))
                store.metadata.append(news_insights_store.metadata[i])
            store.doc_type_map['news_insight'] = [(start_idx, store.index.ntotal)]
    
    # Filings index
    if (doc_types is None or "filing" in doc_types) and config.PROCESSED_FILINGS_DIR.exists():
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

    # Filings insights index
    if (doc_types is None or "filing_insight" in doc_types) and config.PROCESSED_FILINGS_INSIGHTS_DIR.exists():
        filings_insights_store = build_filings_insights_index(
            config.PROCESSED_FILINGS_INSIGHTS_DIR,
            indices_dir / "filings_insights.index",
            ticker,
        )
        if filings_insights_store.index.ntotal > 0:
            start_idx = store.index.ntotal
            for i in range(filings_insights_store.index.ntotal):
                embedding = filings_insights_store.index.reconstruct(i).reshape(1, -1)
                store.index.add(embedding.astype('float32'))
                store.metadata.append(filings_insights_store.metadata[i])
            store.doc_type_map['filing_insight'] = [(start_idx, store.index.ntotal)]
    
    # Transcripts index
    if (doc_types is None or "transcript" in doc_types) and config.PROCESSED_TRANSCRIPTS_DIR.exists():
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

    # Transcript Q&A index
    if (doc_types is None or "transcript_qa" in doc_types) and config.PROCESSED_TRANSCRIPTS_QA_DIR.exists():
        transcripts_qa_store = build_transcript_qa_index(
            config.PROCESSED_TRANSCRIPTS_QA_DIR,
            indices_dir / "transcripts_qa.index",
            ticker,
        )
        if transcripts_qa_store.index.ntotal > 0:
            start_idx = store.index.ntotal
            for i in range(transcripts_qa_store.index.ntotal):
                embedding = transcripts_qa_store.index.reconstruct(i).reshape(1, -1)
                store.index.add(embedding.astype('float32'))
                store.metadata.append(transcripts_qa_store.metadata[i])
            store.doc_type_map['transcript_qa'] = [(start_idx, store.index.ntotal)]

    # Transcript guidance index
    if (doc_types is None or "transcript_guidance" in doc_types) and config.PROCESSED_TRANSCRIPTS_GUIDANCE_DIR.exists():
        transcripts_guidance_store = build_transcript_guidance_index(
            config.PROCESSED_TRANSCRIPTS_GUIDANCE_DIR,
            indices_dir / "transcripts_guidance.index",
            ticker,
        )
        if transcripts_guidance_store.index.ntotal > 0:
            start_idx = store.index.ntotal
            for i in range(transcripts_guidance_store.index.ntotal):
                embedding = transcripts_guidance_store.index.reconstruct(i).reshape(1, -1)
                store.index.add(embedding.astype('float32'))
                store.metadata.append(transcripts_guidance_store.metadata[i])
            store.doc_type_map['transcript_guidance'] = [(start_idx, store.index.ntotal)]
    
    # Save combined index
    combined_path = indices_dir / "combined.index"
    if store.index.ntotal > 0:
        store.save(combined_path, use_storage_adapter=config.USE_SUPABASE_STORAGE, config=config)
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

