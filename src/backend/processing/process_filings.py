import pandas as pd
import os
import sys
import re
from pathlib import Path
from typing import Optional
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from etl.config import ETLConfig
from utils.filing_section_extractor import extract_sections
from utils.nlp import get_embedding, sentiment_score
from processing.docetl_pipelines import (
    DocETLError,
    extract_sec_filing_insights,
)


def process_filing_text(text: str):
    """Process SEC filing text by extracting sections and computing features."""
    sections = extract_sections(text)  # returns dict: {"Risk Factors": "...", ...}

    def chunk_text(s: str, max_chars: int = 2000) -> list[str]:
        s = (s or "").strip()
        if not s:
            return []
        if len(s) <= max_chars:
            return [s]
        chunks = []
        start = 0
        n = len(s)
        while start < n:
            end = min(start + max_chars, n)
            # try to break on a space to avoid cutting words
            if end < n:
                space = s.rfind(" ", start, end)
                if space > start + int(max_chars * 0.6):
                    end = space
            chunks.append(s[start:end].strip())
            start = end
        return [c for c in chunks if c]

    rows = []
    for section, content in sections.items():
        for chunk_idx, chunk in enumerate(chunk_text(content)):
            rows.append({
                "section": section,
                "chunk_index": int(chunk_idx),
                "text": chunk,
                "sentiment_score": sentiment_score(chunk),
                "embedding": get_embedding(chunk)
            })
    return rows


def _strip_html(text: str) -> str:
    """Strip HTML tags and decode entities from filing text."""
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode common HTML entities
    import html
    text = html.unescape(text)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def process_filing_file(input_path, output_path=None, config: Optional[ETLConfig] = None):
    """Process a filing file and save results to parquet."""
    cfg = config or ETLConfig()
    with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
        raw_text = f.read()
    
    # Strip HTML if present
    text = _strip_html(raw_text)
    
    # Ensure we have text to process
    if not text or len(text.strip()) < 100:
        print(f"[PROCESS_FILINGS] Warning: Insufficient text after HTML stripping in {os.path.basename(input_path)} (length: {len(text) if text else 0})")
        # Try using raw text as fallback
        text = raw_text[:100000] if raw_text else ""
        if not text or len(text.strip()) < 100:
            print(f"[PROCESS_FILINGS] Error: Cannot process {os.path.basename(input_path)} - no usable text")
            return pd.DataFrame()
    
    # If no sections found, create a fallback section with the full text
    rows = process_filing_text(text)
    if not rows:
        # Fallback: use entire document as one section
        print(f"[PROCESS_FILINGS] No sections extracted from {os.path.basename(input_path)}, using full document")
        text_chunk = text[:50000] if len(text) > 50000 else text
        if text_chunk and len(text_chunk.strip()) > 0:
            rows = [{
                "section": "Full Document",
                "text": text_chunk,
                "sentiment_score": sentiment_score(text_chunk),
                "embedding": get_embedding(text_chunk)
            }]
        else:
            print(f"[PROCESS_FILINGS] Error: Cannot create fallback - text chunk is empty")
            return pd.DataFrame()
    else:
        print(f"[PROCESS_FILINGS] Extracted {len(rows)} sections from {os.path.basename(input_path)}")
    
    df = pd.DataFrame(rows)
    
    # Verify DataFrame has required columns
    if df.empty:
        print(f"[PROCESS_FILINGS] Error: DataFrame is empty for {os.path.basename(input_path)}")
        return df
    
    required_cols = ['text', 'embedding']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"[PROCESS_FILINGS] Error: Missing required columns {missing_cols} in DataFrame for {os.path.basename(input_path)}")
        return pd.DataFrame()
    
    # Set output path if not provided
    if output_path is None:
        filename = os.path.basename(input_path).replace(".txt", ".parquet")
        output_path = os.path.join("data/processed/filings", filename)
    
    # Save processed data
    from utils.storage import StorageAdapter
    storage = StorageAdapter(cfg)
    output_path_obj = Path(output_path)
    remote_path = f"processed/filings/{output_path_obj.name}"
    storage.save_parquet(df, output_path_obj, remote_path)

    # DocETL structured insights (optional)
    if cfg.DOCETL_ENABLED:
        parts = os.path.basename(input_path).replace(".txt", "").split("_")
        ticker = parts[0] if parts else ""
        filing_type = parts[1] if len(parts) > 1 else ""
        filing_date = parts[2] if len(parts) > 2 else ""
        try:
            insights = extract_sec_filing_insights(
                text,  # Use cleaned text (HTML stripped)
                ticker=ticker,
                filing_type=filing_type,
                filing_date=filing_date,
                config=cfg,
            )
            insights_df = pd.DataFrame([insights])
            insights_path = cfg.PROCESSED_FILINGS_INSIGHTS_DIR / os.path.basename(output_path)
            from utils.storage import StorageAdapter
            storage = StorageAdapter(cfg)
            remote_path = f"processed/filings_insights/{insights_path.name}"
            storage.save_parquet(insights_df, insights_path, remote_path)
        except DocETLError as exc:
            print(f"[DOCETL][FILING] Failed for {input_path}: {exc}")
    
    return df


def process_all_filings(
    input_dir="data/raw/filings",
    output_dir="data/processed/filings",
    config: Optional[ETLConfig] = None,
):
    """Process all filing files in the input directory."""
    import glob
    
    cfg = config or ETLConfig()
    files = glob.glob(os.path.join(input_dir, "*.txt"))
    processed = []
    
    print(f"[PROCESS_FILINGS] Processing {len(files)} filing files from {input_dir}")
    for filepath in files:
        filename = os.path.basename(filepath).replace(".txt", ".parquet")
        output_path = os.path.join(output_dir, filename)
        df = process_filing_file(filepath, output_path=output_path, config=cfg)
        if not df.empty:
            processed.append(df)
            print(f"[PROCESS_FILINGS] Saved {len(df)} rows to {output_path}")
        else:
            print(f"[PROCESS_FILINGS] Warning: No data processed for {filepath}")
    
    print(f"[PROCESS_FILINGS] Processed {len(processed)} filing files successfully")
    return processed