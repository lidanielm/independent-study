import re
import pandas as pd
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.filing_section_extractor import extract_sections
from utils.nlp import get_embedding, sentiment_score


def process_filing(path, encoding="utf-8", errors="ignore"):
    """Process a single SEC filing by extracting sections and computing features."""
    with open(path, "r", encoding=encoding, errors=errors) as f:
        text = f.read()

    sections = extract_sections(text)  # returns dict: {"Risk Factors": "...", ...}

    rows = []
    for section, content in sections.items():
        rows.append({
            "section": section,
            "text": content,
            "sentiment_score": sentiment_score(content),
            "embedding": get_embedding(content)
        })
    return rows


def process_filing_file(input_path, output_path=None):
    """Process a filing file and save results to parquet."""
    rows = process_filing(input_path)
    df = pd.DataFrame(rows)
    
    # Set output path if not provided
    if output_path is None:
        filename = os.path.basename(input_path).replace(".txt", ".parquet")
        output_path = os.path.join("data/processed/filings", filename)
    
    # Save processed data
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    
    return df


def process_all_filings(input_dir="data/raw/filings", output_dir="data/processed/filings"):
    """Process all filing files in the input directory."""
    import glob
    
    files = glob.glob(os.path.join(input_dir, "*.txt"))
    processed = []
    
    for filepath in files:
        df = process_filing_file(filepath)
        processed.append(df)
    
    return processed