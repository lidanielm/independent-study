import pandas as pd
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.sentiment_model import sentiment
from utils.nlp import get_embedding


def process_news_article(title):
    """Process a single news article title."""
    if not title or not isinstance(title, str):
        return {
            "clean_title": "",
            "sentiment": 0.0,
            "embedding": get_embedding("")
        }
    
    clean_title = title.replace("\n", " ").strip()
    return {
        "clean_title": clean_title,
        "sentiment": sentiment(clean_title),
        "embedding": get_embedding(clean_title)
    }


def process_news_file(input_path, output_path=None):
    """Process a news parquet file by adding sentiment and embeddings."""
    df = pd.read_parquet(input_path)
    
    # Process each article
    processed = df["title"].apply(process_news_article)
    
    # Add processed columns
    df["clean_title"] = processed.apply(lambda x: x["clean_title"])
    df["sentiment"] = processed.apply(lambda x: x["sentiment"])
    df["embedding"] = processed.apply(lambda x: x["embedding"])
    
    # Set output path if not provided
    if output_path is None:
        filename = os.path.basename(input_path)
        output_path = os.path.join("data/processed/news", filename)
    
    # Save processed data
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    
    return df


def process_all_news(input_dir="data/raw/news", output_dir="data/processed/news"):
    """Process all news files in the input directory."""
    import glob
    
    files = glob.glob(os.path.join(input_dir, "*.parquet"))
    processed = []
    
    for filepath in files:
        filename = os.path.basename(filepath)
        output_path = os.path.join(output_dir, filename)
        df = process_news_file(filepath, output_path)
        processed.append(df)
    
    return processed


def combine_news_files(input_dir="data/raw/news", output_path="data/processed/news.parquet"):
    """Process and combine all news files into a single DataFrame."""
    processed = process_all_news(input_dir)
    
    if processed:
        combined = pd.concat(processed, ignore_index=True)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        combined.to_parquet(output_path, index=False)
        return combined
    else:
        # Create empty file with expected structure if no data
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        empty_df = pd.DataFrame(columns=["ticker", "title", "clean_title", "sentiment", "published", "publisher"])
        empty_df.to_parquet(output_path, index=False)
        print(f"Warning: No news files found in {input_dir}. Created empty file at {output_path}")
        return empty_df