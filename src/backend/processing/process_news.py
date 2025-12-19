import pandas as pd
import os
import sys
from pathlib import Path
from typing import Optional
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from etl.config import ETLConfig
from processing.docetl_pipelines import (
    DocETLError,
    extract_news_insights,
)
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


def process_news_file(input_path, output_path=None, config: Optional[ETLConfig] = None):
    """Process a news parquet file by adding sentiment and embeddings."""
    cfg = config or ETLConfig()
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
    from utils.storage import StorageAdapter
    storage = StorageAdapter(cfg)
    output_path_obj = Path(output_path)
    remote_path = f"processed/news/{output_path_obj.name}"
    storage.save_parquet(df, output_path_obj, remote_path)

    # DocETL insights per file (optional; combined output also produced later)
    if cfg.DOCETL_ENABLED:
        insights = []
        for _, row in df.iterrows():
            try:
                insights.append(
                    extract_news_insights(
                        row.get("title", ""),
                        description=row.get("description", ""),
                        summary=row.get("summary", ""),
                        ticker=row.get("ticker", ""),
                        link=row.get("link", ""),
                        published=str(row.get("published", "")),
                        config=cfg,
                    )
                )
            except DocETLError as exc:
                print(f"[DOCETL][NEWS] Failed for {input_path}: {exc}")
        if insights:
            insights_df = pd.DataFrame(insights)
            insights_path = Path(output_path)
            insights_file = insights_path.with_name(insights_path.stem + "_insights.parquet")
            from utils.storage import StorageAdapter
            storage = StorageAdapter(cfg)
            remote_path = f"processed/news/{insights_file.name}"
            storage.save_parquet(insights_df, insights_file, remote_path)
    
    return df


def process_all_news(input_dir="data/raw/news", output_dir="data/processed/news", config: Optional[ETLConfig] = None):
    """Process all news files in the input directory."""
    import glob
    
    cfg = config or ETLConfig()
    files = glob.glob(os.path.join(input_dir, "*.parquet"))
    processed = []
    
    for filepath in files:
        filename = os.path.basename(filepath)
        output_path = os.path.join(output_dir, filename)
        df = process_news_file(filepath, output_path, config=cfg)
        processed.append(df)
    
    return processed


def combine_news_files(input_dir="data/raw/news", output_path="data/processed/news.parquet", config: Optional[ETLConfig] = None):
    """Process and combine all news files into a single DataFrame."""
    cfg = config or ETLConfig()
    processed = process_all_news(input_dir, config=cfg)
    
    if processed:
        combined = pd.concat(processed, ignore_index=True)
        from utils.storage import StorageAdapter
        storage = StorageAdapter(cfg)
        output_path_obj = Path(output_path)
        remote_path = f"processed/news/{output_path_obj.name}"
        storage.save_parquet(combined, output_path_obj, remote_path)
        if cfg.DOCETL_ENABLED:
            insights = []
            for _, row in combined.iterrows():
                try:
                    insights.append(
                        extract_news_insights(
                            row.get("title", ""),
                            description=row.get("description", ""),
                            summary=row.get("summary", ""),
                            ticker=row.get("ticker", ""),
                            link=row.get("link", ""),
                            published=str(row.get("published", "")),
                            config=cfg,
                        )
                    )
                except DocETLError as exc:
                    print(f"[DOCETL][NEWS] Failed during combine: {exc}")
            if insights:
                insights_df = pd.DataFrame(insights)
                insights_path = cfg.PROCESSED_NEWS_INSIGHTS_FILE
                from utils.storage import StorageAdapter
                storage = StorageAdapter(cfg)
                remote_path = f"processed/news/{insights_path.name}"
                storage.save_parquet(insights_df, insights_path, remote_path)
        return combined
    else:
        # Create empty file with expected structure if no data
        from utils.storage import StorageAdapter
        storage = StorageAdapter(cfg)
        output_path_obj = Path(output_path)
        empty_df = pd.DataFrame(columns=["ticker", "title", "clean_title", "sentiment", "published", "publisher"])
        remote_path = f"processed/news/{output_path_obj.name}"
        storage.save_parquet(empty_df, output_path_obj, remote_path)
        print(f"Warning: No news files found in {input_dir}. Created empty file at {output_path}")
        return empty_df