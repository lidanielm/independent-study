"""
ETL workflow orchestrator.

Coordinates the complete Extract, Transform, Load pipeline for financial data.
"""

import os
import sys
from pathlib import Path

# Add parent directories to path for imports
backend_path = Path(__file__).parent.parent
sys.path.insert(0, str(backend_path.parent))

from .config import ETLConfig

# Import ingestion modules
sys.path.insert(0, str(backend_path / "ingestion"))
from fetch_prices import fetch_prices_and_save
from fetch_news import fetch_news_and_save
from fetch_earnings_calls import download_transcripts_to_dataframe
from fetch_filings import fetch_filings, filings_to_dataframe

# Import processing modules
sys.path.insert(0, str(backend_path / "processing"))
from clean_prices import combine_price_files
from process_news import process_all_news, combine_news_files
from process_transcripts import process_transcript_from_text
from process_fundamentals import combine_fundamentals
from process_filings import process_all_filings
from build_features import build_features

# Import index builder
sys.path.insert(0, str(backend_path / "retrieval"))
from index_builder import build_combined_index


def extract_data(ticker, config=None):
    """Extract (fetch) all raw data for a ticker."""
    if config is None:
        config = ETLConfig()
    
    config.ensure_directories()
    status = {
        "ticker": ticker,
        "prices": {"success": False, "error": None},
        "news": {"success": False, "error": None},
        "transcripts": {"success": False, "error": None},
        "filings": {"success": False, "error": None},
        "fundamentals": {"success": False, "error": None},
    }
    
    # Extract prices
    try:
        print(f"[EXTRACT] Fetching prices for {ticker}...")
        fetch_prices_and_save(
            ticker,
            period=config.PRICE_PERIOD,
            interval=config.PRICE_INTERVAL,
            save_dir=str(config.RAW_PRICES_DIR)
        )
        status["prices"]["success"] = True
        print(f"[EXTRACT] ✓ Prices extracted for {ticker}")
    except Exception as e:
        status["prices"]["error"] = str(e)
        print(f"[EXTRACT] ✗ Failed to extract prices for {ticker}: {e}")
    
    # Extract news
    try:
        print(f"[EXTRACT] Fetching news for {ticker}...")
        fetch_news_and_save(
            ticker,
            max_articles=config.MAX_NEWS_ARTICLES,
            save_dir=str(config.RAW_NEWS_DIR)
        )
        status["news"]["success"] = True
        print(f"[EXTRACT] ✓ News extracted for {ticker}")
    except Exception as e:
        status["news"]["error"] = str(e)
        print(f"[EXTRACT] ✗ Failed to extract news for {ticker}: {e}")
    
    # Extract transcripts
    try:
        print(f"[EXTRACT] Fetching transcripts for {ticker}...")
        download_transcripts_to_dataframe(
            ticker,
            max_transcripts=config.MAX_TRANSCRIPTS,
            save_dir=str(config.RAW_TRANSCRIPTS_DIR)
        )
        status["transcripts"]["success"] = True
        print(f"[EXTRACT] ✓ Transcripts extracted for {ticker}")
    except Exception as e:
        status["transcripts"]["error"] = str(e)
        print(f"[EXTRACT] ✗ Failed to extract transcripts for {ticker}: {e}")
    
    # Extract filings
    try:
        print(f"[EXTRACT] Fetching filings for {ticker}...")
        filings_data = fetch_filings(ticker)
        df = filings_to_dataframe(filings_data)
        if not df.empty:
            save_path = config.RAW_FILINGS_DIR / f"{ticker}_filings.parquet"
            df.to_parquet(save_path, index=False)
            status["filings"]["success"] = True
            print(f"[EXTRACT] ✓ Filings extracted for {ticker}")
        else:
            status["filings"]["error"] = "No filings data returned"
            print(f"[EXTRACT] ✗ No filings data for {ticker}")
    except Exception as e:
        status["filings"]["error"] = str(e)
        print(f"[EXTRACT] ✗ Failed to extract filings for {ticker}: {e}")
    
    # Extract fundamentals (if API key available)
    try:
        print(f"[EXTRACT] Fetching fundamentals for {ticker}...")
        sys.path.insert(0, str(Path(__file__).parent.parent / "ingestion"))
        from fetch_filings import fetch_fundamentals
        fundamentals_data = fetch_fundamentals(ticker)
        if fundamentals_data and "annualReports" in fundamentals_data:
            import pandas as pd
            df = pd.DataFrame(fundamentals_data["annualReports"])
            if not df.empty:
                df["ticker"] = ticker  # Add ticker column
                save_path = config.RAW_FUNDAMENTALS_DIR / f"{ticker}_fundamentals.parquet"
                df.to_parquet(save_path, index=False)
                status["fundamentals"]["success"] = True
                print(f"[EXTRACT] ✓ Fundamentals extracted for {ticker}")
            else:
                status["fundamentals"]["error"] = "No fundamentals data in response"
                print(f"[EXTRACT] ⚠ No fundamentals data in response for {ticker}")
        else:
            status["fundamentals"]["error"] = "No fundamentals data available"
            print(f"[EXTRACT] ⚠ Fundamentals not available for {ticker} (API key may be required)")
    except Exception as e:
        status["fundamentals"]["error"] = str(e)
        print(f"[EXTRACT] ✗ Failed to extract fundamentals for {ticker}: {e}")
    
    return status


def transform_data(ticker, config=None):
    """Transform (process) all raw data for a ticker."""
    if config is None:
        config = ETLConfig()
    
    config.ensure_directories()
    status = {
        "ticker": ticker,
        "prices": {"success": False, "error": None},
        "news": {"success": False, "error": None},
        "transcripts": {"success": False, "error": None},
        "fundamentals": {"success": False, "error": None},
        "filings": {"success": False, "error": None},
    }
    
    # Transform prices
    try:
        print(f"[TRANSFORM] Processing prices for {ticker}...")
        combine_price_files(
            input_dir=str(config.RAW_PRICES_DIR),
            output_path=str(config.PROCESSED_PRICES_FILE)
        )
        status["prices"]["success"] = True
        print(f"[TRANSFORM] ✓ Prices processed for {ticker}")
    except Exception as e:
        status["prices"]["error"] = str(e)
        print(f"[TRANSFORM] ✗ Failed to process prices for {ticker}: {e}")
    
    # Transform news
    try:
        print(f"[TRANSFORM] Processing news for {ticker}...")
        combine_news_files(
            input_dir=str(config.RAW_NEWS_DIR),
            output_path=str(config.PROCESSED_NEWS_FILE)
        )
        status["news"]["success"] = True
        print(f"[TRANSFORM] ✓ News processed for {ticker}")
    except Exception as e:
        status["news"]["error"] = str(e)
        print(f"[TRANSFORM] ✗ Failed to process news for {ticker}: {e}")
    
    # Transform transcripts
    try:
        print(f"[TRANSFORM] Processing transcripts for {ticker}...")
        import glob
        transcript_files = glob.glob(str(config.RAW_TRANSCRIPTS_DIR / f"{ticker}_*.txt"))
        for transcript_file in transcript_files:
            with open(transcript_file, "r", encoding="utf-8") as f:
                text = f.read()
            filename = os.path.basename(transcript_file).replace(".txt", ".parquet")
            output_path = config.PROCESSED_TRANSCRIPTS_DIR / filename
            process_transcript_from_text(text, str(output_path))
        status["transcripts"]["success"] = True
        print(f"[TRANSFORM] ✓ Transcripts processed for {ticker}")
    except Exception as e:
        status["transcripts"]["error"] = str(e)
        print(f"[TRANSFORM] ✗ Failed to process transcripts for {ticker}: {e}")
    
    # Transform fundamentals
    try:
        print(f"[TRANSFORM] Processing fundamentals for {ticker}...")
        combine_fundamentals(
            input_dir=str(config.RAW_FUNDAMENTALS_DIR),
            output_path=str(config.PROCESSED_FUNDAMENTALS_FILE)
        )
        status["fundamentals"]["success"] = True
        print(f"[TRANSFORM] ✓ Fundamentals processed for {ticker}")
    except Exception as e:
        status["fundamentals"]["error"] = str(e)
        print(f"[TRANSFORM] ✗ Failed to process fundamentals for {ticker}: {e}")
    
    # Transform filings
    try:
        print(f"[TRANSFORM] Processing filings for {ticker}...")
        process_all_filings(
            input_dir=str(config.RAW_FILINGS_DIR),
            output_dir=str(config.PROCESSED_FILINGS_DIR)
        )
        status["filings"]["success"] = True
        print(f"[TRANSFORM] ✓ Filings processed for {ticker}")
    except Exception as e:
        status["filings"]["error"] = str(e)
        print(f"[TRANSFORM] ✗ Failed to process filings for {ticker}: {e}")
    
    return status


def load_features(ticker, config=None):
    """Load (build and save) final features for a ticker."""
    if config is None:
        config = ETLConfig()
    
    config.ensure_directories()
    status = {
        "ticker": ticker,
        "features": {"success": False, "error": None},
    }
    
    try:
        print(f"[LOAD] Building features for {ticker}...")
        build_features(
            prices_path=str(config.PROCESSED_PRICES_FILE),
            news_path=str(config.PROCESSED_NEWS_FILE),
            output_path=str(config.FEATURES_FILE)
        )
        status["features"]["success"] = True
        print(f"[LOAD] ✓ Features built for {ticker}")
    except Exception as e:
        status["features"]["error"] = str(e)
        print(f"[LOAD] ✗ Failed to build features for {ticker}: {e}")
    
    return status


def build_vector_indices(ticker, config=None):
    """Build vector search indices from processed documents."""
    if config is None:
        config = ETLConfig()
    
    config.ensure_directories()
    status = {
        "ticker": ticker,
        "indices": {"success": False, "error": None},
    }
    
    try:
        print(f"[INDICES] Building vector indices for {ticker}...")
        build_combined_index(config, ticker=ticker)
        status["indices"]["success"] = True
        print(f"[INDICES] ✓ Vector indices built for {ticker}")
    except Exception as e:
        status["indices"]["error"] = str(e)
        print(f"[INDICES] ✗ Failed to build indices for {ticker}: {e}")
    
    return status


def run_etl_pipeline(ticker, config=None, skip_extract=False, skip_transform=False, skip_load=False):
    """Run the complete ETL pipeline for a ticker."""
    if config is None:
        config = ETLConfig()
    
    print(f"\n{'='*60}")
    print(f"Starting ETL pipeline for {ticker}")
    print(f"{'='*60}\n")
    
    results = {
        "ticker": ticker,
        "extract": None,
        "transform": None,
        "load": None,
        "indices": None,
        "overall_success": False,
    }
    
    # Extract
    if not skip_extract:
        results["extract"] = extract_data(ticker, config)
    else:
        print("[SKIP] Extraction step skipped")
        results["extract"] = {"ticker": ticker, "skipped": True}
    
    # Transform
    if not skip_transform:
        results["transform"] = transform_data(ticker, config)
    else:
        print("[SKIP] Transformation step skipped")
        results["transform"] = {"ticker": ticker, "skipped": True}
    
    # Load
    if not skip_load:
        results["load"] = load_features(ticker, config)
        # Build vector indices after loading features
        results["indices"] = build_vector_indices(ticker, config)
    else:
        print("[SKIP] Load step skipped")
        results["load"] = {"ticker": ticker, "skipped": True}
        results["indices"] = {"ticker": ticker, "skipped": True}
    
    # Determine overall success
    extract_success = results["extract"] is None or results["extract"].get("skipped") or any(
        v.get("success", False) for k, v in results["extract"].items() if k != "ticker"
    )
    transform_success = results["transform"] is None or results["transform"].get("skipped") or any(
        v.get("success", False) for k, v in results["transform"].items() if k != "ticker"
    )
    load_success = results["load"] is None or results["load"].get("skipped") or results["load"].get("features", {}).get("success", False)
    
    results["overall_success"] = extract_success and transform_success and load_success
    
    print(f"\n{'='*60}")
    print(f"ETL pipeline completed for {ticker}")
    print(f"Overall success: {results['overall_success']}")
    print(f"{'='*60}\n")
    
    return results


if __name__ == "__main__":
    # Example usage
    ticker = "AAPL"
    results = run_etl_pipeline(ticker)
    print("\nResults summary:")
    print(results)

