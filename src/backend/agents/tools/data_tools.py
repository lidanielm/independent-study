"""
Data access tools for agents to retrieve processed financial data.
"""

from typing import Optional, Dict, Any
import pandas as pd
import sys
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from etl.config import ETLConfig


def get_price_data(ticker: str, limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Get price data for a ticker.
    """
    config = ETLConfig()
    
    # Try combined file first
    if config.PROCESSED_PRICES_FILE.exists():
        df = pd.read_parquet(config.PROCESSED_PRICES_FILE)
        ticker_data = df[df["ticker"] == ticker.upper()]
    else:
        # Try individual file
        filepath = config.PROCESSED_PRICES_DIR / f"{ticker.upper()}.parquet"
        if filepath.exists():
            df = pd.read_parquet(filepath)
            ticker_data = df
        else:
            return {"error": f"No price data found for {ticker}"}
    
    if ticker_data.empty:
        return {"error": f"No price data found for {ticker}"}
    
    if limit:
        ticker_data = ticker_data.tail(limit)
    
    return {
        "ticker": ticker.upper(),
        "count": len(ticker_data),
        "data": ticker_data.to_dict(orient='records')
    }


def get_fundamentals(ticker: str) -> Dict[str, Any]:
    """
    Get fundamentals data for a ticker.
    """
    config = ETLConfig()
    
    # Try combined file first
    if config.PROCESSED_FUNDAMENTALS_FILE.exists():
        df = pd.read_parquet(config.PROCESSED_FUNDAMENTALS_FILE)
        if "ticker" in df.columns:
            ticker_data = df[df["ticker"] == ticker.upper()]
        else:
            ticker_data = df
    else:
        # Try individual file
        filepath = config.PROCESSED_FUNDAMENTALS_DIR / f"{ticker.upper()}_fundamentals.parquet"
        if filepath.exists():
            df = pd.read_parquet(filepath)
            ticker_data = df
        else:
            return {"error": f"No fundamentals data found for {ticker}"}
    
    if ticker_data.empty:
        return {"error": f"No fundamentals data found for {ticker}"}
    
    return {
        "ticker": ticker.upper(),
        "count": len(ticker_data),
        "data": ticker_data.to_dict(orient='records')
    }


def get_features(ticker: str, limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Get processed features for a ticker.
    """
    config = ETLConfig()
    
    if not config.FEATURES_FILE.exists():
        return {"error": f"No features data found for {ticker}"}
    
    df = pd.read_parquet(config.FEATURES_FILE)
    ticker_data = df[df["ticker"] == ticker.upper()]
    
    if ticker_data.empty:
        return {"error": f"No features data found for {ticker}"}
    
    if limit:
        ticker_data = ticker_data.tail(limit)
    
    return {
        "ticker": ticker.upper(),
        "count": len(ticker_data),
        "data": ticker_data.to_dict(orient='records')
    }

