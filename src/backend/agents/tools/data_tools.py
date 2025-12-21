"""
Data access tools for agents to retrieve processed financial data.
"""

from typing import Optional, Dict, Any, List
import pandas as pd
import sys
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from etl.config import ETLConfig


def _try_ensure_prices(ticker: str, config: ETLConfig) -> Dict[str, Any]:
    """
    Best-effort: fetch + process prices for a ticker, then update processed parquet.
    Intended to be called when price data is missing.
    """
    status: Dict[str, Any] = {"source": "prices", "ticker": ticker.upper(), "fetched": False, "processed": False, "error": None}
    try:
        if not getattr(config, "AUTO_ENABLED", False):
            status["error"] = "AUTO_ENABLED=False"
            return status

        # Local imports to keep tool import cost low
        from ingestion.fetch_prices import fetch_prices_and_save
        from processing.clean_prices import combine_price_files

        fetch_prices_and_save(
            ticker.upper(),
            period=config.PRICE_PERIOD,
            interval=config.PRICE_INTERVAL,
            save_dir=str(config.RAW_PRICES_DIR),
        )
        status["fetched"] = True

        # Note: combine is global (rebuilds the combined parquet from raw dir)
        combine_price_files(
            input_dir=str(config.RAW_PRICES_DIR),
            output_path=str(config.PROCESSED_PRICES_FILE),
        )
        status["processed"] = True
    except Exception as exc:
        status["error"] = str(exc)
    return status


def _try_ensure_fundamentals(ticker: str, config: ETLConfig) -> Dict[str, Any]:
    """
    Best-effort: fetch + process fundamentals for a ticker, then update processed parquet.
    Intended to be called when fundamentals data is missing.
    """
    status: Dict[str, Any] = {"source": "fundamentals", "ticker": ticker.upper(), "fetched": False, "processed": False, "error": None}
    try:
        if not getattr(config, "AUTO_ENABLED", False):
            status["error"] = "AUTO_ENABLED=False"
            return status

        # Local imports to keep tool import cost low
        from ingestion.fetch_filings import fetch_fundamentals
        from processing.process_fundamentals import combine_fundamentals

        fundamentals_data = fetch_fundamentals(ticker.upper())
        if fundamentals_data and "annualReports" in fundamentals_data:
            df = pd.DataFrame(fundamentals_data["annualReports"])
            if not df.empty:
                df["ticker"] = ticker.upper()
                config.RAW_FUNDAMENTALS_DIR.mkdir(parents=True, exist_ok=True)
                save_path = config.RAW_FUNDAMENTALS_DIR / f"{ticker.upper()}_fundamentals.parquet"
                df.to_parquet(save_path, index=False)
                status["fetched"] = True

        combine_fundamentals(
            input_dir=str(config.RAW_FUNDAMENTALS_DIR),
            output_path=str(config.PROCESSED_FUNDAMENTALS_FILE),
        )
        status["processed"] = True
    except Exception as exc:
        status["error"] = str(exc)
    return status


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
            # Best-effort auto fetch if enabled
            auto = _try_ensure_prices(ticker, config)
            # Retry combined file after fetch
            if config.PROCESSED_PRICES_FILE.exists():
                try:
                    df = pd.read_parquet(config.PROCESSED_PRICES_FILE)
                    ticker_data = df[df["ticker"] == ticker.upper()] if "ticker" in df.columns else df
                except Exception:
                    ticker_data = pd.DataFrame()
            else:
                ticker_data = pd.DataFrame()

            if ticker_data.empty:
                return {"error": f"No price data found for {ticker}", "auto_fetch": auto}
    
    if ticker_data.empty:
        # Best-effort auto fetch if enabled
        auto = _try_ensure_prices(ticker, config)
        if config.PROCESSED_PRICES_FILE.exists():
            try:
                df = pd.read_parquet(config.PROCESSED_PRICES_FILE)
                ticker_data = df[df["ticker"] == ticker.upper()] if "ticker" in df.columns else df
            except Exception:
                ticker_data = pd.DataFrame()

        if ticker_data.empty:
            return {"error": f"No price data found for {ticker}", "auto_fetch": auto}
    
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

