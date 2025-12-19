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


def screen_rising_operational_risk(
    lookback_filings: int = 3,
    top_n: int = 10,
    require_stable_price: bool = False,
    stable_price_days: int = 180,
    max_abs_return: float = 0.20,
    max_daily_vol: float = 0.03,
) -> Dict[str, Any]:
    """
    Screen for firms with rising "operational risk" mentions in recent 10-K Risk Factors sections.

    This is intended for broad queries like:
      "firms with rising operational risk mentions but stable headline metrics"

    - "Rising operational risk mentions" is approximated by keyword frequency in the Risk Factors section
      across the most recent `lookback_filings` 10-Ks.
    - "Stable headline metrics" is approximated by stable stock performance when price data exists.
      If `require_stable_price=True`, tickers without price data are excluded.
    """
    cfg = ETLConfig()
    filings_dir = cfg.PROCESSED_FILINGS_DIR

    if not filings_dir.exists():
        return {"error": f"Processed filings dir not found: {filings_dir}"}

    import glob
    import re
    from datetime import datetime

    # Operational-risk proxy keywords (broad but specific enough for screening)
    kw = [
        r"operational",
        r"supply\s+chain",
        r"manufactur",
        r"capacity\s+constraint",
        r"disruption",
        r"outage",
        r"cyber",
        r"security\s+incident",
        r"third[-\s]?party",
        r"vendor",
        r"regulator",
        r"export\s+control",
        r"geopolit",
        r"sanction",
        r"shortage",
        r"logistics",
    ]
    kw_re = re.compile("|".join(kw), re.IGNORECASE)

    # Collect 10-K parquet files by ticker
    files = glob.glob(str(filings_dir / "*_10-K_*.parquet"))
    by_ticker: Dict[str, List[Path]] = {}
    for f in files:
        p = Path(f)
        ticker = p.name.split("_", 1)[0].upper()
        by_ticker.setdefault(ticker, []).append(p)

    def parse_date_from_name(name: str) -> Optional[datetime]:
        # expects *_10-K_YYYY-MM-DD.parquet
        try:
            parts = name.replace(".parquet", "").split("_")
            date_str = parts[-1]
            return datetime.fromisoformat(date_str)
        except Exception:
            return None

    results = []

    # Optional: price stability metrics
    prices_df = None
    if cfg.PROCESSED_PRICES_FILE.exists():
        try:
            prices_df = pd.read_parquet(cfg.PROCESSED_PRICES_FILE)
            if "ticker" in prices_df.columns:
                prices_df["ticker"] = prices_df["ticker"].astype(str).str.upper()
        except Exception:
            prices_df = None

    for ticker, paths in by_ticker.items():
        # sort by filing date (desc)
        dated = [(parse_date_from_name(p.name), p) for p in paths]
        dated = [(d, p) for d, p in dated if d is not None]
        dated.sort(key=lambda x: x[0], reverse=True)
        if len(dated) < max(2, lookback_filings):
            continue
        dated = dated[:lookback_filings]

        series = []
        for d, p in dated:
            df = pd.read_parquet(p)
            if "section" in df.columns:
                df = df[df["section"].astype(str).str.lower() == "risk factors"]
            if df.empty or "text" not in df.columns:
                continue
            text = " ".join(df["text"].astype(str).tolist())
            hits = len(kw_re.findall(text))
            denom = max(len(text), 1)
            rate = hits / denom * 10000.0  # hits per 10k chars
            series.append({"date": d.date().isoformat(), "hits": hits, "rate_per_10k_chars": rate})

        if len(series) < 2:
            continue

        # Determine "rising": most recent rate > oldest rate by a margin
        newest = series[0]["rate_per_10k_chars"]
        oldest = series[-1]["rate_per_10k_chars"]
        delta = newest - oldest

        # Price stability (optional)
        price_metrics = None
        stable = None
        if prices_df is not None:
            tdf = prices_df[prices_df["ticker"] == ticker].copy()
            if not tdf.empty and "date" in tdf.columns and "close" in tdf.columns:
                tdf["date"] = pd.to_datetime(tdf["date"])
                tdf = tdf.sort_values("date").tail(stable_price_days)
                if len(tdf) >= 10:
                    rets = tdf["close"].pct_change().dropna()
                    abs_return = float((tdf["close"].iloc[-1] / tdf["close"].iloc[0]) - 1.0)
                    daily_vol = float(rets.std()) if len(rets) else None
                    stable = (abs(abs_return) <= max_abs_return) and (daily_vol is not None and daily_vol <= max_daily_vol)
                    price_metrics = {"abs_return": abs_return, "daily_vol": daily_vol, "window_days": int(len(tdf))}

        if require_stable_price and stable is not True:
            continue

        results.append({
            "ticker": ticker,
            "delta_rate_per_10k_chars": delta,
            "series": series,
            "price_stable": stable,
            "price_metrics": price_metrics,
        })

    # Rank by rising delta
    results.sort(key=lambda r: r["delta_rate_per_10k_chars"], reverse=True)
    results = results[:top_n]

    return {
        "count": len(results),
        "lookback_filings": lookback_filings,
        "require_stable_price": require_stable_price,
        "results": results,
        "note": "Price stability is only evaluated for tickers present in processed prices.parquet; others will have price_metrics=None.",
    }

