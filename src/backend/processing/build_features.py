import pandas as pd
import os


def compute_price_features(prices_df):
    """Compute technical features from price data."""
    prices = prices_df.copy()
    prices["returns_1d"] = prices.groupby("ticker")["close"].pct_change()
    prices["momentum_5d"] = prices.groupby("ticker")["close"].pct_change(5)
    prices["volatility_20d"] = prices.groupby("ticker")["close"].rolling(20).std().reset_index(level=0, drop=True)
    return prices


def aggregate_news_sentiment(news_df):
    """Aggregate news sentiment by ticker and date."""
    if news_df.empty or "sentiment" not in news_df.columns:
        return pd.DataFrame()
    
    # Check for date column variations
    date_col = None
    for col in ["date", "published", "publish_date", "timestamp"]:
        if col in news_df.columns:
            date_col = col
            break
    
    if date_col is None:
        # If no date column, aggregate by ticker only
        if "ticker" in news_df.columns:
            news_sent = news_df.groupby("ticker")["sentiment"].mean().reset_index()
            return news_sent
        else:
            # No ticker column either, return empty
            return pd.DataFrame()
    
    # Convert date column to datetime (consistent with prices)
    news_df = news_df.copy()
    news_df[date_col] = pd.to_datetime(news_df[date_col], errors="coerce")
    
    # Group by ticker and date
    if "ticker" in news_df.columns:
        news_sent = news_df.groupby(["ticker", date_col])["sentiment"].mean().reset_index()
        news_sent = news_sent.rename(columns={date_col: "date"})
    else:
        news_sent = news_df.groupby(date_col)["sentiment"].mean().reset_index()
        news_sent = news_sent.rename(columns={date_col: "date"})
    
    return news_sent


def build_features(prices_path="data/processed/prices.parquet",
                   news_path="data/processed/news.parquet",
                   output_path="data/processed/features.parquet"):
    """Build feature set from prices and news data (basic stock-related features only)."""
    from pathlib import Path
    
    # Load data with error handling
    prices = pd.DataFrame()
    if Path(prices_path).exists():
        try:
            prices = pd.read_parquet(prices_path)
        except Exception as e:
            print(f"Warning: Could not load prices from {prices_path}: {e}")
    else:
        print(f"Warning: Prices file not found: {prices_path}")
    
    news = pd.DataFrame()
    if Path(news_path).exists():
        try:
            news = pd.read_parquet(news_path)
        except Exception as e:
            print(f"Warning: Could not load news from {news_path}: {e}")
    else:
        print(f"Warning: News file not found: {news_path}")
    
    # Check if we have at least prices data
    if prices.empty:
        raise ValueError(f"No price data available. Please ensure prices are processed first. Expected file: {prices_path}")
    
    # Fix column names if they're multi-level (from yfinance) or string tuples
    import ast
    if any(isinstance(col, str) and col.startswith('(') and col.endswith(')') for col in prices.columns):
        new_columns = []
        for col in prices.columns:
            if isinstance(col, str) and col.startswith('(') and col.endswith(')'):
                try:
                    parsed = ast.literal_eval(col)
                    if isinstance(parsed, tuple):
                        if len(parsed) > 1 and parsed[1]:
                            new_col = f"{parsed[0]}_{parsed[1]}"
                        else:
                            new_col = str(parsed[0])
                        new_columns.append(new_col.lower())
                    else:
                        new_columns.append(col.lower())
                except:
                    new_columns.append(col.lower())
            else:
                new_columns.append(str(col).lower())
        prices.columns = new_columns
    
    # Handle ticker-specific columns (e.g., close_aapl, close_meta)
    # If we have ticker column and ticker-specific price columns, create unified columns
    if 'ticker' in prices.columns:
        close_cols = [col for col in prices.columns if 'close' in col.lower() and '_' in col]
        if close_cols and 'close' not in prices.columns:
            # Create a unified 'close' column by selecting the appropriate ticker-specific column
            def get_close_value(row):
                ticker = str(row.get('ticker', '')).lower()
                for col in close_cols:
                    if ticker in col.lower():
                        return row.get(col)
                return None
            
            prices['close'] = prices.apply(get_close_value, axis=1)
            
            # Do the same for other metrics
            for metric in ['open', 'high', 'low', 'volume']:
                metric_cols = [col for col in prices.columns if metric in col.lower() and '_' in col]
                if metric_cols and metric not in prices.columns:
                    def get_metric_value(row, m=metric):
                        ticker = str(row.get('ticker', '')).lower()
                        for col in metric_cols:
                            if ticker in col.lower():
                                return row.get(col)
                        return None
                    prices[metric] = prices.apply(get_metric_value, axis=1)
    
    # Ensure we have a 'close' column (check for variations)
    if 'close' not in prices.columns:
        # Try to find close column with different naming
        close_candidates = [col for col in prices.columns if 'close' in col.lower()]
        if close_candidates:
            # If multiple, prefer the one without ticker suffix, or use first
            preferred = [c for c in close_candidates if '_' not in c]
            if preferred:
                prices = prices.rename(columns={preferred[0]: 'close'})
            else:
                prices = prices.rename(columns={close_candidates[0]: 'close'})
        else:
            raise ValueError(f"No 'close' column found in price data. Available columns: {prices.columns.tolist()}")
    
    # Ensure we have a 'date' column
    if 'date' not in prices.columns:
        date_candidates = [col for col in prices.columns if 'date' in col.lower()]
        if date_candidates:
            prices = prices.rename(columns={date_candidates[0]: 'date'})
        else:
            raise ValueError(f"No 'date' column found in price data. Available columns: {prices.columns.tolist()}")
    
    # Ensure date is datetime
    prices['date'] = pd.to_datetime(prices['date'], errors='coerce')
    
    # Compute price features
    prices = compute_price_features(prices)
    
    # Aggregate news sentiment (if news data exists)
    news_sent = pd.DataFrame()
    if not news.empty:
        news_sent = aggregate_news_sentiment(news)
    
    # Start with prices as base
    features = prices.copy()
    
    # Merge news sentiment if available
    if not news_sent.empty and "ticker" in news_sent.columns and "date" in news_sent.columns:
        # Ensure date types match for merge (remove timezone info)
        if "date" in features.columns:
            features["date"] = pd.to_datetime(features["date"]).dt.tz_localize(None)
            news_sent["date"] = pd.to_datetime(news_sent["date"]).dt.tz_localize(None)
        features = features.merge(news_sent, how="left", on=["ticker", "date"])
    
    # Save to parquet
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    features.to_parquet(output_path, index=False)
    
    return features