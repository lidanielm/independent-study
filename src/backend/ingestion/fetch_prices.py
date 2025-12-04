import os
import yfinance as yf
import pandas as pd

def fetch_prices(ticker, period="5y", interval="1d"):
    """Fetch price data for a ticker."""
    data = yf.download(ticker, period=period, interval=interval)
    data.reset_index(inplace=True)
    return data

def fetch_prices_and_save(ticker, period="5y", interval="1d", save_dir="data/raw/prices"):
    """Fetch price data and save to parquet file."""
    os.makedirs(save_dir, exist_ok=True)
    df = fetch_prices(ticker, period, interval)
    filepath = os.path.join(save_dir, f"{ticker}.parquet")
    df.to_parquet(filepath, index=False)
    print(f"Saved price data for {ticker} to {filepath}")
    return df