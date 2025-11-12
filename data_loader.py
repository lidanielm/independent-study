import yfinance as yf
import pandas as pd
import requests
import os

FMP_API_KEY = os.getenv("FMP_API_KEY")

def get_financial_ratios(ticker):
    url = f"https://financialmodelingprep.com/api/v3/ratios/{ticker}?limit=1&apikey={FMP_API_KEY}"
    r = requests.get(url)
    data = r.json()
    return pd.DataFrame(data) if data else pd.DataFrame()

def get_profile(ticker):
    url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
    r = requests.get(url)
    return pd.DataFrame(r.json()) if r.ok else pd.DataFrame()

def get_market_data(ticker):
    df = yf.download(ticker, period="1y", interval="1d", progress=False)
    return df[['Close']].rename(columns={'Close': ticker})

