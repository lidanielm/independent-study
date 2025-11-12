import streamlit as st
import pandas as pd
from data_loader import get_financial_ratios, get_profile
from feature_engineering import compute_basic_metrics
from scoring import score_fundamentals

st.set_page_config(page_title="Fundamental Research Tool", layout="wide")

st.title("📈 Fundamental Stock Research Tool (MVP)")

tickers = st.text_input("Enter tickers (comma-separated):", "AAPL,MSFT,GOOG,AMZN,JNJ")
tickers = [t.strip().upper() for t in tickers.split(",")]

rows = []
for t in tickers:
    ratios = get_financial_ratios(t)
    profile = get_profile(t)
    metrics = compute_basic_metrics(ratios)
    if not metrics.empty:
        metrics["Ticker"] = t
        rows.append(metrics)

if rows:
    df = pd.DataFrame(rows)
    scored = score_fundamentals(df)

    st.subheader("Company Rankings")
    st.dataframe(scored[["Ticker", "CompositeScore"] + [c for c in scored.columns if "_score" in c]])

    selected = st.selectbox("Select a company for details", scored["Ticker"])
    prof = get_profile(selected)
    st.write("### Profile", prof[["companyName", "industry", "sector", "description"]].T)
else:
    st.warning("No data available. Check tickers or API key.")

