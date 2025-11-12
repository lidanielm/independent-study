import pandas as pd

def compute_basic_metrics(ratios_df):
    if ratios_df.empty:
        return pd.Series()
    r = ratios_df.iloc[0]
    return pd.Series({
        "PE": r.get("priceEarningsRatio"),
        "PB": r.get("priceToBookRatio"),
        "ROE": r.get("returnOnEquity"),
        "ROA": r.get("returnOnAssets"),
        "DebtToEquity": r.get("debtEquityRatio"),
        "CurrentRatio": r.get("currentRatio")
    })

