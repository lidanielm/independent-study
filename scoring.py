import pandas as pd

def score_fundamentals(df):
    df = df.copy()
    # For metrics where higher is better
    positive = ["ROE", "ROA", "CurrentRatio"]
    # For metrics where lower is better
    negative = ["PE", "PB", "DebtToEquity"]

    for col in positive:
        df[f"{col}_score"] = df[col].rank(pct=True)
    for col in negative:
        df[f"{col}_score"] = 1 - df[col].rank(pct=True)

    df["CompositeScore"] = df[[c for c in df.columns if "_score" in c]].mean(axis=1)
    return df.sort_values("CompositeScore", ascending=False)

