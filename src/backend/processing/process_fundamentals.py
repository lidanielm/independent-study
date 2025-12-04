import pandas as pd
import glob
import os


def compute_ratios(df):
    """Compute financial ratios from fundamental data."""
    df = df.copy()
    
    # Year-over-year revenue growth
    if "revenue" in df.columns:
        df["revenue_yoy"] = df["revenue"].pct_change()
    
    # Margin calculations
    if "revenue" in df.columns and "grossProfit" in df.columns:
        df["gross_margin"] = df["grossProfit"] / df["revenue"]
    
    if "revenue" in df.columns and "operatingIncome" in df.columns:
        df["operating_margin"] = df["operatingIncome"] / df["revenue"]
    
    if "revenue" in df.columns and "netIncome" in df.columns:
        df["net_margin"] = df["netIncome"] / df["revenue"]
    
    return df


def process_fundamentals_file(input_path, output_path=None):
    """Process a single fundamentals file by computing ratios."""
    df = pd.read_parquet(input_path)
    df = compute_ratios(df)
    
    # Set output path if not provided
    if output_path is None:
        filename = os.path.basename(input_path)
        output_path = os.path.join("data/processed/fundamentals", filename)
    
    # Save processed data
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    
    return df


def process_all_fundamentals(input_dir="data/raw/fundamentals", output_dir="data/processed/fundamentals"):
    """Process all fundamentals files in the input directory."""
    files = glob.glob(os.path.join(input_dir, "*.parquet"))
    processed = []
    
    for filepath in files:
        filename = os.path.basename(filepath)
        output_path = os.path.join(output_dir, filename)
        df = process_fundamentals_file(filepath, output_path)
        processed.append(df)
    
    return processed


def combine_fundamentals(input_dir="data/raw/fundamentals", output_path="data/processed/fundamentals.parquet"):
    """Process and combine all fundamentals files into a single DataFrame."""
    processed = process_all_fundamentals(input_dir)
    
    if processed:
        combined = pd.concat(processed, ignore_index=True)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        combined.to_parquet(output_path, index=False)
        return combined
    else:
        # Create empty file with expected structure if no data
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        empty_df = pd.DataFrame(columns=["ticker"])
        empty_df.to_parquet(output_path, index=False)
        print(f"Warning: No fundamentals files found in {input_dir}. Created empty file at {output_path}")
        return empty_df


if __name__ == "__main__":
    files = glob.glob("data/raw/fundamentals/*.parquet")
    if files:
        df = process_fundamentals_file(files[0])
        print(df.head())
    else:
        print("No fundamentals files found.")