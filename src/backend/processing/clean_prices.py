import pandas as pd
import glob
import os


def clean_price_file(path):
    """
    Clean and normalize a single price file.
    
    Args:
        path: Path to parquet file with price data
    
    Returns:
        Cleaned DataFrame with normalized column names
    """
    df = pd.read_parquet(path)
    
    # Handle multi-level columns (from yfinance)
    if isinstance(df.columns, pd.MultiIndex):
        # Flatten multi-level columns
        df.columns = ['_'.join(str(c) for c in col).strip('_') if col[1] else str(col[0]) for col in df.columns.values]
        # Remove empty strings and normalize
        df.columns = [col.lower().strip('_') if col else 'unnamed' for col in df.columns]
    
    # Normalize column names to lowercase
    df.columns = df.columns.str.lower()
    
    # Handle tuple column names (if they weren't MultiIndex)
    if any(isinstance(col, tuple) for col in df.columns):
        df.columns = ['_'.join(str(c) for c in col).strip('_') if isinstance(col, tuple) else str(col) for col in df.columns]
        df.columns = df.columns.str.lower()
    
    # Handle string representations of tuples (e.g., "('close', 'aapl')")
    import ast
    new_columns = []
    for col in df.columns:
        if isinstance(col, str) and col.startswith('(') and col.endswith(')'):
            try:
                # Try to parse as tuple
                parsed = ast.literal_eval(col)
                if isinstance(parsed, tuple):
                    # Use the first element (metric name) - ignore ticker suffix since we'll add ticker column
                    new_col = str(parsed[0])
                    new_columns.append(new_col.lower())
                else:
                    new_columns.append(col.lower())
            except:
                new_columns.append(col.lower())
        else:
            new_columns.append(str(col).lower())
    df.columns = new_columns
    
    # If we have ticker-specific columns (e.g., close_aapl), extract the metric name
    # This happens when columns weren't tuples but were already flattened
    final_columns = []
    for col in df.columns:
        # Check if column has format like "close_aapl" or "close_meta"
        parts = col.split('_')
        if len(parts) >= 2:
            # Check if last part looks like a ticker (short uppercase)
            last_part = parts[-1].upper()
            if len(last_part) <= 5 and last_part.isalpha():
                # It's likely a ticker suffix, use just the metric name
                metric_name = '_'.join(parts[:-1])
                final_columns.append(metric_name)
            else:
                final_columns.append(col)
        else:
            final_columns.append(col)
    df.columns = final_columns
    
    # Ensure date column exists and is properly named
    if "date" not in df.columns:
        # Try common variations
        for col in ["Date", "DATE", "datetime", "Datetime"]:
            if col in df.columns:
                df = df.rename(columns={col: "date"})
                break
        # Also check for tuple columns with 'date'
        date_cols = [col for col in df.columns if 'date' in str(col).lower()]
        if date_cols and "date" not in df.columns:
            df = df.rename(columns={date_cols[0]: "date"})
    
    # Sort by date if date column exists
    if "date" in df.columns:
        df = df.sort_values("date")
        # Ensure date is datetime type
        if not pd.api.types.is_datetime64_any_dtype(df["date"]):
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
    
    df = df.ffill().bfill()  # fill gaps
    return df


def clean_all_prices(input_dir="data/raw/prices", output_dir="data/processed/prices"):
    """
    Clean all price files in the input directory.
    
    Args:
        input_dir: Directory containing raw price parquet files
        output_dir: Directory to save cleaned price files
    
    Returns:
        List of cleaned DataFrames
    """
    files = glob.glob(os.path.join(input_dir, "*.parquet"))
    cleaned = []
    
    os.makedirs(output_dir, exist_ok=True)
    
    for filepath in files:
        df = clean_price_file(filepath)
        ticker = os.path.basename(filepath).replace(".parquet", "").upper()
        df["ticker"] = ticker
        
        # Ensure we have standard column names (close, open, high, low, volume)
        # Remove any ticker-specific suffixes that might remain
        column_mapping = {}
        for col in df.columns:
            if col == "ticker" or col == "date":
                continue
            # Check if column has ticker suffix
            parts = col.split('_')
            if len(parts) >= 2:
                last_part = parts[-1].upper()
                if last_part == ticker or (len(last_part) <= 5 and last_part.isalpha()):
                    # Remove ticker suffix
                    metric_name = '_'.join(parts[:-1])
                    if metric_name not in column_mapping.values():
                        column_mapping[col] = metric_name
        
        if column_mapping:
            df = df.rename(columns=column_mapping)
        
        # Save cleaned file
        output_path = os.path.join(output_dir, f"{ticker}.parquet")
        df.to_parquet(output_path, index=False)
        
        cleaned.append(df)
    
    return cleaned


def combine_price_files(input_dir="data/raw/prices", output_path="data/processed/prices.parquet"):
    """
    Clean and combine all price files into a single DataFrame.
    
    Args:
        input_dir: Directory containing raw price parquet files
        output_path: Path to save combined cleaned prices
    
    Returns:
        Combined DataFrame with all tickers
    """
    cleaned = clean_all_prices(input_dir)
    
    if cleaned:
        combined = pd.concat(cleaned, ignore_index=True)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        combined.to_parquet(output_path, index=False)
        return combined
    else:
        # Create empty file with expected structure if no data
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        empty_df = pd.DataFrame(columns=["ticker", "date", "close", "open", "high", "low", "volume"])
        empty_df.to_parquet(output_path, index=False)
        print(f"Warning: No price files found in {input_dir}. Created empty file at {output_path}")
        return empty_df