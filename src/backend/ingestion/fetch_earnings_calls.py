import os
from datetime import datetime
import pandas as pd
import requests
from typing import List, Dict, Any, Optional

def download_transcripts(ticker: str, max_transcripts: Optional[int] = None, save_dir: str = "data/raw/earnings_calls", api_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Download earnings call transcripts for a given ticker using API Ninjas API.
    
    Args:
        ticker: Stock ticker symbol
        max_transcripts: Maximum number of transcripts to download
        save_dir: Directory to save transcript files
        api_key: API Ninjas API key (if None, will try to get from environment)
    
    Returns:
        List of metadata dictionaries for downloaded transcripts
    """
    # Create save directory if it doesn't exist
    os.makedirs(save_dir, exist_ok=True)
    
    # Get API key from parameter or environment
    if api_key is None:
        api_key = os.getenv("API_NINJAS_API_KEY")
    
    if not api_key:
        raise ValueError("API_NINJAS_API_KEY is required. Set it as an environment variable or pass it as a parameter.")
    
    downloaded = []
    
    # API Ninjas API base URL
    base_url = "https://api.api-ninjas.com/v1/earningstranscript"
    
    # API Ninjas doesn't have a list endpoint, so we'll try to fetch recent transcripts
    # by iterating through recent quarters. We'll start with the latest (no params) and
    # then try recent quarters going back.
    
    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1
    
    # Headers for API Ninjas
    headers = {
        "X-Api-Key": api_key
    }
    
    # Set to track which transcripts we've already downloaded (by year-quarter)
    downloaded_quarters = set()
    
    # First, try to get the latest transcript (no year/quarter specified)
    try:
        response = requests.get(
            base_url,
            params={"ticker": ticker.upper()},
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        transcript_data = response.json()
        
        # API Ninjas returns a single transcript object
        # Handle different possible response formats
        transcript_text = None
        year = None
        quarter = None
        date_str = None
        
        if isinstance(transcript_data, dict) and transcript_data.get('ticker'):
            year = transcript_data.get('year')
            quarter = transcript_data.get('quarter')
            date_str = transcript_data.get('date')
            
            # Try multiple possible field names for transcript content
            transcript_text = (
                transcript_data.get('transcript') or 
                transcript_data.get('content') or 
                transcript_data.get('text') or
                transcript_data.get('body')
            )
        
        if year and quarter and transcript_text:
            quarter_key = (year, quarter)
            if quarter_key not in downloaded_quarters:
                filename = f"{ticker}_Q{quarter}_{year}.txt"
                filepath = os.path.join(save_dir, filename)
                
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(transcript_text)
                
                metadata = {
                    "ticker": ticker,
                    "quarter": quarter,
                    "year": year,
                    "conference_date": date_str,
                    "filepath": filepath,
                    "filename": filename,
                    "text_length": len(transcript_text)
                }
                downloaded.append(metadata)
                downloaded_quarters.add(quarter_key)
                print(f"* Q{quarter} {year} -- saved ({len(transcript_text)} chars)")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching latest transcript for {ticker}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response body: {e.response.text[:500]}")
    
    # If we need more transcripts, try recent quarters going back
    if max_transcripts is None or len(downloaded) < max_transcripts:
        # Try quarters going back up to 8 quarters (2 years)
        quarters_to_try = []
        for year_offset in range(2):  # Last 2 years
            for q in range(4, 0, -1):  # Q4 to Q1
                year = current_year - year_offset
                # Adjust for current quarter
                if year == current_year and q > current_quarter:
                    continue
                quarters_to_try.append((year, q))
        
        for year, quarter in quarters_to_try:
            if max_transcripts and len(downloaded) >= max_transcripts:
                break
            
            if (year, quarter) in downloaded_quarters:
                continue
            
            try:
                response = requests.get(
                    base_url,
                    params={
                        "ticker": ticker.upper(),
                        "year": year,
                        "quarter": quarter
                    },
                    headers=headers,
                    timeout=30
                )
                response.raise_for_status()
                transcript_data = response.json()
                
                # Handle error responses and different response formats
                transcript_text = None
                date_str = None
                
                if isinstance(transcript_data, dict):
                    if "error" in transcript_data or "Error" in transcript_data:
                        # Skip if error (might be premium-only feature or no transcript available)
                        continue
                    
                    if transcript_data.get('ticker'):
                        date_str = transcript_data.get('date')
                        # Try multiple possible field names for transcript content
                        transcript_text = (
                            transcript_data.get('transcript') or 
                            transcript_data.get('content') or 
                            transcript_data.get('text') or
                            transcript_data.get('body')
                        )
                
                if transcript_text and year and quarter:
                    filename = f"{ticker}_Q{quarter}_{year}.txt"
                    filepath = os.path.join(save_dir, filename)
                    
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(transcript_text)
                    
                    metadata = {
                        "ticker": ticker,
                        "quarter": quarter,
                        "year": year,
                        "conference_date": date_str,
                        "filepath": filepath,
                        "filename": filename,
                        "text_length": len(transcript_text)
                    }
                    downloaded.append(metadata)
                    downloaded_quarters.add((year, quarter))
                    print(f"* Q{quarter} {year} -- saved ({len(transcript_text)} chars)")
                elif year and quarter:
                    print(f"* Q{quarter} {year} -- No transcript content found")
            except requests.exceptions.RequestException as e:
                # Silently skip if request fails (might be premium-only or not available)
                continue
    
    if not downloaded:
        print(f"No transcripts available for {ticker}")
    
    return downloaded

def download_transcripts_to_dataframe(ticker: str, max_transcripts: Optional[int] = None, save_dir: str = "data/raw/earnings_calls", api_key: Optional[str] = None) -> pd.DataFrame:
    """
    Download transcripts and return as a DataFrame.
    
    Args:
        ticker: Stock ticker symbol
        max_transcripts: Maximum number of transcripts to download
        save_dir: Directory to save transcript files
        api_key: API Ninjas API key (if None, will try to get from environment)
    
    Returns:
        DataFrame with transcript metadata
    """
    downloaded = download_transcripts(ticker, max_transcripts, save_dir, api_key)
    if downloaded:
        return pd.DataFrame(downloaded)
    return pd.DataFrame()
