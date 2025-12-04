import os
from datetime import datetime
import pandas as pd

from earningscall import get_company

def download_transcripts(ticker, max_transcripts=None, save_dir="data/raw/earnings_calls"):
    """
    Download earnings call transcripts for a given ticker.
    
    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        max_transcripts: Maximum number of transcripts to download (None for all)
        save_dir: Directory to save transcripts
    
    Returns:
        List of dictionaries containing transcript metadata and file paths
    """
    # Create save directory if it doesn't exist
    os.makedirs(save_dir, exist_ok=True)
    
    company = get_company(ticker)
    downloaded = []
    
    print(f"Getting transcripts for: {company.company_info.symbol} ({company.company_info.name})..")
    
    # Retrieve all earnings conference call events for a company
    events = list(company.events())
    count = 0
    
    for event in events:
        # Skip future events
        if datetime.now().timestamp() < event.conference_date.timestamp():
            print(f"* {ticker} Q{event.quarter} {event.year} -- skipping, conference date in the future")
            continue
        
        # Check if we've reached the limit
        if max_transcripts and count >= max_transcripts:
            break
        
        # Fetch the transcript
        transcript = company.get_transcript(event=event)
        
        if transcript:
            # Create filename: TICKER_Q{quarter}_{year}.txt
            filename = f"{ticker}_Q{event.quarter}_{event.year}.txt"
            filepath = os.path.join(save_dir, filename)
            
            # Save transcript text
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(transcript.text)
            
            # Store metadata
            metadata = {
                "ticker": ticker,
                "quarter": event.quarter,
                "year": event.year,
                "conference_date": event.conference_date.isoformat() if hasattr(event.conference_date, 'isoformat') else str(event.conference_date),
                "filepath": filepath,
                "filename": filename,
                "text_length": len(transcript.text)
            }
            downloaded.append(metadata)
            
            print(f"* Q{event.quarter} {event.year} -- saved ({len(transcript.text)} chars)")
            count += 1
        else:
            print(f"* Q{event.quarter} {event.year} -- No transcript found")
    
    return downloaded

def download_transcripts_to_dataframe(ticker, max_transcripts=None, save_dir="data/raw/earnings_calls"):
    """
    Download transcripts and return as a DataFrame.
    """
    downloaded = download_transcripts(ticker, max_transcripts, save_dir)
    if downloaded:
        return pd.DataFrame(downloaded)
    return pd.DataFrame()