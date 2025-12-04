"""
Download actual SEC filing text files (not just metadata).
"""

import requests
import os
from pathlib import Path
from secedgar.cik_lookup import CIKLookup
from typing import List, Optional


def download_filing_text(filing_url: str, save_path: Path):
    """Download a filing text file from SEC EDGAR."""
    if save_dir is None:
        save_dir = Path("data/raw/filings")
    save_dir.mkdir(parents=True, exist_ok=True)
    
    from fetch_filings import fetch_filings
    
    # Get filing metadata
    filings_data = fetch_filings(ticker)
    
    if "filings" not in filings_data or "recent" not in filings_data["filings"]:
        print(f"No filings found for {ticker}")
        return []
    
    recent = filings_data["filings"]["recent"]
    downloaded = []
    
    # Get CIK for URL construction
    cik = str(CIKLookup(lookups=[ticker], user_agent="Daniel Li dli2004@seas.upenn.edu").lookup_dict[ticker]).zfill(10)
    
    counts = {ft: 0 for ft in filing_types}
    
    for i in range(len(recent.get("form", []))):
        form_type = recent["form"][i]
        
        if form_type not in filing_types:
            continue
        
        if counts[form_type] >= max_filings:
            continue
        
        # Get filing date
        filing_date = recent["filingDate"][i]
        accession = recent["accessionNumber"][i].replace("-", "")
        
        # Construct URL
        if "primaryDocument" in recent and i < len(recent["primaryDocument"]):
            primary_doc = recent["primaryDocument"][i]
        else:
            # Default document name
            primary_doc = f"{form_type.lower()}.txt"
        
        filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{primary_doc}"
        
        # Create filename
        filename = f"{ticker}_{form_type}_{filing_date}.txt"
        filepath = save_dir / filename
        
        try:
            download_filing_text(filing_url, filepath)
            downloaded.append(filepath)
            counts[form_type] += 1
            print(f"Downloaded {form_type} filed on {filing_date}: {filename}")
        except Exception as e:
            print(f"Failed to download {form_type} from {filing_date}: {e}")
    
    return downloaded
