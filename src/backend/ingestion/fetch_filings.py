import requests
import pandas as pd
import os
from dotenv import load_dotenv
from secedgar.cik_lookup import CIKLookup

load_dotenv()

API_KEY = os.getenv("AV_API_KEY")

def fetch_fundamentals(ticker):
    url = f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={ticker}&apikey={API_KEY}"
    r = requests.get(url)
    data = r.json()
    return data

def fetch_filings(ticker):
    """
    Fetch filing history from SEC EDGAR API for a given ticker.
    
    Returns the raw JSON response containing metadata and filing history.
    The 'filings' property contains 'recent' (most recent filings) and 
    'files' (references to additional JSON files if more than 1000 filings).
    """
    cik = CIKLookup(lookups=[ticker], user_agent="Daniel Li dli2004@seas.upenn.edu").lookup_dict[ticker]
    # pad cik to 10 digits with leading zeros
    cik = str(cik).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    r = requests.get(url, headers={"User-Agent": "Daniel Li dli2004@seas.upenn.edu"})
    r.raise_for_status()  # Raise an exception for bad status codes
    data = r.json()
    return data

def filings_to_dataframe(filings_data):
    """
    Convert the 'recent' filings from the SEC JSON response to a pandas DataFrame.
    
    The filings are stored in a columnar format where each key is a column name
    and the value is an array of values for that column.
    """
    if "filings" not in filings_data or "recent" not in filings_data["filings"]:
        return pd.DataFrame()
    
    recent = filings_data["filings"]["recent"]
    # Convert columnar format to DataFrame
    df = pd.DataFrame(recent)
    return df 

def download_filing(filing_url, save_path):
    headers = {"User-Agent": "Daniel Li dli2004@seas.upenn.edu"}
    r = requests.get(filing_url, headers=headers)
    with open(save_path, "wb") as f:
        f.write(r.content)