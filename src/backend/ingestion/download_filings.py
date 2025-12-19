"""
Download actual SEC filing text files (not just metadata).
"""

import requests
from pathlib import Path
from typing import List, Optional

from secedgar.cik_lookup import CIKLookup

from etl.config import ETLConfig
from ingestion.fetch_filings import fetch_filings


USER_AGENT = "DocETL/1.0 (contact: dli2004@seas.upenn.edu)"


def _build_filing_url(cik: str, accession: str, primary_document: str) -> str:
    accession_clean = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/{primary_document}"


def _download_text(url: str, save_path: Path) -> None:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    save_path.parent.mkdir(parents=True, exist_ok=True)
    # Some filings are HTML; keep raw bytes but save as text for downstream parsing
    save_path.write_bytes(resp.content)


def download_recent_filing_documents(
    ticker: str,
    filing_types: Optional[List[str]] = None,
    max_filings: int = 4,
    save_dir: Optional[Path] = None,
) -> List[Path]:
    """
    Download recent filing documents (10-K/10-Q by default) as raw text/html files.

    Returns a list of saved file paths.
    """
    cfg = ETLConfig()
    save_dir = save_dir or cfg.RAW_FILINGS_DOCS_DIR
    filing_types = filing_types or cfg.FILING_TYPES

    data = fetch_filings(ticker)
    if "filings" not in data or "recent" not in data["filings"]:
        print(f"[FILINGS] No filings metadata for {ticker}")
        return []

    recent = data["filings"]["recent"]
    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    primary_docs = recent.get("primaryDocument", [])
    accessions = recent.get("accessionNumber", [])

    cik = str(CIKLookup(lookups=[ticker], user_agent=USER_AGENT).lookup_dict[ticker]).zfill(10)
    counts = {ft: 0 for ft in filing_types}
    downloaded: List[Path] = []

    for idx, form_type in enumerate(forms):
        if form_type not in filing_types:
            continue
        if counts[form_type] >= max_filings:
            continue

        filing_date = filing_dates[idx] if idx < len(filing_dates) else ""
        accession = accessions[idx] if idx < len(accessions) else ""
        primary_doc = primary_docs[idx] if idx < len(primary_docs) else f"{form_type.lower()}.txt"

        if not accession:
            continue

        url = _build_filing_url(cik, accession, primary_doc)
        filename = f"{ticker}_{form_type}_{filing_date or idx}.txt"
        filepath = save_dir / filename

        try:
            _download_text(url, filepath)
            counts[form_type] += 1
            downloaded.append(filepath)
            print(f"[FILINGS] Downloaded {form_type} ({filing_date}) -> {filepath.name}")
        except Exception as exc:
            print(f"[FILINGS] Failed {form_type} ({filing_date}): {exc}")

    return downloaded
