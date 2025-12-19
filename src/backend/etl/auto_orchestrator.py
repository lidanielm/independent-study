"""
Autonomous fetch/process orchestration based on a user query.
"""

from __future__ import annotations

import glob
import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Set, List

from agents.query_intent import parse_intent, IntentResult
TICKER_STOPLIST = {
    "US", "USA",
    "Q1", "Q2", "Q3", "Q4",
    "H1", "H2", "FY", "CY", "LY", "PY",
    "EPS", "EBIT", "EBITDA", "EARNINGS",
    "GAAP", "NON-GAAP", "GUIDE", "GUIDANCE",
    "SALES", "REVENUE", "REV", "MARGIN", "MARGINS",
    "GM", "OM", "OPM", "GPM",
    "YOY", "QOQ", "LTM", "TTM",
    "USD", "EUR", "GBP",
    "SPX", "NDX", "DJI", "RUT",
    "ETF", "ETFS", "ADR", "ADRs",
    "STAPLES", "TECH", "SECTOR",
    "INDEX", "INDICES"
}


def _load_ticker_universe() -> Set[str]:
    try:
        data_path = Path(__file__).parent.parent / "data" / "stock_tickers.json"
        with open(data_path, "r") as f:
            tickers = json.load(f)
        if isinstance(tickers, list):
            return {t.upper() for t in tickers if isinstance(t, str) and t.strip()}
    except Exception as exc:
        print(f"[AUTO_ORCHESTRATOR] Warning: failed to load ticker universe: {exc}")
    return set()


def _fallback_sector_tickers(query: str, universe: Set[str]) -> List[str]:
    q = (query or "").lower()
    seeds: List[str] = []
    if "consumer staple" in q or "staples" in q:
        seeds = ["PG", "KO", "PEP", "CL", "KHC", "COST", "WMT"]
    elif "consumer discretionary" in q or "discretionary" in q:
        seeds = ["HD", "LOW", "NKE", "SBUX", "AMZN", "TSLA"]
    elif "tech" in q or "technology" in q:
        seeds = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META"]
    elif "energy" in q:
        seeds = ["XOM", "CVX", "COP", "SLB"]
    elif "financial" in q or "banks" in q:
        seeds = ["JPM", "BAC", "C", "WFC", "GS", "MS"]
    elif "health" in q or "pharma" in q or "biotech" in q:
        seeds = ["JNJ", "PFE", "MRK", "ABT", "LLY", "ABBV"]
    elif "industrial" in q or "industrials" in q:
        seeds = ["CAT", "DE", "GE", "HON", "UPS"]
    elif "semiconductor" in q or "chip" in q:
        seeds = ["NVDA", "AMD", "INTC", "AVGO", "QCOM"]
    if not seeds:
        return []
    if universe:
        return [s for s in seeds if s in universe]
    return seeds
from etl.config import ETLConfig
from ingestion.fetch_news import fetch_news_and_save
from ingestion.fetch_earnings_calls import download_transcripts_to_dataframe
from ingestion.download_filings import download_recent_filing_documents
from ingestion.fetch_filings import fetch_filings, filings_to_dataframe
from processing.process_news import combine_news_files
from processing.process_transcripts import process_transcript_from_text
from processing.process_filings import process_all_filings
from retrieval.index_builder import build_combined_index


def _is_stale(path: Path, hours: int) -> bool:
    if not path.exists():
        return True
    age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
    return age > timedelta(hours=hours)


def _latest_mtime_in_dir(directory: Path) -> Optional[datetime]:
    if not directory.exists():
        return None
    mtimes = []
    for root, _, files in os.walk(directory):
        for f in files:
            full = Path(root) / f
            mtimes.append(datetime.fromtimestamp(full.stat().st_mtime))
    return max(mtimes) if mtimes else None


def ensure_news(ticker: str, cfg: ETLConfig) -> Dict[str, Any]:
    status = {"source": "news", "ticker": ticker, "fetched": False, "processed": False, "error": None}
    try:
        target = cfg.PROCESSED_NEWS_FILE
        stale = _is_stale(target, cfg.AUTO_STALENESS_HOURS)
        if stale:
            fetch_news_and_save(ticker, max_articles=cfg.AUTO_MAX_NEWS, save_dir=str(cfg.RAW_NEWS_DIR))
            status["fetched"] = True
        combine_news_files(input_dir=str(cfg.RAW_NEWS_DIR), output_path=str(cfg.PROCESSED_NEWS_FILE), config=cfg)
        status["processed"] = True
    except Exception as exc:
        status["error"] = str(exc)
    # region agent log
    try:
        with open("/Users/danielli/Documents/penn/fa25/is/.cursor/debug.log", "a") as _f:
            _f.write(json.dumps({
                "sessionId": "debug-session",
                "runId": "run4",
                "hypothesisId": "H5",
                "location": "auto_orchestrator.py:ensure_news",
                "message": "ensure_news status",
                "data": status,
                "timestamp": int(datetime.now().timestamp() * 1000),
            }) + "\n")
    except Exception:
        pass
    # endregion
    return status


def ensure_transcripts(ticker: str, cfg: ETLConfig) -> Dict[str, Any]:
    status = {"source": "transcripts", "ticker": ticker, "fetched": False, "processed": False, "error": None}
    try:
        print(f"[ENSURE_TRANSCRIPTS] Checking transcripts for {ticker}...")
        processed_dir = cfg.PROCESSED_TRANSCRIPTS_DIR
        # Check staleness for THIS ticker specifically
        latest = _latest_mtime_for_ticker(processed_dir, ticker)
        stale = True
        if latest:
            stale = datetime.now() - latest > timedelta(hours=cfg.AUTO_STALENESS_HOURS)
            print(f"[ENSURE_TRANSCRIPTS] Latest processed transcript for {ticker}: {latest}, stale: {stale}")
        else:
            print(f"[ENSURE_TRANSCRIPTS] No processed transcripts found for {ticker}, will fetch")
        
        # Check if raw files exist for this ticker
        transcript_files = glob.glob(str(cfg.RAW_TRANSCRIPTS_DIR / f"{ticker}_*.txt"))
        if not transcript_files and stale:
            print(f"[ENSURE_TRANSCRIPTS] No raw files for {ticker}, fetching...")
            stale = True  # Force fetch if no files exist
        
        if stale:
            print(f"[ENSURE_TRANSCRIPTS] Fetching transcripts for {ticker}...")
            download_transcripts_to_dataframe(
                ticker,
                max_transcripts=cfg.AUTO_MAX_TRANSCRIPTS,
                save_dir=str(cfg.RAW_TRANSCRIPTS_DIR),
                api_key=cfg.API_NINJAS_API_KEY,
            )
            status["fetched"] = True
            print(f"[ENSURE_TRANSCRIPTS] Download complete for {ticker}")
            # Refresh file list after download
            transcript_files = glob.glob(str(cfg.RAW_TRANSCRIPTS_DIR / f"{ticker}_*.txt"))
        else:
            print(f"[ENSURE_TRANSCRIPTS] Transcripts for {ticker} are fresh, skipping fetch")
        
        print(f"[ENSURE_TRANSCRIPTS] Found {len(transcript_files)} transcript files for {ticker} to process")
        for transcript_file in transcript_files:
            with open(transcript_file, "r", encoding="utf-8") as f:
                text = f.read()
            filename = os.path.basename(transcript_file).replace(".txt", ".parquet")
            output_path = cfg.PROCESSED_TRANSCRIPTS_DIR / filename
            process_transcript_from_text(text, str(output_path), config=cfg)
        status["processed"] = True
        print(f"[ENSURE_TRANSCRIPTS] Processing complete for {ticker}: {len(transcript_files)} files")
    except Exception as exc:
        status["error"] = str(exc)
        print(f"[ENSURE_TRANSCRIPTS] Error for {ticker}: {exc}")
        import traceback
        traceback.print_exc()
    # region agent log
    try:
        with open("/Users/danielli/Documents/penn/fa25/is/.cursor/debug.log", "a") as _f:
            _f.write(json.dumps({
                "sessionId": "debug-session",
                "runId": "run4",
                "hypothesisId": "H5",
                "location": "auto_orchestrator.py:ensure_transcripts",
                "message": "ensure_transcripts status",
                "data": status,
                "timestamp": int(datetime.now().timestamp() * 1000),
            }) + "\n")
    except Exception:
        pass
    # endregion
    return status


def _latest_mtime_for_ticker(directory: Path, ticker: str) -> Optional[datetime]:
    """Get latest modification time for files matching a specific ticker."""
    if not directory.exists():
        return None
    mtimes = []
    pattern = f"{ticker}_*.parquet"
    import glob
    for filepath in glob.glob(str(directory / pattern)):
        full = Path(filepath)
        if full.exists():
            mtimes.append(datetime.fromtimestamp(full.stat().st_mtime))
    return max(mtimes) if mtimes else None


def ensure_filings(ticker: str, cfg: ETLConfig) -> Dict[str, Any]:
    status = {"source": "filings", "ticker": ticker, "fetched": False, "processed": False, "error": None}
    try:
        print(f"[ENSURE_FILINGS] Checking filings for {ticker}...")
        # Check staleness for THIS ticker specifically
        latest = _latest_mtime_for_ticker(cfg.PROCESSED_FILINGS_DIR, ticker)
        stale = True
        if latest:
            stale = datetime.now() - latest > timedelta(hours=cfg.AUTO_STALENESS_HOURS)
            print(f"[ENSURE_FILINGS] Latest processed filing for {ticker}: {latest}, stale: {stale}")
        else:
            print(f"[ENSURE_FILINGS] No processed filings found for {ticker}, will fetch")
        
        # Check if raw files exist for this ticker
        import glob
        raw_files = glob.glob(str(cfg.RAW_FILINGS_DOCS_DIR / f"{ticker}_*.txt"))
        if not raw_files and stale:
            print(f"[ENSURE_FILINGS] No raw files for {ticker}, fetching...")
            stale = True  # Force fetch if no files exist
        
        if stale:
            print(f"[ENSURE_FILINGS] Fetching filings for {ticker}...")
            # refresh metadata parquet
            filings_data = fetch_filings(ticker)
            df = filings_to_dataframe(filings_data)
            if not df.empty:
                save_path = cfg.RAW_FILINGS_DIR / f"{ticker}_filings.parquet"
                df.to_parquet(save_path, index=False)
                print(f"[ENSURE_FILINGS] Saved metadata: {len(df)} filings")
            # download raw docs
            downloaded = download_recent_filing_documents(
                ticker,
                filing_types=cfg.FILING_TYPES,
                max_filings=cfg.AUTO_MAX_FILINGS,
                save_dir=cfg.RAW_FILINGS_DOCS_DIR,
            )
            print(f"[ENSURE_FILINGS] Downloaded {len(downloaded)} filing documents for {ticker}")
            status["fetched"] = True
        else:
            print(f"[ENSURE_FILINGS] Filings for {ticker} are fresh, skipping fetch")
        
        # Process only files for this ticker
        print(f"[ENSURE_FILINGS] Processing filings for {ticker} from {cfg.RAW_FILINGS_DOCS_DIR}...")
        raw_files = glob.glob(str(cfg.RAW_FILINGS_DOCS_DIR / f"{ticker}_*.txt"))
        print(f"[ENSURE_FILINGS] Found {len(raw_files)} raw filing files for {ticker}")
        
        if raw_files:
            for filepath in raw_files:
                filename = os.path.basename(filepath).replace(".txt", ".parquet")
                output_path = cfg.PROCESSED_FILINGS_DIR / filename
                from processing.process_filings import process_filing_file
                process_filing_file(filepath, str(output_path), config=cfg)
            status["processed"] = True
            print(f"[ENSURE_FILINGS] Processing complete for {ticker}: {len(raw_files)} files")
        else:
            print(f"[ENSURE_FILINGS] Warning: No raw filing files found for {ticker} to process")
            status["error"] = f"No filing files found for {ticker}"
    except Exception as exc:
        status["error"] = str(exc)
        print(f"[ENSURE_FILINGS] Error for {ticker}: {exc}")
        import traceback
        traceback.print_exc()
    # region agent log
    try:
        with open("/Users/danielli/Documents/penn/fa25/is/.cursor/debug.log", "a") as _f:
            _f.write(json.dumps({
                "sessionId": "debug-session",
                "runId": "run4",
                "hypothesisId": "H5",
                "location": "auto_orchestrator.py:ensure_filings",
                "message": "ensure_filings status",
                "data": status,
                "timestamp": int(datetime.now().timestamp() * 1000),
            }) + "\n")
    except Exception:
        pass
    # endregion
    return status


def run_autonomous(query: str, ticker_hint: Optional[str] = None, doc_types: Optional[Set[str]] = None) -> Dict[str, Any]:
    """
    Infer ticker/sources, fetch missing data with caps, process, and rebuild indices.
    """
    cfg = ETLConfig()
    intent: IntentResult = parse_intent(query, ticker_hint)
    universe = _load_ticker_universe()
    
    print(f"[AUTO_ORCHESTRATOR] Query: '{query}', ticker_hint: {ticker_hint}")
    print(f"[AUTO_ORCHESTRATOR] Inferred ticker: {intent.ticker}, needs: news={intent.needs_news}, filings={intent.needs_filings}, transcripts={intent.needs_transcripts}")
    # region agent log
    try:
        with open("/Users/danielli/Documents/penn/fa25/is/.cursor/debug.log", "a") as _f:
            _f.write(json.dumps({
                "sessionId": "debug-session",
                "runId": "run1",
                "hypothesisId": "H1",
                "location": "auto_orchestrator.py:195",
                "message": "intent parsed",
                "data": {
                    "query": query,
                    "ticker_hint": ticker_hint,
                    "intent": intent.__dict__,
                },
                "timestamp": int(datetime.now().timestamp() * 1000),
            }) + "\n")
    except Exception:
        pass
    # endregion

    # Apply stoplist and universe filtering to candidates
    raw_candidates: List[str] = []
    if ticker_hint:
        raw_candidates.append(ticker_hint.upper())
    raw_candidates.extend(intent.raw_ticker_candidates)
    if intent.ticker:
        raw_candidates.append(intent.ticker)
    dedup: List[str] = []
    for t in raw_candidates:
        tu = t.upper()
        if tu not in dedup:
            dedup.append(tu)
    filtered: List[str] = []
    for t in dedup:
        if t in TICKER_STOPLIST:
            continue
        if universe and t not in universe:
            continue
        filtered.append(t)
    chosen_ticker = filtered[0] if filtered else None

    # If none, try sector-based fallback seeds
    if not chosen_ticker:
        fallback = _fallback_sector_tickers(query, universe)
        if fallback:
            chosen_ticker = fallback[0]
            filtered = fallback

    # If still none, use corpus-based suggestions (generic, not query-specific heuristics)
    suggested_payload = None
    if not chosen_ticker:
        try:
            # Local import to avoid heavy deps at module import time
            from agents.tools.search_tools import suggest_tickers as _suggest_tickers
            suggested_payload = _suggest_tickers(query=query, doc_type=None, k=5, candidate_k=120, min_score=0.0)
            suggested = [t.get("ticker") for t in (suggested_payload.get("tickers") or []) if t.get("ticker")]
            # Apply universe filtering if configured
            suggested = [t for t in suggested if (not universe or t in universe)]
            if suggested:
                filtered = suggested
                chosen_ticker = suggested[0]
        except Exception as exc:
            print(f"[AUTO_ORCHESTRATOR] Warning: suggest_tickers fallback failed: {exc}")

    # region agent log
    try:
        with open("/Users/danielli/Documents/penn/fa25/is/.cursor/debug.log", "a") as _f:
            _f.write(json.dumps({
                "sessionId": "debug-session",
                "runId": "run1",
                "hypothesisId": "H1",
                "location": "auto_orchestrator.py:filter",
                "message": "ticker filtered",
                "data": {
                    "raw_candidates": dedup,
                    "filtered": filtered,
                    "chosen": chosen_ticker,
                    "stoplist_hit": [t for t in dedup if t in TICKER_STOPLIST],
                    "universe_size": len(universe),
                    "fallback_used": not bool(filtered and dedup),
                },
                "timestamp": int(datetime.now().timestamp() * 1000),
            }) + "\n")
    except Exception:
        pass
    # endregion
    
    if not chosen_ticker:
        print(f"[AUTO_ORCHESTRATOR] Error: Unable to infer ticker from query after filtering (and no corpus suggestions)")
        return {
            "error": "Unable to infer ticker from query",
            "intent": intent.__dict__,
            "suggestions": suggested_payload,
        }

    if doc_types:
        needed = set(doc_types)
    else:
        needed = set()
        if intent.needs_news:
            needed.add("news")
        if intent.needs_filings:
            needed.add("filings")
        if intent.needs_transcripts:
            needed.add("transcripts")
        # default include news to have market context even if not explicitly requested
        if not intent.needs_news:
            needed.add("news")

    # Process up to a small number of tickers if inferred/suggested
    tickers_to_process = filtered[:3] if filtered else [chosen_ticker]
    results = {
        "primary_ticker": chosen_ticker,
        "tickers": tickers_to_process,
        "actions": [],
        "intent": intent.__dict__,
        "suggestions": suggested_payload,
        "index_rebuilt": False,
    }
    index_doc_types: Set[str] = set()

    if cfg.AUTO_ENABLED:
        print(f"[AUTO_ORCHESTRATOR] AUTO_ENABLED=True, fetching sources: {needed}")
        for t in tickers_to_process:
            if "news" in needed:
                print(f"[AUTO_ORCHESTRATOR] Ensuring news for {t}...")
                results["actions"].append(ensure_news(t, cfg))
                index_doc_types.update({"news", "news_insight"})
            if "transcripts" in needed:
                print(f"[AUTO_ORCHESTRATOR] Ensuring transcripts for {t}...")
                results["actions"].append(ensure_transcripts(t, cfg))
                index_doc_types.update({"transcript", "transcript_qa", "transcript_guidance"})
            if "filings" in needed:
                print(f"[AUTO_ORCHESTRATOR] Ensuring filings for {t}...")
                results["actions"].append(ensure_filings(t, cfg))
                index_doc_types.update({"filing", "filing_insight"})
    else:
        print(f"[AUTO_ORCHESTRATOR] AUTO_ENABLED=False, skipping fetch")

    # Rebuild indices (include all documents, not just this ticker)
    # Search will filter by ticker as needed
    print(f"[AUTO_ORCHESTRATOR] Rebuilding indices with doc_types: {index_doc_types or 'all'}")
    try:
        build_combined_index(cfg, ticker=None, doc_types=index_doc_types or None)
        results["index_rebuilt"] = True
        print(f"[AUTO_ORCHESTRATOR] Index rebuild successful")
    except Exception as exc:
        results["index_rebuilt"] = False
        results["index_error"] = str(exc)
        print(f"[AUTO_ORCHESTRATOR] Index rebuild failed: {exc}")
        import traceback
        traceback.print_exc()

    return results


__all__ = ["run_autonomous"]
