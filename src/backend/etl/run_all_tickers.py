"""
Batch ETL runner for all tickers in src/backend/data/stock_tickers.json.

Goal: fetch + process + index filings, transcripts, and news for the entire universe.

Notes:
- News is stored in a combined parquet (`data/processed/news.parquet`), so we fetch per-ticker
  but only need to combine once at the end.
- Filings/transcripts are processed per-ticker via the ensure_* helpers.
- We rebuild indices once at the end to avoid expensive repeated embedding/index builds.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import List, Dict, Any

# Ensure `src/backend` is on sys.path when running as a script
import sys
_BACKEND_DIR = Path(__file__).resolve().parents[1]  # .../src/backend
sys.path.insert(0, str(_BACKEND_DIR))

from etl.config import ETLConfig
from etl.auto_orchestrator import ensure_news, ensure_transcripts, ensure_filings, _load_ticker_universe
from retrieval.index_builder import build_combined_index


def _write_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, default=str) + "\n")


def run_all(
    tickers: List[str],
    sleep_s: float = 0.25,
    progress_path: Path | None = None,
) -> Dict[str, Any]:
    cfg = ETLConfig()
    cfg.ensure_directories()

    progress_path = progress_path or (cfg.PROCESSED_DIR / "etl_runs" / "run_all_tickers.jsonl")

    results = {
        "tickers_total": len(tickers),
        "tickers_processed": 0,
        "errors": 0,
        "progress_path": str(progress_path),
    }

    for i, t in enumerate(tickers, start=1):
        ticker = t.upper().strip()
        if not ticker:
            continue

        print(f"[RUN_ALL] ({i}/{len(tickers)}) {ticker}")
        step = {"ticker": ticker, "news": None, "transcripts": None, "filings": None}

        # Fetch+process per ticker
        try:
            step["news"] = ensure_news(ticker, cfg)
        except Exception as exc:
            step["news"] = {"source": "news", "ticker": ticker, "error": str(exc)}
        try:
            step["transcripts"] = ensure_transcripts(ticker, cfg)
        except Exception as exc:
            step["transcripts"] = {"source": "transcripts", "ticker": ticker, "error": str(exc)}
        try:
            step["filings"] = ensure_filings(ticker, cfg)
        except Exception as exc:
            step["filings"] = {"source": "filings", "ticker": ticker, "error": str(exc)}

        # Record progress
        _write_jsonl(progress_path, step)
        results["tickers_processed"] += 1
        if any((isinstance(step[k], dict) and step[k].get("error")) for k in ["news", "transcripts", "filings"]):
            results["errors"] += 1

        # Gentle throttling for upstream APIs
        if sleep_s:
            time.sleep(sleep_s)

    print("[RUN_ALL] Rebuilding indices (news + filings + transcripts)...")
    build_combined_index(
        cfg,
        ticker=None,
        doc_types={"news", "news_insight", "filing", "filing_insight", "transcript", "transcript_qa", "transcript_guidance"},
    )
    print("[RUN_ALL] Done.")

    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sleep", type=float, default=0.25, help="sleep seconds between tickers (throttle)")
    ap.add_argument("--max-tickers", type=int, default=0, help="limit tickers processed (0 = all)")
    ap.add_argument("--progress-path", type=str, default="", help="jsonl progress output path")
    args = ap.parse_args()

    universe = sorted(_load_ticker_universe())
    tickers = universe[: args.max_tickers] if args.max_tickers and args.max_tickers > 0 else universe
    progress_path = Path(args.progress_path) if args.progress_path else None

    out = run_all(tickers=tickers, sleep_s=args.sleep, progress_path=progress_path)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()


