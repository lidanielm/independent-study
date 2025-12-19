import json
from pathlib import Path

import numpy as np
import pandas as pd

from processing.docetl_pipelines import (
    extract_sec_filing_insights,
    extract_transcript_insights,
    extract_news_insights,
)
from retrieval import index_builder


def test_docetl_pipeline_schemas(monkeypatch):
    """DocETL pipeline wrappers should return structured dicts with metadata."""

    def fake_run(prompt, schema, config):
        return {
            "mdna_summary": "summary",
            "risk_factors": [{"risk": "risk 1"}],
            "accounting_changes": [],
            "guidance": [{"metric": "rev", "value": "up", "period": "FY"}],
            "key_metrics": [],
            "uncertainties": ["supply chain"],
        }

    monkeypatch.setattr("processing.docetl_pipelines._run_docetl", fake_run)

    filing = extract_sec_filing_insights(
        "Some filing text",
        ticker="ABC",
        filing_type="10-K",
        filing_date="2024-01-01",
    )
    assert filing["ticker"] == "ABC"
    assert filing["filing_type"] == "10-K"
    assert filing["mdna_summary"]
    assert filing["risk_factors"][0]["risk"]

    def fake_transcript(prompt, schema, config):
        return {
            "speakers": [{"name": "CEO"}],
            "qa_pairs": [{"question": "Q1?", "answer": "A1"}],
            "guidance": [{"metric": "EPS", "value": "1.00", "period": "FY"}],
        }

    monkeypatch.setattr("processing.docetl_pipelines._run_docetl", fake_transcript)
    transcript = extract_transcript_insights(
        "Q&A transcript text",
        ticker="ABC",
        quarter=1,
        year=2024,
    )
    assert transcript["qa_pairs"]
    assert transcript["guidance"]

    def fake_news(prompt, schema, config):
        return {
            "events": [{"event_type": "M&A", "rationale": "Acquisition"}],
            "tickers_mentioned": ["ABC"],
            "sentiment_with_rationale": "positive: growth",
        }

    monkeypatch.setattr("processing.docetl_pipelines._run_docetl", fake_news)
    news = extract_news_insights(
        "ABC acquires XYZ",
        description="",
        summary="",
        ticker="ABC",
    )
    assert news["events"][0]["event_type"] == "M&A"
    assert "ABC" in news["tickers_mentioned"]


def test_combined_index_includes_docetl_insights(monkeypatch, tmp_path):
    """Combined index should include DocETL-derived insight types."""

    # Mock embeddings to avoid heavy model load
    monkeypatch.setattr("utils.nlp.get_embedding", lambda text: np.ones(4, dtype=np.float32))

    # Build fixture data
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    news_insights_file = processed_dir / "news_insights.parquet"
    news_insights_df = pd.DataFrame([{
        "ticker": "ABC",
        "events": json.dumps([{"event_type": "M&A", "rationale": "growth"}]),
        "sentiment_with_rationale": "positive",
        "link": "http://example.com",
        "published": "2024-01-01",
    }])
    news_insights_df.to_parquet(news_insights_file, index=False)

    filings_insights_dir = processed_dir / "filings_insights"
    filings_insights_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{
        "ticker": "ABC",
        "filing_type": "10-K",
        "filing_date": "2024-01-01",
        "mdna_summary": "Strong year",
        "risk_factors": [{"risk": "competition"}],
    }]).to_parquet(filings_insights_dir / "ABC_10-K.parquet", index=False)

    transcripts_qa_dir = processed_dir / "transcripts_qa"
    transcripts_qa_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{
        "question": "Q?",
        "answer": "A!",
        "asked_by": "Analyst",
        "answered_by": "CEO",
    }]).to_parquet(transcripts_qa_dir / "ABC_Q1_2024.parquet", index=False)

    transcripts_guidance_dir = processed_dir / "transcripts_guidance"
    transcripts_guidance_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{
        "metric": "Revenue",
        "value": "Up",
        "period": "FY",
    }]).to_parquet(transcripts_guidance_dir / "ABC_Q1_2024.parquet", index=False)

    class FakeConfig:
        PROCESSED_DIR = processed_dir
        PROCESSED_NEWS_FILE = processed_dir / "news.parquet"
        PROCESSED_NEWS_INSIGHTS_FILE = news_insights_file
        PROCESSED_FILINGS_DIR = processed_dir / "filings"
        PROCESSED_FILINGS_INSIGHTS_DIR = filings_insights_dir
        PROCESSED_TRANSCRIPTS_DIR = processed_dir / "transcripts"
        PROCESSED_TRANSCRIPTS_QA_DIR = transcripts_qa_dir
        PROCESSED_TRANSCRIPTS_GUIDANCE_DIR = transcripts_guidance_dir

    store = index_builder.build_combined_index(config=FakeConfig(), ticker="ABC")
    stats = store.get_stats()

    # Expect insight doc types to be present
    assert "news_insight" in stats["doc_types"] or stats["doc_type_counts"].get("news_insight")
    assert "filing_insight" in stats["doc_types"] or stats["doc_type_counts"].get("filing_insight")
    assert "transcript_qa" in stats["doc_types"] or stats["doc_type_counts"].get("transcript_qa")
    assert "transcript_guidance" in stats["doc_types"] or stats["doc_type_counts"].get("transcript_guidance")


