import types

from etl import auto_orchestrator
from agents.query_intent import parse_intent


def test_parse_intent_defaults_to_all_sources():
    intent = parse_intent("What happened to ABC?")
    assert intent.ticker == "ABC"
    assert intent.needs_news and intent.needs_filings and intent.needs_transcripts


def test_run_autonomous_uses_doc_types(monkeypatch):
    called = {}

    monkeypatch.setattr(auto_orchestrator, "ensure_news", lambda t, cfg: {"source": "news"})
    monkeypatch.setattr(auto_orchestrator, "ensure_transcripts", lambda t, cfg: {"source": "transcripts"})
    monkeypatch.setattr(auto_orchestrator, "ensure_filings", lambda t, cfg: {"source": "filings"})

    def fake_build(cfg, ticker=None, doc_types=None):
        called["doc_types"] = doc_types
        dummy = types.SimpleNamespace(index=None, metadata=None, doc_type_map=None)
        return dummy

    monkeypatch.setattr(auto_orchestrator, "build_combined_index", fake_build)

    result = auto_orchestrator.run_autonomous("latest transcript and 10-K for ABC")
    assert result["ticker"] == "ABC"
    assert called["doc_types"] is not None
    assert "news" in called["doc_types"]
    assert "filing" in called["doc_types"]
    assert "transcript" in called["doc_types"]
