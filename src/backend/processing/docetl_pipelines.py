"""
DocETL wrappers for filings, transcripts, and news.

These helpers centralize schema definitions and LLM-backed extraction so the
processing modules can stay thin. DocETL (PyPI) is preferred when installed; a
fallback OpenAI JSON-mode call is used when DocETL is unavailable.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from etl.config import ETLConfig
from utils.filing_section_extractor import extract_sections

try:
    import docetl  # type: ignore
except Exception:  # pragma: no cover - optional dependency surface
    docetl = None

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency surface
    OpenAI = None


class DocETLError(RuntimeError):
    """Raised when structured extraction fails."""


def _get_llm_client(config: ETLConfig):
    """Return an LLM client (DocETL preferred, otherwise OpenAI)."""
    if docetl is not None:
        return docetl
    if OpenAI is None:
        raise DocETLError("Neither DocETL nor OpenAI client is available.")
    if not config.DOCETL_OPENAI_API_KEY:
        raise DocETLError("OPENAI_API_KEY is not configured for DocETL extraction.")
    return OpenAI(api_key=config.DOCETL_OPENAI_API_KEY)


def _run_docetl(prompt: str, schema: Dict[str, Any], config: ETLConfig) -> Dict[str, Any]:
    """
    Execute a structured extraction with DocETL if present, else OpenAI JSON mode.
    The schema is a JSON schema dict describing the expected payload.
    """
    client = _get_llm_client(config)

    # DocETL: try a conventional interface if available
    if docetl is not None:
        if hasattr(docetl, "extract"):
            return docetl.extract(
                prompt=prompt,
                schema=schema,
                model=config.DOCETL_MODEL,
                temperature=config.DOCETL_TEMPERATURE,
                max_tokens=config.DOCETL_MAX_TOKENS,
            )
        if hasattr(docetl, "DocETL"):
            runner = docetl.DocETL(
                model=config.DOCETL_MODEL,
                temperature=config.DOCETL_TEMPERATURE,
                max_tokens=config.DOCETL_MAX_TOKENS,
            )
            if hasattr(runner, "extract"):
                return runner.extract(prompt=prompt, schema=schema)
        raise DocETLError("DocETL is installed but no usable extraction interface was found.")

    # OpenAI fallback using response_format for deterministic JSON output
    if OpenAI is None:
        raise DocETLError("OpenAI client missing; cannot run structured extraction.")

    response = client.chat.completions.create(
        model=config.DOCETL_MODEL,
        temperature=config.DOCETL_TEMPERATURE,
        max_tokens=config.DOCETL_MAX_TOKENS,
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": "You are a meticulous information extractor. Return ONLY valid JSON.",
            },
            {
                "role": "user",
                "content": json.dumps({"prompt": prompt, "schema": schema}),
            },
        ],
    )
    content = response.choices[0].message.content or "{}"
    try:
        return json.loads(content)
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise DocETLError(f"Failed to parse LLM JSON: {exc}") from exc


# ---------------------------------------------------------------------------
# Filings
# ---------------------------------------------------------------------------

_FILING_SCHEMA = {
    "type": "object",
    "properties": {
        "mdna_summary": {"type": "string"},
        "risk_factors": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "risk": {"type": "string"},
                    "category": {"type": "string"},
                    "severity": {"type": "string"},
                },
                "required": ["risk"],
            },
        },
        "accounting_changes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "description": {"type": "string"},
                    "impact": {"type": "string"},
                },
                "required": ["description"],
            },
        },
        "guidance": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "metric": {"type": "string"},
                    "value": {"type": "string"},
                    "period": {"type": "string"},
                },
                "required": ["metric", "value"],
            },
        },
        "key_metrics": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "value": {"type": "string"},
                    "trend": {"type": "string"},
                },
                "required": ["name", "value"],
            },
        },
        "uncertainties": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["mdna_summary"],
}


def extract_sec_filing_insights(
    text: str,
    *,
    ticker: str,
    filing_type: str,
    filing_date: str,
    config: Optional[ETLConfig] = None,
) -> Dict[str, Any]:
    """
    Extract structured insights from a filing document.
    Returns a dict matching _FILING_SCHEMA plus metadata.
    """
    cfg = config or ETLConfig()
    sections = extract_sections(text)
    context_blocks = []
    for name, body in sections.items():
        if body:
            context_blocks.append(f"[{name}]\n{body[:4000]}")  # cap for prompt
    context = "\n\n".join(context_blocks) or text[:4000]

    prompt = (
        f"Ticker: {ticker}\n"
        f"Filing type: {filing_type}\n"
        f"Filing date: {filing_date}\n"
        "Extract structured insights focused on MD&A, risk factors, guidance, "
        "and accounting changes. Use the provided JSON schema."
        "\n\n"
        f"Document excerpts:\n{context}"
    )

    result = _run_docetl(prompt, _FILING_SCHEMA, cfg)
    result.update({"ticker": ticker, "filing_type": filing_type, "filing_date": filing_date})
    return result


# ---------------------------------------------------------------------------
# Transcripts
# ---------------------------------------------------------------------------

_TRANSCRIPT_SCHEMA = {
    "type": "object",
    "properties": {
        "speakers": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "role": {"type": "string"},
                },
                "required": ["name"],
            },
        },
        "qa_pairs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "question": {"type": "string"},
                    "asked_by": {"type": "string"},
                    "answer": {"type": "string"},
                    "answered_by": {"type": "string"},
                },
                "required": ["question", "answer"],
            },
        },
        "guidance": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "metric": {"type": "string"},
                    "value": {"type": "string"},
                    "period": {"type": "string"},
                    "confidence": {"type": "string"},
                },
                "required": ["metric", "value"],
            },
        },
    },
    "required": [],
}


def extract_transcript_insights(
    text: str,
    *,
    ticker: str,
    quarter: Optional[int],
    year: Optional[int],
    config: Optional[ETLConfig] = None,
) -> Dict[str, Any]:
    """Extract Q&A and guidance insights from a transcript."""
    cfg = config or ETLConfig()
    prefix = f"Ticker: {ticker}\nQuarter: {quarter}\nYear: {year}\n"
    prompt = (
        prefix
        + "Identify speaker roles, Q&A pairs, and guidance or forward-looking metrics. "
        "Return concise answers; omit fluff. Use the JSON schema."
        "\n\nTranscript excerpt:\n"
        + text[:6000]
    )
    result = _run_docetl(prompt, _TRANSCRIPT_SCHEMA, cfg)
    result.update({"ticker": ticker, "quarter": quarter, "year": year})
    return result


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------

_NEWS_SCHEMA = {
    "type": "object",
    "properties": {
        "events": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "event_type": {"type": "string"},
                    "entities": {"type": "array", "items": {"type": "string"}},
                    "relevance": {"type": "string"},
                    "rationale": {"type": "string"},
                },
                "required": ["event_type"],
            },
        },
        "tickers_mentioned": {"type": "array", "items": {"type": "string"}},
        "sentiment_with_rationale": {"type": "string"},
    },
    "required": [],
}


def extract_news_insights(
    title: str,
    description: str = "",
    summary: str = "",
    *,
    ticker: str,
    link: str = "",
    published: Optional[str] = None,
    config: Optional[ETLConfig] = None,
) -> Dict[str, Any]:
    """Extract events/entities and sentiment rationale from a news record."""
    cfg = config or ETLConfig()
    text = "\n".join(filter(None, [title, description, summary]))
    prompt = (
        f"Ticker: {ticker}\nLink: {link}\nPublished: {published}\n"
        "Determine key events and entities, tickers mentioned, and sentiment with rationale. "
        "Use the JSON schema."
        "\n\nArticle text:\n"
        + text[:4000]
    )
    result = _run_docetl(prompt, _NEWS_SCHEMA, cfg)
    result.update({"ticker": ticker, "link": link, "published": published, "title": title})
    return result


__all__ = [
    "DocETLError",
    "extract_sec_filing_insights",
    "extract_transcript_insights",
    "extract_news_insights",
]


