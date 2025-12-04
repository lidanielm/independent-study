"""
Utilities package for backend processing.
"""

from .sentiment_model import sentiment, sentiment_detailed
from .nlp import get_embedding, sentiment_score, sentiment_detailed as nlp_sentiment_detailed
from .filing_section_extractor import extract_sections, extract_mda, extract_risk_factors
from .text_cleaning import (
    clean_text,
    remove_special_chars,
    normalize_whitespace,
    remove_urls,
    remove_emails,
    clean_financial_text
)

__all__ = [
    # Sentiment
    'sentiment',
    'sentiment_detailed',
    'sentiment_score',
    'nlp_sentiment_detailed',
    # NLP
    'get_embedding',
    # Filing extraction
    'extract_sections',
    'extract_mda',
    'extract_risk_factors',
    # Text cleaning
    'clean_text',
    'remove_special_chars',
    'normalize_whitespace',
    'remove_urls',
    'remove_emails',
    'clean_financial_text',
]

