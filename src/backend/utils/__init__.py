"""
Utilities package for backend processing.
"""

from .sentiment_model import sentiment, sentiment_detailed
from .filing_section_extractor import extract_sections, extract_mda, extract_risk_factors
from .text_cleaning import (
    clean_text,
    remove_special_chars,
    normalize_whitespace,
    remove_urls,
    remove_emails,
    clean_financial_text
)

#
# NOTE: Avoid importing heavy ML dependencies (torch/transformers) at package import time.
# Import `utils.nlp` lazily via thin wrappers so other utilities remain usable in
# restricted environments.
#
def get_embedding(*args, **kwargs):
    from .nlp import get_embedding as _impl
    return _impl(*args, **kwargs)

def sentiment_score(*args, **kwargs):
    from .nlp import sentiment_score as _impl
    return _impl(*args, **kwargs)

def nlp_sentiment_detailed(*args, **kwargs):
    from .nlp import sentiment_detailed as _impl
    return _impl(*args, **kwargs)

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

