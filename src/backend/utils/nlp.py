"""
NLP utilities for text embeddings and sentiment analysis.
"""

import numpy as np
from sentence_transformers import SentenceTransformer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_embedding_model = None
_sentiment_analyzer = SentimentIntensityAnalyzer()

def _get_embedding_model():
    """Lazy load the embedding model."""
    global _embedding_model
    if _embedding_model is None:
        _embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
    return _embedding_model

def get_embedding(text, model_name='all-MiniLM-L6-v2'):
    """
    Generate a text embedding vector for the given text.
    """
    if not text or not isinstance(text, str):
        # Return zero vector if text is empty
        model = _get_embedding_model()
        return np.zeros(model.get_sentence_embedding_dimension())
    
    model = _get_embedding_model()
    embedding = model.encode(text, convert_to_numpy=True)
    return embedding

def sentiment_score(text):
    """
    Compute sentiment score for a given text using VADER.
    """
    if not text or not isinstance(text, str):
        return 0.0
    
    scores = _sentiment_analyzer.polarity_scores(text)
    return scores['compound']

def sentiment_detailed(text):
    """
    Get detailed sentiment scores for a given text.
    """
    if not text or not isinstance(text, str):
        return {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0}
    
    return _sentiment_analyzer.polarity_scores(text)

