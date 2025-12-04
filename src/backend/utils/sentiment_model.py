"""
Sentiment analysis utilities using VADER (Valence Aware Dictionary and sEntiment Reasoner).
VADER is optimized for social media text but works well for financial news and transcripts.
"""

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Initialize analyzer once (it's thread-safe)
_analyzer = SentimentIntensityAnalyzer()

def sentiment(text):
    """
    Compute sentiment score for a given text.
    """
    if not text or not isinstance(text, str):
        return 0.0
    
    scores = _analyzer.polarity_scores(text)
    return scores['compound']

def sentiment_detailed(text):
    """
    Get detailed sentiment scores for a given text.
    """
    if not text or not isinstance(text, str):
        return {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0}
    
    return _analyzer.polarity_scores(text)

