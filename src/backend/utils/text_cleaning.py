"""
Text cleaning utilities for preprocessing financial documents, news, and transcripts.
"""

import re
import html

def clean_text(text, remove_html=True, remove_punctuation=False, lowercase=True, normalize_whitespace=True):
    """
    Clean text by removing HTML, normalizing whitespace, etc.
    
    Args:
        text: Input text string
        remove_html: Whether to remove HTML tags
        remove_punctuation: Whether to remove punctuation (keeps alphanumeric and spaces)
        lowercase: Whether to convert to lowercase
        normalize_whitespace: Whether to normalize whitespace (multiple spaces to single)
    
    Returns:
        str: Cleaned text
    """
    if not text or not isinstance(text, str):
        return ""
    
    cleaned = text
    
    # Remove HTML tags and decode HTML entities
    if remove_html:
        cleaned = re.sub(r'<[^>]+>', '', cleaned)
        cleaned = html.unescape(cleaned)
    
    # Remove punctuation (keep alphanumeric and spaces)
    if remove_punctuation:
        cleaned = re.sub(r'[^A-Za-z0-9\s]', ' ', cleaned)
    
    # Normalize whitespace
    if normalize_whitespace:
        cleaned = re.sub(r'\s+', ' ', cleaned)
        cleaned = cleaned.strip()
    
    # Convert to lowercase
    if lowercase:
        cleaned = cleaned.lower()
    
    return cleaned

def remove_special_chars(text, keep_chars=None):
    """
    Remove special characters from text, optionally keeping specified characters.
    
    Args:
        text: Input text string
        keep_chars: String of characters to keep (e.g., ".,!?-")
    
    Returns:
        str: Text with special characters removed
    """
    if not text or not isinstance(text, str):
        return ""
    
    if keep_chars:
        # Keep alphanumeric, spaces, and specified characters
        pattern = f'[^A-Za-z0-9\\s{re.escape(keep_chars)}]'
    else:
        # Keep only alphanumeric and spaces
        pattern = r'[^A-Za-z0-9\s]'
    
    return re.sub(pattern, '', text)

def normalize_whitespace(text):
    """
    Normalize whitespace in text (multiple spaces/tabs/newlines to single space).
    
    Args:
        text: Input text string
    
    Returns:
        str: Text with normalized whitespace
    """
    if not text or not isinstance(text, str):
        return ""
    
    # Replace all whitespace characters with single space
    normalized = re.sub(r'\s+', ' ', text)
    return normalized.strip()

def remove_urls(text):
    """
    Remove URLs from text.
    
    Args:
        text: Input text string
    
    Returns:
        str: Text with URLs removed
    """
    if not text or not isinstance(text, str):
        return ""
    
    url_pattern = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    )
    return url_pattern.sub('', text)

def remove_emails(text):
    """
    Remove email addresses from text.
    
    Args:
        text: Input text string
    
    Returns:
        str: Text with email addresses removed
    """
    if not text or not isinstance(text, str):
        return ""
    
    email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    return email_pattern.sub('', text)

def clean_financial_text(text):
    """
    Clean text specifically for financial documents (10-K, earnings calls, etc.).
    Removes HTML, normalizes whitespace, but keeps punctuation for financial terms.
    
    Args:
        text: Input text string
    
    Returns:
        str: Cleaned financial text
    """
    if not text or not isinstance(text, str):
        return ""
    
    cleaned = text
    
    # Remove HTML
    cleaned = re.sub(r'<[^>]+>', '', cleaned)
    cleaned = html.unescape(cleaned)
    
    # Remove URLs and emails
    cleaned = remove_urls(cleaned)
    cleaned = remove_emails(cleaned)
    
    # Normalize whitespace
    cleaned = normalize_whitespace(cleaned)
    
    return cleaned.strip()

