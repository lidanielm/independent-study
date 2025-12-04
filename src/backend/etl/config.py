"""
ETL configuration settings.
"""

import os
from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).parent.parent.parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

class ETLConfig:
    """Configuration class for ETL pipeline."""
    
    # Base directories (as class attributes)
    BASE_DIR = BASE_DIR
    DATA_DIR = DATA_DIR
    RAW_DIR = RAW_DIR
    PROCESSED_DIR = PROCESSED_DIR
    
    # Data directories
    RAW_PRICES_DIR = RAW_DIR / "prices"
    RAW_NEWS_DIR = RAW_DIR / "news"
    RAW_FILINGS_DIR = RAW_DIR / "filings"
    RAW_TRANSCRIPTS_DIR = RAW_DIR / "earnings_calls"
    RAW_FUNDAMENTALS_DIR = RAW_DIR / "fundamentals"
    
    PROCESSED_PRICES_DIR = PROCESSED_DIR / "prices"
    PROCESSED_NEWS_DIR = PROCESSED_DIR / "news"
    PROCESSED_FILINGS_DIR = PROCESSED_DIR / "filings"
    PROCESSED_TRANSCRIPTS_DIR = PROCESSED_DIR / "transcripts"
    PROCESSED_FUNDAMENTALS_DIR = PROCESSED_DIR / "fundamentals"
    
    # Default file paths
    PROCESSED_PRICES_FILE = PROCESSED_DIR / "prices.parquet"
    PROCESSED_NEWS_FILE = PROCESSED_DIR / "news.parquet"
    PROCESSED_FUNDAMENTALS_FILE = PROCESSED_DIR / "fundamentals.parquet"
    FEATURES_FILE = PROCESSED_DIR / "features.parquet"
    
    # Default parameters
    PRICE_PERIOD = "5y"
    PRICE_INTERVAL = "1d"
    MAX_NEWS_ARTICLES = 20
    MAX_TRANSCRIPTS = 5
    
    # API settings
    API_HOST = "0.0.0.0"
    API_PORT = 8000
    
    @classmethod
    def ensure_directories(cls):
        """Create all necessary directories if they don't exist."""
        directories = [
            cls.RAW_PRICES_DIR,
            cls.RAW_NEWS_DIR,
            cls.RAW_FILINGS_DIR,
            cls.RAW_TRANSCRIPTS_DIR,
            cls.RAW_FUNDAMENTALS_DIR,
            cls.PROCESSED_PRICES_DIR,
            cls.PROCESSED_NEWS_DIR,
            cls.PROCESSED_FILINGS_DIR,
            cls.PROCESSED_TRANSCRIPTS_DIR,
            cls.PROCESSED_FUNDAMENTALS_DIR,
            PROCESSED_DIR,  # Module-level constant
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

