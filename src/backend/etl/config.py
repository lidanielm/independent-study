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
    RAW_FILINGS_DOCS_DIR = RAW_DIR / "filings_docs"
    RAW_TRANSCRIPTS_DIR = RAW_DIR / "earnings_calls"
    RAW_FUNDAMENTALS_DIR = RAW_DIR / "fundamentals"
    
    PROCESSED_PRICES_DIR = PROCESSED_DIR / "prices"
    PROCESSED_NEWS_DIR = PROCESSED_DIR / "news"
    PROCESSED_FILINGS_DIR = PROCESSED_DIR / "filings"
    PROCESSED_FILINGS_INSIGHTS_DIR = PROCESSED_DIR / "filings_insights"
    PROCESSED_TRANSCRIPTS_DIR = PROCESSED_DIR / "transcripts"
    PROCESSED_TRANSCRIPTS_QA_DIR = PROCESSED_DIR / "transcripts_qa"
    PROCESSED_TRANSCRIPTS_GUIDANCE_DIR = PROCESSED_DIR / "transcripts_guidance"
    PROCESSED_FUNDAMENTALS_DIR = PROCESSED_DIR / "fundamentals"
    
    # Default file paths
    PROCESSED_PRICES_FILE = PROCESSED_DIR / "prices.parquet"
    PROCESSED_NEWS_FILE = PROCESSED_DIR / "news.parquet"
    PROCESSED_NEWS_INSIGHTS_FILE = PROCESSED_DIR / "news_insights.parquet"
    PROCESSED_FUNDAMENTALS_FILE = PROCESSED_DIR / "fundamentals.parquet"
    FEATURES_FILE = PROCESSED_DIR / "features.parquet"
    
    # Default parameters
    PRICE_PERIOD = "5y"
    PRICE_INTERVAL = "1d"
    MAX_NEWS_ARTICLES = 20
    MAX_TRANSCRIPTS = 5
    MAX_FILINGS = 4
    FILING_TYPES = ["10-K", "10-Q"]
    # Autonomous caps / staleness
    AUTO_ENABLED = True
    AUTO_MAX_NEWS = 20
    AUTO_MAX_TRANSCRIPTS = 10
    AUTO_MAX_FILINGS = 10
    AUTO_STALENESS_HOURS = 24
    
    # API settings
    API_HOST = "0.0.0.0"
    API_PORT = 8000

    # DocETL settings
    DOCETL_ENABLED = False
    DOCETL_MODEL = os.getenv("DOCETL_MODEL", "gpt-4o-mini")
    DOCETL_TEMPERATURE = float(os.getenv("DOCETL_TEMPERATURE", 0.1))
    DOCETL_MAX_TOKENS = int(os.getenv("DOCETL_MAX_TOKENS", 1200))
    DOCETL_OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    
    # API Ninjas API settings (for earnings transcripts)
    API_NINJAS_API_KEY = os.getenv("API_NINJAS_API_KEY")
    
    # FMP API settings (deprecated, kept for backward compatibility)
    FMP_API_KEY = os.getenv("FMP_API_KEY")
    
    # Storage settings
    USE_SUPABASE_STORAGE = os.getenv("USE_SUPABASE_STORAGE", "false").lower() == "true"
    SUPABASE_STORAGE_BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET", "financial-data")
    
    @classmethod
    def ensure_directories(cls):
        """Create all necessary directories if they don't exist."""
        directories = [
            cls.RAW_PRICES_DIR,
            cls.RAW_NEWS_DIR,
            cls.RAW_FILINGS_DIR,
            cls.RAW_FILINGS_DOCS_DIR,
            cls.RAW_TRANSCRIPTS_DIR,
            cls.RAW_FUNDAMENTALS_DIR,
            cls.PROCESSED_PRICES_DIR,
            cls.PROCESSED_NEWS_DIR,
            cls.PROCESSED_FILINGS_DIR,
            cls.PROCESSED_FILINGS_INSIGHTS_DIR,
            cls.PROCESSED_TRANSCRIPTS_DIR,
            cls.PROCESSED_TRANSCRIPTS_QA_DIR,
            cls.PROCESSED_TRANSCRIPTS_GUIDANCE_DIR,
            cls.PROCESSED_FUNDAMENTALS_DIR,
            PROCESSED_DIR,  # Module-level constant
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

