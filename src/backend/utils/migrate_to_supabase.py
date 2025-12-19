"""
Script to migrate existing local data to Supabase Storage.
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from project root
project_root = Path(__file__).parent.parent.parent.parent
env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    # Also try loading from current directory
    load_dotenv()

# Add backend directory to path so we can import backend modules
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from etl.config import ETLConfig
from utils.supabase_storage import SupabaseStorage

def migrate_data():
    """Migrate all processed data and indices to Supabase Storage."""
    config = ETLConfig()
    
    # Check if Supabase is configured
    if not config.USE_SUPABASE_STORAGE:
        print("Supabase storage is not enabled. Set USE_SUPABASE_STORAGE=true in .env")
        return
    
    try:
        storage = SupabaseStorage()
        storage.ensure_bucket()
        print(f"Using bucket: {storage.bucket_name}")
    except Exception as e:
        print(f"Error initializing Supabase storage: {e}")
        return
    
    # Migrate processed files
    processed_dirs = [
        ("prices", config.PROCESSED_PRICES_DIR),
        ("news", config.PROCESSED_NEWS_DIR),
        ("filings", config.PROCESSED_FILINGS_DIR),
        ("transcripts", config.PROCESSED_TRANSCRIPTS_DIR),
        ("fundamentals", config.PROCESSED_FUNDAMENTALS_DIR),
        ("filings_insights", config.PROCESSED_FILINGS_INSIGHTS_DIR),
        ("transcripts_qa", config.PROCESSED_TRANSCRIPTS_QA_DIR),
        ("transcripts_guidance", config.PROCESSED_TRANSCRIPTS_GUIDANCE_DIR),
    ]
    
    total_files = 0
    for category, local_dir in processed_dirs:
        if local_dir.exists():
            parquet_files = list(local_dir.glob("*.parquet"))
            for file_path in parquet_files:
                remote_path = f"processed/{category}/{file_path.name}"
                print(f"Uploading {file_path} to {remote_path}")
                if storage.upload_file(file_path, remote_path):
                    total_files += 1
                else:
                    print(f"  Failed to upload {file_path}")
    
    # Migrate combined processed files
    combined_files = [
        (config.PROCESSED_PRICES_FILE, "processed/prices.parquet"),
        (config.PROCESSED_NEWS_FILE, "processed/news.parquet"),
        (config.PROCESSED_NEWS_INSIGHTS_FILE, "processed/news_insights.parquet"),
        (config.PROCESSED_FUNDAMENTALS_FILE, "processed/fundamentals.parquet"),
        (config.FEATURES_FILE, "processed/features.parquet"),
    ]
    
    for local_path, remote_path in combined_files:
        if local_path.exists():
            print(f"Uploading {local_path} to {remote_path}")
            if storage.upload_file(local_path, remote_path):
                total_files += 1
            else:
                print(f"  Failed to upload {local_path}")
    
    # Migrate indices
    indices_dir = config.PROCESSED_DIR / "indices"
    if indices_dir.exists():
        for file_path in indices_dir.glob("*"):
            if file_path.is_file():
                remote_path = f"indices/{file_path.name}"
                print(f"Uploading {file_path} to {remote_path}")
                if storage.upload_file(file_path, remote_path):
                    total_files += 1
                else:
                    print(f"  Failed to upload {file_path}")
    
    print(f"\nMigration complete! Uploaded {total_files} files to Supabase Storage.")

if __name__ == "__main__":
    migrate_data()

