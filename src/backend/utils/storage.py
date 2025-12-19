"""
Storage abstraction layer that supports both local and Supabase storage.
"""
from pathlib import Path
from typing import Optional
import pandas as pd
from etl.config import ETLConfig

class StorageAdapter:
    """Adapter for storage operations that can use local or Supabase."""
    
    def __init__(self, config: Optional[ETLConfig] = None):
        self.config = config or ETLConfig()
        self.use_supabase = self.config.USE_SUPABASE_STORAGE
        
        if self.use_supabase:
            try:
                from utils.supabase_storage import SupabaseStorage
                self.storage = SupabaseStorage()
                self.storage.ensure_bucket()
            except Exception as e:
                print(f"Warning: Could not initialize Supabase storage: {e}")
                print("Falling back to local storage only")
                self.use_supabase = False
                self.storage = None
    
    def save_parquet(self, df: pd.DataFrame, path: Path, remote_path: Optional[str] = None) -> bool:
        """Save DataFrame as parquet, optionally to Supabase."""
        # Always save locally first (for caching/backup)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        
        # Also save to Supabase if enabled
        if self.use_supabase and remote_path and self.storage:
            return self.storage.upload_parquet(df, remote_path)
        return True
    
    def load_parquet(self, path: Path, remote_path: Optional[str] = None) -> Optional[pd.DataFrame]:
        """Load parquet file, from Supabase if enabled."""
        if self.use_supabase and remote_path and self.storage:
            # Try Supabase first
            df = self.storage.download_parquet(remote_path)
            if df is not None:
                # Cache locally
                path.parent.mkdir(parents=True, exist_ok=True)
                df.to_parquet(path, index=False)
                return df
        
        # Fall back to local
        if path.exists():
            return pd.read_parquet(path)
        return None
    
    def save_file(self, local_path: Path, remote_path: Optional[str] = None) -> bool:
        """Save file, optionally to Supabase."""
        if self.use_supabase and remote_path and self.storage:
            return self.storage.upload_file(local_path, remote_path)
        return True
    
    def load_file(self, local_path: Path, remote_path: Optional[str] = None) -> bool:
        """Load file from Supabase if enabled, otherwise from local."""
        if self.use_supabase and remote_path and self.storage:
            # Try Supabase first
            if self.storage.download_file(remote_path, local_path):
                return True
        
        # Fall back to local - just check if exists
        return local_path.exists()
    
    def file_exists(self, path: Path, remote_path: Optional[str] = None) -> bool:
        """Check if file exists."""
        if self.use_supabase and remote_path and self.storage:
            return self.storage.file_exists(remote_path)
        return path.exists()
    
    def save_bytes(self, data: bytes, local_path: Path, remote_path: Optional[str] = None, content_type: str = "application/octet-stream") -> bool:
        """Save bytes to file, optionally to Supabase."""
        # Save locally first
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, 'wb') as f:
            f.write(data)
        
        # Also save to Supabase if enabled
        if self.use_supabase and remote_path and self.storage:
            return self.storage.upload_bytes(data, remote_path, content_type)
        return True
    
    def load_bytes(self, local_path: Path, remote_path: Optional[str] = None) -> Optional[bytes]:
        """Load bytes from Supabase if enabled, otherwise from local."""
        if self.use_supabase and remote_path and self.storage:
            # Try Supabase first
            data = self.storage.download_bytes(remote_path)
            if data is not None:
                # Cache locally
                local_path.parent.mkdir(parents=True, exist_ok=True)
                with open(local_path, 'wb') as f:
                    f.write(data)
                return data
        
        # Fall back to local
        if local_path.exists():
            with open(local_path, 'rb') as f:
                return f.read()
        return None

