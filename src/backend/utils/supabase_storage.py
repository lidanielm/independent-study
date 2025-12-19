"""
Supabase Storage utility for storing and retrieving data files.
"""
import os
from pathlib import Path
from typing import Optional, BinaryIO
from io import BytesIO
import pandas as pd
from supabase import create_client, Client

class SupabaseStorage:
    """Handle file storage operations with Supabase Storage."""
    
    def __init__(self):
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # Use service role key for backend
        
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
        
        self.client: Client = create_client(supabase_url, supabase_key)
        self.bucket_name = os.getenv("SUPABASE_STORAGE_BUCKET", "financial-data")
    
    def ensure_bucket(self):
        """Ensure the storage bucket exists."""
        try:
            buckets = self.client.storage.list_buckets()
            bucket_names = [b.name for b in buckets]
            
            if self.bucket_name not in bucket_names:
                self.client.storage.create_bucket(
                    self.bucket_name,
                    options={"public": False}  # Private bucket
                )
        except Exception as e:
            print(f"Warning: Could not ensure bucket exists: {e}")
    
    def upload_file(self, local_path: Path, remote_path: str) -> bool:
        """Upload a file to Supabase Storage."""
        try:
            with open(local_path, 'rb') as f:
                data = f.read()
            
            response = self.client.storage.from_(self.bucket_name).upload(
                path=remote_path,
                file=data,
                file_options={"content-type": self._get_content_type(local_path)}
            )
            return True
        except Exception as e:
            print(f"Error uploading {local_path} to {remote_path}: {e}")
            return False
    
    def download_file(self, remote_path: str, local_path: Path) -> bool:
        """Download a file from Supabase Storage."""
        try:
            data = self.client.storage.from_(self.bucket_name).download(remote_path)
            
            local_path.parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, 'wb') as f:
                f.write(data)
            return True
        except Exception as e:
            print(f"Error downloading {remote_path} to {local_path}: {e}")
            return False
    
    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists in Supabase Storage."""
        try:
            # Get the directory and filename
            path_parts = remote_path.split('/')
            if len(path_parts) > 1:
                directory = '/'.join(path_parts[:-1])
                filename = path_parts[-1]
            else:
                directory = ""
                filename = remote_path
            
            files = self.client.storage.from_(self.bucket_name).list(directory)
            return any(f.name == filename for f in files)
        except:
            return False
    
    def upload_parquet(self, df: pd.DataFrame, remote_path: str) -> bool:
        """Upload a pandas DataFrame as parquet to Supabase Storage."""
        try:
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            
            response = self.client.storage.from_(self.bucket_name).upload(
                path=remote_path,
                file=buffer.read(),
                file_options={"content-type": "application/parquet"}
            )
            return True
        except Exception as e:
            print(f"Error uploading parquet to {remote_path}: {e}")
            return False
    
    def download_parquet(self, remote_path: str) -> Optional[pd.DataFrame]:
        """Download a parquet file from Supabase Storage as DataFrame."""
        try:
            data = self.client.storage.from_(self.bucket_name).download(remote_path)
            buffer = BytesIO(data)
            return pd.read_parquet(buffer)
        except Exception as e:
            print(f"Error downloading parquet from {remote_path}: {e}")
            return None
    
    def upload_bytes(self, data: bytes, remote_path: str, content_type: str = "application/octet-stream") -> bool:
        """Upload raw bytes to Supabase Storage."""
        try:
            self.client.storage.from_(self.bucket_name).upload(
                path=remote_path,
                file=data,
                file_options={"content-type": content_type}
            )
            return True
        except Exception as e:
            print(f"Error uploading bytes to {remote_path}: {e}")
            return False
    
    def download_bytes(self, remote_path: str) -> Optional[bytes]:
        """Download raw bytes from Supabase Storage."""
        try:
            return self.client.storage.from_(self.bucket_name).download(remote_path)
        except Exception as e:
            print(f"Error downloading bytes from {remote_path}: {e}")
            return None
    
    def _get_content_type(self, path: Path) -> str:
        """Get content type based on file extension."""
        ext = path.suffix.lower()
        content_types = {
            '.parquet': 'application/parquet',
            '.pkl': 'application/octet-stream',
            '.index': 'application/octet-stream',
            '.txt': 'text/plain',
            '.json': 'application/json',
        }
        return content_types.get(ext, 'application/octet-stream')
    
    def list_files(self, prefix: str = "") -> list:
        """List files in the bucket with optional prefix."""
        try:
            files = self.client.storage.from_(self.bucket_name).list(prefix)
            return [f.name for f in files]
        except Exception as e:
            print(f"Error listing files with prefix {prefix}: {e}")
            return []

