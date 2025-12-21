"""
ETL workflow orchestrator package.
"""

from .config import ETLConfig

def run_etl_pipeline(*args, **kwargs):
    from .orchestrator import run_etl_pipeline as _impl
    return _impl(*args, **kwargs)

def extract_data(*args, **kwargs):
    from .orchestrator import extract_data as _impl
    return _impl(*args, **kwargs)

def transform_data(*args, **kwargs):
    from .orchestrator import transform_data as _impl
    return _impl(*args, **kwargs)

def load_features(*args, **kwargs):
    from .orchestrator import load_features as _impl
    return _impl(*args, **kwargs)

__all__ = [
    'run_etl_pipeline',
    'extract_data',
    'transform_data',
    'load_features',
    'ETLConfig',
]

