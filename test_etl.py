"""
Test script for ETL pipeline.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src" / "backend"))

from etl.orchestrator import run_etl_pipeline

if __name__ == "__main__":
    print("Testing ETL pipeline for AAPL...")
    print("This will take a few minutes...\n")
    
    # Run ETL pipeline
    results = run_etl_pipeline("AAPL")
    
    print("\n" + "="*60)
    print("ETL Pipeline Test Results")
    print("="*60)
    print(f"Overall Success: {results['overall_success']}")
    print("\nExtract Results:")
    if results.get("extract"):
        for key, value in results["extract"].items():
            if key != "ticker" and isinstance(value, dict):
                status = "✓" if value.get("success") else "✗"
                print(f"  {status} {key}: {value.get('success', False)}")
                if value.get("error"):
                    print(f"    Error: {value['error']}")
    
    print("\nTransform Results:")
    if results.get("transform"):
        for key, value in results["transform"].items():
            if key != "ticker" and isinstance(value, dict):
                status = "✓" if value.get("success") else "✗"
                print(f"  {status} {key}: {value.get('success', False)}")
                if value.get("error"):
                    print(f"    Error: {value['error']}")
    
    print("\nLoad Results:")
    if results.get("load"):
        for key, value in results["load"].items():
            if key != "ticker" and isinstance(value, dict):
                status = "✓" if value.get("success") else "✗"
                print(f"  {status} {key}: {value.get('success', False)}")
                if value.get("error"):
                    print(f"    Error: {value['error']}")

