"""
Test script for vector retrieval functionality.
Run this after ETL pipeline has processed some data.
"""

import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent / "src" / "backend"))

from retrieval.retrieval_service import get_retrieval_service
from etl.config import ETLConfig

def test_vector_retrieval():
    """Test vector retrieval with sample queries."""
    print("Testing Vector Retrieval System")
    print("=" * 60)
    
    # Initialize service
    service = get_retrieval_service()
    
    # Test queries
    test_queries = [
        "artificial intelligence",
        "revenue growth",
        "risk factors",
        "earnings guidance"
    ]
    
    for query in test_queries:
        print(f"\n🔍 Query: '{query}'")
        print("-" * 60)
        
        try:
            # Search all document types
            results = service.search(query, k=3)
            
            if results:
                print(f"Found {len(results)} results:")
                for i, result in enumerate(results[:3], 1):
                    doc_type = result.get('doc_type', 'unknown')
                    title = result.get('title', result.get('text', '')[:50])
                    score = result.get('similarity_score', 0.0)
                    print(f"  {i}. [{doc_type}] {title} (score: {score:.3f})")
            else:
                print("  No results found. Make sure indices are built.")
                print("  Run ETL pipeline first, or call /api/search/rebuild-indices")
        except Exception as e:
            print(f"  Error: {e}")
    
    print("\n" + "=" * 60)
    print("✅ Vector retrieval test completed!")
    print("\nTo rebuild indices, run:")
    print("  POST /api/search/rebuild-indices")
    print("Or run ETL pipeline which will build indices automatically.")

if __name__ == "__main__":
    test_vector_retrieval()

