#!/usr/bin/env python3
"""
Clear BigQuery cache to force fresh data fetch
"""
import os
import sys

# Add the backend directory to the Python path
backend_dir = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.abspath(backend_dir))

from app.services.bigquery_integration import bigquery_integration_service

def clear_cache():
    """Clear BigQuery cache"""
    print("🧹 Clearing BigQuery cache...")
    
    # Clear both memory and disk cache
    bigquery_integration_service.clear_cache(clear_disk_cache=True)
    
    print("✅ Cache cleared! Next BigQuery test will fetch fresh data.")

if __name__ == "__main__":
    clear_cache()



