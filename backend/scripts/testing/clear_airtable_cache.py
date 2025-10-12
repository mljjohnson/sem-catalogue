#!/usr/bin/env python3
"""
Clear Airtable cache to force fresh data fetch
"""
import os

def clear_airtable_cache():
    """Clear Airtable cache"""
    cache_file = "airtable_cache.json"
    
    if os.path.exists(cache_file):
        os.remove(cache_file)
        print(f"✅ Cleared Airtable cache ({cache_file})")
    else:
        print(f"ℹ️  No Airtable cache found ({cache_file})")

if __name__ == "__main__":
    clear_airtable_cache()


