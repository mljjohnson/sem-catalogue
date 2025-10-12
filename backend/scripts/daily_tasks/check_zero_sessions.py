#!/usr/bin/env python3
"""
Check for URLs with zero sessions and update their status in Airtable
"""
import sys
import os
from datetime import datetime

# Add the backend directory to the Python path
backend_dir = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.abspath(backend_dir))

from app.services.airtable import airtable_service
from app.models.db import get_session
import sqlalchemy as sa
from app.services.task_logger import log_task_execution

def normalize_url(url: str) -> str:
    """Normalize URL for comparison."""
    if not url:
        return ""
    url = url.lower().strip()
    url = url.replace('https://', 'http://')
    url = url.rstrip('/')
    if url.startswith('http://forbes.com'):
        url = url.replace('http://forbes.com', 'http://www.forbes.com')
    return url


def check_and_update_zero_sessions(use_cache=False, auto_update=False):
    """
    Check for URLs with 0 sessions and update their status in Airtable.
    
    Args:
        use_cache: If True, use cached BigQuery data instead of fresh query
        auto_update: If True, skip confirmation prompt (for automated runs)
    """
    print("🔍 Checking for URLs with zero sessions...")
    
    # Get BigQuery session data
    if use_cache:
        print("📖 Loading BigQuery data from cache...")
        import json
        cache_file = os.path.join(backend_dir, "bigquery_cache.json")
        with open(cache_file, 'r') as f:
            bq_data = json.load(f)
        print(f"✅ Loaded {len(bq_data)} records from cache")
    else:
        print("📡 Fetching BigQuery session data...")
        from app.services.bigquery_integration import BigQueryIntegrationService
        bigquery_service = BigQueryIntegrationService()
        df = bigquery_service.get_page_sessions_data(force_refresh=True, use_disk_cache=False)
        bq_data = df.to_dict('records')
        print(f"✅ Fetched {len(bq_data)} records from BigQuery")
    
    # Build lookup: normalized_url -> sessions
    bq_sessions = {}
    for record in bq_data:
        url = record.get("visit_page") or record.get("page_path")
        if url:
            # Construct full URL if it's just a path
            if url.startswith('/'):
                property_name = record.get("property", "")
                if "expertise" in property_name.lower():
                    url = f"https://www.expertise.com{url}"
                else:
                    url = f"https://www.forbes.com{url}"
            
            normalized = normalize_url(url)
            sessions = record.get("sessions", 0)
            bq_sessions[normalized] = sessions
    
    print(f"✅ Built session lookup for {len(bq_sessions)} URLs")
    
    # Get database URLs with Airtable IDs
    print("🗄️  Fetching URLs from database...")
    with get_session() as session:
        result = session.execute(sa.text(
            """SELECT DISTINCT url, airtable_id, page_status 
               FROM pages_sem_inventory 
               WHERE airtable_id IS NOT NULL 
               AND page_status = 'Active'"""
        )).fetchall()
    
    print(f"✅ Found {len(result)} Active URLs with Airtable IDs")
    
    # Check which ones have 0 sessions
    zero_session_urls = []
    for row in result:
        url, airtable_id, page_status = row
        normalized = normalize_url(url)
        sessions = bq_sessions.get(normalized, 0)
        
        if sessions == 0:
            zero_session_urls.append({
                "url": url,
                "airtable_id": airtable_id,
                "sessions": sessions
            })
    
    print(f"\n📊 Found {len(zero_session_urls)} Active URLs with 0 sessions")
    
    if not zero_session_urls:
        print("✅ No updates needed!")
        return {
            "total_checked": len(result),
            "zero_sessions_found": 0,
            "updated": 0,
            "failed": 0
        }
    
    # Show first few examples
    print(f"\nFirst 5 examples:")
    for item in zero_session_urls[:5]:
        print(f"  - {item['url'][:80]}")
    
    # Ask for confirmation (unless auto_update is True)
    if not auto_update:
        response = input(f"\n❓ Update {len(zero_session_urls)} URLs to 'Paused' in Airtable? (y/n): ")
        if response.lower() != 'y':
            print("❌ Aborted")
            return {
                "total_checked": len(result),
                "zero_sessions_found": len(zero_session_urls),
                "updated": 0,
                "failed": 0
            }
    else:
        print(f"\n✅ Auto-update enabled, proceeding with {len(zero_session_urls)} updates...")
    
    # Update in Airtable
    print(f"\n🔄 Updating {len(zero_session_urls)} URLs in Airtable...")
    updated_count = 0
    failed_count = 0
    
    for idx, item in enumerate(zero_session_urls):
        success = airtable_service.update_page_status(item["airtable_id"], "Paused")
        if success:
            updated_count += 1
        else:
            failed_count += 1
        
        if (idx + 1) % 10 == 0:
            print(f"   Progress: {idx + 1}/{len(zero_session_urls)}")
    
    print(f"\n✅ Complete: {updated_count} updated, {failed_count} failed")
    
    return {
        "total_checked": len(result),
        "zero_sessions_found": len(zero_session_urls),
        "updated": updated_count,
        "failed": failed_count
    }


def main(use_cache=False, enable_logging=False):
    """Main entry point with optional task logging"""
    print("🚀 Starting zero sessions check...")
    
    if enable_logging:
        with log_task_execution("check_zero_sessions") as task_log:
            stats = check_and_update_zero_sessions(use_cache=use_cache)
            task_log.update_stats(stats)
            return stats
    else:
        return check_and_update_zero_sessions(use_cache=use_cache)


if __name__ == "__main__":
    import argparse
    
    p = argparse.ArgumentParser(description="Check for URLs with zero sessions and update Airtable")
    p.add_argument("--cache", action="store_true", help="Use cached BigQuery data")
    p.add_argument("--log", action="store_true", help="Enable task logging to database")
    args = p.parse_args()
    
    main(use_cache=args.cache, enable_logging=args.log)

