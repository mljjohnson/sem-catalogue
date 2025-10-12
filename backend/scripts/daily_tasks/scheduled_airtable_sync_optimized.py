#!/usr/bin/env python3
"""
Optimized scheduled Airtable sync entry point for ECS
Runs daily to sync new pages from Airtable and process them through the LLM cataloguer
"""
import logging
import sys
import os
from datetime import datetime

# Add the backend directory to the Python path
backend_dir = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.abspath(backend_dir))

from app.models.db import get_session
import sqlalchemy as sa
from app.services.task_logger import log_task_execution

# Configure logging for AWS CloudWatch
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def normalize_url(url: str) -> str:
    """Normalize URL for comparison."""
    if not url:
        return ""
    
    # Convert to lowercase
    url = url.lower().strip()
    
    # Convert https to http
    url = url.replace('https://', 'http://')
    
    # Remove trailing slash
    url = url.rstrip('/')
    
    # Ensure www. prefix for forbes.com
    if url.startswith('http://forbes.com'):
        url = url.replace('http://forbes.com', 'http://www.forbes.com')
    
    return url

def sync_airtable_data(use_cache=False):
    """Optimized Airtable sync using bulk operations"""
    print("🔄 Starting Airtable sync...")
    
    if use_cache:
        # Load from cache
        import json
        cache_file = os.path.join(backend_dir, "airtable_cache.json")
        print(f"📖 Loading Airtable records from cache...")
        with open(cache_file, 'r') as f:
            airtable_records = json.load(f)
        print(f"✅ Loaded {len(airtable_records)} records from Airtable cache")
    else:
        # Fetch from Airtable API
        from app.services.airtable import airtable_service
        print(f"📡 Fetching records from Airtable API...")
        airtable_records = airtable_service.fetch_all_records()
        print(f"✅ Fetched {len(airtable_records)} records from Airtable API")
    
    # Build Airtable lookup: normalized_url -> record_data
    print("🔨 Building Airtable URL lookup...")
    airtable_by_url = {}
    for record in airtable_records:
        original_url = record.get("landing_page")
        if original_url:
            normalized = normalize_url(original_url)
            airtable_by_url[normalized] = {
                "original_url": original_url,
                "category": record.get("primary_category"),
                "vertical": record.get("vertical"),
                "page_status": record.get("page_status"),
                "airtable_id": record.get("airtable_record_id")
            }
    print(f"✅ Built lookup for {len(airtable_by_url)} Airtable URLs")
    
    # Get all existing URLs from database with their page_status in ONE query
    # Note: A URL may have multiple versions, so we collect all statuses for each URL
    print("🗄️  Fetching all database URLs with page_status...")
    with get_session() as session:
        result = session.execute(sa.text(
            "SELECT url, page_status FROM pages_sem_inventory"
        )).fetchall()
        
        # Build dict: normalized_url -> set of statuses (since a URL can have multiple versions)
        existing_db_urls = {}
        for row in result:
            norm_url = normalize_url(row[0])
            if norm_url not in existing_db_urls:
                existing_db_urls[norm_url] = set()
            existing_db_urls[norm_url].add(row[1])
    
    print(f"✅ Found {len(existing_db_urls)} unique URLs in database")
    
    # Determine what needs to be added vs updated
    urls_to_add = set(airtable_by_url.keys()) - set(existing_db_urls.keys())
    
    # Only update if ALL versions of the URL have a different page_status than Airtable
    # (If ANY version already has the correct status, skip update)
    urls_to_update = []
    for norm_url in set(airtable_by_url.keys()) & set(existing_db_urls.keys()):
        at_status = airtable_by_url[norm_url]["page_status"]
        db_statuses = existing_db_urls[norm_url]
        
        # Normalize None to string for comparison (Airtable may return None or empty)
        at_status_normalized = at_status if at_status else None
        db_statuses_normalized = {(s if s else None) for s in db_statuses}
        
        # Only update if none of the versions have the correct status
        if at_status_normalized not in db_statuses_normalized:
            urls_to_update.append(norm_url)
    
    print(f"📝 Need to add {len(urls_to_add)} new URLs")
    print(f"🔄 Need to update {len(urls_to_update)} URLs with changed page_status")
    
    # Add new URLs in batches
    new_count = 0
    if urls_to_add:
        print(f"➕ Adding {len(urls_to_add)} new URLs...")
        with get_session() as session:
            # Get next page_id once
            max_id_result = session.execute(sa.text(
                "SELECT MAX(CAST(SUBSTRING(page_id, 9) AS UNSIGNED)) FROM pages_sem_inventory WHERE page_id LIKE 'unified_%'"
            )).scalar()
            next_id = (max_id_result or 0) + 1
            
            # Insert in batches
            for idx, norm_url in enumerate(urls_to_add):
                at_data = airtable_by_url[norm_url]
                page_id = f"unified_{next_id:06d}"
                next_id += 1
                
                try:
                    session.execute(sa.text("""
                        INSERT INTO pages_sem_inventory 
                        (page_id, url, canonical_url, status_code, catalogued, primary_category, vertical, page_status, airtable_id)
                        VALUES (:page_id, :url, :canonical_url, 0, 0, :category, :vertical, :page_status, :airtable_id)
                    """), {
                        "page_id": page_id,
                        "url": at_data["original_url"],
                        "canonical_url": at_data["original_url"],
                        "category": at_data["category"],
                        "vertical": at_data["vertical"],
                        "page_status": at_data["page_status"],
                        "airtable_id": at_data["airtable_id"]
                    })
                    new_count += 1
                    
                    # Commit every 100 inserts
                    if (idx + 1) % 100 == 0:
                        session.commit()
                        print(f"   ✓ Inserted {idx + 1}/{len(urls_to_add)}")
                except Exception as e:
                    logger.error(f"❌ Failed to add URL {at_data['original_url']}: {e}")
                    
            # Final commit
            session.commit()
            print(f"✅ Added {new_count} new URLs")
    
    # Update page_status for URLs where it changed
    updated_count = 0
    rows_affected = 0
    if urls_to_update:
        print(f"🔄 Updating page_status for {len(urls_to_update)} URLs...")
        with get_session() as session:
            for idx, norm_url in enumerate(urls_to_update):
                at_data = airtable_by_url[norm_url]
                
                # Strip trailing slash to match database format
                url_for_match = at_data["original_url"].rstrip('/')
                
                result = session.execute(sa.text("""
                    UPDATE pages_sem_inventory 
                    SET page_status = :page_status
                    WHERE url = :url
                """), {
                    "page_status": at_data["page_status"],
                    "url": url_for_match
                })
                rows_affected += result.rowcount
                updated_count += 1
                
                # Debug first few
                if idx < 3:
                    print(f"   [{idx+1}] Updated {result.rowcount} rows for: {at_data['original_url'][:60]}")
                
                # Commit every 100 updates
                if (idx + 1) % 100 == 0:
                    session.commit()
                    print(f"   ✓ Committed {idx + 1}/{len(urls_to_update)} (total rows: {rows_affected})")
                    
            # Final commit
            session.commit()
            print(f"✅ Updated {updated_count} URLs ({rows_affected} total rows affected)")
    
    print(f"✅ Airtable sync complete: {new_count} new, {updated_count} updated")
    return {"new_urls": new_count, "updated_urls": updated_count, "total_airtable": len(airtable_records)}

def main(use_cache=False):
    """Main entry point for Airtable sync - ONLY syncs new URLs and page_status changes"""
    print("🚀 Starting Airtable sync...")
    
    with log_task_execution("airtable_sync") as task_log:
        # Sync Airtable data (adds new URLs and updates page_status)
        airtable_stats = sync_airtable_data(use_cache=use_cache)
        print(f"✅ Airtable sync completed: {airtable_stats}")
        task_log.update_stats(airtable_stats)
        
        return {
            "success": True,
            "timestamp": datetime.utcnow().isoformat(),
            "stats": airtable_stats
        }

if __name__ == "__main__":
    result = main()
    
    # Exit with non-zero code if failed (for ECS task monitoring)
    if not result.get("success"):
        sys.exit(1)
    else:
        sys.exit(0)

