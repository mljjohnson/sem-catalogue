#!/usr/bin/env python3
"""
Check for updated URLs from crawler and mark them for recataloguing
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

# Add backend to path
backend_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(backend_dir))

from app.services.crawler_graphql import crawler_graphql_service
from app.models.db import get_session
from app.models.tables import PageSEMInventory
from app.services.task_logger import log_task_execution
import sqlalchemy as sa
from loguru import logger

def check_updated_urls(days: int = 1):
    """
    Check crawler for URLs updated in the last N days
    Mark them for recataloguing by creating new uncatalogued records
    """
    logger.info(f"Checking for URLs updated in last {days} day(s)...")
    
    # Get updated pages from crawler
    updated_pages = crawler_graphql_service.get_updated_pages_last_n_days(days=days)
    logger.info(f"Found {len(updated_pages)} updated pages from crawler")
    
    if not updated_pages:
        return {"checked": 0, "marked_for_recatalogue": 0}
    
    # Get URLs that exist in our database
    with get_session() as session:
        # Get all URLs from database
        db_urls_result = session.execute(sa.text("""
            SELECT DISTINCT url 
            FROM pages_sem_inventory 
            WHERE airtable_id IS NOT NULL
        """)).fetchall()
        
        db_urls = {row[0].lower() for row in db_urls_result}
    
    logger.info(f"Found {len(db_urls)} URLs in database")
    
    # Find which updated URLs are in our database
    urls_to_recatalogue = []
    for page in updated_pages:
        url = page.get("url", "")
        if url.lower() in db_urls:
            urls_to_recatalogue.append(url)
    
    logger.info(f"Found {len(urls_to_recatalogue)} updated URLs that need recataloguing")
    
    if not urls_to_recatalogue:
        return {"checked": len(updated_pages), "marked_for_recatalogue": 0}
    
    # Create new uncatalogued records for these URLs
    marked_count = 0
    with get_session() as session:
        for url in urls_to_recatalogue:
            try:
                # Get the latest record for this URL
                latest = session.execute(sa.text("""
                    SELECT page_id, canonical_url, primary_category, vertical, 
                           airtable_id, page_status
                    FROM pages_sem_inventory
                    WHERE url = :url
                    ORDER BY last_seen DESC
                    LIMIT 1
                """), {"url": url}).fetchone()
                
                if not latest:
                    continue
                
                page_id, canonical_url, category, vertical, airtable_id, page_status = latest
                
                # Create new uncatalogued record
                today = datetime.now().date()
                session.execute(sa.text("""
                    INSERT INTO pages_sem_inventory 
                    (page_id, url, canonical_url, status_code, catalogued, 
                     primary_category, vertical, airtable_id, page_status,
                     first_seen, last_seen)
                    VALUES (:page_id, :url, :canonical_url, 0, 0,
                            :category, :vertical, :airtable_id, :page_status,
                            :today, :today)
                """), {
                    "page_id": page_id,
                    "url": url,
                    "canonical_url": canonical_url or url,
                    "category": category,
                    "vertical": vertical,
                    "airtable_id": airtable_id,
                    "page_status": page_status,
                    "today": today
                })
                marked_count += 1
                
            except Exception as e:
                logger.error(f"Failed to mark {url} for recatalogue: {e}")
        
        session.commit()
    
    logger.info(f"Marked {marked_count} URLs for recataloguing")
    
    return {
        "checked": len(updated_pages),
        "marked_for_recatalogue": marked_count,
        "updated_urls": urls_to_recatalogue[:10]  # Sample for logging
    }

async def main():
    """Main entry point"""
    with log_task_execution("check_updated_urls") as task_log:
        result = check_updated_urls(days=7)  # Check last 7 days (temporary for initial run)
        task_log.update_stats(result)
        logger.info(f"✅ Check complete: {result}")
        return result

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

