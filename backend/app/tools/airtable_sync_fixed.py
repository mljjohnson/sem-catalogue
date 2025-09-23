#!/usr/bin/env python3
"""
Fixed Airtable sync with proper URL normalization.
"""

import click
import sqlalchemy as sa
from typing import Set
from app.models.db import get_session
from app.models.tables import PageSEMInventory
from app.services.airtable import airtable_service
from app.services.pages import upsert_page
from loguru import logger

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

@click.command()
@click.option("--dry-run", is_flag=True, help="Do not write any changes to the database.")
@click.option("--auto-process", is_flag=True, help="Automatically process new URLs with LLM.")
@click.option("--max-process", type=int, default=5, help="Max new URLs to auto-process.")
def main(dry_run: bool, auto_process: bool, max_process: int):
    """Sync data from Airtable to database with fixed URL matching."""
    
    logger.info("Starting Airtable sync with fixed URL matching...")
    
    # Fetch all records from Airtable
    logger.info("Fetching records from Airtable...")
    airtable_records = airtable_service.fetch_all_records()
    logger.info(f"Found {len(airtable_records)} records in Airtable")
    
    # Create normalized URL mapping for Airtable
    airtable_url_map = {}
    airtable_normalized_urls = set()
    
    for record in airtable_records:
        original_url = record.get("landing_page")
        
        # Skip if URL is missing
        if not original_url:
            continue
            
        normalized = normalize_url(original_url)
        airtable_url_map[normalized] = {
            "original_url": original_url,
            "record": record
        }
        airtable_normalized_urls.add(normalized)
    
    logger.info(f"Found {len(airtable_normalized_urls)} unique normalized URLs in Airtable")
    
    # Get all existing URLs from database
    with get_session() as session:
        db_result = session.execute(
            sa.select(PageSEMInventory.url, PageSEMInventory.page_id)
        ).all()
        
        db_url_map = {}
        db_normalized_urls = set()
        
        for row in db_result:
            original_url = row.url
            normalized = normalize_url(original_url)
            db_url_map[normalized] = {
                "original_url": original_url,
                "page_id": row.page_id
            }
            db_normalized_urls.add(normalized)
    
    logger.info(f"Found {len(db_normalized_urls)} URLs in database")
    
    # Find overlaps and differences
    existing_urls = airtable_normalized_urls & db_normalized_urls
    new_urls = airtable_normalized_urls - db_normalized_urls
    
    logger.info(f"Analysis results:")
    logger.info(f"  - Total Airtable URLs: {len(airtable_normalized_urls)}")
    logger.info(f"  - Total Database URLs: {len(db_normalized_urls)}")
    logger.info(f"  - Existing URLs (would update metadata): {len(existing_urls)}")
    logger.info(f"  - New URLs (would process): {len(new_urls)}")
    
    if len(new_urls) > 0:
        sample_new = list(new_urls)[:10]
        logger.info(f"  - Sample new URLs: {[airtable_url_map[url]['original_url'] for url in sample_new]}")
    
    if len(existing_urls) > 0:
        sample_existing = list(existing_urls)[:5]
        logger.info(f"  - Sample existing URLs:")
        for norm_url in sample_existing:
            airtable_orig = airtable_url_map[norm_url]['original_url']
            db_orig = db_url_map[norm_url]['original_url']
            logger.info(f"    DB: {db_orig}")
            logger.info(f"    AT: {airtable_orig}")
    
    if dry_run:
        logger.info("DRY RUN - No changes made to database")
        return
    
    # Update existing URLs with metadata using direct SQL update
    updates_count = 0
    with get_session() as session:
        for normalized_url in existing_urls:
            airtable_data = airtable_url_map[normalized_url]
            record = airtable_data["record"]
            db_data = db_url_map[normalized_url]
            
            # Extract metadata from Airtable
            channel = record.get("channel")
            team = record.get("team") 
            brand = record.get("brand")
            vertical = record.get("vertical")
            primary_category = record.get("primary_category")
            page_status = record.get("page_status")
            
            try:
                # Update using direct SQL to avoid needing all required fields
                stmt = sa.update(PageSEMInventory).where(
                    PageSEMInventory.page_id == db_data["page_id"]
                ).values(
                    channel=channel,
                    team=team,
                    brand=brand,
                    vertical=vertical,
                    primary_category=primary_category,
                    page_status=page_status,
                )
                session.execute(stmt)
                updates_count += 1
                
                if updates_count % 100 == 0:
                    logger.info(f"Updated {updates_count} existing URLs...")
                    
            except Exception as e:
                logger.error(f"Error updating {db_data['original_url']}: {e}")
        
        session.commit()
    
    logger.info(f"Updated metadata for {updates_count} existing URLs")
    
    # Process new URLs
    if auto_process and new_urls:
        process_count = min(len(new_urls), max_process)
        logger.info(f"Auto-processing {process_count} new URLs...")
        
        processed = 0
        with get_session() as session:
            for normalized_url in list(new_urls)[:process_count]:
                airtable_data = airtable_url_map[normalized_url]
                record = airtable_data["record"]
                original_url = airtable_data["original_url"]
                
                # Extract metadata
                channel = record.get("channel")
                team = record.get("team")
                brand = record.get("brand")
                vertical = record.get("vertical")
                primary_category = record.get("primary_category")
                page_status = record.get("page_status")
                
                try:
                    # Generate page_id from URL
                    import hashlib
                    page_id = hashlib.md5(original_url.encode()).hexdigest()[:16]
                    
                    upsert_page(
                        session=session,
                        page_id=page_id,
                        url=original_url,
                        canonical_url=original_url,  # Use same as URL for now
                        status_code=0,  # 0 = not yet crawled
                        template_type=None,  # To be determined by LLM
                        has_coupons=False,  # Default
                        channel=channel,
                        team=team,
                        brand=brand,
                        vertical=vertical,
                        primary_category=primary_category,
                        page_status=page_status,
                    )
                    processed += 1
                    logger.info(f"Processed new URL: {original_url}")
                    
                except Exception as e:
                    logger.error(f"Error processing {original_url}: {e}")
            
            session.commit()
        
        logger.info(f"Successfully processed {processed} new URLs")
    
    logger.info("Airtable sync completed successfully")

if __name__ == "__main__":
    main()
