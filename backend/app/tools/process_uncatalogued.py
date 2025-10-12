#!/usr/bin/env python3
"""
Simple script to process uncatalogued URLs (status_code=0)
Uses the existing batch processor logic but queries the database instead of reading CSV
"""

import asyncio
import sys
import os
from typing import List
from pathlib import Path
import tempfile

# Add backend directory to Python path
backend_dir = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.abspath(backend_dir))

from sqlalchemy import select
from app.models.db import get_session
from app.models.tables import PageSEMInventory
from app.ai.run_batch import main as run_batch_main
from app.services.task_logger import log_task_execution


def get_uncatalogued_urls(limit: int = None) -> List[tuple]:
    """Get URLs that need cataloguing (catalogued=0 and status_code in 0, 200)
    Returns list of (id, url) tuples"""
    
    with get_session() as session:
        query = (
            select(PageSEMInventory.id, PageSEMInventory.url)
            .where(PageSEMInventory.catalogued == 0)
            .where(PageSEMInventory.status_code.in_([0, 200]))
        )
        
        if limit:
            query = query.limit(limit)
            
        result = session.execute(query)
        return result.fetchall()


async def process_uncatalogued(count: int = None, concurrency: int = 2, enable_logging: bool = False):
    """Process uncatalogued URLs using the existing batch processor
    
    Args:
        count: Number of URLs to process (None = all)
        concurrency: Number of concurrent workers
        enable_logging: If True, log to task_logs table
    """
    
    if enable_logging:
        # Use task logging
        with log_task_execution("llm_cataloguing") as task_log:
            return await _process_uncatalogued_inner(count, concurrency, task_log)
    else:
        # No logging (for manual runs)
        return await _process_uncatalogued_inner(count, concurrency, None)


async def _process_uncatalogued_inner(count: int, concurrency: int, task_log=None):
    """Inner function that does the actual processing"""
    records = get_uncatalogued_urls(limit=count)
    
    if not records:
        print("No uncatalogued URLs found!")
        stats = {"total_urls": 0, "processed": 0, "skipped": 0, "failed": 0}
        if task_log:
            task_log.update_stats(stats)
        return stats
        
    print(f"Found {len(records)} uncatalogued URLs to process")
    if task_log:
        task_log.update_stats({"total_urls": len(records)})
    
    # Create a temporary file with the record IDs and URLs (format: id|url)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        for record_id, url in records:
            f.write(f"{record_id}|{url}\n")
        temp_file = Path(f.name)
    
    try:
        # Use the existing batch processor
        out_file = Path("data/latest/uncatalogued_batch_results.csv")
        await run_batch_main(
            seed=temp_file,
            count=len(records), 
            concurrency=concurrency,
            out_csv=out_file,
            quiet=True,  # Suppress verbose logger output
            force=True  # Reprocess even if exists
        )
        # Don't show CSV save message - internal detail
        
        # Count results
        stats = {
            "total_urls": len(records),
            "concurrency": concurrency,
            "output_file": str(out_file)
        }
        if task_log:
            task_log.update_stats(stats)
        
        return stats
        
    finally:
        # Clean up temp file
        temp_file.unlink(missing_ok=True)


if __name__ == "__main__":
    import argparse
    
    p = argparse.ArgumentParser(description="Process uncatalogued URLs (catalogued=0)")
    p.add_argument("--count", type=int, default=None, help="Max URLs to process (default: all uncatalogued)")
    p.add_argument("--concurrency", type=int, default=2, help="Concurrent workers")
    p.add_argument("--log", action="store_true", help="Enable task logging to database")
    args = p.parse_args()
    
    asyncio.run(process_uncatalogued(args.count, args.concurrency, args.log))
