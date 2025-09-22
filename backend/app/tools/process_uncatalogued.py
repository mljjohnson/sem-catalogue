#!/usr/bin/env python3
"""
Simple script to process uncatalogued URLs (status_code=0)
Uses the existing batch processor logic but queries the database instead of reading CSV
"""

import asyncio
from typing import List
from sqlalchemy import select
from app.models.db import get_session
from app.models.tables import PageSEMInventory
from app.ai.run_batch import main as run_batch_main
import tempfile
from pathlib import Path


def get_uncatalogued_urls(limit: int = None) -> List[str]:
    """Get URLs that need cataloguing (status_code=0)"""
    
    with get_session() as session:
        query = select(PageSEMInventory.url).where(PageSEMInventory.status_code == 0)
        
        if limit:
            query = query.limit(limit)
            
        result = session.execute(query)
        urls = [row[0] for row in result.fetchall()]
        return urls


async def process_uncatalogued(count: int = None, concurrency: int = 2):
    """Process uncatalogued URLs using the existing batch processor"""
    
    urls = get_uncatalogued_urls(limit=count)
    
    if not urls:
        print("No uncatalogued URLs found!")
        return
        
    print(f"Found {len(urls)} uncatalogued URLs to process")
    
    # Create a temporary file with the URLs
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        for url in urls:
            f.write(f"{url}\n")
        temp_file = Path(f.name)
    
    try:
        # Use the existing batch processor
        out_file = Path("data/latest/uncatalogued_batch_results.csv")
        await run_batch_main(
            seed=temp_file,
            count=len(urls), 
            concurrency=concurrency,
            out_csv=out_file,
            quiet=False,
            force=True  # Reprocess even if exists
        )
        print(f"Results saved to: {out_file}")
        
    finally:
        # Clean up temp file
        temp_file.unlink(missing_ok=True)


if __name__ == "__main__":
    import argparse
    
    p = argparse.ArgumentParser(description="Process uncatalogued URLs (status_code=0)")
    p.add_argument("--count", type=int, default=None, help="Max URLs to process (default: all uncatalogued)")
    p.add_argument("--concurrency", type=int, default=2, help="Concurrent workers")
    args = p.parse_args()
    
    asyncio.run(process_uncatalogued(args.count, args.concurrency))
