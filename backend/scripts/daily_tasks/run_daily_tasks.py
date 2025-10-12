#!/usr/bin/env python3
"""
Master orchestrator for daily tasks
Runs in sequence:
1. Airtable sync - Check for new URLs in Airtable
2. Crawler check - Check for updated URLs via crawler API
3. Cataloguing - Process all uncatalogued URLs through LLM
"""
import logging
import sys
import os
from datetime import datetime

# Add the backend directory to the Python path
backend_dir = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.abspath(backend_dir))

from app.services.task_logger import log_task_execution

# Import the individual task scripts
from scripts.daily_tasks.scheduled_airtable_sync_optimized import sync_airtable_data
from scripts.daily_tasks.check_updated_urls import check_updated_urls
from scripts.daily_tasks.check_zero_sessions import check_and_update_zero_sessions
from app.tools.process_uncatalogued import process_uncatalogued

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

async def main():
    """Main entry point for daily tasks orchestration"""
    print("=" * 80)
    print("🚀 STARTING DAILY TASKS")
    print("=" * 80)
    
    with log_task_execution("daily_tasks_orchestrator") as task_log:
        all_stats = {}
        
        # TASK 1: Sync Airtable
        print("\n" + "=" * 80)
        print("📊 TASK 1/3: AIRTABLE SYNC")
        print("=" * 80)
        try:
            airtable_stats = sync_airtable_data()
            print(f"✅ Airtable sync completed: {airtable_stats}")
            all_stats["airtable"] = airtable_stats
            task_log.update_stats({"airtable": airtable_stats})
        except Exception as e:
            logger.error(f"❌ Airtable sync failed: {e}", exc_info=True)
            all_stats["airtable"] = {"error": str(e)}
        
        # TASK 2: Check for updated URLs
        print("\n" + "=" * 80)
        print("🔄 TASK 2/3: CRAWLER UPDATE CHECK")
        print("=" * 80)
        try:
            crawler_stats = check_updated_urls(days=1)
            print(f"✅ Crawler check completed: {crawler_stats}")
            all_stats["crawler"] = crawler_stats
            task_log.update_stats({"crawler": crawler_stats})
        except Exception as e:
            logger.error(f"❌ Crawler check failed: {e}", exc_info=True)
            all_stats["crawler"] = {"error": str(e)}
        
        # TASK 3: Check for zero sessions and update Airtable
        # TEMPORARILY DISABLED - uncomment when ready to enable
        # print("\n" + "=" * 80)
        # print("📊 TASK 3/4: ZERO SESSIONS CHECK")
        # print("=" * 80)
        # try:
        #     zero_sessions_stats = check_and_update_zero_sessions(use_cache=False, auto_update=True)
        #     print(f"✅ Zero sessions check completed: {zero_sessions_stats}")
        #     all_stats["zero_sessions"] = zero_sessions_stats
        #     task_log.update_stats({"zero_sessions": zero_sessions_stats})
        # except Exception as e:
        #     logger.error(f"❌ Zero sessions check failed: {e}", exc_info=True)
        #     all_stats["zero_sessions"] = {"error": str(e)}
        
        # TASK 3: Catalogue uncatalogued URLs (was TASK 4 when zero sessions was enabled)
        print("\n" + "=" * 80)
        print("🤖 TASK 3/3: LLM CATALOGUING")
        print("=" * 80)
        try:
            # Get concurrency from environment variable, default to 5
            concurrency = int(os.environ.get('CATALOGUER_CONCURRENCY', '5'))
            print(f"Using concurrency: {concurrency}")
            cataloguer_stats = await process_uncatalogued(count=None, concurrency=concurrency, enable_logging=True)
            print(f"✅ Cataloguing completed: {cataloguer_stats}")
            all_stats["cataloguer"] = cataloguer_stats
            task_log.update_stats({"cataloguer": cataloguer_stats})
        except Exception as e:
            logger.error(f"❌ Cataloguing failed: {e}", exc_info=True)
            all_stats["cataloguer"] = {"error": str(e)}
        
        # Final summary
        print("\n" + "=" * 80)
        print("✅ DAILY TASKS COMPLETED")
        print("=" * 80)
        print(f"📊 Airtable: {all_stats.get('airtable', {})}")
        print(f"🔄 Crawler: {all_stats.get('crawler', {})}")
        # print(f"📉 Zero Sessions: {all_stats.get('zero_sessions', {})}") # DISABLED
        print(f"🤖 Cataloguer: {all_stats.get('cataloguer', {})}")
        print("=" * 80)
        
        return {
            "success": True,
            "timestamp": datetime.utcnow().isoformat(),
            "stats": all_stats
        }

if __name__ == "__main__":
    import asyncio
    result = asyncio.run(main())
    
    # Exit with non-zero code if any task had errors
    if not result.get("success"):
        sys.exit(1)
    else:
        sys.exit(0)

