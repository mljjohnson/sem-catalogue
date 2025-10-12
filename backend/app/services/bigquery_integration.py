"""
Integration service for BigQuery page views with the SEM catalogue workflow.
Handles session tracking and Airtable status updates.
"""

import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import pandas as pd
from urllib.parse import urlparse
import os
import json

from app.services.bigquery_page_views import BigQueryPageViews
from app.services.airtable import airtable_service
from app.models.db import get_session
from app.models.tables import PageSEMInventory
from pyairtable import Table
from app.core.config import settings

logger = logging.getLogger(__name__)

class BigQueryIntegrationService:
    """Service to integrate BigQuery page views with SEM catalogue workflow"""
    
    def __init__(self):
        # Initialize BigQuery service
        self.bq_service = BigQueryPageViews(
            project_id=settings.google_cloud_project or "fm-gold-layer",
            credentials_path=settings.google_application_credentials
        )
        
        # Cache for BigQuery data to avoid multiple expensive queries
        self._bigquery_data_cache = None
        self._cache_file_path = "bigquery_cache.json"
        
        # Initialize Airtable table for updates
        if settings.airtable_pat:
            self.airtable_table = Table(
                settings.airtable_pat, 
                settings.airtable_base_id, 
                settings.airtable_table_id
            )
        else:
            self.airtable_table = None
            logger.warning("AIRTABLE_PAT not set - cannot update Airtable records")

    def normalize_url_for_comparison(self, url_or_path: str) -> str:
        """
        Normalize URL or path for comparison between BigQuery and database.
        
        Args:
            url_or_path: Full URL or just a path (from BigQuery)
            
        Returns:
            Normalized path for comparison (without domain, without UTM params)
        """
        if not url_or_path:
            return ""
        
        try:
            # Convert to string and strip whitespace
            url_str = str(url_or_path).strip()
            if not url_str:
                return ""
            
            # If it's a full URL, extract the path
            if url_str.startswith(('http://', 'https://')):
                parsed = urlparse(url_str.lower())
                path = parsed.path
            else:
                # It's already a path (from BigQuery)
                path = url_str.lower()
            
            # Remove query parameters (everything after ?)
            if '?' in path:
                path = path.split('?')[0]
            
            # Remove fragment identifiers (everything after #)
            if '#' in path:
                path = path.split('#')[0]
            
            # Remove trailing slash for consistency
            path = path.rstrip('/')
            
            # Ensure we have a clean path
            if not path:
                return ""
                
            return path
            
        except Exception as e:
            logger.warning(f"Failed to normalize URL {url_or_path}: {e}")
            return str(url_or_path).lower().strip() if url_or_path else ""

    def _load_from_disk_cache(self) -> Optional[pd.DataFrame]:
        """Load BigQuery data from disk cache if available and recent"""
        try:
            if os.path.exists(self._cache_file_path):
                # Check if cache is less than 1 hour old
                cache_age = datetime.now().timestamp() - os.path.getmtime(self._cache_file_path)
                if cache_age < 3600:  # 1 hour in seconds
                    logger.info("Loading BigQuery data from disk cache...")
                    with open(self._cache_file_path, 'r') as f:
                        data = json.load(f)
                    df = pd.DataFrame(data)
                    logger.info(f"Loaded {len(df)} records from disk cache")
                    return df
                else:
                    logger.info("Disk cache is older than 1 hour, will fetch fresh data")
            return None
        except Exception as e:
            logger.warning(f"Failed to load from disk cache: {e}")
            return None

    def _save_to_disk_cache(self, df: pd.DataFrame):
        """Save BigQuery data to disk cache"""
        try:
            logger.info(f"Saving {len(df)} records to disk cache: {self._cache_file_path}")
            with open(self._cache_file_path, 'w') as f:
                json.dump(df.to_dict('records'), f, indent=2)
            logger.info("BigQuery data saved to disk cache")
        except Exception as e:
            logger.warning(f"Failed to save to disk cache: {e}")

    def get_page_sessions_data(self, force_refresh: bool = False, use_disk_cache: bool = True) -> pd.DataFrame:
        """
        Get page views data from BigQuery with memory and disk caching.
        
        Args:
            force_refresh: If True, bypass all caches and fetch fresh data
            use_disk_cache: If True, try to load from disk cache first
        
        Returns:
            DataFrame with columns: property, visit_page, sessions, live, normalized_url
        """
        # Return memory cached data if available and not forcing refresh
        if self._bigquery_data_cache is not None and not force_refresh:
            logger.info(f"Using memory cached BigQuery data ({len(self._bigquery_data_cache)} records)")
            return self._bigquery_data_cache
        
        # Try disk cache if enabled and not forcing refresh
        if use_disk_cache and not force_refresh:
            disk_data = self._load_from_disk_cache()
            if disk_data is not None:
                self._bigquery_data_cache = disk_data
                return disk_data
        
        logger.info("Fetching fresh page views data from BigQuery...")
        
        try:
            # Run the BigQuery analysis (expensive operation)
            df = self.bq_service.run_page_views_analysis()
            
            # Add normalized URL column for matching
            df['normalized_url'] = df['visit_page'].apply(self.normalize_url_for_comparison)
            
            # Cache the results in memory
            self._bigquery_data_cache = df
            
            # Save to disk cache
            if use_disk_cache:
                self._save_to_disk_cache(df)
            
            logger.info(f"Retrieved {len(df)} page records from BigQuery")
            logger.info(f"Live pages: {len(df[df['live'] == 'Live'])}, Dead pages: {len(df[df['live'] == 'Dead'])}")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get BigQuery data: {e}")
            raise

    def get_database_urls(self) -> List[Dict[str, str]]:
        """
        Get URLs from the database with their Airtable record IDs.
        
        Returns:
            List of dicts with url, normalized_url, airtable_id, page_status
        """
        logger.info("Fetching URLs from database...")
        
        with get_session() as session:
            # Get URLs that have Airtable IDs and are currently Active
            results = session.query(
                PageSEMInventory.url,
                PageSEMInventory.airtable_id, 
                PageSEMInventory.page_status
            ).filter(
                PageSEMInventory.airtable_id.isnot(None),
                PageSEMInventory.page_status == 'Active'
            ).all()
            
            db_urls = []
            for url, airtable_id, page_status in results:
                normalized_path = self.normalize_url_for_comparison(url)
                db_urls.append({
                    'url': url,
                    'normalized_url': normalized_path,
                    'airtable_id': airtable_id,
                    'page_status': page_status
                })
            
            logger.info(f"Found {len(db_urls)} active URLs with Airtable IDs in database")
            return db_urls

    def find_zero_session_urls(self) -> List[Dict[str, any]]:
        """
        Find URLs in database that have 0 sessions in BigQuery.
        
        Returns:
            List of URLs that should be marked as inactive
        """
        logger.info("Analyzing URLs for zero sessions...")
        
        # Get BigQuery data (will use cache if available)
        bq_data = self.get_page_sessions_data()
        
        # Create lookup of normalized URLs to session counts
        bq_sessions = {}
        for _, row in bq_data.iterrows():
            normalized_url = row['normalized_url']
            sessions = row['sessions']
            
            # Store the highest session count if there are duplicates
            if normalized_url in bq_sessions:
                bq_sessions[normalized_url] = max(bq_sessions[normalized_url], sessions)
            else:
                bq_sessions[normalized_url] = sessions
        
        # Get database URLs
        db_urls = self.get_database_urls()
        
        # Find URLs with 0 sessions (only if they exist in BigQuery data)
        zero_session_urls = []
        for url_data in db_urls:
            normalized_url = url_data['normalized_url']
            
            # Only process URLs that exist in BigQuery data
            if normalized_url in bq_sessions:
                sessions = bq_sessions[normalized_url]
                if sessions == 0:
                    url_data['sessions'] = sessions
                    zero_session_urls.append(url_data)
                    logger.info(f"Zero sessions found for: {url_data['url']}")
            else:
                logger.debug(f"URL not found in BigQuery data (ignoring): {url_data['url']}")
        
        logger.info(f"Found {len(zero_session_urls)} URLs with 0 sessions (out of {len(db_urls)} database URLs)")
        return zero_session_urls

    def update_airtable_status(self, airtable_record_id: str, new_status: str = "Paused") -> bool:
        """
        Update a specific Airtable record's page status.
        
        Args:
            airtable_record_id: Airtable record ID
            new_status: New status value (default: "Paused")
            
        Returns:
            True if successful, False otherwise
        """
        if not self.airtable_table:
            logger.error("Airtable not configured - cannot update status")
            return False
        
        try:
            # Update the Page Status field
            self.airtable_table.update(airtable_record_id, {"Page Status": new_status})
            logger.info(f"Updated Airtable record {airtable_record_id} to status: {new_status}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update Airtable record {airtable_record_id}: {e}")
            return False

    def update_database_status(self, url: str, new_status: str = "Paused") -> bool:
        """
        Update the page status in the database.
        
        Args:
            url: URL to update
            new_status: New status value
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with get_session() as session:
                session.query(PageSEMInventory).filter(
                    PageSEMInventory.url == url
                ).update({"page_status": new_status})
                session.commit()
                
                logger.info(f"Updated database status for {url} to: {new_status}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to update database status for {url}: {e}")
            return False

    def process_zero_session_updates(self, dry_run: bool = False) -> Dict[str, int]:
        """
        Main function to process zero-session URLs and update their status.
        
        Args:
            dry_run: If True, only log what would be updated without making changes
            
        Returns:
            Dict with counts of processed, successful_airtable, successful_database updates
        """
        logger.info(f"Starting zero-session URL processing (dry_run={dry_run})...")
        
        # Find URLs with zero sessions
        zero_session_urls = self.find_zero_session_urls()
        
        if not zero_session_urls:
            logger.info("No URLs with zero sessions found")
            return {"processed": 0, "successful_airtable": 0, "successful_database": 0}
        
        stats = {
            "processed": len(zero_session_urls),
            "successful_airtable": 0,
            "successful_database": 0
        }
        
        for url_data in zero_session_urls:
            url = url_data['url']
            airtable_id = url_data['airtable_id']
            
            logger.info(f"Processing URL: {url} (Airtable ID: {airtable_id})")
            
            if dry_run:
                logger.info(f"[DRY RUN] Would update {url} to Paused status (database only, not Airtable)")
                continue
            
            # Skip Airtable updates for now - only update database
            # if airtable_id and self.update_airtable_status(airtable_id, "Paused"):
            #     stats["successful_airtable"] += 1
            
            # Update Database only
            if self.update_database_status(url, "Paused"):
                stats["successful_database"] += 1
        
        logger.info(f"Zero-session processing complete: {stats}")
        return stats

    def clear_cache(self, clear_disk_cache: bool = False):
        """
        Clear the BigQuery data cache to force fresh data on next request
        
        Args:
            clear_disk_cache: If True, also delete the disk cache file
        """
        self._bigquery_data_cache = None
        logger.info("BigQuery memory cache cleared")
        
        if clear_disk_cache and os.path.exists(self._cache_file_path):
            try:
                os.remove(self._cache_file_path)
                logger.info("BigQuery disk cache file deleted")
            except Exception as e:
                logger.warning(f"Failed to delete disk cache file: {e}")

# Global service instance
bigquery_integration_service = BigQueryIntegrationService()
