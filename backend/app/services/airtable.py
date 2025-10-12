"""Airtable integration service for syncing URLs and metadata."""

from typing import List, Dict, Any, Optional
from pyairtable import Table
from loguru import logger

from app.core.config import settings


class AirtableService:
    """Service for interacting with Airtable API."""
    
    def __init__(self):
        if not settings.airtable_pat:
            raise ValueError("AIRTABLE_PAT environment variable is required")
        
        self.table = Table(settings.airtable_pat, settings.airtable_base_id, settings.airtable_table_id)
        self.view_id = settings.airtable_view_id
        
        # Field mappings from Airtable field names to our column names
        self.field_mappings = {
            "DE: Landing Page": "landing_page",       # Landing Page URL
            "DE: Channel": "channel",                 # DE Channel
            "DE: Team": "team",                       # DE Team
            "DE: Brand: ": "brand",                   # DE Brand (note the trailing space)
            "DE: Vertical": "vertical",               # DE Vertical
            "DE: Category": "primary_category",       # DE Category
            "Page Status": "page_status",             # Page Status (Active/Inactive)
        }
    
    def fetch_all_records(self) -> List[Dict[str, Any]]:
        """
        Fetch all records from the Airtable view.
        
        Returns:
            List of records with mapped field names
        """
        try:
            logger.info(f"Fetching records from Airtable base {settings.airtable_base_id}, table {settings.airtable_table_id}")
            
            # Fetch all records from the specific view
            records = self.table.all(view=self.view_id)
            
            logger.info(f"Retrieved {len(records)} records from Airtable")
            
            # Transform records to use our field names
            transformed_records = []
            excluded_domains = ['usatoday.com', 'cnn.com']
            
            for record in records:
                transformed = self._transform_record(record)
                landing_page = transformed.get("landing_page")
                
                if landing_page:
                    # Filter out unwanted domains
                    if any(domain in landing_page.lower() for domain in excluded_domains):
                        continue
                    
                    transformed_records.append(transformed)
            
            logger.info(f"Processed {len(transformed_records)} valid records with URLs (filtered out USA Today and CNN)")
            return transformed_records
            
        except Exception as e:
            logger.error(f"Error fetching Airtable records: {e}")
            raise
    
    def _transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform an Airtable record to use our field names.
        
        Args:
            record: Raw Airtable record
            
        Returns:
            Transformed record with mapped field names
        """
        transformed = {
            "airtable_record_id": record["id"],  # Airtable's internal record ID
        }
        
        fields = record.get("fields", {})
        
        # Map each field using our field mappings
        for field_id, our_field_name in self.field_mappings.items():
            value = fields.get(field_id)
            if value is not None:
                # Handle different field types
                if isinstance(value, list) and len(value) == 1:
                    # Single-select fields come as lists
                    transformed[our_field_name] = value[0]
                elif isinstance(value, str):
                    transformed[our_field_name] = value.strip()
                else:
                    transformed[our_field_name] = value
        
        return transformed
    
    def get_url_list(self) -> List[str]:
        """
        Get a simple list of all URLs from Airtable.
        
        Returns:
            List of URLs
        """
        records = self.fetch_all_records()
        urls = []
        
        for record in records:
            url = record.get("landing_page")
            if url:
                urls.append(url)
        
        return urls
    
    def update_page_status(self, record_id: str, new_status: str) -> bool:
        """
        Update the Page Status field for a specific Airtable record.
        
        Args:
            record_id: Airtable record ID
            new_status: New status value (e.g., 'Active', 'Paused')
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Updating Airtable record {record_id} to status '{new_status}'")
            
            # Update the Page Status field
            self.table.update(record_id, {"Page Status": new_status})
            
            logger.info(f"Successfully updated record {record_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating Airtable record {record_id}: {e}")
            return False


# Global service instance
airtable_service = AirtableService() if settings.airtable_pat else None
