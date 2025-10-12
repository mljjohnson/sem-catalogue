"""
API endpoint for data source gap analysis
"""
from fastapi import APIRouter
from typing import List, Dict, Any
import sqlalchemy as sa
from app.models.db import get_session

router = APIRouter()

@router.get("/data-gaps")
def get_data_gaps() -> Dict[str, Any]:
    """
    Get gaps between data sources:
    - URLs in Airtable but not in BigQuery
    - URLs in BigQuery but not in Airtable
    """
    with get_session() as session:
        # URLs in Airtable (has airtable_id) but not in BigQuery (sessions is NULL)
        # Exclude carshieldplans.com and gorenewalbyandersen.com
        # Group by normalized URL to show unique URLs and flag duplicates
        at_not_bq = session.execute(sa.text("""
            SELECT 
                url,
                primary_category,
                vertical,
                page_status,
                airtable_id,
                status_code,
                catalogued,
                COUNT(*) as duplicate_count,
                GROUP_CONCAT(DISTINCT page_id) as page_ids
            FROM pages_sem_inventory
            WHERE airtable_id IS NOT NULL 
            AND sessions IS NULL
            AND url NOT LIKE '%carshieldplans.com%'
            AND url NOT LIKE '%gorenewalbyandersen.com%'
            AND catalogued = 1
            GROUP BY TRIM(TRAILING '/' FROM url)
            ORDER BY url
        """)).fetchall()
        
        # URLs in BigQuery (has sessions) but not in Airtable (no airtable_id)
        # Exclude same domains
        bq_not_at = session.execute(sa.text("""
            SELECT 
                url,
                sessions,
                primary_category,
                vertical,
                page_status,
                status_code,
                catalogued,
                COUNT(*) as duplicate_count,
                GROUP_CONCAT(DISTINCT page_id) as page_ids
            FROM pages_sem_inventory
            WHERE sessions IS NOT NULL 
            AND airtable_id IS NULL
            AND url NOT LIKE '%carshieldplans.com%'
            AND url NOT LIKE '%gorenewalbyandersen.com%'
            AND catalogued = 1
            GROUP BY TRIM(TRAILING '/' FROM url)
            ORDER BY sessions DESC
        """)).fetchall()
    
    # Format results
    at_not_bq_list = [
        {
            "url": row[0],
            "primary_category": row[1],
            "vertical": row[2],
            "page_status": row[3],
            "airtable_id": row[4],
            "status_code": row[5],
            "catalogued": bool(row[6]),
            "duplicate_count": row[7],
            "page_ids": row[8]
        }
        for row in at_not_bq
    ]
    
    bq_not_at_list = [
        {
            "url": row[0],
            "sessions": row[1],
            "primary_category": row[2],
            "vertical": row[3],
            "page_status": row[4],
            "status_code": row[5],
            "catalogued": bool(row[6]),
            "duplicate_count": row[7],
            "page_ids": row[8]
        }
        for row in bq_not_at
    ]
    
    return {
        "airtable_not_bigquery": {
            "count": len(at_not_bq_list),
            "urls": at_not_bq_list
        },
        "bigquery_not_airtable": {
            "count": len(bq_not_at_list),
            "urls": bq_not_at_list
        },
        "summary": {
            "total_gaps": len(at_not_bq_list) + len(bq_not_at_list),
            "at_only": len(at_not_bq_list),
            "bq_only": len(bq_not_at_list)
        }
    }

