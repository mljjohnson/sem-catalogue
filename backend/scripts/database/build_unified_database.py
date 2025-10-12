#!/usr/bin/env python3
"""
Build unified database with ALL URLs from AT + BQ + existing sources
"""
import os
import sys
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the backend directory to the Python path
backend_dir = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.abspath(backend_dir))

from app.models.db import get_session
from app.models.tables import PageSEMInventory
import sqlalchemy as sa

def normalize_url_consistent(url):
    """Consistent URL normalization"""
    if not url:
        return ""
    
    try:
        from urllib.parse import urlparse
        
        url_str = str(url).strip()
        if not url_str:
            return ""
        
        if url_str.startswith(('http://', 'https://')):
            parsed = urlparse(url_str.lower())
            path = parsed.path
        else:
            path = url_str.lower()
        
        if '?' in path:
            path = path.split('?')[0]
        if '#' in path:
            path = path.split('#')[0]
        
        path = path.rstrip('/')
        return path
        
    except Exception as e:
        print(f"Warning: Failed to normalize URL {url}: {e}")
        return str(url).lower().strip() if url else ""

def load_all_source_data():
    """Load data from all sources"""
    
    print("📊 Loading data from all sources...")
    
    # 1. Load BigQuery data
    print("\n1. Loading BigQuery data...")
    with open("bigquery_cache.json", 'r') as f:
        bq_raw = json.load(f)
    
    bq_lookup = {}  # normalized_url -> session_count
    bq_records = {}  # normalized_url -> full_bq_record
    
    for record in bq_raw:
        visit_page = record.get('visit_page', '')  # This is just a path like "/l/best-loans"
        normalized = normalize_url_consistent(visit_page)
        sessions = record.get('sessions', 0)
        
        if normalized:
            # Keep highest session count if duplicates
            if normalized in bq_lookup:
                if sessions > bq_lookup[normalized]:
                    bq_lookup[normalized] = sessions
                    bq_records[normalized] = record  # Store full record for property info
            else:
                bq_lookup[normalized] = sessions
                bq_records[normalized] = record  # Store full record for property info
    
    print(f"   BigQuery unique URLs: {len(bq_lookup)}")
    
    # 2. Load Airtable data
    print("\n2. Loading Airtable data...")
    with open("airtable_cache.json", 'r') as f:
        airtable_raw = json.load(f)
    
    airtable_lookup = {}  # normalized_url -> airtable_data
    
    for record in airtable_raw:
        original_url = record.get('landing_page')
        if original_url:
            normalized = normalize_url_consistent(original_url)
            if normalized:
                airtable_lookup[normalized] = {
                    'original_url': original_url,
                    'airtable_id': record.get('airtable_record_id'),
                    'page_status': record.get('page_status'),
                    'primary_category': record.get('primary_category'),
                    'vertical': record.get('vertical'),
                    'channel': record.get('channel'),
                    'team': record.get('team'),
                    'brand': record.get('brand')
                }
    
    print(f"   Airtable unique URLs: {len(airtable_lookup)}")
    
    # 3. Load existing Database data (using actual database columns)
    print("\n3. Loading existing Database data...")
    with get_session() as session:
        # Query using raw SQL to avoid SQLAlchemy model issues
        existing_results = session.execute(sa.text("""
            SELECT page_id, url, airtable_id, page_status, primary_category, vertical, 
                   sessions, catalogued, status_code, template_type, has_coupons, has_promotions
            FROM pages_sem_inventory
        """)).fetchall()
    
    existing_lookup = {}  # normalized_url -> database_record
    
    for record in existing_results:
        normalized = normalize_url_consistent(record.url)
        if normalized:
            existing_lookup[normalized] = record
    
    print(f"   Existing Database URLs: {len(existing_lookup)}")
    
    return bq_lookup, bq_records, airtable_lookup, existing_lookup

def build_unified_records():
    """Build unified records for the database"""
    
    bq_lookup, bq_records, airtable_lookup, existing_lookup = load_all_source_data()
    
    # Get all unique normalized URLs from all sources
    all_normalized_urls = set(bq_lookup.keys()) | set(airtable_lookup.keys()) | set(existing_lookup.keys())
    
    print(f"\n📊 UNIFIED ANALYSIS:")
    print(f"   Total unique URLs across all sources: {len(all_normalized_urls)}")
    print(f"   In BigQuery: {len(bq_lookup)}")
    print(f"   In Airtable: {len(airtable_lookup)}")
    print(f"   In Database: {len(existing_lookup)}")
    
    # Count overlaps
    in_bq_and_at = len(set(bq_lookup.keys()) & set(airtable_lookup.keys()))
    in_bq_not_at = len(set(bq_lookup.keys()) - set(airtable_lookup.keys()))
    in_at_not_bq = len(set(airtable_lookup.keys()) - set(bq_lookup.keys()))
    
    print(f"   In BQ AND AT: {in_bq_and_at}")
    print(f"   In BQ but NOT AT: {in_bq_not_at}")
    print(f"   In AT but NOT BQ: {in_at_not_bq}")
    
    # Build unified records
    unified_records = []
    
    for normalized_url in all_normalized_urls:
        # Determine the best original URL to use
        # Priority: Airtable (full URL) > Existing DB (full URL) > BQ (path only)
        original_url = None
        if normalized_url in airtable_lookup:
            # Airtable has full URLs - use this, but strip trailing slash
            original_url = airtable_lookup[normalized_url]['original_url'].rstrip('/')
        elif normalized_url in existing_lookup:
            # Existing DB has full URLs - use this, but strip trailing slash
            original_url = existing_lookup[normalized_url].url.rstrip('/')
        elif normalized_url in bq_records:
            # BQ only has paths - construct full URL using property field
            bq_record = bq_records[normalized_url]
            bq_path = bq_record.get('visit_page', '')
            property_name = bq_record.get('property', '')
            
            # Map property to correct domain and strip trailing slash
            if property_name == 'advisor':
                original_url = f"https://www.forbes.com{bq_path}".rstrip('/')
            elif property_name == 'health':
                original_url = f"https://www.forbes.com{bq_path}".rstrip('/')
            elif property_name == 'home':
                original_url = f"https://www.forbes.com{bq_path}".rstrip('/')
            elif property_name == 'betting':
                original_url = f"https://www.forbes.com{bq_path}".rstrip('/')
            elif property_name == 'expertise':
                original_url = f"https://www.expertise.com{bq_path}".rstrip('/')
            elif property_name == 'dollargeek':
                original_url = f"https://www.dollargeek.com{bq_path}".rstrip('/')
            else:
                # Fallback to forbes.com for unknown properties
                original_url = f"https://www.forbes.com{bq_path}".rstrip('/')
                print(f"   Warning: Unknown property '{property_name}' for {bq_path}, defaulting to forbes.com")
        else:
            # Shouldn't happen, but fallback
            original_url = f"https://www.forbes.com{normalized_url}"
        
        # Get Airtable data if available
        airtable_data = airtable_lookup.get(normalized_url, {})
        
        # Get BigQuery sessions (NULL if not in BQ)
        bq_sessions = bq_lookup.get(normalized_url)  # None if not in BQ
        
        # Get existing database record if available
        existing_record = existing_lookup.get(normalized_url)
        
        # Determine source flags
        in_bigquery = normalized_url in bq_lookup
        in_airtable = normalized_url in airtable_lookup
        
        # Create source description
        sources = []
        if in_airtable:
            sources.append("AT")
        if in_bigquery:
            sources.append("BQ")
        if existing_record and not in_airtable and not in_bigquery:
            sources.append("DB")
        
        source_description = "+".join(sources) if sources else "UNKNOWN"
        
        # Build the unified record
        record = {
            'normalized_url': normalized_url,
            'original_url': original_url,
            'bq_sessions': bq_sessions,
            'in_bigquery': in_bigquery,
            'in_airtable': in_airtable,
            'source_description': source_description,
            
            # Airtable fields (blank if not in AT)
            'airtable_id': airtable_data.get('airtable_id'),
            'page_status': airtable_data.get('page_status'),
            'primary_category': airtable_data.get('primary_category'),
            'vertical': airtable_data.get('vertical'),
            'channel': airtable_data.get('channel'),
            'team': airtable_data.get('team'),
            'brand': airtable_data.get('brand'),
            
            # Existing database fields (if available)
            'existing_page_id': existing_record.page_id if existing_record else None,
            'existing_status_code': existing_record.status_code if existing_record else None,
            'existing_catalogued': existing_record.catalogued if existing_record else None,
        }
        
        unified_records.append(record)
    
    return unified_records

def update_database(unified_records):
    """Update database with unified records using raw SQL"""
    
    print(f"\n💾 Updating database with {len(unified_records)} unified records...")
    
    updated_count = 0
    created_count = 0
    
    with get_session() as session:
        # Get the starting max ID once at the beginning
        max_id_result = session.execute(sa.text("""
            SELECT COALESCE(MAX(CAST(SUBSTRING(page_id, 9) AS UNSIGNED)), 0) as max_id
            FROM pages_sem_inventory 
            WHERE page_id LIKE 'unified_%'
        """)).fetchone()
        
        next_id_counter = (max_id_result[0] if max_id_result else 0) + 1
        
        for record in unified_records:
            original_url = record['original_url']
            
            # Check if record exists in database using raw SQL
            existing_result = session.execute(sa.text("""
                SELECT page_id FROM pages_sem_inventory WHERE url = :url LIMIT 1
            """), {"url": original_url}).fetchone()
            
            if existing_result:
                # Update existing record using raw SQL
                session.execute(sa.text("""
                    UPDATE pages_sem_inventory 
                    SET airtable_id = :airtable_id,
                        page_status = :page_status,
                        primary_category = :primary_category,
                        vertical = :vertical,
                        channel = :channel,
                        team = :team,
                        brand = :brand,
                        sessions = :sessions
                    WHERE url = :url
                """), {
                    "airtable_id": record['airtable_id'],
                    "page_status": record['page_status'],
                    "primary_category": record['primary_category'],
                    "vertical": record['vertical'],
                    "channel": record['channel'],
                    "team": record['team'],
                    "brand": record['brand'],
                    "sessions": record['bq_sessions'],
                    "url": original_url
                })
                
                updated_count += 1
                
            else:
                # Create new record using raw SQL
                # Use the local counter and increment it
                page_id = f"unified_{next_id_counter:06}"
                next_id_counter += 1
                # Only mark as catalogued if it has actual catalogued data
                # Not just because it exists in Airtable
                catalogued = 0
                
                session.execute(sa.text("""
                    INSERT INTO pages_sem_inventory 
                    (page_id, url, canonical_url, status_code, primary_category, vertical, 
                     template_type, has_coupons, has_promotions, brand_list, brand_positions,
                     product_list, product_positions, first_seen, last_seen, sessions,
                     airtable_id, channel, team, brand, catalogued, page_status)
                    VALUES 
                    (:page_id, :url, :canonical_url, :status_code, :primary_category, :vertical,
                     :template_type, :has_coupons, :has_promotions, :brand_list, :brand_positions,
                     :product_list, :product_positions, :first_seen, :last_seen, :sessions,
                     :airtable_id, :channel, :team, :brand, :catalogued, :page_status)
                """), {
                    "page_id": page_id,
                    "url": original_url,
                    "canonical_url": original_url,
                    "status_code": 200,
                    "primary_category": record['primary_category'],
                    "vertical": record['vertical'],
                    "template_type": None,
                    "has_coupons": False,
                    "has_promotions": False,
                    "brand_list": "[]",  # Empty JSON array
                    "brand_positions": None,
                    "product_list": "[]",  # Empty JSON array
                    "product_positions": None,
                    "first_seen": datetime.now().date(),
                    "last_seen": datetime.now().date(),
                    "sessions": record['bq_sessions'],
                    "airtable_id": record['airtable_id'],
                    "channel": record['channel'],
                    "team": record['team'],
                    "brand": record['brand'],
                    "catalogued": catalogued,
                    "page_status": record['page_status']
                })
                
                created_count += 1
        
        # Commit all changes
        session.commit()
    
    print(f"   Updated existing records: {updated_count}")
    print(f"   Created new records: {created_count}")
    
    return updated_count, created_count

def export_analysis(unified_records):
    """Export analysis of the unified data"""
    
    print(f"\n📊 Exporting analysis...")
    
    # Create DataFrame for analysis
    df = pd.DataFrame(unified_records)
    
    # Add analysis columns
    df['has_sessions'] = df['bq_sessions'].notna()
    df['session_count'] = df['bq_sessions'].fillna(0)
    
    # Sort by sessions (highest first), then by source
    df = df.sort_values(['session_count', 'source_description'], ascending=[False, True])
    
    # Export main analysis
    df.to_csv("unified_database_analysis.csv", index=False)
    print(f"   Exported unified_database_analysis.csv")
    
    # Summary stats
    print(f"\n📈 SUMMARY STATS:")
    print(f"   Total URLs: {len(df)}")
    print(f"   In BigQuery: {df['in_bigquery'].sum()}")
    print(f"   In Airtable: {df['in_airtable'].sum()}")
    print(f"   With sessions: {df['has_sessions'].sum()}")
    print(f"   Total sessions: {df['session_count'].sum():,}")
    
    # Source breakdown
    print(f"\n📋 SOURCE BREAKDOWN:")
    source_counts = df['source_description'].value_counts()
    for source, count in source_counts.items():
        print(f"   {source}: {count}")
    
    return df

def main():
    """Main function"""
    
    print("🏗️  Building Unified Database")
    print("=" * 50)
    
    # Build unified records
    unified_records = build_unified_records()
    
    # Export analysis before updating
    analysis_df = export_analysis(unified_records)
    
    # Ask for confirmation
    print(f"\n⚠️  Ready to update database with {len(unified_records)} records.")
    print(f"   This will update existing records and create new ones.")
    
    confirm = input("\nProceed with database update? (y/N): ").lower().strip()
    
    if confirm == 'y':
        # Update database
        updated, created = update_database(unified_records)
        
        print(f"\n✅ Database update complete!")
        print(f"   Updated: {updated} records")
        print(f"   Created: {created} records")
        print(f"   Total: {updated + created} records processed")
        
    else:
        print(f"\n❌ Database update cancelled.")
        print(f"   Analysis exported to unified_database_analysis.csv")

if __name__ == "__main__":
    main()
