"""
Check Page Status field values in Airtable
"""
from app.services.airtable import airtable_service

def check_page_status():
    """Check Page Status field values"""
    try:
        if not airtable_service:
            print("Airtable service not configured!")
            return
            
        print("Fetching Airtable records...")
        records = airtable_service.fetch_all_records()
        
        if not records:
            print("No records found!")
            return
        
        print(f"Found {len(records)} total records")
        
        # Look for Page Status field variations
        page_status_fields = []
        sample_record = records[0] if records else {}
        
        for field_name in sample_record.keys():
            if 'status' in field_name.lower() or 'page' in field_name.lower():
                page_status_fields.append(field_name)
        
        print(f"\nPossible Page Status fields found:")
        for field in page_status_fields:
            print(f"  - {field}")
        
        # Check each potential field
        for field_name in page_status_fields:
            print(f"\n=== Field: '{field_name}' ===")
            values = set()
            sample_records = []
            
            for i, record in enumerate(records[:20]):  # Check first 20 records
                value = record.get(field_name)
                if value is not None:
                    values.add(str(value))
                    if len(sample_records) < 5:
                        sample_records.append({
                            'url': record.get('landing_page', 'No URL')[:50],
                            'value': value
                        })
            
            print(f"Unique values: {sorted(list(values))}")
            print(f"Sample records:")
            for sample in sample_records:
                print(f"  URL: {sample['url']}... -> {sample['value']}")
        
        # If no obvious field found, show all fields
        if not page_status_fields:
            print("\nNo obvious Page Status fields found. All available fields:")
            all_fields = set()
            for record in records[:5]:
                all_fields.update(record.keys())
            
            for field in sorted(all_fields):
                print(f"  - {field}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_page_status()
