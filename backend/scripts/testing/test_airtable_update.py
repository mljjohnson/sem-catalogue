#!/usr/bin/env python3
"""
Test updating page status in Airtable
"""
import sys
import os

backend_dir = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.abspath(backend_dir))

from app.services.airtable import airtable_service

def test_update():
    """Test updating a page status"""
    
    # First, get a record to test with
    print("Fetching Airtable records...")
    records = airtable_service.fetch_all_records()
    
    if not records:
        print("No records found!")
        return
    
    # Find an Active record
    test_record = None
    for record in records:
        if record.get("page_status") == "Active":
            test_record = record
            break
    
    if not test_record:
        print("No Active records found to test with!")
        return
    
    record_id = test_record["airtable_record_id"]
    url = test_record["landing_page"]
    current_status = test_record["page_status"]
    
    print(f"\nTest Record:")
    print(f"  ID: {record_id}")
    print(f"  URL: {url}")
    print(f"  Current Status: {current_status}")
    
    # Update to Paused
    print(f"\n1. Updating status to 'Paused'...")
    success = airtable_service.update_page_status(record_id, "Paused")
    if success:
        print("   ✅ Update successful")
    else:
        print("   ❌ Update failed")
        return
    
    # Verify the change
    print("\n2. Fetching record to verify...")
    records = airtable_service.fetch_all_records()
    updated_record = next((r for r in records if r["airtable_record_id"] == record_id), None)
    if updated_record:
        print(f"   New Status: {updated_record.get('page_status')}")
        if updated_record.get('page_status') == 'Paused':
            print("   ✅ Verification successful")
        else:
            print("   ❌ Status didn't change!")
            return
    
    # Change it back to Active
    print(f"\n3. Changing status back to 'Active'...")
    success = airtable_service.update_page_status(record_id, "Active")
    if success:
        print("   ✅ Update successful")
    else:
        print("   ❌ Update failed")
        return
    
    # Final verification
    print("\n4. Final verification...")
    records = airtable_service.fetch_all_records()
    final_record = next((r for r in records if r["airtable_record_id"] == record_id), None)
    if final_record:
        print(f"   Final Status: {final_record.get('page_status')}")
        if final_record.get('page_status') == 'Active':
            print("   ✅ Test completed successfully!")
        else:
            print("   ❌ Status didn't change back!")

if __name__ == "__main__":
    test_update()

