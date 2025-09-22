#!/usr/bin/env python3
"""
Check specific URL data to debug frontend issue.
"""

from app.models.db import get_session
import sqlalchemy as sa

def check_specific_url():
    """Check the will-writing-v2 URL data."""
    
    with get_session() as session:
        # Look for any URL containing will-writing-v2
        result = session.execute(
            sa.text("SELECT url, primary_category, vertical, channel, team, brand, status_code FROM pages_sem_inventory WHERE url LIKE '%will-writing-v2%'")
        )
        rows = result.fetchall()
        
        print(f"Found {len(rows)} URLs containing 'will-writing-v2':")
        for i, row in enumerate(rows, 1):
            print(f"\n{i}. URL: {row[0]}")
            print(f"   Category: '{row[1]}'")
            print(f"   Vertical: '{row[2]}'")
            print(f"   Channel: '{row[3]}'")
            print(f"   Team: '{row[4]}'")
            print(f"   Brand: '{row[5]}'")
            print(f"   Status: {row[6]}")
        
        # Also check if there are any URLs with NULL categories/verticals
        result = session.execute(
            sa.text("SELECT COUNT(*) FROM pages_sem_inventory WHERE primary_category IS NULL OR vertical IS NULL")
        )
        null_count = result.scalar()
        
        result = session.execute(
            sa.text("SELECT COUNT(*) FROM pages_sem_inventory")
        )
        total_count = result.scalar()
        
        print(f"\n=== Summary ===")
        print(f"Total URLs in DB: {total_count}")
        print(f"URLs with NULL category/vertical: {null_count}")
        print(f"URLs with populated category/vertical: {total_count - null_count}")
        
        # Check a few random URLs that might still have NULL values
        result = session.execute(
            sa.text("SELECT url, primary_category, vertical FROM pages_sem_inventory WHERE primary_category IS NULL OR vertical IS NULL LIMIT 5")
        )
        null_examples = result.fetchall()
        
        if null_examples:
            print(f"\nSample URLs with NULL category/vertical:")
            for row in null_examples:
                print(f"  URL: {row[0]}")
                print(f"  Category: {row[1]}")
                print(f"  Vertical: {row[2]}")
                print()

if __name__ == "__main__":
    check_specific_url()
