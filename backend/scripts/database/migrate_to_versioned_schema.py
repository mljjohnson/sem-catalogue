#!/usr/bin/env python3
"""
Migrate to versioned schema - allow multiple records per URL
"""
import sys
import os

# Add the backend directory to Python path
backend_dir = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.abspath(backend_dir))

from app.models.db import get_session
import sqlalchemy as sa

def migrate_to_versioned_schema():
    print("🔄 Migrating to versioned schema...\n")
    
    with get_session() as session:
        # Step 1: Check current schema
        print("Step 1: Checking current schema...")
        result = session.execute(sa.text("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'pages_sem_inventory' 
            AND COLUMN_NAME = 'id'
        """)).fetchone()
        
        if result:
            print("❌ 'id' column already exists! Migration may have already run.")
            confirm = input("Continue anyway? (yes/no): ")
            if confirm.lower() != 'yes':
                return
        
        print("✅ Current schema verified\n")
        
        # Step 2: Create backup table
        print("Step 2: Creating backup table...")
        session.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS pages_sem_inventory_backup 
            AS SELECT * FROM pages_sem_inventory
        """))
        session.commit()
        
        backup_count = session.execute(sa.text("""
            SELECT COUNT(*) FROM pages_sem_inventory_backup
        """)).fetchone()[0]
        print(f"✅ Backed up {backup_count} records\n")
        
        # Step 3: Add new id column
        print("Step 3: Adding new 'id' column as auto-increment...")
        session.execute(sa.text("""
            ALTER TABLE pages_sem_inventory 
            ADD COLUMN id INT AUTO_INCREMENT FIRST,
            DROP PRIMARY KEY,
            ADD PRIMARY KEY (id),
            ADD INDEX idx_page_id (page_id),
            ADD INDEX idx_url_last_seen (url(255), last_seen)
        """))
        session.commit()
        print("✅ Added 'id' column and updated indexes\n")
        
        # Step 4: Verify migration
        print("Step 4: Verifying migration...")
        result = session.execute(sa.text("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT page_id) as unique_urls,
                COUNT(DISTINCT id) as unique_ids
            FROM pages_sem_inventory
        """)).fetchone()
        
        print(f"✅ Migration verified:")
        print(f"   Total records: {result[0]}")
        print(f"   Unique URLs (page_id): {result[1]}")
        print(f"   Unique IDs: {result[2]}")
        
        if result[0] != result[2]:
            print("\n❌ ERROR: Record count doesn't match unique IDs!")
            return
        
        print("\n✅ Migration complete!")
        print("\n📋 Next steps:")
        print("   1. Update SQLAlchemy model to use 'id' as primary key")
        print("   2. Update queries to filter by latest 'last_seen' for each URL")
        print("   3. Test the application")
        print("   4. If everything works, drop backup: DROP TABLE pages_sem_inventory_backup")

if __name__ == "__main__":
    migrate_to_versioned_schema()


