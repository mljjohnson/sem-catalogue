#!/usr/bin/env python3
"""
Check what columns actually exist in the pages_sem_inventory table
"""
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the backend directory to the Python path
backend_dir = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.abspath(backend_dir))

from app.models.db import engine
import sqlalchemy as sa

def check_database_schema():
    """Check actual database schema"""
    
    print("🔍 Checking actual database schema...")
    
    try:
        with engine.connect() as conn:
            # Get all columns from pages_sem_inventory table
            result = conn.execute(sa.text("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'pages_sem_inventory' 
                ORDER BY ORDINAL_POSITION
            """))
            
            columns = result.fetchall()
            
            print(f"\n📋 Actual columns in pages_sem_inventory table:")
            print(f"   Total columns: {len(columns)}")
            print()
            
            for i, (col_name, data_type, is_nullable, default_val) in enumerate(columns, 1):
                nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
                default = f" DEFAULT {default_val}" if default_val else ""
                print(f"   {i:2d}. {col_name:<25} {data_type:<15} {nullable}{default}")
            
            return [col[0] for col in columns]
            
    except Exception as e:
        print(f"❌ Failed to check database schema: {e}")
        return []

if __name__ == "__main__":
    columns = check_database_schema()
    if columns:
        print(f"\n✅ Found {len(columns)} columns in the database")
    else:
        print(f"\n❌ Failed to get database schema")



