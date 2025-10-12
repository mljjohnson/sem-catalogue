#!/usr/bin/env python3
"""
Add sessions column to pages_sem_inventory table
"""
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the backend directory to the Python path
backend_dir = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.abspath(backend_dir))

from app.models.db import get_session, engine
import sqlalchemy as sa

def rename_sessions_column():
    """Rename ga_sessions_14d to sessions"""
    
    print("🔧 Renaming ga_sessions_14d to sessions in pages_sem_inventory table...")
    
    try:
        with engine.connect() as conn:
            # Check if ga_sessions_14d exists
            result = conn.execute(sa.text("""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'pages_sem_inventory' 
                AND COLUMN_NAME = 'ga_sessions_14d'
            """))
            
            if result.fetchone():
                # Rename the column
                conn.execute(sa.text("""
                    ALTER TABLE pages_sem_inventory 
                    CHANGE COLUMN ga_sessions_14d sessions INT NULL 
                    COMMENT 'Sessions from BigQuery (NULL = not in BQ)'
                """))
                conn.commit()
                print("   ✅ Renamed ga_sessions_14d to sessions")
                return True
            else:
                # Check if sessions already exists
                result = conn.execute(sa.text("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'pages_sem_inventory' 
                    AND COLUMN_NAME = 'sessions'
                """))
                
                if result.fetchone():
                    print("   ✅ Sessions column already exists")
                    return True
                else:
                    # Add new sessions column
                    conn.execute(sa.text("""
                        ALTER TABLE pages_sem_inventory 
                        ADD COLUMN sessions INT NULL 
                        COMMENT 'Sessions from BigQuery (NULL = not in BQ)'
                    """))
                    conn.commit()
                    print("   ✅ Added new sessions column")
                    return True
            
    except Exception as e:
        print(f"   ❌ Failed to rename/add sessions column: {e}")
        return False

if __name__ == "__main__":
    success = rename_sessions_column()
    if success:
        print("\n✅ Database migration complete!")
    else:
        print("\n❌ Database migration failed!")
