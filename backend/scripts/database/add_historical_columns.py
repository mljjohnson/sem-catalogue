#!/usr/bin/env python3
"""
Add historical tracking columns to the pages_sem_inventory table
"""
import sys
import os
import logging
from sqlalchemy import text

# Add the current directory to Python path to import app modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.models.db import get_session, engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_historical_columns():
    """Add the new historical tracking columns to the database"""
    
    # SQL statements to add the new columns
    alter_statements = [
        "ALTER TABLE pages_sem_inventory ADD COLUMN title VARCHAR(500);",
        "ALTER TABLE pages_sem_inventory ADD COLUMN meta_description TEXT;",
        "ALTER TABLE pages_sem_inventory ADD COLUMN content_summary TEXT;",
        "ALTER TABLE pages_sem_inventory ADD COLUMN promotions JSON;",
        "ALTER TABLE pages_sem_inventory ADD COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP;",
        "ALTER TABLE pages_sem_inventory ADD COLUMN content_last_modified DATETIME;",
        "ALTER TABLE pages_sem_inventory ADD COLUMN processing_trigger VARCHAR(100);"
    ]
    
    logger.info("🔄 Adding historical tracking columns to pages_sem_inventory table...")
    
    with engine.connect() as connection:
        for statement in alter_statements:
            try:
                logger.info(f"Executing: {statement}")
                connection.execute(text(statement))
                logger.info("✅ Column added successfully")
            except Exception as e:
                if "duplicate column name" in str(e).lower() or "already exists" in str(e).lower():
                    logger.info("⏭️  Column already exists, skipping")
                else:
                    logger.error(f"❌ Failed to add column: {e}")
                    raise
        
        # Commit the changes
        connection.commit()
    
    logger.info("✅ All historical columns added successfully!")

def verify_columns():
    """Verify that all columns exist in the table"""
    logger.info("🔍 Verifying column structure...")
    
    with engine.connect() as connection:
        # Get table info (works for both SQLite and MySQL)
        try:
            # Try MySQL DESCRIBE
            result = connection.execute(text("DESCRIBE pages_sem_inventory"))
            columns = [row[0] for row in result]
        except:
            # Fallback to SQLite PRAGMA
            result = connection.execute(text("PRAGMA table_info(pages_sem_inventory)"))
            columns = [row[1] for row in result]
    
    required_columns = [
        'title', 'meta_description', 'content_summary', 'promotions',
        'created_at', 'content_last_modified', 'processing_trigger'
    ]
    
    logger.info(f"Total columns in table: {len(columns)}")
    
    missing_columns = []
    for col in required_columns:
        if col in columns:
            logger.info(f"✅ {col}")
        else:
            logger.error(f"❌ {col} - MISSING")
            missing_columns.append(col)
    
    if missing_columns:
        logger.error(f"Missing columns: {missing_columns}")
        return False
    else:
        logger.info("✅ All required columns are present!")
        return True

if __name__ == "__main__":
    try:
        # First verify current state
        logger.info("Checking current database schema...")
        initial_check = verify_columns()
        
        if initial_check:
            logger.info("✅ All columns already exist - no changes needed!")
        else:
            # Add missing columns
            logger.info("\n" + "="*50)
            add_historical_columns()
            
            # Verify final state
            logger.info("\nVerifying final schema...")
            success = verify_columns()
            
            if success:
                logger.info("🎉 Database schema update completed successfully!")
            else:
                logger.error("❌ Database schema update failed!")
                exit(1)
            
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
