"""
Check which database we're connected to and verify schema
"""
from app.core.config import settings
from app.models.db import get_session
from sqlalchemy import text
import sys

def check_database_connection():
    """Check database connection and schema before running sync"""
    
    print("=== DATABASE CONNECTION CHECK ===")
    print(f"Database URL: {settings.database_url}")
    
    try:
        with get_session() as session:
            # Check which database type we're using
            engine = session.bind
            dialect_name = engine.dialect.name
            
            print(f"Database dialect: {dialect_name}")
            
            if dialect_name == "sqlite":
                print("⚠️  WARNING: Connected to SQLite database")
                # Check if it's the temp dev.db
                if "dev.db" in str(engine.url):
                    print("❌ ERROR: Connected to temporary dev.db SQLite file!")
                    print("   This will not have the page_status column")
                    return False
                    
            elif dialect_name == "mysql":
                print("✅ Connected to MySQL database")
                # Get MySQL connection info
                result = session.execute(text("SELECT DATABASE(), USER(), @@hostname, @@port"))
                db_info = result.fetchone()
                print(f"   Database: {db_info[0]}")
                print(f"   User: {db_info[1]}")
                print(f"   Host: {db_info[2]}")
                print(f"   Port: {db_info[3]}")
                
            else:
                print(f"⚠️  Unexpected database type: {dialect_name}")
            
            # Check if pages_sem_inventory table exists and has required columns
            print(f"\n=== SCHEMA CHECK ===")
            
            if dialect_name == "sqlite":
                result = session.execute(text("PRAGMA table_info(pages_sem_inventory)"))
                columns = [row[1] for row in result.fetchall()]
            else:  # MySQL
                result = session.execute(text("DESCRIBE pages_sem_inventory"))
                columns = [row[0] for row in result.fetchall()]
            
            required_columns = ['page_id', 'url', 'channel', 'team', 'brand', 'page_status']
            
            print(f"Available columns: {len(columns)}")
            for col in sorted(columns):
                if col in required_columns:
                    print(f"   ✅ {col}")
                else:
                    print(f"   - {col}")
            
            missing_columns = [col for col in required_columns if col not in columns]
            if missing_columns:
                print(f"\n❌ MISSING COLUMNS: {missing_columns}")
                return False
            
            # Test a simple query
            result = session.execute(text("SELECT COUNT(*) FROM pages_sem_inventory"))
            count = result.scalar()
            print(f"\nTotal records in database: {count}")
            
            # Check if page_status column has any data
            if 'page_status' in columns:
                result = session.execute(text("SELECT page_status, COUNT(*) FROM pages_sem_inventory WHERE page_status IS NOT NULL GROUP BY page_status LIMIT 5"))
                status_counts = result.fetchall()
                if status_counts:
                    print(f"Page status distribution:")
                    for status, count in status_counts:
                        print(f"   {status}: {count}")
                else:
                    print("Page status column exists but no data yet")
            
            print(f"\n✅ Database connection and schema verified!")
            return True
            
    except Exception as e:
        print(f"\n❌ Database connection failed: {e}")
        return False

if __name__ == "__main__":
    success = check_database_connection()
    if not success:
        print("\n🛑 Database check failed - fix connection before running sync")
        sys.exit(1)
    else:
        print("\n🚀 Database ready for Airtable sync")
