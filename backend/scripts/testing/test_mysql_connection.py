#!/usr/bin/env python3
"""
Test MySQL connection using environment variables
"""
import os
import sys
import logging
from dotenv import load_dotenv
import pymysql

# Add the backend directory to the Python path
backend_dir = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.abspath(backend_dir))

# Load environment variables
load_dotenv(os.path.join(backend_dir, '.env'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_mysql_connection():
    """Test MySQL connection using environment variables"""
    logger.info("=== Testing MySQL Connection ===")
    
    # Get connection details from environment
    mysql_host = os.getenv('MYSQL_HOST')
    mysql_port = int(os.getenv('MYSQL_PORT', 3306))
    mysql_user = os.getenv('MYSQL_USER')
    mysql_password = os.getenv('MYSQL_PASSWORD')
    mysql_database = os.getenv('MYSQL_DATABASE')
    
    logger.info(f"Connection details:")
    logger.info(f"  Host: {mysql_host}")
    logger.info(f"  Port: {mysql_port}")
    logger.info(f"  User: {mysql_user}")
    logger.info(f"  Database: {mysql_database}")
    logger.info(f"  Password: {'*' * len(mysql_password) if mysql_password else 'None'}")
    
    if not all([mysql_host, mysql_user, mysql_password, mysql_database]):
        logger.error("Missing required MySQL environment variables!")
        logger.error("Required: MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE")
        return False
    
    try:
        logger.info("Attempting to connect...")
        
        # Create connection
        connection = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        logger.info("✅ Connection successful!")
        
        # Test basic query
        with connection.cursor() as cursor:
            cursor.execute("SELECT VERSION() as version")
            result = cursor.fetchone()
            logger.info(f"MySQL Version: {result['version']}")
            
            # List tables
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            logger.info(f"Tables in database: {len(tables)}")
            
            if tables:
                logger.info("Available tables:")
                for table in tables:
                    table_name = list(table.values())[0]
                    logger.info(f"  - {table_name}")
            else:
                logger.info("No tables found in database")
        
        connection.close()
        logger.info("Connection closed successfully")
        return True
        
    except pymysql.Error as e:
        logger.error(f"❌ MySQL Error: {e}")
        logger.error(f"Error Code: {e.args[0] if e.args else 'Unknown'}")
        logger.error(f"Error Message: {e.args[1] if len(e.args) > 1 else 'Unknown'}")
        return False
        
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = test_mysql_connection()
    
    if success:
        print("\n✅ MySQL connection test PASSED")
        sys.exit(0)
    else:
        print("\n❌ MySQL connection test FAILED")
        sys.exit(1)



