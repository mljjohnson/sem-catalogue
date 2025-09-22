#!/usr/bin/env python3
"""
Migrate SQLite database to MySQL
"""

import sqlite3
import pymysql
import os
from pathlib import Path
from app.core.config import settings
import click
from typing import Dict, Any, List


def get_sqlite_connection():
    """Get SQLite connection to current database"""
    # Extract SQLite path from current database_url 
    db_path = Path("data/dev.db")  # Current SQLite location
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found at {db_path}")
    return sqlite3.connect(db_path)


def get_mysql_connection(host="localhost", port=3307, user="root", password="", database="ace_sem"):
    """Get MySQL connection"""
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset='utf8mb4'
    )


def create_mysql_tables(mysql_conn):
    """Create MySQL tables with proper schema"""
    
    with mysql_conn.cursor() as cursor:
        # Create pages_sem_inventory table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pages_sem_inventory (
                page_id VARCHAR(64) PRIMARY KEY,
                url TEXT NOT NULL,
                canonical_url TEXT NOT NULL,
                status_code INT NOT NULL,
                primary_category VARCHAR(255),
                vertical VARCHAR(255),
                template_type VARCHAR(128),
                has_coupons BOOLEAN NOT NULL DEFAULT FALSE,
                has_promotions BOOLEAN NOT NULL DEFAULT FALSE,
                brand_list JSON NOT NULL,
                brand_positions TEXT,
                product_list JSON NOT NULL,
                product_positions TEXT,
                first_seen DATE NOT NULL,
                last_seen DATE NOT NULL,
                ga_sessions_14d INT,
                ga_key_events_14d INT,
                airtable_id VARCHAR(255),
                channel VARCHAR(255),
                team VARCHAR(255),
                brand VARCHAR(255),
                catalogued INT NOT NULL DEFAULT 0,
                INDEX idx_status_code (status_code),
                INDEX idx_catalogued (catalogued),
                INDEX idx_primary_category (primary_category),
                INDEX idx_vertical (vertical),
                INDEX idx_first_seen (first_seen),
                INDEX idx_last_seen (last_seen)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Create page_brands table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS page_brands (
                id INT AUTO_INCREMENT PRIMARY KEY,
                page_id VARCHAR(64) NOT NULL,
                brand_slug VARCHAR(255) NOT NULL,
                brand_name VARCHAR(255) NOT NULL,
                position VARCHAR(8),
                module_type VARCHAR(64),
                INDEX idx_page_id (page_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Create page_products table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS page_products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                page_id VARCHAR(64) NOT NULL,
                product_name VARCHAR(255) NOT NULL,
                position VARCHAR(8),
                module_type VARCHAR(64),
                INDEX idx_page_id (page_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Create page_ai_extracts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS page_ai_extracts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                page_id VARCHAR(64) NOT NULL,
                url TEXT NOT NULL,
                created_at VARCHAR(32) NOT NULL,
                html_bytes INT NOT NULL DEFAULT 0,
                screenshot_bytes INT NOT NULL DEFAULT 0,
                data JSON NOT NULL,
                INDEX idx_page_id (page_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
    mysql_conn.commit()
    print("✓ MySQL tables created")


def migrate_table_data(sqlite_conn, mysql_conn, table_name: str):
    """Migrate data from SQLite table to MySQL table"""
    
    # Get data from SQLite
    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.execute(f"SELECT * FROM {table_name}")
    columns = [description[0] for description in sqlite_cursor.description]
    rows = sqlite_cursor.fetchall()
    
    if not rows:
        print(f"✓ No data to migrate for {table_name}")
        return
    
    # Insert into MySQL
    with mysql_conn.cursor() as mysql_cursor:
        # Clear existing data
        mysql_cursor.execute(f"DELETE FROM {table_name}")
        
        # Prepare INSERT statement
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join(columns)
        insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        # Insert all rows
        mysql_cursor.executemany(insert_sql, rows)
        
    mysql_conn.commit()
    print(f"✓ Migrated {len(rows)} rows from {table_name}")


@click.command()
@click.option("--mysql-host", default="localhost", help="MySQL host")
@click.option("--mysql-port", default=3307, help="MySQL port")
@click.option("--mysql-user", default="root", help="MySQL username")
@click.option("--mysql-password", default="", help="MySQL password")
@click.option("--mysql-database", default="ace_sem", help="MySQL database name")
@click.option("--dry-run", is_flag=True, help="Show what would be migrated without doing it")
def main(mysql_host: str, mysql_port: int, mysql_user: str, mysql_password: str, mysql_database: str, dry_run: bool):
    """Migrate SQLite database to MySQL"""
    
    print("🔄 Starting SQLite to MySQL migration...")
    
    if dry_run:
        print("DRY RUN MODE - No changes will be made")
        
    try:
        # Connect to SQLite
        print("📂 Connecting to SQLite database...")
        sqlite_conn = get_sqlite_connection()
        
        # Get table info
        cursor = sqlite_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"📋 Found tables: {', '.join(tables)}")
        
        if dry_run:
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  • {table}: {count} rows")
            return
            
        # Connect to MySQL
        print(f"🐬 Connecting to MySQL database at {mysql_host}:{mysql_port}...")
        mysql_conn = get_mysql_connection(mysql_host, mysql_port, mysql_user, mysql_password, mysql_database)
        
        # Create tables
        print("🏗️  Creating MySQL tables...")
        create_mysql_tables(mysql_conn)
        
        # Migrate data
        print("📊 Migrating data...")
        for table in tables:
            migrate_table_data(sqlite_conn, mysql_conn, table)
            
        # Close connections
        sqlite_conn.close()
        mysql_conn.close()
        
        print("✅ Migration completed successfully!")
        print(f"🎯 Update your DATABASE_URL to: mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        raise


if __name__ == "__main__":
    main()
