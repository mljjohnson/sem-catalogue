#!/usr/bin/env python3
"""
Create task_logs table for tracking daily scheduled tasks
"""
import sys
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(backend_dir))

from app.models.db import get_session
import sqlalchemy as sa

def create_task_logs_table():
    """Create the task_logs table"""
    print("Creating task_logs table...")
    
    with get_session() as session:
        # Check if table exists
        result = session.execute(sa.text("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_name = 'task_logs'
        """)).scalar()
        
        if result > 0:
            print("✅ task_logs table already exists")
            return
        
        # Create table
        session.execute(sa.text("""
            CREATE TABLE task_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                task_name VARCHAR(255) NOT NULL,
                started_at DATETIME NOT NULL,
                completed_at DATETIME NULL,
                status VARCHAR(50) NOT NULL,
                stats JSON NULL,
                error_message TEXT NULL,
                error_traceback TEXT NULL,
                metadata JSON NULL,
                INDEX idx_task_name (task_name),
                INDEX idx_started_at (started_at),
                INDEX idx_status (status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """))
        session.commit()
        
        print("✅ Created task_logs table")

if __name__ == "__main__":
    create_task_logs_table()

