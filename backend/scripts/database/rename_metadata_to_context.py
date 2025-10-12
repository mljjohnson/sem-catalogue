#!/usr/bin/env python3
"""
Rename metadata column to context in task_logs table
(metadata is reserved in SQLAlchemy)
"""
import sys
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(backend_dir))

from app.models.db import get_session
import sqlalchemy as sa

def rename_column():
    """Rename metadata to context"""
    print("Renaming metadata column to context...")
    
    with get_session() as session:
        try:
            session.execute(sa.text("""
                ALTER TABLE task_logs CHANGE metadata context JSON NULL
            """))
            session.commit()
            print("✅ Column renamed successfully")
        except Exception as e:
            print(f"⚠️  Column already renamed or error: {e}")

if __name__ == "__main__":
    rename_column()

