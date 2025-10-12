#!/usr/bin/env python3
"""Delete unwanted URLs from the database"""
import sys
import os

backend_dir = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.abspath(backend_dir))

from app.models.db import get_session
import sqlalchemy as sa

print("🗑️  Deleting unwanted URLs from database...")

with get_session() as session:
    # Delete USAToday/CNN URLs
    result = session.execute(sa.text(
        "DELETE FROM pages_sem_inventory WHERE url LIKE '%usatoday.com%' OR url LIKE '%cnn.com%'"
    ))
    print(f"✅ Deleted {result.rowcount} USAToday/CNN URLs")
    
    # Delete :3020 dev URLs (using REGEXP to avoid bind parameter issue)
    result = session.execute(sa.text(
        "DELETE FROM pages_sem_inventory WHERE url LIKE '%expertise.com:3020%'"
    ))
    print(f"✅ Deleted {result.rowcount} expertise.com:3020 dev URLs")
    
    # Delete builder URLs
    result = session.execute(sa.text(
        "DELETE FROM pages_sem_inventory WHERE url LIKE '%__builder_editing__%'"
    ))
    print(f"✅ Deleted {result.rowcount} builder URLs")
    
    session.commit()
    print("\n✅ Cleanup complete!")

