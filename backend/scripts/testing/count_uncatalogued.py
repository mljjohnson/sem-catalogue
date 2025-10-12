#!/usr/bin/env python3
"""Check how many uncatalogued URLs we have"""
import sys
import os

backend_dir = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.abspath(backend_dir))

from app.models.db import get_session
import sqlalchemy as sa

with get_session() as session:
    # Total uncatalogued
    result = session.execute(sa.text(
        "SELECT COUNT(*) FROM pages_sem_inventory WHERE catalogued = 0"
    )).scalar()
    print(f"Total uncatalogued URLs: {result}")
    
    # Uncatalogued with valid status codes (0 or 200)
    result = session.execute(sa.text(
        "SELECT COUNT(*) FROM pages_sem_inventory WHERE catalogued = 0 AND status_code IN (0, 200)"
    )).scalar()
    print(f"Uncatalogued URLs with status_code 0 or 200: {result}")
    
    # Breakdown by status code
    result = session.execute(sa.text(
        "SELECT status_code, COUNT(*) FROM pages_sem_inventory WHERE catalogued = 0 GROUP BY status_code ORDER BY COUNT(*) DESC"
    )).fetchall()
    print(f"\nBreakdown by status_code:")
    for row in result:
        print(f"  {row[0]}: {row[1]}")

