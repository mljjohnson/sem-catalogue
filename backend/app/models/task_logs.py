"""
Task execution logging for daily scheduled tasks
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime, JSON
from app.models.tables import Base


class TaskLog(Base):
    """Log of scheduled task executions"""
    __tablename__ = "task_logs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_name = Column(String(255), nullable=False, index=True)  # e.g. "airtable_sync", "check_updates", "cataloguing"
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    status = Column(String(50), nullable=False)  # "running", "completed", "failed"
    
    # Summary stats as JSON
    stats = Column(JSON, nullable=True)  # e.g. {"new_urls": 5, "updated_urls": 10, "catalogued": 8}
    
    # Error details if failed
    error_message = Column(Text, nullable=True)
    error_traceback = Column(Text, nullable=True)
    
    # Additional context data
    context = Column(JSON, nullable=True)  # Any other relevant data

