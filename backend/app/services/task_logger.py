"""
Service for logging scheduled task executions
"""
from datetime import datetime
from typing import Dict, Any, Optional
import traceback as tb
from contextlib import contextmanager

from app.models.db import get_session
from app.models.task_logs import TaskLog
from loguru import logger


@contextmanager
def log_task_execution(task_name: str, context: Optional[Dict[str, Any]] = None):
    """
    Context manager for logging task execution
    
    Usage:
        with log_task_execution("airtable_sync", {"source": "scheduled"}) as task_log:
            # Do work
            result = sync_airtable()
            
            # Update stats
            task_log.update_stats({
                "new_urls": result["new"],
                "updated_urls": result["updated"]
            })
    """
    task_log = TaskLogger(task_name, context)
    task_log.start()
    
    try:
        yield task_log
        task_log.complete()
    except Exception as e:
        task_log.fail(str(e), tb.format_exc())
        raise


class TaskLogger:
    """Helper class for logging task execution"""
    
    def __init__(self, task_name: str, context: Optional[Dict[str, Any]] = None):
        self.task_name = task_name
        self.context = context or {}
        self.stats: Dict[str, Any] = {}
        self.log_id: Optional[int] = None
        self.started_at: Optional[datetime] = None
    
    def start(self):
        """Mark task as started"""
        self.started_at = datetime.utcnow()
        
        with get_session() as session:
            log = TaskLog(
                task_name=self.task_name,
                started_at=self.started_at,
                status="running",
                context=self.context
            )
            session.add(log)
            session.commit()
            self.log_id = log.id
        
        logger.info(f"📋 Task started: {self.task_name} (log_id={self.log_id})")
    
    def update_stats(self, stats: Dict[str, Any]):
        """Update task statistics"""
        import json
        self.stats.update(stats)
        
        if self.log_id:
            with get_session() as session:
                import sqlalchemy as sa
                session.execute(
                    sa.text("UPDATE task_logs SET stats = :stats WHERE id = :id"),
                    {"stats": json.dumps(self.stats), "id": self.log_id}
                )
                session.commit()
    
    def complete(self):
        """Mark task as completed"""
        import json
        completed_at = datetime.utcnow()
        duration = (completed_at - self.started_at).total_seconds() if self.started_at else 0
        
        if self.log_id:
            with get_session() as session:
                import sqlalchemy as sa
                session.execute(
                    sa.text("UPDATE task_logs SET status = 'completed', completed_at = :completed_at, stats = :stats WHERE id = :id"),
                    {
                        "completed_at": completed_at,
                        "stats": json.dumps(self.stats),
                        "id": self.log_id
                    }
                )
                session.commit()
        
        logger.info(f"✅ Task completed: {self.task_name} (duration={duration:.1f}s, stats={self.stats})")
    
    def fail(self, error_message: str, error_traceback: str):
        """Mark task as failed"""
        import json
        completed_at = datetime.utcnow()
        duration = (completed_at - self.started_at).total_seconds() if self.started_at else 0
        
        if self.log_id:
            with get_session() as session:
                import sqlalchemy as sa
                session.execute(
                    sa.text("""UPDATE task_logs 
                       SET status = 'failed', 
                           completed_at = :completed_at, 
                           error_message = :error_message,
                           error_traceback = :error_traceback,
                           stats = :stats
                       WHERE id = :id"""),
                    {
                        "completed_at": completed_at,
                        "error_message": error_message,
                        "error_traceback": error_traceback,
                        "stats": json.dumps(self.stats),
                        "id": self.log_id
                    }
                )
                session.commit()
        
        logger.error(f"❌ Task failed: {self.task_name} (duration={duration:.1f}s, error={error_message})")

