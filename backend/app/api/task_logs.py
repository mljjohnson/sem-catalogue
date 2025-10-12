"""
API endpoints for task execution logs
"""
from fastapi import APIRouter, Query
from typing import Optional, List, Dict, Any
import sqlalchemy as sa
from app.models.db import get_session

router = APIRouter()

@router.get("/task-logs")
def get_task_logs(
    task_name: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0)
) -> Dict[str, Any]:
    """
    Get task execution logs with optional filtering
    """
    with get_session() as session:
        # Build query
        query = """
            SELECT 
                id,
                task_name,
                started_at,
                completed_at,
                status,
                stats,
                error_message,
                error_traceback,
                context,
                TIMESTAMPDIFF(SECOND, started_at, COALESCE(completed_at, NOW())) as duration_seconds
            FROM task_logs
            WHERE 1=1
        """
        params = {}
        
        if task_name:
            query += " AND task_name = :task_name"
            params["task_name"] = task_name
        
        if status:
            query += " AND status = :status"
            params["status"] = status
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM ({query}) as t"
        total = session.execute(sa.text(count_query), params).scalar() or 0
        
        # Get paginated results
        query += " ORDER BY started_at DESC LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset
        
        results = session.execute(sa.text(query), params).fetchall()
    
    # Format results
    logs = []
    for row in results:
        logs.append({
            "id": row[0],
            "task_name": row[1],
            "started_at": row[2].isoformat() if row[2] else None,
            "completed_at": row[3].isoformat() if row[3] else None,
            "status": row[4],
            "stats": row[5],
            "error_message": row[6],
            "error_traceback": row[7],
            "context": row[8],
            "duration_seconds": row[9]
        })
    
    return {
        "logs": logs,
        "total": total,
        "limit": limit,
        "offset": offset
    }

