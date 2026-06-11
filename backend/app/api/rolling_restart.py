"""Rolling Restart API."""
import uuid
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.database import get_db
from app.models.user import User
from app.api.deps import require_admin, require_monitor_or_above
from app.services.rolling_restart_manager import (
    rolling_restart_manager, init_restart_task, get_restart_task,
)
from fastapi import BackgroundTasks

router = APIRouter(prefix="/api/rolling-restart", tags=["rolling-restart"])


class RollingRestartRequest(BaseModel):
    scope: str = "brokers"  # "brokers" | "all" | "controllers"


@router.post("/clusters/{cluster_id}")
def start_rolling_restart(
    cluster_id: str, req: RollingRestartRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db), _: User = Depends(require_admin),
):
    """Start a rolling restart."""
    if req.scope not in ("brokers", "all", "controllers"):
        raise HTTPException(status_code=400, detail="Invalid scope. Use: brokers, all, or controllers")
    task_id = str(uuid.uuid4())
    init_restart_task(task_id)
    background_tasks.add_task(rolling_restart_manager.rolling_restart, cluster_id, task_id, req.scope, db)
    return {"task_id": task_id, "status": "running"}


@router.get("/tasks/{task_id}")
def get_task_status(task_id: str, _: User = Depends(require_monitor_or_above)):
    """Get rolling restart task status."""
    task = get_restart_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.get("/clusters/{cluster_id}/pre-check")
def pre_restart_check(
    cluster_id: str, db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    """Run pre-restart validation checks."""
    try:
        return rolling_restart_manager.get_pre_restart_check(cluster_id, db)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ── External cluster rolling restart ────────────────────────────────────


@router.post("/external/{cluster_id}")
def start_external_rolling_restart(
    cluster_id: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db), _: User = Depends(require_admin),
):
    """Start a rolling restart for an external cluster."""
    task_id = str(uuid.uuid4())
    init_restart_task(task_id)
    background_tasks.add_task(
        rolling_restart_manager.rolling_restart_external, cluster_id, task_id, db
    )
    return {"task_id": task_id, "status": "running"}


@router.get("/external/{cluster_id}/pre-check")
def pre_restart_check_external(
    cluster_id: str, db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    """Run pre-restart validation checks for an external cluster."""
    try:
        return rolling_restart_manager.get_pre_restart_check_external(cluster_id, db)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

