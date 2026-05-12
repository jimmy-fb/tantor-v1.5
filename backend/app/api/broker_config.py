"""Broker Configuration Management API."""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.user import User
from app.api.deps import require_admin, require_monitor_or_above
from app.services.config_manager import config_manager

router = APIRouter(prefix="/api/broker-config", tags=["broker-config"])


class ConfigUpdateRequest(BaseModel):
    config_key: str
    config_value: str


class BulkConfigRequest(BaseModel):
    """Apply a config change to every broker in a cluster (v1.4.0 #16).

    The customer asked for "one for all" — instead of editing the same
    key on each broker individually, push it to all of them at once.
    """
    config_key: str
    config_value: str


@router.get("/clusters/{cluster_id}/configs")
def get_configs(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    """Get broker configurations for all brokers in a cluster."""
    try:
        return config_manager.get_broker_configs(cluster_id, db)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.put("/clusters/{cluster_id}/brokers/{broker_id}/config")
def update_config(
    cluster_id: str, broker_id: int, req: ConfigUpdateRequest,
    db: Session = Depends(get_db), user: User = Depends(require_admin),
):
    """Update a single config key on a specific broker (also adds the
    key if it doesn't exist yet — v1.4.0 #10)."""
    try:
        return config_manager.update_broker_config(
            cluster_id, broker_id, req.config_key, req.config_value, user.username, db,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/clusters/{cluster_id}/configs")
def add_config_to_broker(
    cluster_id: str, req: ConfigUpdateRequest,
    broker_id: int | None = None,
    db: Session = Depends(get_db), user: User = Depends(require_admin),
):
    """Add a new config key (or update an existing one) on a specific
    broker. Same backing logic as PUT but reachable without already
    knowing the key — v1.4.0 #10.

    If `broker_id` is omitted, the request is rejected with a 422; use
    the `/bulk-config` endpoint for cluster-wide changes.
    """
    if broker_id is None:
        raise HTTPException(status_code=422, detail="broker_id query parameter is required (or use /bulk-config)")
    try:
        return config_manager.update_broker_config(
            cluster_id, broker_id, req.config_key, req.config_value, user.username, db,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/clusters/{cluster_id}/bulk-config")
def bulk_update_config(
    cluster_id: str, req: BulkConfigRequest,
    db: Session = Depends(get_db), user: User = Depends(require_admin),
):
    """Apply a config change to every broker in the cluster (#16).

    Returns a per-broker result list so the UI can highlight any
    brokers that failed without aborting the whole batch.
    """
    try:
        return config_manager.bulk_update_broker_config(
            cluster_id, req.config_key, req.config_value, user.username, db,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/audit/{audit_id}/rollback")
def rollback_config(
    audit_id: str,
    db: Session = Depends(get_db), user: User = Depends(require_admin),
):
    """Rollback a config change."""
    try:
        return config_manager.rollback_config(audit_id, user.username, db)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/clusters/{cluster_id}/audit")
def get_audit(cluster_id: str, limit: int = 50, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    """Get config audit log for a cluster."""
    return config_manager.get_audit_log(cluster_id, db, limit)


@router.get("/metadata")
def get_metadata(_: User = Depends(require_monitor_or_above)):
    """Get Kafka config key metadata."""
    return config_manager.get_config_metadata()
