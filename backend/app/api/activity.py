"""Cross-cluster activity feed.

Combines `audit_logs` (security/user/ACL actions) with `config_audit_log`
(broker config changes) into a single chronological stream. The cluster-scoped
`/api/clusters/{id}/security/audit-log` endpoint stays as-is for backwards
compatibility.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import require_monitor_or_above
from app.database import get_db
from app.models.audit_log import AuditLog
from app.models.cluster import Cluster
from app.models.config_audit import ConfigAuditLog
from app.models.user import User

router = APIRouter(prefix="/api/activity", tags=["activity"])


class ActivityEntry(BaseModel):
    id: str
    kind: Literal["security", "config"]
    cluster_id: str | None
    cluster_name: str | None
    action: str
    resource: str
    actor: str | None
    details: str | None
    occurred_at: datetime


class ActivityResponse(BaseModel):
    entries: list[ActivityEntry]
    count: int
    has_more: bool


@router.get("", response_model=ActivityResponse)
def list_activity(
    cluster_id: str | None = Query(None, description="Filter to a single cluster"),
    kind: Literal["security", "config"] | None = Query(None, description="Filter to one stream"),
    q: str | None = Query(None, description="Substring match on action / resource / actor"),
    since: datetime | None = Query(None, description="Only entries at or after this timestamp"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    cluster_names = {c.id: c.name for c in db.query(Cluster.id, Cluster.name).all()}

    entries: list[ActivityEntry] = []

    if kind in (None, "security"):
        sec_q = db.query(AuditLog)
        if cluster_id:
            sec_q = sec_q.filter(AuditLog.cluster_id == cluster_id)
        if since:
            sec_q = sec_q.filter(AuditLog.created_at >= since)
        for row in sec_q.all():
            entries.append(
                ActivityEntry(
                    id=row.id,
                    kind="security",
                    cluster_id=row.cluster_id,
                    cluster_name=cluster_names.get(row.cluster_id),
                    action=row.action,
                    resource=f"{row.resource_type}:{row.resource_name}",
                    actor=row.actor_username,  # v1.4.0 #13
                    details=row.details,
                    occurred_at=_to_utc(row.created_at),
                )
            )

    if kind in (None, "config"):
        cfg_q = db.query(ConfigAuditLog)
        if cluster_id:
            cfg_q = cfg_q.filter(ConfigAuditLog.cluster_id == cluster_id)
        if since:
            cfg_q = cfg_q.filter(ConfigAuditLog.created_at >= since)
        for row in cfg_q.all():
            entries.append(
                ActivityEntry(
                    id=row.id,
                    kind="config",
                    cluster_id=row.cluster_id,
                    cluster_name=cluster_names.get(row.cluster_id),
                    action=f"broker_config_{row.change_type}",
                    resource=f"broker:{row.broker_id}/{row.config_key}",
                    actor=row.changed_by,
                    details=f"{row.old_value!r} -> {row.new_value!r}",
                    occurred_at=_to_utc(row.created_at),
                )
            )

    if q:
        needle = q.lower()
        entries = [
            e for e in entries
            if needle in e.action.lower()
            or needle in e.resource.lower()
            or (e.actor and needle in e.actor.lower())
            or (e.details and needle in e.details.lower())
        ]

    entries.sort(key=lambda e: e.occurred_at, reverse=True)
    sliced = entries[offset : offset + limit]
    return ActivityResponse(entries=sliced, count=len(sliced), has_more=len(entries) > offset + limit)


def _to_utc(dt: datetime) -> datetime:
    """SQLite returns naive datetimes; tag them as UTC so JSON serializes with offset."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt
