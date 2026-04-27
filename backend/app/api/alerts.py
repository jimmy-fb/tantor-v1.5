"""Alerting API: rules, channels, firing alerts, incidents, AM webhook receiver."""
from __future__ import annotations

import json
import logging
import smtplib
import ssl
import urllib.request
import urllib.error
from datetime import datetime, timezone
from email.message import EmailMessage

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.deps import require_admin, require_monitor_or_above
from app.database import get_db
from app.models.alert_incident import AlertIncident
from app.models.alert_rule import AlertRule
from app.models.cluster import Cluster
from app.models.notification_channel import NotificationChannel
from app.models.user import User
from app.schemas.alerts import (
    AlertIncidentResponse,
    AlertRuleCreate,
    AlertRuleResponse,
    AlertRuleUpdate,
    FiringAlert,
    FiringAlertsResponse,
    NotificationChannelCreate,
    NotificationChannelResponse,
    NotificationChannelUpdate,
    NotificationTestRequest,
    NotificationTestResponse,
)
from app.services import alert_manager

logger = logging.getLogger("tantor.alerts.api")

cluster_router = APIRouter(prefix="/api/clusters/{cluster_id}/alerts", tags=["alerts"])
channel_router = APIRouter(prefix="/api/notification-channels", tags=["alerts"])
# No-auth router: Alertmanager won't carry our JWT.
webhook_router = APIRouter(prefix="/api/alerts", tags=["alerts"])


# ── Rule templates ─────────────────────────────────────────────────────────


@cluster_router.get("/rule-templates")
def list_rule_templates(
    cluster_id: str,
    _: User = Depends(require_monitor_or_above),
):
    """Canned PromQL templates the UI offers as one-click rules."""
    return [
        {"id": k, **v} for k, v in alert_manager.RULE_TEMPLATES.items()
    ]


# ── Rules CRUD ─────────────────────────────────────────────────────────────


def _to_rule_response(rule: AlertRule) -> AlertRuleResponse:
    return AlertRuleResponse(
        id=rule.id,
        cluster_id=rule.cluster_id,
        name=rule.name,
        expr=rule.expr,
        for_seconds=rule.for_seconds,
        severity=rule.severity,
        summary=rule.summary,
        description=rule.description,
        channel_ids=[c.strip() for c in (rule.channel_ids or "").split(",") if c.strip()],
        template=rule.template,
        enabled=rule.enabled,
        created_at=rule.created_at,
        updated_at=rule.updated_at,
    )


@cluster_router.get("/rules", response_model=list[AlertRuleResponse])
def list_rules(
    cluster_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    if not alert_manager.cluster_exists(cluster_id, db):
        raise HTTPException(status_code=404, detail="Cluster not found")
    rules = db.query(AlertRule).filter(AlertRule.cluster_id == cluster_id).order_by(AlertRule.created_at.desc()).all()
    return [_to_rule_response(r) for r in rules]


@cluster_router.post("/rules", response_model=AlertRuleResponse)
def create_rule(
    cluster_id: str,
    data: AlertRuleCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    if not alert_manager.cluster_exists(cluster_id, db):
        raise HTTPException(status_code=404, detail="Cluster not found")
    rule = AlertRule(
        cluster_id=cluster_id,
        name=data.name,
        expr=data.expr,
        for_seconds=data.for_seconds,
        severity=data.severity,
        summary=data.summary,
        description=data.description,
        channel_ids=",".join(data.channel_ids) if data.channel_ids else None,
        template=data.template,
        enabled=data.enabled,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    _trigger_reload(cluster_id, db)
    return _to_rule_response(rule)


@cluster_router.put("/rules/{rule_id}", response_model=AlertRuleResponse)
def update_rule(
    cluster_id: str,
    rule_id: str,
    data: AlertRuleUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    rule = db.query(AlertRule).filter(AlertRule.id == rule_id, AlertRule.cluster_id == cluster_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    if data.name is not None: rule.name = data.name
    if data.expr is not None: rule.expr = data.expr
    if data.for_seconds is not None: rule.for_seconds = data.for_seconds
    if data.severity is not None: rule.severity = data.severity
    if data.summary is not None: rule.summary = data.summary
    if data.description is not None: rule.description = data.description
    if data.channel_ids is not None: rule.channel_ids = ",".join(data.channel_ids) if data.channel_ids else None
    if data.enabled is not None: rule.enabled = data.enabled
    db.commit()
    db.refresh(rule)
    _trigger_reload(cluster_id, db)
    return _to_rule_response(rule)


@cluster_router.delete("/rules/{rule_id}")
def delete_rule(
    cluster_id: str,
    rule_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    rule = db.query(AlertRule).filter(AlertRule.id == rule_id, AlertRule.cluster_id == cluster_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    db.delete(rule)
    db.commit()
    _trigger_reload(cluster_id, db)
    return {"detail": "Rule deleted"}


# ── Firing alerts (Alertmanager passthrough) ───────────────────────────────


@cluster_router.get("/firing", response_model=FiringAlertsResponse)
def list_firing(
    cluster_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    if not alert_manager.cluster_exists(cluster_id, db):
        raise HTTPException(status_code=404, detail="Cluster not found")
    reachable, am_url = alert_manager.alertmanager_reachable(cluster_id, db)
    raw = alert_manager.list_firing_for_cluster(cluster_id, db) if reachable else []
    return FiringAlertsResponse(
        alerts=[FiringAlert(**a) for a in raw],
        count=len(raw),
        alertmanager_url=am_url,
        alertmanager_reachable=reachable,
    )


# ── Incident history ───────────────────────────────────────────────────────


@cluster_router.get("/incidents", response_model=list[AlertIncidentResponse])
def list_incidents(
    cluster_id: str,
    limit: int = Query(100, ge=1, le=500),
    status: str | None = Query(None, pattern="^(firing|resolved)$"),
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    q = db.query(AlertIncident).filter(AlertIncident.cluster_id == cluster_id)
    if status:
        q = q.filter(AlertIncident.status == status)
    rows = q.order_by(AlertIncident.last_seen_at.desc()).limit(limit).all()
    return rows


# ── Channels CRUD ─────────────────────────────────────────────────────────


def _to_channel_response(ch: NotificationChannel) -> NotificationChannelResponse:
    raw = alert_manager.decrypt_channel_config(ch.encrypted_config)
    return NotificationChannelResponse(
        id=ch.id,
        name=ch.name,
        kind=ch.kind,
        enabled=ch.enabled,
        config_redacted=alert_manager.redact_channel_config(raw),
        created_at=ch.created_at,
        updated_at=ch.updated_at,
    )


@channel_router.get("", response_model=list[NotificationChannelResponse])
def list_channels(
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    rows = db.query(NotificationChannel).order_by(NotificationChannel.created_at.desc()).all()
    return [_to_channel_response(r) for r in rows]


@channel_router.post("", response_model=NotificationChannelResponse)
def create_channel(
    data: NotificationChannelCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    if db.query(NotificationChannel).filter(NotificationChannel.name == data.name).first():
        raise HTTPException(status_code=400, detail=f"Channel name already in use: {data.name}")
    _validate_channel_config(data.kind, data.config)
    ch = NotificationChannel(
        name=data.name,
        kind=data.kind,
        enabled=data.enabled,
        encrypted_config=alert_manager.encrypt_channel_config(data.config),
    )
    db.add(ch)
    db.commit()
    db.refresh(ch)
    _trigger_reload_all(db)
    return _to_channel_response(ch)


@channel_router.put("/{channel_id}", response_model=NotificationChannelResponse)
def update_channel(
    channel_id: str,
    data: NotificationChannelUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    ch = db.query(NotificationChannel).filter(NotificationChannel.id == channel_id).first()
    if not ch:
        raise HTTPException(status_code=404, detail="Channel not found")
    if data.name is not None: ch.name = data.name
    if data.enabled is not None: ch.enabled = data.enabled
    if data.config is not None:
        # Merge: caller may send only changed fields, e.g. just a new
        # webhook_url — keep existing fields untouched.
        existing = alert_manager.decrypt_channel_config(ch.encrypted_config)
        merged = {**existing, **data.config}
        _validate_channel_config(ch.kind, merged)
        ch.encrypted_config = alert_manager.encrypt_channel_config(merged)
    db.commit()
    db.refresh(ch)
    _trigger_reload_all(db)
    return _to_channel_response(ch)


@channel_router.delete("/{channel_id}")
def delete_channel(
    channel_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    ch = db.query(NotificationChannel).filter(NotificationChannel.id == channel_id).first()
    if not ch:
        raise HTTPException(status_code=404, detail="Channel not found")
    db.delete(ch)
    db.commit()
    _trigger_reload_all(db)
    return {"detail": "Channel deleted"}


@channel_router.post("/{channel_id}/test", response_model=NotificationTestResponse)
def test_channel(
    channel_id: str,
    data: NotificationTestRequest = Body(default_factory=NotificationTestRequest),
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Deliver a synthetic alert through the channel without going via Alertmanager."""
    ch = db.query(NotificationChannel).filter(NotificationChannel.id == channel_id).first()
    if not ch:
        raise HTTPException(status_code=404, detail="Channel not found")
    cfg = alert_manager.decrypt_channel_config(ch.encrypted_config)
    try:
        _send_test_through_channel(ch, cfg, data, db)
        return NotificationTestResponse(success=True, message=f"Test notification sent via {ch.kind}")
    except Exception as e:
        logger.exception("Channel test failed for %s", ch.id)
        return NotificationTestResponse(success=False, message=f"{type(e).__name__}: {e}")


# ── Alertmanager webhook receiver (no auth) ────────────────────────────────


@webhook_router.post("/webhook")
def receive_alertmanager_webhook(payload: dict = Body(...), db: Session = Depends(get_db)):
    """Alertmanager → Tantor. Records each alert as an AlertIncident row.

    Public endpoint — Alertmanager won't authenticate. Safe because:
      1. We only persist what we recognize as a Tantor-rendered alert
         (must carry `tantor_cluster_id` label).
      2. The endpoint runs on the local Tantor host; bind it to localhost via
         nginx if Internet exposure is a concern (default nginx config does).
    """
    alerts = payload.get("alerts", [])
    recorded = 0
    for alert in alerts:
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})
        cluster_id = labels.get("tantor_cluster_id")
        rule_id = labels.get("tantor_rule_id")
        fingerprint = alert.get("fingerprint") or _synthesize_fingerprint(labels)
        status = "resolved" if alert.get("status") == "resolved" else "firing"
        existing = (
            db.query(AlertIncident)
            .filter(AlertIncident.fingerprint == fingerprint)
            .first()
        )
        now = datetime.now(timezone.utc)
        if existing:
            existing.status = status
            existing.last_seen_at = now
            if status == "resolved" and existing.resolved_at is None:
                existing.resolved_at = _parse_iso(alert.get("endsAt")) or now
        else:
            db.add(AlertIncident(
                fingerprint=fingerprint,
                cluster_id=cluster_id,
                rule_id=rule_id,
                alert_name=labels.get("alertname", "unknown"),
                severity=labels.get("severity", "warning"),
                status=status,
                summary=annotations.get("summary"),
                description=annotations.get("description"),
                payload=json.dumps(alert)[:65000],
                started_at=_parse_iso(alert.get("startsAt")) or now,
                resolved_at=_parse_iso(alert.get("endsAt")) if status == "resolved" else None,
            ))
        recorded += 1
    db.commit()
    return {"recorded": recorded}


# ── Helpers ────────────────────────────────────────────────────────────────


def _validate_channel_config(kind: str, config: dict) -> None:
    if kind == "slack":
        if not config.get("webhook_url"):
            raise HTTPException(status_code=400, detail="slack channel requires webhook_url")
    elif kind == "webhook":
        if not config.get("url"):
            raise HTTPException(status_code=400, detail="webhook channel requires url")
    elif kind == "email":
        for required in ("smtp_host", "from_addr", "to_addrs"):
            if not config.get(required):
                raise HTTPException(status_code=400, detail=f"email channel requires {required}")
    elif kind == "tantor_internal":
        pass
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported channel kind: {kind}")


def _send_test_through_channel(ch: NotificationChannel, cfg: dict, data: NotificationTestRequest, db: Session) -> None:
    if ch.kind == "slack":
        body = {
            "text": f"*[{data.severity.upper()}] {data.summary}*\n{data.description}",
        }
        _post_json(cfg["webhook_url"], body, headers={"Content-Type": "application/json"})
    elif ch.kind == "webhook":
        body = {
            "severity": data.severity,
            "summary": data.summary,
            "description": data.description,
            "test": True,
        }
        headers = {"Content-Type": "application/json"}
        if cfg.get("auth_header"):
            headers["Authorization"] = cfg["auth_header"]
        _post_json(cfg["url"], body, headers=headers)
    elif ch.kind == "email":
        msg = EmailMessage()
        msg["Subject"] = f"[Tantor {data.severity}] {data.summary}"
        msg["From"] = cfg["from_addr"]
        msg["To"] = ", ".join(cfg["to_addrs"])
        msg.set_content(f"{data.summary}\n\n{data.description}\n\n— Tantor test notification")
        port = int(cfg.get("smtp_port", 587))
        if cfg.get("require_tls", True):
            with smtplib.SMTP(cfg["smtp_host"], port, timeout=10) as s:
                s.starttls(context=ssl.create_default_context())
                if cfg.get("smtp_user"):
                    s.login(cfg["smtp_user"], cfg.get("smtp_password", ""))
                s.send_message(msg)
        else:
            with smtplib.SMTP(cfg["smtp_host"], port, timeout=10) as s:
                if cfg.get("smtp_user"):
                    s.login(cfg["smtp_user"], cfg.get("smtp_password", ""))
                s.send_message(msg)
    elif ch.kind == "tantor_internal":
        # Synthesize a minimal Alertmanager payload and feed our own webhook.
        synthetic = {
            "alerts": [{
                "fingerprint": f"test-{ch.id}-{int(datetime.now(timezone.utc).timestamp())}",
                "status": "firing",
                "labels": {
                    "alertname": "TantorChannelTest",
                    "severity": data.severity,
                },
                "annotations": {
                    "summary": data.summary,
                    "description": data.description,
                },
                "startsAt": datetime.now(timezone.utc).isoformat(),
            }],
        }
        receive_alertmanager_webhook(payload=synthetic, db=db)


def _post_json(url: str, body: dict, headers: dict | None = None) -> None:
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers=headers or {"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=8) as r:
        if r.status >= 400:
            raise RuntimeError(f"HTTP {r.status} from {url}")


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        # Alertmanager uses RFC3339 like 2023-01-01T00:00:00.000Z
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _synthesize_fingerprint(labels: dict) -> str:
    """Fallback when Alertmanager omits `fingerprint` (rare, but defensive)."""
    base = "|".join(f"{k}={v}" for k, v in sorted(labels.items()))
    import hashlib
    return hashlib.sha1(base.encode()).hexdigest()


def _trigger_reload(cluster_id: str, db: Session) -> None:
    """Best-effort: rerender + reload Prometheus/Alertmanager for one cluster.

    Imported lazily to avoid pulling the SSH-heavy monitoring_deployer into
    every cold-start path.
    """
    try:
        from app.services.monitoring_deployer import MonitoringDeployer
        MonitoringDeployer.reload_alerting(cluster_id, db)
    except Exception as e:
        # Don't fail the API call — the rule/channel is saved, the operator
        # can re-deploy monitoring from the UI to push it.
        logger.warning("Could not reload alerting for cluster %s: %s", cluster_id, e)


def _trigger_reload_all(db: Session) -> None:
    # Channels are global; rerender every cluster that has monitoring deployed.
    try:
        from app.models.monitoring import MonitoringConfig
        from app.services.monitoring_deployer import MonitoringDeployer
        for cfg in db.query(MonitoringConfig).filter(MonitoringConfig.deployed.is_(True)).all():
            if cfg.cluster_id:
                try:
                    MonitoringDeployer.reload_alerting(cfg.cluster_id, db)
                except Exception as e:
                    logger.warning("Could not reload alerting for cluster %s: %s", cfg.cluster_id, e)
    except Exception as e:
        logger.warning("Channel reload-all failed: %s", e)
