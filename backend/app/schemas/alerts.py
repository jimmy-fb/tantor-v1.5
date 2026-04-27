"""Pydantic schemas for the alerting feature."""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


# ── Notification channels ──────────────────────────────────────────────────


ChannelKind = Literal["slack", "webhook", "email", "tantor_internal"]


class SlackChannelConfig(BaseModel):
    webhook_url: str
    channel: str | None = None  # override for #channel


class WebhookChannelConfig(BaseModel):
    url: str
    auth_header: str | None = None  # raw value of `Authorization: ...`


class EmailChannelConfig(BaseModel):
    smtp_host: str
    smtp_port: int = 587
    smtp_user: str | None = None
    smtp_password: str | None = None
    from_addr: str
    to_addrs: list[str]
    require_tls: bool = True


class NotificationChannelCreate(BaseModel):
    name: str
    kind: ChannelKind
    enabled: bool = True
    # Free-form to keep validation here simple — alert_manager.py validates
    # by kind when it renders alertmanager.yml.
    config: dict


class NotificationChannelUpdate(BaseModel):
    name: str | None = None
    enabled: bool | None = None
    config: dict | None = None


class NotificationChannelResponse(BaseModel):
    id: str
    name: str
    kind: ChannelKind
    enabled: bool
    # Decrypted config WITH secrets redacted (passwords / webhook URLs masked).
    # Caller can still see structure to confirm what's stored.
    config_redacted: dict
    created_at: datetime
    updated_at: datetime


class NotificationTestRequest(BaseModel):
    severity: Literal["info", "warning", "critical"] = "info"
    summary: str = "Tantor test notification"
    description: str = "If you received this, your channel is wired correctly."


class NotificationTestResponse(BaseModel):
    success: bool
    message: str


# ── Alert rules ────────────────────────────────────────────────────────────


Severity = Literal["info", "warning", "critical"]


class AlertRuleCreate(BaseModel):
    name: str
    expr: str
    for_seconds: int = Field(default=60, ge=0, le=86400)
    severity: Severity = "warning"
    summary: str | None = None
    description: str | None = None
    channel_ids: list[str] = []
    template: str | None = None
    enabled: bool = True


class AlertRuleUpdate(BaseModel):
    name: str | None = None
    expr: str | None = None
    for_seconds: int | None = Field(default=None, ge=0, le=86400)
    severity: Severity | None = None
    summary: str | None = None
    description: str | None = None
    channel_ids: list[str] | None = None
    enabled: bool | None = None


class AlertRuleResponse(BaseModel):
    id: str
    cluster_id: str
    name: str
    expr: str
    for_seconds: int
    severity: Severity
    summary: str | None
    description: str | None
    channel_ids: list[str]
    template: str | None
    enabled: bool
    created_at: datetime
    updated_at: datetime


# ── Firing alerts (Alertmanager passthrough) ───────────────────────────────


class FiringAlert(BaseModel):
    fingerprint: str
    alert_name: str
    severity: Severity
    state: Literal["firing", "pending", "resolved"]
    started_at: datetime | None = None
    ends_at: datetime | None = None
    summary: str | None = None
    description: str | None = None
    labels: dict[str, str] = {}


class FiringAlertsResponse(BaseModel):
    alerts: list[FiringAlert]
    count: int
    alertmanager_url: str | None = None
    alertmanager_reachable: bool


# ── Alert incidents (history) ──────────────────────────────────────────────


class AlertIncidentResponse(BaseModel):
    id: str
    fingerprint: str
    cluster_id: str | None
    rule_id: str | None
    alert_name: str
    severity: Severity
    status: Literal["firing", "resolved"]
    summary: str | None
    description: str | None
    started_at: datetime
    resolved_at: datetime | None
    last_seen_at: datetime
