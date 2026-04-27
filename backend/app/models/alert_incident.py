import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Text, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class AlertIncident(Base):
    """One firing/resolution pair recorded from Alertmanager's webhook.

    Tantor stores these so the UI can show alert history even after the
    incident clears in Alertmanager (which only keeps recent activity).
    """

    __tablename__ = "alert_incidents"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    # Alertmanager-supplied stable identity for the firing instance — lets us
    # update the same row when the alert resolves rather than appending a new one.
    fingerprint: Mapped[str] = mapped_column(String(64), index=True)
    cluster_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    # If we can match the alert label `alertname` back to a stored rule, link it.
    rule_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("alert_rules.id", ondelete="SET NULL"), nullable=True,
    )
    alert_name: Mapped[str] = mapped_column(String(120))
    severity: Mapped[str] = mapped_column(String(20), default="warning")
    # firing | resolved
    status: Mapped[str] = mapped_column(String(20), default="firing")
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Raw JSON of the Alertmanager alert payload so we can replay later.
    payload: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_alert_incidents_status_started", "status", "started_at"),
    )
