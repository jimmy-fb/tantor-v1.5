import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Text, Boolean, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class AlertRule(Base):
    """A Prometheus alerting rule scoped to a Tantor cluster.

    Tantor renders these into a `prometheus.alert.rules.yml` file that the
    monitoring stack reloads. The PromQL `expr` is the source of truth — UI
    templates produce it but it is stored as text so users can also edit raw.
    """

    __tablename__ = "alert_rules"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    cluster_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("clusters.id", ondelete="CASCADE"), index=True,
    )
    name: Mapped[str] = mapped_column(String(120))
    # PromQL expression that returns a non-empty vector when the rule is firing.
    expr: Mapped[str] = mapped_column(Text)
    # How long the expression must be true before alerting (Prometheus `for` field).
    # Stored as seconds; rendered as `<n>s` / `<n>m` in the rules YAML.
    for_seconds: Mapped[int] = mapped_column(Integer, default=60)
    # critical | warning | info — passed through as a Prometheus label so
    # Alertmanager routing can match on it.
    severity: Mapped[str] = mapped_column(String(20), default="warning")
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Comma-separated NotificationChannel ids — mapped to Alertmanager
    # receivers via routing labels (alertmanager.yml is also rendered by us).
    channel_ids: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Optional template name (broker_down, isr_shrunk, ...) so the UI can
    # show "this is the standard X rule" without recomputing the expression.
    template: Mapped[str | None] = mapped_column(String(40), nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
