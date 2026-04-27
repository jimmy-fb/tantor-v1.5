import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Text, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class DeploymentTask(Base):
    """Tracks a Kafka cluster deployment job. Persists past backend restarts so
    the UI can poll progress without losing log history mid-deploy.
    """

    __tablename__ = "deployment_tasks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    cluster_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("clusters.id", ondelete="CASCADE"), index=True,
    )
    # running | completed | completed_with_errors | error
    status: Mapped[str] = mapped_column(String(40), default="running", index=True)
    current_step: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # JSON-encoded list of log lines. Trimmed to keep a row from growing unbounded.
    logs: Mapped[str] = mapped_column(Text, default="[]")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
