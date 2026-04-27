import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Text, Boolean
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class NotificationChannel(Base):
    """Where alerts get delivered. Mapped onto an Alertmanager `receiver`.

    The provider-specific config (Slack webhook URL, SMTP creds, etc.) is
    Fernet-encrypted as JSON in `encrypted_config` so secrets never sit in
    plaintext on disk. `kind` selects how it's rendered into alertmanager.yml.
    """

    __tablename__ = "notification_channels"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(120), unique=True)
    # slack | webhook | email | tantor_internal
    kind: Mapped[str] = mapped_column(String(40))
    # Fernet-encrypted JSON. Schema depends on `kind`:
    #   slack:           {"webhook_url": "...", "channel": "#alerts" (optional)}
    #   webhook:         {"url": "...", "auth_header": "Bearer ..." (optional)}
    #   email:           {"smtp_host","smtp_port","smtp_user","smtp_password","from","to"}
    #   tantor_internal: {} (always POSTs to local /api/alerts/webhook)
    encrypted_config: Mapped[str] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
