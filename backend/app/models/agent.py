"""Agent — one row per host-side tantor-agent identity.

Created when an admin mints a registration token from the Hosts UI. The
agent's first WS connect consumes the token and switches to a long-lived
JWT (stored in /var/lib/tantor-agent/agent.jwt on the broker host).

This row tracks the AGENT IDENTITY, not its live connection state. Live
state lives in app.services.agent_registry (per-worker in-memory map) and
is computed at request time. See docs/AGENT_PROTOCOL.md.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Boolean, Text, Integer
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Agent(Base):
    __tablename__ = "agents"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    # The host this agent runs on. One agent per host.
    host_id: Mapped[str] = mapped_column(String(36), index=True)

    # First registration token. Mode: stored hashed (bcrypt), single-use.
    # Cleared (set to NULL) once the agent successfully registers and
    # receives its long-lived JWT.
    registration_token_hash: Mapped[str | None] = mapped_column(String(255), nullable=True)
    registration_token_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Per-agent token_version. Bumped on rotate / revoke so existing JWTs
    # become invalid. Mirrors the User.token_version pattern (v1.4.3 #22).
    token_version: Mapped[int] = mapped_column(Integer, default=0)

    # Last things the agent self-reported during register. Convenience
    # fields for the Hosts UI; not authoritative for auth/dispatch.
    agent_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    os_family: Mapped[str | None] = mapped_column(String(64), nullable=True)
    os_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    kernel: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # JSON array of features (e.g. ["systemctl","journalctl","kafka_cli"]).
    features_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Operator-controlled active toggle. When false the registry rejects
    # connection attempts. Useful for retiring a host without deleting
    # the row + its history.
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_registered_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
