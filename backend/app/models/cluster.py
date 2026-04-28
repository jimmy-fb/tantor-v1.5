import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Text, Boolean
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Cluster(Base):
    __tablename__ = "clusters"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(255))
    kafka_version: Mapped[str] = mapped_column(String(20))
    mode: Mapped[str] = mapped_column(String(20))  # "kraft" or "zookeeper"
    state: Mapped[str] = mapped_column(String(20), default="configured")  # configured, deploying, running, stopped, error
    config_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    cluster_uuid: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    # ── External cluster support ────────────────────────────────────────
    # `kind` distinguishes Tantor-deployed clusters (managed) from clusters
    # we just connect to via bootstrap.servers (external). External clusters
    # have NO Service rows and cannot be deployed/started/stopped/upgraded.
    kind: Mapped[str] = mapped_column(String(20), default="managed", server_default="managed")
    bootstrap_servers: Mapped[str | None] = mapped_column(Text, nullable=True)
    # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
    security_protocol: Mapped[str | None] = mapped_column(String(30), nullable=True)
    # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | OAUTHBEARER | GSSAPI (only when SASL)
    sasl_mechanism: Mapped[str | None] = mapped_column(String(40), nullable=True)
    # Fernet-encrypted JSON of the connection secrets:
    #   {"sasl_username": "...", "sasl_password": "...",
    #    "ssl_ca_pem": "...", "ssl_cert_pem": "...", "ssl_key_pem": "..."}
    encrypted_connection_secrets: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Verify server certificate. False bypasses validation (INSECURE) — leave on
    # for production, only flip off for self-signed dev environments.
    ssl_verify: Mapped[bool] = mapped_column(Boolean, default=True, server_default="1")

    # ── TLS / mTLS for managed clusters ─────────────────────────────────
    # When ssl_enabled is true, brokers run a second listener on
    # config.ssl_listener_port using a Tantor-generated CA. mtls_required
    # turns on `ssl.client.auth=required` so producers/consumers must
    # present a client cert signed by the same CA.
    ssl_enabled: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0")
    mtls_required: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0")
    # Fernet-encrypted; same password used for keystore + truststore on every
    # broker so we don't have to thread per-broker passwords through Ansible.
    encrypted_tls_password: Mapped[str | None] = mapped_column(Text, nullable=True)
