"""Schemas for connecting to externally-managed Kafka clusters."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


SecurityProtocol = Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
SaslMechanism = Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER", "GSSAPI"]


class ExternalConnectionSecrets(BaseModel):
    """Write-only secret blob — never returned by the API."""
    sasl_username: str | None = None
    sasl_password: str | None = None
    ssl_ca_pem: str | None = None
    ssl_cert_pem: str | None = None
    ssl_key_pem: str | None = None


class ExternalClusterCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    bootstrap_servers: str = Field(description="Comma-separated host:port list")
    security_protocol: SecurityProtocol = "PLAINTEXT"
    sasl_mechanism: SaslMechanism | None = None
    ssl_verify: bool = True
    secrets: ExternalConnectionSecrets = ExternalConnectionSecrets()


class ExternalClusterUpdate(BaseModel):
    name: str | None = None
    bootstrap_servers: str | None = None
    security_protocol: SecurityProtocol | None = None
    sasl_mechanism: SaslMechanism | None = None
    ssl_verify: bool | None = None
    # Only fields the operator actually fills are merged into the stored
    # secret blob; redacted placeholder strings (returned in GET responses)
    # are never round-tripped here.
    secrets: ExternalConnectionSecrets | None = None


class ExternalClusterResponse(BaseModel):
    id: str
    name: str
    kind: Literal["external"]
    state: str
    bootstrap_servers: str | None
    security_protocol: SecurityProtocol
    sasl_mechanism: SaslMechanism | None
    sasl_username: str | None = None
    sasl_password_set: bool = False
    ssl_ca_set: bool = False
    ssl_cert_set: bool = False
    ssl_key_set: bool = False
    ssl_verify: bool


class ExternalConnectionTestRequest(BaseModel):
    """Used by the test-connection endpoint, which does NOT persist anything."""
    bootstrap_servers: str
    security_protocol: SecurityProtocol = "PLAINTEXT"
    sasl_mechanism: SaslMechanism | None = None
    ssl_verify: bool = True
    secrets: ExternalConnectionSecrets = ExternalConnectionSecrets()


class ExternalConnectionTestResponse(BaseModel):
    success: bool
    message: str
    broker_count: int | None = None
    controller_id: int | None = None
    cluster_id: str | None = None
