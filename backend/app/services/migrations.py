"""Lightweight runtime migrations for SQLite.

We don't ship Alembic, so when columns are added to existing tables we ALTER
them in place at startup. New tables are handled by Base.metadata.create_all.
Idempotent — safe to run on every boot.
"""
from __future__ import annotations

import logging

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

logger = logging.getLogger("tantor.migrations")


# (table, column, sql_type, default_clause_or_none)
_REQUIRED_COLUMNS: list[tuple[str, str, str, str | None]] = [
    # PR #1: harden LDAPS — server-cert validation toggle + optional CA PEM
    ("ldap_configs", "tls_validate_cert", "BOOLEAN", "DEFAULT 1"),
    ("ldap_configs", "tls_ca_cert", "TEXT", None),
    # PR #3: external cluster connect (managed vs external + auth blob)
    ("clusters", "kind", "VARCHAR(20)", "DEFAULT 'managed'"),
    ("clusters", "bootstrap_servers", "TEXT", None),
    ("clusters", "security_protocol", "VARCHAR(30)", None),
    ("clusters", "sasl_mechanism", "VARCHAR(40)", None),
    ("clusters", "encrypted_connection_secrets", "TEXT", None),
    ("clusters", "ssl_verify", "BOOLEAN", "DEFAULT 1"),
    # PR #4: TLS / mTLS for managed clusters
    ("clusters", "ssl_enabled", "BOOLEAN", "DEFAULT 0"),
    ("clusters", "mtls_required", "BOOLEAN", "DEFAULT 0"),
    ("clusters", "encrypted_tls_password", "TEXT", None),
    # QA #51: environment tagging for cluster organization
    ("clusters", "environment", "VARCHAR(40)", "DEFAULT ''"),
    # APB v1.3: SSH broker hosts for externally-managed clusters (start/restart)
    ("clusters", "external_broker_hosts_json", "TEXT", None),
    # APB v1.3.5 — multi-cluster /opt/kafka collision fix
    ("clusters", "kafka_install_dir", "VARCHAR(255)", None),
    ("clusters", "kafka_data_dir", "VARCHAR(255)", None),
    ("clusters", "kafka_unit_name", "VARCHAR(120)", None),
    # APB v1.4.0 — capture actor on every audit row (#13)
    ("audit_logs", "actor_user_id", "VARCHAR(36)", None),
    ("audit_logs", "actor_username", "VARCHAR(255)", None),
    # APB v1.4.0 — LDAP-vs-manual user provenance (#11)
    ("users", "auth_source", "VARCHAR(20)", "DEFAULT 'local'"),
    ("users", "ldap_dn", "VARCHAR(500)", None),
    # APB v1.4.3 — bump token_version on privilege change to invalidate
    # in-flight JWTs (#22)
    ("users", "token_version", "INTEGER", "DEFAULT 0"),
]


def apply_runtime_migrations(engine: Engine) -> None:
    insp = inspect(engine)
    existing_tables = set(insp.get_table_names())

    with engine.begin() as conn:
        for table, column, sql_type, default_clause in _REQUIRED_COLUMNS:
            if table not in existing_tables:
                # Table itself is missing; create_all will handle it on first boot.
                continue
            cols = {c["name"] for c in insp.get_columns(table)}
            if column in cols:
                continue
            ddl = f'ALTER TABLE {table} ADD COLUMN {column} {sql_type}'
            if default_clause:
                ddl += f' {default_clause}'
            logger.info("Applying migration: %s", ddl)
            conn.execute(text(ddl))
