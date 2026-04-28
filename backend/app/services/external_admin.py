"""Kafka admin client for externally-connected clusters.

Tantor's normal admin path uses SSH+CLI on broker hosts. That doesn't work
when we're connecting to a cluster Tantor didn't deploy and only has
bootstrap.servers + auth credentials for.

This module wraps kafka-python with the SSL/SASL config Tantor stores on
Cluster rows when `kind="external"`.
"""
from __future__ import annotations

import json
import logging
import ssl
import tempfile
from contextlib import contextmanager

from cryptography.fernet import Fernet, InvalidToken
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError

from app.config import settings
from app.models.cluster import Cluster

logger = logging.getLogger("tantor.external_admin")


# ── Connection-config plumbing ────────────────────────────────────────────


SECRET_KEYS = ("sasl_username", "sasl_password", "ssl_ca_pem", "ssl_cert_pem", "ssl_key_pem")


def encrypt_secrets(plain: dict) -> str:
    """Fernet-encrypt the connection secret blob."""
    return Fernet(settings.FERNET_KEY.encode()).encrypt(json.dumps(plain).encode()).decode()


def decrypt_secrets(encrypted: str | None) -> dict:
    if not encrypted:
        return {}
    try:
        return json.loads(Fernet(settings.FERNET_KEY.encode()).decrypt(encrypted.encode()).decode())
    except (InvalidToken, ValueError, json.JSONDecodeError) as e:
        logger.warning("Failed to decrypt connection secrets: %s", e)
        return {}


def redact_connection(cluster: Cluster) -> dict:
    """Build a UI-safe view of the cluster's connection config.

    Never returns plaintext passwords or PEM bodies — only their presence.
    """
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    return {
        "bootstrap_servers": cluster.bootstrap_servers,
        "security_protocol": cluster.security_protocol or "PLAINTEXT",
        "sasl_mechanism": cluster.sasl_mechanism,
        "sasl_username": secrets.get("sasl_username"),
        "sasl_password_set": bool(secrets.get("sasl_password")),
        "ssl_ca_set": bool(secrets.get("ssl_ca_pem")),
        "ssl_cert_set": bool(secrets.get("ssl_cert_pem")),
        "ssl_key_set": bool(secrets.get("ssl_key_pem")),
        "ssl_verify": bool(cluster.ssl_verify) if cluster.ssl_verify is not None else True,
    }


# ── kafka-python config builder ───────────────────────────────────────────


@contextmanager
def _ssl_files_for(secrets: dict):
    """Spill any PEM bodies in `secrets` to short-lived temp files.

    kafka-python's AdminClient takes file paths (not in-memory PEM), so we
    materialize whatever the operator stored, yield the paths, and clean up.
    """
    paths: dict[str, str] = {}
    handles: list[str] = []
    try:
        for k in ("ssl_ca_pem", "ssl_cert_pem", "ssl_key_pem"):
            body = secrets.get(k)
            if not body:
                continue
            fd_name = tempfile.NamedTemporaryFile(prefix=f"tantor-{k}-", suffix=".pem", delete=False)
            fd_name.write(body.encode() if isinstance(body, str) else body)
            fd_name.close()
            paths[k] = fd_name.name
            handles.append(fd_name.name)
        yield paths
    finally:
        for p in handles:
            try:
                import os
                os.unlink(p)
            except OSError:
                pass


def _common_kwargs(cluster: Cluster, secrets: dict, ssl_paths: dict) -> dict:
    """Build the kwargs every kafka-python client (Admin/Producer/Consumer) shares."""
    protocol = cluster.security_protocol or "PLAINTEXT"
    kw: dict = {
        "bootstrap_servers": [s.strip() for s in (cluster.bootstrap_servers or "").split(",") if s.strip()],
        "security_protocol": protocol,
        "client_id": "tantor",
        "request_timeout_ms": 10_000,
    }
    if protocol in ("SSL", "SASL_SSL"):
        ctx = ssl.create_default_context()
        if cluster.ssl_verify is False:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        if "ssl_ca_pem" in ssl_paths:
            ctx.load_verify_locations(cafile=ssl_paths["ssl_ca_pem"])
        if "ssl_cert_pem" in ssl_paths and "ssl_key_pem" in ssl_paths:
            ctx.load_cert_chain(ssl_paths["ssl_cert_pem"], keyfile=ssl_paths["ssl_key_pem"])
        kw["ssl_context"] = ctx
    if protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
        kw["sasl_mechanism"] = cluster.sasl_mechanism or "PLAIN"
        kw["sasl_plain_username"] = secrets.get("sasl_username", "")
        kw["sasl_plain_password"] = secrets.get("sasl_password", "")
    return kw


# ── Public operations ─────────────────────────────────────────────────────


def test_connection(cluster: Cluster) -> dict:
    """Open an admin client and call describe_cluster — fastest end-to-end probe.

    Returns {success, message, broker_count, controller_id} so the UI can show
    what it's actually connected to.
    """
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    try:
        with _ssl_files_for(secrets) as ssl_paths:
            kw = _common_kwargs(cluster, secrets, ssl_paths)
            admin = KafkaAdminClient(**kw)
            try:
                desc = admin.describe_cluster()
                brokers = desc.get("brokers", [])
                return {
                    "success": True,
                    "message": f"Connected to {len(brokers)} broker(s)",
                    "broker_count": len(brokers),
                    "controller_id": desc.get("controller_id"),
                    "cluster_id": desc.get("cluster_id"),
                }
            finally:
                admin.close()
    except KafkaError as e:
        return {"success": False, "message": f"Kafka error: {e.__class__.__name__}: {e}"}
    except Exception as e:
        return {"success": False, "message": f"{type(e).__name__}: {e}"}


def list_topics(cluster: Cluster) -> list[dict]:
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    with _ssl_files_for(secrets) as ssl_paths:
        admin = KafkaAdminClient(**_common_kwargs(cluster, secrets, ssl_paths))
        try:
            metadata = admin.list_topics()
            descs = admin.describe_topics(metadata)
            out = []
            for d in descs:
                # kafka-python returns dicts already; normalize to Tantor's shape.
                out.append({
                    "name": d.get("topic"),
                    "partitions": len(d.get("partitions", [])),
                    "replication_factor": (
                        len(d["partitions"][0].get("replicas", [])) if d.get("partitions") else 0
                    ),
                })
            return out
        finally:
            admin.close()


def list_consumer_groups(cluster: Cluster) -> list[dict]:
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    with _ssl_files_for(secrets) as ssl_paths:
        admin = KafkaAdminClient(**_common_kwargs(cluster, secrets, ssl_paths))
        try:
            return [
                {"group_id": g[0], "protocol_type": g[1] if len(g) > 1 else ""}
                for g in admin.list_consumer_groups()
            ]
        finally:
            admin.close()


def create_topic(cluster: Cluster, name: str, partitions: int, replication_factor: int) -> dict:
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    with _ssl_files_for(secrets) as ssl_paths:
        admin = KafkaAdminClient(**_common_kwargs(cluster, secrets, ssl_paths))
        try:
            admin.create_topics([NewTopic(
                name=name, num_partitions=partitions, replication_factor=replication_factor,
            )])
            return {"name": name, "partitions": partitions, "replication_factor": replication_factor}
        finally:
            admin.close()


def delete_topic(cluster: Cluster, name: str) -> dict:
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    with _ssl_files_for(secrets) as ssl_paths:
        admin = KafkaAdminClient(**_common_kwargs(cluster, secrets, ssl_paths))
        try:
            admin.delete_topics([name])
            return {"name": name, "deleted": True}
        finally:
            admin.close()


def produce_message(cluster: Cluster, topic: str, key: str | None, value: str) -> dict:
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    with _ssl_files_for(secrets) as ssl_paths:
        producer = KafkaProducer(**_common_kwargs(cluster, secrets, ssl_paths))
        try:
            future = producer.send(
                topic,
                key=key.encode() if key else None,
                value=value.encode(),
            )
            md = future.get(timeout=10)
            return {"topic": md.topic, "partition": md.partition, "offset": md.offset}
        finally:
            producer.close(timeout=5)


def consume_messages(cluster: Cluster, topic: str, max_messages: int = 10, timeout_ms: int = 5000, from_beginning: bool = True) -> list[dict]:
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    with _ssl_files_for(secrets) as ssl_paths:
        kw = _common_kwargs(cluster, secrets, ssl_paths)
        kw.update({
            "auto_offset_reset": "earliest" if from_beginning else "latest",
            "enable_auto_commit": False,
            "consumer_timeout_ms": timeout_ms,
            "group_id": None,  # one-shot poll, no committed offsets
        })
        consumer = KafkaConsumer(topic, **kw)
        try:
            messages = []
            for msg in consumer:
                messages.append({
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "timestamp": msg.timestamp,
                    "key": msg.key.decode() if msg.key else None,
                    "value": msg.value.decode("utf-8", errors="replace") if msg.value else None,
                })
                if len(messages) >= max_messages:
                    break
            return messages
        finally:
            consumer.close()
