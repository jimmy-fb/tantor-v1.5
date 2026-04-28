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


def get_topic_detail(cluster: Cluster, topic_name: str) -> dict:
    """Return the same shape kafka_admin._parse_topic_describe produces."""
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    with _ssl_files_for(secrets) as ssl_paths:
        admin = KafkaAdminClient(**_common_kwargs(cluster, secrets, ssl_paths))
        try:
            descs = admin.describe_topics([topic_name])
            if not descs:
                raise ValueError(f"Topic not found: {topic_name}")
            d = descs[0]
            partitions_info = []
            for p in d.get("partitions", []):
                partitions_info.append({
                    "partition": p.get("partition"),
                    "leader": p.get("leader"),
                    "replicas": p.get("replicas", []),
                    "isr": p.get("isr", []),
                })
            # Topic configs (retention.ms, cleanup.policy, etc.) via separate call.
            from kafka.admin import ConfigResource, ConfigResourceType
            cr = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            cfg_map: dict[str, str] = {}
            try:
                cfg_resp = admin.describe_configs([cr])
                # The shape of describe_configs varies; flatten defensively.
                for resp in cfg_resp or []:
                    resources = getattr(resp, "resources", None) or resp
                    if isinstance(resources, list):
                        for entry in resources:
                            entries = entry[4] if isinstance(entry, tuple) and len(entry) > 4 else []
                            for e in entries or []:
                                if isinstance(e, tuple) and len(e) >= 2:
                                    cfg_map[e[0]] = e[1]
            except Exception:
                pass
            return {
                "name": topic_name,
                "partitions": len(partitions_info),
                "replication_factor": len(partitions_info[0]["replicas"]) if partitions_info else 0,
                "partitions_info": partitions_info,
                "configs": cfg_map,
            }
        finally:
            admin.close()


def list_consumer_groups_with_lag(cluster: Cluster) -> list[dict]:
    """Match the managed-cluster shape: id/state/members/topics/offsets/lag."""
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    with _ssl_files_for(secrets) as ssl_paths:
        admin = KafkaAdminClient(**_common_kwargs(cluster, secrets, ssl_paths))
        try:
            groups = [g[0] for g in admin.list_consumer_groups()]
            if not groups:
                return []
            # describe_consumer_groups returns ConsumerGroupResponse_v0/...
            descriptions = admin.describe_consumer_groups(groups)
            # Coordinator offsets give us per-partition committed offsets.
            from kafka import KafkaConsumer
            consumer_kw = _common_kwargs(cluster, secrets, ssl_paths)
            consumer_kw.pop("client_id", None)
            out = []
            for desc in descriptions:
                gid = desc.group
                state = desc.state
                members = []
                topics: set[str] = set()
                for m in desc.members:
                    info = getattr(m, "member_metadata", None)
                    member_topics: list[str] = []
                    if info and hasattr(info, "subscription"):
                        member_topics = list(info.subscription)
                    elif info and hasattr(info, "topics"):
                        member_topics = list(info.topics)
                    topics.update(member_topics)
                    members.append({
                        "id": m.member_id,
                        "client_id": m.client_id,
                        "client_host": m.client_host,
                        "topics": member_topics,
                    })
                # Committed offsets (best-effort).
                offsets: list[dict] = []
                try:
                    offset_map = admin.list_consumer_group_offsets(gid)
                    for tp, om in offset_map.items():
                        offsets.append({
                            "topic": tp.topic,
                            "partition": tp.partition,
                            "current_offset": om.offset,
                        })
                except Exception:
                    pass
                out.append({
                    "group_id": gid,
                    "state": state,
                    "members": len(members),
                    "topics": sorted(topics),
                    "offsets": offsets,
                    "member_details": members,
                })
            return out
        finally:
            admin.close()


def get_consumer_group_detail(cluster: Cluster, group_id: str) -> dict:
    matches = [g for g in list_consumer_groups_with_lag(cluster) if g["group_id"] == group_id]
    if not matches:
        raise ValueError(f"Consumer group not found: {group_id}")
    return matches[0]


def alter_topic_config(cluster: Cluster, topic_name: str, configs: dict) -> dict:
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    from kafka.admin import ConfigResource, ConfigResourceType
    with _ssl_files_for(secrets) as ssl_paths:
        admin = KafkaAdminClient(**_common_kwargs(cluster, secrets, ssl_paths))
        try:
            cr = ConfigResource(ConfigResourceType.TOPIC, topic_name, configs=configs)
            admin.alter_configs([cr])
            return {"topic": topic_name, "updated": True, "configs": configs}
        finally:
            admin.close()


def increase_partitions(cluster: Cluster, topic_name: str, new_count: int) -> dict:
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    from kafka.admin import NewPartitions
    with _ssl_files_for(secrets) as ssl_paths:
        admin = KafkaAdminClient(**_common_kwargs(cluster, secrets, ssl_paths))
        try:
            admin.create_partitions({topic_name: NewPartitions(total_count=new_count)})
            return {"topic": topic_name, "partitions": new_count, "updated": True}
        finally:
            admin.close()


def validate_cluster(cluster: Cluster, create_test_topic: bool = True) -> dict:
    """Mirrors kafka_admin.validate_cluster but using kafka-python."""
    steps: list[dict] = []
    test_topic = "__tantor_validation_test"
    success_overall = True
    try:
        topics = list_topics(cluster)
        steps.append({"step": "list_topics", "success": True,
                      "message": f"Found {len(topics)} topic(s)", "data": [t["name"] for t in topics][:5]})
    except Exception as e:
        steps.append({"step": "list_topics", "success": False, "message": str(e), "data": None})
        return {"steps": steps, "success": False}

    if create_test_topic:
        try:
            create_topic(cluster, test_topic, 1, 1)
            steps.append({"step": "create_test_topic", "success": True,
                          "message": f"Created topic '{test_topic}'", "data": None})
        except Exception as e:
            msg = str(e)
            if "already exists" in msg.lower() or "TopicAlreadyExistsError" in msg:
                steps.append({"step": "create_test_topic", "success": True,
                              "message": "Test topic already present (reusing)", "data": None})
            else:
                steps.append({"step": "create_test_topic", "success": False, "message": msg, "data": None})
                return {"steps": steps, "success": False}
    try:
        produce_message(cluster, test_topic, "validation-key", '{"validation": true}')
        steps.append({"step": "produce_message", "success": True, "message": "Message produced", "data": None})
    except Exception as e:
        steps.append({"step": "produce_message", "success": False, "message": str(e), "data": None})
        return {"steps": steps, "success": False}

    try:
        msgs = consume_messages(cluster, test_topic, max_messages=5, timeout_ms=8000, from_beginning=True)
        found = any('"validation"' in (m.get("value") or "") for m in msgs)
        steps.append({"step": "consume_message",
                      "success": True if found else False,
                      "message": (f"Consumed {len(msgs)} message(s), validation message {'found' if found else 'NOT found'}"),
                      "data": msgs})
        if not found:
            success_overall = False
    except Exception as e:
        steps.append({"step": "consume_message", "success": False, "message": str(e), "data": None})
        success_overall = False

    if create_test_topic:
        try:
            delete_topic(cluster, test_topic)
            steps.append({"step": "cleanup", "success": True, "message": "Cleaned up test topic", "data": None})
        except Exception as e:
            steps.append({"step": "cleanup", "success": False, "message": str(e), "data": None})

    return {"steps": steps, "success": success_overall}


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
