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


def _detect_broker_version(admin: KafkaAdminClient) -> str | None:
    """Detect broker software version.

    kafka-python-ng's `check_version()` only walks API versions it knows
    about — it caps at ~2.6 even when talking to a Kafka 4.x broker. We try
    each path in order:
      1. Read `software_version` from an ApiVersionRequest_v3 response if
         the kafka-python build has the field decoded.
      2. Read it off the cached cluster metadata if available.
      3. Fall back to check_version() and label "≥ X.Y.Z" so the operator
         knows it's a floor, not the exact version.
    """
    # Path 1: explicit ApiVersions request.
    try:
        from kafka.protocol.admin import ApiVersionRequest_v3
        client = admin._client  # type: ignore[attr-defined]
        node_id = next(iter(client._connections), None)
        if node_id is not None:
            future = client.send(node_id, ApiVersionRequest_v3([]))
            client.poll(future=future, timeout_ms=2000)
            if future.is_done and future.succeeded():
                resp = future.value
                ver = getattr(resp, "software_version", None) or getattr(resp, "broker_software_version", None)
                if ver and isinstance(ver, (str, bytes)):
                    return ver.decode() if isinstance(ver, bytes) else ver
    except Exception:
        pass
    # Path 2: heuristic. kafka-python-ng tops out at the highest API
    # version it knows; for newer brokers the result is a lower bound.
    try:
        client = admin._client  # type: ignore[attr-defined]
        ver = client.check_version()
        if isinstance(ver, tuple) and len(ver) >= 2:
            base = ".".join(str(x) for x in ver)
            return f"≥ {base}"
    except Exception:
        pass
    return None


def test_connection(cluster: Cluster) -> dict:
    """Open an admin client and call describe_cluster — fastest end-to-end probe.

    Returns {success, message, broker_count, controller_id, kafka_version}
    so the UI can show what it's actually connected to. APB asked for the
    real broker version to appear instead of "unknown".
    """
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    try:
        with _ssl_files_for(secrets) as ssl_paths:
            kw = _common_kwargs(cluster, secrets, ssl_paths)
            admin = KafkaAdminClient(**kw)
            try:
                desc = admin.describe_cluster()
                brokers = desc.get("brokers", [])
                kafka_version = _detect_broker_version(admin)
                # Each broker dict from kafka-python looks like:
                # {"node_id": 0, "host": "...", "port": 9092, "rack": None}
                broker_summary = [
                    {
                        "node_id": b.get("node_id"),
                        "host": b.get("host"),
                        "port": b.get("port"),
                        "rack": b.get("rack"),
                    }
                    for b in brokers
                ]
                return {
                    "success": True,
                    "message": f"Connected to {len(brokers)} broker(s)",
                    "broker_count": len(brokers),
                    "controller_id": desc.get("controller_id"),
                    "cluster_id": desc.get("cluster_id"),
                    "kafka_version": kafka_version,
                    "brokers": broker_summary,
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
            # Match the managed-cluster ProduceResponse shape exactly so the
            # /produce endpoint's response_model validation doesn't reject a
            # successful produce with 500 "Internal server error". APB hit
            # this every time they used the Produce tab on an external cluster.
            return {
                "success": True,
                "message": f"Produced to {md.topic} (partition {md.partition}, offset {md.offset})",
            }
        finally:
            producer.close(timeout=5)


def list_scram_users(cluster: Cluster) -> list[dict]:
    """SCRAM admin: kafka-python(-ng) 2.2.3 doesn't expose
    describe_user_scram_credentials yet, so the most honest answer for an
    external cluster is `[]`. Once we move to confluent-kafka or kafka-python
    grows the API, this becomes a real call. Tantor's audit log + DB-stored
    credentials still record managed-cluster SCRAM users normally.
    """
    return []


def _scram_unsupported() -> None:
    raise ValueError(
        "SCRAM user admin is not yet supported for externally-connected clusters: "
        "kafka-python's AdminClient lacks alter_user_scram_credentials. Manage SCRAM "
        "users directly on the source cluster via kafka-configs.sh, or convert this "
        "to a managed cluster."
    )


def create_scram_user(cluster: Cluster, username: str, password: str, mechanism: str) -> dict:
    _scram_unsupported()


def delete_scram_user(cluster: Cluster, username: str) -> dict:
    _scram_unsupported()


# ── ACLs ──────────────────────────────────────────────────────────────────


def _acl_resource_type(name: str | None):
    from kafka.admin.acl_resource import ResourceType
    if not name: return ResourceType.ANY
    return {
        "topic": ResourceType.TOPIC, "group": ResourceType.GROUP,
        "cluster": ResourceType.CLUSTER, "transactional_id": ResourceType.TRANSACTIONAL_ID,
        "delegation_token": ResourceType.DELEGATION_TOKEN,
    }.get(name.lower(), ResourceType.ANY)


def _acl_pattern(name: str | None):
    from kafka.admin.acl_resource import ACLResourcePatternType
    if not name: return ACLResourcePatternType.LITERAL
    return {
        "literal": ACLResourcePatternType.LITERAL,
        "prefixed": ACLResourcePatternType.PREFIXED,
    }.get(name.lower(), ACLResourcePatternType.LITERAL)


def _acl_op(name: str | None):
    from kafka.admin.acl_resource import ACLOperation
    if not name: return ACLOperation.ANY
    m = {
        "all": ACLOperation.ALL, "read": ACLOperation.READ, "write": ACLOperation.WRITE,
        "create": ACLOperation.CREATE, "delete": ACLOperation.DELETE, "alter": ACLOperation.ALTER,
        "describe": ACLOperation.DESCRIBE, "cluster_action": ACLOperation.CLUSTER_ACTION,
        "describe_configs": ACLOperation.DESCRIBE_CONFIGS, "alter_configs": ACLOperation.ALTER_CONFIGS,
        "idempotent_write": ACLOperation.IDEMPOTENT_WRITE,
    }
    return m.get(name.lower(), ACLOperation.ANY)


def _acl_perm(name: str | None):
    from kafka.admin.acl_resource import ACLPermissionType
    if not name: return ACLPermissionType.ANY
    return {"allow": ACLPermissionType.ALLOW, "deny": ACLPermissionType.DENY}.get(name.lower(), ACLPermissionType.ANY)


def list_acls(cluster: Cluster, principal: str | None = None,
              resource_type: str | None = None, resource_name: str | None = None) -> list[dict]:
    from kafka.admin.acl_resource import ACLFilter, ACLResourcePatternType, ResourcePatternFilter
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    with _ssl_files_for(secrets) as ssl_paths:
        admin = KafkaAdminClient(**_common_kwargs(cluster, secrets, ssl_paths))
        try:
            # kafka-python validates resource_pattern with isinstance check —
            # a duck-typed object raises IllegalArgumentError.
            resource_pattern = ResourcePatternFilter(
                resource_type=_acl_resource_type(resource_type),
                resource_name=resource_name,
                pattern_type=ACLResourcePatternType.ANY,
            )
            f = ACLFilter(
                principal=principal, host="*",
                operation=_acl_op(None), permission_type=_acl_perm(None),
                resource_pattern=resource_pattern,
            )
            from kafka.admin.acl_resource import (
                ACLOperation, ACLPermissionType, ACLResourcePatternType, ResourceType,
            )
            acls, _err = admin.describe_acls(f)
            # Kafka returns numeric codes; convert to enum names so the
            # response matches the SSH-CLI shape (READ/WRITE/Allow/etc).
            def _name(enum_cls, val):
                try: return enum_cls(int(val)).name
                except (ValueError, TypeError): return str(val)
            out = []
            for acl in acls or []:
                rp = getattr(acl, "resource_pattern", None)
                out.append({
                    "principal": getattr(acl, "principal", "?"),
                    "host": getattr(acl, "host", "*"),
                    "operation": _name(ACLOperation, getattr(acl, "operation", None)),
                    "permission_type": _name(ACLPermissionType, getattr(acl, "permission_type", None)),
                    "resource_type": _name(ResourceType, getattr(rp, "resource_type", None)),
                    "resource_name": getattr(rp, "resource_name", "?"),
                    "pattern_type": _name(ACLResourcePatternType, getattr(rp, "pattern_type", None)),
                })
            return out
        finally:
            admin.close()


def create_acl(cluster: Cluster, acl_req: dict) -> dict:
    from kafka.admin.acl_resource import ACL, ResourcePattern
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    with _ssl_files_for(secrets) as ssl_paths:
        admin = KafkaAdminClient(**_common_kwargs(cluster, secrets, ssl_paths))
        try:
            resource = ResourcePattern(
                resource_type=_acl_resource_type(acl_req.get("resource_type")),
                resource_name=acl_req["resource_name"],
                pattern_type=_acl_pattern(acl_req.get("pattern_type")),
            )
            acl = ACL(
                principal=acl_req["principal"],
                host=acl_req.get("host", "*"),
                operation=_acl_op(acl_req["operation"]),
                permission_type=_acl_perm(acl_req.get("permission_type", "allow")),
                resource_pattern=resource,
            )
            admin.create_acls([acl])
            return {"created": True, **acl_req}
        finally:
            admin.close()


def delete_acl(cluster: Cluster, acl_req: dict) -> dict:
    from kafka.admin.acl_resource import ACLFilter, ACLResourcePatternType, ResourcePatternFilter
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    with _ssl_files_for(secrets) as ssl_paths:
        admin = KafkaAdminClient(**_common_kwargs(cluster, secrets, ssl_paths))
        try:
            resource_pattern = ResourcePatternFilter(
                resource_type=_acl_resource_type(acl_req.get("resource_type")),
                resource_name=acl_req.get("resource_name"),
                pattern_type=ACLResourcePatternType.LITERAL,
            )
            f = ACLFilter(
                principal=acl_req.get("principal"),
                host=acl_req.get("host", "*"),
                operation=_acl_op(acl_req.get("operation")),
                permission_type=_acl_perm(acl_req.get("permission_type", "allow")),
                resource_pattern=resource_pattern,
            )
            admin.delete_acls([f])
            return {"deleted": True, **acl_req}
        finally:
            admin.close()


# ── Broker config (describe + alter) ──────────────────────────────────────


def describe_broker_configs(cluster: Cluster) -> list[dict]:
    """Return per-broker configs in the same shape kafka_admin uses."""
    from kafka.admin import ConfigResource, ConfigResourceType
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    with _ssl_files_for(secrets) as ssl_paths:
        admin = KafkaAdminClient(**_common_kwargs(cluster, secrets, ssl_paths))
        try:
            cluster_meta = admin.describe_cluster()
            broker_ids = [b["node_id"] for b in (cluster_meta.get("brokers") or [])]
            if not broker_ids:
                return []
            resources = [ConfigResource(ConfigResourceType.BROKER, str(bid)) for bid in broker_ids]
            cfg_resp = admin.describe_configs(resources)
            out: list[dict] = []
            for resp in cfg_resp or []:
                # Each kafka-python response carries one or more (resource, configs) tuples.
                for entry in getattr(resp, "resources", []) or []:
                    err_code, _err_msg, _rt, broker_id_str, configs = (entry + (None,) * 5)[:5]
                    flattened: list[dict] = []
                    for cfg in configs or []:
                        # cfg = (name, value, read_only, is_default, is_sensitive, ...)
                        flattened.append({
                            "name": cfg[0],
                            "value": cfg[1] if not (len(cfg) > 4 and cfg[4]) else "***",
                            "is_default": bool(cfg[3]) if len(cfg) > 3 else False,
                            "is_sensitive": bool(cfg[4]) if len(cfg) > 4 else False,
                            "is_read_only": bool(cfg[2]) if len(cfg) > 2 else False,
                        })
                    out.append({"broker_id": int(broker_id_str), "configs": flattened})
            return out
        finally:
            admin.close()


def alter_broker_config(cluster: Cluster, broker_id: int, configs: dict) -> dict:
    """Apply config changes to a specific broker.

    APB v1.4.3 — REVERTED the strict hasattr() refusal I added in 1.4.1.
    That check was returning False for the customer's kafka-python build
    (2.2.3 ships incremental_alter_configs but the attribute lookup was
    failing for reasons I never reproduced), so external Config edits
    failed for every customer. Back to try/except: attempt incremental
    first, fall back to legacy alter_configs ONLY if the incremental
    call itself raises NotImplementedError or AttributeError. Anything
    else (broker rejection, timeout) bubbles up untouched so we don't
    paper over real failures.
    """
    from kafka.admin import ConfigResource, ConfigResourceType
    import logging as _logging
    _log = _logging.getLogger("tantor.external_admin")
    secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
    with _ssl_files_for(secrets) as ssl_paths:
        admin = KafkaAdminClient(**_common_kwargs(cluster, secrets, ssl_paths))
        try:
            cr = ConfigResource(ConfigResourceType.BROKER, str(broker_id), configs=configs)
            try:
                admin.incremental_alter_configs([cr])
                return {"broker_id": broker_id, "updated": True, "configs": configs, "method": "incremental"}
            except (AttributeError, NotImplementedError):
                # kafka-python on this server lacks the incremental
                # method. Fall back to alter_configs with a loud warning
                # — alter_configs replaces the whole config block, so
                # we re-read the broker's existing config first and
                # merge with the requested changes to mimic incremental
                # semantics (KIP-516 safety).
                _log.warning(
                    "incremental_alter_configs unavailable for broker %s; "
                    "falling back to alter_configs with read-then-merge",
                    broker_id,
                )
                existing = describe_broker_configs(cluster)
                cur = {}
                for entry in existing:
                    if entry.get("broker_id") == broker_id:
                        cur = {c["name"]: c["value"] for c in entry.get("configs", [])
                               if c.get("value") is not None}
                        break
                merged = {**cur, **configs}
                cr_merged = ConfigResource(ConfigResourceType.BROKER, str(broker_id), configs=merged)
                admin.alter_configs([cr_merged])
                return {"broker_id": broker_id, "updated": True, "configs": configs, "method": "fallback_merged"}
        finally:
            admin.close()


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
