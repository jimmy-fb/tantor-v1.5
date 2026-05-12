import json
import re
import secrets
import threading
import time
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from app.config import settings
from app.models.cluster import Cluster
from app.models.host import Host
from app.models.service import Service
from app.models.kafka_user import KafkaUser
from app.models.audit_log import AuditLog
from app.services import external_admin
from app.services.ssh_manager import SSHManager
from app.services.crypto import encrypt

if TYPE_CHECKING:
    from app.models.user import User


# APB v1.4.3 follow-up — per-cluster topic-list cache. kafka-topics.sh
# --describe over SSH takes 3-4 s (JVM startup dominates). With the
# Topics tab polling every 10s the UI showed a spinner for ~3s every
# tick. 5s TTL cache means at most 1-in-2 calls hits Kafka while keeping
# the data fresh enough for human observation.
_TOPIC_LIST_CACHE: dict[str, tuple[float, list[dict]]] = {}
_TOPIC_LIST_CACHE_TTL = 5.0
_TOPIC_LIST_CACHE_LOCK = threading.Lock()


def _topic_list_cached(cluster_id: str) -> list[dict] | None:
    with _TOPIC_LIST_CACHE_LOCK:
        entry = _TOPIC_LIST_CACHE.get(cluster_id)
        if entry and (time.time() - entry[0]) < _TOPIC_LIST_CACHE_TTL:
            return entry[1]
    return None


def _topic_list_set(cluster_id: str, topics: list[dict]) -> None:
    with _TOPIC_LIST_CACHE_LOCK:
        _TOPIC_LIST_CACHE[cluster_id] = (time.time(), topics)


def _topic_list_invalidate(cluster_id: str) -> None:
    """Called from create/delete/alter so the UI sees the change
    immediately on the next fetch instead of waiting 5s."""
    with _TOPIC_LIST_CACHE_LOCK:
        _TOPIC_LIST_CACHE.pop(cluster_id, None)


def _is_external(cluster_id: str, db: Session) -> Cluster | None:
    """Return the Cluster row only if it's externally-connected (kind=external).

    Kept module-private so the dispatch points stay readable. Caller decides
    what to do with `None` (proceed with managed SSH+CLI path).
    """
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise ValueError("Cluster not found")
    return cluster if (cluster.kind or "managed") == "external" else None


def _external_only_unsupported(operation: str):
    raise ValueError(
        f"{operation} is not supported on externally-connected clusters yet — "
        "the action requires SSH access to broker hosts that Tantor doesn't have."
    )


class KafkaAdmin:
    """Manages Kafka topics, consumer groups, and test messages via SSH CLI tools."""

    @staticmethod
    def _get_broker(cluster_id: str, db: Session) -> tuple[Host, str]:
        """Find a running broker and return (host, bootstrap_servers).

        Kept for backward compat — new code should use _get_broker_with_paths
        so it picks up the per-cluster Kafka install dir (APB v1.2.0 #5).
        """
        host, bootstrap, _ = KafkaAdmin._get_broker_with_paths(cluster_id, db)
        return host, bootstrap

    @staticmethod
    def _get_broker_with_paths(cluster_id: str, db: Session) -> tuple[Host, str, str]:
        """(host, bootstrap, kafka_install_dir) — single DB walk per call.

        Per-cluster install dir prevents /opt/kafka collisions when two
        Tantor-managed clusters run on the same broker host. Falls back to
        the global default for clusters created before that field existed.
        """
        from app.services import cluster_paths
        svc = db.query(Service).filter(
            Service.cluster_id == cluster_id,
            Service.role.in_(["broker", "broker_controller"]),
        ).first()
        if not svc:
            raise ValueError("No broker found in cluster")
        host = db.query(Host).filter(Host.id == svc.host_id).first()
        if not host:
            raise ValueError("Broker host not found")
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        config = json.loads(cluster.config_json) if cluster and cluster.config_json else {}
        port = config.get("listener_port", 9092)
        bootstrap = f"{host.ip_address}:{port}"
        kafka_dir = cluster_paths.install_dir(cluster) if cluster else cluster_paths.DEFAULT_INSTALL_DIR
        return host, bootstrap, kafka_dir

    @staticmethod
    def _run_kafka_cmd(host: Host, cmd: str, timeout: int = 30) -> tuple[int, str, str]:
        with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
            return SSHManager.exec_command(client, cmd, timeout=timeout)

    # ── Topics ──────────────────────────────────────────

    @staticmethod
    def _list_topics_via_kafka_python(bootstrap: str) -> list[dict]:
        """Fast path: hit the broker over TCP with kafka-python.

        APB v1.4.4 — kafka-topics.sh over SSH was 3-4s of JVM cold start
        per call and that latency was visible in the UI ("topic load is
        still slow"). The same metadata is available via TCP in ~50-200ms.
        Use KafkaConsumer.partitions_for_topic + a single
        describe_topics() round-trip to keep the response shape
        identical to the legacy SSH+CLI path.

        Caller falls back to the SSH path if this raises (e.g. broker
        unreachable from Tantor backend, SSL listener not exposed, etc.).
        """
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap,
            client_id="tantor-list-topics",
            request_timeout_ms=10000,
            # APB v1.4.4 — set api_version explicitly so kafka-python
            # skips the auto-detect handshake (saves ~500ms per call).
            # 4.0.0 / 4.1.0 brokers speak the same protocol surface for
            # describe_topics so this is safe; the fallback SSH path
            # handles older brokers if anyone shows up.
            api_version=(2, 8, 0),
        )
        try:
            metadata = admin.describe_topics()
            result: list[dict] = []
            for entry in metadata or []:
                # entry shape: {"topic": str, "is_internal": bool, "partitions": [{"partition", "leader", "replicas", "isr"}]}
                name = entry.get("topic") or entry.get("name")
                if not name:
                    continue
                # Skip internal topics by default (the SSH path does the same)
                if name.startswith("__"):
                    continue
                parts = entry.get("partitions") or []
                partition_count = len(parts)
                rf = 0
                if parts:
                    # All partitions on the same topic have identical RF;
                    # take the first one.
                    rf = len(parts[0].get("replicas") or [])
                result.append({
                    "name": name,
                    "partitions": partition_count,
                    "replication_factor": rf,
                })
            return result
        finally:
            admin.close()

    @staticmethod
    def list_topics(cluster_id: str, db: Session) -> list[dict]:
        # 5s cache — even the fast path benefits because the Topics tab
        # polls every 10s. Invalidated by create / delete so user actions
        # show up immediately.
        cached = _topic_list_cached(cluster_id)
        if cached is not None:
            return cached

        ext = _is_external(cluster_id, db)
        if ext:
            result = external_admin.list_topics(ext)
            _topic_list_set(cluster_id, result)
            return result
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        # Fast path: kafka-python over TCP. Falls back to the SSH+CLI
        # path on any failure (network, SSL handshake, etc.) so the
        # behavior is no worse than 1.4.3 in degraded environments.
        try:
            result = KafkaAdmin._list_topics_via_kafka_python(bootstrap)
            _topic_list_set(cluster_id, result)
            return result
        except Exception as e:
            import logging as _logging
            _logging.getLogger("tantor.kafka_admin").warning(
                "kafka-python list_topics failed (%s); falling back to SSH+kafka-topics.sh", e
            )

        # Fallback: shell out via SSH. Slower (3-4s) but works when the
        # broker isn't reachable from the Tantor backend over TCP.
        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(
            host, f"{kh}/bin/kafka-topics.sh --bootstrap-server {bootstrap} --describe",
            timeout=60,
        )
        if exit_code != 0:
            raise ValueError(f"Failed to list topics: {stderr}")
        result = KafkaAdmin._parse_all_topics_describe(stdout)
        _topic_list_set(cluster_id, result)
        return result

    @staticmethod
    def get_topic_detail(cluster_id: str, topic_name: str, db: Session) -> dict:
        ext = _is_external(cluster_id, db)
        if ext:
            return external_admin.get_topic_detail(ext, topic_name)
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(
            host, f"{kh}/bin/kafka-topics.sh --bootstrap-server {bootstrap} --describe --topic {topic_name}"
        )
        if exit_code != 0:
            raise ValueError(f"Failed to describe topic: {stderr}")

        return KafkaAdmin._parse_topic_describe(topic_name, stdout)

    @staticmethod
    def create_topic(cluster_id: str, name: str, partitions: int, replication_factor: int, config: dict, db: Session) -> dict:
        ext = _is_external(cluster_id, db)
        if ext:
            r = external_admin.create_topic(ext, name, partitions, replication_factor)
            _topic_list_invalidate(cluster_id)
            return r
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        cmd = (
            f"{kh}/bin/kafka-topics.sh --bootstrap-server {bootstrap} "
            f"--create --topic {name} --partitions {partitions} "
            f"--replication-factor {replication_factor}"
        )
        for k, v in (config or {}).items():
            cmd += f" --config {k}={v}"

        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(host, cmd)
        if exit_code != 0:
            raise ValueError(f"Failed to create topic: {stderr}")
        _topic_list_invalidate(cluster_id)
        return {"topic": name, "created": True, "message": stdout or "Topic created"}

    @staticmethod
    def delete_topic(cluster_id: str, name: str, db: Session) -> dict:
        ext = _is_external(cluster_id, db)
        if ext:
            r = external_admin.delete_topic(ext, name)
            _topic_list_invalidate(cluster_id)
            return r
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(
            host, f"{kh}/bin/kafka-topics.sh --bootstrap-server {bootstrap} --delete --topic {name}"
        )
        if exit_code != 0:
            raise ValueError(f"Failed to delete topic: {stderr}")
        _topic_list_invalidate(cluster_id)
        return {"topic": name, "deleted": True}

    # ── Topic Settings ───────────────────────────────────

    @staticmethod
    def alter_topic_config(cluster_id: str, topic_name: str, configs: dict, db: Session,
                            actor: "User | None" = None) -> dict:
        """Alter topic-level configuration (e.g. retention.ms, cleanup.policy)."""
        ext = _is_external(cluster_id, db)
        if ext:
            result = external_admin.alter_topic_config(ext, topic_name, configs)
            KafkaAdmin._audit(db, cluster_id, "topic_config_altered", "topic", topic_name, str(configs), actor=actor)
            db.commit()
            return result
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        config_str = ",".join(f"{k}={v}" for k, v in configs.items())
        cmd = (
            f"{kh}/bin/kafka-configs.sh --bootstrap-server {bootstrap} "
            f"--alter --entity-type topics --entity-name {topic_name} "
            f"--add-config {config_str}"
        )

        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(host, cmd)
        if exit_code != 0:
            raise ValueError(f"Failed to alter topic config: {stderr}")

        KafkaAdmin._audit(db, cluster_id, "topic_config_altered", "topic", topic_name, str(configs), actor=actor)
        db.commit()

        return {"topic": topic_name, "updated": True, "configs": configs}

    @staticmethod
    def increase_partitions(cluster_id: str, topic_name: str, new_count: int, db: Session,
                              actor: "User | None" = None) -> dict:
        """Increase the partition count for an existing topic."""
        ext = _is_external(cluster_id, db)
        if ext:
            result = external_admin.increase_partitions(ext, topic_name, new_count)
            KafkaAdmin._audit(db, cluster_id, "topic_partitions_increased", "topic", topic_name, f"to {new_count}", actor=actor)
            db.commit()
            return result
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        # Validate new_count > current partitions
        detail = KafkaAdmin.get_topic_detail(cluster_id, topic_name, db)
        current = detail.get("partitions", 0)
        if new_count <= current:
            raise ValueError(
                f"New partition count ({new_count}) must be greater than current ({current})"
            )

        cmd = (
            f"{kh}/bin/kafka-topics.sh --bootstrap-server {bootstrap} "
            f"--alter --topic {topic_name} --partitions {new_count}"
        )

        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(host, cmd)
        if exit_code != 0:
            raise ValueError(f"Failed to increase partitions: {stderr}")

        KafkaAdmin._audit(db, cluster_id, "topic_partitions_increased", "topic", topic_name,
                          f"from {current} to {new_count}", actor=actor)
        db.commit()

        return {"topic": topic_name, "partitions": new_count, "updated": True}

    # ── Consumer Groups ──────────────────────────────────

    @staticmethod
    def list_consumer_groups(cluster_id: str, db: Session) -> list[dict]:
        ext = _is_external(cluster_id, db)
        if ext:
            return external_admin.list_consumer_groups_with_lag(ext)
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        # First get the list of groups
        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(
            host, f"{kh}/bin/kafka-consumer-groups.sh --bootstrap-server {bootstrap} --list"
        )
        if exit_code != 0:
            raise ValueError(f"Failed to list consumer groups: {stderr}")

        # APB issue v1.2.0 #4 — kafka-consumer-groups.sh occasionally interleaves
        # log lines, warnings, or "Error:" stderr into stdout. Without filtering,
        # these end up in the UI as if they were group IDs (e.g. "[2024-01-05
        # 10:00:00,123] WARN..." appears as a group). Restrict to lines that
        # look like real Kafka group IDs: non-empty, no whitespace, ASCII
        # printable, ≤ 256 chars (Kafka's max).
        import re as _re
        _GROUP_ID_PATTERN = _re.compile(r'^[A-Za-z0-9._\-]+$')
        groups = []
        for raw in stdout.splitlines():
            g = raw.strip()
            if not g:
                continue
            # Skip obvious noise
            low = g.lower()
            if (
                g.startswith("[") or g.startswith("WARN") or g.startswith("INFO")
                or g.startswith("ERROR") or g.startswith("Error:") or low.startswith("exception")
                or "log4j" in low or " is deprecated " in low
                or g.startswith("(") or " " in g
            ):
                continue
            if len(g) > 256:
                continue
            # Final strict pattern check
            if not _GROUP_ID_PATTERN.match(g):
                continue
            groups.append(g)
        if not groups:
            return []

        # Batch describe all groups in a single SSH call
        group_args = " ".join(f"--group {g}" for g in groups)
        exit_code2, stdout2, stderr2 = KafkaAdmin._run_kafka_cmd(
            host,
            f"{kh}/bin/kafka-consumer-groups.sh --bootstrap-server {bootstrap} --describe {group_args}",
            timeout=60,
        )

        if exit_code2 != 0:
            # Fallback: return basic info without details
            return [{"group_id": g, "state": "Unknown", "members": 0, "topics": [], "offsets": []} for g in groups]

        # Parse all groups from the combined output
        result = []
        current_group = None
        current_lines: list[str] = []

        for line in stdout2.splitlines():
            if line.startswith("GROUP") or not line.strip():
                continue
            # New group section detected by group name in first column
            parts = line.split()
            if len(parts) >= 6:
                gid = parts[0]
                if gid != current_group:
                    if current_group and current_lines:
                        result.append(KafkaAdmin._parse_consumer_group(current_group, "\n".join(current_lines)))
                    current_group = gid
                    current_lines = []
                current_lines.append(line)
            elif "has no active members" in line:
                for g in groups:
                    if g in line:
                        result.append({"group_id": g, "state": "Empty", "members": 0, "topics": [], "offsets": []})
                        break

        if current_group and current_lines:
            result.append(KafkaAdmin._parse_consumer_group(current_group, "\n".join(current_lines)))

        # Add any groups that weren't in the describe output
        found_ids = {r["group_id"] for r in result}
        for g in groups:
            if g not in found_ids:
                result.append({"group_id": g, "state": "Unknown", "members": 0, "topics": [], "offsets": []})

        return result

    @staticmethod
    def get_consumer_group_detail(cluster_id: str, group_id: str, db: Session) -> dict:
        ext = _is_external(cluster_id, db)
        if ext:
            return external_admin.get_consumer_group_detail(ext, group_id)
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(
            host, f"{kh}/bin/kafka-consumer-groups.sh --bootstrap-server {bootstrap} --describe --group {group_id}"
        )
        if exit_code != 0:
            raise ValueError(f"Failed to describe group: {stderr}")

        return KafkaAdmin._parse_consumer_group(group_id, stdout)

    # ── Produce ──────────────────────────────────────────

    @staticmethod
    def produce_message(cluster_id: str, topic: str, key: str | None, value: str, db: Session) -> dict:
        ext = _is_external(cluster_id, db)
        if ext:
            return external_admin.produce_message(ext, topic, key, value)
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        # Escape value for shell
        safe_value = value.replace("'", "'\\''")
        if key:
            safe_key = key.replace("'", "'\\''")
            cmd = (
                f"echo '{safe_key}:{safe_value}' | {kh}/bin/kafka-console-producer.sh "
                f"--bootstrap-server {bootstrap} --topic {topic} "
                f"--property parse.key=true --property key.separator=:"
            )
        else:
            cmd = (
                f"echo '{safe_value}' | {kh}/bin/kafka-console-producer.sh "
                f"--bootstrap-server {bootstrap} --topic {topic}"
            )

        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(host, cmd, timeout=15)
        if exit_code != 0:
            return {"success": False, "message": f"Failed: {stderr}"}
        return {"success": True, "message": "Message produced successfully"}

    # ── Consume ──────────────────────────────────────────

    @staticmethod
    def consume_messages_dispatch_external(cluster_id, db, topic, max_messages, timeout_ms, from_beginning):
        """Tiny shim because consume_messages takes many args; keeps the block tidy."""
        ext = _is_external(cluster_id, db)
        if ext:
            return external_admin.consume_messages(ext, topic, max_messages, timeout_ms, from_beginning)
        return None

    @staticmethod
    def consume_messages(
        cluster_id: str, topic: str, db: Session,
        from_beginning: bool = False,
        max_messages: int = 10,
        group_id: str | None = None,
        timeout_ms: int = 10000,
    ) -> list[dict]:
        """Consume messages from a topic and return with full metadata."""
        ext_msgs = KafkaAdmin.consume_messages_dispatch_external(
            cluster_id, db, topic, max_messages, timeout_ms, from_beginning,
        )
        if ext_msgs is not None:
            return ext_msgs
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        cmd_parts = [
            f"{kh}/bin/kafka-console-consumer.sh",
            f"--bootstrap-server {bootstrap}",
            f"--topic {topic}",
            f"--max-messages {max_messages}",
            f"--timeout-ms {timeout_ms}",
            "--property print.timestamp=true",
            "--property print.key=true",
            "--property print.offset=true",
            "--property print.partition=true",
            "--property print.headers=true",
        ]
        if from_beginning:
            cmd_parts.append("--from-beginning")
        if group_id:
            safe_gid = group_id.replace("'", "'\\''")
            cmd_parts.append(f"--group '{safe_gid}'")

        # APB v1.4.3 #8 — redirect kafka-console-consumer stderr to
        # /dev/null. Kafka 4.x's console-consumer occasionally prints
        # log4j2 StatusLogger warnings + "Reconfiguration failed"
        # noise to stderr even on healthy consumes; SSH merged
        # stdout+stderr in some edge cases and the UI showed those
        # log lines as message values. We only want the actual
        # records on stdout.
        cmd = " ".join(cmd_parts) + " 2>/dev/null"
        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(host, cmd, timeout=30)

        messages = []
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            # Defensive: skip lines that obviously look like log4j2
            # records — anything starting with "[YYYY-..." or "INFO/" /
            # "ERROR/" is a stray log, not a real Kafka record. With
            # 2>/dev/null this shouldn't happen, but in case kafka-
            # console-consumer changes its output format, this
            # backstop keeps the consume tab clean.
            import re as _re
            if _re.match(r"^\[\d{4}-\d{2}-\d{2}", line):
                continue
            if _re.match(r"^(INFO|WARN|ERROR|FATAL|DEBUG)\s", line):
                continue
            if "StatusLogger" in line or "Reconfiguration failed" in line:
                continue
            msg = KafkaAdmin._parse_consumer_line(line)
            if msg:
                messages.append(msg)
        return messages

    @staticmethod
    def _parse_consumer_line(line: str) -> dict | None:
        """Parse a kafka-console-consumer line with print.* properties enabled.

        Actual Kafka 3.9+ output format with all print.* properties:
        CreateTime:<ts>\tPartition:<p>\tOffset:<o>\t<headers>\t<key>\t<value>

        The headers field comes BEFORE key and value when print.headers=true.
        Headers is "NO_HEADERS" when no headers exist.
        """
        try:
            parts = line.split("\t")
            if len(parts) < 5:
                # Fallback: treat entire line as value
                return {"timestamp": None, "partition": None, "offset": None, "key": None, "value": line, "headers": None}

            timestamp_raw = parts[0]
            partition_raw = parts[1]
            offset_raw = parts[2]
            # Kafka outputs: headers, key, value (in that order)
            headers_raw = parts[3]
            key_raw = parts[4]
            value_raw = parts[5] if len(parts) > 5 else ""

            timestamp = None
            if ":" in timestamp_raw:
                ts_val = timestamp_raw.split(":", 1)[1].strip()
                try:
                    timestamp = int(ts_val)
                except ValueError:
                    timestamp = ts_val

            partition = None
            if ":" in partition_raw:
                try:
                    partition = int(partition_raw.split(":", 1)[1].strip())
                except ValueError:
                    pass

            offset = None
            if ":" in offset_raw:
                try:
                    offset = int(offset_raw.split(":", 1)[1].strip())
                except ValueError:
                    pass

            key = key_raw if key_raw and key_raw != "null" else None
            value = value_raw
            headers = headers_raw if headers_raw and headers_raw != "NO_HEADERS" else None

            return {
                "timestamp": timestamp,
                "partition": partition,
                "offset": offset,
                "key": key,
                "value": value,
                "headers": headers,
            }
        except Exception:
            return {"timestamp": None, "partition": None, "offset": None, "key": None, "value": line, "headers": None}

    # ── Validate Cluster ──────────────────────────────────

    @staticmethod
    def validate_cluster(cluster_id: str, db: Session, create_test_topic: bool = True) -> dict:
        """Run post-install validation: list topics, optionally create test topic, produce & consume."""
        ext = _is_external(cluster_id, db)
        if ext:
            return external_admin.validate_cluster(ext, create_test_topic=create_test_topic)
        results: dict = {"steps": [], "success": True}

        # Step 1: List topics
        try:
            topics = KafkaAdmin.list_topics(cluster_id, db)
            results["steps"].append({
                "step": "list_topics",
                "success": True,
                "message": f"Found {len(topics)} topic(s)",
                "data": [t["name"] for t in topics],
            })
        except Exception as e:
            results["steps"].append({"step": "list_topics", "success": False, "message": str(e)})
            results["success"] = False
            return results

        # Step 2: Create test topic (optional)
        test_topic = "__tantor_validation_test"
        if create_test_topic:
            try:
                KafkaAdmin.create_topic(cluster_id, test_topic, 1, 1, {}, db)
                results["steps"].append({
                    "step": "create_test_topic",
                    "success": True,
                    "message": f"Created topic '{test_topic}'",
                })
            except Exception as e:
                # Topic might already exist
                if "already exists" in str(e).lower():
                    results["steps"].append({
                        "step": "create_test_topic",
                        "success": True,
                        "message": f"Topic '{test_topic}' already exists",
                    })
                else:
                    results["steps"].append({"step": "create_test_topic", "success": False, "message": str(e)})
                    results["success"] = False
                    return results

        # Step 3: Produce test message
        import time
        test_value = f'{{"validation": true, "timestamp": {int(time.time())}}}'
        try:
            produce_result = KafkaAdmin.produce_message(cluster_id, test_topic, "validation-key", test_value, db)
            results["steps"].append({
                "step": "produce_message",
                "success": produce_result["success"],
                "message": produce_result["message"],
            })
            if not produce_result["success"]:
                results["success"] = False
                return results
        except Exception as e:
            results["steps"].append({"step": "produce_message", "success": False, "message": str(e)})
            results["success"] = False
            return results

        # Step 4: Consume and verify
        try:
            import time as _t
            _t.sleep(2)  # Give broker a moment to commit
            messages = KafkaAdmin.consume_messages(
                cluster_id, test_topic, db,
                from_beginning=True, max_messages=5, timeout_ms=15000,
            )
            found = any("validation" in (m.get("value") or "") for m in messages)
            results["steps"].append({
                "step": "consume_message",
                "success": found,
                "message": f"Consumed {len(messages)} message(s), validation message {'found' if found else 'not found'}",
                "data": messages,
            })
            if not found:
                results["success"] = False
        except Exception as e:
            results["steps"].append({"step": "consume_message", "success": False, "message": str(e)})
            results["success"] = False

        # Step 5: Cleanup test topic
        if create_test_topic:
            try:
                KafkaAdmin.delete_topic(cluster_id, test_topic, db)
                results["steps"].append({"step": "cleanup", "success": True, "message": "Cleaned up test topic"})
            except Exception:
                results["steps"].append({"step": "cleanup", "success": True, "message": "Test topic cleanup skipped"})

        return results

    # ── Parsers ──────────────────────────────────────────

    @staticmethod
    def _parse_all_topics_describe(raw: str) -> list[dict]:
        """Parse kafka-topics.sh --describe output for ALL topics at once.

        The output contains multiple topic blocks separated by header lines.
        Each block starts with 'Topic: <name>\tPartitionCount:...'
        """
        if not raw.strip():
            return []

        topics = []
        current_name = None
        current_lines: list[str] = []

        for line in raw.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("Topic:") and "PartitionCount:" in stripped:
                # New topic header — save previous topic if any
                if current_name is not None:
                    topics.append(KafkaAdmin._parse_topic_describe(current_name, "\n".join(current_lines)))
                # Extract topic name
                name_part = stripped.split("\t")[0]
                current_name = name_part.split(":", 1)[1].strip()
                current_lines = [stripped]
            elif stripped.startswith("Topic:") and "Partition:" in stripped:
                current_lines.append(stripped)

        # Don't forget the last topic
        if current_name is not None:
            topics.append(KafkaAdmin._parse_topic_describe(current_name, "\n".join(current_lines)))

        return topics

    @staticmethod
    def _parse_topic_describe(topic_name: str, raw: str) -> dict:
        """Parse kafka-topics.sh --describe output."""
        lines = raw.strip().splitlines()
        partitions = 0
        replication_factor = 0
        configs = {}
        partition_details = []

        for line in lines:
            line = line.strip()
            if line.startswith("Topic:") and "PartitionCount:" in line:
                # Header line
                parts = line.split("\t")
                for part in parts:
                    part = part.strip()
                    if part.startswith("PartitionCount:"):
                        partitions = int(part.split(":")[1].strip())
                    elif part.startswith("ReplicationFactor:"):
                        replication_factor = int(part.split(":")[1].strip())
                    elif part.startswith("Configs:"):
                        cfg_str = part.split(":", 1)[1].strip()
                        if cfg_str:
                            for pair in cfg_str.split(","):
                                if "=" in pair:
                                    k, v = pair.split("=", 1)
                                    configs[k.strip()] = v.strip()
            elif line.startswith("Topic:") and "Partition:" in line:
                # Partition detail line
                parts = line.split("\t")
                detail = {}
                for part in parts:
                    part = part.strip()
                    if part.startswith("Partition:"):
                        detail["partition"] = int(part.split(":")[1].strip())
                    elif part.startswith("Leader:"):
                        detail["leader"] = int(part.split(":")[1].strip())
                    elif part.startswith("Replicas:"):
                        detail["replicas"] = [int(x) for x in part.split(":")[1].strip().split(",")]
                    elif part.startswith("Isr:"):
                        detail["isr"] = [int(x) for x in part.split(":")[1].strip().split(",")]
                if detail:
                    partition_details.append(detail)

        return {
            "name": topic_name,
            "partitions": partitions,
            "replication_factor": replication_factor,
            "configs": configs or None,
            "partition_details": partition_details,
        }

    @staticmethod
    def _parse_consumer_group(group_id: str, raw: str) -> dict:
        """Parse kafka-consumer-groups.sh --describe output."""
        lines = raw.strip().splitlines()
        offsets = []
        topics = set()
        members = set()
        state = "Unknown"

        for line in lines:
            if line.startswith("GROUP") or line.startswith("Consumer group"):
                continue
            if "has no active members" in line:
                state = "Empty"
                continue

            parts = line.split()
            if len(parts) >= 6:
                try:
                    topic = parts[1]
                    partition = int(parts[2])
                    current = int(parts[3]) if parts[3] != "-" else 0
                    end = int(parts[4]) if parts[4] != "-" else 0
                    lag = int(parts[5]) if parts[5] != "-" else 0
                    consumer_id = parts[6] if len(parts) > 6 else ""
                    topics.add(topic)
                    if consumer_id and consumer_id != "-":
                        members.add(consumer_id)
                    offsets.append({
                        "topic": topic,
                        "partition": partition,
                        "current_offset": current,
                        "log_end_offset": end,
                        "lag": lag,
                    })
                    if state == "Unknown":
                        state = "Stable" if consumer_id and consumer_id != "-" else "Empty"
                except (ValueError, IndexError):
                    continue

        return {
            "group_id": group_id,
            "state": state,
            "members": len(members),
            "topics": list(topics),
            "offsets": offsets,
        }


    # ── SCRAM User Management ──────────────────────────────

    @staticmethod
    def list_scram_users(cluster_id: str, db: Session) -> list[dict]:
        """List all SCRAM users configured in Kafka, enriched with local DB metadata."""
        ext = _is_external(cluster_id, db)
        if ext:
            return external_admin.list_scram_users(ext)
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(
            host,
            f"{kh}/bin/kafka-configs.sh --bootstrap-server {bootstrap} --describe --entity-type users",
        )
        if exit_code != 0:
            raise ValueError(f"Failed to list users: {stderr}")

        kafka_users = KafkaAdmin._parse_scram_users(stdout)

        # Enrich with local DB records
        local_users = db.query(KafkaUser).filter(KafkaUser.cluster_id == cluster_id).all()
        local_map = {u.username: u for u in local_users}

        result = []
        for ku in kafka_users:
            local = local_map.get(ku["username"])
            result.append({
                "id": local.id if local else "",
                "cluster_id": cluster_id,
                "username": ku["username"],
                "mechanism": ku["mechanism"],
                "created_at": local.created_at if local else None,
                "updated_at": local.updated_at if local else None,
            })
        return result

    @staticmethod
    def create_scram_user(cluster_id: str, username: str, password: str | None, mechanism: str, db: Session,
                            actor: "User | None" = None) -> dict:
        """Create a SCRAM user in Kafka and store encrypted password locally."""
        if not password:
            password = secrets.token_urlsafe(24)
        ext = _is_external(cluster_id, db)
        if ext:
            external_admin.create_scram_user(ext, username, password, mechanism)
            kafka_user = KafkaUser(
                cluster_id=cluster_id, username=username, mechanism=mechanism,
                encrypted_password=encrypt(password),
            )
            db.add(kafka_user)
            KafkaAdmin._audit(db, cluster_id, "user_created", "user", username, f"mechanism={mechanism}", actor=actor)
            db.commit()
            db.refresh(kafka_user)
            return {
                "id": kafka_user.id, "username": username, "mechanism": mechanism, "password": password,
            }
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        safe_password = password.replace("'", "'\\''")

        cmd = (
            f"{kh}/bin/kafka-configs.sh --bootstrap-server {bootstrap} "
            f"--alter --add-config '{mechanism}=[password={safe_password}]' "
            f"--entity-type users --entity-name {username}"
        )

        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(host, cmd)
        if exit_code != 0:
            raise ValueError(f"Failed to create user: {stderr}")

        # Store in local DB
        kafka_user = KafkaUser(
            cluster_id=cluster_id,
            username=username,
            mechanism=mechanism,
            encrypted_password=encrypt(password),
        )
        db.add(kafka_user)

        KafkaAdmin._audit(db, cluster_id, "user_created", "user", username, f"mechanism={mechanism}", actor=actor)
        db.commit()
        db.refresh(kafka_user)

        return {
            "id": kafka_user.id,
            "username": username,
            "mechanism": mechanism,
            "password": password,
            "message": f"User '{username}' created with {mechanism}",
        }

    @staticmethod
    def delete_scram_user(cluster_id: str, username: str, db: Session,
                            actor: "User | None" = None) -> dict:
        """Delete a SCRAM user from Kafka and remove from local DB."""
        ext = _is_external(cluster_id, db)
        if ext:
            external_admin.delete_scram_user(ext, username)
            db.query(KafkaUser).filter(
                KafkaUser.cluster_id == cluster_id, KafkaUser.username == username,
            ).delete()
            KafkaAdmin._audit(db, cluster_id, "user_deleted", "user", username, "", actor=actor)
            db.commit()
            return {"username": username, "deleted": True}
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        # Try both mechanisms to ensure full removal
        for mech in ["SCRAM-SHA-256", "SCRAM-SHA-512"]:
            cmd = (
                f"{kh}/bin/kafka-configs.sh --bootstrap-server {bootstrap} "
                f"--alter --delete-config '{mech}' "
                f"--entity-type users --entity-name {username}"
            )
            KafkaAdmin._run_kafka_cmd(host, cmd)

        # Remove from local DB
        db.query(KafkaUser).filter(
            KafkaUser.cluster_id == cluster_id,
            KafkaUser.username == username,
        ).delete()

        KafkaAdmin._audit(db, cluster_id, "user_deleted", "user", username, actor=actor)
        db.commit()

        return {"username": username, "deleted": True, "message": f"User '{username}' deleted"}

    @staticmethod
    def rotate_scram_password(cluster_id: str, username: str, password: str | None, db: Session,
                                actor: "User | None" = None) -> dict:
        """Rotate the password for an existing SCRAM user."""
        from datetime import datetime, timezone

        if not password:
            password = secrets.token_urlsafe(24)
        ext = _is_external(cluster_id, db)
        if ext:
            # Look up the existing mechanism from local DB so we don't change it.
            existing = db.query(KafkaUser).filter(
                KafkaUser.cluster_id == cluster_id, KafkaUser.username == username,
            ).first()
            mech = existing.mechanism if existing else "SCRAM-SHA-512"
            external_admin.create_scram_user(ext, username, password, mech)
            if existing:
                existing.encrypted_password = encrypt(password)
                existing.updated_at = datetime.now(timezone.utc)
            KafkaAdmin._audit(db, cluster_id, "user_password_rotated", "user", username, "", actor=actor)
            db.commit()
            return {"username": username, "password": password, "rotated": True}

        local_user = db.query(KafkaUser).filter(
            KafkaUser.cluster_id == cluster_id,
            KafkaUser.username == username,
        ).first()

        mechanism = local_user.mechanism if local_user else "SCRAM-SHA-256"

        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)
        safe_password = password.replace("'", "'\\''")

        cmd = (
            f"{kh}/bin/kafka-configs.sh --bootstrap-server {bootstrap} "
            f"--alter --add-config '{mechanism}=[password={safe_password}]' "
            f"--entity-type users --entity-name {username}"
        )

        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(host, cmd)
        if exit_code != 0:
            raise ValueError(f"Failed to rotate password: {stderr}")

        # Update local DB
        if local_user:
            local_user.encrypted_password = encrypt(password)
            local_user.updated_at = datetime.now(timezone.utc)

        KafkaAdmin._audit(db, cluster_id, "user_password_rotated", "user", username, actor=actor)
        db.commit()

        return {
            "username": username,
            "mechanism": mechanism,
            "password": password,
            "message": f"Password rotated for '{username}'",
        }

    # ── ACL Management ──────────────────────────────────

    @staticmethod
    def list_acls(
        cluster_id: str, db: Session,
        principal: str | None = None,
        resource_type: str | None = None,
        resource_name: str | None = None,
    ) -> list[dict]:
        """List ACLs from Kafka, optionally filtered."""
        ext = _is_external(cluster_id, db)
        if ext:
            return external_admin.list_acls(ext, principal, resource_type, resource_name)
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        cmd = f"{kh}/bin/kafka-acls.sh --bootstrap-server {bootstrap} --list"
        if principal:
            cmd += f" --principal {principal}"
        if resource_type and resource_name:
            rt = resource_type.lower()
            if rt == "topic":
                cmd += f" --topic {resource_name}"
            elif rt == "group":
                cmd += f" --group {resource_name}"
            elif rt == "cluster":
                cmd += " --cluster"
            elif rt == "transactional-id":
                cmd += f" --transactional-id {resource_name}"

        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(host, cmd, timeout=15)
        if exit_code != 0:
            raise ValueError(f"Failed to list ACLs: {stderr}")

        return KafkaAdmin._parse_acl_list(stdout)

    @staticmethod
    def create_acl(cluster_id: str, acl_req: dict, db: Session,
                    actor: "User | None" = None) -> dict:
        """Create one or more ACL entries in Kafka."""
        ext = _is_external(cluster_id, db)
        if ext:
            # The managed CLI accepts a list of operations; loop them through
            # the kafka-python single-ACL API so the user-facing shape stays
            # the same regardless of cluster.kind.
            results = []
            for op in acl_req["operations"]:
                req = dict(acl_req)
                req["operation"] = op
                results.append(external_admin.create_acl(ext, req))
            for op in acl_req["operations"]:
                KafkaAdmin._audit(
                    db, cluster_id, "acl_created", "acl",
                    f"{acl_req['principal']}::{acl_req['resource_type']}:{acl_req['resource_name']}::{op}",
                    f"permission={acl_req.get('permission_type', 'Allow')}",
                    actor=actor,
                )
            db.commit()
            return {
                "success": True,
                "message": f"ACL(s) created via kafka-python AdminClient on external cluster",
                "acls_added": len(results),
            }
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        principal = acl_req["principal"]
        resource_type = acl_req["resource_type"].lower()
        resource_name = acl_req["resource_name"]
        pattern_type = acl_req.get("pattern_type", "literal")
        operations = acl_req["operations"]
        permission_type = acl_req.get("permission_type", "Allow")
        acl_host = acl_req.get("host", "*")

        permission_flag = "--allow-principal" if permission_type == "Allow" else "--deny-principal"

        cmd_parts = [
            f"{kh}/bin/kafka-acls.sh",
            f"--bootstrap-server {bootstrap}",
            "--add",
            f"{permission_flag} {principal}",
        ]

        if resource_type == "topic":
            cmd_parts.append(f"--topic {resource_name}")
        elif resource_type == "group":
            cmd_parts.append(f"--group {resource_name}")
        elif resource_type == "cluster":
            cmd_parts.append("--cluster")
        elif resource_type == "transactional-id":
            cmd_parts.append(f"--transactional-id {resource_name}")

        for op in operations:
            cmd_parts.append(f"--operation {op}")

        cmd_parts.append(f"--resource-pattern-type {pattern_type}")

        if acl_host != "*":
            cmd_parts.append(f"--host {acl_host}")

        cmd = " ".join(cmd_parts)
        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(host, cmd)
        if exit_code != 0:
            raise ValueError(f"Failed to create ACL: {stderr}")

        details = json.dumps({
            "principal": principal,
            "resource_type": resource_type,
            "resource_name": resource_name,
            "operations": operations,
            "pattern_type": pattern_type,
            "permission_type": permission_type,
        })
        KafkaAdmin._audit(db, cluster_id, "acl_created", "acl", f"{principal}:{resource_name}", details, actor=actor)
        db.commit()

        return {"success": True, "message": stdout.strip() or "ACL(s) created", "acls_added": len(operations)}

    @staticmethod
    def delete_acl(cluster_id: str, acl_req: dict, db: Session,
                    actor: "User | None" = None) -> dict:
        """Delete ACL entries from Kafka."""
        ext = _is_external(cluster_id, db)
        if ext:
            for op in acl_req["operations"]:
                req = dict(acl_req)
                req["operation"] = op
                external_admin.delete_acl(ext, req)
                KafkaAdmin._audit(
                    db, cluster_id, "acl_deleted", "acl",
                    f"{acl_req['principal']}::{acl_req['resource_type']}:{acl_req['resource_name']}::{op}",
                    "",
                    actor=actor,
                )
            db.commit()
            return {
                "success": True,
                "message": "ACL(s) deleted via kafka-python AdminClient on external cluster",
                "acls_deleted": len(acl_req["operations"]),
            }
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        principal = acl_req["principal"]
        resource_type = acl_req["resource_type"].lower()
        resource_name = acl_req["resource_name"]
        pattern_type = acl_req.get("pattern_type", "literal")
        operations = acl_req["operations"]
        permission_type = acl_req.get("permission_type", "Allow")

        permission_flag = "--allow-principal" if permission_type == "Allow" else "--deny-principal"

        cmd_parts = [
            f"{kh}/bin/kafka-acls.sh",
            f"--bootstrap-server {bootstrap}",
            "--remove",
            f"{permission_flag} {principal}",
        ]

        if resource_type == "topic":
            cmd_parts.append(f"--topic {resource_name}")
        elif resource_type == "group":
            cmd_parts.append(f"--group {resource_name}")
        elif resource_type == "cluster":
            cmd_parts.append("--cluster")
        elif resource_type == "transactional-id":
            cmd_parts.append(f"--transactional-id {resource_name}")

        for op in operations:
            cmd_parts.append(f"--operation {op}")

        cmd_parts.append(f"--resource-pattern-type {pattern_type}")
        cmd_parts.append("--force")  # Skip confirmation prompt

        cmd = " ".join(cmd_parts)
        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(host, cmd)
        if exit_code != 0:
            raise ValueError(f"Failed to delete ACL: {stderr}")

        details = json.dumps({
            "principal": principal,
            "resource_type": resource_type,
            "resource_name": resource_name,
            "operations": operations,
        })
        KafkaAdmin._audit(db, cluster_id, "acl_deleted", "acl", f"{principal}:{resource_name}", details, actor=actor)
        db.commit()

        return {"success": True, "message": stdout.strip() or "ACL(s) deleted"}

    # ── Security Parsers ──────────────────────────────────

    @staticmethod
    def _parse_scram_users(raw: str) -> list[dict]:
        """Parse kafka-configs.sh --describe --entity-type users output.

        Example lines:
          SCRAM credential configs for user-principal 'myuser' are SCRAM-SHA-256=...
        """
        users = []
        for line in raw.strip().splitlines():
            line = line.strip()
            if not line or "SCRAM credential configs" not in line:
                continue
            username_match = re.search(r"'([^']+)'", line)
            if not username_match:
                continue
            username = username_match.group(1)
            mechanism = "SCRAM-SHA-512" if "SCRAM-SHA-512" in line else "SCRAM-SHA-256"
            users.append({"username": username, "mechanism": mechanism})
        return users

    @staticmethod
    def _parse_acl_list(raw: str) -> list[dict]:
        """Parse kafka-acls.sh --list output.

        Example:
        Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=my-topic, patternType=LITERAL)`:
            (principal=User:myuser, host=*, operation=READ, permissionType=ALLOW)
        """
        acls = []
        current_resource_type = ""
        current_resource_name = ""
        current_pattern_type = ""

        for line in raw.strip().splitlines():
            line = line.strip()

            resource_match = re.search(
                r"ResourcePattern\(resourceType=(\w+),\s*name=([^,]+),\s*patternType=(\w+)\)",
                line,
            )
            if resource_match:
                current_resource_type = resource_match.group(1).lower()
                current_resource_name = resource_match.group(2).strip()
                current_pattern_type = resource_match.group(3).lower()
                continue

            acl_match = re.search(
                r"\(principal=([^,]+),\s*host=([^,]+),\s*operation=([^,]+),\s*permissionType=(\w+)\)",
                line,
            )
            if acl_match:
                acls.append({
                    "principal": acl_match.group(1).strip(),
                    "host": acl_match.group(2).strip(),
                    "operation": acl_match.group(3).strip(),
                    "permission_type": acl_match.group(4).strip().capitalize(),
                    "resource_type": current_resource_type,
                    "resource_name": current_resource_name,
                    "pattern_type": current_pattern_type,
                })

        return acls

    # ── Partition Rebalancing ─────────────────────────────

    @staticmethod
    def get_partition_distribution(cluster_id: str, db: Session) -> dict:
        """Describe all topics and build broker-to-partition distribution map."""
        if _is_external(cluster_id, db):
            _external_only_unsupported("Partition rebalance/reassignment")
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(
            host, f"{kh}/bin/kafka-topics.sh --bootstrap-server {bootstrap} --describe",
            timeout=60,
        )
        if exit_code != 0:
            raise ValueError(f"Failed to describe topics: {stderr}")

        broker_leaders: dict[int, int] = {}
        broker_replicas: dict[int, int] = {}
        topics: dict[str, list[dict]] = {}

        for line in stdout.strip().splitlines():
            line = line.strip()
            if not line.startswith("Topic:") or "Partition:" not in line:
                continue
            parts = line.split("\t")
            topic_name = ""
            partition = 0
            leader = -1
            replicas: list[int] = []
            isr: list[int] = []
            for part in parts:
                part = part.strip()
                if part.startswith("Topic:"):
                    topic_name = part.split(":", 1)[1].strip()
                elif part.startswith("Partition:"):
                    partition = int(part.split(":")[1].strip())
                elif part.startswith("Leader:"):
                    leader = int(part.split(":")[1].strip())
                elif part.startswith("Replicas:"):
                    replicas = [int(x) for x in part.split(":")[1].strip().split(",")]
                elif part.startswith("Isr:"):
                    isr = [int(x) for x in part.split(":")[1].strip().split(",")]

            if topic_name:
                topics.setdefault(topic_name, [])
                topics[topic_name].append({
                    "partition": partition,
                    "leader": leader,
                    "replicas": replicas,
                    "isr": isr,
                })
                if leader >= 0:
                    broker_leaders[leader] = broker_leaders.get(leader, 0) + 1
                for r in replicas:
                    broker_replicas[r] = broker_replicas.get(r, 0) + 1

        all_broker_ids = sorted(set(list(broker_leaders.keys()) + list(broker_replicas.keys())))
        brokers = [
            {
                "broker_id": bid,
                "leader_count": broker_leaders.get(bid, 0),
                "replica_count": broker_replicas.get(bid, 0),
            }
            for bid in all_broker_ids
        ]

        topic_list = [
            {"name": name, "partitions": parts}
            for name, parts in sorted(topics.items())
        ]

        return {"brokers": brokers, "topics": topic_list}

    @staticmethod
    def generate_reassignment_plan(cluster_id: str, topics: list[str], broker_ids: list[int], db: Session) -> dict:
        """Generate a partition reassignment plan using kafka-reassign-partitions.sh."""
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        topics_json = json.dumps({"topics": [{"topic": t} for t in topics], "version": 1})
        broker_list = ",".join(str(b) for b in broker_ids)

        # Write topics JSON to temp file on broker
        safe_json = topics_json.replace("'", "'\\''")
        write_cmd = f"echo '{safe_json}' > /tmp/tantor_topics_to_move.json"
        exit_code, _, stderr = KafkaAdmin._run_kafka_cmd(host, write_cmd)
        if exit_code != 0:
            raise ValueError(f"Failed to write topics file: {stderr}")

        cmd = (
            f"{kh}/bin/kafka-reassign-partitions.sh "
            f"--bootstrap-server {bootstrap} "
            f"--topics-to-move-json-file /tmp/tantor_topics_to_move.json "
            f"--broker-list \"{broker_list}\" "
            f"--generate"
        )
        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(host, cmd, timeout=60)
        if exit_code != 0:
            raise ValueError(f"Failed to generate reassignment plan: {stderr}")

        current = {}
        proposed = {}
        lines = stdout.strip().splitlines()
        section = None
        for line in lines:
            line = line.strip()
            if "Current partition replica assignment" in line:
                section = "current"
                continue
            elif "Proposed partition reassignment configuration" in line:
                section = "proposed"
                continue

            if section and line.startswith("{"):
                try:
                    parsed = json.loads(line)
                    if section == "current":
                        current = parsed
                    elif section == "proposed":
                        proposed = parsed
                except json.JSONDecodeError:
                    pass

        return {"current": current, "proposed": proposed}

    @staticmethod
    def execute_reassignment(cluster_id: str, reassignment_json: dict, db: Session) -> dict:
        """Execute a partition reassignment plan."""
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        reassign_str = json.dumps(reassignment_json)
        safe_json = reassign_str.replace("'", "'\\''")
        write_cmd = f"echo '{safe_json}' > /tmp/tantor_reassignment.json"
        exit_code, _, stderr = KafkaAdmin._run_kafka_cmd(host, write_cmd)
        if exit_code != 0:
            raise ValueError(f"Failed to write reassignment file: {stderr}")

        cmd = (
            f"{kh}/bin/kafka-reassign-partitions.sh "
            f"--bootstrap-server {bootstrap} "
            f"--reassignment-json-file /tmp/tantor_reassignment.json "
            f"--execute"
        )
        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(host, cmd, timeout=120)
        if exit_code != 0:
            raise ValueError(f"Failed to execute reassignment: {stderr}")

        KafkaAdmin._audit(db, cluster_id, "partition_reassignment_executed", "cluster", cluster_id,
                          f"partitions: {len(reassignment_json.get('partitions', []))}")
        db.commit()

        return {"success": True, "message": stdout.strip() or "Reassignment started"}

    @staticmethod
    def verify_reassignment(cluster_id: str, reassignment_json: dict, db: Session) -> dict:
        """Verify the status of a partition reassignment."""
        host, bootstrap, kh = KafkaAdmin._get_broker_with_paths(cluster_id, db)

        reassign_str = json.dumps(reassignment_json)
        safe_json = reassign_str.replace("'", "'\\''")
        write_cmd = f"echo '{safe_json}' > /tmp/tantor_reassignment.json"
        exit_code, _, stderr = KafkaAdmin._run_kafka_cmd(host, write_cmd)
        if exit_code != 0:
            raise ValueError(f"Failed to write reassignment file: {stderr}")

        cmd = (
            f"{kh}/bin/kafka-reassign-partitions.sh "
            f"--bootstrap-server {bootstrap} "
            f"--reassignment-json-file /tmp/tantor_reassignment.json "
            f"--verify"
        )
        exit_code, stdout, stderr = KafkaAdmin._run_kafka_cmd(host, cmd, timeout=60)
        if exit_code != 0:
            raise ValueError(f"Failed to verify reassignment: {stderr}")

        results = []
        all_complete = True
        for line in stdout.strip().splitlines():
            line = line.strip()
            if not line.startswith("Status of partition reassignment"):
                # Parse lines like: "Reassignment of partition topic-0 is completed" or "still in progress"
                if "is completed" in line.lower() or "is complete" in line.lower():
                    results.append({"partition": line.split("partition")[1].split("is")[0].strip() if "partition" in line else line, "status": "completed"})
                elif "in progress" in line.lower():
                    results.append({"partition": line.split("partition")[1].split("is")[0].strip() if "partition" in line else line, "status": "in_progress"})
                    all_complete = False
                elif line:
                    results.append({"partition": line, "status": "unknown"})

        return {"complete": all_complete, "partitions": results, "raw": stdout.strip()}

    # ── Audit Helper ──────────────────────────────────────

    @staticmethod
    def _audit(db: Session, cluster_id: str, action: str, resource_type: str,
               resource_name: str, details: str | None = None,
               actor: "User | None" = None):
        """Create an audit log entry.

        actor is the calling User (from FastAPI dependency); we capture
        both id + username so the activity feed can show "alice rotated
        password for foo" even after the User row is later deleted.
        """
        log = AuditLog(
            cluster_id=cluster_id,
            action=action,
            resource_type=resource_type,
            resource_name=resource_name,
            details=details,
            actor_user_id=getattr(actor, "id", None) if actor else None,
            actor_username=getattr(actor, "username", None) if actor else None,
        )
        db.add(log)


kafka_admin = KafkaAdmin()
