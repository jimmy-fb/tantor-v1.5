"""Broker Configuration Management -- read/write Kafka broker configs via SSH."""
import logging
import re

from sqlalchemy.orm import Session

from app.models.cluster import Cluster
from app.models.service import Service
from app.models.host import Host
from app.models.config_audit import ConfigAuditLog
from app.services.ssh_manager import SSHManager
from app.config import settings

logger = logging.getLogger("tantor.config_manager")

# Define known Kafka broker configuration keys with descriptions and types
KAFKA_BROKER_CONFIGS = {
    # Core
    "log.retention.hours": {"type": "int", "description": "Hours to retain log segments", "dynamic": True, "category": "Log"},
    "log.retention.bytes": {"type": "long", "description": "Maximum size of the log before deletion", "dynamic": True, "category": "Log"},
    "log.segment.bytes": {"type": "int", "description": "Size of a single log segment file", "dynamic": True, "category": "Log"},
    "log.cleanup.policy": {"type": "string", "description": "Log cleanup policy: delete or compact", "dynamic": True, "category": "Log"},
    "num.partitions": {"type": "int", "description": "Default number of partitions per topic", "dynamic": False, "category": "Core"},
    "default.replication.factor": {"type": "int", "description": "Default replication factor for auto-created topics", "dynamic": False, "category": "Core"},
    "min.insync.replicas": {"type": "int", "description": "Minimum number of in-sync replicas", "dynamic": True, "category": "Replication"},
    "message.max.bytes": {"type": "int", "description": "Maximum size of a message", "dynamic": True, "category": "Network"},
    "compression.type": {"type": "string", "description": "Compression codec: none, gzip, snappy, lz4, zstd", "dynamic": True, "category": "Core"},
    "auto.create.topics.enable": {"type": "boolean", "description": "Enable auto creation of topics", "dynamic": False, "category": "Core"},
    "delete.topic.enable": {"type": "boolean", "description": "Enable topic deletion", "dynamic": False, "category": "Core"},
    "max.connections.per.ip": {"type": "int", "description": "Maximum connections per IP", "dynamic": True, "category": "Network"},
    "num.io.threads": {"type": "int", "description": "Number of I/O threads", "dynamic": False, "category": "Performance"},
    "num.network.threads": {"type": "int", "description": "Number of network threads", "dynamic": False, "category": "Performance"},
    "socket.send.buffer.bytes": {"type": "int", "description": "SO_SNDBUF buffer size", "dynamic": True, "category": "Network"},
    "socket.receive.buffer.bytes": {"type": "int", "description": "SO_RCVBUF buffer size", "dynamic": True, "category": "Network"},
    "replica.fetch.max.bytes": {"type": "int", "description": "Max bytes fetched per partition for replication", "dynamic": True, "category": "Replication"},
    "unclean.leader.election.enable": {"type": "boolean", "description": "Allow unclean leader election", "dynamic": True, "category": "Replication"},
    "log.retention.check.interval.ms": {"type": "long", "description": "Interval to check for log retention", "dynamic": True, "category": "Log"},
    "offsets.retention.minutes": {"type": "int", "description": "Offset retention time", "dynamic": True, "category": "Core"},
}


class ConfigManager:
    """Read and modify Kafka broker configurations via SSH (managed) or
    kafka-python AdminClient (external)."""

    def get_broker_configs(self, cluster_id: str, db: Session) -> list[dict]:
        """Get current server.properties from all brokers in the cluster."""
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise ValueError("Cluster not found")

        if (cluster.kind or "managed") == "external":
            from app.services import external_admin
            return [
                {
                    "broker_id": entry["broker_id"],
                    "host_ip": (cluster.bootstrap_servers or "").split(",")[0].strip(),
                    "service_id": "",
                    "configs": {c["name"]: c["value"] for c in entry["configs"] if c["value"] is not None},
                    "raw": "",
                }
                for entry in external_admin.describe_broker_configs(cluster)
            ]

        services = db.query(Service).filter(
            Service.cluster_id == cluster_id,
            Service.role.in_(["broker", "broker_controller"])
        ).all()

        results = []
        for svc in services:
            host = db.query(Host).filter(Host.id == svc.host_id).first()
            if not host:
                continue
            try:
                from app.services import cluster_paths
                with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                    config_path = f"{cluster_paths.install_dir(cluster)}/config/server.properties"
                    # Kafka's server.properties is owned kafka:kafka mode 640
                    # — the SSH user (ec2-user / tantor) can't read it without
                    # sudo. Use `sudo -n` so we never block on a password prompt.
                    exit_code, stdout, stderr = SSHManager.exec_command(
                        client, f"sudo -n cat {config_path}", timeout=15
                    )
                    if exit_code == 0:
                        configs = self._parse_properties(stdout)
                        results.append({
                            "broker_id": svc.node_id,
                            "host_ip": host.ip_address,
                            "service_id": svc.id,
                            "configs": configs,
                            "raw": stdout,
                        })
                    else:
                        results.append({
                            "broker_id": svc.node_id,
                            "host_ip": host.ip_address,
                            "service_id": svc.id,
                            "error": stderr or "Failed to read config",
                        })
            except Exception as e:
                results.append({
                    "broker_id": svc.node_id,
                    "host_ip": host.ip_address,
                    "service_id": svc.id,
                    "error": str(e),
                })
        return results

    def update_broker_config(
        self, cluster_id: str, broker_id: int, config_key: str,
        config_value: str, username: str, db: Session,
    ) -> dict:
        """Update a single config key on a specific broker's server.properties."""
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise ValueError("Cluster not found")

        # Validate config key
        if config_key not in KAFKA_BROKER_CONFIGS and not config_key.startswith("listener.") and not config_key.startswith("ssl."):
            logger.warning(f"Unknown config key: {config_key}")

        if (cluster.kind or "managed") == "external":
            from app.services import external_admin
            external_admin.alter_broker_config(cluster, broker_id, {config_key: config_value})
            # Audit log + return shape kept identical to the SSH path so the
            # frontend doesn't have to special-case external clusters.
            # NOTE: ConfigAuditLog is already imported at module scope. Don't
            # re-import here — Python would scope the name to this function
            # only, breaking the managed-path branch below with UnboundLocalError.
            audit = ConfigAuditLog(
                cluster_id=cluster_id, broker_id=broker_id,
                config_key=config_key, old_value=None, new_value=config_value,
                changed_by=username, change_type="update",
            )
            db.add(audit)
            db.commit()
            return {
                "broker_id": broker_id, "config_key": config_key, "new_value": config_value,
                "old_value": None, "audit_id": audit.id, "changed_by": username,
            }

        svc = db.query(Service).filter(
            Service.cluster_id == cluster_id,
            Service.node_id == broker_id,
            Service.role.in_(["broker", "broker_controller"])
        ).first()
        if not svc:
            raise ValueError(f"Broker {broker_id} not found in cluster")

        host = db.query(Host).filter(Host.id == svc.host_id).first()
        if not host:
            raise ValueError("Host not found")

        from app.services import cluster_paths
        config_path = f"{cluster_paths.install_dir(cluster)}/config/server.properties"

        with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
            # Read current config (sudo — file is owned by kafka:kafka mode 640)
            exit_code, stdout, stderr = SSHManager.exec_command(client, f"sudo -n cat {config_path}", timeout=15)
            if exit_code != 0:
                raise RuntimeError(f"Failed to read config: {stderr}")

            current_configs = self._parse_properties(stdout)
            old_value = current_configs.get(config_key)

            # Update the config file
            lines = stdout.splitlines()
            updated = False
            new_lines = []
            for line in lines:
                stripped = line.strip()
                if stripped.startswith(f"{config_key}=") or stripped.startswith(f"{config_key} ="):
                    new_lines.append(f"{config_key}={config_value}")
                    updated = True
                else:
                    new_lines.append(line)
            if not updated:
                new_lines.append(f"{config_key}={config_value}")

            new_content = "\n".join(new_lines) + "\n"

            # Write back. SFTP can't write a file the SSH user doesn't own,
            # so we stage in /tmp via SFTP then `sudo install` to the kafka
            # location preserving the kafka:kafka ownership and 640 mode.
            import uuid as _uuid
            tmp_path = f"/tmp/tantor-{_uuid.uuid4().hex}.properties"
            SSHManager.upload_content(client, new_content, tmp_path)
            install_cmd = (
                f"sudo -n install -o kafka -g kafka -m 640 {tmp_path} {config_path} "
                f"&& sudo -n rm -f {tmp_path}"
            )
            rc, out, err = SSHManager.exec_command(client, install_cmd, timeout=15)
            if rc != 0:
                raise RuntimeError(f"Failed to write config (sudo install): {err or out}")

            # Audit log
            audit = ConfigAuditLog(
                cluster_id=cluster_id,
                broker_id=broker_id,
                config_key=config_key,
                old_value=old_value,
                new_value=config_value,
                changed_by=username,
                change_type="update",
            )
            db.add(audit)
            db.commit()

            logger.info(f"Config updated: {config_key}={config_value} on broker {broker_id} by {username}")
            return {
                "broker_id": broker_id,
                "config_key": config_key,
                "old_value": old_value,
                "new_value": config_value,
                "requires_restart": not KAFKA_BROKER_CONFIGS.get(config_key, {}).get("dynamic", False),
            }

    def bulk_update_broker_config(
        self, cluster_id: str, config_key: str, config_value: str,
        username: str, db: Session,
    ) -> dict:
        """Apply a single config change to every broker in the cluster.

        v1.4.0 #16. We loop the per-broker update so a failure on one
        broker doesn't roll back already-succeeded brokers — the customer
        operationally needs partial-success visibility (the UI can
        highlight failed brokers and let them retry).
        """
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise ValueError("Cluster not found")

        # External clusters: prefer kafka-python's incremental-alter to
        # batch all brokers in a single Admin API call when possible.
        if (cluster.kind or "managed") == "external":
            from app.services import external_admin
            services = []
            try:
                # describe_cluster gives synthetic broker rows for external
                describe = external_admin.describe_cluster(cluster)
                services = [{"node_id": b["broker_id"]} for b in describe.get("brokers", [])]
            except Exception as e:
                logger.warning("Failed to enumerate external brokers: %s", e)
        else:
            services = db.query(Service).filter(
                Service.cluster_id == cluster_id,
                Service.role.in_(["broker", "broker_controller"])
            ).all()

        results: list[dict] = []
        success_count = 0
        for svc in services:
            broker_id = svc["node_id"] if isinstance(svc, dict) else svc.node_id
            try:
                r = self.update_broker_config(
                    cluster_id, broker_id, config_key, config_value, username, db,
                )
                results.append({"broker_id": broker_id, "ok": True, "result": r})
                success_count += 1
            except (ValueError, RuntimeError) as e:
                results.append({"broker_id": broker_id, "ok": False, "error": str(e)})

        return {
            "cluster_id": cluster_id,
            "config_key": config_key,
            "config_value": config_value,
            "broker_count": len(results),
            "success_count": success_count,
            "results": results,
        }

    def rollback_config(self, audit_id: str, username: str, db: Session) -> dict:
        """Rollback a config change by its audit log ID."""
        audit = db.query(ConfigAuditLog).filter(ConfigAuditLog.id == audit_id).first()
        if not audit:
            raise ValueError("Audit log entry not found")
        if audit.old_value is None:
            raise ValueError("No previous value to rollback to (config was newly added)")

        result = self.update_broker_config(
            audit.cluster_id, audit.broker_id, audit.config_key,
            audit.old_value, username, db,
        )
        # Update the rollback audit entry type
        latest = db.query(ConfigAuditLog).filter(
            ConfigAuditLog.cluster_id == audit.cluster_id,
            ConfigAuditLog.config_key == audit.config_key,
        ).order_by(ConfigAuditLog.created_at.desc()).first()
        if latest:
            latest.change_type = "rollback"
            db.commit()
        return result

    def get_audit_log(self, cluster_id: str, db: Session, limit: int = 50) -> list[dict]:
        """Get config change audit log for a cluster."""
        entries = db.query(ConfigAuditLog).filter(
            ConfigAuditLog.cluster_id == cluster_id
        ).order_by(ConfigAuditLog.created_at.desc()).limit(limit).all()
        return [
            {
                "id": e.id,
                "broker_id": e.broker_id,
                "config_key": e.config_key,
                "old_value": e.old_value,
                "new_value": e.new_value,
                "changed_by": e.changed_by,
                "change_type": e.change_type,
                "created_at": e.created_at.isoformat(),
            }
            for e in entries
        ]

    def get_config_metadata(self) -> list[dict]:
        """Return known Kafka config keys with descriptions."""
        return [
            {"key": k, **v} for k, v in KAFKA_BROKER_CONFIGS.items()
        ]

    @staticmethod
    def _parse_properties(content: str) -> dict[str, str]:
        """Parse Java properties file content into a dict."""
        result = {}
        for line in content.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if "=" in stripped:
                key, _, value = stripped.partition("=")
                result[key.strip()] = value.strip()
        return result


config_manager = ConfigManager()
