import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.config import settings
from app.models.cluster import Cluster
from app.models.cluster_link import ClusterLink
from app.models.host import Host
from app.models.service import Service
from app.services.ssh_manager import SSHManager

logger = logging.getLogger("tantor.cluster_linking")

# In-memory task tracking for deploy operations
_link_tasks: dict[str, dict] = {}

MM2_SERVICE_NAME = "kafka-mirror-maker-2"
MM2_SYSTEMD_UNIT = f"{MM2_SERVICE_NAME}.service"
KAFKA_INSTALL_DIR = settings.KAFKA_INSTALL_DIR
MM2_CONFIG_PATH = f"{KAFKA_INSTALL_DIR}/config/mm2.properties"


def init_link_task(task_id: str):
    _link_tasks[task_id] = {
        "task_id": task_id,
        "status": "running",
        "logs": [],
    }


def get_link_task(task_id: str) -> dict | None:
    return _link_tasks.get(task_id)


def _log(task_id: str, message: str):
    task = _link_tasks.get(task_id)
    if task:
        task["logs"].append(message)
    if message.strip():
        logger.info("[%s] %s", task_id[:8], message)


def _validate_bootstrap_address(addr: str) -> bool:
    """v1.4.1 — sanity-check bootstrap addresses before MM2 sees them.

    MM2 fails late and silently on a missing port or IPv6 unbracketed.
    """
    addr = addr.strip()
    if not addr:
        return False
    # bare hostname without port — invalid for Kafka bootstrap
    if ":" not in addr:
        return False
    # IPv6 needs brackets: [::1]:9092 — bare ::1:9092 is ambiguous
    if addr.count(":") > 1 and not addr.startswith("["):
        return False
    host, _, port = addr.rpartition(":")
    if not host or not port.isdigit():
        return False
    return True


def _get_broker_addresses(cluster_id: str, db: Session) -> list[str]:
    """Get bootstrap server addresses for a cluster.

    v1.4.0 #4 — external clusters have no Service rows, so we fall
    back to their saved `bootstrap_servers` field. Without this fix
    cluster-linking refused to create any link involving an external
    cluster ("Source cluster has no brokers").

    v1.4.1 — also validates each address has a port + correct shape.
    Caller raises ValueError on empty list, surfacing the bad input
    before MM2 starts and fails opaquely.
    """
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if cluster and (cluster.kind or "managed") == "external":
        bs = (cluster.bootstrap_servers or "").strip()
        candidates = [s.strip() for s in bs.split(",") if s.strip()]
        return [a for a in candidates if _validate_bootstrap_address(a)]

    services = db.query(Service).filter(
        Service.cluster_id == cluster_id,
        Service.role.in_(["broker", "broker_controller"]),
    ).all()
    hosts = {h.id: h for h in db.query(Host).all()}
    addresses = []
    for svc in services:
        host = hosts.get(svc.host_id)
        if host:
            # Use the cluster's listener port if recorded; default 9092.
            port = 9092
            if cluster and cluster.config_json:
                try:
                    import json as _json
                    cfg = _json.loads(cluster.config_json or "{}")
                    port = int(cfg.get("listener_port") or 9092)
                except Exception:
                    pass
            addresses.append(f"{host.ip_address}:{port}")
    return addresses


def _security_block(prefix: str, cluster: "Cluster | None") -> str:
    """v1.4.1 — emit per-cluster security.protocol / SASL / SSL
    properties for MM2 so external clusters using SASL_SSL or SSL
    actually authenticate.

    `prefix` is "source" or "target". For managed clusters (which speak
    PLAINTEXT by default at the listener Tantor configures), we emit
    nothing — MM2 defaults are correct.
    """
    if cluster is None or (cluster.kind or "managed") == "managed":
        return ""
    proto = (cluster.security_protocol or "PLAINTEXT").upper()
    if proto == "PLAINTEXT":
        return ""
    lines = [f"{prefix}.security.protocol = {proto}"]
    if proto in ("SASL_PLAINTEXT", "SASL_SSL"):
        from app.services.external_admin import decrypt_secrets
        secrets = decrypt_secrets(cluster.encrypted_connection_secrets)
        mech = (cluster.sasl_mechanism or "PLAIN").upper()
        lines.append(f"{prefix}.sasl.mechanism = {mech}")
        # JAAS module — SCRAM uses ScramLoginModule, PLAIN uses PlainLoginModule
        if mech.startswith("SCRAM"):
            module = "org.apache.kafka.common.security.scram.ScramLoginModule"
        elif mech == "GSSAPI":
            module = "com.sun.security.auth.module.Krb5LoginModule"
        else:
            module = "org.apache.kafka.common.security.plain.PlainLoginModule"
        u = secrets.get("sasl_username") or ""
        p = secrets.get("sasl_password") or ""
        # Escape backslashes + double-quotes in passwords for JAAS string
        p_esc = p.replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f'{prefix}.sasl.jaas.config = {module} required username="{u}" password="{p_esc}";')
    if proto in ("SSL", "SASL_SSL"):
        # Tantor doesn't auto-stage truststores on the MM2 host today;
        # operators typically export TANTOR-trusted CAs. Leave a hint so
        # operators see it in the config and know to point at their
        # truststore. (Empty values cause connect to use system trust.)
        lines.append(f"{prefix}.ssl.endpoint.identification.algorithm = https")
        if not bool(cluster.ssl_verify):
            lines.append(f"{prefix}.ssl.endpoint.identification.algorithm = ")
    return "\n".join(lines) + "\n"


def _generate_mm2_config(
    source_brokers: list[str],
    dest_brokers: list[str],
    topics_pattern: str,
    sync_consumer_offsets: bool,
    sync_topic_configs: bool,
    source_cluster: "Cluster | None" = None,
    dest_cluster: "Cluster | None" = None,
) -> str:
    """Generate MirrorMaker 2 properties file content."""
    source_bootstrap = ",".join(source_brokers)
    target_bootstrap = ",".join(dest_brokers)
    sync_offsets_str = "true" if sync_consumer_offsets else "false"
    sync_configs_str = "true" if sync_topic_configs else "false"
    source_security = _security_block("source", source_cluster)
    target_security = _security_block("target", dest_cluster)

    config = f"""# MirrorMaker 2 Configuration
clusters = source, target
source.bootstrap.servers = {source_bootstrap}
target.bootstrap.servers = {target_bootstrap}
{source_security}{target_security}
# Replication
source->target.enabled = true
source->target.topics = {topics_pattern}
target->source.enabled = false

# Offset sync
sync.topic.configs.enabled = {sync_configs_str}
emit.checkpoints.enabled = {sync_offsets_str}
emit.heartbeats.enabled = true

# Consumer offsets
sync.group.offsets.enabled = {sync_offsets_str}

# Connect settings
replication.factor = 1
offset-syncs.topic.replication.factor = 1
heartbeats.topic.replication.factor = 1
checkpoints.topic.replication.factor = 1

tasks.max = 4
"""
    return config


def _generate_systemd_unit() -> str:
    """Generate systemd unit file for MirrorMaker 2."""
    return f"""[Unit]
Description=Kafka MirrorMaker 2
After=network.target

[Service]
Type=simple
User=kafka
ExecStart={KAFKA_INSTALL_DIR}/bin/connect-mirror-maker.sh {MM2_CONFIG_PATH}
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
"""


class ClusterLinkingManager:
    """Manages MirrorMaker 2 cluster links for cross-cluster replication."""

    @staticmethod
    def create_link(
        name: str,
        source_cluster_id: str,
        dest_cluster_id: str,
        topics_pattern: str,
        sync_offsets: bool,
        sync_configs: bool,
        db: Session,
    ) -> ClusterLink:
        """Create a new cluster link configuration."""
        # Validate clusters exist
        source = db.query(Cluster).filter(Cluster.id == source_cluster_id).first()
        if not source:
            raise ValueError(f"Source cluster not found: {source_cluster_id}")

        dest = db.query(Cluster).filter(Cluster.id == dest_cluster_id).first()
        if not dest:
            raise ValueError(f"Destination cluster not found: {dest_cluster_id}")

        if source_cluster_id == dest_cluster_id:
            raise ValueError("Source and destination clusters must be different")

        # Generate MM2 config
        source_brokers = _get_broker_addresses(source_cluster_id, db)
        dest_brokers = _get_broker_addresses(dest_cluster_id, db)

        # v1.4.3 #20 — distinguish "no brokers at all" from "external
        # cluster has malformed bootstrap_servers". The customer reported
        # external clusters showing 1 broker in overview but link create
        # failing — root cause was bootstrap_servers without a :port
        # which _validate_bootstrap_address filters out. Tell the
        # operator EXACTLY what's wrong.
        def _explain_empty(c: Cluster, label: str) -> str:
            kind = c.kind or "managed"
            if kind == "external":
                raw = (c.bootstrap_servers or "").strip()
                if not raw:
                    return f"{label} cluster ({c.name}): no bootstrap_servers configured. Edit the external cluster and add a comma-separated list of host:port entries."
                return (
                    f"{label} cluster ({c.name}): bootstrap_servers='{raw}' "
                    "rejected — every entry must look like 'host:port' (or '[ipv6]:port'). "
                    "Edit the external cluster to fix."
                )
            return f"{label} cluster ({c.name}) has no broker services registered. Deploy at least one broker before creating a link."
        if not source_brokers:
            raise ValueError(_explain_empty(source, "Source"))
        if not dest_brokers:
            raise ValueError(_explain_empty(dest, "Destination"))

        mm2_config = _generate_mm2_config(
            source_brokers, dest_brokers, topics_pattern, sync_offsets, sync_configs,
            source_cluster=source, dest_cluster=dest,
        )

        # Pick a deploy host. Preference order:
        #   1. A broker host from the source cluster (managed)
        #   2. A broker host from the destination cluster (managed)
        #   3. ANY Tantor-registered host (we just need somewhere to run MM2)
        # v1.4.0 #4 — without this, an external→external or
        # external→managed link fails with "no deploy host configured".
        deploy_host_id = None
        for cid in (source_cluster_id, dest_cluster_id):
            svcs = db.query(Service).filter(
                Service.cluster_id == cid,
                Service.role.in_(["broker", "broker_controller"]),
            ).all()
            if svcs:
                deploy_host_id = svcs[0].host_id
                break
        if not deploy_host_id:
            any_host = db.query(Host).first()
            if any_host:
                deploy_host_id = any_host.id

        link = ClusterLink(
            name=name,
            source_cluster_id=source_cluster_id,
            destination_cluster_id=dest_cluster_id,
            topics_pattern=topics_pattern,
            sync_consumer_offsets=sync_offsets,
            sync_topic_configs=sync_configs,
            mm2_config=mm2_config,
            deploy_host_id=deploy_host_id,
        )
        db.add(link)
        db.commit()
        db.refresh(link)
        return link

    @staticmethod
    def deploy_link(link_id: str, task_id: str, db: Session):
        """Deploy MirrorMaker 2 on the target host. Runs as background task."""
        link = db.query(ClusterLink).filter(ClusterLink.id == link_id).first()
        if not link:
            _log(task_id, "ERROR: Cluster link not found")
            _link_tasks[task_id]["status"] = "error"
            return

        if not link.deploy_host_id:
            _log(task_id, "ERROR: No deploy host configured for this link")
            _link_tasks[task_id]["status"] = "error"
            link.state = "error"
            db.commit()
            return

        host = db.query(Host).filter(Host.id == link.deploy_host_id).first()
        if not host:
            _log(task_id, "ERROR: Deploy host not found")
            _link_tasks[task_id]["status"] = "error"
            link.state = "error"
            db.commit()
            return

        _log(task_id, f"Deploying MirrorMaker 2 for link '{link.name}' on {host.hostname} ({host.ip_address})")

        try:
            with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                # Step 1: Write MM2 config
                _log(task_id, f"Writing MM2 config to {MM2_CONFIG_PATH}")
                mm2_config = link.mm2_config or ""
                SSHManager.upload_content(client, mm2_config, MM2_CONFIG_PATH)
                _log(task_id, "MM2 config written successfully")

                # Step 2: Set ownership
                _log(task_id, "Setting file ownership")
                exit_code, _, stderr = SSHManager.exec_command(client, f"sudo chown kafka:kafka {MM2_CONFIG_PATH}")
                if exit_code != 0:
                    _log(task_id, f"WARNING: Could not set ownership: {stderr}")

                # Step 3: Create systemd unit
                unit_content = _generate_systemd_unit()
                unit_path = f"/etc/systemd/system/{MM2_SYSTEMD_UNIT}"
                _log(task_id, f"Creating systemd unit: {unit_path}")

                # Write unit file via temp then move with sudo
                tmp_unit = "/tmp/mm2.service"
                SSHManager.upload_content(client, unit_content, tmp_unit)
                exit_code, _, stderr = SSHManager.exec_command(client, f"sudo mv {tmp_unit} {unit_path}")
                if exit_code != 0:
                    _log(task_id, f"ERROR: Failed to install systemd unit: {stderr}")
                    _link_tasks[task_id]["status"] = "error"
                    link.state = "error"
                    db.commit()
                    return

                # Step 4: Reload systemd
                _log(task_id, "Reloading systemd daemon")
                exit_code, _, stderr = SSHManager.exec_command(client, "sudo systemctl daemon-reload")
                if exit_code != 0:
                    _log(task_id, f"WARNING: daemon-reload issue: {stderr}")

                # Step 5: Enable the service
                _log(task_id, "Enabling MirrorMaker 2 service")
                exit_code, _, stderr = SSHManager.exec_command(client, f"sudo systemctl enable {MM2_SYSTEMD_UNIT}")
                if exit_code != 0:
                    _log(task_id, f"WARNING: Could not enable service: {stderr}")

                # Step 6: Start the service
                _log(task_id, "Starting MirrorMaker 2 service")
                exit_code, _, stderr = SSHManager.exec_command(client, f"sudo systemctl start {MM2_SYSTEMD_UNIT}", timeout=60)
                if exit_code != 0:
                    _log(task_id, f"ERROR: Failed to start MirrorMaker 2: {stderr}")
                    _link_tasks[task_id]["status"] = "error"
                    link.state = "error"
                    db.commit()
                    return

                # Step 7: Verify service is running
                _log(task_id, "Verifying service status")
                exit_code, stdout, _ = SSHManager.exec_command(client, f"systemctl is-active {MM2_SYSTEMD_UNIT}")
                if stdout.strip() == "active":
                    _log(task_id, "MirrorMaker 2 is running successfully")
                    link.state = "running"
                    _link_tasks[task_id]["status"] = "completed"
                else:
                    _log(task_id, f"WARNING: Service status is '{stdout.strip()}', may need time to initialize")
                    link.state = "running"
                    _link_tasks[task_id]["status"] = "completed"

                _log(task_id, "Deployment completed successfully")

        except Exception as e:
            logger.exception("Failed to deploy MM2 for link %s", link_id)
            _log(task_id, f"FATAL ERROR: {e}")
            _link_tasks[task_id]["status"] = "error"
            link.state = "error"

        db.commit()

    @staticmethod
    def start_link(link_id: str, db: Session) -> dict:
        """Start MirrorMaker 2 service for a link."""
        link = db.query(ClusterLink).filter(ClusterLink.id == link_id).first()
        if not link:
            raise ValueError("Cluster link not found")

        if not link.deploy_host_id:
            raise ValueError("Link has not been deployed yet")

        host = db.query(Host).filter(Host.id == link.deploy_host_id).first()
        if not host:
            raise ValueError("Deploy host not found")

        try:
            with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                # Re-upload config in case it was updated
                if link.mm2_config:
                    SSHManager.upload_content(client, link.mm2_config, MM2_CONFIG_PATH)
                    SSHManager.exec_command(client, f"sudo chown kafka:kafka {MM2_CONFIG_PATH}")

                exit_code, _, stderr = SSHManager.exec_command(client, f"sudo systemctl start {MM2_SYSTEMD_UNIT}", timeout=60)
                if exit_code == 0:
                    link.state = "running"
                    link.updated_at = datetime.now(timezone.utc)
                    db.commit()
                    return {"success": True, "message": f"MirrorMaker 2 started on {host.ip_address}"}
                else:
                    link.state = "error"
                    link.updated_at = datetime.now(timezone.utc)
                    db.commit()
                    return {"success": False, "message": f"Failed to start: {stderr}"}
        except Exception as e:
            link.state = "error"
            link.updated_at = datetime.now(timezone.utc)
            db.commit()
            return {"success": False, "message": str(e)}

    @staticmethod
    def stop_link(link_id: str, db: Session) -> dict:
        """Stop MirrorMaker 2 service for a link."""
        link = db.query(ClusterLink).filter(ClusterLink.id == link_id).first()
        if not link:
            raise ValueError("Cluster link not found")

        if not link.deploy_host_id:
            raise ValueError("Link has not been deployed yet")

        host = db.query(Host).filter(Host.id == link.deploy_host_id).first()
        if not host:
            raise ValueError("Deploy host not found")

        try:
            with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                exit_code, _, stderr = SSHManager.exec_command(client, f"sudo systemctl stop {MM2_SYSTEMD_UNIT}", timeout=60)
                if exit_code == 0:
                    link.state = "stopped"
                    link.updated_at = datetime.now(timezone.utc)
                    db.commit()
                    return {"success": True, "message": f"MirrorMaker 2 stopped on {host.ip_address}"}
                else:
                    return {"success": False, "message": f"Failed to stop: {stderr}"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    @staticmethod
    def get_link_status(link_id: str, db: Session) -> dict:
        """Get live status of a cluster link including MM2 service status."""
        link = db.query(ClusterLink).filter(ClusterLink.id == link_id).first()
        if not link:
            raise ValueError("Cluster link not found")

        source_cluster = db.query(Cluster).filter(Cluster.id == link.source_cluster_id).first()
        dest_cluster = db.query(Cluster).filter(Cluster.id == link.destination_cluster_id).first()

        result = {
            "id": link.id,
            "name": link.name,
            "state": link.state,
            "source_cluster": {
                "id": source_cluster.id,
                "name": source_cluster.name,
            } if source_cluster else None,
            "destination_cluster": {
                "id": dest_cluster.id,
                "name": dest_cluster.name,
            } if dest_cluster else None,
            "topics_pattern": link.topics_pattern,
            "sync_consumer_offsets": link.sync_consumer_offsets,
            "sync_topic_configs": link.sync_topic_configs,
            "mm2_config": link.mm2_config,
            "deploy_host_id": link.deploy_host_id,
            "mm2_port": link.mm2_port,
            "created_at": link.created_at.isoformat() if link.created_at else None,
            "updated_at": link.updated_at.isoformat() if link.updated_at else None,
            "service_status": "unknown",
            "deploy_host": None,
        }

        # Check live service status if deployed
        if link.deploy_host_id:
            host = db.query(Host).filter(Host.id == link.deploy_host_id).first()
            if host:
                result["deploy_host"] = {
                    "id": host.id,
                    "hostname": host.hostname,
                    "ip_address": host.ip_address,
                }
                try:
                    with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                        exit_code, stdout, _ = SSHManager.exec_command(client, f"systemctl is-active {MM2_SYSTEMD_UNIT}")
                        service_status = stdout.strip()
                        result["service_status"] = service_status

                        # Update persisted state to match reality
                        if service_status == "active" and link.state != "running":
                            link.state = "running"
                            db.commit()
                        elif service_status in ("inactive", "failed") and link.state == "running":
                            link.state = "stopped" if service_status == "inactive" else "error"
                            db.commit()
                except Exception as e:
                    result["service_status"] = f"error: {e}"

        return result

    @staticmethod
    def get_links(db: Session) -> list[dict]:
        """List all cluster links with cluster name lookups."""
        links = db.query(ClusterLink).order_by(ClusterLink.created_at.desc()).all()
        clusters = {c.id: c for c in db.query(Cluster).all()}

        result = []
        for link in links:
            source = clusters.get(link.source_cluster_id)
            dest = clusters.get(link.destination_cluster_id)
            result.append({
                "id": link.id,
                "name": link.name,
                "source_cluster_id": link.source_cluster_id,
                "source_cluster_name": source.name if source else "Unknown",
                "destination_cluster_id": link.destination_cluster_id,
                "destination_cluster_name": dest.name if dest else "Unknown",
                "topics_pattern": link.topics_pattern,
                "sync_consumer_offsets": link.sync_consumer_offsets,
                "sync_topic_configs": link.sync_topic_configs,
                "state": link.state,
                "mm2_port": link.mm2_port,
                "deploy_host_id": link.deploy_host_id,
                "created_at": link.created_at.isoformat() if link.created_at else None,
                "updated_at": link.updated_at.isoformat() if link.updated_at else None,
            })
        return result

    @staticmethod
    def delete_link(link_id: str, db: Session) -> dict:
        """Delete a cluster link. Stops MM2 if running, removes systemd unit."""
        link = db.query(ClusterLink).filter(ClusterLink.id == link_id).first()
        if not link:
            raise ValueError("Cluster link not found")

        # Try to stop and clean up if deployed
        if link.deploy_host_id:
            host = db.query(Host).filter(Host.id == link.deploy_host_id).first()
            if host:
                try:
                    with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                        SSHManager.exec_command(client, f"sudo systemctl stop {MM2_SYSTEMD_UNIT}")
                        SSHManager.exec_command(client, f"sudo systemctl disable {MM2_SYSTEMD_UNIT}")
                        SSHManager.exec_command(client, f"sudo rm -f /etc/systemd/system/{MM2_SYSTEMD_UNIT}")
                        SSHManager.exec_command(client, "sudo systemctl daemon-reload")
                        SSHManager.exec_command(client, f"sudo rm -f {MM2_CONFIG_PATH}")
                except Exception:
                    pass  # Best effort cleanup

        db.delete(link)
        db.commit()
        return {"detail": f"Cluster link '{link.name}' deleted"}

    @staticmethod
    def update_link(
        link_id: str,
        topics_pattern: str | None,
        sync_offsets: bool | None,
        sync_configs: bool | None,
        mm2_config_override: str | None,
        db: Session,
    ) -> ClusterLink:
        """Update link settings and regenerate config if needed."""
        link = db.query(ClusterLink).filter(ClusterLink.id == link_id).first()
        if not link:
            raise ValueError("Cluster link not found")

        changed = False

        if topics_pattern is not None and topics_pattern != link.topics_pattern:
            link.topics_pattern = topics_pattern
            changed = True

        if sync_offsets is not None and sync_offsets != link.sync_consumer_offsets:
            link.sync_consumer_offsets = sync_offsets
            changed = True

        if sync_configs is not None and sync_configs != link.sync_topic_configs:
            link.sync_topic_configs = sync_configs
            changed = True

        # If a full config override is provided, use it directly
        if mm2_config_override is not None:
            link.mm2_config = mm2_config_override
        elif changed:
            # Regenerate config from current settings
            source_brokers = _get_broker_addresses(link.source_cluster_id, db)
            dest_brokers = _get_broker_addresses(link.destination_cluster_id, db)
            if source_brokers and dest_brokers:
                src = db.query(Cluster).filter(Cluster.id == link.source_cluster_id).first()
                dst = db.query(Cluster).filter(Cluster.id == link.destination_cluster_id).first()
                link.mm2_config = _generate_mm2_config(
                    source_brokers, dest_brokers,
                    link.topics_pattern,
                    link.sync_consumer_offsets,
                    link.sync_topic_configs,
                    source_cluster=src, dest_cluster=dst,
                )

        link.updated_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(link)
        return link

    @staticmethod
    def get_link_metrics(link_id: str, db: Session) -> dict:
        """Get replication metrics from MirrorMaker 2 via its Connect REST API."""
        link = db.query(ClusterLink).filter(ClusterLink.id == link_id).first()
        if not link:
            raise ValueError("Cluster link not found")

        metrics = {
            "link_id": link.id,
            "link_name": link.name,
            "state": link.state,
            "connectors": [],
            "replication_lag": None,
            "throughput": None,
            "error": None,
        }

        if link.state != "running" or not link.deploy_host_id:
            metrics["error"] = "Link is not running or not deployed"
            return metrics

        host = db.query(Host).filter(Host.id == link.deploy_host_id).first()
        if not host:
            metrics["error"] = "Deploy host not found"
            return metrics

        try:
            with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                # Query MM2 Connect REST API for connector status
                rest_url = f"http://localhost:{link.mm2_port}"

                # Get connectors list
                exit_code, stdout, stderr = SSHManager.exec_command(
                    client, f"curl -s {rest_url}/connectors", timeout=10
                )
                if exit_code == 0 and stdout:
                    try:
                        import json
                        connectors = json.loads(stdout)
                        metrics["connectors"] = connectors if isinstance(connectors, list) else []
                    except (json.JSONDecodeError, ValueError):
                        metrics["connectors"] = []

                # Get connector statuses
                connector_statuses = []
                for connector_name in metrics["connectors"]:
                    exit_code, stdout, _ = SSHManager.exec_command(
                        client, f"curl -s {rest_url}/connectors/{connector_name}/status", timeout=10
                    )
                    if exit_code == 0 and stdout:
                        try:
                            import json
                            status = json.loads(stdout)
                            connector_statuses.append(status)
                        except (json.JSONDecodeError, ValueError):
                            pass

                metrics["connector_statuses"] = connector_statuses

                # Try to get lag information from consumer group offsets
                # MM2 creates internal consumer groups for tracking
                exit_code, stdout, stderr = SSHManager.exec_command(
                    client,
                    f"{KAFKA_INSTALL_DIR}/bin/kafka-consumer-groups.sh "
                    f"--bootstrap-server localhost:9092 --list",
                    timeout=15,
                )
                if exit_code == 0:
                    groups = [g for g in stdout.strip().split("\n") if g.strip()]
                    mm2_groups = [g for g in groups if "mirror" in g.lower() or "mm2" in g.lower()]
                    metrics["mm2_consumer_groups"] = mm2_groups

                    # Get lag for first MM2 group if any
                    total_lag = 0
                    if mm2_groups:
                        exit_code, stdout, _ = SSHManager.exec_command(
                            client,
                            f"{KAFKA_INSTALL_DIR}/bin/kafka-consumer-groups.sh "
                            f"--bootstrap-server localhost:9092 "
                            f"--describe --group {mm2_groups[0]}",
                            timeout=15,
                        )
                        if exit_code == 0 and stdout:
                            for line in stdout.strip().split("\n")[1:]:  # Skip header
                                parts = line.split()
                                if len(parts) >= 6:
                                    try:
                                        lag = int(parts[5]) if parts[5] != "-" else 0
                                        total_lag += lag
                                    except (ValueError, IndexError):
                                        pass
                            metrics["replication_lag"] = total_lag

                # Check MM2 service uptime as throughput proxy
                exit_code, stdout, _ = SSHManager.exec_command(
                    client, f"systemctl show {MM2_SYSTEMD_UNIT} --property=ActiveEnterTimestamp"
                )
                if exit_code == 0 and stdout:
                    metrics["service_uptime"] = stdout.strip()

        except Exception as e:
            metrics["error"] = str(e)

        return metrics


cluster_linking_manager = ClusterLinkingManager()
