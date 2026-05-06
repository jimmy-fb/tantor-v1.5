"""Rolling Restart Manager — restart Kafka brokers one by one without downtime."""
import json
import logging
import time
import uuid
from sqlalchemy.orm import Session
from app.models.cluster import Cluster
from app.models.service import Service
from app.models.host import Host
from app.services.ssh_manager import SSHManager
from app.services import cluster_paths
from app.config import settings

logger = logging.getLogger("tantor.rolling_restart")


def _cluster_listener_port(cluster: Cluster) -> int:
    """Read the broker listener port from the cluster's config_json.
    Falls back to 9092 for clusters that predate per-cluster ports.
    Multi-cluster on the same host depends on this — the readiness probe
    can't just hit localhost:9092 for both."""
    try:
        cfg = json.loads(cluster.config_json or "{}")
    except Exception:
        cfg = {}
    try:
        return int(cfg.get("listener_port") or 9092)
    except (TypeError, ValueError):
        return 9092

# In-memory task tracking (same pattern as deployer.py)
_restart_tasks: dict[str, dict] = {}

def init_restart_task(task_id: str):
    _restart_tasks[task_id] = {"status": "running", "logs": [], "progress": {"current": 0, "total": 0, "current_broker": None}}

def get_restart_task(task_id: str) -> dict | None:
    return _restart_tasks.get(task_id)

# Fallback unit names for non-Kafka roles. Kafka-role units are now
# resolved via cluster_paths.unit_name(cluster) so multi-cluster
# deployments hit the right kafka-<slug>-<id>.service.
SERVICE_TYPE_MAP = {
    "broker": "kafka",
    "broker_controller": "kafka",
    "controller": "kafka-kraft-controller",
    "zookeeper": "kafka",
    "ksqldb": "ksqldb",
    "kafka_connect": "kafka-connect",
}

KAFKA_ROLES = ("broker", "broker_controller", "controller", "zookeeper")


def _unit_for_service(cluster: Cluster, svc: Service) -> str:
    """Resolve the systemd unit name to use for this service.

    For Kafka roles on a managed cluster, returns the per-cluster unit
    (e.g. kafka-prod-1ac9bbbe.service). Falls back to the legacy role
    map for non-Kafka services (ksqlDB, Connect) and external clusters.
    """
    is_managed = (cluster.kind or "managed") == "managed"
    if is_managed and svc.role in KAFKA_ROLES:
        return cluster_paths.unit_name(cluster)
    return SERVICE_TYPE_MAP.get(svc.role, "kafka") + ".service"

class RollingRestartManager:
    def rolling_restart(self, cluster_id: str, task_id: str, restart_scope: str, db: Session):
        """
        Perform rolling restart on cluster services.
        restart_scope: "brokers" | "all" | "controllers"
        """
        task = _restart_tasks.get(task_id)
        if not task:
            return

        def log(msg: str):
            task["logs"].append(msg)
            logger.info(f"[{task_id[:8]}] {msg}")

        try:
            cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
            if not cluster:
                log("ERROR: Cluster not found")
                task["status"] = "error"
                return

            # Get services to restart based on scope
            role_filter = []
            if restart_scope == "brokers":
                role_filter = ["broker", "broker_controller"]
            elif restart_scope == "controllers":
                role_filter = ["controller"]
            else:  # all
                role_filter = ["controller", "broker_controller", "broker", "ksqldb", "kafka_connect", "zookeeper"]

            services = db.query(Service).filter(
                Service.cluster_id == cluster_id,
                Service.role.in_(role_filter),
            ).all()

            if not services:
                log("No services found matching the restart scope")
                task["status"] = "completed"
                return

            # Sort: restart controllers first, then brokers, then others
            order = {"controller": 0, "zookeeper": 0, "broker_controller": 1, "broker": 1, "ksqldb": 2, "kafka_connect": 2}
            services = sorted(services, key=lambda s: order.get(s.role, 3))

            hosts = {h.id: h for h in db.query(Host).all()}
            task["progress"]["total"] = len(services)

            log(f"Starting rolling restart of {len(services)} service(s) — scope: {restart_scope}")
            log(f"Cluster: {cluster.name}")
            log("")

            for idx, svc in enumerate(services):
                host = hosts.get(svc.host_id)
                if not host:
                    log(f"⚠ Skipping service {svc.id} — host not found")
                    continue

                unit_name = _unit_for_service(cluster, svc)
                task["progress"]["current"] = idx + 1
                task["progress"]["current_broker"] = f"Node {svc.node_id} ({host.ip_address})"

                log(f"━━━ [{idx+1}/{len(services)}] Restarting {svc.role} node {svc.node_id} on {host.ip_address} ━━━")

                try:
                    with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                        # Step 1: Pre-restart health check
                        log(f"  Pre-restart health check...")
                        healthy = self._check_broker_health(client, svc.role, cluster)
                        if not healthy:
                            log(f"  ⚠ Broker not healthy before restart, proceeding anyway")

                        # Step 2: Stop the service
                        log(f"  Stopping {unit_name}...")
                        exit_code, stdout, stderr = SSHManager.exec_command(
                            client, f"sudo systemctl stop {unit_name}", timeout=60
                        )
                        if exit_code != 0:
                            log(f"  ⚠ Stop warning: {stderr[:200]}")

                        # Wait for graceful shutdown
                        log(f"  Waiting for graceful shutdown...")
                        for i in range(30):
                            exit_code, stdout, _ = SSHManager.exec_command(
                                client, f"systemctl is-active {unit_name}", timeout=10
                            )
                            if stdout.strip() != "active":
                                break
                            time.sleep(1)

                        log(f"  Service stopped")

                        # Step 3: Start the service
                        log(f"  Starting {unit_name}...")
                        exit_code, stdout, stderr = SSHManager.exec_command(
                            client, f"sudo systemctl start {unit_name}", timeout=60
                        )
                        if exit_code != 0:
                            log(f"  ✗ Start failed: {stderr[:200]}")
                            task["status"] = "error"
                            return

                        # Step 4: Wait for service to become healthy
                        log(f"  Waiting for service to become healthy...")
                        healthy = False
                        for attempt in range(60):  # Wait up to 60 seconds
                            time.sleep(1)
                            if self._check_service_running(client, unit_name):
                                if svc.role in ("broker", "broker_controller"):
                                    # For brokers, also check Kafka is accepting connections
                                    if self._check_kafka_port(client, _cluster_listener_port(cluster)):
                                        healthy = True
                                        break
                                else:
                                    healthy = True
                                    break

                        if healthy:
                            log(f"  ✓ Node {svc.node_id} restarted successfully")
                            svc.status = "running"
                        else:
                            log(f"  ✗ Node {svc.node_id} failed health check after restart")
                            svc.status = "error"
                            task["status"] = "error"
                            db.commit()
                            return

                        # Step 5: Wait for ISR recovery (for brokers)
                        if svc.role in ("broker", "broker_controller"):
                            log(f"  Waiting for ISR recovery...")
                            isr_ok = self._wait_for_isr(client, cluster, max_wait=120)
                            if isr_ok:
                                log(f"  ✓ ISR recovery complete")
                            else:
                                log(f"  ⚠ ISR recovery timeout — proceeding cautiously")

                        log("")

                except Exception as e:
                    log(f"  ✗ Error: {str(e)}")
                    svc.status = "error"
                    task["status"] = "error"
                    db.commit()
                    return

            db.commit()
            log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            log(f"✓ Rolling restart completed successfully — {len(services)} service(s) restarted")
            task["status"] = "completed"

        except Exception as e:
            log(f"ERROR: {str(e)}")
            task["status"] = "error"

    def _check_broker_health(self, client, role: str, cluster: Cluster | None = None) -> bool:
        """Check if broker is healthy before restart.

        Uses the cluster's per-install Kafka path + listener port so
        multi-cluster hosts probe the right broker."""
        if role not in ("broker", "broker_controller"):
            return True
        kafka_home = cluster_paths.install_dir(cluster) if cluster else settings.KAFKA_INSTALL_DIR
        port = _cluster_listener_port(cluster) if cluster else 9092
        exit_code, stdout, _ = SSHManager.exec_command(
            client,
            f"{kafka_home}/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:{port} 2>/dev/null | head -1",
            timeout=15,
        )
        return exit_code == 0

    def _check_service_running(self, client, unit_name: str) -> bool:
        """Check if systemd service is active."""
        exit_code, stdout, _ = SSHManager.exec_command(
            client, f"systemctl is-active {unit_name}", timeout=10
        )
        return stdout.strip() == "active"

    def _check_kafka_port(self, client, port: int = 9092) -> bool:
        """Check if Kafka is listening on its port."""
        exit_code, _, _ = SSHManager.exec_command(
            client, f"bash -c 'echo > /dev/tcp/localhost/{port}' 2>/dev/null", timeout=5
        )
        return exit_code == 0

    def _wait_for_isr(self, client, cluster: Cluster | None = None, max_wait: int = 120) -> bool:
        """Wait for all partitions to be in-sync after broker restart."""
        kafka_home = cluster_paths.install_dir(cluster) if cluster else settings.KAFKA_INSTALL_DIR
        port = _cluster_listener_port(cluster) if cluster else 9092
        for _ in range(max_wait // 5):
            exit_code, stdout, _ = SSHManager.exec_command(
                client,
                f"{kafka_home}/bin/kafka-topics.sh --bootstrap-server localhost:{port} --describe --under-replicated-partitions 2>/dev/null",
                timeout=15,
            )
            if exit_code == 0 and not stdout.strip():
                return True  # No under-replicated partitions
            time.sleep(5)
        return False

    def get_pre_restart_check(self, cluster_id: str, db: Session) -> dict:
        """Run pre-restart validation checks."""
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise ValueError("Cluster not found")

        services = db.query(Service).filter(
            Service.cluster_id == cluster_id,
            Service.role.in_(["broker", "broker_controller"])
        ).all()
        hosts = {h.id: h for h in db.query(Host).all()}

        checks = []
        for svc in services:
            host = hosts.get(svc.host_id)
            if not host:
                checks.append({"broker_id": svc.node_id, "host": "unknown", "healthy": False, "message": "Host not found"})
                continue
            try:
                with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                    healthy = self._check_broker_health(client, svc.role, cluster)
                    port_ok = self._check_kafka_port(client, _cluster_listener_port(cluster))
                    checks.append({
                        "broker_id": svc.node_id,
                        "host": host.ip_address,
                        "healthy": healthy and port_ok,
                        "message": "Healthy" if (healthy and port_ok) else "Unhealthy",
                    })
            except Exception as e:
                checks.append({"broker_id": svc.node_id, "host": host.ip_address, "healthy": False, "message": str(e)})

        all_healthy = all(c["healthy"] for c in checks)
        return {
            "cluster_name": cluster.name,
            "broker_count": len(services),
            "all_healthy": all_healthy,
            "checks": checks,
        }


rolling_restart_manager = RollingRestartManager()
