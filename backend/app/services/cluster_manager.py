"""Manages running Kafka clusters — start, stop, status.

APB v1.4.3 — fixed the per-cluster systemd unit regression that caused
"refresh stops cluster" and Start/Stop being unresponsive. Every probe
now resolves the kafka unit name via cluster_paths.unit_name(cluster)
the same way rolling_restart_manager + log_manager do.
"""
from sqlalchemy.orm import Session

from app.models.cluster import Cluster
from app.models.host import Host
from app.models.service import Service
from app.services import cluster_paths
from app.services.ssh_manager import SSHManager


# Non-Kafka roles still use the legacy single-name unit (per-cluster naming
# for ksqlDB / Connect is a v1.5 item).
NON_KAFKA_UNITS = {
    "controller": "kafka-kraft-controller",
    "ksqldb": "ksqldb",
    "kafka_connect": "kafka-connect",
}
KAFKA_ROLES = ("broker", "broker_controller", "zookeeper")


def _unit_for(cluster: Cluster, role: str) -> str:
    """Resolve the systemd unit name. For Kafka roles on a managed
    cluster, returns the per-cluster unit (kafka-prod-1ac9bbbe.service).
    For non-Kafka roles or unknown clusters, falls back to the legacy
    name so we never miss-name an SR/ksqlDB service.
    """
    is_managed = cluster and (cluster.kind or "managed") == "managed"
    if is_managed and role in KAFKA_ROLES:
        return cluster_paths.unit_name(cluster)
    return NON_KAFKA_UNITS.get(role, "kafka") + ".service"


class ClusterManager:
    """Manages running Kafka clusters — start, stop, status."""

    @staticmethod
    def start_cluster(cluster_id: str, db: Session) -> list[dict]:
        """Start all services in a cluster. Returns status per service."""
        results = []
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        services = db.query(Service).filter(Service.cluster_id == cluster_id).all()
        hosts = {h.id: h for h in db.query(Host).all()}

        # Start controllers/zookeepers first, then brokers, then ksqldb/connect
        order = {"controller": 0, "zookeeper": 0, "broker_controller": 1, "broker": 1, "ksqldb": 2, "kafka_connect": 2}
        sorted_services = sorted(services, key=lambda s: order.get(s.role, 3))

        for svc in sorted_services:
            host = hosts.get(svc.host_id)
            if not host:
                results.append({"service_id": svc.id, "action": "start", "success": False, "message": "Host not found"})
                continue

            unit_name = _unit_for(cluster, svc.role)
            try:
                with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                    # APB v1.4.3 — use sudo -n. The SSH user generally can't start
                    # systemd units without elevation; without -n we'd block on a
                    # password prompt.
                    exit_code, stdout, stderr = SSHManager.exec_command(client, f"sudo -n systemctl start {unit_name}", timeout=60)
                    if exit_code == 0:
                        svc.status = "running"
                        results.append({"service_id": svc.id, "action": "start", "success": True, "message": f"Started {unit_name} on {host.ip_address}"})
                    else:
                        svc.status = "error"
                        results.append({"service_id": svc.id, "action": "start", "success": False, "message": stderr or f"systemctl exit {exit_code}"})
            except Exception as e:
                svc.status = "error"
                results.append({"service_id": svc.id, "action": "start", "success": False, "message": str(e)})

        if cluster:
            if all(r["success"] for r in results):
                cluster.state = "running"
            else:
                cluster.state = "error"
        db.commit()
        return results

    @staticmethod
    def stop_cluster(cluster_id: str, db: Session) -> list[dict]:
        """Stop all services in a cluster. Returns status per service."""
        results = []
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        services = db.query(Service).filter(Service.cluster_id == cluster_id).all()
        hosts = {h.id: h for h in db.query(Host).all()}

        # Stop in reverse order: ksqldb/connect first, then brokers, then controllers
        order = {"kafka_connect": 0, "ksqldb": 0, "broker": 1, "broker_controller": 1, "controller": 2, "zookeeper": 2}
        sorted_services = sorted(services, key=lambda s: order.get(s.role, 3))

        for svc in sorted_services:
            host = hosts.get(svc.host_id)
            if not host:
                results.append({"service_id": svc.id, "action": "stop", "success": False, "message": "Host not found"})
                continue

            unit_name = _unit_for(cluster, svc.role)
            try:
                with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                    exit_code, stdout, stderr = SSHManager.exec_command(client, f"sudo -n systemctl stop {unit_name}", timeout=60)
                    if exit_code == 0:
                        svc.status = "stopped"
                        results.append({"service_id": svc.id, "action": "stop", "success": True, "message": f"Stopped {unit_name} on {host.ip_address}"})
                    else:
                        results.append({"service_id": svc.id, "action": "stop", "success": False, "message": stderr or f"systemctl exit {exit_code}"})
            except Exception as e:
                results.append({"service_id": svc.id, "action": "stop", "success": False, "message": str(e)})

        if cluster:
            cluster.state = "stopped"
        db.commit()
        return results

    @staticmethod
    def get_cluster_status(cluster_id: str, db: Session) -> list[dict]:
        """Get live status of all services in a cluster.

        APB v1.4.3 — uses the per-cluster systemd unit. Previously this
        always probed `kafka.service` so the UI refresh ALWAYS reported
        every kafka service as stopped (#3/#4/#5/#10), which then made
        Start fail because cluster.state was set to "error" and the UI
        gates the Start button on cluster.state.

        Also: DO NOT mutate cluster.state from a status probe. State
        transitions belong to start/stop/deploy. A refresh is a
        read-only observation.
        """
        results = []
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        services = db.query(Service).filter(Service.cluster_id == cluster_id).all()
        hosts = {h.id: h for h in db.query(Host).all()}

        for svc in services:
            host = hosts.get(svc.host_id)
            if not host:
                results.append({"service_id": svc.id, "host": "unknown", "role": svc.role, "status": "unknown"})
                continue

            unit_name = _unit_for(cluster, svc.role)
            try:
                with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                    # systemctl is-active does NOT need sudo (read-only).
                    exit_code, stdout, _ = SSHManager.exec_command(client, f"systemctl is-active {unit_name}", timeout=10)
                    status_raw = stdout.strip()
                    # Treat both "active" and "activating" as running — Kafka
                    # cold-start passes through activating for ~10s and a
                    # refresh during that window shouldn't show "stopped".
                    if status_raw in ("active", "activating"):
                        svc.status = "running"
                    elif status_raw == "inactive":
                        svc.status = "stopped"
                    elif status_raw == "failed":
                        svc.status = "error"
                    else:
                        # "unknown", "deactivating", or empty — keep the previous
                        # status rather than flipping based on a transient state.
                        pass
                    results.append({
                        "service_id": svc.id,
                        "host": host.ip_address,
                        "hostname": host.hostname,
                        "role": svc.role,
                        "node_id": svc.node_id,
                        "unit": unit_name,
                        "status": svc.status,
                        "raw": status_raw,
                    })
            except Exception as e:
                # SSH failure is a connectivity issue, not a service-down
                # signal. Don't overwrite svc.status — return it as-is and
                # surface the SSH error so the UI can show "cannot reach host"
                # without flipping the cluster to error on every refresh.
                results.append({
                    "service_id": svc.id,
                    "host": host.ip_address,
                    "hostname": host.hostname,
                    "role": svc.role,
                    "node_id": svc.node_id,
                    "unit": unit_name,
                    "status": svc.status,  # whatever we last knew
                    "error": str(e),
                })

        db.commit()
        return results


cluster_manager = ClusterManager()
