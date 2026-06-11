"""Manages running Kafka clusters: start, stop, cleanup, and status."""

from __future__ import annotations

import json
import shlex
import time

from sqlalchemy.orm import Session

from app.models.cluster import Cluster
from app.models.host import Host
from app.models.service import Service
from app.services import agent_transport, cluster_paths, port_preflight
from app.services.ssh_manager import SSHManager


# Non-Kafka roles still use single-name units, but lifecycle cleanup must stop
# them too or their ports remain occupied after a cluster stop/delete.
NON_KAFKA_UNITS = {
    "ksqldb": "ksqldb",
    "kafka_connect": "kafka-connect",
    "schema_registry": "schema-registry",
}

# Fallbacks for clusters deployed before all Kafka roles used per-cluster units.
LEGACY_KAFKA_UNITS = {
    "broker": "kafka.service",
    "broker_controller": "kafka.service",
    "controller": "kafka-kraft-controller.service",
    "zookeeper": "kafka.service",
}

KAFKA_ROLES = ("broker", "broker_controller", "controller", "zookeeper")

STOP_ORDER = {
    "schema_registry": 0,
    "kafka_connect": 1,
    "ksqldb": 1,
    "broker": 2,
    "broker_controller": 2,
    "controller": 3,
    "zookeeper": 3,
}

START_ORDER = {
    "controller": 0,
    "zookeeper": 0,
    "broker_controller": 1,
    "broker": 1,
    "ksqldb": 2,
    "kafka_connect": 2,
    "schema_registry": 3,
}
KAFKA_ROLES = ("broker", "broker_controller", "zookeeper")
CORE_SERVICE_ROLES = ("broker", "broker_controller", "controller", "zookeeper")


def _q(value: str) -> str:
    return shlex.quote(str(value))


def _unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _is_managed(cluster: Cluster | None) -> bool:
    return bool(cluster and (cluster.kind or "managed") == "managed")


def _unit_candidates(cluster: Cluster | None, role: str) -> list[str]:
    """Return primary and legacy systemd unit names for a service role."""
    if role in KAFKA_ROLES:
        units: list[str] = []
        if _is_managed(cluster):
            units.append(cluster_paths.unit_name(cluster))
        units.append(LEGACY_KAFKA_UNITS.get(role, "kafka.service"))
        return _unique(units)
    return [NON_KAFKA_UNITS.get(role, "kafka") + ".service"]


def _unit_for(cluster: Cluster | None, role: str) -> str:
    """Compatibility helper for callers that only need the primary unit."""
    return _unit_candidates(cluster, role)[0]


def _cluster_config(cluster: Cluster | None) -> dict:
    if not cluster or not cluster.config_json:
        return {}
    try:
        return json.loads(cluster.config_json)
    except Exception:
        return {}


def _int_config(config: dict, key: str, default: int) -> int:
    try:
        return int(config.get(key) or default)
    except (TypeError, ValueError):
        return default


def _service_ports(cluster: Cluster | None, svc: Service) -> list[int]:
    """Ports this service is expected to release when stopped."""
    config = _cluster_config(cluster)
    role = svc.role
    ports: list[int] = []

    if role in ("broker", "broker_controller"):
        ports.append(_int_config(config, "listener_port", 9092))
        if bool(config.get("ssl_enabled")) or bool(getattr(cluster, "ssl_enabled", False)):
            ports.append(_int_config(config, "ssl_listener_port", 9096))
    if role in ("controller", "broker_controller"):
        ports.append(_int_config(config, "controller_port", 9093))
    if role == "ksqldb":
        ports.append(_int_config(config, "ksqldb_port", 8088))
    if role == "kafka_connect":
        ports.append(_int_config(config, "connect_rest_port", 8083))
    if role == "schema_registry":
        ports.append(_int_config(config, "schema_registry_port", 8085))

    return sorted(set(p for p in ports if p > 0))


def _unit_exists(client, unit_name: str) -> bool:
    rc, _, _ = SSHManager.exec_command(
        client, f"systemctl cat {_q(unit_name)} >/dev/null 2>&1", timeout=10
    )
    return rc == 0


def _unit_state(client, unit_name: str) -> str:
    _, stdout, _ = SSHManager.exec_command(
        client, f"systemctl is-active {_q(unit_name)} 2>/dev/null || true", timeout=10
    )
    return (stdout or "").strip()


def _select_existing_unit(client, candidates: list[str]) -> str:
    for unit_name in candidates:
        if _unit_exists(client, unit_name):
            return unit_name
    return candidates[0]


def _stop_unit(client, unit_name: str, remove_unit: bool) -> tuple[bool, str]:
    exists = _unit_exists(client, unit_name)
    state = _unit_state(client, unit_name)
    if not exists and state in ("", "inactive", "unknown"):
        return True, f"{unit_name} not present"

    rc, _, stderr = SSHManager.exec_command(
        client, f"sudo -n systemctl stop {_q(unit_name)}", timeout=60
    )
    if rc != 0 and "not loaded" not in (stderr or "").lower():
        return False, f"failed to stop {unit_name}: {stderr or f'systemctl exit {rc}'}"

    for _ in range(15):
        state = _unit_state(client, unit_name)
        if state in ("", "inactive", "failed", "unknown"):
            break
        time.sleep(1)
    else:
        SSHManager.exec_command(
            client,
            f"sudo -n systemctl kill --kill-who=all {_q(unit_name)} 2>/dev/null || true",
            timeout=20,
        )
        time.sleep(2)

    SSHManager.exec_command(
        client,
        f"sudo -n systemctl reset-failed {_q(unit_name)} 2>/dev/null || true",
        timeout=10,
    )

    if remove_unit:
        SSHManager.exec_command(
            client,
            f"sudo -n systemctl disable {_q(unit_name)} 2>/dev/null || true",
            timeout=20,
        )
        SSHManager.exec_command(
            client,
            f"sudo -n rm -f /etc/systemd/system/{_q(unit_name)} 2>/dev/null || true",
            timeout=20,
        )
        SSHManager.exec_command(client, "sudo -n systemctl daemon-reload || true", timeout=20)

    state = _unit_state(client, unit_name)
    if state in ("active", "activating", "deactivating"):
        return False, f"{unit_name} still {state} after stop"
    return True, f"stopped {unit_name}"


def _wait_ports_released(client, ports: list[int], timeout: int = 20) -> tuple[bool, dict[int, str]]:
    if not ports:
        return True, {}

    listening: dict[int, str] = {}
    deadline = time.time() + timeout
    while time.time() <= deadline:
        listening = port_preflight.list_listening_ports(client, ports)
        if not listening:
            return True, {}
        time.sleep(1)
    return False, listening


def _probe_status(client, candidates: list[str]) -> tuple[str, str]:
    fallback_unit = candidates[0]
    fallback_state = "unknown"

    for unit_name in candidates:
        exists = _unit_exists(client, unit_name)
        state = _unit_state(client, unit_name) or "unknown"
        if state in ("active", "activating", "deactivating", "failed"):
            return unit_name, state
        if exists and fallback_state == "unknown":
            fallback_unit = unit_name
            fallback_state = state

    return fallback_unit, fallback_state


def _cleanup_units(client, candidates: list[str]) -> list[tuple[str, bool, str]]:
    primary = candidates[0]
    primary_exists = _unit_exists(client, primary)
    primary_state = _unit_state(client, primary)
    if primary_exists or primary_state in ("active", "activating", "deactivating", "failed"):
        return [(primary, primary_exists, primary_state)]

    for fallback in candidates[1:]:
        exists = _unit_exists(client, fallback)
        state = _unit_state(client, fallback)
        if exists or state in ("active", "activating", "deactivating", "failed"):
            return [(fallback, exists, state)]

    return [(primary, primary_exists, primary_state)]


class ClusterManager:
    """Manages running Kafka clusters: start, stop, cleanup, and status."""

    @staticmethod
    def _start_service(client, cluster: Cluster | None, svc: Service, host: Host) -> dict:
        unit_name = _select_existing_unit(client, _unit_candidates(cluster, svc.role))
        rc, _, stderr = SSHManager.exec_command(
            client, f"sudo -n systemctl start {_q(unit_name)}", timeout=60
        )
        if rc == 0:
            return {
                "service_id": svc.id,
                "action": "start",
                "success": True,
                "message": f"Started {unit_name} on {host.ip_address}",
            }
        return {
            "service_id": svc.id,
            "action": "start",
            "success": False,
            "message": stderr or f"systemctl exit {rc}",
        }

    @staticmethod
    def cleanup_service(cluster: Cluster | None, svc: Service, host: Host, remove_unit: bool = False) -> dict:
        """Stop a service, optionally remove its unit, and verify its ports are free."""
        messages: list[str] = []
        success = True

        try:
            with SSHManager.connect(
                host.ip_address,
                host.ssh_port,
                host.username,
                host.auth_type,
                host.encrypted_credential,
            ) as client:
                verify_ports = (
                    bool(cluster and cluster.state != "configured")
                    or svc.status not in ("pending", "configured")
                )
                for unit_name, exists_before, before_state in _cleanup_units(
                    client, _unit_candidates(cluster, svc.role)
                ):
                    if exists_before or before_state in (
                        "active",
                        "activating",
                        "deactivating",
                        "failed",
                    ):
                        verify_ports = True
                    stopped, message = _stop_unit(client, unit_name, remove_unit)
                    messages.append(message)
                    success = success and stopped

                if verify_ports:
                    ports = _service_ports(cluster, svc)
                    released, listening = _wait_ports_released(client, ports)
                    if not released:
                        success = False
                        held = ", ".join(
                            f"{port} by {process}" for port, process in sorted(listening.items())
                        )
                        messages.append(f"ports still in use: {held}")
        except Exception as e:
            success = False
            messages.append(str(e))

        if success:
            svc.status = "stopped"

        return {
            "service_id": svc.id,
            "action": "stop",
            "success": success,
            "message": "; ".join(m for m in messages if m) or "stopped",
        }

    @staticmethod
    def _stop_services(cluster_id: str, db: Session, remove_units: bool = False) -> list[dict]:
        results = []
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        services = db.query(Service).filter(Service.cluster_id == cluster_id).all()
        hosts = {h.id: h for h in db.query(Host).all()}

        sorted_services = sorted(services, key=lambda s: STOP_ORDER.get(s.role, 9))
        for svc in sorted_services:
            host = hosts.get(svc.host_id)
            if not host:
                results.append({
                    "service_id": svc.id,
                    "action": "stop",
                    "success": False,
                    "message": "Host not found",
                })
                continue
            results.append(ClusterManager.cleanup_service(cluster, svc, host, remove_unit=remove_units))
        return results

    @staticmethod
    def sync_cluster_state_from_services(cluster: Cluster | None, services: list[Service]) -> bool:
        if not cluster or (cluster.kind or "managed") != "managed" or cluster.state == "deploying":
            return False

        core_services = [svc for svc in services if svc.role in CORE_SERVICE_ROLES]
        if not core_services:
            return False

        statuses = {(svc.status or "").lower() for svc in core_services}
        new_state = None
        if "error" in statuses:
            new_state = "error"
        elif statuses == {"running"}:
            new_state = "running"
        elif statuses == {"stopped"}:
            new_state = "stopped"
        elif cluster.state == "running" and any(status != "running" for status in statuses):
            new_state = "error"

        if new_state and cluster.state != new_state:
            cluster.state = new_state
            return True
        return False

    @staticmethod
    def start_cluster(cluster_id: str, db: Session) -> list[dict]:
        """Start all services in a cluster. Returns status per service."""
        results = []
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        services = db.query(Service).filter(Service.cluster_id == cluster_id).all()
        hosts = {h.id: h for h in db.query(Host).all()}

        sorted_services = sorted(services, key=lambda s: START_ORDER.get(s.role, 9))

        for svc in sorted_services:
            host = hosts.get(svc.host_id)
            if not host:
                results.append({"service_id": svc.id, "action": "start", "success": False, "message": "Host not found"})
                continue

            try:
                with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                    result = ClusterManager._start_service(client, cluster, svc, host)
                    if result["success"]:
                        svc.status = "running"
                    else:
                        svc.status = "error"
                    results.append(result)
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
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        results = ClusterManager._stop_services(cluster_id, db, remove_units=False)

        if cluster:
            cluster.state = "stopped" if all(r["success"] for r in results) else "error"
        db.commit()
        return results

    @staticmethod
    def cleanup_cluster(cluster_id: str, db: Session, remove_units: bool = False) -> list[dict]:
        """Cleanup services for destructive operations such as cluster delete."""
        results = ClusterManager._stop_services(cluster_id, db, remove_units=remove_units)
        db.commit()
        return results

    @staticmethod
    def get_cluster_status(cluster_id: str, db: Session) -> list[dict]:
        """Get live status of all services in a cluster.

        v1.4.3 — uses the per-cluster systemd unit. Previously this
        always probed `kafka.service` so the UI refresh ALWAYS reported
        every kafka service as stopped (#3/#4/#5/#10), which then made
        Start fail because cluster.state was set to "error" and the UI
        gates the Start button on cluster.state.

        Keep cluster.state synchronized with core service health so the
        header never says "running" while a broker/controller is errored.
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

            try:
                # Agent-first dispatch: if the host has a connected
                # tantor-agent we ask it for systemctl is-active over the
                # reverse tunnel. ~100x faster than SSH+CLI (no fork+JVM)
                # and works without inbound SSH from the SCM. Falls
                # through to the existing SSH path when no agent is
                # connected. See docs/AGENT_PROTOCOL.md.
                unit_name = None
                status_raw = None
                if agent_transport.agent_available(host.id):
                    candidates = _unit_candidates(cluster, svc.role)
                    for cand in candidates:
                        res = agent_transport.try_systemctl_is_active(host, cand)
                        if res is None:
                            break  # agent dropped mid-call; fall through to SSH
                        _, raw = res
                        if raw in ("active", "activating", "deactivating", "failed"):
                            unit_name = cand
                            status_raw = raw
                            break
                        if status_raw is None:
                            unit_name = cand
                            status_raw = raw or "unknown"

                if status_raw is None:
                    with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                        unit_name, status_raw = _probe_status(client, _unit_candidates(cluster, svc.role))

                if status_raw in ("active", "activating"):
                    svc.status = "running"
                elif status_raw == "inactive":
                    svc.status = "stopped"
                elif status_raw == "failed":
                    svc.status = "error"

                results.append({
                    "service_id": svc.id,
                    "host": host.ip_address,
                    "hostname": host.hostname,
                    "role": svc.role,
                    "node_id": svc.node_id,
                    "unit": unit_name,
                    "status": svc.status,
                    "raw": status_raw,
                    "via": "agent" if agent_transport.agent_available(host.id) else "ssh",
                })
            except Exception as e:
                results.append({
                    "service_id": svc.id,
                    "host": host.ip_address,
                    "hostname": host.hostname,
                    "role": svc.role,
                    "node_id": svc.node_id,
                    "unit": _unit_for(cluster, svc.role),
                    "status": svc.status,
                    "error": str(e),
                })

        ClusterManager.sync_cluster_state_from_services(cluster, services)
        db.commit()
        return results


cluster_manager = ClusterManager()
