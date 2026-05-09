"""Port-conflict pre-flight for cluster deploys.

The customer asked for this after a Quick-Deploy on a host that already
had a Kafka cluster failed silently with a 0.0.0.0:9093 bind error in
ansible logs. We now SSH to each target host BEFORE the playbook starts,
list the ports the deploy will try to use, and bail with a precise error
("port 9093 is held by pid 32333 java — stop the existing cluster, or
pick a different controller_port") instead of silent ansible failure.

The same helper drives:
  - deploy_cluster pre-flight (managed cluster create/redeploy)
  - deploy_schema_registry pre-flight (port 8085 by default)
  - /api/clusters/preflight-ports for the wizard "Check ports" button
  - quick-deploy auto-pick (scans existing clusters' configs to find a
    free port set instead of always starting at 9092/9093)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from app.models.host import Host
from app.services.ssh_manager import SSHManager


# Defaults Tantor publishes — keep in sync with ClusterConfig schema.
# Used by quick-deploy auto-pick + the wizard preview.
DEFAULT_PORTS: dict[str, int] = {
    "listener": 9092,
    "controller": 9093,
    "ssl_listener": 9096,
    "jmx_exporter": 7071,
    "schema_registry": 8085,
    "ksqldb": 8088,
    "connect": 8083,
}


@dataclass
class PortCheck:
    host_id: str
    host_ip: str
    port: int
    label: str  # human-readable: "listener", "controller", "schema_registry", etc.


@dataclass
class PortConflict:
    host_ip: str
    port: int
    label: str
    process: str  # e.g. "java pid=32333 (kafka-cluster-1-96907e3d.service)"

    def message(self) -> str:
        return (
            f"port {self.port} ({self.label}) on {self.host_ip} is already "
            f"in use by {self.process}"
        )


def _list_listening(client, ports: Iterable[int]) -> dict[int, str]:
    """Return {port: process-description} for every port in `ports` that
    something is currently bound to on this host.

    Uses `ss -tnlp` (works on RHEL/Debian; no sudo because we only need
    listening sockets which are world-visible). Falls back to `netstat`
    if ss is unavailable. Empty dict means all ports are free.
    """
    port_list = " ".join(str(p) for p in ports)
    cmd = (
        # First try: ss is preferred (stable output, present on all
        # supported distros). Use the in-kernel filter so we don't
        # transfer an entire socket dump.
        f"if command -v ss >/dev/null 2>&1; then "
        f"  for p in {port_list}; do "
        f"    ss -tnlH 'sport = :'$p 2>/dev/null | awk -v p=$p "
        f"      '{{print p\":\"$0}}'; "
        f"  done; "
        # netstat fallback — older containers, busybox, etc.
        f"else "
        f"  for p in {port_list}; do "
        f"    netstat -tnlp 2>/dev/null | awk -v p=\":\"$p "
        f"      '$4 ~ p {{print substr(p,2)\":\"$0}}'; "
        f"  done; "
        f"fi"
    )
    exit_code, stdout, _ = SSHManager.exec_command(client, cmd, timeout=15)
    if exit_code != 0 or not stdout:
        return {}

    result: dict[int, str] = {}
    for line in stdout.splitlines():
        if ":" not in line:
            continue
        port_str, _, rest = line.partition(":")
        try:
            port = int(port_str)
        except ValueError:
            continue
        if port in result:
            continue
        # rest looks like:
        #   "LISTEN 0 50  *:9092 *:* users:((\"java\",pid=32333,fd=164))"
        # Extract a readable summary.
        proc = "unknown"
        if "users:" in rest:
            proc = rest.split("users:", 1)[1].strip().strip("(()")
        elif "/" in rest:
            # netstat format: "java/32333"
            tail = rest.strip().split()[-1] if rest.strip().split() else ""
            proc = tail
        result[port] = proc
    return result


def check_ports(checks: list[PortCheck], hosts: dict[str, Host]) -> list[PortConflict]:
    """Run port-availability checks against every host in `checks`.

    Groups checks by host so we open one SSH connection per host instead
    of one per port. Failures to reach a host are logged but not raised
    — the deploy will surface those errors itself and they shouldn't
    block port-checking on hosts that ARE reachable.
    """
    by_host: dict[str, list[PortCheck]] = {}
    for c in checks:
        by_host.setdefault(c.host_id, []).append(c)

    conflicts: list[PortConflict] = []
    for host_id, host_checks in by_host.items():
        host = hosts.get(host_id)
        if not host:
            continue
        ports = sorted({c.port for c in host_checks})
        try:
            with SSHManager.connect(
                host.ip_address, host.ssh_port, host.username,
                host.auth_type, host.encrypted_credential,
            ) as client:
                listening = _list_listening(client, ports)
        except Exception as e:
            # Can't reach the host → don't block deploy on a port check
            # we couldn't run. The deploy itself will fail with a
            # clearer error if the host is genuinely unreachable.
            conflicts.append(PortConflict(
                host_ip=host.ip_address, port=0,
                label=f"ssh-precheck-failed:{e.__class__.__name__}",
                process=str(e)[:100],
            ))
            continue
        for c in host_checks:
            if c.port in listening:
                conflicts.append(PortConflict(
                    host_ip=host.ip_address,
                    port=c.port,
                    label=c.label,
                    process=listening[c.port],
                ))
    # Strip the "ssh-precheck-failed" sentinel rows when constructing the
    # human message (caller can still see them via the `label`).
    return conflicts


def cluster_port_checks(cluster, services: list, cluster_config: dict) -> list[PortCheck]:
    """Build the list of port checks for a managed cluster deploy.

    Skips the cluster's OWN previously-deployed broker if it's currently
    running on those ports — a redeploy is allowed to take its own ports
    back. Detection: if there's a Service row for this cluster on this
    host with the same role, it owns the port.

    For now we just enumerate "what the new deploy will try to bind"
    and let the caller decide. The "redeploy reclaims own port"
    exception is handled by the deployer before calling us — it stops
    the old kafka-<cluster>-<id>.service first.
    """
    checks: list[PortCheck] = []
    listener = int(cluster_config.get("listener_port", 9092))
    controller = int(cluster_config.get("controller_port", 9093))
    ssl_listener = int(cluster_config.get("ssl_listener_port", 9096))
    sr_port = int(cluster_config.get("schema_registry_port", 8085))
    ksqldb_port = int(cluster_config.get("ksqldb_port", 8088))
    connect_port = int(cluster_config.get("connect_rest_port", 8083))

    for svc in services:
        if svc.role in ("broker", "broker_controller"):
            checks.append(PortCheck(svc.host_id, "", listener, "listener"))
            if cluster_config.get("ssl_enabled") or getattr(cluster, "ssl_enabled", False):
                checks.append(PortCheck(svc.host_id, "", ssl_listener, "ssl_listener"))
        if svc.role in ("controller", "broker_controller"):
            checks.append(PortCheck(svc.host_id, "", controller, "controller"))
        if svc.role == "schema_registry":
            checks.append(PortCheck(svc.host_id, "", sr_port, "schema_registry"))
        if svc.role == "ksqldb":
            checks.append(PortCheck(svc.host_id, "", ksqldb_port, "ksqldb"))
        if svc.role == "kafka_connect":
            checks.append(PortCheck(svc.host_id, "", connect_port, "kafka_connect"))
    return checks


def find_free_port_set(occupied: dict[int, set[int]], host_ids: Iterable[str]) -> dict[str, int]:
    """Pick a {listener, controller, ssl_listener} set that doesn't
    conflict with any port already in use on any of `host_ids`.

    `occupied` is {host_id: {ports}} — pass the union of ports configured
    on existing clusters for the same host(s). Returns a fresh dict.

    Strategy: try (9092, 9093, 9096), then (9192, 9193, 9196), etc.
    Increment by 100 — gives space for JMX (+ -979) and SR (+ -7) too.
    """
    used: set[int] = set()
    for h in host_ids:
        used |= occupied.get(h, set())

    for offset in range(0, 1000, 100):
        cand = {
            "listener_port": 9092 + offset,
            "controller_port": 9093 + offset,
            "ssl_listener_port": 9096 + offset,
        }
        if not any(p in used for p in cand.values()):
            return cand
    # Shouldn't happen unless the host has 10 clusters — fall back to defaults.
    return {"listener_port": 9092, "controller_port": 9093, "ssl_listener_port": 9096}
