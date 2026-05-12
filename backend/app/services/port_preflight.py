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
    """Return {port: process-description} for every port in `ports`.

    v1.4.3 #16 — when ss reports a port held by `java pid=X`, we
    also walk `systemctl status -- --pid=X` (via cgroup lookup) to
    resolve the holding unit name. Previously the description was just
    "java pid=32333" which leaves the operator guessing whether it's a
    Tantor cluster or a hand-installed Kafka. Now we surface e.g.
    "java pid=32333 (kafka-prod-1ac9bbbe.service)".

    Uses `sudo -n ss -tnlp` if available so we get the PID even without
    being root. Falls back to no-sudo `ss` then `netstat`.
    """
    port_list = " ".join(str(p) for p in ports)
    cmd = (
        f"if command -v ss >/dev/null 2>&1; then "
        f"  for p in {port_list}; do "
        # sudo -n first (gives us PIDs); fall back to no-sudo (no PIDs
        # but still tells us "port is held").
        f"    out=$(sudo -n ss -tnlpH 'sport = :'$p 2>/dev/null || ss -tnlH 'sport = :'$p 2>/dev/null); "
        f"    echo \"$out\" | awk -v p=$p 'NF{{print p\"::\"$0}}'; "
        f"  done; "
        f"else "
        f"  for p in {port_list}; do "
        f"    netstat -tnlp 2>/dev/null | awk -v p=\":\"$p "
        f"      '$4 ~ p {{print substr(p,2)\"::\"$0}}'; "
        f"  done; "
        f"fi"
    )
    exit_code, stdout, _ = SSHManager.exec_command(client, cmd, timeout=15)
    if exit_code != 0 or not stdout:
        return {}

    result: dict[int, str] = {}
    pids_to_resolve: set[int] = set()
    pid_for_port: dict[int, int] = {}
    for line in stdout.splitlines():
        if "::" not in line:
            continue
        port_str, _, rest = line.partition("::")
        try:
            port = int(port_str)
        except ValueError:
            continue
        if port in result:
            continue
        proc = "unknown"
        pid_int: int | None = None
        if "users:" in rest:
            users_part = rest.split("users:", 1)[1].strip().strip("(()")
            proc = users_part
            # Extract pid for unit lookup: "(\"java\",pid=32333,fd=164)"
            import re as _re
            m = _re.search(r"pid=(\d+)", users_part)
            if m:
                pid_int = int(m.group(1))
        elif "/" in rest:
            tail = rest.strip().split()[-1] if rest.strip().split() else ""
            proc = tail
            import re as _re
            m = _re.search(r"(\d+)/", tail)
            if m:
                pid_int = int(m.group(1))
        result[port] = proc
        if pid_int:
            pids_to_resolve.add(pid_int)
            pid_for_port[port] = pid_int

    # Resolve each PID's owning systemd unit in a single SSH round-trip.
    if pids_to_resolve:
        pid_args = " ".join(str(p) for p in pids_to_resolve)
        unit_cmd = (
            f"for pid in {pid_args}; do "
            # systemctl status takes a pid via --pid; output line 1 has the unit.
            f"  u=$(systemctl status --no-pager -p Id --value -- $pid 2>/dev/null"
            f"      || awk -F/ '/^0::/{{print $NF}}' /proc/$pid/cgroup 2>/dev/null"
            f"      | sed 's/\\.service.*$/.service/'); "
            f"  echo \"$pid=$u\"; "
            f"done"
        )
        rc2, out2, _ = SSHManager.exec_command(client, unit_cmd, timeout=10)
        if rc2 == 0 and out2:
            pid_unit: dict[int, str] = {}
            for line in out2.splitlines():
                if "=" not in line:
                    continue
                pid_s, _, unit = line.partition("=")
                unit = unit.strip()
                try:
                    pid_unit[int(pid_s)] = unit
                except ValueError:
                    continue
            for port, pid_int in pid_for_port.items():
                unit = pid_unit.get(pid_int, "")
                if unit and unit not in ("0", "n/a", ""):
                    result[port] = f"{result[port]} ({unit})"
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
