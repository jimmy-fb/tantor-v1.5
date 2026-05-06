"""Monitoring API — Built-in Kafka & system metrics via SSH + optional Prometheus/Grafana."""

import logging
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.cluster import Cluster
from app.models.host import Host
from app.models.service import Service
from app.models.user import User
from app.services.ssh_manager import SSHManager
from app.services.monitoring_deployer import MonitoringDeployer
from app.api.deps import require_monitor_or_above, require_admin

logger = logging.getLogger("tantor.monitoring")
router = APIRouter(prefix="/api/monitoring", tags=["monitoring"])


class MonitoringDeployRequest(BaseModel):
    monitoring_host_id: str
    grafana_port: int = 3000
    prometheus_port: int = 9090
    # Only consulted for external clusters — list of "host:port" the customer's
    # brokers expose JMX (or JMX exporter) on. Tantor's Prometheus scrapes
    # these directly. Ignored for managed clusters where Tantor pushes its own
    # JMX exporter to each broker.
    external_jmx_endpoints: list[str] | None = None


def _ssh_exec(host: Host, command: str, timeout: int = 15) -> str:
    """Execute command on host and return stdout."""
    try:
        with SSHManager.connect(
            host.ip_address, host.ssh_port, host.username,
            host.auth_type, host.encrypted_credential,
        ) as client:
            exit_code, stdout, stderr = SSHManager.exec_command(client, command, timeout=timeout)
            return stdout.strip() if exit_code == 0 else ""
    except Exception as e:
        logger.warning(f"SSH to {host.ip_address} failed: {e}")
        return ""


@router.get("/status")
def get_monitoring_status(_: User = Depends(require_monitor_or_above)):
    """Return monitoring status — built-in, always available."""
    return {
        "enabled": True,
        "type": "built-in",
        "description": "Built-in Kafka & system metrics via SSH. No external tools required.",
    }


@router.get("/clusters/{cluster_id}/metrics")
def get_cluster_metrics(
    cluster_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    """Get live metrics for all nodes in a cluster.

    APB v1.4.0 #14 — also works for external clusters now: we synthesize
    broker entries from kafka-python's describe_cluster() and pull Kafka-
    level metrics (topic count, ISR, etc) from the AdminClient. System
    metrics (CPU/memory/disk) require SSH and stay unavailable on
    external clusters unless the operator registers the broker hosts via
    `external_broker_hosts_json`.
    """
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")

    if (cluster.kind or "managed") == "external":
        return _external_cluster_metrics(cluster, db)

    services = db.query(Service).filter(Service.cluster_id == cluster_id).all()
    nodes = []

    # Deduplicate by host_id — multiple services can run on the same host (#10, #15)
    seen_hosts: dict[str, dict] = {}
    for svc in services:
        host = db.query(Host).filter(Host.id == svc.host_id).first()
        if not host:
            continue

        if host.id in seen_hosts:
            # Append role to existing node entry
            existing = seen_hosts[host.id]
            existing["role"] = existing["role"] + ", " + svc.role
            existing["node_id"] = min(existing["node_id"], svc.node_id)
            continue

        node_metrics = {
            "host_id": host.id,
            "hostname": host.hostname,
            "ip_address": host.ip_address,
            "role": svc.role,
            "node_id": svc.node_id,
            "status": svc.status,
            "system": _get_system_metrics(host),
            "kafka": _get_kafka_metrics(host),
            "disk": _get_disk_metrics(host),
        }
        seen_hosts[host.id] = node_metrics
        nodes.append(node_metrics)

    return {
        "cluster_id": cluster_id,
        "cluster_name": cluster.name,
        "nodes": nodes,
    }


def _external_cluster_metrics(cluster: Cluster, db: Session) -> dict:
    """Synthesize per-broker rows for an external cluster.

    APB v1.4.0 #14. We don't own SSH to the customer's brokers (in the
    common case), so we can only return what kafka-python's AdminClient
    exposes plus whatever JMX metrics Prometheus has scraped (if the
    operator deployed monitoring against `external_jmx_endpoints`).
    """
    from app.services import external_admin
    from app.models.host import Host as _Host

    # Broker list via test_connection() — only describe_cluster surface
    # external_admin exposes today.
    info = external_admin.test_connection(cluster)
    if not info.get("success"):
        return {
            "cluster_id": cluster.id,
            "cluster_name": cluster.name,
            "nodes": [],
            "error": info.get("message", "AdminClient connection failed"),
            "external": True,
        }

    # Best-effort cluster-wide Kafka metrics (single AdminClient round-trip)
    cluster_kafka: dict = {}
    try:
        topics = external_admin.list_topics(cluster) or []
        cluster_kafka["topics"] = len(topics)
        cluster_kafka["partitions"] = sum(t.get("partitions", 0) for t in topics)
    except Exception:
        pass

    # If the operator registered SSH-able hosts for the external brokers
    # (external_broker_hosts_json), we'll thread system metrics in too.
    ssh_hosts: dict[str, _Host] = {}
    try:
        import json as _json
        registered = _json.loads(cluster.external_broker_hosts_json or "[]")
        # registered is a list of {broker_id, host_id} mappings
        host_ids = {entry.get("host_id") for entry in registered if entry.get("host_id")}
        if host_ids:
            for h in db.query(_Host).filter(_Host.id.in_(host_ids)).all():
                ssh_hosts[h.id] = h
        # Index by broker_id
        broker_to_host = {entry["broker_id"]: ssh_hosts.get(entry["host_id"])
                          for entry in registered
                          if entry.get("broker_id") is not None and entry.get("host_id")}
    except Exception:
        broker_to_host = {}

    nodes: list[dict] = []
    for b in info.get("brokers", []):
        bid = b.get("node_id")  # test_connection returns kafka-python's node_id
        host_obj = broker_to_host.get(bid)
        node = {
            "host_id": host_obj.id if host_obj else f"external-{bid}",
            "hostname": host_obj.hostname if host_obj else b.get("host", ""),
            "ip_address": host_obj.ip_address if host_obj else b.get("host", ""),
            "role": "broker",
            "node_id": bid,
            "status": "connected",
            "external": True,
            "kafka": dict(cluster_kafka, port=b.get("port")),
        }
        if host_obj:
            node["system"] = _get_system_metrics(host_obj)
            node["disk"] = _get_disk_metrics(host_obj)
        else:
            node["system"] = {"unavailable": "external cluster — register broker host for SSH metrics"}
            node["disk"] = {"unavailable": "external cluster — register broker host for SSH metrics"}
        nodes.append(node)

    return {
        "cluster_id": cluster.id,
        "cluster_name": cluster.name,
        "nodes": nodes,
        "external": True,
    }


def _get_system_metrics(host: Host) -> dict:
    """Get CPU, memory, uptime from a host."""
    # All in one SSH call for performance
    cmd = """bash -c '
echo "UPTIME:$(uptime -s 2>/dev/null || uptime | head -1)"
echo "LOAD:$(cat /proc/loadavg 2>/dev/null || echo "0 0 0")"
echo "CPU_CORES:$(nproc 2>/dev/null || echo 1)"

# Memory
MEM=$(free -m 2>/dev/null | grep Mem:)
TOTAL=$(echo $MEM | awk "{print \\$2}")
USED=$(echo $MEM | awk "{print \\$3}")
AVAIL=$(echo $MEM | awk "{print \\$7}")
echo "MEM_TOTAL_MB:$TOTAL"
echo "MEM_USED_MB:$USED"
echo "MEM_AVAIL_MB:$AVAIL"

# CPU usage — use /proc/stat for reliable cross-distro measurement
read -r _ u1 n1 s1 i1 w1 _ < /proc/stat
sleep 1
read -r _ u2 n2 s2 i2 w2 _ < /proc/stat
IDLE=$((i2 - i1))
TOTAL=$(( (u2+n2+s2+i2+w2) - (u1+n1+s1+i1+w1) ))
if [ "$TOTAL" -gt 0 ]; then
    CPU_IDLE=$((IDLE * 100 / TOTAL))
else
    CPU_IDLE=100
fi
echo "CPU_IDLE:$CPU_IDLE"
'"""
    output = _ssh_exec(host, cmd, timeout=15)
    if not output:
        return {"error": "unreachable"}

    metrics = {}
    for line in output.splitlines():
        if ":" in line:
            key, val = line.split(":", 1)
            metrics[key.strip()] = val.strip()

    def safe_int(val: str, default: int = 0) -> int:
        try:
            return int(val.strip()) if val.strip() else default
        except (ValueError, AttributeError):
            return default

    def safe_float(val: str, default: float = 0.0) -> float:
        try:
            return float(val.strip()) if val.strip() else default
        except (ValueError, AttributeError):
            return default

    try:
        load_parts = metrics.get("LOAD", "0 0 0").split()
        cpu_cores = safe_int(metrics.get("CPU_CORES", "1"), 1)
        mem_total = safe_int(metrics.get("MEM_TOTAL_MB", "0"))
        mem_used = safe_int(metrics.get("MEM_USED_MB", "0"))
        mem_avail = safe_int(metrics.get("MEM_AVAIL_MB", "0"))
        cpu_idle = safe_float(metrics.get("CPU_IDLE", "0"))

        return {
            "uptime": metrics.get("UPTIME", "unknown"),
            "cpu_cores": cpu_cores,
            "load_1m": safe_float(load_parts[0]) if load_parts else 0,
            "load_5m": safe_float(load_parts[1]) if len(load_parts) > 1 else 0,
            "load_15m": safe_float(load_parts[2]) if len(load_parts) > 2 else 0,
            "cpu_usage_pct": round(100.0 - cpu_idle, 1),
            "memory_total_mb": mem_total,
            "memory_used_mb": mem_used,
            "memory_available_mb": mem_avail,
            "memory_usage_pct": round((mem_used / mem_total * 100), 1) if mem_total > 0 else 0,
        }
    except Exception as e:
        return {"error": str(e)}


def _get_kafka_metrics(host: Host) -> dict:
    """Get Kafka broker metrics — service status, log size, topic count."""
    cmd = """bash -c '
# Service status
ACTIVE=$(systemctl is-active kafka 2>/dev/null || echo "unknown")
echo "KAFKA_STATUS:$ACTIVE"

# PID and uptime
PID=$(systemctl show kafka -p MainPID --value 2>/dev/null || echo 0)
echo "KAFKA_PID:$PID"
if [ "$PID" != "0" ] && [ -d "/proc/$PID" ]; then
    START=$(stat -c %Y /proc/$PID 2>/dev/null || echo 0)
    NOW=$(date +%s)
    UPTIME_SECS=$((NOW - START))
    echo "KAFKA_UPTIME_SECS:$UPTIME_SECS"

    # JVM memory from /proc
    RSS=$(awk "/^VmRSS/{print \\$2}" /proc/$PID/status 2>/dev/null || echo 0)
    echo "KAFKA_RSS_KB:$RSS"
else
    echo "KAFKA_UPTIME_SECS:0"
    echo "KAFKA_RSS_KB:0"
fi

# Data directory size
DATA_SIZE=$(du -sm /var/lib/kafka/data 2>/dev/null | awk "{print \\$1}" || sudo du -sm /var/lib/kafka/data 2>/dev/null | awk "{print \\$1}" || echo 0)
echo "KAFKA_DATA_MB:$DATA_SIZE"

# Log directory size
LOG_SIZE=$(du -sm /opt/kafka/logs 2>/dev/null | awk "{print \\$1}" || sudo du -sm /opt/kafka/logs 2>/dev/null | awk "{print \\$1}" || echo 0)
echo "KAFKA_LOG_MB:$LOG_SIZE"

# Topic & partition count (prefer kafka CLI for accuracy, fall back to filesystem)
JAVA_HOME_DIR=$(readlink -f $(which java 2>/dev/null) 2>/dev/null | sed "s|/bin/java||" || echo "/usr")
export JAVA_HOME=$JAVA_HOME_DIR
TOPIC_INFO=$(/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -v "^$" | wc -l || echo -1)
if [ "$TOPIC_INFO" -ge 0 ] 2>/dev/null; then
    echo "KAFKA_TOPICS:$TOPIC_INFO"
    PART_INFO=$(/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe 2>/dev/null | grep "PartitionCount" | awk -F'PartitionCount:' "{sum+=\\$2} END {print sum+0}" || echo 0)
    echo "KAFKA_PARTITIONS:$PART_INFO"
else
    # Fallback to filesystem
    TOPICS=$(ls -d /var/lib/kafka/data/*-* 2>/dev/null | sed "s/-[0-9]*$//" | sort -u | wc -l || echo 0)
    echo "KAFKA_TOPICS:$TOPICS"
    PARTITIONS=$(ls -d /var/lib/kafka/data/*-* 2>/dev/null | wc -l || echo 0)
    echo "KAFKA_PARTITIONS:$PARTITIONS"
fi

# Open file descriptors (try direct, then sudo)
if [ "$PID" != "0" ] && [ -d "/proc/$PID" ]; then
    FDS=$(ls /proc/$PID/fd 2>/dev/null | wc -l || echo 0)
    if [ "$FDS" = "0" ]; then
        FDS=$(sudo ls /proc/$PID/fd 2>/dev/null | wc -l || echo 0)
    fi
    echo "KAFKA_FDS:$FDS"
else
    echo "KAFKA_FDS:0"
fi

# Network connections on 9092
CONNECTIONS=$(ss -tn 2>/dev/null | grep -c ":9092" || echo 0)
echo "KAFKA_CONNECTIONS:$CONNECTIONS"
'"""
    output = _ssh_exec(host, cmd, timeout=15)
    if not output:
        return {"error": "unreachable"}

    metrics = {}
    for line in output.splitlines():
        if ":" in line:
            key, val = line.split(":", 1)
            metrics[key.strip()] = val.strip()

    def safe_int(val: str, default: int = 0) -> int:
        try:
            return int(val.strip()) if val.strip() else default
        except (ValueError, AttributeError):
            return default

    try:
        uptime_secs = safe_int(metrics.get("KAFKA_UPTIME_SECS", "0"))
        hours = uptime_secs // 3600
        minutes = (uptime_secs % 3600) // 60

        return {
            "status": metrics.get("KAFKA_STATUS", "unknown"),
            "pid": safe_int(metrics.get("KAFKA_PID", "0")),
            "uptime": f"{hours}h {minutes}m" if uptime_secs > 0 else "not running",
            "uptime_seconds": uptime_secs,
            "memory_rss_mb": round(safe_int(metrics.get("KAFKA_RSS_KB", "0")) / 1024, 1),
            "data_size_mb": safe_int(metrics.get("KAFKA_DATA_MB", "0")),
            "log_size_mb": safe_int(metrics.get("KAFKA_LOG_MB", "0")),
            "topics": safe_int(metrics.get("KAFKA_TOPICS", "0")),
            "partitions": safe_int(metrics.get("KAFKA_PARTITIONS", "0")),
            "open_fds": safe_int(metrics.get("KAFKA_FDS", "0")),
            "connections": safe_int(metrics.get("KAFKA_CONNECTIONS", "0")),
        }
    except Exception as e:
        return {"error": str(e)}


def _get_disk_metrics(host: Host) -> dict:
    """Get disk usage for Kafka data and root partitions."""
    cmd = """bash -c '
df -m / 2>/dev/null | tail -1 | awk "{print \\"ROOT_TOTAL_MB:\\"\\$2\\"\\nROOT_USED_MB:\\"\\$3\\"\\nROOT_AVAIL_MB:\\"\\$4\\"\\nROOT_USE_PCT:\\"\\$5}"
df -m /var/lib/kafka/data 2>/dev/null | tail -1 | awk "{print \\"DATA_TOTAL_MB:\\"\\$2\\"\\nDATA_USED_MB:\\"\\$3\\"\\nDATA_AVAIL_MB:\\"\\$4\\"\\nDATA_USE_PCT:\\"\\$5}"
'"""
    output = _ssh_exec(host, cmd, timeout=10)
    if not output:
        return {"error": "unreachable"}

    metrics = {}
    for line in output.splitlines():
        if ":" in line:
            key, val = line.split(":", 1)
            metrics[key.strip()] = val.strip().rstrip("%")

    try:
        return {
            "root": {
                "total_mb": int(metrics.get("ROOT_TOTAL_MB", "0")),
                "used_mb": int(metrics.get("ROOT_USED_MB", "0")),
                "available_mb": int(metrics.get("ROOT_AVAIL_MB", "0")),
                "usage_pct": int(metrics.get("ROOT_USE_PCT", "0")),
            },
            "data": {
                "total_mb": int(metrics.get("DATA_TOTAL_MB", "0")),
                "used_mb": int(metrics.get("DATA_USED_MB", "0")),
                "available_mb": int(metrics.get("DATA_AVAIL_MB", "0")),
                "usage_pct": int(metrics.get("DATA_USE_PCT", "0")),
            },
        }
    except Exception as e:
        return {"error": str(e)}


# ── Prometheus/Grafana deployment ─────────────────────

@router.post("/clusters/{cluster_id}/deploy")
def deploy_monitoring(
    cluster_id: str,
    req: MonitoringDeployRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Deploy Prometheus + Grafana + JMX exporter for a cluster.

    Works for both managed clusters (Tantor pushes JMX exporter to each
    broker via SSH) and external clusters (customer exposes JMX themselves
    and passes the endpoints in `external_jmx_endpoints`).
    """
    try:
        result = MonitoringDeployer.deploy_monitoring_stack(
            cluster_id, req.monitoring_host_id,
            req.grafana_port, req.prometheus_port, db,
            external_jmx_endpoints=req.external_jmx_endpoints,
        )
        # Seed the four default alert rules + reload Prometheus so external
        # cluster operators get the same out-of-the-box alerting story as
        # managed clusters.
        try:
            from app.services import alert_manager as _am
            seeded = _am.seed_default_rules(cluster_id, db)
            if seeded:
                mon_host = db.query(Host).filter(Host.id == req.monitoring_host_id).first()
                if mon_host:
                    MonitoringDeployer._render_alerting_files(cluster_id, mon_host, db)
                result.setdefault("steps", []).append({
                    "step": f"Seeded {seeded} default alert rule(s)",
                    "status": "success",
                })
        except Exception as seed_e:
            result.setdefault("steps", []).append({
                "step": "Seed default alert rules",
                "status": "failed",
                "error": str(seed_e),
            })
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/clusters/{cluster_id}/grafana")
def get_grafana_info(
    cluster_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    """Get Grafana connection info for embedding."""
    return MonitoringDeployer.get_grafana_info(cluster_id, db)


# ── Capacity trend forecasting ──────────────────────────────────────────────
# APB-requested feature: predict future storage and throughput growth using
# historical cluster metrics so they can plan infrastructure proactively.
# Implementation: pull 14 days of disk usage from Prometheus, fit a linear
# regression on (time, used_bytes), project forward, return ETA-to-X%-full
# plus the raw series so the UI can chart it.

import urllib.parse
import urllib.request
import json
import time
import math

from app.models.monitoring import MonitoringConfig


def _prom_query_range(prom_url: str, query: str, start_ts: float, end_ts: float, step: int) -> list[tuple[float, float]]:
    """Run a Prometheus query_range and return [(timestamp, value), ...]."""
    params = urllib.parse.urlencode({
        "query": query,
        "start": f"{start_ts:.0f}",
        "end": f"{end_ts:.0f}",
        "step": str(step),
    })
    url = f"{prom_url}/api/v1/query_range?{params}"
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.loads(r.read())
    if data.get("status") != "success" or not data.get("data", {}).get("result"):
        return []
    # Sum across instances at each timestamp so multi-broker clusters give a
    # single capacity series instead of per-broker.
    by_ts: dict[float, float] = {}
    for series in data["data"]["result"]:
        for ts, val in series.get("values", []):
            try:
                f = float(val)
                if math.isnan(f) or math.isinf(f):
                    continue
                by_ts[float(ts)] = by_ts.get(float(ts), 0.0) + f
            except (TypeError, ValueError):
                continue
    return sorted(by_ts.items())


def _linear_fit(xs: list[float], ys: list[float]) -> tuple[float, float]:
    """Return (slope, intercept) for least-squares fit. Empty input → (0, 0)."""
    n = len(xs)
    if n < 2:
        return (0.0, ys[0] if ys else 0.0)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n))
    den = sum((xs[i] - mean_x) ** 2 for i in range(n))
    slope = num / den if den != 0 else 0.0
    intercept = mean_y - slope * mean_x
    return (slope, intercept)


@router.get("/clusters/{cluster_id}/capacity-forecast")
def capacity_forecast(
    cluster_id: str,
    days_history: int = 14,
    days_forecast: int = 30,
    full_threshold: float = 0.85,
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    """Storage growth forecast based on Prometheus history.

    Returns the historical disk-used series, a linear projection forward
    `days_forecast` days, and an ETA for when the disk will hit
    `full_threshold` (default 85%). When monitoring isn't deployed or there
    isn't enough history, returns an empty `forecast` with a `reason` so
    the UI can render an empty state.
    """
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    config = db.query(MonitoringConfig).filter(MonitoringConfig.cluster_id == cluster_id).first()
    if not config or not config.deployed or not config.prometheus_url:
        return {
            "available": False,
            "reason": "Monitoring not deployed for this cluster — deploy Prometheus first.",
        }

    now = time.time()
    start = now - days_history * 86400
    step = 3600  # one sample per hour is plenty for a multi-day forecast

    try:
        used = _prom_query_range(
            config.prometheus_url,
            'node_filesystem_size_bytes{mountpoint="/"} - node_filesystem_avail_bytes{mountpoint="/"}',
            start, now, step,
        )
        total = _prom_query_range(
            config.prometheus_url,
            'node_filesystem_size_bytes{mountpoint="/"}',
            start, now, step,
        )
    except Exception as e:
        return {"available": False, "reason": f"Prometheus query failed: {e}"}

    if len(used) < 2 or not total:
        return {
            "available": False,
            "reason": (
                f"Not enough history yet ({len(used)} sample(s)). "
                "Forecast requires monitoring deployed for at least 2 hours."
            ),
        }

    used_xs = [t for t, _ in used]
    used_ys = [v for _, v in used]
    slope, intercept = _linear_fit(used_xs, used_ys)

    # Latest total disk size — assume mostly stable; use the most recent value.
    latest_total = total[-1][1]
    latest_used = used_ys[-1]
    pct_now = (latest_used / latest_total) * 100 if latest_total else 0.0

    # Generate the projected series: same step (3600s) for `days_forecast` ahead.
    projected: list[tuple[float, float]] = []
    forecast_start = now
    forecast_end = now + days_forecast * 86400
    t = forecast_start
    while t <= forecast_end:
        projected.append((t, slope * t + intercept))
        t += step

    # ETA — time when the line crosses full_threshold * total
    eta: float | None = None
    if slope > 0:
        target = full_threshold * latest_total
        if intercept < target:
            eta_unix = (target - intercept) / slope
            if eta_unix > now:
                eta = eta_unix

    return {
        "available": True,
        "history": [{"t": t, "used_bytes": v} for t, v in used],
        "forecast": [{"t": t, "used_bytes": v} for t, v in projected],
        "total_bytes": latest_total,
        "current_used_bytes": latest_used,
        "current_used_pct": round(pct_now, 2),
        "growth_bytes_per_day": slope * 86400,
        "eta_to_threshold_unix": eta,
        "eta_to_threshold_days": (eta - now) / 86400 if eta else None,
        "full_threshold": full_threshold,
    }


# ── Detailed per-cluster monitoring summary ────────────────────────────────
# APB asked for "monitoring can be detailed" inside the per-cluster tab.
# This endpoint hits Prometheus once and returns a digest the UI renders as
# a dashboard panel — total throughput, broker up/down, top-N topics by
# rate, top-N consumer groups by lag, JVM heap, GC count, scrape targets.
# Doing the fan-out server-side avoids CORS issues + keeps Prom credentials
# (none today, but eventually basic auth) on the backend.


def _prom_query(prom_url: str, query: str) -> list[dict]:
    """Run an instant Prometheus query. Returns the raw `result` array."""
    params = urllib.parse.urlencode({"query": query})
    url = f"{prom_url}/api/v1/query?{params}"
    try:
        with urllib.request.urlopen(url, timeout=8) as r:
            data = json.loads(r.read())
    except Exception:
        return []
    if data.get("status") != "success":
        return []
    return data.get("data", {}).get("result", [])


def _scalar_sum(prom_url: str, query: str) -> float:
    """Sum the values of every series returned by `query`."""
    total = 0.0
    for r in _prom_query(prom_url, query):
        try:
            total += float(r["value"][1])
        except (KeyError, ValueError, TypeError):
            continue
    return total


def _top_n(prom_url: str, query: str, label: str, n: int = 5) -> list[dict]:
    """Return [{label_name: ..., value: float}] sorted desc, top N."""
    rows = []
    for r in _prom_query(prom_url, query):
        try:
            rows.append({
                "key": r["metric"].get(label, "?"),
                "value": float(r["value"][1]),
            })
        except (KeyError, ValueError, TypeError):
            continue
    rows.sort(key=lambda x: x["value"], reverse=True)
    return rows[:n]


@router.get("/clusters/{cluster_id}/summary")
def monitoring_summary(
    cluster_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    config = db.query(MonitoringConfig).filter(MonitoringConfig.cluster_id == cluster_id).first()
    if not config or not config.deployed or not config.prometheus_url:
        return {"available": False, "reason": "Monitoring not deployed for this cluster."}

    p = config.prometheus_url

    # Scrape targets — directly from Prometheus's `up` time-series. value=1 up.
    scrape_targets: list[dict] = []
    for r in _prom_query(p, 'up'):
        scrape_targets.append({
            "job": r["metric"].get("job", "?"),
            "instance": r["metric"].get("instance", "?"),
            "up": r["value"][1] == "1",
        })

    return {
        "available": True,
        "throughput": {
            "messages_in_per_sec": _scalar_sum(
                p, 'sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_count{topic=""}[1m]))'
            ) or _scalar_sum(
                p, 'sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_count[1m]))'
            ) / 2,  # halve when topic="" filter empty (per-topic + cluster doubles up)
            "bytes_in_per_sec": _scalar_sum(
                p, 'sum(rate(kafka_server_brokertopicmetrics_bytesinpersec_count{topic=""}[1m]))'
            ) or _scalar_sum(
                p, 'sum(rate(kafka_server_brokertopicmetrics_bytesinpersec_count[1m]))'
            ) / 2,
            "bytes_out_per_sec": _scalar_sum(
                p, 'sum(rate(kafka_server_brokertopicmetrics_bytesoutpersec_count{topic=""}[1m]))'
            ) or _scalar_sum(
                p, 'sum(rate(kafka_server_brokertopicmetrics_bytesoutpersec_count[1m]))'
            ) / 2,
        },
        "scrape_targets": scrape_targets,
        "broker_up_count": sum(1 for t in scrape_targets if t["up"] and t["job"] == "kafka-jmx"),
        "broker_total_count": sum(1 for t in scrape_targets if t["job"] == "kafka-jmx"),
        "top_topics_by_msgs": _top_n(
            p,
            'topk(5, sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesinpersec_count{topic!=""}[5m])))',
            "topic",
        ),
        "top_consumer_groups_by_lag": _top_n(
            p,
            'topk(5, sum by (consumergroup) (kafka_consumergroup_lag))',
            "consumergroup",
        ),
        "under_replicated_partitions": int(_scalar_sum(
            p, 'sum(kafka_server_replicamanager_underreplicatedpartitions_value)'
        )),
        "jvm_heap_mb": round(_scalar_sum(p, 'sum(jvm_memory_heap_used) / 1024 / 1024'), 1),
        "jvm_gc_count_per_sec": round(_scalar_sum(p, 'sum(rate(jvm_gc_collection_count[5m]))'), 3),
    }
