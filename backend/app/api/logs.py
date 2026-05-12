"""Cluster service log endpoints — fetch and stream via SSH + journalctl."""

import asyncio
import threading

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from sqlalchemy.orm import Session

from app.database import get_db, SessionLocal
from app.models.cluster import Cluster
from app.models.service import Service
from app.models.host import Host
from app.models.user import User
from app.services.log_manager import log_manager
from app.api.deps import require_monitor_or_above, get_ws_user

router = APIRouter(tags=["logs"])


@router.get("/api/clusters/{cluster_id}/logs")
def get_service_logs(
    cluster_id: str,
    service_id: str = Query(None, description="Filter by specific service ID"),
    role: str = Query(None, description="Filter by role (broker, controller, ksqldb, etc.)"),
    lines: int = Query(200, ge=10, le=5000, description="Number of log lines"),
    since: str = Query(None, description="Time filter (e.g., '1 hour ago', '2024-01-01')"),
    priority: str = Query(None, description="Priority filter (emerg, alert, crit, err, warning, notice, info, debug)"),
    grep: str = Query(None, description="Text filter (case-insensitive grep)"),
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    """Fetch historical logs for services in a cluster."""
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")

    # Get services to query
    query = db.query(Service).filter(Service.cluster_id == cluster_id)
    if service_id:
        query = query.filter(Service.id == service_id)
    if role:
        query = query.filter(Service.role == role)
    services = query.all()

    if not services:
        raise HTTPException(status_code=404, detail="No services found matching criteria")

    hosts = {h.id: h for h in db.query(Host).all()}
    results = []

    # v1.2.0 #5: pass per-cluster systemd unit + install dir so the
    # log fetch hits "kafka-prod-XYZ.service" / "/opt/kafka-prod-XYZ/logs"
    # not the legacy "kafka.service" / "/opt/kafka/logs".
    from app.services import cluster_paths
    unit_for_role = (
        cluster_paths.unit_name(cluster)
        if (cluster.kind or "managed") == "managed"
        else None
    )
    install_dir_for_role = (
        cluster_paths.install_dir(cluster)
        if (cluster.kind or "managed") == "managed"
        else None
    )

    for svc in services:
        host = hosts.get(svc.host_id)
        if not host:
            continue
        # Only override for kafka roles — ksqlDB and Connect have their own
        # unit names that aren't per-cluster yet.
        is_kafka_role = svc.role in ("broker", "broker_controller", "controller", "zookeeper")
        log_data = log_manager.get_logs(
            host, svc.role,
            lines=lines, since=since,
            priority=priority, grep_filter=grep,
            unit_override=unit_for_role if is_kafka_role else None,
            kafka_install_dir=install_dir_for_role if is_kafka_role else None,
        )
        log_data["service_id"] = svc.id
        results.append(log_data)

    # If querying a single service, return flat response
    if len(results) == 1:
        return results[0]

    return results


@router.websocket("/api/ws/logs/{cluster_id}/{service_id}")
async def tail_service_logs(websocket: WebSocket, cluster_id: str, service_id: str, token: str = Query("")):
    """Stream real-time logs from a specific service via WebSocket."""
    # Authenticate
    db = SessionLocal()
    try:
        user = get_ws_user(token, db)
        if not user:
            await websocket.close(code=4001)
            return

        # Look up service and host
        svc = db.query(Service).filter(
            Service.id == service_id,
            Service.cluster_id == cluster_id,
        ).first()
        if not svc:
            await websocket.close(code=4004)
            return

        host = db.query(Host).filter(Host.id == svc.host_id).first()
        if not host:
            await websocket.close(code=4004)
            return

        # Copy host data before closing DB
        host_copy = Host(
            id=host.id, hostname=host.hostname, ip_address=host.ip_address,
            ssh_port=host.ssh_port, username=host.username,
            auth_type=host.auth_type, encrypted_credential=host.encrypted_credential,
        )
        role = svc.role
        # Resolve per-cluster unit/install dir before closing the DB session.
        from app.services import cluster_paths
        cluster_row = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        is_kafka_role = role in ("broker", "broker_controller", "controller", "zookeeper")
        unit_override = (
            cluster_paths.unit_name(cluster_row)
            if (cluster_row and (cluster_row.kind or "managed") == "managed" and is_kafka_role)
            else None
        )
        kafka_install_dir = (
            cluster_paths.install_dir(cluster_row)
            if (cluster_row and (cluster_row.kind or "managed") == "managed" and is_kafka_role)
            else None
        )
    finally:
        db.close()

    await websocket.accept()

    # Start log tailing in a background thread
    stop_event = threading.Event()
    log_queue: list[str] = []
    log_lock = threading.Lock()

    def tail_thread():
        try:
            for line in log_manager.tail_logs(
                host_copy, role,
                unit_override=unit_override,
                kafka_install_dir=kafka_install_dir,
            ):
                if stop_event.is_set():
                    break
                with log_lock:
                    log_queue.append(line)
        except Exception as e:
            with log_lock:
                log_queue.append(f"Error: {e}")

    thread = threading.Thread(target=tail_thread, daemon=True)
    thread.start()

    try:
        while True:
            # Forward queued log lines to websocket
            lines_to_send = []
            with log_lock:
                if log_queue:
                    lines_to_send = log_queue[:]
                    log_queue.clear()

            for line in lines_to_send:
                await websocket.send_json({"type": "log", "line": line})

            await asyncio.sleep(0.3)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        stop_event.set()
        try:
            await websocket.close()
        except Exception:
            pass
