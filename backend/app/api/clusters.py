import json
import uuid

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.cluster import Cluster
from app.models.service import Service
from app.models.host import Host
from app.schemas.cluster import (
    ClusterCreate, ClusterResponse, ClusterDetailResponse,
    ServiceResponse, DeploymentTaskResponse, ServiceAssignment,
    ClusterUpdateRequest,
)
from app.schemas.service import ServiceActionResponse
from app.services.deployer import deploy_cluster, get_task, init_task, deploy_schema_registry
from app.services.cluster_manager import cluster_manager
from app.api.deps import require_admin, require_monitor_or_above
from app.models.user import User

router = APIRouter(prefix="/api/clusters", tags=["clusters"])


@router.post("", response_model=ClusterResponse)
def create_cluster(data: ClusterCreate, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    host_ids = {s.host_id for s in data.services}
    existing = db.query(Host.id).filter(Host.id.in_(host_ids)).all()
    existing_ids = {h.id for h in existing}
    missing = host_ids - existing_ids
    if missing:
        raise HTTPException(status_code=400, detail=f"Hosts not found: {', '.join(missing)}")

    cluster = Cluster(
        name=data.name,
        kafka_version=data.kafka_version,
        mode=data.mode,
        config_json=json.dumps(data.config.model_dump()),
        environment=(data.environment or "").strip().lower(),
    )
    db.add(cluster)
    db.flush()  # populate cluster.id before path assignment

    # APB v1.2.0 #5 — give every new cluster its own Kafka install dir,
    # data dir, and systemd unit name so two clusters on the same broker
    # host coexist instead of stomping on /opt/kafka + kafka.service.
    from app.services import cluster_paths
    cluster_paths.assign_paths_for_new_cluster(cluster)

    for svc_data in data.services:
        svc = Service(
            cluster_id=cluster.id,
            host_id=svc_data.host_id,
            role=svc_data.role,
            node_id=svc_data.node_id,
        )
        db.add(svc)

    db.commit()
    db.refresh(cluster)
    return cluster


@router.get("", response_model=list[ClusterResponse])
def list_clusters(db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    return db.query(Cluster).order_by(Cluster.created_at.desc()).all()


@router.get("/{cluster_id}", response_model=ClusterDetailResponse)
def get_cluster(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    services = db.query(Service).filter(Service.cluster_id == cluster_id).all()

    # APB v1.2.0 #7: external clusters had an empty Overview tab because they
    # have no Service rows. Synthesize one row per broker by calling
    # describe_cluster — purely cosmetic, gives the operator a list of
    # connected brokers + ids on the same Overview screen as managed clusters.
    if (cluster.kind or "managed") == "external" and not services:
        try:
            from app.services import external_admin
            from app.models.service import Service as _SvcModel
            probe = external_admin.test_connection(cluster)
            for b in probe.get("brokers") or []:
                services.append(_SvcModel(
                    id=f"ext-{cluster.id[:8]}-{b.get('node_id', 0)}",
                    cluster_id=cluster.id,
                    host_id="",  # synthetic — no Tantor Host record
                    role="broker",
                    node_id=int(b.get("node_id", 0)),
                    config_overrides=None,
                    status="connected" if probe.get("success") else "unknown",
                ))
        except Exception:
            # Best-effort. Empty list is the existing behavior; never break the
            # detail load because the external cluster is momentarily unreachable.
            pass

    return ClusterDetailResponse(
        cluster=ClusterResponse.model_validate(cluster),
        services=[ServiceResponse.model_validate(s) for s in services],
    )


@router.delete("/{cluster_id}")
def delete_cluster(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    db.query(Service).filter(Service.cluster_id == cluster_id).delete()
    db.delete(cluster)
    db.commit()
    return {"detail": "Cluster deleted"}


@router.patch("/{cluster_id}", response_model=ClusterResponse)
def patch_cluster(
    cluster_id: str,
    data: ClusterUpdateRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Update mutable cluster metadata: name + environment tag.

    kafka_version, mode, and service composition aren't mutable here —
    they need a redeploy. This endpoint exists for renames + tagging.
    """
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    if data.name is not None:
        cluster.name = data.name.strip()
    if data.environment is not None:
        cluster.environment = data.environment.strip().lower()
    db.commit()
    db.refresh(cluster)
    return cluster


@router.post("/{cluster_id}/deploy", response_model=DeploymentTaskResponse)
def start_deployment(cluster_id: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    if cluster.kind == "external":
        raise HTTPException(status_code=400, detail="Deploy is not supported for externally-connected clusters")
    if cluster.state == "deploying":
        raise HTTPException(status_code=400, detail="Deployment already in progress")

    task_id = str(uuid.uuid4())
    init_task(task_id, cluster_id)
    # The deploy worker opens its own DB session — never pass the request's
    # session into a background task, that one closes when the response sends.
    background_tasks.add_task(deploy_cluster, cluster_id, task_id)
    return DeploymentTaskResponse(task_id=task_id, cluster_id=cluster_id, status="running")


@router.get("/{cluster_id}/deploy/{task_id}")
def get_deployment_status(
    cluster_id: str,
    task_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    task = get_task(task_id, db=db)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.get("/{cluster_id}/deploy")
def list_deployment_tasks(
    cluster_id: str,
    limit: int = 20,
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    """List recent deployment tasks for a cluster (most recent first)."""
    from app.models.deployment_task import DeploymentTask
    rows = (
        db.query(DeploymentTask)
        .filter(DeploymentTask.cluster_id == cluster_id)
        .order_by(DeploymentTask.started_at.desc())
        .limit(min(max(limit, 1), 100))
        .all()
    )
    return [
        {
            "task_id": r.id,
            "cluster_id": r.cluster_id,
            "status": r.status,
            "current_step": r.current_step,
            "error_message": r.error_message,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "finished_at": r.finished_at.isoformat() if r.finished_at else None,
        }
        for r in rows
    ]


@router.post("/{cluster_id}/start", response_model=list[ServiceActionResponse])
def start_cluster(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    if cluster.kind == "external":
        raise HTTPException(status_code=400, detail="start/stop are not supported for externally-connected clusters")
    results = cluster_manager.start_cluster(cluster_id, db)
    return [ServiceActionResponse(**r) for r in results]


@router.post("/{cluster_id}/stop", response_model=list[ServiceActionResponse])
def stop_cluster(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    if cluster.kind == "external":
        raise HTTPException(status_code=400, detail="start/stop are not supported for externally-connected clusters")
    results = cluster_manager.stop_cluster(cluster_id, db)
    return [ServiceActionResponse(**r) for r in results]


@router.get("/{cluster_id}/status")
def get_cluster_status(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return cluster_manager.get_cluster_status(cluster_id, db)


# ── One-click deploy (APB v1.4.0 #6) ─────────────────

class QuickDeployRequest(BaseModel):
    """No required fields — the endpoint picks sensible defaults from
    whatever hosts the operator has already registered."""
    name: str | None = None
    environment: str = "dev"


@router.post("/quick-deploy")
def quick_deploy(
    req: QuickDeployRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db), _: User = Depends(require_admin),
):
    """Create + deploy a cluster with sensible defaults in one call.

    Picks the latest Kafka version available locally, KRaft mode, and
    assigns all registered hosts as `broker_controller` (combined mode).
    Replication factor scales with broker count (max 3). Ideal for QA
    smoke environments and demos — the customer asked for "without
    user" deployment.
    """
    hosts = db.query(Host).all()
    if not hosts:
        raise HTTPException(status_code=400, detail="Register at least one host before quick-deploy")

    # Pick name
    name = (req.name or "").strip()
    if not name:
        existing = {c.name for c in db.query(Cluster.name).all()}
        i = 1
        while f"cluster-{i}" in existing:
            i += 1
        name = f"cluster-{i}"
    elif db.query(Cluster).filter(Cluster.name == name).first():
        raise HTTPException(status_code=400, detail=f"Cluster '{name}' already exists")

    # Kafka version: latest available locally; falls back to a sensible default
    from app.config import settings as _settings
    from pathlib import Path as _P
    repo = _P(_settings.KAFKA_REPO_DIR)
    versions = []
    if repo.exists():
        for f in repo.glob("kafka_*-*.tgz"):
            try:
                # filename like kafka_2.13-4.1.0.tgz
                ver = f.stem.split("-", 1)[1]
                versions.append(ver)
            except IndexError:
                continue
    if versions:
        # Pick the highest version semantically
        def _v(s: str) -> tuple:
            try:
                return tuple(int(x) for x in s.split("."))
            except ValueError:
                return (0,)
        versions.sort(key=_v, reverse=True)
        kafka_version = versions[0]
    else:
        kafka_version = "4.1.0"  # baked-in fallback

    # Pick replication factor — never exceed broker count
    rf = min(3, len(hosts))

    # Assign each host as combined broker_controller
    from app.schemas.cluster import ServiceAssignment as _SA
    services = []
    for i, h in enumerate(hosts, start=1):
        services.append(_SA(host_id=h.id, role="broker_controller", node_id=i))

    # APB v1.4.2 — auto-pick a port set that doesn't collide with any
    # existing cluster on the same host(s). The customer hit this when
    # quick-deploying twice on the same machine: second cluster failed
    # to bind 9093. Now we walk forward by 100 (9092→9192→9292…) until
    # we find a free set.
    from app.services import port_preflight
    occupied: dict[str, set[int]] = {}
    for c in db.query(Cluster).all():
        try:
            cfg = json.loads(c.config_json or "{}")
        except Exception:
            continue
        ports = {int(cfg.get(k) or 0) for k in
                 ("listener_port", "controller_port", "ssl_listener_port",
                  "schema_registry_port", "ksqldb_port", "connect_rest_port")}
        ports.discard(0)
        # Walk this cluster's services to find their host ids
        for svc in db.query(Service).filter(Service.cluster_id == c.id).all():
            occupied.setdefault(svc.host_id, set()).update(ports)
    free = port_preflight.find_free_port_set(occupied, [h.id for h in hosts])

    cluster_config = {
        "replication_factor": rf,
        "num_partitions": 3,
        "log_dirs": "/var/lib/kafka/data",
        "listener_port": free["listener_port"],
        "controller_port": free["controller_port"],
        "ssl_listener_port": free["ssl_listener_port"],
        "heap_size": "1G",
    }

    cluster = Cluster(
        name=name,
        kafka_version=kafka_version,
        mode="kraft",
        config_json=json.dumps(cluster_config),
        environment=(req.environment or "dev").strip().lower(),
    )
    db.add(cluster)
    db.flush()

    from app.services import cluster_paths
    cluster_paths.assign_paths_for_new_cluster(cluster)

    for svc_data in services:
        svc = Service(
            cluster_id=cluster.id,
            host_id=svc_data.host_id,
            role=svc_data.role,
            node_id=svc_data.node_id,
        )
        db.add(svc)
    db.commit()

    # Kick off the deploy in the background — caller polls
    # /api/clusters/{id}/deploy/{task_id} the same way the wizard does.
    task_id = str(uuid.uuid4())
    init_task(task_id, cluster.id)
    background_tasks.add_task(deploy_cluster, cluster.id, task_id)

    return {
        "cluster_id": cluster.id,
        "name": cluster.name,
        "task_id": task_id,
        "kafka_version": kafka_version,
        "broker_count": len(services),
        "ports": {
            "listener": free["listener_port"],
            "controller": free["controller_port"],
            "ssl_listener": free["ssl_listener_port"],
        },
    }


# ── Pre-flight port checker for the create-cluster wizard ──

class PreflightPortsRequest(BaseModel):
    host_ids: list[str]
    ports: list[int]  # the wizard sends listener+controller+SR+ksqlDB+connect_port etc.


@router.post("/preflight-ports")
def preflight_ports(
    req: PreflightPortsRequest,
    db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above),
):
    """SSH to each host, check whether any of `ports` is already bound.

    Used by the cluster create wizard so operators can hit a "Check
    ports" button BEFORE submit. Returns one row per conflict with the
    process holding the port; empty list = all clear.
    """
    from app.services import port_preflight
    hosts = {h.id: h for h in db.query(Host).filter(Host.id.in_(req.host_ids)).all()}
    checks = []
    for hid in req.host_ids:
        for p in req.ports:
            checks.append(port_preflight.PortCheck(hid, "", int(p), f"port {p}"))
    conflicts = port_preflight.check_ports(checks, hosts)
    return {
        "ok": not any(not c.label.startswith("ssh-precheck-failed") for c in conflicts),
        "conflicts": [
            {"host_ip": c.host_ip, "port": c.port, "label": c.label, "process": c.process}
            for c in conflicts
            if not c.label.startswith("ssh-precheck-failed")
        ],
        "ssh_failures": [
            {"host_ip": c.host_ip, "error": c.process}
            for c in conflicts
            if c.label.startswith("ssh-precheck-failed")
        ],
        "defaults": port_preflight.DEFAULT_PORTS,
    }


# ── Schema Registry deploy (APB v1.4.0 #2) ──────────

class SchemaRegistryDeployRequest(BaseModel):
    host_id: str
    port: int = 8085


@router.post("/{cluster_id}/services/schema-registry")
def deploy_schema_registry_endpoint(
    cluster_id: str, req: SchemaRegistryDeployRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db), _: User = Depends(require_admin),
):
    """Deploy a Schema Registry instance bound to this cluster.

    The customer asked for SR to be deployable per-cluster from the
    cluster detail page rather than globally from a sidebar entry.
    Returns a deploy task_id the UI can poll the same way it polls the
    main cluster deploy.
    """
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    if (cluster.kind or "managed") == "external":
        raise HTTPException(status_code=400, detail="Schema Registry deploy is only supported on managed clusters")
    host = db.query(Host).filter(Host.id == req.host_id).first()
    if not host:
        raise HTTPException(status_code=400, detail="Host not found")

    task_id = str(uuid.uuid4())
    init_task(task_id, cluster_id)
    background_tasks.add_task(deploy_schema_registry, cluster_id, req.host_id, req.port, task_id)
    return DeploymentTaskResponse(task_id=task_id, cluster_id=cluster_id, status="running")


# ── Node scaling (add/remove hosts) ──────────────────

@router.post("/{cluster_id}/services", response_model=list[ServiceResponse])
def add_services(cluster_id: str, services: list[ServiceAssignment], db: Session = Depends(get_db), _: User = Depends(require_admin)):
    """Add new service(s) to an existing cluster. Used for scaling out."""
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")

    # Validate hosts exist
    host_ids = {s.host_id for s in services}
    existing = db.query(Host.id).filter(Host.id.in_(host_ids)).all()
    existing_ids = {h.id for h in existing}
    missing = host_ids - existing_ids
    if missing:
        raise HTTPException(status_code=400, detail=f"Hosts not found: {', '.join(missing)}")

    created = []
    for svc_data in services:
        svc = Service(
            cluster_id=cluster.id,
            host_id=svc_data.host_id,
            role=svc_data.role,
            node_id=svc_data.node_id,
        )
        db.add(svc)
        db.flush()
        created.append(svc)

    db.commit()
    return [ServiceResponse.model_validate(s) for s in created]


@router.delete("/{cluster_id}/services/{service_id}")
def remove_service(cluster_id: str, service_id: str, force: bool = False, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    """Remove a service from the cluster with dependency checks.

    Prevents removing the last broker, last controller, or a broker
    that has under-replicated topics unless force=True.
    """
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")

    target = db.query(Service).filter(Service.id == service_id, Service.cluster_id == cluster_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Service not found in cluster")

    all_services = db.query(Service).filter(Service.cluster_id == cluster_id).all()

    # Dependency checks (skip if force=True)
    if not force:
        # Count services by role (excluding the one being removed)
        remaining = [s for s in all_services if s.id != service_id]
        broker_roles = {"broker", "broker_controller"}
        controller_roles = {"controller", "broker_controller"}

        remaining_brokers = [s for s in remaining if s.role in broker_roles]
        remaining_controllers = [s for s in remaining if s.role in controller_roles]

        if target.role in broker_roles and len(remaining_brokers) == 0:
            raise HTTPException(
                status_code=409,
                detail="Cannot remove the last broker. This would leave the cluster with no brokers. Use force=true to override.",
            )

        if target.role in controller_roles and len(remaining_controllers) == 0:
            raise HTTPException(
                status_code=409,
                detail="Cannot remove the last controller. This would leave the cluster with no controllers. Use force=true to override.",
            )

        # Check replication: if cluster config has replication_factor > remaining brokers
        config = json.loads(cluster.config_json) if cluster.config_json else {}
        rf = config.get("replication_factor", 1)
        if target.role in broker_roles and len(remaining_brokers) < rf:
            raise HTTPException(
                status_code=409,
                detail=f"Removing this broker would leave {len(remaining_brokers)} broker(s), "
                       f"but replication_factor is {rf}. Topics would be under-replicated. Use force=true to override.",
            )

        # Check if ksqldb/connect depends on brokers
        has_ksqldb = any(s.role == "ksqldb" for s in remaining)
        has_connect = any(s.role == "kafka_connect" for s in remaining)
        if target.role in broker_roles and len(remaining_brokers) == 0 and (has_ksqldb or has_connect):
            raise HTTPException(
                status_code=409,
                detail="Cannot remove the last broker: ksqlDB/Connect services depend on it. Use force=true to override.",
            )

    # If the service was running, try to stop it first
    if target.status == "running":
        host = db.query(Host).filter(Host.id == target.host_id).first()
        if host:
            try:
                from app.services.cluster_manager import ClusterManager
                unit_name = ClusterManager._get_systemd_name(target.role)
                from app.services.ssh_manager import SSHManager
                with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                    SSHManager.exec_command(client, f"sudo systemctl stop {unit_name}")
                    SSHManager.exec_command(client, f"sudo systemctl disable {unit_name}")
                    SSHManager.exec_command(client, f"sudo rm -f /etc/systemd/system/{unit_name}")
                    SSHManager.exec_command(client, "sudo systemctl daemon-reload")
            except Exception:
                pass  # Best effort cleanup

    db.delete(target)
    db.commit()

    return {
        "detail": f"Service {service_id} ({target.role}) removed from cluster",
        "service_id": service_id,
        "role": target.role,
    }
