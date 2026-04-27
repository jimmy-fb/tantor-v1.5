import json
import uuid

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.cluster import Cluster
from app.models.service import Service
from app.models.host import Host
from app.schemas.cluster import (
    ClusterCreate, ClusterResponse, ClusterDetailResponse,
    ServiceResponse, DeploymentTaskResponse, ServiceAssignment,
)
from app.schemas.service import ServiceActionResponse
from app.services.deployer import deploy_cluster, get_task, init_task
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
    )
    db.add(cluster)
    db.flush()

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


@router.post("/{cluster_id}/deploy", response_model=DeploymentTaskResponse)
def start_deployment(cluster_id: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
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
    results = cluster_manager.start_cluster(cluster_id, db)
    return [ServiceActionResponse(**r) for r in results]


@router.post("/{cluster_id}/stop", response_model=list[ServiceActionResponse])
def stop_cluster(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    results = cluster_manager.stop_cluster(cluster_id, db)
    return [ServiceActionResponse(**r) for r in results]


@router.get("/{cluster_id}/status")
def get_cluster_status(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return cluster_manager.get_cluster_status(cluster_id, db)


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
