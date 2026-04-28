import base64
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.models.cluster import Cluster
from app.models.deployment_task import DeploymentTask
from app.models.host import Host
from app.models.service import Service
from app.services.ansible_runner import ansible_runner
from app.services.config_generator import config_generator
from app.services.crypto import decrypt

logger = logging.getLogger("tantor.deployer")


# Hard cap on retained log lines per task — keeps the SQLite row bounded even
# for noisy multi-hour Ansible runs.
_MAX_LOG_LINES = 5000


# ── Task store (DB-backed) ────────────────────────────────────────────────


def get_task(task_id: str, db: Session | None = None) -> dict | None:
    """Return the task as a dict matching the legacy in-memory shape.

    The optional `db` arg lets callers reuse their request session; otherwise
    we open and close a short-lived one so this is safe to call from anywhere.
    """
    own_session = db is None
    db = db or SessionLocal()
    try:
        row = db.query(DeploymentTask).filter(DeploymentTask.id == task_id).first()
        if not row:
            return None
        return _row_to_dict(row)
    finally:
        if own_session:
            db.close()


def init_task(task_id: str, cluster_id: str) -> None:
    """Insert a fresh row marking a deploy as running."""
    db = SessionLocal()
    try:
        row = DeploymentTask(id=task_id, cluster_id=cluster_id, status="running", logs="[]")
        db.add(row)
        db.commit()
    finally:
        db.close()


def _row_to_dict(row: DeploymentTask) -> dict:
    try:
        logs = json.loads(row.logs or "[]")
    except (TypeError, ValueError):
        logs = []
    return {
        "task_id": row.id,
        "cluster_id": row.cluster_id,
        "status": row.status,
        "current_step": row.current_step,
        "logs": logs,
        "error_message": row.error_message,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _append_log(db: Session, task_id: str, message: str) -> None:
    """Append one log line to a task's persisted log list, with a hard cap."""
    row = db.query(DeploymentTask).filter(DeploymentTask.id == task_id).first()
    if not row:
        return
    try:
        logs = json.loads(row.logs or "[]")
    except (TypeError, ValueError):
        logs = []
    logs.append(message)
    if len(logs) > _MAX_LOG_LINES:
        # Keep the head (so the operator sees how the deploy started) and the
        # tail (so they see what's happening now). Drop the middle.
        head = logs[:200]
        tail = logs[-(_MAX_LOG_LINES - 200 - 1):]
        logs = head + ["... [truncated due to log size limit] ..."] + tail
    row.logs = json.dumps(logs)
    db.commit()


def _set_status(db: Session, task_id: str, status: str, error_message: str | None = None) -> None:
    row = db.query(DeploymentTask).filter(DeploymentTask.id == task_id).first()
    if not row:
        return
    row.status = status
    if error_message is not None:
        row.error_message = error_message
    if status != "running":
        row.finished_at = datetime.now(timezone.utc)
    db.commit()


def _set_step(db: Session, task_id: str, step: str) -> None:
    row = db.query(DeploymentTask).filter(DeploymentTask.id == task_id).first()
    if row:
        row.current_step = step
        db.commit()


def _build_service_info(svc: Service, host: Host, cluster_config: dict) -> dict:
    return {
        "id": svc.id,
        "host_id": svc.host_id,
        "ip_address": host.ip_address,
        "port": host.ssh_port,
        "username": host.username,
        "auth_type": host.auth_type,
        "encrypted_credential": host.encrypted_credential,
        "role": svc.role,
        "node_id": svc.node_id,
        "controller_port": cluster_config.get("controller_port", 9093),
    }


# ── Background entrypoint ─────────────────────────────────────────────────


def deploy_cluster(cluster_id: str, task_id: str) -> None:
    """Background task entrypoint.

    Owns its own DB session — must NOT receive the request's session (which
    closes the moment the HTTP response is sent). Always logs into the
    persisted `deployment_tasks` row so the API can report progress even if
    the worker is on a different process.
    """
    db = SessionLocal()
    try:
        _deploy_cluster_inner(cluster_id, task_id, db)
    except Exception as e:
        # Last-ditch catch — anything that escapes the inner function still
        # has to mark the row as errored, otherwise the UI hangs on "running".
        logger.exception("Deployment failed for cluster %s", cluster_id)
        try:
            _append_log(db, task_id, f"FATAL ERROR: {e}")
            _set_status(db, task_id, "error", error_message=str(e))
            cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
            if cluster:
                cluster.state = "error"
                db.commit()
        except Exception:  # nosec: best-effort cleanup
            pass
    finally:
        db.close()


def _deploy_cluster_inner(cluster_id: str, task_id: str, db: Session) -> None:
    def log(msg: str) -> None:
        _append_log(db, task_id, msg)
        if msg.strip():
            logger.info("[%s] %s", task_id[:8], msg)

    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        log("ERROR: Cluster not found")
        _set_status(db, task_id, "error", error_message="Cluster not found")
        return

    services = db.query(Service).filter(Service.cluster_id == cluster_id).all()
    hosts = {h.id: h for h in db.query(Host).all()}
    cluster_config = json.loads(cluster.config_json) if cluster.config_json else {}

    # ── Pre-flight validation ──────────────────────────────────────
    _set_step(db, task_id, "Pre-flight checks")
    log("Running pre-flight checks...")

    # Check 1: Node IDs must be unique within the cluster
    node_ids = [svc.node_id for svc in services]
    if len(node_ids) != len(set(node_ids)):
        log(f"ERROR: Duplicate node_ids detected: {node_ids}")
        _set_status(db, task_id, "error", error_message=f"Duplicate node_ids: {node_ids}")
        cluster.state = "error"
        db.commit()
        return

    # Check 2: All hosts must exist and be reachable
    offline_hosts = []
    for svc in services:
        host = hosts.get(svc.host_id)
        if not host:
            log(f"ERROR: Host {svc.host_id} not found for service {svc.role}")
            _set_status(db, task_id, "error", error_message=f"Host {svc.host_id} not found")
            cluster.state = "error"
            db.commit()
            return
        if host.status != "online":
            offline_hosts.append(f"{host.hostname} ({host.ip_address})")
            log(f"WARNING: Host {host.hostname} ({host.ip_address}) status is '{host.status}', not 'online'")

    if offline_hosts:
        log(f"WARNING: {len(offline_hosts)} host(s) are offline: {', '.join(offline_hosts)}. Deployment may fail.")

    # Check 3: KRaft mode needs at least one controller
    if cluster.mode == "kraft":
        controller_roles = {svc.role for svc in services} & {"controller", "broker_controller"}
        if not controller_roles:
            log("ERROR: KRaft mode requires at least one controller or broker_controller role")
            _set_status(db, task_id, "error", error_message="KRaft mode requires a controller role")
            cluster.state = "error"
            db.commit()
            return

    # Check 4: Replication factor can't exceed broker count
    broker_count = sum(1 for svc in services if svc.role in ("broker", "broker_controller"))
    rf = cluster_config.get("replication_factor", 3)
    if rf > broker_count:
        log(f"WARNING: replication_factor={rf} exceeds broker count={broker_count}. Adjusting to {broker_count}.")
        cluster_config["replication_factor"] = broker_count

    log(f"Pre-flight checks passed: {len(services)} services, {broker_count} brokers, RF={cluster_config.get('replication_factor', 1)}")

    all_service_infos = []
    for svc in services:
        host = hosts.get(svc.host_id)
        if host:
            all_service_infos.append(_build_service_info(svc, host, cluster_config))

    cluster.state = "deploying"
    db.commit()

    _run_ansible_deployment(task_id, cluster, services, hosts, all_service_infos, cluster_config, db, log)


def _run_ansible_deployment(
    task_id: str,
    cluster: Cluster,
    services: list[Service],
    hosts: dict[str, Host],
    all_service_infos: list[dict],
    cluster_config: dict,
    db: Session,
    log,
):
    kafka_version = cluster.kafka_version
    scala_version = settings.KAFKA_SCALA_VERSION
    kafka_tgz = f"kafka_{scala_version}-{kafka_version}.tgz"
    kafka_tgz_path = Path(settings.KAFKA_REPO_DIR) / kafka_tgz

    # Check binary exists in airgapped repo; auto-download if missing
    if not kafka_tgz_path.exists():
        _set_step(db, task_id, "Download Kafka binary")
        log(f"Kafka binary not found locally. Downloading {kafka_tgz}...")
        kafka_tgz_path.parent.mkdir(parents=True, exist_ok=True)
        download_url = f"https://downloads.apache.org/kafka/{kafka_version}/{kafka_tgz}"
        archive_url = f"https://archive.apache.org/dist/kafka/{kafka_version}/{kafka_tgz}"
        # Try primary, then archive
        import urllib.request
        try:
            urllib.request.urlretrieve(download_url, str(kafka_tgz_path))
        except Exception:
            log("Primary mirror failed, trying archive...")
            try:
                urllib.request.urlretrieve(archive_url, str(kafka_tgz_path))
            except Exception as dl_err:
                log(f"ERROR: Failed to download Kafka binary: {dl_err}")
                log(f"Place {kafka_tgz} in {settings.KAFKA_REPO_DIR}/ or upload via the Kafka Versions page")
                _set_status(db, task_id, "error", error_message=f"Kafka binary download failed: {dl_err}")
                cluster.state = "error"
                db.commit()
                return
        log(f"Downloaded {kafka_tgz} ({kafka_tgz_path.stat().st_size // (1024*1024)} MB)")

    log(f"Using local Kafka binary: {kafka_tgz} ({kafka_tgz_path.stat().st_size // (1024*1024)} MB)")

    # Generate or reuse KRaft cluster UUID
    cluster_uuid = ""
    if cluster.mode == "kraft":
        if cluster.cluster_uuid:
            cluster_uuid = cluster.cluster_uuid
            log(f"Reusing KRaft cluster ID: {cluster_uuid}")
        else:
            cluster_uuid = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")
            cluster.cluster_uuid = cluster_uuid
            db.commit()
            log(f"Generated KRaft cluster ID: {cluster_uuid}")

    # Prepare Ansible workspace
    _set_step(db, task_id, "Generate Ansible workspace")
    work_dir = ansible_runner.prepare_workspace(task_id)
    log("Prepared Ansible workspace")

    # Build service dicts with decrypted credentials
    svc_dicts = []
    for svc in services:
        host = hosts.get(svc.host_id)
        if not host:
            continue
        svc_dicts.append({
            "ip_address": host.ip_address,
            "port": host.ssh_port,
            "username": host.username,
            "auth_type": host.auth_type,
            "credential": decrypt(host.encrypted_credential),
            "role": svc.role,
            "node_id": svc.node_id,
        })

    # Generate Ansible inventory
    inv_path = ansible_runner.generate_inventory(work_dir, svc_dicts)
    ansible_runner.generate_ansible_cfg(work_dir)
    log("Generated Ansible inventory and config")

    # Pre-render Kafka configs using existing ConfigGenerator
    log("Generating Kafka configurations...")
    configs = {}
    systemd_units = {}

    for svc_info in all_service_infos:
        host = hosts.get(svc_info["host_id"])
        if not host:
            continue

        ip = host.ip_address
        nid = svc_info["node_id"]
        role = svc_info["role"]

        config_content = config_generator.generate_config_for_service(
            svc_info, all_service_infos, cluster_config
        )

        if role in ("broker", "broker_controller", "controller"):
            config_name = f"{ip}_{nid}_server.properties"
            remote_config = f"{settings.KAFKA_INSTALL_DIR}/config/server.properties"
        elif role == "ksqldb":
            config_name = f"{ip}_{nid}_ksql-server.properties"
            remote_config = f"{settings.KSQLDB_INSTALL_DIR}/config/ksql-server.properties"
        elif role == "kafka_connect":
            config_name = f"{ip}_{nid}_connect-distributed.properties"
            remote_config = f"{settings.KAFKA_INSTALL_DIR}/config/connect-distributed.properties"
        elif role == "zookeeper":
            config_name = f"{ip}_{nid}_zookeeper.properties"
            remote_config = f"{settings.KAFKA_INSTALL_DIR}/config/zookeeper.properties"
        elif role == "schema_registry":
            config_name = f"{ip}_{nid}_apicurio.properties"
            remote_config = f"{settings.APICURIO_INSTALL_DIR}/application.properties"
        else:
            config_name = f"{ip}_{nid}_server.properties"
            remote_config = f"{settings.KAFKA_INSTALL_DIR}/config/server.properties"

        configs[config_name] = config_content

        # Generate systemd unit
        service_type_map = {
            "broker": "kafka", "broker_controller": "kafka",
            "controller": "kafka-kraft-controller",
            "ksqldb": "ksqldb", "kafka_connect": "kafka-connect",
            "zookeeper": "kafka",
            "schema_registry": "schema-registry",
        }
        service_type = service_type_map.get(role, "kafka")
        unit_content = config_generator.generate_systemd_unit(
            service_type, remote_config, settings.KAFKA_INSTALL_DIR, settings.KSQLDB_INSTALL_DIR
        )
        unit_name = f"{ip}_{nid}_{service_type}.service"
        systemd_units[unit_name] = unit_content

    configs_dir = ansible_runner.write_config_files(work_dir, configs)
    systemd_dir = ansible_runner.write_systemd_units(work_dir, systemd_units)
    log(f"Generated {len(configs)} configs and {len(systemd_units)} systemd units")

    # Determine which role groups exist
    roles_present = {svc.role for svc in services}
    has_brokers = bool(roles_present & {"broker", "broker_controller"})
    has_controllers = "controller" in roles_present
    has_ksqldb = "ksqldb" in roles_present
    has_connect = "kafka_connect" in roles_present
    has_schema_registry = "schema_registry" in roles_present

    # Resolve ksqlDB binary if needed
    ksqldb_tgz = ""
    ksqldb_tgz_path = ""
    if has_ksqldb:
        ksqldb_repo = Path(settings.KSQLDB_REPO_DIR)
        ksqldb_files = sorted(ksqldb_repo.glob("ksqldb-*.tgz")) if ksqldb_repo.exists() else []
        if ksqldb_files:
            ksqldb_tgz_path = str(ksqldb_files[-1])  # latest version
            ksqldb_tgz = ksqldb_files[-1].name
            log(f"Using ksqlDB binary: {ksqldb_tgz} ({ksqldb_files[-1].stat().st_size // (1024*1024)} MB)")
        else:
            log("WARNING: No ksqlDB binary found in repo. ksqlDB deployment may fail.")

    # Resolve Apicurio Registry binary if needed
    apicurio_tgz = ""
    apicurio_tgz_path = ""
    # Apicurio 3.x's `-all.tar.gz` extracts directly to `quarkus-app/` — no
    # top-level versioned directory, so we don't strip any components.
    apicurio_strip_components = 0
    if has_schema_registry:
        apicurio_repo = Path(settings.APICURIO_REPO_DIR)
        apicurio_repo.mkdir(parents=True, exist_ok=True)
        # Accept any Apicurio app tarball already in the repo (operator may
        # have dropped a different version for air-gapped installs).
        existing = sorted(apicurio_repo.glob("apicurio-registry-app-*-all.tar.gz"))
        if not existing:
            apicurio_tgz = f"apicurio-registry-app-{settings.APICURIO_VERSION}-all.tar.gz"
            apicurio_tgz_path = str(apicurio_repo / apicurio_tgz)
            url = f"https://github.com/Apicurio/apicurio-registry/releases/download/{settings.APICURIO_VERSION}/{apicurio_tgz}"
            log(f"Apicurio binary not found locally. Downloading {apicurio_tgz}...")
            import urllib.request
            try:
                urllib.request.urlretrieve(url, apicurio_tgz_path)
                log(f"Downloaded {apicurio_tgz} ({Path(apicurio_tgz_path).stat().st_size // (1024*1024)} MB)")
            except Exception as dl_err:
                log(f"ERROR: Failed to download Apicurio: {dl_err}")
                log(f"Place an Apicurio app tarball at {apicurio_repo}/ and retry.")
                _set_status(db, task_id, "error", error_message=f"Apicurio download failed: {dl_err}")
                cluster.state = "error"
                db.commit()
                return
        else:
            apicurio_tgz_path = str(existing[-1])
            apicurio_tgz = existing[-1].name
            log(f"Using Apicurio binary: {apicurio_tgz} ({existing[-1].stat().st_size // (1024*1024)} MB)")

    # Generate playbook
    playbook_path = ansible_runner.generate_playbook(work_dir, "deploy_kafka.yml.j2", {
        "kafka_install_dir": settings.KAFKA_INSTALL_DIR,
        "kafka_binary_filename": kafka_tgz,
        "kafka_binary_local_path": str(kafka_tgz_path),
        "kafka_data_dir": cluster_config.get("log_dirs", settings.KAFKA_DATA_DIR),
        "kafka_log_dir": settings.KAFKA_LOG_DIR,
        "cluster_uuid": cluster_uuid,
        "cluster_mode": cluster.mode,
        "cluster_config": cluster_config,
        "configs_dir": str(configs_dir),
        "systemd_dir": str(systemd_dir),
        "has_brokers": has_brokers,
        "has_controllers": has_controllers,
        "has_ksqldb": has_ksqldb,
        "has_connect": has_connect,
        "has_schema_registry": has_schema_registry,
        "apicurio_install_dir": settings.APICURIO_INSTALL_DIR,
        "apicurio_binary_filename": apicurio_tgz,
        "apicurio_binary_local_path": apicurio_tgz_path,
        "apicurio_strip_components": apicurio_strip_components,
        "ksqldb_install_dir": settings.KSQLDB_INSTALL_DIR,
        "ksqldb_binary_filename": ksqldb_tgz,
        "ksqldb_binary_local_path": ksqldb_tgz_path,
    })
    log("Generated Ansible playbook")

    # Run playbook with real-time streaming
    _set_step(db, task_id, "Running Ansible playbook")
    log("")
    log("=" * 60)
    log("Starting Ansible playbook execution...")
    log("=" * 60)
    log("")

    exit_code = ansible_runner.run_playbook(
        work_dir, playbook_path, inv_path,
        log_callback=lambda line: log(line),
    )

    # Update statuses
    if exit_code == 0:
        for svc in services:
            svc.status = "running"
        cluster.state = "running"
        db.commit()
        _set_status(db, task_id, "completed")
        _set_step(db, task_id, "Completed")
        log("")
        log("Deployment completed successfully!")
    else:
        cluster.state = "error"
        db.commit()
        _set_status(db, task_id, "completed_with_errors", error_message=f"ansible exit code: {exit_code}")
        _set_step(db, task_id, "Failed")
        log("")
        log(f"Deployment failed (ansible exit code: {exit_code})")

    # Cleanup
    ansible_runner.cleanup_workspace(work_dir)
    log("Workspace cleaned up")
