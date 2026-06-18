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
from app.services import cert_manager, cluster_paths
from app.services.ansible_runner import ansible_runner
from app.services.config_generator import config_generator
from app.services.crypto import decrypt
from app.services.ssh_manager import SSHManager

logger = logging.getLogger("tantor.deployer")


# Hard cap on retained log lines per task — keeps the SQLite row bounded even
# for noisy multi-hour Ansible runs.
_MAX_LOG_LINES = 5000


def _append_to_log(task_id: str, message: str) -> None:
    """Append a single log line using a short-lived DB session. Used by the
    transport-selection logic before either deployer takes ownership of the
    task row."""
    db = SessionLocal()
    try:
        _append_log(db, task_id, message)
    finally:
        db.close()


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

    Picks an agent-based deploy when:
      * `cluster.config_json.deploy_via == "agent"` (operator opted in), OR
      * the agent path supports this cluster AND every target host has a
        connected agent — auto-pick.

    Falls back to the legacy Ansible+SSH deployer in every other case.
    """
    # First, decide which deployer runs. The decision must be made in a
    # short-lived session so the SSH path (which opens its own session
    # later) doesn't see the same row.
    decision_db = SessionLocal()
    use_agent = False
    agent_reason = "auto-pick declined"
    try:
        cluster = decision_db.query(Cluster).filter(Cluster.id == cluster_id).first()
        services = decision_db.query(Service).filter(Service.cluster_id == cluster_id).all() if cluster else []
        hosts_by_id = {h.id: h for h in decision_db.query(Host).all()}
        if cluster:
            from app.services import agent_deployer
            cfg = json.loads(cluster.config_json) if cluster.config_json else {}
            explicit = (cfg.get("deploy_via") or "").lower()  # "agent" | "ssh" | ""
            if explicit == "ssh":
                use_agent = False
                agent_reason = "operator opted out (deploy_via=ssh)"
            elif explicit == "agent":
                ok, reason = agent_deployer.supports_agent_deploy(cluster, services, hosts_by_id)
                if ok:
                    use_agent = True
                    agent_reason = "operator opted in"
                else:
                    use_agent = False
                    agent_reason = f"operator opted in but prereqs not met: {reason}"
            else:
                ok, reason = agent_deployer.supports_agent_deploy(cluster, services, hosts_by_id)
                use_agent = ok
                agent_reason = "auto-pick: " + reason
    finally:
        decision_db.close()

    if use_agent:
        from app.services import agent_deployer
        # Resolve SCM base URL + tarball filename. For now we hardcode the
        # tarball name to match the SSH deployer's convention; the SCM serves
        # it from /repo/kafka/. SCM URL needs to be reachable BY the broker —
        # operators set TANTOR_SCM_PUBLIC_URL or we fall back to the bind URL.
        import os
        from app.services import cert_manager  # noqa: F401 — keep import order
        scm_base = os.environ.get("TANTOR_SCM_PUBLIC_URL", "http://127.0.0.1:8000")
        # Pick the version-specific tarball that already lives under repo/kafka.
        decision_db = SessionLocal()
        try:
            cluster = decision_db.query(Cluster).filter(Cluster.id == cluster_id).first()
            tarball = f"kafka_2.13-{cluster.kafka_version}.tgz" if cluster and cluster.kafka_version else "kafka_2.13-3.7.0.tgz"
        finally:
            decision_db.close()
        _append_to_log(task_id, f"[deployer] using agent-based deploy (reason: {agent_reason})")
        agent_deployer.deploy_cluster(cluster_id, task_id, scm_base, tarball)
        return

    _append_to_log(task_id, f"[deployer] using SSH/Ansible deploy ({agent_reason})")

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

    # Check 5b: Java pre-flight on every host (v1.4.3 #17). The
    # ansible playbook installs Java via apt/dnf, but if those package
    # repos aren't reachable (corporate proxy, RHEL not subscribed, etc.)
    # the install fails mid-deploy leaving a half-deployed cluster. Run
    # a fast `java -version` probe + a `which dnf || which apt` check
    # per host so the operator sees the gap up front.
    missing_java_hosts: list[str] = []
    for host_id in {svc.host_id for svc in services}:
        host = hosts.get(host_id)
        if not host:
            continue
        try:
            with SSHManager.connect(
                host.ip_address, host.ssh_port, host.username,
                host.auth_type, host.encrypted_credential,
            ) as client:
                rc, _, _ = SSHManager.exec_command(client, "java -version 2>&1 | head -1", timeout=10)
                if rc == 0:
                    continue
                # No Java — verify we have a package manager that can install it.
                pm_cmd = "command -v dnf || command -v apt-get || command -v yum || echo NONE"
                rc2, pm_out, _ = SSHManager.exec_command(client, pm_cmd, timeout=10)
                pm = (pm_out or "").strip().splitlines()[0] if pm_out else "NONE"
                if pm == "NONE" or not pm:
                    missing_java_hosts.append(
                        f"{host.ip_address}: Java not installed and no apt/dnf available — install openjdk-17-jre-headless manually"
                    )
                else:
                    pkg_name = "java-17-openjdk-headless" if "dnf" in pm or "yum" in pm else "openjdk-17-jre-headless"
                    missing_java_hosts.append(
                        f"{host.ip_address}: Java not installed — Tantor's playbook will run `{pm} install -y {pkg_name}` (may fail if repos blocked)"
                    )
        except Exception as e:
            # SSH problems are surfaced by later checks — don't double-fail.
            log(f"  warning: java pre-check skipped for {host.ip_address}: {e}")
    if missing_java_hosts:
        msg = "Java pre-flight findings:\n  " + "\n  ".join(missing_java_hosts)
        log(msg)
        # Don't BLOCK on this — the playbook may still succeed if repos work.
        # But surface it so the operator knows what to expect.

    # Check 5: Port-conflict pre-flight (v1.4.2). Catches the
    # collision-with-an-existing-cluster failure mode before ansible
    # binds and crashes silently.
    from app.services import port_preflight
    pp_checks = port_preflight.cluster_port_checks(cluster, services, cluster_config)
    if pp_checks:
        # Stop this cluster's own systemd unit first so a redeploy can
        # reclaim its own ports without false-positive conflicts.
        from app.services import cluster_paths
        unit = cluster_paths.unit_name(cluster)
        kafka_role_hosts = {svc.host_id for svc in services if svc.role in ("broker", "broker_controller", "controller", "zookeeper")}
        for hid in kafka_role_hosts:
            host = hosts.get(hid)
            if not host:
                continue
            try:
                with SSHManager.connect(
                    host.ip_address, host.ssh_port, host.username,
                    host.auth_type, host.encrypted_credential,
                ) as client:
                    SSHManager.exec_command(client, f"sudo -n systemctl stop {unit} 2>/dev/null || true", timeout=20)
            except Exception:
                pass
        # Now check the actual ports.
        conflicts = port_preflight.check_ports(pp_checks, hosts)
        # Filter out the ssh-precheck-failed rows — they're not real conflicts.
        real = [c for c in conflicts if not c.label.startswith("ssh-precheck-failed")]
        if real:
            msg = (
                "Port conflict — refusing to deploy:\n  "
                + "\n  ".join(c.message() for c in real)
                + "\n\nFix: stop the conflicting process, or change the port in the cluster config "
                  "(listener_port / controller_port / ssl_listener_port) and retry."
            )
            log(f"ERROR: {msg}")
            _set_status(db, task_id, "error", error_message=msg)
            cluster.state = "error"
            db.commit()
            return

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

    # ── TLS keystores (only when ssl_enabled) ────────────────────────
    # Must run BEFORE generate_inventory so per-broker keystore paths land
    # in inventory.yml as Ansible host vars.
    tls_keystores: dict[str, dict] = {}
    if cluster.ssl_enabled:
        log(f"TLS enabled — minting CA + broker keystores for cluster {cluster.id}")
        tls_keystores = cert_manager.materialize_broker_keystores(cluster, db, all_service_infos)
        cluster_config["_ssl_keystore_password"] = cert_manager.get_tls_password(cluster) or ""
        cluster_config["ssl_enabled"] = True
        cluster_config["mtls_required"] = bool(cluster.mtls_required)
        log(f"Generated {len(tls_keystores)} broker keystore(s)")

    # Generate Ansible inventory (with TLS keystore paths if SSL is enabled)
    inv_path = ansible_runner.generate_inventory(
        work_dir, svc_dicts, tls_keystores=tls_keystores or None,
    )
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

        kafka_dir = cluster_paths.install_dir(cluster)
        if role in ("broker", "broker_controller", "controller"):
            config_name = f"{ip}_{nid}_server.properties"
            remote_config = f"{kafka_dir}/config/server.properties"
        elif role == "ksqldb":
            config_name = f"{ip}_{nid}_ksql-server.properties"
            remote_config = f"{settings.KSQLDB_INSTALL_DIR}/config/ksql-server.properties"
        elif role == "kafka_connect":
            config_name = f"{ip}_{nid}_connect-distributed.properties"
            remote_config = f"{kafka_dir}/config/connect-distributed.properties"
        elif role == "zookeeper":
            config_name = f"{ip}_{nid}_zookeeper.properties"
            remote_config = f"{kafka_dir}/config/zookeeper.properties"
        elif role == "schema_registry":
            config_name = f"{ip}_{nid}_apicurio.properties"
            remote_config = f"{settings.APICURIO_INSTALL_DIR}/application.properties"
        else:
            config_name = f"{ip}_{nid}_server.properties"
            remote_config = f"{kafka_dir}/config/server.properties"

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
            service_type, remote_config, kafka_dir, settings.KSQLDB_INSTALL_DIR,
            unit_name=cluster_paths.unit_name(cluster) if service_type == "kafka" else None,
            kafka_log_dir=settings.KAFKA_LOG_DIR,
            cpu_quota=cluster_config.get("cpu_quota"),
            memory_max=cluster_config.get("memory_max"),
            jvm_performance_opts=cluster_config.get("jvm_performance_opts"),
            jmx_port=cluster_config.get("jmx_port"),
            gc_logging_enabled=cluster_config.get("gc_logging_enabled", False),
        )
        unit_name = f"{ip}_{nid}_{service_type}.service"
        systemd_units[unit_name] = unit_content

    configs_dir = ansible_runner.write_config_files(work_dir, configs)
    systemd_dir = ansible_runner.write_systemd_units(work_dir, systemd_units)
    log4j2_path = ansible_runner.write_kafka_log4j2(work_dir, settings.KAFKA_LOG_DIR)
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

    # Generate playbook. Kafka paths are PER-CLUSTER (v1.2.0 #5) so two
    # managed clusters on the same host don't collide on /opt/kafka.
    playbook_path = ansible_runner.generate_playbook(work_dir, "deploy_kafka.yml.j2", {
        "kafka_install_dir": cluster_paths.install_dir(cluster),
        "kafka_unit_name": cluster_paths.unit_name(cluster),
        "kafka_binary_filename": kafka_tgz,
        "kafka_binary_local_path": str(kafka_tgz_path),
        "kafka_data_dir": cluster_paths.data_dir(cluster),
        "kafka_log_dir": settings.KAFKA_LOG_DIR,
        "cluster_uuid": cluster_uuid,
        "cluster_mode": cluster.mode,
        "cluster_config": cluster_config,
        "configs_dir": str(configs_dir),
        "systemd_dir": str(systemd_dir),
        "log4j2_local_path": str(log4j2_path),
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
        "ssl_enabled": bool(cluster.ssl_enabled),
        "tls_keystores": tls_keystores,
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

        # ── Apply initial ACLs (v1.4.6) ──────────────────────────────────
        # The operator may have specified ACLs in the wizard. We apply them
        # now, after Ansible exits 0, once we confirm the broker is actually
        # accepting TCP connections. kafka-acls.sh needs --bootstrap-server
        # to be reachable — the JVM typically takes 5-15 s after systemctl
        # start to bind the port, so we poll with a timeout instead of
        # hitting it immediately.
        try:
            from app.models.cluster import Cluster as _Cluster
            _cluster_fresh = db.query(_Cluster).filter(_Cluster.id == cluster.id).first()
            cfg_for_acl = json.loads(_cluster_fresh.config_json or "{}") if _cluster_fresh else {}
            acl_port = cfg_for_acl.get("listener_port", 9092)

            # Resolve the first broker host for the readiness check
            broker_svc = next(
                (s for s in services if s.role in ("broker", "broker_controller")), None
            )
            broker_host = hosts.get(broker_svc.host_id) if broker_svc else None

            # Parse initial_acls out of the task's stored payload.
            # The deployer doesn't receive ClusterCreate directly, so we
            # stash initial_acls in the DeploymentTask row's logs under a
            # sentinel line "INITIAL_ACLS:<json>" written by the API layer.
            # Retrieve and remove it so it doesn't clutter the log display.
            _acl_task_row = db.query(DeploymentTask).filter(DeploymentTask.id == task_id).first()
            initial_acls: list[dict] = []
            if _acl_task_row:
                try:
                    _logs = json.loads(_acl_task_row.logs or "[]")
                    _clean_logs = []
                    for _line in _logs:
                        if isinstance(_line, str) and _line.startswith("INITIAL_ACLS:"):
                            try:
                                initial_acls = json.loads(_line[len("INITIAL_ACLS:"):])
                            except Exception:
                                pass
                        else:
                            _clean_logs.append(_line)
                    _acl_task_row.logs = json.dumps(_clean_logs)
                    db.commit()
                except Exception:
                    pass

            if initial_acls and broker_host:
                import socket, time
                log(f"Waiting for broker at {broker_host.ip_address}:{acl_port} to accept connections...")
                _deadline = time.monotonic() + 60
                _ready = False
                while time.monotonic() < _deadline:
                    try:
                        with socket.create_connection((broker_host.ip_address, acl_port), timeout=3):
                            _ready = True
                            break
                    except OSError:
                        time.sleep(3)

                if not _ready:
                    log(f"WARNING: Broker did not become reachable within 60 s — skipping initial ACLs. Apply them manually from the Security tab.")
                else:
                    log(f"Broker is reachable. Applying {len(initial_acls)} initial ACL rule(s)...")
                    from app.services.kafka_admin import KafkaAdmin
                    applied = 0
                    skipped = 0
                    for _acl in initial_acls:
                        try:
                            KafkaAdmin.create_acl(cluster.id, _acl, db, actor=None)
                            principal = _acl.get("principal", "?")
                            resource = f"{_acl.get('resource_type','?')}:{_acl.get('resource_name','?')}"
                            ops = ",".join(_acl.get("operations") or [])
                            log(f"  ACL applied — {principal} → {resource} [{ops}]")
                            applied += 1
                        except Exception as _acl_err:
                            log(f"  WARNING: ACL apply failed ({_acl}): {_acl_err}")
                            skipped += 1
                    log(f"Initial ACLs: {applied} applied, {skipped} failed (see warnings above).")
        except Exception as _acl_outer_err:
            # Never let ACL application break the deployment success state.
            log(f"WARNING: Initial ACL phase encountered an unexpected error: {_acl_outer_err}")

        # Auto-deploy monitoring + alerting on the same host as the first broker.
        # Best-effort — if monitoring deploy fails the cluster is still usable;
        # operator can retry from the Monitoring page. We pre-seed the four
        # default rule templates so the alerting story works out of the box.
        try:
            from app.services.monitoring_deployer import MonitoringDeployer
            from app.services import alert_manager as _am
            mon_host_id = services[0].host_id if services else None
            if mon_host_id:
                log("Auto-deploying monitoring stack (Prometheus + Alertmanager + Grafana + JMX)...")
                mon_result = MonitoringDeployer.deploy_monitoring_stack(
                    cluster_id=cluster.id,
                    monitoring_host_id=mon_host_id,
                    grafana_port=settings.GRAFANA_PORT,
                    prometheus_port=settings.PROMETHEUS_PORT,
                    db=db,
                )
                ok_steps = sum(1 for s in mon_result.get("steps", []) if s.get("status") == "success")
                log(f"Monitoring deployed ({ok_steps}/{len(mon_result.get('steps', []))} steps green)")
                seeded = _am.seed_default_rules(cluster.id, db)
                if seeded:
                    log(f"Seeded {seeded} default alert rule(s)")
                    # Re-render Prometheus rules.yml + reload — the monitoring
                    # deploy above wrote an empty rules file because the seed
                    # had not run yet.
                    try:
                        from app.models.host import Host as _Host
                        mon_host = db.query(_Host).filter(_Host.id == mon_host_id).first()
                        if mon_host:
                            MonitoringDeployer._render_alerting_files(cluster.id, mon_host, db)
                            log("Prometheus rules reloaded with seeded alerts")
                    except Exception as render_e:
                        log(f"Rules render after seed failed (non-fatal): {render_e}")
        except Exception as mon_e:
            # Don't fail the cluster deploy on a monitoring hiccup.
            log(f"Monitoring auto-deploy failed (cluster is still up): {mon_e}")

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


def deploy_schema_registry(cluster_id: str, host_id: str, port: int, task_id: str) -> None:
    """Add an Apicurio Schema Registry to an already-deployed cluster (v1.4.0 #2).

    Runs in a background thread the same way deploy_cluster does. Uses
    deploy_schema_registry.yml.j2 — a compact playbook that ONLY touches
    the SR install dir + systemd unit, leaving brokers untouched.
    """
    import base64, uuid as _uuid
    from app.services.crypto import decrypt
    from app.models.service import Service as _Service

    db = SessionLocal()
    try:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            _set_status(db, task_id, "error", error_message="Cluster not found")
            return

        host = db.query(Host).filter(Host.id == host_id).first()
        if not host:
            _set_status(db, task_id, "error", error_message="Host not found")
            return

        def log(msg: str = ""):
            _append_log(db, task_id, msg)

        log(f"Adding Schema Registry to cluster '{cluster.name}' on {host.ip_address}:{port}")

        # v1.4.2 — port pre-flight. Stops any existing SR on this
        # host first (so a redeploy can reclaim its own port), then
        # checks that the SR port is actually free.
        try:
            with SSHManager.connect(
                host.ip_address, host.ssh_port, host.username,
                host.auth_type, host.encrypted_credential,
            ) as client:
                SSHManager.exec_command(client, "sudo -n systemctl stop schema-registry 2>/dev/null || true", timeout=20)
        except Exception:
            pass
        from app.services import port_preflight
        sr_conflicts = port_preflight.check_ports(
            [port_preflight.PortCheck(host_id, "", port, "schema_registry")],
            {host_id: host},
        )
        sr_real = [c for c in sr_conflicts if not c.label.startswith("ssh-precheck-failed")]
        if sr_real:
            msg = (
                "Port conflict — refusing to deploy Schema Registry:\n  "
                + "\n  ".join(c.message() for c in sr_real)
                + f"\n\nFix: stop the conflicting process, or pick a different port (current: {port})."
            )
            log(f"ERROR: {msg}")
            _set_status(db, task_id, "error", error_message=msg)
            return

        # Up-front config_json updates: ensure schema_registry_port is recorded
        try:
            cfg = json.loads(cluster.config_json or "{}")
        except Exception:
            cfg = {}
        cfg["schema_registry_port"] = port
        cluster.config_json = json.dumps(cfg)

        # Find an existing SR Service row or create one
        sr_svc = db.query(_Service).filter(
            _Service.cluster_id == cluster_id, _Service.role == "schema_registry",
        ).first()
        # node_id is unique within the cluster — pick the next free
        existing_ids = [s.node_id for s in db.query(_Service).filter(_Service.cluster_id == cluster_id).all()]
        next_id = (max(existing_ids) + 1) if existing_ids else 1001
        if not sr_svc:
            sr_svc = _Service(
                cluster_id=cluster_id, host_id=host_id, role="schema_registry",
                node_id=next_id, status="deploying",
            )
            db.add(sr_svc)
        else:
            sr_svc.host_id = host_id
            sr_svc.status = "deploying"
            # v1.4.1 — re-deploy idempotency: stop the old SR before
            # re-running the playbook so two java processes don't fight
            # over port 8085.
            try:
                from app.services.ssh_manager import SSHManager
                with SSHManager.connect(
                    host.ip_address, host.ssh_port, host.username,
                    host.auth_type, host.encrypted_credential,
                ) as client:
                    SSHManager.exec_command(client, "sudo -n systemctl stop schema-registry || true", timeout=20)
            except Exception as e:
                log(f"  warn: pre-deploy stop failed (continuing anyway): {e}")
        db.commit()

        # Get broker hosts for bootstrap
        brokers = db.query(_Service).filter(
            _Service.cluster_id == cluster_id,
            _Service.role.in_(["broker", "broker_controller"]),
        ).all()
        if not brokers:
            log("No brokers found in cluster — Schema Registry needs Kafka to be deployed first.")
            _set_status(db, task_id, "error", error_message="No brokers in cluster")
            sr_svc.status = "error"
            db.commit()
            return
        host_rows = {h.id: h for h in db.query(Host).all()}
        broker_infos = []
        for b in brokers:
            bh = host_rows.get(b.host_id)
            if bh:
                broker_infos.append({"ip_address": bh.ip_address, "node_id": b.node_id, "role": b.role})

        # Render config + systemd unit
        cluster_config = json.loads(cluster.config_json or "{}")
        sr_info = {"ip_address": host.ip_address, "node_id": sr_svc.node_id, "role": "schema_registry"}
        sr_config = config_generator.generate_schema_registry_config(sr_info, broker_infos, cluster_config)
        # Bootstrap = the cluster's broker host:port — Apicurio stores
        # schemas in this cluster's own Kafka.
        bs_str = ",".join(f"{b['ip_address']}:{cluster_config.get('listener_port', 9092)}" for b in broker_infos)
        sr_unit = config_generator.generate_systemd_unit(
            "schema-registry",
            f"{settings.APICURIO_INSTALL_DIR}/application.properties",
            cluster_paths.install_dir(cluster), settings.KSQLDB_INSTALL_DIR,
            bootstrap_servers=bs_str or "127.0.0.1:9092",
            schema_registry_port=port,
            cpu_quota=cluster_config.get("cpu_quota"),
            memory_max=cluster_config.get("memory_max"),
            jvm_performance_opts=cluster_config.get("jvm_performance_opts"),
            jmx_port=cluster_config.get("jmx_port"),
            gc_logging_enabled=cluster_config.get("gc_logging_enabled", False),
        )

        configs = {f"{host.ip_address}_{sr_svc.node_id}_apicurio.properties": sr_config}
        units = {f"{host.ip_address}_{sr_svc.node_id}_schema-registry.service": sr_unit}

        work_dir = ansible_runner.prepare_workspace(task_id)
        configs_dir = ansible_runner.write_config_files(work_dir, configs)
        systemd_dir = ansible_runner.write_systemd_units(work_dir, units)

        # Apicurio binary — auto-download on first SR deploy if missing
        # (mirrors deploy_cluster). Customer doesn't have to pre-stage the
        # tarball in /var/lib/tantor/repo/apicurio.
        from pathlib import Path as _Path
        apicurio_repo = settings.APICURIO_REPO_DIR
        _Path(apicurio_repo).mkdir(parents=True, exist_ok=True)
        existing = sorted(_Path(apicurio_repo).glob("apicurio-registry-app-*.tar.gz"))
        if not existing:
            apicurio_tgz = f"apicurio-registry-app-{settings.APICURIO_VERSION}-all.tar.gz"
            apicurio_tgz_path = str(_Path(apicurio_repo) / apicurio_tgz)
            url = f"https://github.com/Apicurio/apicurio-registry/releases/download/{settings.APICURIO_VERSION}/{apicurio_tgz}"
            # v1.4.1 — retry with timeout so a flaky corp proxy or
            # GitHub blip doesn't kill the deploy. urlretrieve has no
            # timeout, so we wrap urlopen + write ourselves.
            import urllib.request, socket
            ok = False
            last_err = None
            for attempt in (1, 2, 3):
                log(f"Apicurio binary not found locally. Downloading {apicurio_tgz} (attempt {attempt}/3)…")
                try:
                    req = urllib.request.Request(url, headers={"User-Agent": "tantor/1.4.1"})
                    with urllib.request.urlopen(req, timeout=180) as r, open(apicurio_tgz_path, "wb") as f:
                        while True:
                            chunk = r.read(1 << 20)  # 1 MiB
                            if not chunk:
                                break
                            f.write(chunk)
                    size_mb = _Path(apicurio_tgz_path).stat().st_size // (1024*1024)
                    if size_mb < 50:  # sanity check — real tarball is ~130 MB
                        raise IOError(f"download truncated ({size_mb} MB; expected ~130)")
                    log(f"Downloaded {apicurio_tgz} ({size_mb} MB)")
                    ok = True
                    break
                except (socket.timeout, OSError, IOError) as e:
                    last_err = e
                    log(f"  attempt {attempt} failed: {e}")
                    try:
                        _Path(apicurio_tgz_path).unlink(missing_ok=True)
                    except Exception:
                        pass
            if not ok:
                log(f"ERROR: Apicurio download failed after 3 attempts: {last_err}")
                log(f"  If you're behind a corporate proxy, set HTTP_PROXY/HTTPS_PROXY in /etc/systemd/system/tantor-backend.service.d/proxy.conf")
                log(f"  Or air-gap workaround: place {apicurio_tgz} at {apicurio_repo}/ and retry.")
                _set_status(db, task_id, "error", error_message=f"Apicurio download failed: {last_err}")
                sr_svc.status = "error"
                db.commit()
                return
        else:
            apicurio_tgz = existing[-1].name
            apicurio_tgz_path = str(existing[-1])
        # Detect strip-components from the tarball layout
        try:
            import tarfile
            with tarfile.open(apicurio_tgz_path) as tf:
                names = tf.getnames()[:5]
            apicurio_strip = 1 if names and "/" in names[0] else 0
        except Exception:
            apicurio_strip = 1

        # Build a single-host inventory for SR
        svc_dicts = [{
            "ip_address": host.ip_address, "port": host.ssh_port,
            "username": host.username, "auth_type": host.auth_type,
            "credential": decrypt(host.encrypted_credential),
            "role": "schema_registry", "node_id": sr_svc.node_id,
        }]
        inv_path = ansible_runner.generate_inventory(work_dir, svc_dicts, tls_keystores=None)
        ansible_runner.generate_ansible_cfg(work_dir)
        playbook_path = ansible_runner.generate_playbook(work_dir, "deploy_schema_registry.yml.j2", {
            "apicurio_install_dir": settings.APICURIO_INSTALL_DIR,
            "apicurio_binary_filename": apicurio_tgz,
            "apicurio_binary_local_path": apicurio_tgz_path,
            "apicurio_strip_components": apicurio_strip,
            "configs_dir": str(configs_dir),
            "systemd_dir": str(systemd_dir),
            "schema_registry_port": port,
        })

        _set_step(db, task_id, "Running Schema Registry playbook")
        log("Running Ansible…")

        def emit(line: str):
            log(line)

        exit_code = ansible_runner.run_playbook(work_dir, playbook_path, inv_path, emit)
        if exit_code == 0:
            sr_svc.status = "running"
            db.commit()
            _set_status(db, task_id, "completed")
            _set_step(db, task_id, "Completed")
            log("Schema Registry deployed successfully")
        else:
            sr_svc.status = "error"
            db.commit()
            _set_status(db, task_id, "completed_with_errors", error_message=f"ansible exit code: {exit_code}")
            _set_step(db, task_id, "Failed")
            log(f"Deployment failed (ansible exit code: {exit_code})")

        ansible_runner.cleanup_workspace(work_dir)
    finally:
        db.close()
