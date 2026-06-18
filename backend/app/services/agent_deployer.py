"""Agent-based deployer.

Drop-in for the Ansible+SSH deployer when every broker host has a connected
tantor-agent. The agent's install scripts + file.download + file.write ops
mean the SCM never opens an SSH connection during deploy. See
docs/AGENT_PROTOCOL.md sections 7 + 10.

What this MVP supports
----------------------
* KRaft mode only — broker, broker_controller, controller.
* Java 17 install via the agent's install-java.sh.
* Kafka tarball downloaded from the SCM's `/repo/kafka/<filename>` over HTTPS.
* server.properties rendered by ConfigGenerator and written via file.write.
* systemd unit rendered, written to /etc/systemd/system/, then enabled+started.
* Optional initial ACLs (delegated to kafka_admin once the broker is up).

What is NOT yet covered
-----------------------
* Schema Registry, Kafka Connect, ksqlDB. Operators with those workloads
  still use the SSH+Ansible deployer (set `cluster.config_json.deploy_via='ssh'`
  or omit the override).
* Zookeeper-mode clusters. KRaft is the v1.5 default; zk operators stay on SSH.
* TLS / SSL — the cert plumbing in cert_manager is SSH-flavored and gets a
  follow-up pass.
* mTLS, JAAS, OAuth — same as above.

These omissions are intentional for the MVP. The orchestrator picks the SSH
deployer when ANY of the above features are enabled, so operators don't get
silent feature loss.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models.cluster import Cluster
from app.models.deployment_task import DeploymentTask
from app.models.host import Host
from app.models.service import Service
from app.services import agent_transport, cluster_paths
from app.services.config_generator import ConfigGenerator

logger = logging.getLogger("tantor.agent_deployer")

# Where the SCM serves the Kafka tarball from. The customer pre-stages the
# tarball alongside the Tantor backend (same path the SSH playbook reads
# from) and this URL is consumed by the agent's file.download op.
DEFAULT_KAFKA_TARBALL_URL_FMT = "{scm_base}/api/repo/kafka/{filename}"


def supports_agent_deploy(cluster: Cluster, services: list[Service], hosts_by_id: dict[str, Host]) -> tuple[bool, str]:
    """Return (ok, reason) — True if every prerequisite for the agent-based
    deploy path is satisfied for this cluster."""
    if cluster.mode != "kraft":
        return False, f"agent deployer requires KRaft mode (got {cluster.mode!r})"
    if cluster.ssl_enabled or cluster.mtls_required:
        return False, "agent deployer does not yet support SSL/mTLS"
    unsupported_roles = {svc.role for svc in services} - {"broker", "broker_controller", "controller"}
    if unsupported_roles:
        return False, f"agent deployer does not yet support roles: {sorted(unsupported_roles)}"
    for svc in services:
        host = hosts_by_id.get(svc.host_id)
        if not host:
            return False, f"host {svc.host_id} missing for service {svc.role}"
        if not agent_transport.agent_available(host.id):
            return False, f"no agent connected for host {host.hostname or host.ip_address}"
    return True, "ok"


def _service_to_dict(svc: Service, host: Host) -> dict:
    """Match the shape ConfigGenerator expects."""
    return {
        "node_id": svc.node_id,
        "role": svc.role,
        "ip_address": host.ip_address,
        "host_id": host.id,
        "id": svc.id,
    }


def _append_log(db: Session, task_id: str, message: str) -> None:
    row = db.query(DeploymentTask).filter(DeploymentTask.id == task_id).first()
    if row is None:
        return
    try:
        logs = json.loads(row.logs or "[]")
    except (TypeError, ValueError):
        logs = []
    logs.append(message)
    # Keep bounded — same cap as the SSH deployer.
    if len(logs) > 5000:
        logs = logs[-5000:]
    row.logs = json.dumps(logs)
    row.updated_at = datetime.now(timezone.utc)
    db.commit()


def _set_step(db: Session, task_id: str, step: str) -> None:
    row = db.query(DeploymentTask).filter(DeploymentTask.id == task_id).first()
    if row is None:
        return
    row.current_step = step
    row.updated_at = datetime.now(timezone.utc)
    db.commit()


def _set_status(db: Session, task_id: str, status: str, *, error_message: str | None = None) -> None:
    row = db.query(DeploymentTask).filter(DeploymentTask.id == task_id).first()
    if row is None:
        return
    row.status = status
    if error_message:
        row.error_message = error_message
    if status in ("success", "error"):
        row.finished_at = datetime.now(timezone.utc)
    row.updated_at = datetime.now(timezone.utc)
    db.commit()


def deploy_cluster(cluster_id: str, task_id: str, scm_base_url: str, kafka_tarball_filename: str) -> None:
    """Orchestrate an agent-only deploy. Runs in a background thread.

    scm_base_url is the public URL the brokers can reach Tantor at — used by
    file.download. kafka_tarball_filename is e.g. "kafka_2.13-3.7.0.tgz" —
    the file must exist under backend/repo/kafka/ on the SCM.
    """
    db = SessionLocal()
    try:
        _deploy_inner(cluster_id, task_id, scm_base_url, kafka_tarball_filename, db)
    except Exception as e:
        logger.exception("agent-based deploy crashed for cluster %s", cluster_id)
        _append_log(db, task_id, f"ERROR: {e}")
        _set_status(db, task_id, "error", error_message=str(e))
    finally:
        db.close()


def _deploy_inner(cluster_id: str, task_id: str, scm_base_url: str, kafka_tarball_filename: str, db: Session) -> None:
    def log(m: str) -> None:
        _append_log(db, task_id, m)
        if m.strip():
            logger.info("[%s] %s", task_id[:8], m)

    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        log("ERROR: cluster not found")
        _set_status(db, task_id, "error", error_message="cluster not found")
        return

    services = db.query(Service).filter(Service.cluster_id == cluster_id).all()
    hosts_by_id = {h.id: h for h in db.query(Host).all()}
    cluster_config = json.loads(cluster.config_json) if cluster.config_json else {}

    ok, reason = supports_agent_deploy(cluster, services, hosts_by_id)
    if not ok:
        log(f"ERROR: agent deployer can't handle this cluster: {reason}")
        _set_status(db, task_id, "error", error_message=reason)
        return

    cluster.state = "deploying"
    db.commit()

    # ── Pre-flight ───────────────────────────────────────────────────────
    _set_step(db, task_id, "Pre-flight (agent path)")
    log("All target hosts have a connected tantor-agent")
    log(f"Kafka tarball: {scm_base_url}/repo/kafka/{kafka_tarball_filename}")
    log(f"Install dir: {cluster_paths.install_dir(cluster)}")
    log(f"Data dir: {cluster_paths.data_dir(cluster)}")
    log(f"Systemd unit: {cluster_paths.unit_name(cluster)}")

    services_dicts = [_service_to_dict(s, hosts_by_id[s.host_id]) for s in services]

    install_dir = cluster_paths.install_dir(cluster)
    data_dir = cluster_paths.data_dir(cluster)
    unit_name = cluster_paths.unit_name(cluster)
    tarball_url = DEFAULT_KAFKA_TARBALL_URL_FMT.format(scm_base=scm_base_url, filename=kafka_tarball_filename)
    tarball_dest = f"/opt/tantor-stage/{kafka_tarball_filename}"

    # ── Per-host install loop ────────────────────────────────────────────
    for svc in sorted(services, key=lambda s: 0 if s.role == "controller" else 1):
        host = hosts_by_id[svc.host_id]
        prefix = f"{host.hostname or host.ip_address}/{svc.role}#{svc.node_id}"

        # 1. Java
        _set_step(db, task_id, f"{prefix}: java")
        log(f"[{prefix}] checking Java...")
        ok, stdout, stderr = agent_transport.exec_script_via_agent(host, "install-java.sh")
        if not ok:
            log(f"[{prefix}] ERROR: install-java.sh failed: {stderr}")
            _set_status(db, task_id, "error", error_message=f"java install failed on {prefix}")
            return
        log(f"[{prefix}] java ok: {stdout.strip().splitlines()[-1] if stdout else 'installed'}")

        # 2. Download tarball
        _set_step(db, task_id, f"{prefix}: download kafka")
        log(f"[{prefix}] downloading Kafka tarball from SCM...")
        ok, msg = agent_transport.file_download_via_agent(host, tarball_url, tarball_dest, mode=0o644)
        if not ok:
            log(f"[{prefix}] ERROR: download failed: {msg}")
            _set_status(db, task_id, "error", error_message=f"tarball download failed on {prefix}")
            return
        log(f"[{prefix}] download ok: {msg.strip().splitlines()[0] if msg else ''}")

        # 3. Extract + create kafka user
        _set_step(db, task_id, f"{prefix}: extract kafka")
        log(f"[{prefix}] extracting Kafka to {install_dir}...")
        ok, stdout, stderr = agent_transport.exec_script_via_agent(
            host, "install-kafka.sh", [tarball_dest, install_dir, data_dir, "kafka"],
        )
        if not ok:
            log(f"[{prefix}] ERROR: install-kafka.sh failed: {stderr}")
            _set_status(db, task_id, "error", error_message=f"extract failed on {prefix}")
            return
        log(f"[{prefix}] extracted: {stdout.strip().splitlines()[-1] if stdout else ''}")

        # 4. Render + write server.properties
        _set_step(db, task_id, f"{prefix}: configure")
        log(f"[{prefix}] generating server.properties...")
        # log_dirs MUST match the per-cluster data dir
        cluster_config["log_dirs"] = data_dir
        server_props = ConfigGenerator.generate_kraft_broker_config(
            _service_to_dict(svc, host), services_dicts, cluster_config,
        )
        cfg_path = f"{install_dir}/config/server.properties"
        ok, msg = agent_transport.file_write_via_agent(host, cfg_path, server_props, mode=0o640)
        if not ok:
            log(f"[{prefix}] ERROR: server.properties write failed: {msg}")
            _set_status(db, task_id, "error", error_message=f"config write failed on {prefix}")
            return
        log(f"[{prefix}] wrote {cfg_path}")

        # 5. Render + write systemd unit
        _set_step(db, task_id, f"{prefix}: systemd unit")
        log(f"[{prefix}] generating systemd unit {unit_name}...")
        unit_body = ConfigGenerator.generate_systemd_unit(
            service_type="kafka",
            config_path=cfg_path,
            kafka_home=install_dir,
            unit_name=unit_name,
        )
        unit_path = f"/etc/systemd/system/{unit_name}"
        ok, msg = agent_transport.file_write_via_agent(host, unit_path, unit_body, mode=0o644)
        if not ok:
            log(f"[{prefix}] ERROR: systemd unit write failed: {msg}")
            _set_status(db, task_id, "error", error_message=f"unit write failed on {prefix}")
            return
        log(f"[{prefix}] wrote {unit_path}")

        # 6. daemon-reload + enable
        ok, stdout, stderr = agent_transport.exec_script_via_agent(host, "install-systemd-unit.sh", [unit_name])
        if not ok:
            log(f"[{prefix}] ERROR: enable unit failed: {stderr}")
            _set_status(db, task_id, "error", error_message=f"unit enable failed on {prefix}")
            return
        log(f"[{prefix}] unit enabled")

        # 7. Start
        _set_step(db, task_id, f"{prefix}: start")
        ok, msg = agent_transport.systemctl_action(host, "start", unit_name)
        if not ok:
            log(f"[{prefix}] ERROR: start failed: {msg}")
            _set_status(db, task_id, "error", error_message=f"start failed on {prefix}")
            return
        log(f"[{prefix}] started ✓")
        svc.status = "running"
        db.commit()

    cluster.state = "running"
    db.commit()
    _set_step(db, task_id, "complete")
    log("Agent-based deploy complete — every step ran over the WebSocket.")
    _set_status(db, task_id, "success")
