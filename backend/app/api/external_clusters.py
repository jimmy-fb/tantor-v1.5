"""API for connecting to externally-managed Kafka clusters.

Tantor stores these as `Cluster` rows with `kind="external"`. They share the
clusters listing but are surfaced through their own create/update/test path
because the connection-secret shape is different from a managed deploy and
many actions (deploy / start / stop / upgrade / rolling restart) don't apply.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import json

from pydantic import BaseModel
from app.api.deps import require_admin, require_monitor_or_above
from app.database import get_db
from app.models.cluster import Cluster
from app.models.host import Host
from app.models.user import User
from app.schemas.external_cluster import (
    ExternalClusterCreate,
    ExternalClusterResponse,
    ExternalClusterUpdate,
    ExternalConnectionTestRequest,
    ExternalConnectionTestResponse,
)
from app.services import external_admin
from app.services.crypto import decrypt
from app.services.ssh_manager import SSHManager

logger = logging.getLogger("tantor.external_clusters")

router = APIRouter(prefix="/api/external-clusters", tags=["external-clusters"])


def _to_response(cluster: Cluster) -> ExternalClusterResponse:
    redacted = external_admin.redact_connection(cluster)
    return ExternalClusterResponse(
        id=cluster.id,
        name=cluster.name,
        kind="external",
        state=cluster.state,
        bootstrap_servers=redacted["bootstrap_servers"],
        security_protocol=redacted["security_protocol"],
        sasl_mechanism=redacted["sasl_mechanism"],
        sasl_username=redacted["sasl_username"],
        sasl_password_set=redacted["sasl_password_set"],
        ssl_ca_set=redacted["ssl_ca_set"],
        ssl_cert_set=redacted["ssl_cert_set"],
        ssl_key_set=redacted["ssl_key_set"],
        ssl_verify=redacted["ssl_verify"],
    )


@router.get("", response_model=list[ExternalClusterResponse])
def list_external(
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    rows = (
        db.query(Cluster)
        .filter(Cluster.kind == "external")
        .order_by(Cluster.created_at.desc())
        .all()
    )
    return [_to_response(c) for c in rows]


@router.post("", response_model=ExternalClusterResponse)
def create_external(
    data: ExternalClusterCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    if not data.bootstrap_servers.strip():
        raise HTTPException(status_code=400, detail="bootstrap_servers is required")

    secrets_dict = data.secrets.model_dump(exclude_none=True) if data.secrets else {}
    cluster = Cluster(
        name=data.name,
        kafka_version="external",  # placeholder, replaced below if probe succeeds
        mode="kraft",
        kind="external",
        state="connected",
        bootstrap_servers=data.bootstrap_servers,
        security_protocol=data.security_protocol,
        sasl_mechanism=data.sasl_mechanism,
        ssl_verify=data.ssl_verify,
        encrypted_connection_secrets=external_admin.encrypt_secrets(secrets_dict) if secrets_dict else None,
    )
    # Probe the cluster on add so we can store the real broker version
    # instead of leaving the listing showing "external" / "unknown".
    # Best-effort — connection failures don't block the create.
    try:
        probe = external_admin.test_connection(cluster)
        if probe.get("success") and probe.get("kafka_version"):
            cluster.kafka_version = probe["kafka_version"]
    except Exception:
        pass
    db.add(cluster)
    db.commit()
    db.refresh(cluster)
    return _to_response(cluster)


@router.put("/{cluster_id}", response_model=ExternalClusterResponse)
def update_external(
    cluster_id: str,
    data: ExternalClusterUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id, Cluster.kind == "external").first()
    if not cluster:
        raise HTTPException(status_code=404, detail="External cluster not found")
    if data.name is not None: cluster.name = data.name
    if data.bootstrap_servers is not None: cluster.bootstrap_servers = data.bootstrap_servers
    if data.security_protocol is not None: cluster.security_protocol = data.security_protocol
    if data.sasl_mechanism is not None: cluster.sasl_mechanism = data.sasl_mechanism
    if data.ssl_verify is not None: cluster.ssl_verify = data.ssl_verify
    if data.secrets is not None:
        # Merge: only overwrite stored secrets that the operator actually filled in.
        existing = external_admin.decrypt_secrets(cluster.encrypted_connection_secrets)
        for k, v in data.secrets.model_dump(exclude_none=True).items():
            if v == "" or v is None:
                continue
            existing[k] = v
        cluster.encrypted_connection_secrets = external_admin.encrypt_secrets(existing) if existing else None
    db.commit()
    db.refresh(cluster)
    return _to_response(cluster)


@router.delete("/{cluster_id}")
def delete_external(
    cluster_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id, Cluster.kind == "external").first()
    if not cluster:
        raise HTTPException(status_code=404, detail="External cluster not found")
    db.delete(cluster)
    db.commit()
    return {"detail": "External cluster removed"}


@router.post("/test-connection", response_model=ExternalConnectionTestResponse)
def test_connection_unsaved(
    data: ExternalConnectionTestRequest,
    _: User = Depends(require_admin),
):
    """Validate connection params WITHOUT persisting them.

    Used by the UI's 'Test Connection' button before save. Builds a stand-in
    Cluster object and runs the same probe as test_connection_saved.
    """
    transient = Cluster(
        id="<unsaved>",
        name="<unsaved>",
        kafka_version="external",
        mode="kraft",
        kind="external",
        bootstrap_servers=data.bootstrap_servers,
        security_protocol=data.security_protocol,
        sasl_mechanism=data.sasl_mechanism,
        ssl_verify=data.ssl_verify,
        encrypted_connection_secrets=(
            external_admin.encrypt_secrets(data.secrets.model_dump(exclude_none=True))
            if data.secrets else None
        ),
    )
    result = external_admin.test_connection(transient)
    return ExternalConnectionTestResponse(**result)


@router.post("/{cluster_id}/test", response_model=ExternalConnectionTestResponse)
def test_connection_saved(
    cluster_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id, Cluster.kind == "external").first()
    if not cluster:
        raise HTTPException(status_code=404, detail="External cluster not found")
    result = external_admin.test_connection(cluster)
    # If we successfully detected a real broker version, store it so the
    # cluster listing shows "4.1.0" instead of "unknown" / "external".
    if result.get("success") and result.get("kafka_version"):
        cluster.kafka_version = result["kafka_version"]
        db.commit()
    return ExternalConnectionTestResponse(**result)


# ── Read-only operations (topics + consumer groups + produce/consume) ─────


@router.get("/{cluster_id}/topics")
def list_topics(
    cluster_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id, Cluster.kind == "external").first()
    if not cluster:
        raise HTTPException(status_code=404, detail="External cluster not found")
    try:
        return external_admin.list_topics(cluster)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"{type(e).__name__}: {e}")


@router.get("/{cluster_id}/consumer-groups")
def list_consumer_groups(
    cluster_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id, Cluster.kind == "external").first()
    if not cluster:
        raise HTTPException(status_code=404, detail="External cluster not found")
    try:
        return external_admin.list_consumer_groups(cluster)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"{type(e).__name__}: {e}")


@router.post("/{cluster_id}/topics")
def create_topic(
    cluster_id: str,
    body: dict,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id, Cluster.kind == "external").first()
    if not cluster:
        raise HTTPException(status_code=404, detail="External cluster not found")
    try:
        return external_admin.create_topic(
            cluster,
            body["name"],
            int(body.get("partitions", 1)),
            int(body.get("replication_factor", 1)),
        )
    except KeyError as e:
        raise HTTPException(status_code=422, detail=f"Missing field: {e}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"{type(e).__name__}: {e}")


@router.delete("/{cluster_id}/topics/{topic_name}")
def delete_topic(
    cluster_id: str,
    topic_name: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id, Cluster.kind == "external").first()
    if not cluster:
        raise HTTPException(status_code=404, detail="External cluster not found")
    try:
        return external_admin.delete_topic(cluster, topic_name)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"{type(e).__name__}: {e}")


@router.post("/{cluster_id}/produce")
def produce(
    cluster_id: str,
    body: dict,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id, Cluster.kind == "external").first()
    if not cluster:
        raise HTTPException(status_code=404, detail="External cluster not found")
    try:
        return external_admin.produce_message(
            cluster, body["topic"], body.get("key"), body["value"],
        )
    except KeyError as e:
        raise HTTPException(status_code=422, detail=f"Missing field: {e}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"{type(e).__name__}: {e}")


@router.post("/{cluster_id}/consume")
def consume(
    cluster_id: str,
    body: dict,
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id, Cluster.kind == "external").first()
    if not cluster:
        raise HTTPException(status_code=404, detail="External cluster not found")
    try:
        msgs = external_admin.consume_messages(
            cluster,
            body["topic"],
            int(body.get("max_messages", 10)),
            int(body.get("timeout_ms", 5000)),
            bool(body.get("from_beginning", True)),
        )
        return {"messages": msgs, "count": len(msgs)}
    except KeyError as e:
        raise HTTPException(status_code=422, detail=f"Missing field: {e}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"{type(e).__name__}: {e}")


# ── SSH-based lifecycle for externally-managed clusters (APB v1.3) ──────────
# An external cluster is one Tantor didn't deploy, so by default we can't
# start/restart it — we don't know where the brokers run or what the systemd
# unit is called. The operator can opt in by registering broker hosts (which
# already have SSH credentials in Tantor's Host table) and the systemd unit
# name. After that, /start, /stop, /restart issue `systemctl <action> <unit>`
# on each host. We never touch Kafka data — this is just a remote button.


class BrokerHostEntry(BaseModel):
    host_id: str
    kafka_unit: str = "kafka.service"


class BrokerHostsRequest(BaseModel):
    hosts: list[BrokerHostEntry]


def _read_external_broker_hosts(cluster: Cluster) -> list[dict]:
    if not cluster.external_broker_hosts_json:
        return []
    try:
        return json.loads(cluster.external_broker_hosts_json)
    except Exception:
        return []


@router.get("/{cluster_id}/broker-hosts")
def list_broker_hosts(
    cluster_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id, Cluster.kind == "external").first()
    if not cluster:
        raise HTTPException(status_code=404, detail="External cluster not found")
    entries = _read_external_broker_hosts(cluster)
    enriched = []
    for e in entries:
        h = db.query(Host).filter(Host.id == e["host_id"]).first()
        enriched.append({
            "host_id": e["host_id"],
            "kafka_unit": e.get("kafka_unit", "kafka.service"),
            "hostname": h.hostname if h else None,
            "ip_address": h.ip_address if h else None,
            "online": h.status == "online" if h else False,
        })
    return enriched


@router.put("/{cluster_id}/broker-hosts")
def set_broker_hosts(
    cluster_id: str,
    body: BrokerHostsRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id, Cluster.kind == "external").first()
    if not cluster:
        raise HTTPException(status_code=404, detail="External cluster not found")
    # Validate every referenced host exists.
    bad = []
    for entry in body.hosts:
        h = db.query(Host).filter(Host.id == entry.host_id).first()
        if not h:
            bad.append(entry.host_id)
    if bad:
        raise HTTPException(status_code=400, detail=f"Unknown host_id(s): {bad}")
    cluster.external_broker_hosts_json = json.dumps(
        [{"host_id": e.host_id, "kafka_unit": e.kafka_unit} for e in body.hosts]
    )
    db.commit()
    return list_broker_hosts(cluster_id, db, _)


def _systemctl_each(cluster_id: str, action: str, db: Session) -> list[dict]:
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id, Cluster.kind == "external").first()
    if not cluster:
        raise HTTPException(status_code=404, detail="External cluster not found")
    entries = _read_external_broker_hosts(cluster)
    if not entries:
        raise HTTPException(
            status_code=400,
            detail=(
                "No broker hosts registered for this external cluster. "
                "Configure them in the Lifecycle tab first."
            ),
        )
    results = []
    for e in entries:
        host = db.query(Host).filter(Host.id == e["host_id"]).first()
        if not host:
            results.append({"host_id": e["host_id"], "ok": False, "message": "host record missing"})
            continue
        unit = e.get("kafka_unit", "kafka.service")
        cmd = f"sudo systemctl {action} {unit}"
        try:
            with SSHManager.connect(
                host.ip_address, host.ssh_port, host.username,
                host.auth_type, host.encrypted_credential,
            ) as client:
                rc, stdout, stderr = SSHManager.exec_command(client, cmd, timeout=30)
            results.append({
                "host_id": e["host_id"],
                "hostname": host.hostname,
                "kafka_unit": unit,
                "exit_code": rc,
                "ok": rc == 0,
                "message": (stdout or stderr or "").strip()[:300],
            })
        except Exception as ex:
            results.append({
                "host_id": e["host_id"],
                "hostname": host.hostname,
                "kafka_unit": unit,
                "ok": False,
                "message": f"ssh failed: {type(ex).__name__}: {ex}",
            })
    return results


@router.post("/{cluster_id}/start")
def start_external(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    return {"results": _systemctl_each(cluster_id, "start", db)}


@router.post("/{cluster_id}/stop")
def stop_external(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    return {"results": _systemctl_each(cluster_id, "stop", db)}


@router.post("/{cluster_id}/restart")
def restart_external(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    return {"results": _systemctl_each(cluster_id, "restart", db)}
