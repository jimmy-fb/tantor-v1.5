"""TLS/mTLS API for managed clusters.

Surface:
  GET  /api/clusters/{id}/security/tls            — current state
  POST /api/clusters/{id}/security/tls            — toggle ssl_enabled / mtls_required
  GET  /api/clusters/{id}/security/tls/ca         — download cluster CA cert (PEM)
  GET  /api/clusters/{id}/security/tls/clients    — list issued client certs
  POST /api/clusters/{id}/security/tls/clients    — mint a new client cert bundle
  DELETE /api/clusters/{id}/security/tls/clients/{cn} — remove a client cert

Issuing a client cert returns the full PEM bundle (cert + key + CA) so the
operator can wire up a producer/consumer in one round-trip. Tantor doesn't
keep the private key for clients (it's only sent in the response and then
discarded — but for convenience we do persist it under
/var/lib/tantor/certs/{cluster}/clients/{cn}/ so the operator can re-download).
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Body, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.deps import require_admin, require_monitor_or_above
from app.database import get_db
from app.models.cluster import Cluster
from app.models.user import User
from app.services import cert_manager

logger = logging.getLogger("tantor.security.tls")

router = APIRouter(prefix="/api/clusters/{cluster_id}/security/tls", tags=["security-tls"])


class TLSStateResponse(BaseModel):
    ssl_enabled: bool
    mtls_required: bool
    ca_present: bool
    ssl_listener_port: int


class TLSToggleRequest(BaseModel):
    ssl_enabled: bool
    mtls_required: bool = False


class ClientCertCreate(BaseModel):
    common_name: str = Field(min_length=1, max_length=120)
    ttl_days: int = Field(default=365, ge=1, le=3650)
    force_rotate: bool = False


class ClientCertSummary(BaseModel):
    common_name: str
    issued_at: str
    expires_at: str
    serial_number: str


class ClientCertBundle(BaseModel):
    common_name: str
    ca_pem: str
    cert_pem: str
    key_pem: str
    p12_password: str
    issued_at: str
    expires_at: str


def _get_managed_cluster(cluster_id: str, db: Session) -> Cluster:
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    if (cluster.kind or "managed") == "external":
        raise HTTPException(status_code=400, detail="TLS management is for Tantor-deployed clusters; for external clusters configure SSL on the source cluster")
    return cluster


@router.get("", response_model=TLSStateResponse)
def get_tls_state(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    cluster = _get_managed_cluster(cluster_id, db)
    import json as _json
    config = _json.loads(cluster.config_json) if cluster.config_json else {}
    return TLSStateResponse(
        ssl_enabled=bool(cluster.ssl_enabled),
        mtls_required=bool(cluster.mtls_required),
        ca_present=(cert_manager.CERTS_BASE / cluster.id / "ca.crt").exists(),
        ssl_listener_port=int(config.get("ssl_listener_port", 9096)),
    )


@router.post("", response_model=TLSStateResponse)
def set_tls_state(
    cluster_id: str,
    body: TLSToggleRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Toggle SSL/mTLS on the cluster row. Operator must redeploy (or rolling-
    restart brokers) for the change to take effect on running brokers.
    """
    cluster = _get_managed_cluster(cluster_id, db)
    cluster.ssl_enabled = bool(body.ssl_enabled)
    cluster.mtls_required = bool(body.mtls_required) and bool(body.ssl_enabled)
    if cluster.ssl_enabled:
        cert_manager.ensure_cluster_ca(cluster)  # eager-generate so /ca download works immediately
    db.commit()
    return get_tls_state(cluster_id=cluster_id, db=db, _=None)  # type: ignore[arg-type]


@router.get("/ca")
def download_ca(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    """Stream the cluster CA cert (PEM) so producers/consumers can trust it."""
    cluster = _get_managed_cluster(cluster_id, db)
    if not cluster.ssl_enabled:
        raise HTTPException(status_code=400, detail="SSL is not enabled on this cluster")
    pem = cert_manager.get_ca_pem(cluster)
    return PlainTextResponse(
        content=pem.decode(),
        headers={"Content-Disposition": f'attachment; filename="{cluster.name}-ca.crt"'},
    )


@router.get("/clients", response_model=list[ClientCertSummary])
def list_client_certs(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    cluster = _get_managed_cluster(cluster_id, db)
    return [ClientCertSummary(**c) for c in cert_manager.list_client_certs(cluster)]


@router.post("/clients", response_model=ClientCertBundle)
def issue_client_cert(
    cluster_id: str,
    body: ClientCertCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    cluster = _get_managed_cluster(cluster_id, db)
    if not cluster.ssl_enabled:
        raise HTTPException(status_code=400, detail="Enable SSL on the cluster first")
    try:
        bundle = cert_manager.issue_client_cert(
            cluster, db, body.common_name, body.ttl_days, force_rotate=body.force_rotate
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return ClientCertBundle(**bundle)


@router.delete("/clients/{common_name}")
def revoke_client_cert(
    cluster_id: str,
    common_name: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    cluster = _get_managed_cluster(cluster_id, db)
    removed = cert_manager.revoke_client_cert(cluster, common_name)
    if not removed:
        raise HTTPException(status_code=404, detail="Client cert not found")
    return {"common_name": common_name, "revoked": True}
