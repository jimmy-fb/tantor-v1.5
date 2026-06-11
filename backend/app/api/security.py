from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Form
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.audit_log import AuditLog
from app.models.cluster import Cluster
from app.schemas.security import (
    KafkaUserCreate,
    KafkaUserResponse,
    KafkaUserCreatedResponse,
    KafkaUserRotateRequest,
    KafkaUserRotateResponse,
    KafkaUserDeleteResponse,
    AclCreateRequest,
    AclCreateResponse,
    AclDeleteRequest,
    AclDeleteResponse,
    AclListResponse,
    AuditLogEntry,
)
from app.services.kafka_admin import kafka_admin
from app.services import cert_manager
from app.api.deps import require_admin, require_monitor_or_above
from app.models.user import User

router = APIRouter(prefix="/api/clusters/{cluster_id}/security", tags=["kafka-security"])


# ── SCRAM Users ──────────────────────────────────────

@router.get("/users", response_model=list[KafkaUserResponse])
def list_users(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    try:
        return kafka_admin.list_scram_users(cluster_id, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/users", response_model=KafkaUserCreatedResponse)
def create_user(cluster_id: str, data: KafkaUserCreate, db: Session = Depends(get_db), current_user: User = Depends(require_admin)):
    try:
        return kafka_admin.create_scram_user(
            cluster_id, data.username, data.password, data.mechanism, db, actor=current_user,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/users/{username}", response_model=KafkaUserDeleteResponse)
def delete_user(cluster_id: str, username: str, db: Session = Depends(get_db), current_user: User = Depends(require_admin)):
    try:
        return kafka_admin.delete_scram_user(cluster_id, username, db, actor=current_user)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/users/{username}/rotate", response_model=KafkaUserRotateResponse)
def rotate_user_password(
    cluster_id: str, username: str, data: KafkaUserRotateRequest, db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    try:
        return kafka_admin.rotate_scram_password(cluster_id, username, data.password, db, actor=current_user)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── ACLs ──────────────────────────────────────────────

@router.get("/acls", response_model=AclListResponse)
def list_acls(
    cluster_id: str,
    principal: str | None = None,
    resource_type: str | None = None,
    resource_name: str | None = None,
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    try:
        acls = kafka_admin.list_acls(
            cluster_id, db, principal=principal,
            resource_type=resource_type, resource_name=resource_name,
        )
        return AclListResponse(acls=acls, count=len(acls))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        err = str(e)
        # Kafka returns a TopicAuthorizationFailedException or
        # ClusterAuthorizationException when the ACL authorizer is not
        # configured on the broker (common on default installs) or when
        # the connecting user lacks the DescribeAcls permission.
        # Return a clean 400 instead of a 500 so the UI can show a
        # helpful message rather than "Internal server error".
        acl_keywords = (
            "ClusterAuthorization", "SecurityDisabled",
            "PolicyViolation", "AuthorizationException",
            "authorizer", "SECURITY_DISABLED", "acl",
            "NoBrokersAvailable", "KafkaConnectionError",
        )
        if any(k.lower() in err.lower() for k in acl_keywords):
            raise HTTPException(
                status_code=400,
                detail=(
                    "ACLs are not enabled on this cluster. "
                    "Set 'authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer' "
                    "in server.properties and restart the broker to enable ACL support. "
                    f"Broker error: {err}"
                ),
            )
        raise HTTPException(status_code=500, detail=f"Failed to list ACLs: {err}")


@router.post("/acls", response_model=AclCreateResponse)
def create_acl(cluster_id: str, data: AclCreateRequest, db: Session = Depends(get_db), current_user: User = Depends(require_admin)):
    try:
        return kafka_admin.create_acl(cluster_id, data.model_dump(), db, actor=current_user)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/acls", response_model=AclDeleteResponse)
def delete_acl(cluster_id: str, data: AclDeleteRequest, db: Session = Depends(get_db), current_user: User = Depends(require_admin)):
    try:
        return kafka_admin.delete_acl(cluster_id, data.model_dump(), db, actor=current_user)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/acls/topic/{topic_name}", response_model=AclListResponse)
def get_topic_acls(cluster_id: str, topic_name: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    try:
        acls = kafka_admin.list_acls(cluster_id, db, resource_type="topic", resource_name=topic_name)
        return AclListResponse(acls=acls, count=len(acls))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/acls/principal/{principal}", response_model=AclListResponse)
def get_principal_acls(cluster_id: str, principal: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    try:
        acls = kafka_admin.list_acls(cluster_id, db, principal=principal)
        return AclListResponse(acls=acls, count=len(acls))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── Audit Log ──────────────────────────────────────────

@router.get("/audit-log", response_model=list[AuditLogEntry])
def get_audit_log(
    cluster_id: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    action: str | None = None,
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    query = db.query(AuditLog).filter(AuditLog.cluster_id == cluster_id)
    if action:
        query = query.filter(AuditLog.action == action)
    logs = query.order_by(AuditLog.created_at.desc()).offset(offset).limit(limit).all()
    return logs


# ── Certificates (v1.4.0 #8) ──────────────────────


@router.get("/certificates")
def list_certificates(
    cluster_id: str, db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    """List the cluster's stored certificate material."""
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return cert_manager.list_cluster_certificates(cluster)


@router.post("/certificates/ca")
def upload_certificate_ca(
    cluster_id: str,
    ca_cert: UploadFile = File(..., description="CA certificate (PEM)"),
    ca_key: UploadFile | None = File(None, description="CA private key (PEM, optional)"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """Replace the cluster's CA with operator-supplied PEM material.

    v1.4.0 #8 — operators bring their own CA so broker certs chain
    up to a trust anchor their downstream clients already accept.
    """
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    try:
        cert_pem = ca_cert.file.read()
        key_pem = ca_key.file.read() if ca_key else None
        result = cert_manager.upload_cluster_ca(cluster, cert_pem, key_pem)
        # Audit row so the activity feed shows who uploaded the CA.
        kafka_admin._audit(
            db, cluster_id, "ca_uploaded", "certificate", "cluster_ca",
            details=result.get("subject"), actor=current_user,
        )
        db.commit()
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
