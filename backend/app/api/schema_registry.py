"""Schema Registry API — proxies to the cluster's deployed Apicurio instance."""
from __future__ import annotations

import logging

import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import require_admin, require_monitor_or_above
from app.database import get_db
from app.models.user import User
from app.schemas.schema_registry import (
    CompatibilityResponse,
    CompatibilityUpdate,
    RegisterSchemaRequest,
    RegisterSchemaResponse,
    RegistryHealthResponse,
    SchemaVersion,
)
from app.services.schema_registry_manager import SchemaRegistryManager

logger = logging.getLogger("tantor.schema_registry.api")

router = APIRouter(prefix="/api/clusters/{cluster_id}/schema-registry", tags=["schema-registry"])


def _wrap(call):
    """Translate Apicurio HTTP errors into Tantor HTTPException with the upstream message."""
    try:
        return call()
    except ValueError as e:
        # Raised by manager when cluster has no SR service.
        raise HTTPException(status_code=400, detail=str(e))
    except httpx.HTTPStatusError as e:
        body = e.response.text[:500] if e.response is not None else ""
        raise HTTPException(
            status_code=e.response.status_code if e.response is not None else 502,
            detail=f"Schema Registry: {body or str(e)}",
        )
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Schema Registry unreachable: {e}")


@router.get("/health", response_model=RegistryHealthResponse)
def health(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    reachable, url = SchemaRegistryManager.reachable(cluster_id, db)
    subject_count = None
    if reachable:
        try:
            subject_count = len(SchemaRegistryManager.list_subjects(cluster_id, db))
        except Exception:
            subject_count = None
    return RegistryHealthResponse(reachable=reachable, url=url, subject_count=subject_count)


# ── Subjects ──────────────────────────────────────────────────────────────


@router.get("/subjects", response_model=list[str])
def list_subjects(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    return _wrap(lambda: SchemaRegistryManager.list_subjects(cluster_id, db))


@router.get("/subjects/{subject}/versions", response_model=list[int])
def list_versions(cluster_id: str, subject: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    return _wrap(lambda: SchemaRegistryManager.list_versions(cluster_id, subject, db))


@router.get("/subjects/{subject}/versions/{version}", response_model=SchemaVersion)
def get_schema(cluster_id: str, subject: str, version: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    raw = _wrap(lambda: SchemaRegistryManager.get_schema(cluster_id, subject, version, db))
    return SchemaVersion(
        subject=raw.get("subject", subject),
        version=int(raw.get("version", 0)),
        id=int(raw.get("id", 0)),
        schema_text=raw.get("schema", ""),
        schema_type=raw.get("schemaType"),
    )


@router.post("/subjects/{subject}/versions", response_model=RegisterSchemaResponse)
def register_schema(
    cluster_id: str,
    subject: str,
    body: RegisterSchemaRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    raw = _wrap(lambda: SchemaRegistryManager.register_schema(
        cluster_id, subject, body.schema_text, body.schema_type, db,
    ))
    return RegisterSchemaResponse(id=int(raw["id"]))


@router.delete("/subjects/{subject}", response_model=list[int])
def delete_subject(cluster_id: str, subject: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    return _wrap(lambda: SchemaRegistryManager.delete_subject(cluster_id, subject, db))


# ── Compatibility ─────────────────────────────────────────────────────────


@router.get("/config", response_model=CompatibilityResponse)
def get_global_compat(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    raw = _wrap(lambda: SchemaRegistryManager.get_global_compatibility(cluster_id, db))
    return CompatibilityResponse(compatibility=raw.get("compatibilityLevel") or raw.get("compatibility", "NONE"))


@router.put("/config", response_model=CompatibilityResponse)
def set_global_compat(
    cluster_id: str,
    body: CompatibilityUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    _wrap(lambda: SchemaRegistryManager.set_global_compatibility(cluster_id, body.compatibility, db))
    return CompatibilityResponse(compatibility=body.compatibility)


@router.get("/config/{subject}", response_model=CompatibilityResponse)
def get_subject_compat(cluster_id: str, subject: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    raw = _wrap(lambda: SchemaRegistryManager.get_subject_compatibility(cluster_id, subject, db))
    return CompatibilityResponse(compatibility=raw.get("compatibilityLevel") or raw.get("compatibility", "NONE"))


@router.put("/config/{subject}", response_model=CompatibilityResponse)
def set_subject_compat(
    cluster_id: str,
    subject: str,
    body: CompatibilityUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    _wrap(lambda: SchemaRegistryManager.set_subject_compatibility(cluster_id, subject, body.compatibility, db))
    return CompatibilityResponse(compatibility=body.compatibility)
