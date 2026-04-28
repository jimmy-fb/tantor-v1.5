"""Schema Registry proxy.

Tantor deploys Apicurio Registry (Apache 2 license) and exposes its
ccompat-v7 endpoint, which is wire-compatible with the Confluent Schema
Registry REST API. Existing Kafka clients that target Confluent SR work
against Apicurio without changes — they just see another Schema Registry.

Why Apicurio over Confluent's Schema Registry:
- Apache 2 vs Confluent Community License — cleaner story for self-hosted
  deployments in regulated industries.
- Single ~80 MB jar (Quarkus runner) vs ~700 MB Confluent Platform tarball.
- Storage backend is the cluster's own Kafka (kafkasql) — no separate DB.
"""
from __future__ import annotations

import json

import httpx
from sqlalchemy.orm import Session

from app.models.cluster import Cluster
from app.models.host import Host
from app.models.service import Service


class SchemaRegistryManager:
    """HTTP proxy to Apicurio's Confluent-compatible /apis/ccompat/v7 endpoint."""

    # Apicurio surfaces the SR-compatible REST API at this prefix.
    CCOMPAT_PREFIX = "/apis/ccompat/v7"

    @staticmethod
    def _get_base_url(cluster_id: str, db: Session) -> str:
        svc = db.query(Service).filter(
            Service.cluster_id == cluster_id,
            Service.role == "schema_registry",
        ).first()
        if not svc:
            raise ValueError("No Schema Registry service found in this cluster")

        host = db.query(Host).filter(Host.id == svc.host_id).first()
        if not host:
            raise ValueError("Schema Registry host not found")

        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        config = json.loads(cluster.config_json) if cluster and cluster.config_json else {}
        port = config.get("schema_registry_port", 8085)
        return f"http://{host.ip_address}:{port}{SchemaRegistryManager.CCOMPAT_PREFIX}"

    # ── Subjects ──────────────────────────────────────────────────────

    @staticmethod
    def list_subjects(cluster_id: str, db: Session) -> list[str]:
        url = SchemaRegistryManager._get_base_url(cluster_id, db)
        resp = httpx.get(f"{url}/subjects", timeout=10)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def list_versions(cluster_id: str, subject: str, db: Session) -> list[int]:
        url = SchemaRegistryManager._get_base_url(cluster_id, db)
        resp = httpx.get(f"{url}/subjects/{subject}/versions", timeout=10)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def get_schema(cluster_id: str, subject: str, version: str, db: Session) -> dict:
        """`version` is an int or the literal 'latest'."""
        url = SchemaRegistryManager._get_base_url(cluster_id, db)
        resp = httpx.get(f"{url}/subjects/{subject}/versions/{version}", timeout=10)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def register_schema(cluster_id: str, subject: str, schema: str, schema_type: str | None, db: Session) -> dict:
        url = SchemaRegistryManager._get_base_url(cluster_id, db)
        body: dict = {"schema": schema}
        if schema_type:
            body["schemaType"] = schema_type  # AVRO | JSON | PROTOBUF
        resp = httpx.post(
            f"{url}/subjects/{subject}/versions",
            json=body,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def delete_subject(cluster_id: str, subject: str, db: Session) -> list[int]:
        url = SchemaRegistryManager._get_base_url(cluster_id, db)
        resp = httpx.delete(f"{url}/subjects/{subject}", timeout=15)
        resp.raise_for_status()
        return resp.json()

    # ── Compatibility ─────────────────────────────────────────────────

    @staticmethod
    def get_global_compatibility(cluster_id: str, db: Session) -> dict:
        url = SchemaRegistryManager._get_base_url(cluster_id, db)
        resp = httpx.get(f"{url}/config", timeout=10)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def set_global_compatibility(cluster_id: str, level: str, db: Session) -> dict:
        url = SchemaRegistryManager._get_base_url(cluster_id, db)
        resp = httpx.put(f"{url}/config", json={"compatibility": level}, timeout=10)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def get_subject_compatibility(cluster_id: str, subject: str, db: Session) -> dict:
        url = SchemaRegistryManager._get_base_url(cluster_id, db)
        resp = httpx.get(f"{url}/config/{subject}", timeout=10)
        if resp.status_code == 404:
            # Apicurio returns 404 when no subject-level config is set; fall back.
            return SchemaRegistryManager.get_global_compatibility(cluster_id, db)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def set_subject_compatibility(cluster_id: str, subject: str, level: str, db: Session) -> dict:
        url = SchemaRegistryManager._get_base_url(cluster_id, db)
        resp = httpx.put(f"{url}/config/{subject}", json={"compatibility": level}, timeout=10)
        resp.raise_for_status()
        return resp.json()

    # ── Health ────────────────────────────────────────────────────────

    @staticmethod
    def reachable(cluster_id: str, db: Session) -> tuple[bool, str | None]:
        try:
            url = SchemaRegistryManager._get_base_url(cluster_id, db)
        except ValueError:
            return False, None
        try:
            resp = httpx.get(f"{url}/subjects", timeout=3)
            return resp.status_code == 200, url
        except Exception:
            return False, url
