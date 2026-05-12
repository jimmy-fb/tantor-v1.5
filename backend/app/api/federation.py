"""Data Federation API.

customerrequested feature: a single pane of glass that gives unified visibility
across every cluster Tantor manages, both Tantor-deployed (managed) and
imported (external). Returns one row per cluster with topic count, broker
count, lifecycle state, and environment tag, plus a global cross-cluster
search that finds a topic name in any cluster.

This is intentionally aggregation-only — no admin actions, no fan-out
deletes — so a single read doesn't risk multi-cluster damage.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import SessionLocal, get_db
from app.models.cluster import Cluster
from app.models.service import Service
from app.models.user import User
from app.api.deps import require_monitor_or_above
from app.services import kafka_admin

logger = logging.getLogger("tantor.federation")
router = APIRouter(prefix="/api/federation", tags=["federation"])


# Topic-count cache. customer: "Data Federation takes excessive time to load".
# Per-cluster list_topics is the slow path (each call opens an admin
# client + waits on metadata). We cache for 30s and fan out parallel
# probes for any clusters whose count isn't cached. Manual refresh via
# `?force=1` blows the cache.
_TOPIC_COUNT_CACHE: dict[str, tuple[float, int | None]] = {}
_TOPIC_COUNT_TTL = 30.0
_TOPIC_COUNT_LOCK = threading.Lock()

# v1.4.0 #1 — also cache an external cluster's broker count so the
# Federation overview shows real numbers instead of "—" for imported
# clusters. Same TTL + lock as topic count.
_EXTERNAL_BROKER_COUNT: dict[str, tuple[float, int | None]] = {}


def _fetch_topic_count(cluster_id: str) -> tuple[str, int | None]:
    """Run in a worker thread. Opens its own DB session — never reuses
    the request's session, since SQLAlchemy sessions are not thread-safe.

    For external clusters, opportunistically capture the broker count
    too (kafka-python's list_topics roundtrip already includes the
    metadata response so adding a describe_cluster is one round-trip).
    """
    db = SessionLocal()
    try:
        topics = kafka_admin.KafkaAdmin.list_topics(cluster_id, db)
        # Capture broker count for external clusters opportunistically
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if cluster and (cluster.kind or "managed") == "external":
            try:
                from app.services import external_admin
                desc = external_admin.test_connection(cluster)
                if desc.get("success"):
                    with _TOPIC_COUNT_LOCK:
                        _EXTERNAL_BROKER_COUNT[cluster_id] = (time.time(), desc.get("broker_count"))
            except Exception:
                pass
        return cluster_id, len(topics)
    except Exception as e:
        logger.debug("federation: list_topics failed for %s: %s", cluster_id, e)
        return cluster_id, None
    finally:
        db.close()


@router.get("/overview")
def federation_overview(
    force: bool = Query(False, description="Bypass the 30s topic-count cache"),
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    """One row per cluster — name, kind, state, environment, broker count,
    topic count, bootstrap. Topic count is fetched in parallel across all
    reachable clusters with a 30s in-memory cache, so federation page
    loads in O(slowest broker) instead of O(sum of all brokers).
    """
    clusters = db.query(Cluster).order_by(Cluster.created_at.desc()).all()

    # Decide which clusters need a fresh topic-count probe
    now = time.time()
    if force:
        with _TOPIC_COUNT_LOCK:
            _TOPIC_COUNT_CACHE.clear()

    to_probe: list[str] = []
    cached: dict[str, int | None] = {}
    with _TOPIC_COUNT_LOCK:
        for c in clusters:
            if (c.state or "").lower() not in ("running", "connected"):
                cached[c.id] = None
                continue
            entry = _TOPIC_COUNT_CACHE.get(c.id)
            if entry and (now - entry[0]) < _TOPIC_COUNT_TTL:
                cached[c.id] = entry[1]
            else:
                to_probe.append(c.id)

    # Parallel fan-out — capped at 8 concurrent so we don't smoke the box
    if to_probe:
        with ThreadPoolExecutor(max_workers=min(8, len(to_probe))) as ex:
            for cid, count in ex.map(_fetch_topic_count, to_probe):
                cached[cid] = count
                with _TOPIC_COUNT_LOCK:
                    _TOPIC_COUNT_CACHE[cid] = (time.time(), count)

    # Pre-fetch service rows for all managed clusters in one query
    managed_ids = [c.id for c in clusters if (c.kind or "managed") == "managed"]
    services_by_cluster: dict[str, int] = {}
    if managed_ids:
        all_services = db.query(Service).filter(Service.cluster_id.in_(managed_ids)).all()
        for svc in all_services:
            if "broker" in (svc.role or ""):
                services_by_cluster[svc.cluster_id] = services_by_cluster.get(svc.cluster_id, 0) + 1

    rows = []
    for c in clusters:
        # v1.4.0 #1 — external clusters now get a real broker count
        # from the cached metadata probe instead of always "—".
        if (c.kind or "managed") == "managed":
            broker_count: int | None = services_by_cluster.get(c.id, 0)
        else:
            ext_entry = _EXTERNAL_BROKER_COUNT.get(c.id)
            if ext_entry and (now - ext_entry[0]) < _TOPIC_COUNT_TTL:
                broker_count = ext_entry[1]
            else:
                # Fall back to the bootstrap_servers count — better than null.
                bs = (c.bootstrap_servers or "").strip()
                broker_count = len([s for s in bs.split(",") if s.strip()]) if bs else None
        rows.append({
            "id": c.id,
            "name": c.name,
            "kind": c.kind or "managed",
            "state": c.state,
            "environment": c.environment or "",
            "kafka_version": c.kafka_version,
            "mode": c.mode,
            "broker_count": broker_count,
            "topic_count": cached.get(c.id),
            "bootstrap_servers": c.bootstrap_servers,
            "created_at": c.created_at,
        })
    return {
        "clusters": rows,
        "total": len(rows),
        "managed": sum(1 for r in rows if r["kind"] == "managed"),
        "external": sum(1 for r in rows if r["kind"] == "external"),
    }


def _search_one(cluster_id: str, needle: str) -> tuple[str, list[dict] | None, str | None]:
    """Worker — list a cluster's topics and filter by needle. Each thread
    gets its own SessionLocal because SQLAlchemy sessions are not
    thread-safe."""
    db = SessionLocal()
    try:
        topics = kafka_admin.KafkaAdmin.list_topics(cluster_id, db)
        return cluster_id, [t for t in topics if needle in (t.get("name") or "").lower()], None
    except Exception as e:
        return cluster_id, None, str(e)[:120]
    finally:
        db.close()


@router.get("/topics/search")
def federation_topic_search(
    q: str = Query(..., min_length=1, description="Topic name substring to search for"),
    db: Session = Depends(get_db),
    _: User = Depends(require_monitor_or_above),
):
    """Search every reachable cluster for topics whose name contains `q`.

    Parallelised — each cluster is probed in its own thread so the search
    is bounded by the slowest cluster, not the sum.
    """
    clusters = db.query(Cluster).all()
    matches: list[dict] = []
    skipped: list[dict] = []
    needle = q.lower()

    reachable = [c for c in clusters if (c.state or "").lower() in ("running", "connected")]
    cluster_lookup = {c.id: c for c in clusters}
    for c in clusters:
        if (c.state or "").lower() not in ("running", "connected"):
            skipped.append({"cluster_id": c.id, "name": c.name, "reason": f"state={c.state}"})

    if reachable:
        with ThreadPoolExecutor(max_workers=min(8, len(reachable))) as ex:
            futures = [ex.submit(_search_one, c.id, needle) for c in reachable]
            for fut in futures:
                cid, hits, err = fut.result()
                c = cluster_lookup[cid]
                if hits is None:
                    skipped.append({"cluster_id": c.id, "name": c.name, "reason": err or "unknown"})
                    continue
                for t in hits:
                    matches.append({
                        "cluster_id": c.id,
                        "cluster_name": c.name,
                        "cluster_kind": c.kind or "managed",
                        "environment": c.environment or "",
                        "topic": t.get("name"),
                        "partitions": t.get("partitions"),
                        "replication_factor": t.get("replication_factor"),
                    })
    return {
        "query": q,
        "matches": matches,
        "match_count": len(matches),
        "skipped": skipped,
    }
