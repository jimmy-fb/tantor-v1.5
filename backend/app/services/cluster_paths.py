"""Per-cluster Kafka path helper.

APB v1.2.0 #5 — running two Tantor-managed clusters on the same broker
host used to collide on a shared `/opt/kafka` symlink + `kafka.service`
unit. Each cluster now owns its own install dir, data dir, and systemd
unit. This module centralizes the resolution so callers don't sprinkle
fallback logic across the codebase.

For clusters created BEFORE these columns existed, the row's
`kafka_install_dir` / `kafka_data_dir` / `kafka_unit_name` are NULL — we
return the legacy defaults so the existing deployment keeps working.

For new clusters, `assign_paths_for_new_cluster` derives unique paths
from the cluster's short id (the first 8 chars of its UUID, which is
already unique within a Tantor deployment).
"""
from __future__ import annotations

import re

from app.config import settings
from app.models.cluster import Cluster


# Default Kafka paths used by every cluster created before per-cluster
# paths were introduced. New clusters get unique paths via
# assign_paths_for_new_cluster() but we still fall back to these so
# legacy rows keep deploying.
DEFAULT_INSTALL_DIR = settings.KAFKA_INSTALL_DIR or "/opt/kafka"
DEFAULT_DATA_DIR = settings.KAFKA_DATA_DIR or "/var/lib/kafka/data"
DEFAULT_UNIT_NAME = "kafka.service"


def _slug(name: str) -> str:
    """Turn a cluster name into a filesystem/unit-safe slug."""
    s = re.sub(r"[^A-Za-z0-9_-]+", "-", name.strip().lower())
    s = re.sub(r"-+", "-", s).strip("-")
    return s[:32] or "cluster"


def assign_paths_for_new_cluster(cluster: Cluster) -> None:
    """Populate kafka_install_dir / kafka_data_dir / kafka_unit_name on a
    freshly-created Cluster row using its short id + slugified name.

    Also rewrites the cluster's config_json `log_dirs` field if it still
    holds the legacy default of /var/lib/kafka/data — without this, two
    clusters on the same broker host would share a Kafka data dir even
    though they have separate install dirs and systemd units.

    Idempotent. Does nothing if the columns are already set.
    """
    if not cluster.id:
        return
    short = cluster.id[:8]
    slug = _slug(cluster.name)
    suffix = f"{slug}-{short}"
    if not cluster.kafka_install_dir:
        cluster.kafka_install_dir = f"/opt/kafka-{suffix}"
    if not cluster.kafka_data_dir:
        cluster.kafka_data_dir = f"/var/lib/kafka-{suffix}/data"
    if not cluster.kafka_unit_name:
        cluster.kafka_unit_name = f"kafka-{suffix}.service"

    # Rewrite log_dirs in config_json if it's the legacy default — the
    # broker config template uses cluster_config["log_dirs"] verbatim.
    import json as _json
    try:
        cfg = _json.loads(cluster.config_json or "{}")
    except Exception:
        cfg = {}
    if cfg.get("log_dirs") in (None, "", "/var/lib/kafka/data"):
        cfg["log_dirs"] = cluster.kafka_data_dir
        cluster.config_json = _json.dumps(cfg)


def install_dir(cluster: Cluster) -> str:
    """Where Kafka binaries live for this cluster."""
    return cluster.kafka_install_dir or DEFAULT_INSTALL_DIR


def data_dir(cluster: Cluster) -> str:
    """Where Kafka log segments + meta.properties live for this cluster."""
    return cluster.kafka_data_dir or DEFAULT_DATA_DIR


def unit_name(cluster: Cluster) -> str:
    """systemd unit name for this cluster's broker."""
    return cluster.kafka_unit_name or DEFAULT_UNIT_NAME


def short_unit_name(cluster: Cluster) -> str:
    """Same as unit_name but without the .service suffix (handy for
    `systemctl <action> <short>` invocations)."""
    return unit_name(cluster).removesuffix(".service")
