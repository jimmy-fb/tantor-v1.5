"""Per-cluster Kafka path helper.

v1.2.0 #5 — running two Tantor-managed clusters on the same broker
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

Custom paths (v1.4.5): operators may supply explicit install/data dirs
during cluster creation. Those values are written to the Cluster row
BEFORE this function runs, so the idempotency guards below preserve them.
The function still auto-derives the systemd unit name from the cluster
slug+id regardless — operators have no reason to override that, and
keeping it predictable makes cleanup/purge logic reliable.
"""
from __future__ import annotations

import json as _json
import logging
import re

from app.config import settings
from app.models.cluster import Cluster

logger = logging.getLogger("tantor.cluster_paths")

# Default Kafka paths used by every cluster created before per-cluster
# paths were introduced. New clusters get unique paths via
# assign_paths_for_new_cluster() but we still fall back to these so
# legacy rows keep deploying.
DEFAULT_INSTALL_DIR = settings.KAFKA_INSTALL_DIR or "/opt/kafka"
DEFAULT_DATA_DIR = settings.KAFKA_DATA_DIR or "/var/lib/kafka/data"
DEFAULT_UNIT_NAME = "kafka.service"


def _slug(name: str) -> str:
    """Turn a cluster name into a filesystem/unit-safe slug (max 32 chars)."""
    s = re.sub(r"[^A-Za-z0-9_-]+", "-", name.strip().lower())
    s = re.sub(r"-+", "-", s).strip("-")
    return s[:32] or "cluster"


def assign_paths_for_new_cluster(cluster: Cluster) -> None:
    """Populate kafka_install_dir / kafka_data_dir / kafka_unit_name on a
    freshly-created Cluster row.

    The function is idempotent: any column that is already set (e.g. because
    the operator supplied a custom path at creation time) is left untouched.
    Only missing columns get auto-derived from the cluster's UUID short id
    and slugified name, ensuring uniqueness across clusters on the same host.

    Also synchronises config_json["log_dirs"] with kafka_data_dir so that
    the broker `server.properties` template receives the correct path. The
    sync runs whenever log_dirs still holds the legacy default value OR
    whenever a custom kafka_data_dir was explicitly set (the operator almost
    certainly wants log_dirs to match their data dir). If the operator has
    already set log_dirs to a completely different value from kafka_data_dir
    we leave it alone — they may be intentionally splitting data and log
    segments across different mounts.
    """
    if not cluster.id:
        return

    short = cluster.id[:8]
    slug = _slug(cluster.name)
    suffix = f"{slug}-{short}"

    custom_install_dir = bool(cluster.kafka_install_dir)
    custom_data_dir = bool(cluster.kafka_data_dir)

    if not cluster.kafka_install_dir:
        cluster.kafka_install_dir = f"/opt/kafka-{suffix}"
    if not cluster.kafka_data_dir:
        cluster.kafka_data_dir = f"/var/lib/kafka-{suffix}/data"
    # Always auto-derive the unit name from slug+id regardless of whether the
    # operator supplied custom paths — keeping unit names predictable is
    # critical for cleanup, rolling restart, and upgrade code paths.
    if not cluster.kafka_unit_name:
        cluster.kafka_unit_name = f"kafka-{suffix}.service"

    # ── Synchronise log_dirs in config_json ──────────────────────────────
    # The broker server.properties template uses cluster_config["log_dirs"]
    # verbatim. We want log_dirs to match kafka_data_dir in two cases:
    #   1. log_dirs is still the legacy default sentinel — must be updated or
    #      two clusters on the same host would share the same data directory.
    #   2. The operator supplied an explicit kafka_data_dir — they almost
    #      certainly want log_dirs to agree with it (and the UI sends log_dirs
    #      as the same value as kafka_data_dir when the feature is used).
    try:
        cfg = _json.loads(cluster.config_json or "{}")
    except Exception:
        cfg = {}

    legacy_sentinel = "/var/lib/kafka/data"
    current_log_dirs = cfg.get("log_dirs")

    should_sync = (
        current_log_dirs in (None, "", legacy_sentinel)  # case 1
        or custom_data_dir                                # case 2
    )

    if should_sync and current_log_dirs != cluster.kafka_data_dir:
        cfg["log_dirs"] = cluster.kafka_data_dir
        cluster.config_json = _json.dumps(cfg)
        if custom_install_dir or custom_data_dir:
            logger.debug(
                "Cluster %s: synced log_dirs to custom kafka_data_dir %s",
                cluster.id[:8],
                cluster.kafka_data_dir,
            )


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
