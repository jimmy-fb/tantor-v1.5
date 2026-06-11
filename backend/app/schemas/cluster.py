import re
from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, field_validator, Field


# ---------------------------------------------------------------------------
# Path validation helpers
# ---------------------------------------------------------------------------

# Absolute POSIX path: must start with /, no null bytes, no path traversal
# segments, max 512 chars. We allow alphanumerics, hyphens, underscores, dots,
# and forward slashes. This is intentionally strict — Tantor shells out to
# Ansible which passes these as shell variables. A tainted path is an RCE
# vector on the broker hosts.
_SAFE_PATH_RE = re.compile(r'^/[A-Za-z0-9/_\-\.]{1,510}$')


def _validate_deploy_path(value: str | None, field_name: str) -> str | None:
    """Validate an operator-supplied filesystem path for deploy-time use.

    Returns None (auto-derive) when the value is empty/whitespace.
    Raises ValueError for paths that look dangerous or malformed.
    """
    if not value or not value.strip():
        return None

    path = value.strip()

    if not path.startswith('/'):
        raise ValueError(
            f"{field_name} must be an absolute path (start with '/'); "
            f"got: {path!r}"
        )

    if '..' in path.split('/'):
        raise ValueError(
            f"{field_name} must not contain '..' path traversal components; "
            f"got: {path!r}"
        )

    if not _SAFE_PATH_RE.match(path):
        raise ValueError(
            f"{field_name} contains characters that are not allowed in a "
            f"deploy-time path. Use only alphanumerics, '/', '-', '_', '.'; "
            f"got: {path!r}"
        )

    return path


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class ServiceAssignment(BaseModel):
    host_id: str
    role: str  # broker, controller, broker_controller, zookeeper, ksqldb, kafka_connect
    node_id: int


class ClusterConfig(BaseModel):
    replication_factor: int = 3
    num_partitions: int = 3
    log_dirs: str = "/var/lib/kafka/data"
    listener_port: int = 9092
    controller_port: int = 9093
    heap_size: str = "1G"
    ksqldb_port: int = 8088
    connect_port: int = 8083
    connect_rest_port: int = 8083
    schema_registry_port: int = 8085
    # SSL listener runs alongside PLAINTEXT when ssl_enabled is on. Default
    # avoids 9094 (Alertmanager) and 9095 (commonly used by Kafka REST).
    ssl_listener_port: int = 9096
    
    # Advanced resource and operational configs
    cpu_quota: str | None = None
    memory_max: str | None = None
    retention_hours: int = 168

    # JVM troubleshooting configs
    jvm_performance_opts: str | None = None
    jmx_port: int | None = None
    gc_logging_enabled: bool = False

    # Optional custom Kafka install and data directories.
    #
    # When None (default) assign_paths_for_new_cluster() auto-derives unique
    # paths from the cluster UUID so multiple clusters on the same host
    # coexist cleanly (e.g. /opt/kafka-prod-a1b2c3d4 / kafka-staging-e5f6...).
    #
    # When provided the operator's values are written verbatim to the
    # Cluster row BEFORE auto-derivation runs, so the idempotency guard
    # ("if not cluster.kafka_install_dir") preserves them.
    #
    # Security: both paths are validated against _SAFE_PATH_RE before use
    # because they flow into Ansible as unquoted shell variables.
    kafka_install_dir: Annotated[str | None, Field(default=None, max_length=512)]
    kafka_data_dir: Annotated[str | None, Field(default=None, max_length=512)]

    @field_validator("kafka_install_dir", mode="before")
    @classmethod
    def validate_install_dir(cls, v: str | None) -> str | None:
        return _validate_deploy_path(v, "kafka_install_dir")

    @field_validator("kafka_data_dir", mode="before")
    @classmethod
    def validate_data_dir(cls, v: str | None) -> str | None:
        return _validate_deploy_path(v, "kafka_data_dir")


class InitialAcl(BaseModel):
    """One ACL rule to apply immediately after the cluster is deployed.

    Mirrors AclCreateRequest in schemas/security.py but is intentionally
    a separate model so ClusterCreate stays self-contained and doesn't
    create a circular import with the security schema module.

    The deployer applies these in order after the broker TCP port is
    confirmed reachable. Failures are logged as warnings — they do NOT
    roll back the cluster deployment.
    """
    principal: str          # e.g. "User:myapp" or bare "myapp" (deployer prefixes User:)
    resource_type: str      # "topic" | "group" | "cluster" | "transactional-id"
    resource_name: str      # "*" for wildcard, or a specific name / prefix
    pattern_type: str = "literal"       # "literal" or "prefixed"
    operations: list[str]               # ["Read", "Write", "Describe", ...]
    permission_type: str = "Allow"      # "Allow" or "Deny"
    host: str = "*"                     # source IP filter; "*" = any host


class ClusterCreate(BaseModel):
    name: str
    # Kafka 4.x is KRaft-only. Tantor still accepts the `zookeeper` mode for
    # legacy 3.x deployments but new installs default to 4.1.
    kafka_version: str = "4.1.0"
    mode: str = "kraft"  # "kraft" (4.x supported) or "zookeeper" (3.x only)
    services: list[ServiceAssignment]
    config: ClusterConfig = ClusterConfig()
    environment: str = ""
    # Optional ACLs to apply immediately after the broker is up.
    # The deployer waits for the broker TCP port to become reachable
    # (up to 60 s) before applying these, so a Connection refused on a
    # slow JVM start doesn't silently drop the ACLs.
    # Failures are logged as warnings and do NOT roll back the deployment.
    initial_acls: list[InitialAcl] = []


class ClusterResponse(BaseModel):
    id: str
    name: str
    kafka_version: str
    mode: str
    state: str
    config_json: str | None
    created_at: datetime
    # `managed` (Tantor-deployed) or `external` (connected via bootstrap.servers).
    kind: str = "managed"
    # External clusters expose their connection info here so the UI can render
    # it without round-tripping to /external-clusters. Secrets are NOT included.
    bootstrap_servers: str | None = None
    security_protocol: str | None = None
    # Free-form env tag (dev / qa / prod / "us-east" / etc) — empty string = none.
    environment: str = ""
    # Resolved deploy paths — populated on all managed clusters that were
    # created with per-cluster path support (v1.3.5+). NULL for legacy rows
    # means the cluster uses the shared /opt/kafka defaults.
    kafka_install_dir: str | None = None
    kafka_data_dir: str | None = None
    kafka_unit_name: str | None = None

    model_config = {"from_attributes": True}


class ClusterUpdateRequest(BaseModel):
    """Mutable cluster metadata. Cannot change kafka_version, mode, services here."""
    name: str | None = None
    environment: str | None = None


class ServiceResponse(BaseModel):
    id: str
    cluster_id: str
    host_id: str
    role: str
    node_id: int
    config_overrides: str | None
    status: str

    model_config = {"from_attributes": True}


class ClusterDetailResponse(BaseModel):
    cluster: ClusterResponse
    services: list[ServiceResponse]


class DeploymentTaskResponse(BaseModel):
    task_id: str
    cluster_id: str
    status: str
