from datetime import datetime
from pydantic import BaseModel


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


class ClusterCreate(BaseModel):
    name: str
    # Kafka 4.x is KRaft-only. Tantor still accepts the `zookeeper` mode for
    # legacy 3.x deployments but new installs default to 4.1.
    kafka_version: str = "4.1.0"
    mode: str = "kraft"  # "kraft" (4.x supported) or "zookeeper" (3.x only)
    services: list[ServiceAssignment]
    config: ClusterConfig = ClusterConfig()


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

    model_config = {"from_attributes": True}


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
