from datetime import datetime

from pydantic import BaseModel


# ── Kafka Users (SCRAM) ────────────────────────────

class KafkaUserCreate(BaseModel):
    username: str
    password: str | None = None  # None = auto-generate
    mechanism: str = "SCRAM-SHA-256"  # "SCRAM-SHA-256" or "SCRAM-SHA-512"


class KafkaUserResponse(BaseModel):
    id: str
    cluster_id: str
    username: str
    mechanism: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class KafkaUserCreatedResponse(BaseModel):
    id: str
    username: str
    mechanism: str
    password: str  # Plaintext, shown once
    message: str


class KafkaUserRotateRequest(BaseModel):
    password: str | None = None  # None = auto-generate new password


class KafkaUserRotateResponse(BaseModel):
    username: str
    mechanism: str
    password: str  # New plaintext password, shown once
    message: str


class KafkaUserDeleteResponse(BaseModel):
    username: str
    deleted: bool
    message: str


# ── Kafka ACLs ──────────────────────────────────────

class AclEntry(BaseModel):
    principal: str          # "User:myuser"
    resource_type: str      # "topic", "group", "cluster", "transactional-id"
    resource_name: str      # topic name, group name, "kafka-cluster"
    pattern_type: str       # "literal" or "prefixed"
    operation: str          # "Read", "Write", "Create", "Describe", etc.
    permission_type: str    # "Allow" or "Deny"
    host: str = "*"


class AclCreateRequest(BaseModel):
    principal: str              # "User:myuser"
    resource_type: str          # "topic", "group", "cluster"
    resource_name: str          # e.g. "my-topic", "*"
    pattern_type: str = "literal"  # "literal" or "prefixed"
    operations: list[str]       # ["Read", "Describe"], etc.
    permission_type: str = "Allow"  # "Allow" or "Deny"
    host: str = "*"


class AclCreateResponse(BaseModel):
    success: bool
    message: str
    acls_added: int


class AclDeleteRequest(BaseModel):
    principal: str
    resource_type: str
    resource_name: str
    pattern_type: str = "literal"
    operations: list[str]
    permission_type: str = "Allow"
    host: str = "*"


class AclDeleteResponse(BaseModel):
    success: bool
    message: str


class AclListResponse(BaseModel):
    acls: list[AclEntry]
    count: int


# ── Audit Log ────────────────────────────────────────

class AuditLogEntry(BaseModel):
    id: str
    cluster_id: str
    action: str
    resource_type: str
    resource_name: str
    details: str | None
    actor_username: str | None = None  # v1.4.0 #13
    created_at: datetime

    model_config = {"from_attributes": True}
