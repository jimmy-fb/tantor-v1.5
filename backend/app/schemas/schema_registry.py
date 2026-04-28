"""Schema Registry request/response schemas."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


SchemaType = Literal["AVRO", "JSON", "PROTOBUF"]
CompatibilityLevel = Literal[
    "BACKWARD", "BACKWARD_TRANSITIVE",
    "FORWARD", "FORWARD_TRANSITIVE",
    "FULL", "FULL_TRANSITIVE",
    "NONE",
]


class RegisterSchemaRequest(BaseModel):
    schema_text: str
    schema_type: SchemaType = "AVRO"


class RegisterSchemaResponse(BaseModel):
    id: int


class SchemaVersion(BaseModel):
    subject: str
    version: int
    id: int
    schema_text: str
    schema_type: SchemaType | None = None


class CompatibilityResponse(BaseModel):
    # Apicurio returns either {"compatibility": "..."} or {"compatibilityLevel": "..."}
    compatibility: CompatibilityLevel


class CompatibilityUpdate(BaseModel):
    compatibility: CompatibilityLevel


class RegistryHealthResponse(BaseModel):
    reachable: bool
    url: str | None
    subject_count: int | None = None
