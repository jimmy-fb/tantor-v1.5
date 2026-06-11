from datetime import datetime
from typing import Literal

from pydantic import BaseModel

HostAuthType = Literal["password", "key", "arcos"]


class HostCreate(BaseModel):
    hostname: str
    ip_address: str
    ssh_port: int = 22
    username: str
    auth_type: HostAuthType
    credential: str  # plaintext password, ARCOS token/password, or private key content


class HostUpdate(BaseModel):
    hostname: str | None = None
    ip_address: str | None = None
    ssh_port: int | None = None
    username: str | None = None
    auth_type: HostAuthType | None = None
    credential: str | None = None


class HostResponse(BaseModel):
    id: str
    hostname: str
    ip_address: str
    ssh_port: int
    username: str
    auth_type: HostAuthType
    os_info: str | None
    status: str
    created_at: datetime

    model_config = {"from_attributes": True}


class HostTestResult(BaseModel):
    success: bool
    message: str
    os_info: str | None = None


class PrereqCheck(BaseModel):
    name: str
    status: str  # "pass", "fail", "warn"
    message: str
    details: str | None = None


class PrereqResult(BaseModel):
    host_id: str
    checks: list[PrereqCheck]
    all_passed: bool
