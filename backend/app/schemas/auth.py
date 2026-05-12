from datetime import datetime

from pydantic import BaseModel


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    role: str


class TokenRefreshRequest(BaseModel):
    refresh_token: str


class UserCreate(BaseModel):
    username: str
    password: str
    role: str = "monitor"  # admin or monitor


class UserResponse(BaseModel):
    id: str
    username: str
    role: str
    is_active: bool
    auth_source: str = "local"  # v1.4.0 #11 — local | ldap
    ldap_dn: str | None = None
    created_at: datetime
    last_login: datetime | None

    model_config = {"from_attributes": True}


class UserUpdate(BaseModel):
    role: str | None = None
    password: str | None = None
    is_active: bool | None = None
