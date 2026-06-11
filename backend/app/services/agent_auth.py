"""Token helpers for the agent: registration tokens (one-shot) and the
long-lived agent JWT issued at first connect.

Registration tokens look like:
    tat_<host_short>_<32-char-random>
The Tantor UI mints one when an admin clicks "Generate agent token" on a
Host row. The token is stored hashed in agents.registration_token_hash;
we never persist the plaintext. The plaintext is shown to the admin once.

The long-lived agent JWT carries:
    {"sub": agent_id, "host_id": host_id, "type": "agent", "tv": token_version}
and lasts 1 year. token_version on the Agent row works the same way as
User.token_version (v1.4.3 #22) — bump it to invalidate every JWT issued
so far.
"""
from __future__ import annotations

import bcrypt
import logging
import secrets
from datetime import datetime, timedelta, timezone

import jwt

from app.config import settings

logger = logging.getLogger("tantor.agent_auth")

REG_TOKEN_PREFIX = "tat_"
REG_TOKEN_TTL_SEC = 3600  # 1 hour to first-connect

AGENT_JWT_TTL_DAYS = 365


def mint_registration_token(host_short: str) -> tuple[str, str, datetime]:
    """Return (plaintext, bcrypt_hash, expires_at).

    Plaintext is shown to the admin ONCE; bcrypt_hash is what we persist.
    """
    rand = secrets.token_urlsafe(32).rstrip("=")
    plaintext = f"{REG_TOKEN_PREFIX}{host_short}_{rand}"
    hashed = bcrypt.hashpw(plaintext.encode(), bcrypt.gensalt()).decode()
    expires = datetime.now(timezone.utc) + timedelta(seconds=REG_TOKEN_TTL_SEC)
    return plaintext, hashed, expires


def verify_registration_token(plaintext: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plaintext.encode(), hashed.encode())
    except Exception:
        return False


def issue_agent_jwt(agent_id: str, host_id: str, token_version: int = 0) -> str:
    payload = {
        "sub": agent_id,
        "host_id": host_id,
        "type": "agent",
        "tv": token_version,
        "exp": datetime.now(timezone.utc) + timedelta(days=AGENT_JWT_TTL_DAYS),
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm="HS256")


def decode_agent_jwt(token: str) -> dict:
    """Decode and validate the JWT signature + type claim. Returns the
    payload. Raises jwt.* on failure."""
    payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=["HS256"])
    if payload.get("type") != "agent":
        raise jwt.InvalidTokenError("not an agent token")
    return payload


def looks_like_reg_token(value: str) -> bool:
    return value.startswith(REG_TOKEN_PREFIX)
