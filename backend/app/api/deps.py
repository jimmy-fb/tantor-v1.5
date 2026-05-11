from fastapi import Depends, HTTPException, status, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
import jwt

from app.database import get_db
from app.models.user import User
from app.services.auth_service import AuthService

security = HTTPBearer()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
) -> User:
    """Extracts and validates JWT from Authorization header.

    APB v1.4.3 #22 — also verifies token_version matches the row.
    When an admin changes a user's role / deactivates them, we bump
    user.token_version so existing JWTs (carrying the old version) are
    rejected on next API call, forcing a fresh login.
    """
    try:
        payload = AuthService.decode_token(credentials.credentials)
        if payload.get("type") != "access":
            raise HTTPException(status_code=401, detail="Invalid token type")
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

    user = db.query(User).filter(User.id == payload["sub"]).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or inactive")
    # Token version gate. Old tokens (no tv claim) default to 0 — for
    # existing fresh-install users with token_version=0 they still
    # work; once an admin bumps the version, all old tokens are dead.
    expected = getattr(user, "token_version", 0) or 0
    got = payload.get("tv", 0) or 0
    if got != expected:
        raise HTTPException(
            status_code=401,
            detail="Token revoked — your account's role or status changed. Please log in again.",
        )
    return user


def require_admin(current_user: User = Depends(get_current_user)) -> User:
    """Only admin role can access."""
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


def require_monitor_or_above(current_user: User = Depends(get_current_user)) -> User:
    """Both admin and monitor roles can access."""
    return current_user


def get_ws_user(token: str, db: Session) -> User | None:
    """Validate a token passed as WebSocket query parameter. Returns User or None."""
    if not token:
        return None
    try:
        payload = AuthService.decode_token(token)
        if payload.get("type") != "access":
            return None
        user = db.query(User).filter(User.id == payload["sub"]).first()
        if not user or not user.is_active:
            return None
        return user
    except Exception:
        return None
