from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.user import User
from app.schemas.auth import (
    LoginRequest, TokenResponse, TokenRefreshRequest,
    UserCreate, UserResponse, UserUpdate,
)
from app.services.auth_service import AuthService
from app.api.deps import get_current_user, require_admin

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/login", response_model=TokenResponse)
def login(data: LoginRequest, db: Session = Depends(get_db)):
    user = AuthService.authenticate(data.username, data.password, db)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return TokenResponse(
        access_token=AuthService.create_access_token(user.id, user.role),
        refresh_token=AuthService.create_refresh_token(user.id),
        role=user.role,
    )


@router.post("/refresh", response_model=TokenResponse)
def refresh(data: TokenRefreshRequest, db: Session = Depends(get_db)):
    try:
        payload = AuthService.decode_token(data.refresh_token)
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=401, detail="Invalid token type")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")

    user = db.query(User).filter(User.id == payload["sub"]).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or inactive")

    return TokenResponse(
        access_token=AuthService.create_access_token(user.id, user.role),
        refresh_token=AuthService.create_refresh_token(user.id),
        role=user.role,
    )


@router.get("/me", response_model=UserResponse)
def get_me(current_user: User = Depends(get_current_user)):
    return current_user


# --- Admin-only user management ---

@router.get("/users", response_model=list[UserResponse])
def list_users(db: Session = Depends(get_db), _: User = Depends(require_admin)):
    return db.query(User).order_by(User.created_at.desc()).all()


@router.post("/users", response_model=UserResponse, status_code=201)
def create_user(data: UserCreate, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    if data.role not in ("admin", "monitor"):
        raise HTTPException(status_code=400, detail="Role must be 'admin' or 'monitor'")

    existing = db.query(User).filter(User.username == data.username).first()
    if existing:
        raise HTTPException(status_code=409, detail="Username already exists")

    user = User(
        username=data.username,
        hashed_password=AuthService.hash_password(data.password),
        role=data.role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@router.put("/users/{user_id}", response_model=UserResponse)
def update_user(user_id: str, data: UserUpdate, db: Session = Depends(get_db), current_user: User = Depends(require_admin)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if data.role is not None:
        if data.role not in ("admin", "monitor"):
            raise HTTPException(status_code=400, detail="Role must be 'admin' or 'monitor'")
        # Prevent removing admin from yourself
        if user.id == current_user.id and data.role != "admin":
            raise HTTPException(status_code=400, detail="Cannot remove admin role from yourself")
        user.role = data.role

    if data.password is not None:
        # APB v1.4.0 #11 — LDAP-synced users authenticate against the
        # directory; setting a local password would create two paths
        # (one of them shadowing LDAP) so we reject the change here.
        if user.auth_source == "ldap":
            raise HTTPException(
                status_code=400,
                detail="Cannot set a local password on an LDAP-synced user. Change the password in the directory.",
            )
        user.hashed_password = AuthService.hash_password(data.password)

    if data.is_active is not None:
        # Prevent deactivating yourself
        if user.id == current_user.id and not data.is_active:
            raise HTTPException(status_code=400, detail="Cannot deactivate yourself")
        user.is_active = data.is_active

    db.commit()
    db.refresh(user)
    return user


@router.delete("/users/{user_id}")
def delete_user(user_id: str, db: Session = Depends(get_db), current_user: User = Depends(require_admin)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user.id == current_user.id:
        raise HTTPException(status_code=400, detail="Cannot delete yourself")
    db.delete(user)
    db.commit()
    return {"deleted": True, "username": user.username}
