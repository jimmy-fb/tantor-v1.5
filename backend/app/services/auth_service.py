import logging
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt
from cryptography.fernet import Fernet
from sqlalchemy.orm import Session

from app.config import settings
from app.models.user import User
from app.models.ldap_config import LdapConfig
from app.services.ldap_service import LdapService

logger = logging.getLogger("tantor.auth")


class AuthService:
    """Handles password hashing, JWT creation/validation, and user management."""

    @staticmethod
    def hash_password(password: str) -> str:
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    @staticmethod
    def verify_password(password: str, hashed: str) -> bool:
        return bcrypt.checkpw(password.encode(), hashed.encode())

    @staticmethod
    def create_access_token(user_id: str, role: str, token_version: int = 0) -> str:
        payload = {
            "sub": user_id,
            "role": role,
            "type": "access",
            # v1.4.3 #22 — embedded token_version. Auth dep rejects
            # tokens whose tv != user.token_version, which means an
            # admin role-change bumps token_version and forces re-login.
            "tv": token_version,
            "exp": datetime.now(timezone.utc) + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
        }
        return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm="HS256")

    @staticmethod
    def create_refresh_token(user_id: str, token_version: int = 0) -> str:
        payload = {
            "sub": user_id,
            "type": "refresh",
            "tv": token_version,
            "exp": datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS),
        }
        return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm="HS256")

    @staticmethod
    def decode_token(token: str) -> dict:
        return jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=["HS256"])

    @staticmethod
    def authenticate_ldap(username: str, password: str, db: Session) -> User | None:
        """Try to authenticate via LDAP. Returns User or None."""
        ldap_config = db.query(LdapConfig).first()
        if not ldap_config or not ldap_config.enabled:
            return None

        # Decrypt bind password
        try:
            fernet = Fernet(settings.FERNET_KEY.encode())
            bind_password = fernet.decrypt(ldap_config.encrypted_bind_password.encode()).decode()
        except Exception:
            logger.error("Failed to decrypt LDAP bind password")
            return None

        result = LdapService.authenticate(username, password, ldap_config, bind_password)
        if not result:
            return None

        # Determine role from group membership
        role = LdapService.determine_role(result.get("groups", []), ldap_config)

        # Find or create local user record. v1.4.0 #11 — store the
        # LDAP DN so the User Management page can hide the password
        # change UI and label the row with its source.
        ldap_dn = result.get("dn")
        user = db.query(User).filter(User.username == username).first()
        if user:
            # Update role from LDAP groups and mark as LDAP user
            user.role = role
            user.auth_source = "ldap"
            if ldap_dn:
                user.ldap_dn = ldap_dn
            user.last_login = datetime.now(timezone.utc)
            db.commit()
            return user
        else:
            # Create new user from LDAP
            user = User(
                username=username,
                hashed_password="LDAP_AUTH",  # placeholder, not used for LDAP users
                role=role,
                auth_source="ldap",
                ldap_dn=ldap_dn,
                last_login=datetime.now(timezone.utc),
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            return user

    @staticmethod
    def authenticate(username: str, password: str, db: Session) -> User | None:
        # Try LDAP first
        ldap_user = AuthService.authenticate_ldap(username, password, db)
        if ldap_user:
            return ldap_user

        # Fall back to local authentication
        user = db.query(User).filter(User.username == username, User.is_active == True).first()  # noqa: E712
        if user and user.auth_source == "ldap":
            # LDAP users should not authenticate locally
            return None
        if user and AuthService.verify_password(password, user.hashed_password):
            user.last_login = datetime.now(timezone.utc)
            db.commit()
            return user
        return None

    @staticmethod
    def create_default_admin(db: Session):
        """Create default admin user if no users exist. Called on startup."""
        if db.query(User).count() == 0:
            admin = User(
                username="admin",
                hashed_password=AuthService.hash_password("admin"),
                role="admin",
            )
            db.add(admin)
            db.commit()
            logger.warning("Default admin user created (username: admin, password: admin). Change the password immediately!")
