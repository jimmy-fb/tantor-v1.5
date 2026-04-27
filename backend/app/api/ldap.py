import logging

from cryptography.fernet import Fernet
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models.ldap_config import LdapConfig
from app.models.user import User
from app.schemas.ldap import (
    LdapConfigCreate,
    LdapConfigUpdate,
    LdapConfigResponse,
    LdapTestRequest,
    LdapTestResponse,
)
from app.services.ldap_service import LdapService
from app.api.deps import require_admin

logger = logging.getLogger("tantor.ldap")

router = APIRouter(prefix="/api/ldap", tags=["ldap"])


def _get_fernet() -> Fernet:
    return Fernet(settings.FERNET_KEY.encode())


def _to_response(config: LdapConfig) -> LdapConfigResponse:
    """Convert ORM row to response DTO without exposing the CA PEM body."""
    return LdapConfigResponse(
        id=config.id,
        enabled=config.enabled,
        server_url=config.server_url,
        use_ssl=config.use_ssl,
        tls_validate_cert=bool(getattr(config, "tls_validate_cert", True)),
        tls_ca_cert_present=bool(getattr(config, "tls_ca_cert", None)),
        bind_dn=config.bind_dn,
        user_search_base=config.user_search_base,
        user_search_filter=config.user_search_filter,
        group_search_base=config.group_search_base,
        admin_group_dn=config.admin_group_dn,
        monitor_group_dn=config.monitor_group_dn,
        default_role=config.default_role,
        connection_timeout=config.connection_timeout,
    )


# Sentinel sent by the frontend when the operator does not want to overwrite
# the stored bind password during an update.
_UNCHANGED_PASSWORD = "UNCHANGED"


@router.get("/config", response_model=LdapConfigResponse | None)
def get_ldap_config(db: Session = Depends(get_db), _: User = Depends(require_admin)):
    """Get current LDAP configuration (excludes bind password and CA PEM body)."""
    config = db.query(LdapConfig).first()
    if not config:
        return None
    return _to_response(config)


@router.put("/config", response_model=LdapConfigResponse)
def update_ldap_config(
    data: LdapConfigCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Create or update LDAP configuration."""
    fernet = _get_fernet()
    config = db.query(LdapConfig).first()

    if config:
        config.enabled = data.enabled
        config.server_url = data.server_url
        config.use_ssl = data.use_ssl
        config.tls_validate_cert = data.tls_validate_cert
        # Empty string from the UI clears the cert; None leaves it unchanged.
        if data.tls_ca_cert is not None:
            config.tls_ca_cert = data.tls_ca_cert or None
        config.bind_dn = data.bind_dn
        if data.bind_password and data.bind_password != _UNCHANGED_PASSWORD:
            config.encrypted_bind_password = fernet.encrypt(data.bind_password.encode()).decode()
        config.user_search_base = data.user_search_base
        config.user_search_filter = data.user_search_filter
        config.group_search_base = data.group_search_base
        config.admin_group_dn = data.admin_group_dn
        config.monitor_group_dn = data.monitor_group_dn
        config.default_role = data.default_role
        config.connection_timeout = data.connection_timeout
    else:
        if not data.bind_password or data.bind_password == _UNCHANGED_PASSWORD:
            raise HTTPException(status_code=400, detail="bind_password is required for initial configuration")
        config = LdapConfig(
            enabled=data.enabled,
            server_url=data.server_url,
            use_ssl=data.use_ssl,
            tls_validate_cert=data.tls_validate_cert,
            tls_ca_cert=data.tls_ca_cert or None,
            bind_dn=data.bind_dn,
            encrypted_bind_password=fernet.encrypt(data.bind_password.encode()).decode(),
            user_search_base=data.user_search_base,
            user_search_filter=data.user_search_filter,
            group_search_base=data.group_search_base,
            admin_group_dn=data.admin_group_dn,
            monitor_group_dn=data.monitor_group_dn,
            default_role=data.default_role,
            connection_timeout=data.connection_timeout,
        )
        db.add(config)

    db.commit()
    db.refresh(config)
    logger.info(
        "LDAP config updated: enabled=%s, server=%s, ssl=%s, validate_cert=%s, ca_cert_set=%s",
        config.enabled, config.server_url, config.use_ssl,
        config.tls_validate_cert, bool(config.tls_ca_cert),
    )
    return _to_response(config)


@router.post("/test", response_model=LdapTestResponse)
def test_ldap_connection(
    data: LdapTestRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Test LDAP connection and optionally authenticate a test user."""
    config = db.query(LdapConfig).first()
    if not config:
        raise HTTPException(status_code=400, detail="LDAP not configured. Save configuration first.")

    # Decrypt bind password
    fernet = _get_fernet()
    try:
        bind_password = fernet.decrypt(config.encrypted_bind_password.encode()).decode()
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to decrypt bind password")

    # Test service account connection
    conn_result = LdapService.test_connection(config, bind_password)
    if not conn_result["success"]:
        return LdapTestResponse(
            success=False,
            message=conn_result["message"],
        )

    # Test user authentication
    auth_result = LdapService.authenticate(data.username, data.password, config, bind_password)
    if auth_result:
        role = LdapService.determine_role(auth_result.get("groups", []), config)
        return LdapTestResponse(
            success=True,
            message=f"Authentication successful. User: {auth_result['display_name']}, Role: {role}",
            user_dn=auth_result["dn"],
            groups=auth_result.get("groups", []),
        )
    else:
        return LdapTestResponse(
            success=False,
            message="Service account connected successfully, but user authentication failed. Check username/password and search filter.",
        )


@router.post("/sync-users")
def sync_ldap_users(
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Search LDAP directory and return discoverable users."""
    config = db.query(LdapConfig).first()
    if not config:
        raise HTTPException(status_code=400, detail="LDAP not configured")

    fernet = _get_fernet()
    try:
        bind_password = fernet.decrypt(config.encrypted_bind_password.encode()).decode()
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to decrypt bind password")

    users = LdapService.search_users(config, bind_password)
    return {"users": users, "count": len(users)}
