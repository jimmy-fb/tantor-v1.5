from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.host import Host
from app.schemas.host import HostCreate, HostUpdate, HostResponse, HostTestResult, PrereqResult
from app.services.crypto import encrypt
from app.services.ssh_manager import ssh_manager
from app.services.prereq_checker import prereq_checker
from app.api.deps import require_admin, require_monitor_or_above
from app.models.user import User

router = APIRouter(prefix="/api/hosts", tags=["hosts"])


@router.post("", response_model=HostResponse)
def create_host(host_data: HostCreate, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    # Agent-mode hosts don't need SSH credentials. We still write a
    # placeholder so the encrypted_credential column (NOT NULL on
    # existing installs) stays satisfied; SSH paths never touch it for
    # agent hosts because auth_type == "agent" is checked first.
    credential_blob = host_data.credential if host_data.credential else "__agent__"
    host = Host(
        hostname=host_data.hostname,
        ip_address=host_data.ip_address,
        ssh_port=host_data.ssh_port,
        username=host_data.username or "tantor-agent",
        auth_type=host_data.auth_type,
        encrypted_credential=encrypt(credential_blob),
    )
    db.add(host)
    db.commit()
    db.refresh(host)
    return host


@router.get("", response_model=list[HostResponse])
def list_hosts(db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    return db.query(Host).order_by(Host.created_at.desc()).all()


@router.get("/{host_id}", response_model=HostResponse)
def get_host(host_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    host = db.query(Host).filter(Host.id == host_id).first()
    if not host:
        raise HTTPException(status_code=404, detail="Host not found")
    return host


@router.put("/{host_id}", response_model=HostResponse)
def update_host(host_id: str, host_data: HostUpdate, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    host = db.query(Host).filter(Host.id == host_id).first()
    if not host:
        raise HTTPException(status_code=404, detail="Host not found")

    if host_data.hostname is not None:
        host.hostname = host_data.hostname
    if host_data.ip_address is not None:
        host.ip_address = host_data.ip_address
    if host_data.ssh_port is not None:
        host.ssh_port = host_data.ssh_port
    if host_data.username is not None:
        host.username = host_data.username
    if host_data.auth_type is not None:
        host.auth_type = host_data.auth_type
    if host_data.credential is not None:
        host.encrypted_credential = encrypt(host_data.credential)

    db.commit()
    db.refresh(host)
    return host


@router.delete("/{host_id}")
def delete_host(host_id: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    host = db.query(Host).filter(Host.id == host_id).first()
    if not host:
        raise HTTPException(status_code=404, detail="Host not found")
    db.delete(host)
    db.commit()
    return {"detail": "Host deleted"}


@router.post("/{host_id}/test", response_model=HostTestResult)
def test_host_connection(host_id: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    host = db.query(Host).filter(Host.id == host_id).first()
    if not host:
        raise HTTPException(status_code=404, detail="Host not found")

    # Agent-mode hosts don't have SSH credentials — "is the agent
    # connected and heartbeating?" IS the test. Reuses the in-memory
    # registry the WS endpoint maintains; no SSH attempt is made.
    if host.auth_type == "agent":
        from app.services import agent_transport
        if agent_transport.agent_available(host.id):
            host.status = "online"
            db.commit()
            return HostTestResult(
                success=True,
                message="tantor-agent is connected and heartbeating",
                os_info=host.os_info,
            )
        host.status = "offline"
        db.commit()
        return HostTestResult(
            success=False,
            message=(
                "No tantor-agent connected for this host. Mint a token via "
                "POST /api/hosts/{id}/agent/token and run the install command "
                "on the broker host."
            ),
            os_info=host.os_info,
        )

    success, message, os_info = ssh_manager.test_connection(
        host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential
    )

    if success and os_info:
        host.os_info = os_info
        host.status = "online"
    elif not success:
        host.status = "offline"
    db.commit()

    return HostTestResult(success=success, message=message, os_info=os_info)


@router.post("/{host_id}/prerequisites", response_model=PrereqResult)
def check_prerequisites(host_id: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    host = db.query(Host).filter(Host.id == host_id).first()
    if not host:
        raise HTTPException(status_code=404, detail="Host not found")

    try:
        from app.services.ssh_manager import SSHManager
        with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
            checks = prereq_checker.run_all(client)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SSH connection failed: {e}")

    all_passed = all(c["status"] != "fail" for c in checks)
    return PrereqResult(host_id=host_id, checks=checks, all_passed=all_passed)
