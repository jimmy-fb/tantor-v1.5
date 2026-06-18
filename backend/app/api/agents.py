"""Agent registration + connection endpoints.

POST   /api/hosts/{host_id}/agent/token       — mint a one-shot registration token
DELETE /api/hosts/{host_id}/agent             — revoke/disable agent for host
GET    /api/agents                            — list agents (admin/monitor)
GET    /api/agents/connected                  — live connection map (per-worker)
GET    /api/agents/install.sh                 — one-line bootstrap installer
GET    /api/agents/binary/{platform}          — download the platform-matched binary
WS     /api/agents/connect                    — the actual agent connection

The token endpoint and install.sh are admin-only; everything else is admin/monitor.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import jwt
from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect, status
from fastapi.responses import FileResponse, PlainTextResponse
from sqlalchemy.orm import Session

from app.api.deps import require_admin, require_monitor_or_above
from app.database import SessionLocal, get_db
from app.models.agent import Agent
from app.models.host import Host
from app.models.user import User
from app.services.agent_auth import (
    decode_agent_jwt,
    issue_agent_jwt,
    looks_like_reg_token,
    mint_registration_token,
    verify_registration_token,
)
from app.services.agent_registry import (
    DEFAULT_CMD_TIMEOUT_SEC,
    DEFAULT_HEARTBEAT_INTERVAL_SEC,
    AgentSession,
    PROTOCOL_VERSION,
    registry,
)

logger = logging.getLogger("tantor.agents_api")

router = APIRouter(prefix="/api", tags=["agents"])


# Default server-side allowlist a fresh agent receives. The agent has its
# own local allowlist (defense in depth) — the intersection of the two wins.
DEFAULT_ALLOWED_OPS = [
    # Service lifecycle (used by cluster_manager start/stop/restart, status probes,
    # cleanup-on-delete, rolling restart).
    "systemctl.is_active",
    "systemctl.status",
    "systemctl.cat",
    "systemctl.start",
    "systemctl.stop",
    "systemctl.restart",
    "systemctl.enable",
    "systemctl.disable",
    "systemctl.reset_failed",
    "systemctl.reset_failed_all",
    "systemctl.kill",
    "systemctl.daemon_reload",
    # Logs (logs.py)
    "journalctl.read",
    # File ops (broker_config.py edits, agent-based deploy writes unit files
    # to /etc/systemd/system/kafka-*).
    "file.read:/etc/kafka/",
    "file.read:/opt/kafka-*/config/",
    "file.write:/opt/kafka-*/config/server.properties",
    "file.write:/etc/systemd/system/kafka-*.service",
    "file.write:/etc/systemd/system/zookeeper-*.service",
    "file.delete:/etc/systemd/system/kafka-*.service",
    "file.delete:/etc/systemd/system/zookeeper-*.service",
    # kafka CLI (kafka_admin.py SSH fallback path swaps onto this).
    "kafka_cli.topics",
    "kafka_cli.configs",
    "kafka_cli.acls",
    "kafka_cli.consumer_groups",
    # Diagnostics
    "exec.ss",
    "exec.systemd-cgls",
    # Install-time ops (Step 3 — agent-based deployer)
    "file.download",
    "exec.script",
]


# ---------- Token + admin endpoints ----------


@router.post("/hosts/{host_id}/agent/token", status_code=201)
def mint_token(host_id: str, request: Request, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    """Generate a one-shot registration token for the named host.

    The plaintext token is returned ONCE; we persist only its bcrypt hash.
    Replaces any prior pending token for this host (only one outstanding
    at a time).
    """
    host = db.query(Host).filter(Host.id == host_id).first()
    if not host:
        raise HTTPException(status_code=404, detail="Host not found")

    agent = db.query(Agent).filter(Agent.host_id == host_id).first()
    short = host_id.replace("-", "")[:8]
    plaintext, hashed, expires = mint_registration_token(short)

    if agent is None:
        agent = Agent(host_id=host_id)
        db.add(agent)
    agent.registration_token_hash = hashed
    agent.registration_token_expires_at = expires
    agent.is_active = True
    # Bump token_version so any old long-lived JWT is invalidated when an
    # admin re-issues a registration token.
    agent.token_version = (agent.token_version or 0) + 1
    db.commit()
    db.refresh(agent)

    # Build the one-line install command the operator can paste into the
    # broker host. Uses the inbound Host header so the URL points back to
    # the same SCM that minted the token.
    scm_http_url = f"{request.url.scheme}://{request.headers.get('host', request.base_url.hostname or 'tantor.local')}"
    install_cmd = (
        f"curl -fsSL '{scm_http_url}/api/agents/install.sh?host={host_id}&token={plaintext}' | sudo bash"
    )
    scheme = "wss" if request.url.scheme == "https" else "ws"
    scm_ws_url = f"{scheme}://{request.headers.get('host', 'tantor.local')}/api/agents/connect"

    return {
        "registration_token": plaintext,
        "expires_at": expires.isoformat(),
        "agent_id": agent.id,
        "host_id": host_id,
        # One-line installer the operator runs on the broker host.
        "install_command": install_cmd,
        # Config snippet for manual installs (air-gapped, custom packaging).
        "config_hint": {
            "scm_url": scm_ws_url,
            "registration_token": plaintext,
        },
    }


@router.delete("/hosts/{host_id}/agent")
def revoke_agent(host_id: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    """Disable the agent for this host: bump token_version so future JWT
    auth fails, drop any active session, and clear the pending registration
    token."""
    agent = db.query(Agent).filter(Agent.host_id == host_id).first()
    if not agent:
        raise HTTPException(status_code=404, detail="No agent for this host")
    agent.is_active = False
    agent.registration_token_hash = None
    agent.registration_token_expires_at = None
    agent.token_version = (agent.token_version or 0) + 1
    db.commit()

    # Best-effort: if a session is still attached, the receive loop will
    # close on the next heartbeat / cmd round-trip when token_version no
    # longer matches. The in-process registry will clean up on disconnect.
    return {"revoked": True, "agent_id": agent.id}


def _public_ws_url() -> str:
    """Return a placeholder wss:// URL — the admin overrides this in the
    agent's config.yaml with the actual SCM hostname."""
    return "wss://<TANTOR-PUBLIC-HOSTNAME>/api/agents/connect"


# ---------- Bootstrap installer ----------
#
# Operators run a single curl|sh on the broker host to install the agent.
# The flow:
#   1. UI calls POST /api/hosts/{id}/agent/token to mint a one-shot token.
#   2. UI shows the operator: `curl -fsSL https://<SCM>/api/agents/install.sh?host=<id>&token=<tok> | sudo bash`
#   3. install.sh detects the platform (linux/amd64 vs linux/arm64), downloads
#      the matching binary from /api/agents/binary/<platform>, drops the
#      systemd unit + sudoers profile, writes config.yaml with the SCM URL +
#      token, and `systemctl enable --now tantor-agent`.
#
# Zero SSH needed. The customer's network admin only needs outbound 443 from
# the broker host to the SCM — the same connection the agent will use for
# normal operations.
#
# The endpoint that serves install.sh is intentionally NOT auth-gated by
# JWT — the secret material is the registration token embedded in the
# URL, which is one-shot and host-specific. Anyone who has the URL has
# already been given enough credential to install the agent.

_AGENT_BIN_DIR = os.environ.get("TANTOR_AGENT_BIN_DIR", "/opt/tantor/agent-binaries")
_AGENT_PLATFORMS = {
    "linux-amd64": "tantor-agent-linux-amd64",
    "linux-arm64": "tantor-agent-linux-arm64",
}
# Scripts shipped alongside the binary. The bootstrap installer pulls these
# down so the agent can run them via exec.script.
_AGENT_SCRIPTS_DIR = os.environ.get(
    "TANTOR_AGENT_SCRIPTS_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "agent", "installer", "scripts"),
)
_AGENT_SCRIPT_NAMES = (
    "install-kafka.sh",
    "install-java.sh",
    "install-systemd-unit.sh",
    "uninstall-kafka.sh",
)


@router.get("/agents/install.sh", response_class=PlainTextResponse)
def install_script(request: Request, host: str = Query(..., description="Host UUID minted via POST /api/hosts/{id}/agent/token"), token: str = Query(..., description="Registration token plaintext")):
    """Renders the bootstrap install script with the SCM URL and token baked in."""
    # SCM URL: figure out from the inbound request's host header so the
    # script always points back to the same SCM the admin reached.
    scheme = "wss" if request.url.scheme == "https" else "ws"
    scm_host = request.headers.get("host", str(request.base_url.hostname or "tantor.local"))
    scm_ws_url = f"{scheme}://{scm_host}/api/agents/connect"
    scm_http_url = f"{request.url.scheme}://{scm_host}"

    # Note: this script is rendered server-side once per install. The
    # token + URL are embedded inline; the agent will swap to a long-lived
    # JWT on first connect.
    script = f"""#!/usr/bin/env bash
# Tantor agent bootstrap installer — auto-generated by the SCM.
# Run as root on a fresh broker host:
#   curl -fsSL '{scm_http_url}/api/agents/install.sh?host={host}&token={token}' | sudo bash
set -euo pipefail

if [ "$(id -u)" != "0" ]; then
    echo "tantor-agent installer must run as root" >&2; exit 1
fi

ARCH="$(uname -m)"
case "$ARCH" in
    x86_64|amd64)  PLATFORM=linux-amd64 ;;
    aarch64|arm64) PLATFORM=linux-arm64 ;;
    *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

SCM_URL="{scm_http_url}"
WS_URL="{scm_ws_url}"
HOST_ID="{host}"
REG_TOKEN="{token}"
BIN_DIR=/usr/local/bin
CFG_DIR=/etc/tantor-agent
STATE_DIR=/var/lib/tantor-agent

echo "[tantor-agent] detected platform: $PLATFORM"
echo "[tantor-agent] SCM: $SCM_URL"

# 1) Service account + dirs
if ! id tantor-agent >/dev/null 2>&1; then
    useradd --system --shell /usr/sbin/nologin --home-dir "$STATE_DIR" tantor-agent
fi
install -d -o tantor-agent -g tantor-agent -m 0700 "$STATE_DIR"
install -d -o root -g tantor-agent -m 0750 "$CFG_DIR"

# 2) Download binary
echo "[tantor-agent] downloading binary..."
TMP_BIN=$(mktemp)
curl -fsSL "$SCM_URL/api/agents/binary/$PLATFORM" -o "$TMP_BIN"
install -m 0755 -o root -g root "$TMP_BIN" "$BIN_DIR/tantor-agent"
rm -f "$TMP_BIN"

# 2b) Download install scripts (used by Tantor's agent-based deployer for
# install-kafka / install-java / install-systemd-unit / uninstall-kafka).
echo "[tantor-agent] installing helper scripts..."
SCRIPTS_DIR=/usr/local/lib/tantor-agent/scripts
install -d -o root -g root -m 0755 "$SCRIPTS_DIR"
for SCRIPT in install-kafka.sh install-java.sh install-systemd-unit.sh uninstall-kafka.sh; do
    TMP_SCRIPT=$(mktemp)
    curl -fsSL "$SCM_URL/api/agents/scripts/$SCRIPT" -o "$TMP_SCRIPT"
    install -m 0755 -o root -g root "$TMP_SCRIPT" "$SCRIPTS_DIR/$SCRIPT"
    rm -f "$TMP_SCRIPT"
done

# 3) Write config.yaml
cat > "$CFG_DIR/config.yaml" <<EOF
scm_url: "$WS_URL"
registration_token: "$REG_TOKEN"
tls_verify: true
state_dir: "$STATE_DIR"

allowed_operations:
  - systemctl.is_active
  - systemctl.status
  - systemctl.cat
  - systemctl.start
  - systemctl.stop
  - systemctl.restart
  - systemctl.enable
  - systemctl.disable
  - systemctl.reset_failed
  - systemctl.reset_failed_all
  - systemctl.kill
  - systemctl.daemon_reload
  - journalctl.read
  - file.read:/etc/kafka/
  - file.read:/opt/kafka-*/config/
  - file.write:/opt/kafka-*/config/server.properties
  - file.write:/etc/systemd/system/kafka-*.service
  - file.write:/etc/systemd/system/zookeeper-*.service
  - file.delete:/etc/systemd/system/kafka-*.service
  - file.delete:/etc/systemd/system/zookeeper-*.service
  - kafka_cli.topics
  - kafka_cli.configs
  - kafka_cli.acls
  - kafka_cli.consumer_groups
  - exec.ss
  - exec.systemd-cgls
  - file.download
  - exec.script
EOF
chmod 0640 "$CFG_DIR/config.yaml"
chown root:tantor-agent "$CFG_DIR/config.yaml"

# 4) Sudoers profile
cat > /etc/sudoers.d/tantor-agent.tmp <<'SUDOERS_EOF'
Cmnd_Alias TANTOR_SYSTEMCTL = \\
    /bin/systemctl is-active *, /bin/systemctl status *, /bin/systemctl cat *, \\
    /bin/systemctl start kafka-*, /bin/systemctl stop kafka-*, /bin/systemctl restart kafka-*, \\
    /bin/systemctl enable kafka-*, /bin/systemctl disable kafka-*, /bin/systemctl reset-failed kafka-*, \\
    /bin/systemctl reset-failed, /bin/systemctl kill --kill-who=* kafka-*, /bin/systemctl daemon-reload, \\
    /bin/systemctl start zookeeper-*, /bin/systemctl stop zookeeper-*, /bin/systemctl restart zookeeper-*, \\
    /bin/systemctl enable zookeeper-*, /bin/systemctl disable zookeeper-*, /bin/systemctl reset-failed zookeeper-*, \\
    /bin/systemctl kill --kill-who=* zookeeper-*, \\
    /usr/bin/systemctl is-active *, /usr/bin/systemctl status *, /usr/bin/systemctl cat *, \\
    /usr/bin/systemctl start kafka-*, /usr/bin/systemctl stop kafka-*, /usr/bin/systemctl restart kafka-*, \\
    /usr/bin/systemctl enable kafka-*, /usr/bin/systemctl disable kafka-*, /usr/bin/systemctl reset-failed kafka-*, \\
    /usr/bin/systemctl reset-failed, /usr/bin/systemctl kill --kill-who=* kafka-*, /usr/bin/systemctl daemon-reload, \\
    /usr/bin/systemctl start zookeeper-*, /usr/bin/systemctl stop zookeeper-*, /usr/bin/systemctl restart zookeeper-*, \\
    /usr/bin/systemctl enable zookeeper-*, /usr/bin/systemctl disable zookeeper-*, /usr/bin/systemctl reset-failed zookeeper-*, \\
    /usr/bin/systemctl kill --kill-who=* zookeeper-*
Cmnd_Alias TANTOR_JOURNALCTL = /bin/journalctl -u *, /usr/bin/journalctl -u *
Cmnd_Alias TANTOR_FILEREAD = /bin/cat /etc/kafka/*, /bin/cat /opt/kafka-*/config/*, /usr/bin/cat /etc/kafka/*, /usr/bin/cat /opt/kafka-*/config/*
Cmnd_Alias TANTOR_FILEWRITE = \\
    /usr/bin/install -m * /tmp/tantor-agent-write-* /opt/kafka-*/config/server.properties, \\
    /usr/bin/install -m * /tmp/tantor-agent-write-* /opt/kafka-*/config/server.properties.bak, \\
    /usr/bin/install -m * /tmp/tantor-agent-write-* /etc/systemd/system/kafka-*.service, \\
    /usr/bin/install -m * /tmp/tantor-agent-write-* /etc/systemd/system/zookeeper-*.service
Cmnd_Alias TANTOR_FILEDELETE = \\
    /bin/rm -f /etc/systemd/system/kafka-*.service, /bin/rm -f /etc/systemd/system/zookeeper-*.service, \\
    /usr/bin/rm -f /etc/systemd/system/kafka-*.service, /usr/bin/rm -f /etc/systemd/system/zookeeper-*.service
Cmnd_Alias TANTOR_KAFKA_CLI = /opt/kafka-*/bin/kafka-topics.sh, /opt/kafka-*/bin/kafka-configs.sh, /opt/kafka-*/bin/kafka-acls.sh, /opt/kafka-*/bin/kafka-consumer-groups.sh
Cmnd_Alias TANTOR_EXEC = /bin/ss -tnlp, /usr/sbin/ss -tnlp, /usr/bin/ss -tnlp, /bin/systemd-cgls --unit * --no-pager, /usr/bin/systemd-cgls --unit * --no-pager
Cmnd_Alias TANTOR_INSTALL = \\
    /usr/local/lib/tantor-agent/scripts/install-kafka.sh *, \\
    /usr/local/lib/tantor-agent/scripts/install-java.sh *, \\
    /usr/local/lib/tantor-agent/scripts/install-systemd-unit.sh *, \\
    /usr/local/lib/tantor-agent/scripts/uninstall-kafka.sh *
Cmnd_Alias TANTOR_DOWNLOAD = \\
    /usr/bin/install -m * /tmp/tantor-dl-* /opt/kafka-*/*, \\
    /usr/bin/install -m * /tmp/tantor-dl-* /opt/tantor-stage/*

tantor-agent ALL=(root) NOPASSWD: TANTOR_SYSTEMCTL, TANTOR_JOURNALCTL, TANTOR_FILEREAD, TANTOR_FILEWRITE, TANTOR_FILEDELETE, TANTOR_KAFKA_CLI, TANTOR_EXEC, TANTOR_INSTALL, TANTOR_DOWNLOAD
Defaults!TANTOR_SYSTEMCTL,TANTOR_JOURNALCTL,TANTOR_FILEREAD,TANTOR_FILEWRITE,TANTOR_FILEDELETE,TANTOR_KAFKA_CLI,TANTOR_EXEC,TANTOR_INSTALL,TANTOR_DOWNLOAD !requiretty
SUDOERS_EOF
visudo -c -f /etc/sudoers.d/tantor-agent.tmp >/dev/null
install -m 0440 -o root -g root /etc/sudoers.d/tantor-agent.tmp /etc/sudoers.d/tantor-agent
rm -f /etc/sudoers.d/tantor-agent.tmp

# 5) systemd unit
cat > /etc/systemd/system/tantor-agent.service <<'SYSTEMD_EOF'
[Unit]
Description=Tantor host agent (reverse-tunnel to Tantor SCM)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=tantor-agent
Group=tantor-agent
ExecStart=/usr/local/bin/tantor-agent --config /etc/tantor-agent/config.yaml
Restart=always
RestartSec=5
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=/var/lib/tantor-agent
NoNewPrivileges=false
CPUQuota=50%
MemoryMax=256M
TasksMax=64

[Install]
WantedBy=multi-user.target
SYSTEMD_EOF
chmod 0644 /etc/systemd/system/tantor-agent.service

# 6) Enable + start
systemctl daemon-reload
systemctl enable --now tantor-agent

# 7) Wait briefly for first connect to validate
sleep 3
if systemctl is-active --quiet tantor-agent; then
    echo "[tantor-agent] installed and running. Watch logs with: journalctl -u tantor-agent -f"
else
    echo "[tantor-agent] WARNING: service not active. Check: journalctl -u tantor-agent -n 50"
    exit 1
fi
"""
    return PlainTextResponse(content=script, media_type="text/x-shellscript")


@router.get("/agents/scripts/{name}", response_class=PlainTextResponse)
def download_agent_script(name: str):
    """Serve a single agent install script (pulled by the bootstrap installer).

    Only the names in _AGENT_SCRIPT_NAMES are served — the SCM never exposes
    arbitrary files from its filesystem under this prefix.
    """
    if name not in _AGENT_SCRIPT_NAMES:
        raise HTTPException(status_code=404, detail=f"unknown agent script {name!r}")
    path = os.path.abspath(os.path.join(_AGENT_SCRIPTS_DIR, name))
    if not os.path.isfile(path):
        raise HTTPException(
            status_code=503,
            detail=f"agent script {name} not present at {path} (operator deployment issue)",
        )
    with open(path, "rb") as f:
        return PlainTextResponse(content=f.read().decode("utf-8"), media_type="text/x-shellscript")


@router.get("/agents/binary/{platform}")
def download_binary(platform: str):
    """Serve the platform-matched agent binary. Used by install.sh.

    Operators populate TANTOR_AGENT_BIN_DIR with the cross-compiled binaries
    at install time (or via the rpm/deb package). The default location is
    `/opt/tantor/agent-binaries/`.
    """
    binary_name = _AGENT_PLATFORMS.get(platform)
    if binary_name is None:
        raise HTTPException(status_code=404, detail=f"unknown platform {platform!r}; expected one of {sorted(_AGENT_PLATFORMS)}")
    path = os.path.join(_AGENT_BIN_DIR, binary_name)
    if not os.path.isfile(path):
        raise HTTPException(
            status_code=503,
            detail=(
                f"agent binary not present at {path}. "
                f"Operators must populate {_AGENT_BIN_DIR}/ on the SCM host "
                f"with cross-compiled binaries (`make agent-binaries` or "
                f"`cd agent && GOOS=linux GOARCH=amd64 go build ...`)."
            ),
        )
    return FileResponse(path, media_type="application/octet-stream", filename=binary_name)


@router.get("/agents")
def list_agents(db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    """List all Agent rows joined with their Host for the UI table."""
    rows = (
        db.query(Agent, Host)
        .join(Host, Host.id == Agent.host_id)
        .order_by(Agent.created_at.desc())
        .all()
    )
    out = []
    for agent, host in rows:
        features = []
        if agent.features_json:
            try:
                features = json.loads(agent.features_json)
            except Exception:
                features = []
        out.append({
            "id": agent.id,
            "host_id": agent.host_id,
            "hostname": host.hostname,
            "ip_address": host.ip_address,
            "is_active": agent.is_active,
            "agent_version": agent.agent_version,
            "os_family": agent.os_family,
            "os_version": agent.os_version,
            "kernel": agent.kernel,
            "features": features,
            "connected": registry.is_connected(agent.host_id),
            "last_registered_at": agent.last_registered_at.isoformat() if agent.last_registered_at else None,
            "last_heartbeat_at": agent.last_heartbeat_at.isoformat() if agent.last_heartbeat_at else None,
            "has_pending_token": bool(agent.registration_token_hash),
            "registration_token_expires_at": (
                agent.registration_token_expires_at.isoformat()
                if agent.registration_token_expires_at else None
            ),
        })
    return out


@router.get("/agents/connected")
def list_connected(_: User = Depends(require_monitor_or_above)):
    """List the host_ids whose agent is currently connected to THIS worker.
    Per-worker; aggregate across workers is a future Redis-fanout feature.
    """
    return {"connected_host_ids": registry.connected_host_ids(), "count": registry.session_count()}


# ---------- The WebSocket endpoint ----------


@router.websocket("/agents/connect")
async def agent_ws(ws: WebSocket):
    """Agent ↔ SCM WebSocket. See docs/AGENT_PROTOCOL.md for the wire
    protocol. Auth is via the Authorization: Bearer <token> upgrade header.
    """
    bearer = _extract_bearer(ws)
    if not bearer:
        # 1008 = policy violation (close code WebSocket spec); FastAPI
        # serializes that into the close frame.
        await ws.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await ws.accept()

    db = SessionLocal()
    try:
        # First frame MUST be `register`.
        try:
            first = await ws.receive_text()
        except WebSocketDisconnect:
            return
        try:
            reg = json.loads(first)
        except Exception:
            await _send_error(ws, "bad_args", "first frame is not valid JSON")
            await ws.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        if reg.get("kind") != "register" or reg.get("v") != PROTOCOL_VERSION:
            await _send_error(ws, "version_mismatch", "first frame must be a v1 register")
            await ws.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        # Authenticate. Two paths: registration token (first connect) or
        # the long-lived agent JWT.
        agent, host = _authenticate(db, bearer, reg)
        if agent is None:
            await _send_error(ws, "auth_failed", "invalid or expired token", ref=reg.get("id"))
            await ws.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        # Update the Agent row with what the agent self-reported.
        payload = reg.get("payload") or {}
        agent.agent_version = (payload.get("agent_version") or "")[:64] or None
        os_info = payload.get("os") or {}
        agent.os_family = (os_info.get("family") or "")[:64] or None
        agent.os_version = (os_info.get("version") or "")[:64] or None
        agent.kernel = (os_info.get("kernel") or "")[:255] or None
        features = payload.get("agent_features") or []
        agent.features_json = json.dumps(features) if features else None
        agent.last_registered_at = datetime.now(timezone.utc)
        # Whichever token path got us here, clear the pending registration
        # token so the same plaintext can't be replayed.
        agent.registration_token_hash = None
        agent.registration_token_expires_at = None
        db.commit()
        db.refresh(agent)

        # Issue the long-lived JWT and send register_ack.
        jwt_token = issue_agent_jwt(agent.id, agent.host_id, token_version=agent.token_version or 0)
        ack_payload = {
            "agent_id": agent.id,
            "host_id": agent.host_id,
            "agent_jwt": jwt_token,
            "heartbeat_interval_sec": DEFAULT_HEARTBEAT_INTERVAL_SEC,
            "command_timeout_default_sec": DEFAULT_CMD_TIMEOUT_SEC,
            "allowed_operations": DEFAULT_ALLOWED_OPS,
        }
        ack_env = {
            "v": PROTOCOL_VERSION,
            "kind": "register_ack",
            "ref": reg.get("id"),
            "ts": datetime.now(timezone.utc).isoformat(),
            "payload": ack_payload,
        }
        await ws.send_text(json.dumps(ack_env))

        # Register in the in-memory map. If a previous session existed for
        # the same host, close it cleanly first. We capture the main
        # uvicorn loop here so sync FastAPI handlers (running in the
        # threadpool) can dispatch onto this session via
        # run_coroutine_threadsafe.
        import asyncio as _asyncio
        session = AgentSession(
            agent_id=agent.id,
            host_id=agent.host_id,
            websocket=ws,
            agent_version=agent.agent_version,
            features=features,
            loop=_asyncio.get_running_loop(),
        )
        previous = await registry.register(session)
        if previous is not None and previous is not session:
            logger.info("replacing prior session for host %s", agent.host_id)
            try:
                await previous.websocket.close(code=status.WS_1001_GOING_AWAY)
            except Exception:
                pass

        logger.info("agent connected: agent_id=%s host_id=%s version=%s",
                    agent.id, agent.host_id, agent.agent_version)

        # Receive loop. db sessions inside this loop are short — we open a
        # fresh SessionLocal for heartbeat persistence so the long-lived
        # outer `db` doesn't accumulate stale state.
        await _receive_loop(ws, session)

    finally:
        await registry.remove_for_session_if_present(ws)
        db.close()


# ---------- helpers ----------


def _extract_bearer(ws: WebSocket) -> str | None:
    auth = ws.headers.get("authorization") or ws.headers.get("Authorization")
    if not auth:
        return None
    if not auth.lower().startswith("bearer "):
        return None
    return auth[7:].strip()


async def _send_error(ws: WebSocket, code: str, message: str, ref: str | None = None) -> None:
    env: dict = {
        "v": PROTOCOL_VERSION,
        "kind": "error",
        "ts": datetime.now(timezone.utc).isoformat(),
        "payload": {"code": code, "message": message},
    }
    if ref:
        env["ref"] = ref
    try:
        await ws.send_text(json.dumps(env))
    except Exception:
        pass


def _authenticate(db: Session, token: str, register_frame: dict) -> tuple[Agent | None, Host | None]:
    """Returns (agent, host) on success, (None, None) on auth failure."""
    payload = register_frame.get("payload") or {}

    # Path A: registration token (first-connect).
    if looks_like_reg_token(token):
        host_id_hint = payload.get("host_id_hint") or ""
        # Try host_id_hint first; if absent, scan agents with pending
        # tokens until we find a matching hash (small N, so this is fine).
        candidates = []
        if host_id_hint:
            row = db.query(Agent).filter(Agent.host_id == host_id_hint).first()
            if row:
                candidates.append(row)
        else:
            candidates = (
                db.query(Agent)
                .filter(Agent.registration_token_hash.isnot(None))
                .filter(Agent.is_active.is_(True))
                .all()
            )
        now = datetime.now(timezone.utc)
        for cand in candidates:
            if not cand.registration_token_hash:
                continue
            exp = cand.registration_token_expires_at
            # SQLite's DateTime column drops tz info on round-trip; treat
            # the persisted timestamp as UTC if it came back naive.
            if exp is not None and exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if exp is not None and exp < now:
                continue
            if verify_registration_token(token, cand.registration_token_hash):
                host = db.query(Host).filter(Host.id == cand.host_id).first()
                if not host:
                    return None, None
                return cand, host
        return None, None

    # Path B: long-lived agent JWT.
    try:
        claims = decode_agent_jwt(token)
    except jwt.ExpiredSignatureError:
        logger.info("agent JWT expired")
        return None, None
    except jwt.InvalidTokenError as e:
        logger.info("agent JWT invalid: %s", e)
        return None, None

    agent_id = claims.get("sub")
    if not agent_id:
        return None, None
    agent = db.query(Agent).filter(Agent.id == agent_id).first()
    if not agent or not agent.is_active:
        return None, None
    expected_tv = agent.token_version or 0
    got_tv = claims.get("tv", 0) or 0
    if got_tv != expected_tv:
        logger.info("agent JWT token_version mismatch (got %d, expected %d) — rejecting", got_tv, expected_tv)
        return None, None

    host = db.query(Host).filter(Host.id == agent.host_id).first()
    if not host:
        return None, None
    return agent, host


async def _receive_loop(ws: WebSocket, session: AgentSession) -> None:
    """Drain inbound frames forever. Heartbeats update last_heartbeat;
    result/error frames are routed to pending cmd futures; everything
    else is logged + ignored."""
    try:
        while True:
            raw = await ws.receive_text()
            try:
                env = json.loads(raw)
            except Exception:
                continue
            kind = env.get("kind")
            if kind == "heartbeat":
                session.touch_heartbeat()
                _persist_heartbeat(session)
            elif kind in ("result", "error"):
                if not session.deliver(env):
                    logger.debug("dropped %s frame (no matching cmd ref)", kind)
            elif kind == "event":
                # Streaming output — not yet forwarded to the UI. Logged
                # so an operator running `tail -f` on the SCM logs can
                # eyeball the line in dev. Future work: bridge into the
                # main /api/ws UI WebSocket via a pub/sub.
                logger.debug("agent %s event: %s", session.agent_id, env.get("payload"))
            else:
                logger.debug("ignored frame kind=%s", kind)
    except WebSocketDisconnect:
        logger.info("agent disconnected: agent_id=%s host_id=%s", session.agent_id, session.host_id)
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("agent receive loop crashed: %s", e)
    finally:
        await registry.remove(session)


def _persist_heartbeat(session: AgentSession) -> None:
    """Best-effort: keep agents.last_heartbeat_at fresh so the UI shows
    when the agent was last alive."""
    db = SessionLocal()
    try:
        agent = db.query(Agent).filter(Agent.id == session.agent_id).first()
        if agent:
            agent.last_heartbeat_at = datetime.now(timezone.utc)
            db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


