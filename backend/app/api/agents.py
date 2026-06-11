"""Agent registration + connection endpoints.

POST   /api/hosts/{host_id}/agent/token   — mint a one-shot registration token
DELETE /api/hosts/{host_id}/agent         — revoke/disable agent for host
GET    /api/agents                        — list agents (admin/monitor)
GET    /api/agents/connected              — live connection map (per-worker)
WS     /api/agents/connect                — the actual agent connection

The token endpoint is admin-only; everything else is admin/monitor.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import jwt
from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, status
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
    "systemctl.is_active",
    "systemctl.status",
    "systemctl.start",
    "systemctl.stop",
    "systemctl.restart",
    "journalctl.read",
    "file.read:/etc/kafka/",
    "file.read:/opt/kafka-*/config/",
    "file.write:/opt/kafka-*/config/server.properties",
    "kafka_cli.topics",
    "kafka_cli.configs",
    "kafka_cli.acls",
    "kafka_cli.consumer_groups",
    "exec.ss",
    "exec.systemd-cgls",
]


# ---------- Token + admin endpoints ----------


@router.post("/hosts/{host_id}/agent/token", status_code=201)
def mint_token(host_id: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
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

    return {
        "registration_token": plaintext,
        "expires_at": expires.isoformat(),
        "agent_id": agent.id,
        "host_id": host_id,
        # Pre-rendered config snippet the admin can paste into
        # /etc/tantor-agent/config.yaml on the broker host.
        "config_hint": {
            "scm_url": _public_ws_url(),
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


