"""Per-worker registry of currently-connected agents.

Each connected agent maps to a small async object that holds the live
WebSocket plus a dict of pending command futures. The transport dispatch
layer (`app.services.agent_transport`) calls `dispatch_cmd` here to send an
op through an agent and await its result.

Scope: in-process. Across uvicorn workers this is per-worker (the agent
lands on whichever worker accepted the WS upgrade). For HA across workers
we'd add a Redis pub/sub fanout, but a single-process Tantor (the default
deploy) is already fully functional.

See docs/AGENT_PROTOCOL.md section 10 for the multi-worker caveat.
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from fastapi import WebSocket

logger = logging.getLogger("tantor.agent_registry")


PROTOCOL_VERSION = 1
DEFAULT_HEARTBEAT_INTERVAL_SEC = 15
DEFAULT_CMD_TIMEOUT_SEC = 60


@dataclass
class AgentSession:
    """One connected agent. Lives until the WebSocket closes.

    `loop` is captured when the WS handler accepts the connection — it's
    the uvicorn main event loop. Sync callers (FastAPI's threadpool-served
    routes use sync handlers) hand work onto this loop via
    `asyncio.run_coroutine_threadsafe` so the future + the receive loop
    that resolves it share one loop. Without this, a `Future` created on
    a transient loop would never see its `set_result` come through, and
    the cmd hangs until timeout."""

    agent_id: str
    host_id: str
    websocket: WebSocket
    agent_version: str | None
    features: list[str] = field(default_factory=list)
    connected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_heartbeat: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    pending: dict[str, asyncio.Future] = field(default_factory=dict)
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    loop: asyncio.AbstractEventLoop | None = None

    async def send_envelope(self, kind: str, payload: dict[str, Any], ref: str | None = None) -> str:
        """Serialize and send one envelope. Returns the new frame's id."""
        frame_id = uuid.uuid4().hex
        env = {
            "v": PROTOCOL_VERSION,
            "kind": kind,
            "id": frame_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }
        if ref:
            env["ref"] = ref
        async with self.send_lock:
            await self.websocket.send_text(json.dumps(env))
        return frame_id

    async def send_cmd(self, op: str, args: dict[str, Any] | None = None, timeout_sec: int = DEFAULT_CMD_TIMEOUT_SEC) -> dict[str, Any]:
        """Send a cmd frame and wait for the matching result/error.

        Returns a dict with one of:
          {"ok": True, "result": {...}}   — agent returned a result frame
          {"ok": False, "error": {...}}   — agent returned an error frame
        Raises asyncio.TimeoutError if no response within timeout_sec + 5s.
        """
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        # Generate the frame id up-front so we can register the future BEFORE
        # the send call resolves — otherwise a very fast agent could land the
        # response before we have somewhere to put it.
        frame_id = uuid.uuid4().hex
        self.pending[frame_id] = fut
        try:
            env = {
                "v": PROTOCOL_VERSION,
                "kind": "cmd",
                "id": frame_id,
                "ts": datetime.now(timezone.utc).isoformat(),
                "payload": {"op": op, "args": args or {}, "timeout_sec": timeout_sec},
            }
            async with self.send_lock:
                await self.websocket.send_text(json.dumps(env))
            try:
                return await asyncio.wait_for(fut, timeout=timeout_sec + 5)
            except asyncio.TimeoutError:
                logger.warning("agent %s: op=%s frame_id=%s timed out", self.agent_id, op, frame_id)
                raise
        finally:
            self.pending.pop(frame_id, None)

    def deliver(self, frame: dict[str, Any]) -> bool:
        """Match a result/error frame to a pending future. Returns True if delivered."""
        ref = frame.get("ref")
        if not ref:
            return False
        fut = self.pending.get(ref)
        if not fut or fut.done():
            return False
        payload = frame.get("payload") or {}
        if frame.get("kind") == "result":
            fut.set_result({"ok": True, "result": payload})
        elif frame.get("kind") == "error":
            fut.set_result({"ok": False, "error": payload})
        else:
            return False
        return True

    def touch_heartbeat(self) -> None:
        self.last_heartbeat = datetime.now(timezone.utc)


class AgentRegistry:
    """Process-global map of host_id -> AgentSession."""

    def __init__(self) -> None:
        self._by_host: dict[str, AgentSession] = {}
        self._by_agent: dict[str, AgentSession] = {}
        self._lock = asyncio.Lock()

    async def register(self, session: AgentSession) -> AgentSession | None:
        """Add a session. If an old session for the same host is connected,
        return it so the caller can close it cleanly."""
        async with self._lock:
            previous = self._by_host.get(session.host_id)
            self._by_host[session.host_id] = session
            self._by_agent[session.agent_id] = session
            return previous

    async def remove(self, session: AgentSession) -> None:
        async with self._lock:
            # Only remove if this session is still the active one for the host.
            # Reconnects may have already replaced us.
            current = self._by_host.get(session.host_id)
            if current and current.agent_id == session.agent_id:
                self._by_host.pop(session.host_id, None)
            self._by_agent.pop(session.agent_id, None)

    async def remove_for_session_if_present(self, ws) -> None:
        """Find a session by its WebSocket and remove it. Used as a
        defensive cleanup when the receive loop bails before the normal
        path runs."""
        async with self._lock:
            target: AgentSession | None = None
            for session in self._by_host.values():
                if session.websocket is ws:
                    target = session
                    break
            if target is None:
                return
            current = self._by_host.get(target.host_id)
            if current and current.agent_id == target.agent_id:
                self._by_host.pop(target.host_id, None)
            self._by_agent.pop(target.agent_id, None)

    def get(self, host_id: str) -> AgentSession | None:
        return self._by_host.get(host_id)

    def is_connected(self, host_id: str) -> bool:
        return host_id in self._by_host

    def connected_host_ids(self) -> list[str]:
        return list(self._by_host.keys())

    def session_count(self) -> int:
        return len(self._by_host)


# Module-level singleton. FastAPI app gets one of these per worker.
registry = AgentRegistry()
