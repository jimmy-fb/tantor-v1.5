"""Transport dispatch — prefer the agent over SSH where available.

The rest of Tantor calls one of these helpers; if a host's agent is
connected, the call goes through the WebSocket; otherwise it falls back to
the existing SSHManager-based path. This keeps the existing deploy model
working unchanged for operators who don't install the agent (the agent is
optional — see docs/AGENT_PROTOCOL.md).

This file is async-aware: the callsites that were sync (SSHManager-style)
get a sync wrapper that runs the agent dispatch on a fresh event loop when
needed. The wrappers preserve the existing return-shape conventions used by
ClusterManager and friends so we don't have to retrofit hundreds of call
sites.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

from app.models.host import Host
from app.services.agent_registry import registry

logger = logging.getLogger("tantor.agent_transport")


# Long systemctl/journalctl calls shouldn't tie up the SCM forever. These
# defaults are deliberately tight — operators tune via per-call overrides.
DEFAULT_SYSTEMCTL_TIMEOUT_SEC = 30
DEFAULT_JOURNAL_TIMEOUT_SEC = 30
DEFAULT_KAFKA_CLI_TIMEOUT_SEC = 60


def agent_available(host_id: str) -> bool:
    """True when this worker has a live agent session for the host. See
    AGENT_PROTOCOL.md section 10 for the multi-worker caveat."""
    return registry.is_connected(host_id)


# ---------- low-level dispatch ----------

async def run_op_async(host_id: str, op: str, args: dict[str, Any] | None = None, *, timeout_sec: int) -> dict:
    """Async dispatch. Returns the raw protocol result/error wrapper:
    {"ok": True, "result": {...}} or {"ok": False, "error": {...}}.
    Raises RuntimeError if no agent is connected for the host."""
    session = registry.get(host_id)
    if session is None:
        raise RuntimeError(f"no agent connected for host {host_id}")
    return await session.send_cmd(op, args or {}, timeout_sec=timeout_sec)


def run_op(host_id: str, op: str, args: dict[str, Any] | None = None, *, timeout_sec: int) -> dict:
    """Sync wrapper for non-async callers (ClusterManager, etc.).

    The session's `send_cmd` coroutine must run on the SAME event loop the
    WS receive task lives on — otherwise the future it awaits is never
    resolved by the receive loop's `session.deliver(...)`. We resolve this
    by submitting onto the session's captured loop via
    `run_coroutine_threadsafe`. FastAPI runs sync handlers in a threadpool
    so this is always safe (we're never the main loop ourselves).
    """
    session = registry.get(host_id)
    if session is None:
        raise RuntimeError(f"no agent connected for host {host_id}")
    if session.loop is None:
        # Session created without a loop — only happens in unit tests that
        # bypass the WS handler. Fall back to running locally.
        return asyncio.run(session.send_cmd(op, args or {}, timeout_sec=timeout_sec))
    future = asyncio.run_coroutine_threadsafe(
        session.send_cmd(op, args or {}, timeout_sec=timeout_sec),
        session.loop,
    )
    return future.result(timeout=timeout_sec + 10)


# ---------- high-level helpers mirroring the SSH manager's idioms ----------


def systemctl_is_active(host: Host, unit: str) -> tuple[bool, str]:
    """Return (running, raw_state). `running` mirrors how cluster_manager
    interprets `systemctl is-active` output: active/activating counts as
    running, anything else doesn't."""
    if not agent_available(host.id):
        raise RuntimeError("agent not available")
    res = run_op(
        host.id, "systemctl.is_active", {"unit": unit},
        timeout_sec=DEFAULT_SYSTEMCTL_TIMEOUT_SEC,
    )
    if not res.get("ok"):
        err = res.get("error") or {}
        return False, err.get("code", "agent_error")
    result = res["result"]
    state = (result.get("stdout") or "").strip() or "unknown"
    return state in ("active", "activating"), state


def systemctl_action(host: Host, action: str, unit: str) -> tuple[bool, str]:
    """Run a systemctl start/stop/restart. Returns (success, message)."""
    op = f"systemctl.{action}"
    if not agent_available(host.id):
        raise RuntimeError("agent not available")
    res = run_op(host.id, op, {"unit": unit}, timeout_sec=DEFAULT_SYSTEMCTL_TIMEOUT_SEC)
    if not res.get("ok"):
        err = res.get("error") or {}
        return False, f"{err.get('code', 'agent_error')}: {err.get('message', '')}"
    r = res["result"]
    if r.get("exit_code", 0) == 0:
        return True, f"{action} {unit} ok"
    return False, (r.get("stderr") or f"systemctl exit {r.get('exit_code')}")


def journal_read(host: Host, unit: str, lines: int = 200, since: str | None = None, priority: str | None = None) -> tuple[bool, str]:
    """Read last N lines of a unit's journal via the agent. Returns
    (success, body)."""
    if not agent_available(host.id):
        raise RuntimeError("agent not available")
    args: dict[str, Any] = {"unit": unit, "lines": lines}
    if since:
        args["since"] = since
    if priority:
        args["priority"] = priority
    res = run_op(host.id, "journalctl.read", args, timeout_sec=DEFAULT_JOURNAL_TIMEOUT_SEC)
    if not res.get("ok"):
        err = res.get("error") or {}
        return False, f"{err.get('code', 'agent_error')}: {err.get('message', '')}"
    r = res["result"]
    if r.get("exit_code", 0) != 0:
        return False, (r.get("stderr") or "journalctl returned non-zero")
    return True, r.get("stdout") or ""


# ---------- helpers that combine agent + SSH fallback ----------


def try_systemctl_is_active(host: Host, unit: str) -> tuple[bool, str] | None:
    """Agent path only — returns None if no agent is connected so the
    caller can fall through to SSH. Keeps the existing SSH semantics
    intact (the SSH path is the canonical implementation)."""
    if not agent_available(host.id):
        return None
    try:
        return systemctl_is_active(host, unit)
    except Exception as e:
        logger.warning("agent systemctl.is_active failed for %s/%s: %s; will fall back to SSH", host.id, unit, e)
        return None


def try_systemctl_action(host: Host, action: str, unit: str) -> tuple[bool, str] | None:
    if not agent_available(host.id):
        return None
    try:
        return systemctl_action(host, action, unit)
    except Exception as e:
        logger.warning("agent systemctl.%s failed for %s/%s: %s; will fall back to SSH", action, host.id, unit, e)
        return None


def try_journal_read(host: Host, unit: str, lines: int = 200, since: str | None = None, priority: str | None = None) -> tuple[bool, str] | None:
    if not agent_available(host.id):
        return None
    try:
        return journal_read(host, unit, lines, since, priority)
    except Exception as e:
        logger.warning("agent journalctl.read failed for %s/%s: %s; will fall back to SSH", host.id, unit, e)
        return None


# ---------- kafka_cli (used by topics/configs/acls hot paths) ----------


_INSTALL_DIR_RE = re.compile(r"^/opt/kafka[A-Za-z0-9_./-]*$")


def kafka_cli(host: Host, verb: str, bootstrap: str, install_dir: str, args: list[str], command_config: str | None = None, timeout_sec: int = DEFAULT_KAFKA_CLI_TIMEOUT_SEC) -> tuple[bool, str, str]:
    """Run kafka-*.sh through the agent. Returns (success, stdout, stderr).

    verb is one of: topics, configs, acls, consumer_groups.
    install_dir is the cluster's /opt/kafka-<slug>-<id>.
    """
    if not agent_available(host.id):
        raise RuntimeError("agent not available")
    if not _INSTALL_DIR_RE.match(install_dir):
        raise RuntimeError(f"invalid install_dir for agent dispatch: {install_dir}")
    payload: dict[str, Any] = {
        "bootstrap": bootstrap,
        "install_dir": install_dir,
        "args": args,
    }
    if command_config:
        payload["command_config"] = command_config
    res = run_op(host.id, f"kafka_cli.{verb}", payload, timeout_sec=timeout_sec)
    if not res.get("ok"):
        err = res.get("error") or {}
        return False, "", f"{err.get('code', 'agent_error')}: {err.get('message', '')}"
    r = res["result"]
    return r.get("exit_code", 0) == 0, r.get("stdout") or "", r.get("stderr") or ""
