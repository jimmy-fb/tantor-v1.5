# Agent Protocol (v1)

Wire format and lifecycle for the Tantor host agent. Designed for the secure-banking deployment scenario described in `docs/ROADMAP.md` (#23) and the original "Agent-Based Kafka Infrastructure Management" design brief:

- **Reverse-tunnel** — agent always dials OUT to the Tantor management server (SCM). No inbound management ports on the broker host.
- **Air-gappable** — works without internet. Single static binary, offline `.deb` / `.rpm` / `.tar.gz` install.
- **Restricted sudo allowlist** — agent's service account has passwordless sudo only on a hard-coded set of operations (no shell escape).
- **TLS by default** — the WebSocket connection is `wss://` (TLS terminated at Tantor's nginx).
- **Stateless on the SCM side** — agents register on connect; the SCM keeps an in-process map. Reconnects re-register.

The agent is OPTIONAL. If no agent is connected for a host, Tantor falls back to its existing SSH+CLI transport. Operators in non-restricted environments don't need to install the agent at all.

---

## 1. Transport

WebSocket over TLS. The agent connects to:

```
wss://<tantor-host>/api/agents/connect
```

Two upgrade headers are required:

| Header | Value |
|---|---|
| `Authorization` | `Bearer <registration-token>` (first connect) OR `Bearer <agent-jwt>` (re-connect) |
| `X-Tantor-Agent-Version` | semver of the agent binary, e.g. `1.5.0` |

The registration token is a one-shot value an admin mints via the Tantor UI (Host detail → "Generate agent token"). Agent stores the issued JWT in `/etc/tantor-agent/agent.jwt` after the first successful connect and uses that for subsequent reconnects.

---

## 2. Message envelope

Every frame on the WebSocket is JSON, UTF-8:

```json
{
  "v": 1,
  "kind": "register" | "register_ack" | "heartbeat" | "cmd" | "result" | "event" | "error",
  "id": "<uuid4>",
  "ref": "<uuid4 of the originating cmd, when kind=result>",
  "ts": "2026-06-11T08:30:00Z",
  "payload": { ... }
}
```

- `v` — protocol version. The SCM rejects unknown versions at connect time.
- `kind` — frame type. See sections 3-7 for each.
- `id` — globally unique frame ID. Used for de-dup on resume.
- `ref` — for `result` and `error` frames, the `id` of the `cmd` they answer.
- `ts` — server time (UTC) or agent time. Informational; both sides use their own clocks for timeouts.
- `payload` — frame-specific body.

Frames are independent — the agent can have multiple in-flight commands and respond out of order.

---

## 3. Registration

### Agent → SCM (immediately after WS upgrade)

```json
{ "v": 1, "kind": "register", "id": "<uuid>", "ts": "...",
  "payload": {
    "hostname": "broker-01.bank.internal",
    "ip_addresses": ["10.20.30.5", "fe80::1"],
    "os": { "family": "rhel", "version": "9.4", "kernel": "5.14.0-..." },
    "agent_version": "1.5.0",
    "agent_features": ["systemctl", "journalctl", "kafka_cli", "file_read", "file_write"],
    "host_id_hint": "<optional UUID if known from a previous registration>"
  }
}
```

`agent_features` is the operator-controlled list of capabilities this agent will accept. If the agent is configured without `kafka_cli`, the SCM is informed and won't try to dispatch kafka CLI commands through it (it will fall back to SSH or fail with a clear error).

### SCM → Agent

If the registration token is valid and the (host_id_hint or hostname) maps to a known Host row, the SCM responds:

```json
{ "v": 1, "kind": "register_ack", "ref": "<register.id>",
  "payload": {
    "agent_id": "<server-assigned uuid>",
    "host_id": "<the Host row's id>",
    "agent_jwt": "<long-lived JWT, valid 1 year, embeds agent_id + host_id>",
    "heartbeat_interval_sec": 15,
    "command_timeout_default_sec": 60,
    "allowed_operations": [
      "systemctl.start", "systemctl.stop", "systemctl.restart",
      "systemctl.is_active", "systemctl.status",
      "journalctl.read",
      "file.read:/etc/kafka/", "file.read:/opt/kafka-*/config/",
      "file.write:/opt/kafka-*/config/server.properties",
      "kafka_cli.topics", "kafka_cli.configs", "kafka_cli.acls", "kafka_cli.consumer_groups",
      "exec.systemd-cgls", "exec.ss", "exec.jstack"
    ]
  }
}
```

The agent persists `agent_id` + `agent_jwt` to disk (mode 600, owner `tantor-agent`) and uses them for reconnects.

`allowed_operations` is the **server-enforced** allowlist of commands the SCM may send to this agent. The agent ALSO enforces its own local allowlist — defense in depth. If the SCM and agent disagree, the agent's list wins (refuses the operation with an `error` frame).

If registration fails (bad token, hostname conflict, etc.), the SCM sends:

```json
{ "v": 1, "kind": "error", "ref": "<register.id>",
  "payload": { "code": "auth_failed" | "host_not_found" | "version_mismatch", "message": "..." }
}
```

and closes the WebSocket.

---

## 4. Heartbeat

Every `heartbeat_interval_sec` (default 15s) the agent sends:

```json
{ "v": 1, "kind": "heartbeat", "id": "<uuid>", "ts": "...",
  "payload": {
    "uptime_sec": 12345,
    "kafka_units": [
      { "unit": "kafka-prod-1ac9bbbe.service", "active_state": "active", "sub_state": "running",
        "main_pid": 12345, "memory_bytes": 1234567890 }
    ],
    "load1": 0.45, "mem_used_pct": 32.1, "disk_used_pct": { "/var/lib/kafka-prod-1ac9bbbe": 41.0 }
  }
}
```

The SCM uses heartbeats both as a connection liveness check (no heartbeat for 4 × interval → mark agent disconnected) and as a low-cost source of system metrics. The heartbeat body intentionally subsumes the cheap things the Monitoring tab needs so we don't have to round-trip a `cmd` for every refresh.

The SCM does NOT need to respond to heartbeats. Silence is acknowledgment.

---

## 5. Command dispatch

### SCM → Agent

```json
{ "v": 1, "kind": "cmd", "id": "<uuid>", "ts": "...",
  "payload": {
    "op": "systemctl.is_active",
    "args": { "unit": "kafka-prod-1ac9bbbe.service" },
    "timeout_sec": 10
  }
}
```

The `op` is one of the entries in the agent's `allowed_operations` list (path-prefixed entries like `file.write:/opt/kafka-*/config/server.properties` use a glob match on `args.path`). The agent rejects any `op` not in its local allowlist.

`timeout_sec` overrides the per-frame default. If absent, the SCM's `command_timeout_default_sec` applies.

### Agent → SCM (result)

```json
{ "v": 1, "kind": "result", "ref": "<cmd.id>", "ts": "...",
  "payload": {
    "exit_code": 0,
    "stdout": "active\n",
    "stderr": "",
    "duration_ms": 28
  }
}
```

For binary file reads, stdout is base64 with a `"encoding": "base64"` sibling.

For long-running operations (journalctl tail, rolling restart), the agent emits `event` frames (section 6) until the command completes, then a final `result`.

### Agent → SCM (error)

```json
{ "v": 1, "kind": "error", "ref": "<cmd.id>", "ts": "...",
  "payload": { "code": "denied" | "timeout" | "exec_failed" | "bad_args", "message": "..." }
}
```

`code=denied` means the operation isn't in the agent's local allowlist — visible to the operator as "agent refused this command; check its config".

---

## 6. Streaming events

For operations that produce output incrementally (`journalctl -f`, `tail -f /var/log/kafka-*/server.log`), the agent sends:

```json
{ "v": 1, "kind": "event", "ref": "<cmd.id>", "ts": "...",
  "payload": { "stream": "stdout", "data": "[2026-06-11 08:30:00] INFO ..." }
}
```

The SCM forwards these to any subscribed UI WebSocket so the operator sees output in real time. A final `result` frame marks the command done.

---

## 7. Operations (current set)

| Op | Purpose | Args | Returns |
|---|---|---|---|
| `systemctl.is_active` | service liveness probe (replaces SSH `systemctl is-active`) | `unit` | stdout = `active` / `inactive` / `failed` / `activating` |
| `systemctl.status` | full status with last 20 log lines | `unit` | stdout = systemctl status output |
| `systemctl.start` | start a service | `unit` | exit_code |
| `systemctl.stop` | stop a service | `unit` | exit_code |
| `systemctl.restart` | restart a service | `unit` | exit_code |
| `journalctl.read` | read last N lines (non-streaming) | `unit, lines, since, priority, grep` | stdout |
| `journalctl.tail` | stream live log lines | `unit, grep` | `event` frames until cancelled |
| `file.read` | read a file (mode 644 + path in allowlist) | `path` | stdout (utf-8 or base64) |
| `file.write` | write a file (path in allowlist) | `path, content, mode` | exit_code |
| `kafka_cli.topics` | shell out to `kafka-topics.sh` | `bootstrap, args` | parsed JSON |
| `kafka_cli.configs` | `kafka-configs.sh` | `bootstrap, args` | parsed JSON |
| `kafka_cli.acls` | `kafka-acls.sh` | `bootstrap, args` | parsed JSON |
| `kafka_cli.consumer_groups` | `kafka-consumer-groups.sh` | `bootstrap, args` | parsed JSON |
| `exec.ss` | run `ss -tnlp` (port preflight) | `ports` | parsed JSON |
| `exec.systemd-cgls` | cgroup tree (for "which process owns port X") | `unit` | stdout |

This list grows over time. Adding an op requires:
1. The agent recognizes the new `op` string and routes to a handler
2. The op is added to the SCM's default `allowed_operations` for new agents
3. Existing agents get the new op next time the operator runs `tantor-agent restart` (no auto-update)

---

## 8. Reconnect + resume

If the WebSocket drops, the agent reconnects with exponential backoff (1s → 30s cap). The reconnect uses the persisted `agent_jwt` rather than the original registration token.

Commands in flight when the connection drops are lost — the SCM marks them as failed with `code="connection_lost"`. The operator retries.

No resume protocol in v1. If we want exactly-once command execution later, we can layer a command journal on top.

---

## 9. Security model

| Threat | Mitigation |
|---|---|
| Compromised SCM trying to escalate beyond Kafka ops | Agent's local allowlist refuses ops not in its config. Cannot be overridden by SCM. |
| Compromised agent host | SCM treats agent input as untrusted; never `eval`'s frames. Heartbeat metrics are advisory only. |
| Replayed `cmd` frames | Each frame has `id` (uuid4); agent dedups within a 1-hour window. |
| Token theft from disk | `agent.jwt` is mode 600 owned by `tantor-agent`. Rotate via `tantor-agent rotate-token` (calls SCM, receives new JWT). |
| MITM between agent and SCM | WSS only; agent validates the SCM's TLS chain against the bundled CA (in the install package) OR system trust store. |
| Privilege escalation via passwordless sudo | Sudo rule is hardcoded to specific commands with arg patterns (see `installer/agent/sudoers.d/tantor-agent`). Agent service account has no shell. |

---

## 10. Implementation notes

- The Tantor backend maintains an in-process `Dict[host_id, WebSocket]` of connected agents. Across uvicorn workers this is per-worker; for now multi-worker behavior is "the agent connects to whichever worker accepted the upgrade, that worker handles dispatch". For HA we'd add a Redis pub/sub layer.
- The agent binary is Go (single static binary, no runtime deps). Cross-compile for linux/amd64 and linux/arm64.
- Reconnect backoff: 1s, 2s, 4s, 8s, 16s, 30s cap, +jitter ±20%. Survives temporary SCM restarts cleanly.
- The agent CAN run on the same host as Tantor (useful for the local install — sidesteps the multi-worker in-memory state problem for hot paths like the Topics tab refresh).

See `agent/README.md` for build + install instructions.
