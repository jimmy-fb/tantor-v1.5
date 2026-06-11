# tantor-agent

Optional host-side agent for Tantor. Connects OUT to a Tantor management server (SCM) over WebSocket-over-TLS and executes Kafka operations locally (systemctl, journalctl, kafka-*.sh, file read/write within an allowlist).

When deployed, Tantor's backend prefers the agent over SSH for the operations it supports — same UX, lower latency, and zero inbound network access required on the broker host. If the agent isn't connected, Tantor falls back to SSH+CLI as before. **The agent is optional.**

See [`docs/AGENT_PROTOCOL.md`](../docs/AGENT_PROTOCOL.md) for the wire protocol.

---

## Why install the agent

| You should install the agent if… | You can skip it if… |
|---|---|
| Your security policy forbids inbound SSH from the management server | You're fine with the existing SSH-based deploy |
| You're in an air-gapped environment with no outbound from the SCM to brokers | You have free SSH connectivity in both directions |
| You want fine-grained sudo (allowlist of specific Kafka commands) instead of broad SSH access | Broad SSH-user-with-sudo is acceptable |
| You want sub-second status probes and live log tailing | 1-3s SSH-based probes are acceptable |

The agent and SSH are not mutually exclusive — operators can deploy the agent to high-security clusters and leave SSH for everything else.

---

## Build

```bash
cd agent
go build -ldflags="-s -w" -o tantor-agent ./cmd/tantor-agent
```

Produces a static linux/amd64 binary (~8 MB). Cross-compile for arm64:

```bash
GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o tantor-agent-arm64 ./cmd/tantor-agent
```

Run the unit tests:

```bash
go test ./...
```

---

## Install on a broker host

### 1. Copy the binary

```bash
sudo install -m 755 -o root -g root tantor-agent /usr/local/bin/tantor-agent
```

### 2. Create the service account + dirs

```bash
sudo useradd --system --shell /usr/sbin/nologin --home-dir /var/lib/tantor-agent tantor-agent
sudo install -d -o tantor-agent -g tantor-agent -m 0700 /var/lib/tantor-agent /etc/tantor-agent
```

### 3. Drop the config file

```bash
sudo tee /etc/tantor-agent/config.yaml > /dev/null <<'EOF'
# Tantor SCM endpoint. Use wss:// in production; ws:// only for local dev.
scm_url: "wss://tantor.your-bank.internal/api/agents/connect"

# One-shot registration token from Tantor UI → Hosts → <host> → Generate agent token.
# Wiped after first successful connect (the agent persists a long-lived JWT to
# /var/lib/tantor-agent/agent.jwt instead).
registration_token: "REPLACE_ME_PASTE_FROM_TANTOR_UI"

# Local allowlist of operations. The SCM cannot send operations not in this list.
# See docs/AGENT_PROTOCOL.md section 7 for the catalog.
allowed_operations:
  - systemctl.is_active
  - systemctl.status
  - systemctl.start
  - systemctl.stop
  - systemctl.restart
  - journalctl.read
  - journalctl.tail
  - file.read:/etc/kafka/
  - file.read:/opt/kafka-*/config/
  - file.write:/opt/kafka-*/config/server.properties
  - kafka_cli.topics
  - kafka_cli.configs
  - kafka_cli.acls
  - kafka_cli.consumer_groups
  - exec.ss
  - exec.systemd-cgls

# TLS validation. true is the production setting; false only for self-signed
# SCMs in dev/lab environments.
tls_verify: true

# Optional: pinned CA bundle for the SCM. If empty, system trust store is used.
# ca_bundle_path: /etc/tantor-agent/scm-ca.pem
EOF
sudo chmod 0600 /etc/tantor-agent/config.yaml
sudo chown root:tantor-agent /etc/tantor-agent/config.yaml
```

### 4. Install the sudoers rule

```bash
sudo install -m 0440 -o root -g root \
  installer/sudoers.d/tantor-agent /etc/sudoers.d/tantor-agent
sudo visudo -c -f /etc/sudoers.d/tantor-agent   # validate
```

This grants `tantor-agent` passwordless sudo on a hardcoded list of `systemctl`, `journalctl`, `cat`, and `install` commands scoped to `/etc/kafka/*` and `/opt/kafka-*/`. No shell escape.

### 5. Install the systemd unit

```bash
sudo install -m 0644 -o root -g root \
  installer/systemd/tantor-agent.service /etc/systemd/system/tantor-agent.service
sudo systemctl daemon-reload
sudo systemctl enable --now tantor-agent
sudo systemctl status tantor-agent
```

### 6. Verify

Open Tantor UI → Hosts. The host you just installed the agent on should show a green "Agent connected" badge within ~15s.

`journalctl -u tantor-agent -f` on the broker host shows registration + heartbeat events.

---

## Air-gapped install

Build the binary on a machine with internet, then ship the resulting `tantor-agent` plus the `installer/` directory as a tarball:

```bash
cd agent
tar czf tantor-agent-1.5.0.tgz tantor-agent installer/
# scp to the air-gapped host, then untar + run the install steps above.
```

A `.deb` and `.rpm` build pipeline is on the roadmap (post-MVP).

---

## Config file reference

| Key | Type | Default | Meaning |
|---|---|---|---|
| `scm_url` | string | (required) | `wss://host/api/agents/connect` |
| `registration_token` | string | (required on first connect) | One-shot token from Tantor UI |
| `allowed_operations` | list[string] | (required) | Allowlist of `op` strings the agent will accept from the SCM |
| `tls_verify` | bool | `true` | Set `false` only for dev/lab with self-signed SCM |
| `ca_bundle_path` | string | `""` | Pin a specific CA bundle for the SCM's TLS cert |
| `heartbeat_interval_sec` | int | `15` | Override SCM's default heartbeat cadence |
| `state_dir` | string | `/var/lib/tantor-agent` | Where the persisted agent JWT lives |

---

## Troubleshooting

| Symptom | First thing to check |
|---|---|
| Agent doesn't show up in Tantor UI | `journalctl -u tantor-agent -n 50` on the broker host. Look for `register failed:` or `dial:` errors. |
| `auth_failed` in agent logs | Registration token expired (1-hour TTL) or wrong host. Regenerate from the Tantor UI. |
| Operations refused with `denied` | Op isn't in the agent's `allowed_operations`. Edit `/etc/tantor-agent/config.yaml`, `systemctl restart tantor-agent`. |
| TLS handshake fails | `tls_verify: true` but SCM has a self-signed cert. Either set `tls_verify: false` (dev only) or pin the CA via `ca_bundle_path`. |
| Heartbeats arrive but commands don't | Multi-worker SCM issue: agent landed on a different worker than the one handling the API request. Tantor backend has a known limitation here; see `docs/AGENT_PROTOCOL.md` section 10. |
