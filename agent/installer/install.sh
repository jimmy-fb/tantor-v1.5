#!/usr/bin/env bash
# tantor-agent installer.
#
# Run as root on the broker host. Idempotent — safe to re-run for upgrades
# (preserves /etc/tantor-agent/config.yaml and /var/lib/tantor-agent/agent.jwt).
#
# Usage:
#   sudo ./install.sh                        # installs from sibling files
#   sudo ./install.sh --binary tantor-agent  # specify binary location
#   sudo ./install.sh --uninstall            # remove agent (keeps config)

set -euo pipefail

BINARY=""
UNINSTALL=0

while [ $# -gt 0 ]; do
    case "$1" in
        --binary) BINARY="$2"; shift 2 ;;
        --uninstall) UNINSTALL=1; shift ;;
        -h|--help)
            cat <<EOF
tantor-agent installer

Usage:
    sudo ./install.sh [--binary PATH] [--uninstall]

  --binary PATH    Path to the tantor-agent binary. Default: ./tantor-agent
                   next to this script.
  --uninstall      Stop + remove the agent. Preserves /etc/tantor-agent and
                   /var/lib/tantor-agent so a re-install picks up the same
                   identity.
EOF
            exit 0
            ;;
        *) echo "unknown flag: $1" >&2; exit 2 ;;
    esac
done

if [ "$(id -u)" != "0" ]; then
    echo "must run as root" >&2; exit 1
fi

HERE="$(cd "$(dirname "$0")" && pwd)"

if [ "$UNINSTALL" -eq 1 ]; then
    systemctl disable --now tantor-agent 2>/dev/null || true
    rm -f /etc/systemd/system/tantor-agent.service
    rm -f /etc/sudoers.d/tantor-agent
    rm -f /usr/local/bin/tantor-agent
    systemctl daemon-reload
    echo "tantor-agent uninstalled."
    echo "Note: /etc/tantor-agent/ and /var/lib/tantor-agent/ kept on disk."
    echo "      Remove manually if you don't plan to re-install."
    exit 0
fi

if [ -z "$BINARY" ]; then
    BINARY="$HERE/../tantor-agent"
fi
if [ ! -f "$BINARY" ]; then
    echo "binary not found at $BINARY (use --binary PATH)" >&2
    exit 1
fi

# 1) service account
if ! id tantor-agent >/dev/null 2>&1; then
    useradd --system --shell /usr/sbin/nologin --home-dir /var/lib/tantor-agent tantor-agent
fi
install -d -o tantor-agent -g tantor-agent -m 0700 /var/lib/tantor-agent
install -d -o root -g tantor-agent -m 0750 /etc/tantor-agent

# 2) binary
install -m 0755 -o root -g root "$BINARY" /usr/local/bin/tantor-agent

# 3) systemd unit
install -m 0644 -o root -g root "$HERE/systemd/tantor-agent.service" \
    /etc/systemd/system/tantor-agent.service

# 4) sudoers (validate before placing — bad syntax would lock out sudo entirely)
TMP_SUDOERS="$(mktemp)"
cp "$HERE/sudoers.d/tantor-agent" "$TMP_SUDOERS"
visudo -c -f "$TMP_SUDOERS" >/dev/null
install -m 0440 -o root -g root "$TMP_SUDOERS" /etc/sudoers.d/tantor-agent
rm -f "$TMP_SUDOERS"

# 5) starter config — only written when none exists
if [ ! -f /etc/tantor-agent/config.yaml ]; then
    cat > /etc/tantor-agent/config.yaml <<'EOF'
# Tantor SCM endpoint. Use wss:// in production.
scm_url: "wss://tantor.example.internal/api/agents/connect"

# One-shot token from Tantor UI → Hosts → <host> → Generate agent token.
registration_token: "REPLACE_ME"

allowed_operations:
  - systemctl.is_active
  - systemctl.status
  - systemctl.start
  - systemctl.stop
  - systemctl.restart
  - journalctl.read
  - file.read:/etc/kafka/
  - file.read:/opt/kafka-*/config/
  - file.write:/opt/kafka-*/config/server.properties
  - kafka_cli.topics
  - kafka_cli.configs
  - kafka_cli.acls
  - kafka_cli.consumer_groups
  - exec.ss
  - exec.systemd-cgls

tls_verify: true
EOF
    chmod 0640 /etc/tantor-agent/config.yaml
    chown root:tantor-agent /etc/tantor-agent/config.yaml
    echo
    echo "*** Edit /etc/tantor-agent/config.yaml — set scm_url + registration_token before starting ***"
    echo
fi

systemctl daemon-reload

echo "tantor-agent installed."
echo "Next steps:"
echo "  1. Edit /etc/tantor-agent/config.yaml (scm_url + registration_token)"
echo "  2. systemctl enable --now tantor-agent"
echo "  3. journalctl -u tantor-agent -f"
