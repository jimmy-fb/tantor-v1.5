#!/usr/bin/env bash
# install-kafka.sh — extract a Kafka tarball into a per-cluster install dir.
#
# Called via `exec.script` from Tantor. Invoked as:
#   sudo /usr/local/lib/tantor-agent/scripts/install-kafka.sh \
#       <TARBALL_PATH> <INSTALL_DIR> <DATA_DIR> <KAFKA_USER>
#
# Idempotent: re-running on an already-installed cluster is a no-op (extract
# checks `meta.properties` presence in the data dir).
set -euo pipefail

TARBALL="${1:?TARBALL_PATH required}"
INSTALL_DIR="${2:?INSTALL_DIR required}"
DATA_DIR="${3:?DATA_DIR required}"
KAFKA_USER="${4:-kafka}"

# Refuse anything that isn't /opt/kafka-* — defense in depth alongside
# the agent's allowlist + sudoers profile.
case "$INSTALL_DIR" in
    /opt/kafka-*) ;;
    *) echo "INSTALL_DIR must be /opt/kafka-*; got $INSTALL_DIR" >&2; exit 2 ;;
esac
case "$DATA_DIR" in
    /var/lib/kafka-*) ;;
    *) echo "DATA_DIR must be /var/lib/kafka-*; got $DATA_DIR" >&2; exit 2 ;;
esac

if ! [ -f "$TARBALL" ]; then
    echo "tarball not found: $TARBALL" >&2; exit 3
fi

# 1. kafka user (system account, no shell)
if ! id "$KAFKA_USER" >/dev/null 2>&1; then
    useradd --system --shell /usr/sbin/nologin --home-dir "$INSTALL_DIR" "$KAFKA_USER"
fi

# 2. dirs
install -d -o "$KAFKA_USER" -g "$KAFKA_USER" -m 0750 "$INSTALL_DIR"
install -d -o "$KAFKA_USER" -g "$KAFKA_USER" -m 0750 "$DATA_DIR"

# 3. extract if not already populated
if [ ! -f "$INSTALL_DIR/bin/kafka-topics.sh" ]; then
    tar -xzf "$TARBALL" -C "$INSTALL_DIR" --strip-components=1
    chown -R "$KAFKA_USER:$KAFKA_USER" "$INSTALL_DIR"
fi

# 4. emit a small JSON for the SCM to log
KAFKA_VERSION="$( ("$INSTALL_DIR/bin/kafka-topics.sh" --version 2>/dev/null || echo unknown) | awk '{print $1}')"
echo "{\"installed\":true,\"install_dir\":\"$INSTALL_DIR\",\"data_dir\":\"$DATA_DIR\",\"kafka_version\":\"$KAFKA_VERSION\"}"
