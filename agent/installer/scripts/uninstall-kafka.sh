#!/usr/bin/env bash
# uninstall-kafka.sh — remove a per-cluster install (cleanup on cluster delete).
#
# Idempotent. Wipes /opt/kafka-<slug>-<id>/ and /var/lib/kafka-<slug>-<id>/.
# Does NOT remove the kafka user (shared across clusters on the same host).
#
# Invoked as:
#   sudo /usr/local/lib/tantor-agent/scripts/uninstall-kafka.sh <INSTALL_DIR> <DATA_DIR>
set -euo pipefail

INSTALL_DIR="${1:?INSTALL_DIR required}"
DATA_DIR="${2:?DATA_DIR required}"

case "$INSTALL_DIR" in
    /opt/kafka-*) ;;
    *) echo "INSTALL_DIR must be /opt/kafka-*; got $INSTALL_DIR" >&2; exit 2 ;;
esac
case "$DATA_DIR" in
    /var/lib/kafka-*) ;;
    *) echo "DATA_DIR must be /var/lib/kafka-*; got $DATA_DIR" >&2; exit 2 ;;
esac

rm -rf "$INSTALL_DIR"
rm -rf "$DATA_DIR"
echo "{\"removed\":true,\"install_dir\":\"$INSTALL_DIR\",\"data_dir\":\"$DATA_DIR\"}"
