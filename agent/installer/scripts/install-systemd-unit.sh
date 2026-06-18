#!/usr/bin/env bash
# install-systemd-unit.sh — wire a generated systemd unit file into place.
#
# Called via `exec.script` from Tantor after file.write has staged the unit
# at /etc/systemd/system/<unit>. Just runs daemon-reload + enable.
#
# Invoked as:
#   sudo /usr/local/lib/tantor-agent/scripts/install-systemd-unit.sh <UNIT_NAME>
set -euo pipefail

UNIT="${1:?UNIT_NAME required}"
case "$UNIT" in
    kafka-*.service|zookeeper-*.service) ;;
    *) echo "unit name must be kafka-* or zookeeper-*; got $UNIT" >&2; exit 2 ;;
esac

if [ ! -f "/etc/systemd/system/$UNIT" ]; then
    echo "unit file /etc/systemd/system/$UNIT not present" >&2; exit 3
fi

systemctl daemon-reload
systemctl enable "$UNIT"
echo "{\"unit\":\"$UNIT\",\"enabled\":true}"
