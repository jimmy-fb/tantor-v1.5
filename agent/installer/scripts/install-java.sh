#!/usr/bin/env bash
# install-java.sh — ensure a JDK is available on this host.
#
# Called via `exec.script` from Tantor. Invoked as:
#   sudo /usr/local/lib/tantor-agent/scripts/install-java.sh [JAVA_PKG_OVERRIDE]
#
# Idempotent. If java is already on PATH and the version is >= 17, exits 0
# without touching the package manager.
set -euo pipefail

OVERRIDE="${1:-}"

JAVA_VER=$( (java -version 2>&1 | head -1 | awk -F'"' '{print $2}') 2>/dev/null || true )
if [ -n "$JAVA_VER" ]; then
    MAJOR=$(echo "$JAVA_VER" | awk -F. '{print $1}')
    if [ "$MAJOR" -ge 17 ] 2>/dev/null; then
        echo "{\"java_already_installed\":true,\"version\":\"$JAVA_VER\"}"
        exit 0
    fi
fi

PKG=""
PM=""
if [ -n "$OVERRIDE" ]; then
    PKG="$OVERRIDE"
fi

if command -v dnf >/dev/null 2>&1; then
    PM="dnf"
    PKG="${PKG:-java-17-openjdk-headless}"
elif command -v apt-get >/dev/null 2>&1; then
    PM="apt-get"
    PKG="${PKG:-openjdk-17-jre-headless}"
elif command -v yum >/dev/null 2>&1; then
    PM="yum"
    PKG="${PKG:-java-17-openjdk-headless}"
else
    echo "no package manager found (dnf/apt-get/yum)" >&2
    exit 1
fi

case "$PM" in
    apt-get) apt-get update -y && apt-get install -y "$PKG" ;;
    dnf|yum) "$PM" install -y "$PKG" ;;
esac

JAVA_VER=$( (java -version 2>&1 | head -1 | awk -F'"' '{print $2}') 2>/dev/null || echo unknown )
echo "{\"installed\":true,\"package\":\"$PKG\",\"version\":\"$JAVA_VER\",\"package_manager\":\"$PM\"}"
