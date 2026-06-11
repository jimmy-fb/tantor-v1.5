#!/usr/bin/env bash
###############################################################################
#
#   ████████╗ █████╗ ███╗   ██╗████████╗ ██████╗ ██████╗
#   ╚══██╔══╝██╔══██╗████╗  ██║╚══██╔══╝██╔═══██╗██╔══██╗
#      ██║   ███████║██╔██╗ ██║   ██║   ██║   ██║██████╔╝
#      ██║   ██╔══██║██║╚██╗██║   ██║   ██║   ██║██╔══██╗
#      ██║   ██║  ██║██║ ╚████║   ██║   ╚██████╔╝██║  ██║
#      ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═══╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝
#
#   Tantor Kafka Manager — One-Click Installer v1.0.0
#
#   Usage:
#     sudo ./install.sh              # Install (auto-detects OS)
#     sudo ./install.sh --uninstall  # Remove Tantor
#
#   Supported OS:
#     Ubuntu 20.04+, Debian 11+, RHEL 8+, CentOS Stream 8+,
#     Rocky Linux 8+, AlmaLinux 8+, Oracle Linux 8+, Amazon Linux 2023
#
#   After install:
#     Open http://<your-ip> in a browser
#     Login: admin / admin
#
###############################################################################

set -e

VERSION="1.4.5"

# Default paths follow FHS: code in /opt, mutable data in /var/lib, logs in /var/log.
# `--install-dir <BASE>` collapses everything under BASE so customers with a
# dedicated /data, /apps, etc. mountpoint don't need three separate paths
# scattered across the system. customer asked for this in v1.2.0 #1 / v1.1 #43.
#
# We remember the operator's choice in /etc/tantor/install.conf so a later
# `--reinstall` (or `--purge`) without --install-dir doesn't accidentally
# install a second copy at /opt/tantor while the customer's real install
# is at /data/tantor.
TANTOR_HOME="/opt/tantor"
TANTOR_DATA="/var/lib/tantor"
TANTOR_LOG="/var/log/tantor"
TANTOR_USER="tantor"
if [ -f /etc/tantor/install.conf ]; then
    # shellcheck disable=SC1091
    . /etc/tantor/install.conf
fi
KAFKA_VERSION="4.1.0"
KAFKA_SCALA="2.13"
KAFKA_TGZ="kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

FORCE=false
UNINSTALL=false
PURGE=false
REINSTALL=false
TLS_ENABLE=false
TLS_CERT_PATH=""
TLS_KEY_PATH=""
INSTALL_DIR="$(cd "$(dirname "$0")" && pwd)"

# ─── Install log + error trap ────────────────────────────────────────────────
# Persist EVERYTHING to a file so frozen / failed installs are diagnosable.
# Without this, customers see "stuck on step N" with no log to send back.
INSTALL_LOG="/var/log/tantor-install.log"
mkdir -p "$(dirname "$INSTALL_LOG")" 2>/dev/null || true
: > "$INSTALL_LOG" 2>/dev/null || INSTALL_LOG="/tmp/tantor-install.log"
exec 3>>"$INSTALL_LOG"  # fd 3 is the "verbose only" sink
echo "=== Tantor installer v${VERSION} started at $(date -u +%Y-%m-%dT%H:%M:%SZ) ===" >&3

# Run a command, write its stdout+stderr to the log file, do NOT show on screen.
# If it fails, print the exit code and the last 40 lines of log so the operator
# sees something actionable instead of a blank "stuck on step N" screen.
log_run() {
    local label="$1"; shift
    echo "+ [$label] $*" >&3
    if ! ( "$@" >&3 2>&1 ); then
        local rc=$?
        echo "" >&2
        echo -e "${RED}✗ [$label] failed (exit $rc)${NC}" >&2
        echo -e "${YELLOW}  Last log lines (full log: $INSTALL_LOG):${NC}" >&2
        tail -n 40 "$INSTALL_LOG" >&2 || true
        return $rc
    fi
}

# Same idea but with a hard timeout — for commands that can hang on network
# stalls (curl, dnf metadata refresh on broken proxies, etc.).
log_run_timeout() {
    local secs="$1"; local label="$2"; shift 2
    echo "+ [$label] (timeout ${secs}s) $*" >&3
    if ! timeout "$secs" "$@" >&3 2>&1; then
        local rc=$?
        echo "" >&2
        if [ $rc -eq 124 ]; then
            echo -e "${RED}✗ [$label] timed out after ${secs}s${NC}" >&2
            echo -e "${YELLOW}  Likely a network / firewall / proxy issue.${NC}" >&2
        else
            echo -e "${RED}✗ [$label] failed (exit $rc)${NC}" >&2
        fi
        echo -e "${YELLOW}  Last log lines (full log: $INSTALL_LOG):${NC}" >&2
        tail -n 40 "$INSTALL_LOG" >&2 || true
        return $rc
    fi
}

# Background heartbeat — prints "still working" every 30s during long steps so
# the operator knows the installer hasn't frozen.
_HEARTBEAT_PID=""
heartbeat_start() {
    local label="$1"
    heartbeat_stop  # belt-and-braces in case of nested calls
    (
        local n=0
        while sleep 30; do
            n=$((n + 30))
            echo -e "  ${YELLOW}… ${label} still running (${n}s elapsed)${NC}"
        done
    ) &
    _HEARTBEAT_PID=$!
    disown 2>/dev/null || true
}
heartbeat_stop() {
    if [ -n "$_HEARTBEAT_PID" ] && kill -0 "$_HEARTBEAT_PID" 2>/dev/null; then
        kill "$_HEARTBEAT_PID" 2>/dev/null || true
        wait "$_HEARTBEAT_PID" 2>/dev/null || true
    fi
    _HEARTBEAT_PID=""
}

# Trap any unhandled error and tell the operator where to look.
on_error() {
    local rc=$?
    local line=$1
    heartbeat_stop
    echo "" >&2
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}" >&2
    echo -e "${RED}✗ Tantor install failed (line $line, exit $rc)${NC}" >&2
    echo -e "${YELLOW}  Full log:    $INSTALL_LOG${NC}" >&2
    echo -e "${YELLOW}  Send this file to your Tantor contact for diagnosis.${NC}" >&2
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}" >&2
    exit $rc
}
trap 'on_error $LINENO' ERR
trap 'heartbeat_stop' EXIT

# ─── Parse Arguments ───
while [[ $# -gt 0 ]]; do
    case $1 in
        --force|-f|--yes|-y)  FORCE=true; shift ;;
        --uninstall)          UNINSTALL=true; shift ;;
        # --purge implies --uninstall AND wipes the data dir (DB + repos).
        --purge)              UNINSTALL=true; PURGE=true; FORCE=true; shift ;;
        # --reinstall = uninstall (preserve data) then run a fresh install.
        --reinstall)          REINSTALL=true; FORCE=true; shift ;;
        # --install-dir <BASE> overrides the default FHS layout. App goes to
        # BASE/app, data to BASE/data, logs to BASE/log. The runtime backend
        # picks these up via the TANTOR_HOME / TANTOR_DATA / TANTOR_LOG env
        # vars on its systemd unit, so nothing else is hardcoded.
        --install-dir|--prefix)
            if [ -z "$2" ]; then echo "Error: --install-dir requires a path"; exit 1; fi
            BASE="$2"
            TANTOR_HOME="$BASE/app"
            TANTOR_DATA="$BASE/data"
            TANTOR_LOG="$BASE/log"
            shift 2
            ;;
        # v1.4.0 #12 — wrap the Tantor UI in HTTPS. With no cert+key
        # paths supplied, the installer mints a self-signed cert valid
        # for the host's IP/hostname. Operators that already have a real
        # cert can pass --tls-cert and --tls-key.
        --tls)
            TLS_ENABLE=true
            shift
            ;;
        --tls-cert)
            if [ -z "$2" ]; then echo "Error: --tls-cert requires a path"; exit 1; fi
            TLS_ENABLE=true
            TLS_CERT_PATH="$2"
            shift 2
            ;;
        --tls-key)
            if [ -z "$2" ]; then echo "Error: --tls-key requires a path"; exit 1; fi
            TLS_ENABLE=true
            TLS_KEY_PATH="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: sudo ./install.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --force                 Skip confirmation prompts"
            echo "  --uninstall             Remove Tantor (preserves data dir)"
            echo "  --purge                 Uninstall AND wipe data + DB + repos"
            echo "  --reinstall             Uninstall + fresh install in one step (data preserved)"
            echo "  --install-dir <BASE>    Install everything under BASE/{app,data,log}"
            echo "                          Default: /opt/tantor + /var/lib/tantor + /var/log/tantor"
            echo "  --tls                   Enable HTTPS for the Tantor UI (auto-generates a self-signed cert)"
            echo "  --tls-cert <path>       Use this cert file instead of self-signing (PEM)"
            echo "  --tls-key  <path>       Use this private key (PEM, must match --tls-cert)"
            echo "  --help                  Show this message"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ─── Root Check ───
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: Please run as root (sudo ./install.sh)${NC}"
    exit 1
fi

# ─── Detect OS ───
detect_os() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        case "$ID" in
            ubuntu|debian|linuxmint|pop)
                OS_FAMILY="debian"
                OS_NAME="$PRETTY_NAME"
                ;;
            rhel|centos|rocky|almalinux|ol|fedora|amzn)
                OS_FAMILY="rhel"
                OS_NAME="$PRETTY_NAME"
                ;;
            *)
                case "$ID_LIKE" in
                    *debian*|*ubuntu*) OS_FAMILY="debian"; OS_NAME="$PRETTY_NAME" ;;
                    *rhel*|*fedora*|*centos*) OS_FAMILY="rhel"; OS_NAME="$PRETTY_NAME" ;;
                    *) echo -e "${RED}Unsupported OS: $PRETTY_NAME${NC}"; exit 1 ;;
                esac
                ;;
        esac
    else
        echo -e "${RED}Cannot detect OS (missing /etc/os-release)${NC}"
        exit 1
    fi
}

# ─── Uninstall ───
do_uninstall() {
    echo -e "${YELLOW}▶ Uninstalling Tantor...${NC}"
    # v1.4.3 #14/#15 — enumerate every per-cluster Kafka unit. Prior
    # to this we only stopped the legacy `kafka.service`, leaving every
    # `kafka-<slug>-<id>.service` running and 15+ stale data dirs piling
    # up across reinstall cycles.
    KAFKA_UNITS=$(systemctl list-unit-files --no-legend --no-pager 2>/dev/null \
                  | awk '$1 ~ /^kafka-.*\.service/ {print $1}')
    if [ -n "$KAFKA_UNITS" ]; then
        echo -e "${YELLOW}  Stopping per-cluster Kafka units: $(echo $KAFKA_UNITS | tr '\n' ' ')${NC}"
    fi
    for unit in $KAFKA_UNITS tantor-backend kafka schema-registry prometheus alertmanager grafana-server; do
        if systemctl list-unit-files --no-legend --no-pager 2>/dev/null | grep -q "^$unit"; then
            systemctl stop "$unit" 2>/dev/null || true
            systemctl disable "$unit" 2>/dev/null || true
        fi
    done
    # Murder any Kafka JVM stragglers (systemd Type=simple with start.sh
    # wrappers can leave forked children behind on failed restarts).
    pkill -9 -f '/opt/kafka' 2>/dev/null || true

    rm -f /etc/systemd/system/tantor-backend.service
    rm -f /etc/systemd/system/kafka.service
    # v1.4.3 #14 — remove every per-cluster unit file.
    rm -f /etc/systemd/system/kafka-*.service
    rm -f /etc/systemd/system/schema-registry.service
    rm -f /etc/systemd/system/prometheus.service
    rm -f /etc/systemd/system/alertmanager.service
    rm -rf /etc/systemd/system/tantor-backend.service.d
    rm -f /etc/nginx/sites-enabled/tantor.conf
    rm -f /etc/nginx/conf.d/tantor.conf
    # Restore stock RHEL nginx.conf if we replaced it during install — without
    # this, nginx after uninstall still points to a Tantor-mangled config and
    # may fail to start.
    if [ -f /etc/nginx/nginx.conf.tantor-bak ]; then
        mv /etc/nginx/nginx.conf.tantor-bak /etc/nginx/nginx.conf
    fi
    systemctl daemon-reload 2>/dev/null || true
    systemctl reset-failed 2>/dev/null || true
    systemctl restart nginx 2>/dev/null || true
    rm -rf "$TANTOR_HOME"
    rm -rf "$TANTOR_LOG"
    rm -rf /opt/kafka /opt/apicurio /opt/prometheus /opt/alertmanager /opt/jmx_exporter
    # v1.4.3 #14 — per-cluster install dirs (kafka-<slug>-<id>).
    rm -rf /opt/kafka-*
    rm -rf /etc/kafka/ssl /etc/prometheus /etc/alertmanager
    rm -f /usr/local/bin/tantorctl
    if [ "$PURGE" = true ]; then
        echo -e "${YELLOW}  --purge requested: also wiping data at $TANTOR_DATA${NC}"
        rm -rf "$TANTOR_DATA"
        # Kafka, Apicurio, Prometheus etc. own their own data dirs — wipe those
        # too on --purge, otherwise stale meta.properties / KRaft cluster IDs
        # break the next deploy with "Invalid cluster.id".
        rm -rf /var/lib/kafka /var/log/kafka /var/lib/prometheus /var/lib/grafana /var/lib/alertmanager
        # v1.4.3 #14 — per-cluster data dirs + log dirs.
        rm -rf /var/lib/kafka-* /var/log/kafka-*
        # Grafana ships as a deb/rpm package and its dpkg/rpm state survives
        # a plain `rm -rf` on its data dir — leaving its postinst convinced
        # it's still "installed" and skipping data dir recreation on next
        # deploy. Fully purge the package state too.
        if command -v dpkg-query &>/dev/null && dpkg-query -W grafana &>/dev/null; then
            DEBIAN_FRONTEND=noninteractive dpkg --purge --force-all grafana 2>/dev/null || true
        fi
        if command -v rpm &>/dev/null && rpm -q grafana &>/dev/null; then
            dnf remove -y grafana 2>/dev/null || rpm -e --nodeps grafana 2>/dev/null || true
        fi
        # nginx dpkg/rpm state can stay — we only manage its config drop-in.
        # Wipe install log + tantor user home (userdel -r is best-effort).
        rm -f /var/log/tantor-install.log
        rm -rf /home/${TANTOR_USER}
        # Drop the install layout file so the next install starts at default
        # paths unless --install-dir is supplied again.
        rm -rf /etc/tantor
        # Also remove the system user since we're nuking everything.
        userdel -r "$TANTOR_USER" 2>/dev/null || true
    fi
    echo -e "${GREEN}✓ Tantor removed${NC}"
    if [ "$PURGE" != true ]; then
        echo -e "${YELLOW}  Data preserved at: $TANTOR_DATA${NC}"
        echo "  To wipe data on next run: sudo $0 --purge"
    fi
}

if [ "$UNINSTALL" = true ]; then
    do_uninstall
    exit 0
fi

# Reinstall: uninstall first, preserving data, then continue into install.
if [ "$REINSTALL" = true ]; then
    do_uninstall
    echo ""
    echo -e "${BLUE}▶ Continuing with fresh install...${NC}"
    echo ""
fi

# ─── Banner ───
echo -e "${CYAN}"
echo "  ████████╗ █████╗ ███╗   ██╗████████╗ ██████╗ ██████╗ "
echo "  ╚══██╔══╝██╔══██╗████╗  ██║╚══██╔══╝██╔═══██╗██╔══██╗"
echo "     ██║   ███████║██╔██╗ ██║   ██║   ██║   ██║██████╔╝"
echo "     ██║   ██╔══██║██║╚██╗██║   ██║   ██║   ██║██╔══██╗"
echo "     ██║   ██║  ██║██║ ╚████║   ██║   ╚██████╔╝██║  ██║"
echo "     ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═══╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝"
echo -e "${NC}"
echo -e "  ${BOLD}Kafka Cluster Manager — One-Click Installer v${VERSION}${NC}"
echo ""

detect_os
echo -e "  ${BLUE}OS Detected:${NC}  $OS_NAME"
echo -e "  ${BLUE}OS Family:${NC}    $OS_FAMILY"
echo -e "  ${BLUE}App dir:${NC}      $TANTOR_HOME"
echo -e "  ${BLUE}Data dir:${NC}     $TANTOR_DATA"
echo -e "  ${BLUE}Log dir:${NC}      $TANTOR_LOG"
echo ""

# ─── Verify source ───
if [ ! -d "$INSTALL_DIR/backend/app" ]; then
    echo -e "${RED}Error: backend/app not found. Run from the tantor repo root.${NC}"
    exit 1
fi

# ─── Confirm ───
if [ "$FORCE" != true ]; then
    echo -e "${YELLOW}This will install Tantor on this machine.${NC}"
    read -p "  Continue? [y/N] " CONFIRM
    case "$CONFIRM" in [yY]|[yY][eE][sS]) ;; *) echo "Cancelled."; exit 0 ;; esac
fi

# ─── Step 1: System Dependencies ───
echo -e "\n${BLUE}▶ Step 1/9: Installing system dependencies...${NC}"

install_deps_debian() {
    export DEBIAN_FRONTEND=noninteractive
    echo -e "  apt-get update..."
    heartbeat_start "apt-get update"
    log_run_timeout 300 "apt-get update" apt-get update -qq
    heartbeat_stop

    echo -e "  Installing python, nginx, ssh, curl..."
    heartbeat_start "apt install (core packages)"
    log_run_timeout 600 "apt install core" apt-get install -y -qq \
        python3 python3-pip python3-venv python3-full \
        nginx \
        openssh-client sshpass \
        wget curl jq gnupg ca-certificates net-tools
    heartbeat_stop

    if ! command -v node &>/dev/null || [ "$(node -v | cut -d. -f1 | tr -d v)" -lt 18 ]; then
        echo -e "  Adding NodeSource repo + installing nodejs 20..."
        heartbeat_start "NodeSource setup + nodejs"
        log_run_timeout 120 "NodeSource setup_20.x" bash -c \
            "curl -fsSL --connect-timeout 15 --max-time 90 https://deb.nodesource.com/setup_20.x | bash -"
        log_run_timeout 300 "apt install nodejs" apt-get install -y -qq nodejs
        heartbeat_stop
    fi
    echo -e "${GREEN}✓ System packages installed${NC}"
}

install_deps_rhel() {
    echo -e "  Installing EPEL..."
    log_run_timeout 180 "dnf install epel-release" bash -c "dnf install -y -q epel-release || true"

    # RHEL 8.x ships Python 3.6, RHEL/Rocky 9 ships 3.9.
    # ansible-core 2.16+ requires Python 3.10+, so install Python 3.11 from AppStream
    # for anything older than 3.10.
    local py_ver
    py_ver=$(python3 -c 'import sys; print(sys.version_info.minor)' 2>/dev/null || echo "0")
    if [ "$py_ver" -lt 10 ]; then
        echo -e "  Default Python 3.${py_ver} too old for ansible-core 2.16+, installing Python 3.11..."
        heartbeat_start "dnf install python3.11"
        log_run_timeout 60 "dnf module enable python311" bash -c "dnf module enable -y python311 || true"
        log_run_timeout 600 "dnf install python3.11" dnf install -y -q python3.11 python3.11-pip python3.11-devel
        heartbeat_stop
        if command -v python3.11 &>/dev/null; then
            log_run "alternatives python3.11" bash -c \
                "alternatives --set python3 /usr/bin/python3.11 2>/dev/null || \
                 alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 2>/dev/null || true"
        else
            echo -e "${RED}ERROR: Failed to install Python 3.11. See $INSTALL_LOG${NC}" >&2
            exit 1
        fi
    fi

    echo -e "  Installing nginx, ssh, curl..."
    heartbeat_start "dnf install (core packages)"
    log_run_timeout 600 "dnf install core" dnf install -y -q \
        nginx \
        openssh-clients sshpass \
        wget curl jq ca-certificates net-tools
    heartbeat_stop

    if ! command -v node &>/dev/null || [ "$(node -v | cut -d. -f1 | tr -d v)" -lt 18 ]; then
        echo -e "  Adding NodeSource repo + installing nodejs 20..."
        heartbeat_start "NodeSource setup + nodejs"
        log_run_timeout 120 "NodeSource setup_20.x" bash -c \
            "curl -fsSL --connect-timeout 15 --max-time 90 https://rpm.nodesource.com/setup_20.x | bash -"
        log_run_timeout 300 "dnf install nodejs" dnf install -y -q nodejs
        heartbeat_stop
    fi
    echo -e "${GREEN}✓ System packages installed${NC}"
}

if [ "$OS_FAMILY" = "debian" ]; then
    install_deps_debian
else
    install_deps_rhel
fi

# ─── Step 2: Create User & Directories ───
echo -e "${BLUE}▶ Step 2/9: Creating user and directories...${NC}"

id "$TANTOR_USER" &>/dev/null || useradd -r -m -s /bin/bash "$TANTOR_USER"

# (#42) Make sure parent dirs exist with traversable permissions BEFORE
# creating Tantor's tree. On systems where /opt or /var/lib doesn't exist,
# mkdir creates them with `umask 0077` (root-only), which makes the tantor
# system user fail to traverse and the backend service crashes on boot.
for parent in /opt /var/lib /var/log; do
    if [ ! -d "$parent" ]; then
        mkdir -p "$parent"
    fi
    chmod 755 "$parent"
done

mkdir -p \
    "$TANTOR_HOME/backend" \
    "$TANTOR_HOME/frontend/dist" \
    "$TANTOR_HOME/bin" \
    "$TANTOR_DATA/db" \
    "$TANTOR_DATA/repo/kafka" \
    "$TANTOR_DATA/repo/ksqldb" \
    "$TANTOR_DATA/repo/connect-plugins" \
    "$TANTOR_DATA/repo/monitoring" \
    "$TANTOR_DATA/repo/apicurio" \
    "$TANTOR_DATA/ansible_work" \
    "$TANTOR_DATA/ssh" \
    "$TANTOR_DATA/backups" \
    "$TANTOR_DATA/certs" \
    "$TANTOR_DATA/secrets" \
    "$TANTOR_LOG/backend" \
    "$TANTOR_LOG/nginx"

# Migrate Fernet/JWT secrets from the old in-tree location to the data dir on
# upgrade. /opt/tantor is wiped on --reinstall but /var/lib/tantor is preserved,
# so secrets must live with the data they decrypt — see backend/app/config.py.
if [ -d "$TANTOR_HOME/backend/.secrets" ] && [ ! -f "$TANTOR_DATA/secrets/fernet.key" ]; then
    cp -an "$TANTOR_HOME/backend/.secrets/." "$TANTOR_DATA/secrets/" 2>/dev/null || true
fi

# Set ownership + traversable permissions on the Tantor tree right after
# creation so the service user can read/write before chown -R at the end.
chown -R "$TANTOR_USER:$TANTOR_USER" "$TANTOR_HOME" "$TANTOR_DATA" "$TANTOR_LOG"
chmod 755 "$TANTOR_HOME" "$TANTOR_DATA" "$TANTOR_LOG"

# When --install-dir is used (e.g. /data/tantor), SELinux blocks systemd
# from writing logs because the custom path doesn't carry var_log_t /
# usr_t / var_lib_t labels. Set them explicitly so systemd-managed
# StandardOutput= and exec policy work on enforcing systems.
if command -v chcon >/dev/null 2>&1 && [ -f /sys/fs/selinux/enforce ]; then
    chcon -R -t var_log_t   "$TANTOR_LOG"  2>/dev/null || true
    chcon -R -t var_lib_t   "$TANTOR_DATA" 2>/dev/null || true
    chcon -R -t bin_t       "$TANTOR_HOME" 2>/dev/null || true
fi

# Grant tantor user passwordless sudo
echo "${TANTOR_USER} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/tantor
chmod 440 /etc/sudoers.d/tantor

# Persist install layout so future invocations of install.sh (e.g.
# --reinstall, --purge) keep using the same paths instead of silently
# reverting to /opt/tantor. customer hit this when their --reinstall created
# a parallel install while the original was at /data/tantor.
mkdir -p /etc/tantor
cat > /etc/tantor/install.conf <<CONFEOF
# Auto-generated by tantor installer. Edit at your own risk — run
# 'sudo /tmp/tantor-installer.bin --install-dir <new>' to relocate.
TANTOR_HOME="$TANTOR_HOME"
TANTOR_DATA="$TANTOR_DATA"
TANTOR_LOG="$TANTOR_LOG"
TANTOR_USER="$TANTOR_USER"
TLS_ENABLE=$TLS_ENABLE
CONFEOF
chmod 644 /etc/tantor/install.conf

echo -e "${GREEN}✓ User '${TANTOR_USER}' and directories created${NC}"

# ─── Step 3: Build Frontend ───
echo -e "${BLUE}▶ Step 3/9: Building frontend...${NC}"

# Check if pre-built dist exists (tarball install)
if [ -d "$INSTALL_DIR/frontend/dist" ] && [ -f "$INSTALL_DIR/frontend/dist/index.html" ]; then
    echo -e "${GREEN}✓ Frontend already built (pre-built dist found)${NC}"
else
    # Build from source (GitHub clone install)
    cd "$INSTALL_DIR/frontend"
    npm ci --prefer-offline 2>/dev/null || npm install 2>/dev/null
    npm run build 2>/dev/null
    cd "$INSTALL_DIR"
    echo -e "${GREEN}✓ Frontend built from source${NC}"
fi

# ─── Step 4: Python Dependencies ───
echo -e "${BLUE}▶ Step 4/9: Installing Python dependencies...${NC}"

# Pick the newest available python — ansible-core 2.16+ wants 3.10+. Prefer python3.11
# (installed by us on RHEL 8/9) over the system python3 which may be 3.6 or 3.9.
PYTHON_BIN=$(command -v python3.11 || command -v python3.12 || command -v python3.10 || command -v python3)
echo "  Using $PYTHON_BIN ($($PYTHON_BIN --version 2>&1))"

"$PYTHON_BIN" -m venv "$TANTOR_HOME/venv"
"$TANTOR_HOME/venv/bin/pip" install --upgrade pip -q 2>/dev/null
"$TANTOR_HOME/venv/bin/pip" install -q -r "$INSTALL_DIR/backend/requirements.txt"

echo -e "${GREEN}✓ Python venv created and dependencies installed${NC}"

# ─── Step 5: Install Backend ───
echo -e "${BLUE}▶ Step 5/9: Installing backend...${NC}"

cp -r "$INSTALL_DIR/backend/app" "$TANTOR_HOME/backend/"
cp "$INSTALL_DIR/backend/requirements.txt" "$TANTOR_HOME/backend/"

# Symlinks for persistent data
ln -sf "$TANTOR_DATA/db/tantor.db" "$TANTOR_HOME/backend/tantor.db"
ln -sf "$TANTOR_DATA/repo" "$TANTOR_HOME/backend/repo"
ln -sf "$TANTOR_DATA/ansible_work" "$TANTOR_HOME/backend/ansible_work"

# Create .env with CORS
SERVER_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "localhost")
cat > "$TANTOR_HOME/backend/.env" << ENVEOF
CORS_ORIGINS=["http://localhost","http://127.0.0.1","http://${SERVER_IP}"]
ENVEOF

echo -e "${GREEN}✓ Backend installed${NC}"

# ─── Step 6: Install Frontend ───
echo -e "${BLUE}▶ Step 6/9: Installing frontend...${NC}"

cp -r "$INSTALL_DIR/frontend/dist/"* "$TANTOR_HOME/frontend/dist/"

# Validate. customer hit a case where --purge + reinstall left an empty dist/
# because of a network issue during npm ci — verify the bundle exists,
# the assets directory has at least one .js file, and index.html mentions
# at least one of those .js files. If any of these fail, abort with a
# clear error instead of finishing "successfully" and leaving a blank UI.
if [ ! -f "$TANTOR_HOME/frontend/dist/index.html" ]; then
    echo -e "${RED}ERROR: frontend/dist/index.html is missing after copy.${NC}" >&2
    echo "  Source dir: $INSTALL_DIR/frontend/dist (probably empty in this build)." >&2
    exit 1
fi
JS_COUNT=$(ls "$TANTOR_HOME/frontend/dist/assets/"*.js 2>/dev/null | wc -l | tr -d ' ')
if [ "${JS_COUNT:-0}" -lt 1 ]; then
    echo -e "${RED}ERROR: no JavaScript bundle in $TANTOR_HOME/frontend/dist/assets/${NC}" >&2
    echo "  The .bin was packaged with a broken frontend dist. Re-build the .bin." >&2
    exit 1
fi
if ! grep -q "/assets/" "$TANTOR_HOME/frontend/dist/index.html"; then
    echo -e "${RED}ERROR: frontend/dist/index.html does not reference /assets/ — UI will not load.${NC}" >&2
    exit 1
fi
echo -e "${GREEN}✓ Frontend installed (${JS_COUNT} JS bundle$([ "$JS_COUNT" = "1" ] || echo s))${NC}"

# ─── Step 7: Download Kafka Binary ───
echo -e "${BLUE}▶ Step 7/9: Installing Kafka binary...${NC}"

KAFKA_DEST="$TANTOR_DATA/repo/kafka/$KAFKA_TGZ"

# Check bundled (tarball install), local repo, or download
if [ -f "$INSTALL_DIR/repo/kafka/$KAFKA_TGZ" ]; then
    cp "$INSTALL_DIR/repo/kafka/$KAFKA_TGZ" "$KAFKA_DEST"
    echo -e "${GREEN}✓ Kafka ${KAFKA_VERSION} installed from bundle${NC}"
elif [ -f "$INSTALL_DIR/backend/repo/kafka/$KAFKA_TGZ" ]; then
    cp "$INSTALL_DIR/backend/repo/kafka/$KAFKA_TGZ" "$KAFKA_DEST"
    echo -e "${GREEN}✓ Kafka ${KAFKA_VERSION} installed from local repo${NC}"
elif [ -f "$KAFKA_DEST" ]; then
    echo -e "${GREEN}✓ Kafka ${KAFKA_VERSION} already present${NC}"
else
    echo -e "${YELLOW}  Downloading Kafka ${KAFKA_VERSION} (~113 MB)...${NC}"
    # Try the primary mirror first, then archive, with one retry each.
    # archive.apache.org keeps every release; downloads.apache.org rotates point
    # releases off the CDN as new ones ship. Hit archive first — it always works.
    KAFKA_URLS=(
        "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"
        "https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"
    )
    DOWNLOAD_OK=0
    for url in "${KAFKA_URLS[@]}"; do
        for attempt in 1 2; do
            echo -e "${YELLOW}    [$attempt/2] $url${NC}"
            if curl -fSL --connect-timeout 15 --max-time 600 --retry 0 --progress-bar -o "$KAFKA_DEST" "$url"; then
                DOWNLOAD_OK=1
                break 2
            fi
            rm -f "$KAFKA_DEST"
            sleep 2
        done
    done

    if [ $DOWNLOAD_OK -eq 1 ]; then
        echo -e "${GREEN}✓ Kafka ${KAFKA_VERSION} downloaded ($(du -sh "$KAFKA_DEST" | awk '{print $1}'))${NC}"
    else
        # Don't claim success — be explicit so the operator knows the state.
        echo -e "${YELLOW}⚠ Could not download Kafka ${KAFKA_VERSION} from Apache mirrors.${NC}"
        echo -e "${YELLOW}  Tantor will auto-download it on the first cluster deploy.${NC}"
        echo -e "${YELLOW}  Air-gapped? Place ${KAFKA_TGZ} at:${NC}"
        echo -e "${YELLOW}    $KAFKA_DEST${NC}"
        echo -e "${YELLOW}  or upload via the Kafka Versions page after install.${NC}"
    fi
fi

# ─── Step 8: Configure Services ───
echo -e "${BLUE}▶ Step 8/9: Configuring services...${NC}"

# v1.4.0 #12 — optionally generate / accept a TLS cert for the UI.
if [ "$TLS_ENABLE" = true ]; then
    TLS_DIR="/etc/tantor/tls"
    mkdir -p "$TLS_DIR"
    chmod 750 "$TLS_DIR"
    if [ -n "$TLS_CERT_PATH" ] && [ -n "$TLS_KEY_PATH" ]; then
        echo -e "${BLUE}  Using operator-supplied TLS material${NC}"
        cp "$TLS_CERT_PATH" "$TLS_DIR/server.crt"
        cp "$TLS_KEY_PATH"  "$TLS_DIR/server.key"
    elif [ -f "$TLS_DIR/server.crt" ] && [ -f "$TLS_DIR/server.key" ]; then
        echo -e "${BLUE}  Reusing existing TLS cert at $TLS_DIR${NC}"
    else
        echo -e "${BLUE}  Generating self-signed TLS cert (valid 825 days)${NC}"
        HOST_FQDN="$(hostname -f 2>/dev/null || hostname)"
        HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
        SAN="DNS:${HOST_FQDN},DNS:localhost,IP:127.0.0.1"
        if [ -n "$HOST_IP" ]; then SAN="$SAN,IP:${HOST_IP}"; fi
        log_run "tls-keygen" openssl req -x509 -nodes -newkey rsa:2048 \
            -keyout "$TLS_DIR/server.key" \
            -out    "$TLS_DIR/server.crt" \
            -days 825 \
            -subj "/CN=${HOST_FQDN}" \
            -addext "subjectAltName=${SAN}" || true
    fi
    chmod 600 "$TLS_DIR/server.key" 2>/dev/null || true
    chmod 644 "$TLS_DIR/server.crt" 2>/dev/null || true
fi

# Nginx config. Single-quoted heredoc keeps nginx variables ($host, $uri,
# $remote_addr) literal; we sed-substitute __TANTOR_HOME__ afterwards so the
# `--install-dir` override propagates without breaking nginx's own variables.
if [ "$TLS_ENABLE" = true ]; then
    NGINX_CONF='# v1.4.0 #12 — HTTPS for the Tantor UI. The :80 server
# unconditionally redirects to https so bookmarks keep working.
server {
    listen 80 default_server;
    server_name _;
    return 301 https://$host$request_uri;
}

server {
    # Use the legacy listen-with-http2 form: RHEL 9 ships nginx 1.20
    # which does not recognize the standalone http2 directive (introduced
    # in 1.25.1). Both forms work on 1.25+.
    listen 443 ssl http2 default_server;
    server_name _;

    ssl_certificate     /etc/tantor/tls/server.crt;
    ssl_certificate_key /etc/tantor/tls/server.key;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    ssl_prefer_server_ciphers on;

    root __TANTOR_HOME__/frontend/dist;
    index index.html;

    client_max_body_size 500M;

    gzip on;
    gzip_types text/plain text/css application/json application/javascript text/xml application/xml text/javascript image/svg+xml;
    gzip_min_length 256;

    location /api/ {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
    }

    location ^~ /grafana/ {
        proxy_pass http://127.0.0.1:3000/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location / {
        try_files $uri $uri/ /index.html;
    }

    location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg|woff|woff2|ttf|eot)$ {
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}'
else
    NGINX_CONF='server {
    listen 80 default_server;
    server_name _;

    root __TANTOR_HOME__/frontend/dist;
    index index.html;

    client_max_body_size 500M;

    gzip on;
    gzip_types text/plain text/css application/json application/javascript text/xml application/xml text/javascript image/svg+xml;
    gzip_min_length 256;

    location /api/ {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
    }

    location ^~ /grafana/ {
        proxy_pass http://127.0.0.1:3000/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location / {
        try_files $uri $uri/ /index.html;
    }

    location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg|woff|woff2|ttf|eot)$ {
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}'
fi
NGINX_CONF="${NGINX_CONF//__TANTOR_HOME__/$TANTOR_HOME}"

if [ "$OS_FAMILY" = "debian" ]; then
    echo "$NGINX_CONF" > /etc/nginx/sites-enabled/tantor.conf
    rm -f /etc/nginx/sites-enabled/default
else
    echo "$NGINX_CONF" > /etc/nginx/conf.d/tantor.conf
    rm -f /etc/nginx/conf.d/default.conf
    # Replace the stock RHEL nginx.conf with a minimal version that drops the
    # embedded default server block (which would otherwise win on :80) but keeps
    # the include of conf.d/*.conf so our tantor.conf takes over.
    if [ ! -f /etc/nginx/nginx.conf.tantor-bak ]; then
        cp /etc/nginx/nginx.conf /etc/nginx/nginx.conf.tantor-bak
    fi
    cat > /etc/nginx/nginx.conf <<'NGXEOF'
user nginx;
worker_processes auto;
error_log /var/log/nginx/error.log;
pid /run/nginx.pid;

include /usr/share/nginx/modules/*.conf;

events {
    worker_connections 1024;
}

http {
    log_format  main  '$remote_addr - $remote_user [$time_local] "$request" '
                      '$status $body_bytes_sent "$http_referer" '
                      '"$http_user_agent" "$http_x_forwarded_for"';
    access_log  /var/log/nginx/access.log  main;
    sendfile            on;
    tcp_nopush          on;
    tcp_nodelay         on;
    keepalive_timeout   65;
    types_hash_max_size 4096;
    include             /etc/nginx/mime.types;
    default_type        application/octet-stream;

    include /etc/nginx/conf.d/*.conf;
}
NGXEOF
    # SELinux
    if command -v setsebool &>/dev/null; then
        setsebool -P httpd_can_network_connect 1 2>/dev/null || true
    fi
fi

# Systemd service. Use double-quoted heredoc + escaped $MAINPID so the
# install-time TANTOR_HOME / TANTOR_DATA / TANTOR_LOG variables interpolate
# but systemd's own $MAINPID stays literal.
cat > /etc/systemd/system/tantor-backend.service <<SYSEOF
[Unit]
Description=Tantor Kafka Manager — Backend API
After=network.target
Wants=nginx.service

[Service]
Type=simple
User=${TANTOR_USER}
Group=${TANTOR_USER}
WorkingDirectory=${TANTOR_HOME}/backend
Environment=DATABASE_URL=sqlite:///${TANTOR_DATA}/db/tantor.db
Environment=TANTOR_HOME=${TANTOR_HOME}
Environment=TANTOR_DATA=${TANTOR_DATA}
Environment=TANTOR_LOG=${TANTOR_LOG}
Environment=TANTOR_SECRETS_DIR=${TANTOR_DATA}/secrets
Environment=ANSIBLE_WORKING_DIR=${TANTOR_DATA}/ansible_work
Environment=KAFKA_REPO_DIR=${TANTOR_DATA}/repo/kafka
Environment=APICURIO_REPO_DIR=${TANTOR_DATA}/repo/apicurio
Environment=PYTHONUNBUFFERED=1
ExecStart=${TANTOR_HOME}/venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 2
ExecReload=/bin/kill -HUP \$MAINPID
Restart=always
RestartSec=5
StandardOutput=append:${TANTOR_LOG}/backend/stdout.log
StandardError=append:${TANTOR_LOG}/backend/stderr.log
NoNewPrivileges=false
ProtectSystem=false

[Install]
WantedBy=multi-user.target
SYSEOF

systemctl daemon-reload
systemctl enable tantor-backend nginx >/dev/null 2>&1

# Symlink ansible binaries
ln -sf "$TANTOR_HOME/venv/bin/ansible-playbook" /usr/local/bin/ansible-playbook 2>/dev/null || true
ln -sf "$TANTOR_HOME/venv/bin/ansible" /usr/local/bin/ansible 2>/dev/null || true

# Setup SSH for tantor user (for deploying Kafka to remote hosts)
mkdir -p /home/${TANTOR_USER}/.ssh /home/${TANTOR_USER}/.ansible/tmp
if [ ! -f /home/${TANTOR_USER}/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -b 4096 -f /home/${TANTOR_USER}/.ssh/id_rsa -N "" -q
fi
chown -R "${TANTOR_USER}:${TANTOR_USER}" /home/${TANTOR_USER}
chmod 700 /home/${TANTOR_USER}/.ssh
chmod 600 /home/${TANTOR_USER}/.ssh/id_rsa 2>/dev/null || true

# Centralized-server mode: authorize tantor's pubkey for root@localhost so
# Tantor can SSH-deploy Kafka onto its own host without operator intervention.
mkdir -p /root/.ssh
touch /root/.ssh/authorized_keys
chmod 700 /root/.ssh
chmod 600 /root/.ssh/authorized_keys
TANTOR_PUBKEY=$(cat /home/${TANTOR_USER}/.ssh/id_rsa.pub)
if ! grep -qF "${TANTOR_PUBKEY}" /root/.ssh/authorized_keys 2>/dev/null; then
    echo "${TANTOR_PUBKEY}" >> /root/.ssh/authorized_keys
fi

# Auto-register localhost as a Host so the operator can deploy a cluster
# in one click. Stores the encrypted private key directly in the DB so the
# usual Tantor host-deploy code path works against this VM.
"$TANTOR_HOME/venv/bin/python3" - <<PYEOF || echo "  (localhost auto-register skipped)"
import os, sys, uuid
sys.path.insert(0, "${TANTOR_HOME}/backend")
os.environ["TANTOR_HOME"] = "${TANTOR_HOME}"
os.environ["TANTOR_DATA"] = "${TANTOR_DATA}"
os.environ["TANTOR_LOG"] = "${TANTOR_LOG}"
os.environ["TANTOR_SECRETS_DIR"] = "${TANTOR_DATA}/secrets"
os.environ.setdefault("DATABASE_URL", "sqlite:///${TANTOR_DATA}/db/tantor.db")
from app.database import Base, engine, SessionLocal
from app.models.host import Host
from app.services.crypto import encrypt
Base.metadata.create_all(bind=engine)
db = SessionLocal()
try:
    if not db.query(Host).filter(Host.hostname == "localhost").first():
        with open("/home/${TANTOR_USER}/.ssh/id_rsa") as f:
            key_pem = f.read()
        host = Host(
            id=str(uuid.uuid4()), hostname="localhost", ip_address="127.0.0.1",
            ssh_port=22, username="root", auth_type="key",
            encrypted_credential=encrypt(key_pem), os_info="local", status="online",
        )
        db.add(host); db.commit()
        print(f"  Localhost registered as host id={host.id}")
finally:
    db.close()
PYEOF

echo -e "${GREEN}✓ Nginx, systemd, SSH, and localhost host configured${NC}"

# ─── Step 9: Start Services ───
echo -e "${BLUE}▶ Step 9/9: Starting services...${NC}"

chown -R "$TANTOR_USER:$TANTOR_USER" "$TANTOR_HOME" "$TANTOR_DATA" "$TANTOR_LOG"
chmod -R o+r "$TANTOR_HOME/frontend/dist"

# Re-apply SELinux labels after the recursive chown (chown can reset xattrs).
if command -v chcon >/dev/null 2>&1 && [ -f /sys/fs/selinux/enforce ]; then
    chcon -R -t var_log_t   "$TANTOR_LOG"  2>/dev/null || true
    chcon -R -t var_lib_t   "$TANTOR_DATA" 2>/dev/null || true
    chcon -R -t bin_t       "$TANTOR_HOME" 2>/dev/null || true
fi

systemctl restart nginx
systemctl restart tantor-backend

# Wait for health. Hit local nginx via the right scheme so the
# health-check actually exercises the live config (TLS cert + redirect
# work end-to-end before we declare success).
if [ "$TLS_ENABLE" = true ]; then
    HEALTH_URL="https://localhost/api/health"
    HEALTH_CURL_OPTS="-k"
    PUBLIC_URL="https://${SERVER_IP}"
else
    HEALTH_URL="http://localhost/api/health"
    HEALTH_CURL_OPTS=""
    PUBLIC_URL="http://${SERVER_IP}"
fi

echo -n "  Waiting for Tantor to start"
for i in $(seq 1 30); do
    HTTP=$(curl -sf $HEALTH_CURL_OPTS -o /dev/null -w "%{http_code}" "$HEALTH_URL" 2>/dev/null || echo "000")
    if [ "$HTTP" = "200" ]; then
        echo ""
        break
    fi
    echo -n "."
    sleep 2
done

if [ "$HTTP" = "200" ]; then
    echo -e "${GREEN}✓ Tantor is running!${NC}"
else
    echo ""
    echo -e "${YELLOW}⚠ Tantor is still starting. Check: journalctl -u tantor-backend${NC}"
fi

# ─── Done ───
echo ""
echo -e "${CYAN}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║           Installation Complete!                    ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${GREEN}Open in browser:${NC}  ${PUBLIC_URL}"
if [ "$TLS_ENABLE" = true ]; then
    echo -e "  ${YELLOW}TLS:${NC}              self-signed cert at /etc/tantor/tls/server.crt"
    echo -e "                    your browser will warn on first connect — that's expected"
fi
echo -e "  ${GREEN}Login:${NC}            admin / admin"
echo ""
echo -e "  ${BLUE}Quick start:${NC}"
echo "    1. Open the UI and change the default password"
echo "    2. Add your Linux servers as hosts (Hosts → Add Host)"
echo "    3. Create a Kafka cluster (Clusters → Create)"
echo "    4. Deploy and manage from the UI"
echo ""
echo -e "  ${BLUE}Service commands:${NC}"
echo "    systemctl status tantor-backend    — Check backend"
echo "    systemctl status nginx             — Check nginx"
echo "    journalctl -u tantor-backend -f    — View logs"
echo ""
echo -e "  ${BLUE}Install log:${NC}      $INSTALL_LOG"
echo ""
