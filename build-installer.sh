#!/usr/bin/env bash
###############################################################################
#
#   Tantor Kafka Manager — .bin Installer Builder
#
#   Creates a self-extracting .bin installer that bundles:
#     - Pre-built frontend (dist/)
#     - Backend source
#     - install.sh
#     - Kafka binary (optional, if present)
#
#   Usage:
#     ./build-installer.sh                    # Build without Kafka binary
#     ./build-installer.sh --with-kafka       # Bundle Kafka binary (~114MB larger)
#
#   Output: tantor-installer-<version>.bin
#
###############################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VERSION="1.0.0"
WITH_KAFKA=false
OUTPUT="tantor-installer-${VERSION}.bin"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

while [[ $# -gt 0 ]]; do
    case $1 in
        --with-kafka) WITH_KAFKA=true; shift ;;
        --output|-o) OUTPUT="$2"; shift 2 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

echo -e "${CYAN}${BOLD}Tantor Installer Builder v${VERSION}${NC}"
echo ""

# ─── Step 1: Build frontend ───
echo -e "${BLUE}▶ Building frontend...${NC}"
cd "$SCRIPT_DIR/frontend"
if [ ! -d node_modules ]; then
    npm ci --prefer-offline 2>/dev/null || npm install
fi
npm run build 2>&1 | tail -3
if [ ! -f dist/index.html ]; then
    echo -e "${RED}Frontend build failed — dist/index.html not found${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Frontend built${NC}"

# ─── Step 2: Create payload tarball ───
echo -e "${BLUE}▶ Creating payload...${NC}"
cd "$SCRIPT_DIR"

TMPDIR=$(mktemp -d)
PAYLOAD_DIR="$TMPDIR/tantor"
mkdir -p "$PAYLOAD_DIR"

# Copy backend (app + requirements only, skip repo/ansible_work/venv)
mkdir -p "$PAYLOAD_DIR/backend"
cp -r backend/app "$PAYLOAD_DIR/backend/"
cp backend/requirements.txt "$PAYLOAD_DIR/backend/"

# Copy pre-built frontend dist
mkdir -p "$PAYLOAD_DIR/frontend/dist"
cp -r frontend/dist/* "$PAYLOAD_DIR/frontend/dist/"

# Copy installer
cp install.sh "$PAYLOAD_DIR/"

# Optionally bundle Kafka binary
KAFKA_VERSION="4.1.0"
KAFKA_TGZ="kafka_2.13-${KAFKA_VERSION}.tgz"
if [ "$WITH_KAFKA" = true ]; then
    # Look for Kafka binary in common locations
    KAFKA_SRC=""
    for loc in \
        "$SCRIPT_DIR/backend/repo/kafka/$KAFKA_TGZ" \
        "/var/lib/tantor/repo/kafka/$KAFKA_TGZ" \
        "$HOME/Downloads/$KAFKA_TGZ"; do
        if [ -f "$loc" ]; then
            KAFKA_SRC="$loc"
            break
        fi
    done

    if [ -n "$KAFKA_SRC" ]; then
        mkdir -p "$PAYLOAD_DIR/repo/kafka"
        cp "$KAFKA_SRC" "$PAYLOAD_DIR/repo/kafka/"
        echo -e "${GREEN}✓ Kafka binary bundled ($(du -sh "$KAFKA_SRC" | awk '{print $1}'))${NC}"
    else
        echo -e "${YELLOW}⚠ Kafka binary not found locally. Installer will download it.${NC}"
    fi
fi

# Remove unnecessary files
find "$PAYLOAD_DIR" -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
find "$PAYLOAD_DIR" -name '*.pyc' -delete 2>/dev/null || true
find "$PAYLOAD_DIR" -name '.DS_Store' -delete 2>/dev/null || true
find "$PAYLOAD_DIR" -name '._*' -delete 2>/dev/null || true
rm -rf "$PAYLOAD_DIR/frontend/node_modules" 2>/dev/null || true
rm -rf "$PAYLOAD_DIR/frontend/src" 2>/dev/null || true
rm -rf "$PAYLOAD_DIR/frontend/public" 2>/dev/null || true
rm -f "$PAYLOAD_DIR/frontend/package.json" 2>/dev/null || true
rm -f "$PAYLOAD_DIR/frontend/package-lock.json" 2>/dev/null || true
rm -f "$PAYLOAD_DIR/frontend/tsconfig*.json" 2>/dev/null || true
rm -f "$PAYLOAD_DIR/frontend/vite.config.ts" 2>/dev/null || true
rm -f "$PAYLOAD_DIR/frontend/index.html" 2>/dev/null || true
rm -f "$PAYLOAD_DIR/frontend/eslint.config.js" 2>/dev/null || true
rm -rf "$PAYLOAD_DIR/backend/app/__pycache__" 2>/dev/null || true

# Create tarball
PAYLOAD_TAR="$TMPDIR/payload.tar.gz"
cd "$TMPDIR"
tar -czf "$PAYLOAD_TAR" tantor/
PAYLOAD_SIZE=$(du -sh "$PAYLOAD_TAR" | awk '{print $1}')
echo -e "${GREEN}✓ Payload created (${PAYLOAD_SIZE})${NC}"

# ─── Step 3: Create self-extracting .bin ───
echo -e "${BLUE}▶ Building .bin installer...${NC}"

INSTALLER="$SCRIPT_DIR/$OUTPUT"

cat > "$INSTALLER" << 'HEADER_EOF'
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
#   Tantor Kafka Manager — Self-Extracting Installer
#
#   Usage:
#     sudo ./tantor-installer-1.0.0.bin              # Install
#     sudo ./tantor-installer-1.0.0.bin --uninstall   # Remove
#     ./tantor-installer-1.0.0.bin --info             # Show info
#     ./tantor-installer-1.0.0.bin --extract <dir>    # Extract only
#
###############################################################################

set -e

ARCHIVE_LINE=$(awk '/^__ARCHIVE_BELOW__$/{print NR + 1; exit 0;}' "$0")
VERSION="1.0.0"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

show_info() {
    echo -e "${CYAN}${BOLD}Tantor Kafka Manager v${VERSION}${NC}"
    echo ""
    echo "  Self-extracting installer for Linux (Ubuntu/Debian/RHEL/CentOS)"
    echo "  Installs: Python backend, React frontend, Nginx, systemd service"
    echo ""
    echo "  Usage:"
    echo "    sudo $0              Install Tantor"
    echo "    sudo $0 --uninstall  Remove Tantor"
    echo "    $0 --extract <dir>   Extract files without installing"
    echo "    $0 --info            Show this info"
}

# Parse arguments
EXTRACT_ONLY=false
EXTRACT_DIR=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --info|-i) show_info; exit 0 ;;
        --extract|-x)
            EXTRACT_ONLY=true
            EXTRACT_DIR="${2:-.}"
            shift 2
            ;;
        *) EXTRA_ARGS+=("$1"); shift ;;
    esac
done

# Extract payload
TMPDIR=$(mktemp -d /tmp/tantor-install.XXXXXX)
trap "rm -rf $TMPDIR" EXIT

echo -e "${CYAN}Extracting Tantor installer...${NC}"
tail -n +${ARCHIVE_LINE} "$0" | tar -xzf - -C "$TMPDIR" 2>/dev/null

if [ ! -d "$TMPDIR/tantor" ]; then
    echo -e "${RED}Error: Failed to extract installer payload${NC}"
    exit 1
fi

# Extract-only mode
if [ "$EXTRACT_ONLY" = true ]; then
    mkdir -p "$EXTRACT_DIR"
    cp -r "$TMPDIR/tantor/"* "$EXTRACT_DIR/"
    echo -e "${GREEN}✓ Extracted to: $EXTRACT_DIR${NC}"
    exit 0
fi

# Must be root for installation
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: Installation requires root. Run: sudo $0${NC}"
    exit 1
fi

# Run the installer
cd "$TMPDIR/tantor"
exec bash ./install.sh "${EXTRA_ARGS[@]}"

# Everything below this line is the compressed archive
__ARCHIVE_BELOW__
HEADER_EOF

# Append the payload tarball
cat "$PAYLOAD_TAR" >> "$INSTALLER"
chmod +x "$INSTALLER"

# Cleanup
rm -rf "$TMPDIR"

BIN_SIZE=$(du -sh "$INSTALLER" | awk '{print $1}')
echo -e "${GREEN}✓ Installer built: ${BOLD}${OUTPUT}${NC} (${BIN_SIZE})"
echo ""
echo -e "${CYAN}To install on a server:${NC}"
echo "  scp $OUTPUT user@server:/tmp/"
echo "  ssh user@server 'sudo /tmp/$OUTPUT'"
echo ""
echo -e "${CYAN}Other options:${NC}"
echo "  ./$OUTPUT --info              # Show info"
echo "  ./$OUTPUT --extract ./tantor  # Extract without installing"
echo "  sudo ./$OUTPUT --uninstall    # Remove Tantor"
echo ""
