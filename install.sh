#!/usr/bin/env bash
set -euo pipefail

# System-wide installer for Captain/Sailor
# - Installs files to /opt/captain
# - Creates a dedicated virtualenv at /opt/captain/.captainenv
# - Installs Python dependencies
# - Creates global wrappers: /usr/local/bin/captain and /usr/local/bin/sailor
#
# Usage:
#   curl -fsSL <repo>/install.sh | bash    # or
#   ./install.sh

APP_NAME="captain"
SRC_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTALL_DIR="/opt/captain"
VENV_DIR="$INSTALL_DIR/.captainenv"
BIN_DIR="/usr/local/bin"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Error: required command '$1' not found in PATH" >&2
    exit 1
  }
}

# Elevate if not root
if [ "${EUID:-$(id -u)}" -ne 0 ]; then
  echo "This installer needs root privileges; re-running with sudo..."
  exec sudo -E bash "$0" "$@"
fi

echo "Installing $APP_NAME to $INSTALL_DIR"

require_cmd bash
require_cmd mkdir
require_cmd cp
require_cmd python3

# Prefer rsync or tar for efficient copy with excludes
copy_tree() {
  local src="$1" dst="$2"
  mkdir -p "$dst"
  if command -v rsync >/dev/null 2>&1; then
    rsync -a --delete \
      --exclude='.git' \
      --exclude='.venv' \
      --exclude='.mypy_cache' \
      --exclude='__pycache__' \
      --exclude='*.pyc' \
      "$src"/ "$dst"/
  else
    # Fallback: tar pipeline with excludes
    (cd "$src" && tar --exclude .git --exclude .venv --exclude __pycache__ --exclude '*.pyc' -cf - .) | (cd "$dst" && tar -xf -)
  fi
}

# 1) Copy sources
copy_tree "$SRC_DIR" "$INSTALL_DIR"

# 2) Create/upgrade venv and install deps
PY3="$(command -v python3)"
if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtualenv at $VENV_DIR"
  "$PY3" -m venv "$VENV_DIR" || {
    echo "venv module failed; ensure python3-venv is installed (Debian/Ubuntu: apt install python3-venv)" >&2
    exit 1
  }
fi

VENVPY="$VENV_DIR/bin/python"
echo "Upgrading pip/setuptools/wheel in venv"
if ! "$VENVPY" -m pip install --upgrade pip setuptools wheel; then
  echo "Bootstrapping pip in venv with ensurepip..."
  "$VENVPY" -m ensurepip --upgrade
  "$VENVPY" -m pip install --upgrade pip setuptools wheel
fi

REQ_FILE="$INSTALL_DIR/requirements.txt"
if [ -f "$REQ_FILE" ]; then
  echo "Installing dependencies from $REQ_FILE"
  "$VENVPY" -m pip install -r "$REQ_FILE"
else
  echo "Warning: requirements.txt not found at $REQ_FILE (skipping)"
fi

# 3) Create global wrappers
install_wrapper() {
  local name="$1" target="$2"
  local wrapper="$BIN_DIR/$name"
  mkdir -p "$BIN_DIR"
  cat > "$wrapper" <<EOF
#!/usr/bin/env bash
set -euo pipefail
VENV_PY="$VENV_DIR/bin/python"
exec "\${VENV_PY}" "$INSTALL_DIR/$target" "\$@"
EOF
  chmod 0755 "$wrapper"
  echo "Installed wrapper: $wrapper"
}

install_wrapper captain captain.py
install_wrapper sailor sailor.py

# 4) Ensure runtime data directories exist and are writable
mkdir -p "$INSTALL_DIR/data/captain" "$INSTALL_DIR/data/sailor"
# Note: world-writable to allow non-root usage across users; tighten if desired (e.g., dedicated group)
chmod 0777 "$INSTALL_DIR/data" "$INSTALL_DIR/data/captain" "$INSTALL_DIR/data/sailor" || true

echo
echo "Installation complete. Binaries installed to $BIN_DIR: captain, sailor"
echo "Project location: $INSTALL_DIR"
echo
echo "Try:"
echo "  captain --serve 8000    # start API server"
echo "  sailor --serve 8001     # start worker (first run prompts config)"
echo
echo "Note: Data is stored under $INSTALL_DIR/data (currently world-writable for convenience)."
echo "      For stricter permissions, change ownership to a dedicated user/group and adjust systemd services."
