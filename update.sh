#!/usr/bin/env bash
set -euo pipefail

# Update script for Captain/Sailor
# - Pull latest changes from git remote (in this repo)
# - Sync only modified files to /opt/captain (preserving data and venv)
# - Optionally update Python deps in /opt/captain/.captainenv
#
# Usage:
#   ./update.sh            # pull + sync
#   ./update.sh --no-deps  # skip dependency install step
#
APP_NAME="captain"
SRC_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTALL_DIR="/opt/captain"
VENV_DIR="$INSTALL_DIR/.captainenv"
UPDATE_DEPS=1
# Frontend paths
FRONT_SRC="$SRC_DIR/front"
FRONT_DEST="$INSTALL_DIR/front"

for arg in "$@"; do
  case "$arg" in
    --no-deps) UPDATE_DEPS=0 ;;
    *) echo "Unknown argument: $arg" >&2; exit 2 ;;
  esac
done

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Error: required command '$1' not found in PATH" >&2
    exit 1
  }
}

require_cmd git
require_cmd bash

# 1) Update local repository
if [ -d "$SRC_DIR/.git" ]; then
  echo "[1/3] Pulling latest changes in $SRC_DIR"
  # Fetch and pull current branch
  current_branch="$(git -C "$SRC_DIR" rev-parse --abbrev-ref HEAD)"
  git -C "$SRC_DIR" fetch --all --prune
  if ! git -C "$SRC_DIR" pull --ff-only origin "$current_branch"; then
    echo "Fast-forward failed; trying rebase with autostash"
    git -C "$SRC_DIR" pull --rebase --autostash origin "$current_branch" || {
      echo "Warning: git pull (rebase) failed; continuing with local state" >&2
    }
  fi
else
  echo "Warning: $SRC_DIR is not a git repository; skipping 'git pull'"
fi

# 2) Sync files to /opt/captain
require_cmd rsync || true

echo "[2/3] Syncing repo files to $INSTALL_DIR (preserving data/ and .captainenv/)"
RSYNC_EXCLUDES=(
  --exclude='.git'
  --exclude='.venv'
  --exclude='.mypy_cache'
  --exclude='__pycache__'
  --exclude='*.pyc'
  --exclude='.captainenv'
  --exclude='data/'
)

# Ensure destination exists and owned by root
if [ "${EUID:-$(id -u)}" -ne 0 ]; then
  echo "Using sudo for privileged operations..."
  sudo mkdir -p "$INSTALL_DIR"
  # Use rsync to copy only changed files; delete removed files but keep data/ and .captainenv/
  sudo rsync -a --delete "${RSYNC_EXCLUDES[@]}" "$SRC_DIR"/ "$INSTALL_DIR"/
else
  mkdir -p "$INSTALL_DIR"
  rsync -a --delete "${RSYNC_EXCLUDES[@]}" "$SRC_DIR"/ "$INSTALL_DIR"/
fi

echo "[2/3] Sync complete."

# 2b) Fully replace the front directory (remove and copy)
if [ -d "$FRONT_SRC" ]; then
  echo "[2b/3] Replacing $FRONT_DEST from $FRONT_SRC"
  if [ "${EUID:-$(id -u)}" -ne 0 ]; then
    sudo rm -rf "$FRONT_DEST"
    sudo mkdir -p "$FRONT_DEST"
    sudo rsync -a "$FRONT_SRC"/ "$FRONT_DEST"/
  else
    rm -rf "$FRONT_DEST"
    mkdir -p "$FRONT_DEST"
    rsync -a "$FRONT_SRC"/ "$FRONT_DEST"/
  fi
else
  echo "No front directory found at $FRONT_SRC (skipping front sync)"
fi

# 3) Optionally update deps inside the existing virtualenv
if [ "$UPDATE_DEPS" = "1" ]; then
  if [ -d "$VENV_DIR" ]; then
    echo "[3/3] Updating Python dependencies in $VENV_DIR"
    if [ -f "$INSTALL_DIR/requirements.txt" ]; then
      if [ "${EUID:-$(id -u)}" -ne 0 ]; then
        sudo "$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel
        sudo "$VENV_DIR/bin/python" -m pip install -r "$INSTALL_DIR/requirements.txt"
      else
        "$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel
        "$VENV_DIR/bin/python" -m pip install -r "$INSTALL_DIR/requirements.txt"
      fi
    else
      echo "requirements.txt not found at $INSTALL_DIR/requirements.txt (skipping deps)"
    fi
  else
    echo "Virtualenv not found at $VENV_DIR (skipping deps)"
  fi
else
  echo "Dependency update skipped (--no-deps)"
fi

echo "Update complete. Location: $INSTALL_DIR"

sudo service sailor restart || sudo service lieutenant restart || sudo service lieutenant-web restart