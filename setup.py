#!/usr/bin/env python3
import argparse
import os
import stat
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.resolve()
VENV_DIR = ROOT / ".captainenv"
REQ = ROOT / "requirements.txt"
BASHRC = Path.home() / ".bashrc"


def run(cmd):
    print("$", " ".join(cmd))
    subprocess.check_call(cmd)


def ensure_venv():
    if not VENV_DIR.exists():
        run([sys.executable, "-m", "venv", str(VENV_DIR)])
    py = str(VENV_DIR / "bin" / "python")
    # Always invoke pip via the venv's Python to avoid broken shebang/relocation issues.
    try:
        run([py, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
    except subprocess.CalledProcessError:
        # Some systems create a venv without pip available; bootstrap with ensurepip then retry.
        print("pip not available in venv yet; bootstrapping with ensurepip...")
        run([py, "-m", "ensurepip", "--upgrade"])  # ensure pip exists in the venv
        run([py, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])

    if REQ.exists():
        run([py, "-m", "pip", "install", "-r", str(REQ)])
    else:
        print(f"Requirements file not found at {REQ}. Skipping dependency installation.")


WRAPPER_TEMPLATE = """#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="{root}"
VENV_PY="$ROOT_DIR/.captainenv/bin/python"
if [ -x "$VENV_PY" ]; then
  PY="$VENV_PY"
else
  PY="$(command -v python3 || command -v python)"
fi
exec "$PY" "$ROOT_DIR/{target}" "$@"
"""


def write_wrapper(dest: Path, target_script: str):
    content = WRAPPER_TEMPLATE.format(root=str(ROOT), target=target_script)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(content)
    dest.chmod(dest.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    print(f"Installed wrapper: {dest}")


def install_cli(prefix: Path | None = None) -> Path:
    """Install captain/sailor wrappers to a suitable bin directory.
    Returns the directory where files were installed.
    Preference: prefix (if provided) -> /usr/local/bin -> /usr/bin -> ~/.local/bin
    """
    candidates = []
    if prefix:
        candidates.append(prefix)
    candidates.extend([Path("/usr/local/bin"), Path("/usr/bin"), Path.home() / ".local" / "bin"])
    for d in candidates:
        try:
            d.mkdir(parents=True, exist_ok=True)
            if os.access(d, os.W_OK):
                write_wrapper(d / "captain", "captain.py")
                write_wrapper(d / "sailor", "sailor.py")
                return d
        except Exception as e:
            print(f"Skipping {d}: {e}")
    # As a last resort, write to project bin/
    proj_bin = ROOT / "bin"
    write_wrapper(proj_bin / "captain", "captain.py")
    write_wrapper(proj_bin / "sailor", "sailor.py")
    return proj_bin


def main():
    parser = argparse.ArgumentParser(description="Setup Captain/Sailor environment and CLI wrappers")
    parser.add_argument("--prefix", default=None, help="Directory to install CLI wrappers (default: /usr/local/bin if writable, else fallback)")
    parser.add_argument("--no-venv", action="store_true", help="Skip creating/upgrading the virtual environment")
    args = parser.parse_args()

    if not args.no_venv:
        ensure_venv()

    dest = install_cli(Path(args.prefix) if args.prefix else None)
    # Advise PATH if user-local bin
    home_bin = Path.home() / ".local" / "bin"
    if dest == home_bin and str(home_bin) not in os.getenv("PATH", ""):
        print(f"Note: Add {home_bin} to your PATH, e.g., echo 'export PATH=\"{home_bin}:$PATH\"' >> {BASHRC}")
    print(f"CLI wrappers installed to {dest}. Use 'captain --serve 8000' and 'sailor --serve 8001'.")


if __name__ == "__main__":
    main()
