#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.resolve()
VENV_DIR = ROOT / ".captainenv"
REQ = ROOT / "requirements.txt"
BASHRC = Path.home() / ".bashrc"

ALIASES = f"""
# Captain/Sailor aliases (added by setup.py)
function captain() {{
  source "{VENV_DIR}/bin/activate" && python "{ROOT}/captain.py" "$@"
}}
function sailor() {{
  source "{VENV_DIR}/bin/activate" && python "{ROOT}/sailor.py" "$@"
}}
"""


def run(cmd):
    print("$", " ".join(cmd))
    subprocess.check_call(cmd)


def ensure_venv():
    if not VENV_DIR.exists():
        run([sys.executable, "-m", "venv", str(VENV_DIR)])
    run([str(VENV_DIR / "bin" / "python"), "-m", "pip", "install", "--upgrade", "pip"])
    run([str(VENV_DIR / "bin" / "pip"), "install", "-r", str(REQ)])


def ensure_aliases():
    content = BASHRC.read_text() if BASHRC.exists() else ""
    if "Captain/Sailor aliases (added by setup.py)" not in content:
        with BASHRC.open("a") as f:
            f.write("\n" + ALIASES + "\n")
        print(f"Aliases appended to {BASHRC}. Restart your shell or run: source {BASHRC}")
    else:
        print("Aliases already present in ~/.bashrc")


def main():
    ensure_venv()
    ensure_aliases()
    print("Setup complete. Use 'captain --serve 8000' to start and 'sailor --serve 8001' on sailors.")


if __name__ == "__main__":
    main()
