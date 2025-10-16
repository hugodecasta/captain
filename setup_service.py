#!/usr/bin/env python3
import argparse
import getpass
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.resolve()
SERVICES_DIR = ROOT / "services"
CAPTAIN_TPL = SERVICES_DIR / "captain.service.tpl"
SAILOR_TPL = SERVICES_DIR / "sailor.service.tpl"

DATA_SAILOR_DIR = ROOT / "data" / "sailor"
RESOURCES_JSON = DATA_SAILOR_DIR / "resources.json"


def run(cmd) -> None:
    print("$", " ".join(cmd))
    subprocess.check_call(cmd)


def detect_python() -> str:
    # Prefer the venv created by setup.py if present
    venv_py = ROOT / ".captainenv" / "bin" / "python"
    if venv_py.exists():
        return str(venv_py)
    return sys.executable


def load_json(path: Path):
    try:
        if path.exists():
            return json.loads(path.read_text())
    except Exception:
        pass
    return None


def ensure_sailor_resources():
    # If resources.json is missing or empty, run the first-run prompt in sailor.py
    data = load_json(RESOURCES_JSON) or {}
    if data:
        return
    print("No sailor resources found. Launching interactive prompt to set them up...")
    py = detect_python()
    # Running sailor.py without --serve triggers the prompt logic on first run
    run([py, str(ROOT / "sailor.py")])


def make_unit(template_path: Path, port: int, user: str) -> str:
    py = detect_python()
    tpl = template_path.read_text()
    unit = (
        tpl.replace("__ROOT__", str(ROOT))
        .replace("__PYTHON__", py)
        .replace("__PORT__", str(port))
        .replace("__USER__", user)
    )
    return unit


def install_systemd_unit(name: str, content: str):
    tmp = Path(f"/tmp/{name}.service")
    tmp.write_text(content)
    try:
        run(["sudo", "mv", str(tmp), f"/etc/systemd/system/{name}.service"])
        run(["sudo", "chmod", "644", f"/etc/systemd/system/{name}.service"])
        run(["sudo", "systemctl", "daemon-reload"])
        run(["sudo", "systemctl", "enable", f"{name}.service"])
        run(["sudo", "systemctl", "restart", f"{name}.service"])
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description="Install and enable systemd services for Captain/Sailor")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--captain", action="store_true", help="Setup Captain service")
    group.add_argument("--sailor", action="store_true", help="Setup Sailor service")
    parser.add_argument("--port", type=int, help="Service port to use (prompts if omitted)")
    parser.add_argument("--user", type=str, default=getpass.getuser(), help="Linux user to run the service as")
    args = parser.parse_args()

    if args.captain:
        port = args.port or int(input("Captain port [8000]: ") or 8000)
        content = make_unit(CAPTAIN_TPL, port, args.user)
        install_systemd_unit("captain", content)
        print(f"Captain service installed on port {port}. View logs: sudo journalctl -u captain -f")
        return

    if args.sailor:
        # Ensure initial sailor configuration exists before installing the service
        ensure_sailor_resources()
        # If prompt set a port, use it as default
        res = load_json(RESOURCES_JSON) or {}
        default_port = int(res.get("port", 8001) or 8001)
        port = args.port or int(input(f"Sailor port [{default_port}]: ") or default_port)
        # Persist selected port back to resources.json so sailor registers with that
        try:
            res["port"] = port
            DATA_SAILOR_DIR.mkdir(parents=True, exist_ok=True)
            (RESOURCES_JSON.with_suffix(".tmp")).write_text(json.dumps(res, indent=2))
            os.replace(RESOURCES_JSON.with_suffix(".tmp"), RESOURCES_JSON)
        except Exception:
            pass
        content = make_unit(SAILOR_TPL, port, args.user)
        install_systemd_unit("sailor", content)
        print(f"Sailor service installed on port {port}. View logs: sudo journalctl -u sailor -f")


if __name__ == "__main__":
    main()
