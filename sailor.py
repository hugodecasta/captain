#!/usr/bin/env python3
import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Dict, Any, Optional

import psutil
import requests
from fastapi import FastAPI, HTTPException
import uvicorn
import socket

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data" / "sailor"
CONF_FILE = DATA_DIR / "resources.json"
RUN_FILE = DATA_DIR / "running_chores.json"
DATA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
logger = logging.getLogger("sailor")

# Persistence helpers


def _read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        with path.open("r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed reading {path}: {e}")
        return default


def _write_json(path: Path, data):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp, path)

# Config


def first_run_prompt():
    print("Sailor first-time setup:")
    name = input("Sailor name: ").strip()
    captain_ip = input("Captain IP: ").strip()
    try:
        captain_port = int(input("Captain port [8000]: ") or "8000")
    except Exception:
        captain_port = 8000
    try:
        cpus = int(input("CPUs (int): ").strip())
    except Exception:
        cpus = psutil.cpu_count(logical=True) or 1
    gpus_str = input("GPUs (comma-separated type:vramMB, or empty): ").strip()
    gpus = []
    if gpus_str:
        for item in gpus_str.split(","):
            item = item.strip()
            if ":" in item:
                t, v = item.split(":", 1)
                try:
                    gpus.append({"type": t.strip(), "vram": int(v)})
                except Exception:
                    pass
            else:
                gpus.append({"type": item, "vram": 0})
    try:
        ram = psutil.virtual_memory().total
    except Exception:
        try:
            ram = int(input("RAM in bytes: ").strip())
        except Exception:
            ram = 0
    conf = {
        "name": name,
        "captain_ip": captain_ip,
        "captain_port": captain_port,
        "cpus": cpus,
        "gpus": gpus,
        "ram": int(ram),
        "port": 8001,
    }
    _write_json(CONF_FILE, conf)
    print("Configuration saved to", CONF_FILE)


# FastAPI
app = FastAPI(title="Sailor")

running: Dict[str, Dict[str, Any]] = {}
proc_by_chore: Dict[str, psutil.Popen] = {}


def load_conf():
    return _read_json(CONF_FILE, {})


def load_running():
    return _read_json(RUN_FILE, {})


def save_running(data):
    _write_json(RUN_FILE, data)

# Endpoint: captain_request


@app.post("/captain_request")
def captain_request(payload: Dict[str, Any]):
    conf = load_conf()
    chore_id = str(payload.get("chore_id"))
    script = payload.get("script")
    ressources = payload.get("ressources", {})
    out_file = payload.get("out")
    if not chore_id or not script:
        raise HTTPException(400, "chore_id and script required")
    if chore_id in proc_by_chore:
        return {"ok": True}

    env = os.environ.copy()
    cpus = int(ressources.get("cpus", 1) or 1)
    gpus = int(ressources.get("gpus", 0) or 0)
    env["OMP_NUM_THREADS"] = str(cpus)
    if gpus > 0:
        env["CUDA_VISIBLE_DEVICES"] = ",".join(str(i) for i in range(gpus))

    # run script as current user; assume script is executable
    try:
        stdout_target = subprocess.DEVNULL
        stderr_target = subprocess.DEVNULL
        out_fh = None
        if out_file:
            try:
                out_path = Path(str(out_file)).expanduser()
                out_path.parent.mkdir(parents=True, exist_ok=True)
                # open in append mode to accumulate output
                out_fh = out_path.open("ab", buffering=0)
                stdout_target = out_fh
                stderr_target = out_fh
            except Exception as e:
                logger.warning(f"Failed to open out file {out_file}: {e}. Falling back to PIPE.")
                out_fh = None
        popen = psutil.Popen(["/bin/bash", script], env=env, stdout=stdout_target, stderr=stderr_target)
        proc_by_chore[chore_id] = popen
        running[chore_id] = {"chore_id": chore_id, "pid": popen.pid, "start": int(time.time()), "cancel_requested": False, **({"out": str(out_file)} if out_file else {})}
        save_running(running)
        threading.Thread(target=_watch_process, args=(chore_id, out_fh), daemon=True).start()
        return {"ok": True}
    except Exception as e:
        logger.error(f"Failed to start chore {chore_id}: {e}")
        _report_status(conf, chore_id, "Failed", exit_code=-1)
        raise HTTPException(500, "failed to start")

# Endpoint: captain_cancel


@app.post("/captain_cancel")
@app.post("/captain_cancels")
@app.post("/captain_cancels/")
def captain_cancel(payload: Dict[str, Any]):
    chore_id = str(payload.get("chore_id"))
    if not chore_id:
        raise HTTPException(400, "chore_id required")
    p = proc_by_chore.get(chore_id)
    if not p:
        return {"ok": True}
    try:
        # mark cancel requested so watcher reports Canceled instead of Failed
        try:
            info = running.get(chore_id) or {}
            info["cancel_requested"] = True
            running[chore_id] = info
            save_running(running)
        except Exception:
            pass
        p.terminate()
        try:
            p.wait(timeout=5)
        except Exception:
            p.kill()
        return {"ok": True}
    finally:
        # watcher will report
        pass

# background watchers and reporting


def _watch_process(chore_id: str, out_fh=None):
    conf = load_conf()
    p = proc_by_chore.get(chore_id)
    if not p:
        return
    status = "Running"
    _report_status(conf, chore_id, status)
    try:
        ret = p.wait()
        # Determine if this termination was due to a cancel request
        info = running.get(chore_id) or {}
        was_canceled = bool(info.get("cancel_requested"))
        # If process exited due to SIGTERM/SIGKILL and we requested cancel, mark as Canceled
        if was_canceled:
            status = "Canceled"
        else:
            status = "Done" if ret == 0 else "Failed"
        _report_status(conf, chore_id, status, exit_code=ret)
    except Exception:
        _report_status(conf, chore_id, "Failed", exit_code=-1)
    finally:
        proc_by_chore.pop(chore_id, None)
        running.pop(chore_id, None)
        save_running(running)
        try:
            if out_fh:
                out_fh.flush()
                out_fh.close()
        except Exception:
            pass


def _report_status(conf: Dict[str, Any], chore_id: str, status: str, exit_code: Optional[int] = None):
    url = f"http://{conf['captain_ip']}:{conf.get('captain_port', 8000)}/sailor_report"
    payload = {"name": conf.get("name"), "chore_id": chore_id, "status": status}
    if exit_code is not None:
        payload["exit_code"] = exit_code
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        logger.error(f"report failed: {e}")


def _heartbeat_loop():
    while True:
        conf = load_conf()
        payload = {"name": conf.get("name")}
        try:
            requests.post(f"http://{conf['captain_ip']}:{conf.get('captain_port', 8000)}/sailor_awake", json=payload, timeout=5)
        except Exception:
            pass
        time.sleep(0.5)

# CLI


def cli():
    parser = argparse.ArgumentParser(description="Sailor server")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--serve", type=int, metavar="PORT", help="Run API server on PORT")
    args = parser.parse_args()

    conf = load_conf()
    if not conf:
        first_run_prompt()
        print("First-time configuration complete. Now start the server with: sailor --serve 8001")
        sys.exit(0)

    # register with captain
    # derive local IP used to reach captain
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((conf.get("captain_ip", "127.0.0.1"), conf.get("captain_port", 8000)))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        local_ip = "127.0.0.1"
    reg_payload = {
        "name": conf.get("name"),
        "ip": local_ip,
        "port": conf.get("port", 8001),
        "cpus": conf.get("cpus", 0),
        "gpus": conf.get("gpus", []),
        "ram": conf.get("ram", 0),
    }
    try:
        requests.post(f"http://{conf['captain_ip']}:{conf.get('captain_port', 8000)}/sailor_register", json=reg_payload, timeout=5).raise_for_status()
    except Exception as e:
        logger.warning(f"Registration refused or failed: {e}")

    port = args.serve if args.serve is not None else conf.get("port", 8001)
    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    logger.info(f"Sailor serving on 0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")


if __name__ == "__main__":
    cli()
