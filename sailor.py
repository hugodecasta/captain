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
from typing import Dict, Any, Optional, List

import psutil
import requests
from fastapi import FastAPI, HTTPException
import uvicorn
import socket
import shlex
import pwd

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


# --- Privilege drop helpers (inspired by old exec_sub.py) ---


def _build_env(pw: Optional[pwd.struct_passwd], preserve: bool, uid: int, workdir: Optional[str]):
    """
    Build environment for target user. If no passwd entry is available, fall back to a numeric-based env.
    """
    fallback = {
        "HOME": workdir or "/",
        "LOGNAME": str(uid),
        "USER": str(uid),
        "SHELL": "/bin/sh",
        "PATH": os.environ.get(
            "PATH",
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        ),
        "LANG": os.environ.get("LANG", "C.UTF-8"),
        "LC_ALL": os.environ.get("LC_ALL") or os.environ.get("LANG") or "C.UTF-8",
    }

    if pw is None:
        if preserve:
            env = dict(os.environ)
            env.update(fallback)
            return env
        return fallback

    if preserve:
        env = dict(os.environ)
        env.update(
            {
                "HOME": pw.pw_dir,
                "LOGNAME": pw.pw_name,
                "USER": pw.pw_name,
                "SHELL": pw.pw_shell or "/bin/sh",
            }
        )
        return env
    return {
        "HOME": pw.pw_dir,
        "LOGNAME": pw.pw_name,
        "USER": pw.pw_name,
        "SHELL": pw.pw_shell or "/bin/sh",
        "PATH": os.environ.get(
            "PATH",
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        ),
        "LANG": os.environ.get("LANG", "C.UTF-8"),
        "LC_ALL": os.environ.get("LC_ALL") or os.environ.get("LANG") or "C.UTF-8",
    }


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
    owner = payload.get("owner")
    wd = payload.get("wd")
    if not chore_id or not script:
        raise HTTPException(400, "chore_id and script required")
    if chore_id in proc_by_chore:
        return {"ok": True}

    # Resolve target user UID
    try:
        uid = int(owner) if owner is not None else os.getuid()
    except Exception:
        logger.error(f"invalid owner uid: {owner}")
        raise HTTPException(400, "invalid owner uid")

    # Ensure we can switch identity when necessary
    euid = os.geteuid()
    if uid != euid and euid != 0:
        raise HTTPException(403, "sailor must run as root to switch uid")

    # Try to look up passwd entry; proceed without it if missing
    pw_entry = None
    try:
        pw_entry = pwd.getpwuid(uid)
    except KeyError:
        pw_entry = None

    # Determine target GID
    target_gid = pw_entry.pw_gid if pw_entry is not None else uid

    # Resolve working directory
    workdir = None
    if wd:
        workdir = wd if os.path.isabs(wd) else os.path.abspath(wd)
        if not os.path.isdir(workdir):
            raise HTTPException(400, f"working directory not found: {workdir}")
    else:
        workdir = pw_entry.pw_dir if pw_entry is not None else "/"

    # Environment for target user (preserve current env, but set HOME/USER appropriately)
    env = _build_env(pw_entry, True, uid, workdir)

    # CPU constraints
    cpu_total = os.cpu_count() or 1
    try:
        cpus_req = int(ressources.get("cpus", 1) or 1)
    except Exception:
        cpus_req = 1
    n_cpus = max(1, min(int(cpus_req), cpu_total))
    cpu_set = set(range(n_cpus))

    # Threading environment to honor CPU budget
    for v in (
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
    ):
        env[v] = str(n_cpus)
    env["MKL_DYNAMIC"] = "FALSE"
    env["OMP_DYNAMIC"] = "FALSE"
    # Help OpenMP keep threads within our affinity mask
    # try:
    #     if cpu_set == set(range(min(cpu_set), max(cpu_set) + 1)):
    #         env["GOMP_CPU_AFFINITY"] = f"{min(cpu_set)}-{max(cpu_set)}"
    #     else:
    #         env["GOMP_CPU_AFFINITY"] = " ".join(str(i) for i in sorted(cpu_set))
    # except Exception:
    #     pass
    env["TORCH_NUM_THREADS"] = str(n_cpus)
    env["TORCH_NUM_INTEROP_THREADS"] = str(max(1, min(n_cpus, 8)))

    # GPU constraints: support list of indices or integer count
    gspec = ressources.get("gpus", 0)
    gpu_list: Optional[List[int]] = None
    if isinstance(gspec, list):
        try:
            gpu_list = [int(x) for x in gspec]
        except Exception:
            gpu_list = None
    elif isinstance(gspec, str):
        try:
            parts = [p.strip() for p in gspec.split(",") if p.strip() != ""]
            gpu_list = [int(p) for p in parts]
        except Exception:
            gpu_list = None
    else:
        # treat as count
        try:
            count = int(gspec or 0)
            if count > 0:
                gpu_list = list(range(count))
            elif count == 0:
                gpu_list = []  # explicitly hide GPUs
        except Exception:
            gpu_list = None

    if gpu_list is not None:
        gpu_str = ",".join(str(i) for i in gpu_list)
        # Constrain visible GPUs across common env knobs
        env["CUDA_VISIBLE_DEVICES"] = gpu_str
        env["NVIDIA_VISIBLE_DEVICES"] = gpu_str
        env["HIP_VISIBLE_DEVICES"] = gpu_str
        env["ROCR_VISIBLE_DEVICES"] = gpu_str

    # Build command and stdout/stderr handling
    cmd: List[str]
    stdout_target = None
    stderr_target = None
    if out_file:
        # Use a bash -lc wrapper to create the output directory and redirect to the output file
        out_dir = os.path.dirname(out_file) or "."
        inner = (
            f"mkdir -p {shlex.quote(out_dir)}; "
            f"exec >> {shlex.quote(out_file)} 2>&1; "
            f"exec /bin/bash {shlex.quote(script)}"
        )
        cmd = ["/bin/bash", "-lc", inner]
    else:
        cmd = ["/bin/bash", script]
        stdout_target = subprocess.DEVNULL
        stderr_target = subprocess.DEVNULL

    # preexec function to drop privileges and change working directory
    def _demote_and_setup():
        # Apply CPU affinity as early as possible for the child process
        try:
            os.sched_setaffinity(0, cpu_set)
            print('setting CPUS', cpu_set)
        except Exception:
            try:
                os.write(2, b"Failed to set CPU affinity, continuing anyway.\n")
            except Exception:
                pass
        # If we're already the target user, avoid attempting privileged ops
        try:
            if os.geteuid() == uid:
                if workdir:
                    try:
                        os.chdir(workdir)
                    except Exception as e:
                        try:
                            os.write(2, f"Failed to chdir to {workdir}: {e}\n".encode())
                        except Exception:
                            pass
                        os._exit(154)
                return
        except Exception:
            pass

        # Supplementary groups
        try:
            if pw_entry is not None and target_gid is not None:
                os.initgroups(pw_entry.pw_name, target_gid)
            else:
                os.setgroups([])
        except Exception:
            try:
                os.setgroups([])
            except Exception:
                pass

        # Primary GID
        try:
            if target_gid is not None:
                if hasattr(os, "setresgid"):
                    os.setresgid(target_gid, target_gid, target_gid)
                else:
                    os.setgid(target_gid)
        except Exception:
            # Best effort; continue to try setting UID
            pass

        # UID
        if hasattr(os, "setresuid"):
            os.setresuid(uid, uid, uid)
        else:
            os.setuid(uid)

        # Umask
        os.umask(0o022)

        # CWD
        try:
            os.chdir(workdir or "/")
        except Exception as e:
            try:
                os.write(2, f"Failed to chdir to {workdir}: {e}\n".encode())
            except Exception:
                pass
            os._exit(154)

    # run script as target uid; assume script is accessible (e.g., on shared storage)
    try:
        popen = psutil.Popen(
            cmd,
            env=env,
            preexec_fn=_demote_and_setup,
            stdout=stdout_target,
            stderr=stderr_target,
        )
        proc_by_chore[chore_id] = popen
        running[chore_id] = {
            "chore_id": chore_id,
            "pid": popen.pid,
            "start": int(time.time()),
            "cancel_requested": False,
            **({"out": str(out_file)} if out_file else {}),
            **({"wd": workdir} if workdir else {}),
            "owner": int(uid),
        }
        save_running(running)
        threading.Thread(target=_watch_process, args=(chore_id, None), daemon=True).start()
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
