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
import shutil

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
    # Auto-detect CPUs (logical cores)
    try:
        cpus = psutil.cpu_count(logical=True) or os.cpu_count() or 1
    except Exception:
        cpus = os.cpu_count() or 1
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
    # Auto-detect RAM (bytes)
    try:
        ram = psutil.virtual_memory().total
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

# Lightweight status endpoint for captain reconciliation


@app.get("/status")
def status():
    try:
        current = list(running.keys())
        # Fallback to persisted file if in-memory empty (e.g., after restart)
        if not current:
            persisted = load_running()
            current = list((persisted or {}).keys())
        pids = {cid: (proc_by_chore[cid].pid if cid in proc_by_chore else (running.get(cid) or {}).get("pid")) for cid in current}
        return {"ok": True, "running": current, "pids": pids}
    except Exception as e:
        logger.error(f"status endpoint failed: {e}")
        return {"ok": False, "running": [], "pids": {}}

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
        # Use a bash -lc wrapper to create the output directory, start the log, run the script with redirection,
        # append the END marker, and propagate the script's exit code.
        out_dir = os.path.dirname(out_file) or "."
        inner = (
            f"mkdir -p {shlex.quote(out_dir)}; "
            f"echo 'START CHORE::{chore_id}' > {shlex.quote(out_file)}; "
            f"( /bin/bash {shlex.quote(script)}; ret=$?; echo 'END CHORE::{chore_id}'; exit $ret ) >> {shlex.quote(out_file)} 2>&1"
        )
        cmd = ["/bin/bash", "-lc", inner]
    else:
        cmd = ["/bin/bash", script]
        stdout_target = subprocess.DEVNULL
        stderr_target = subprocess.DEVNULL

    # preexec function to set a new session/process group, drop privileges and change working directory
    def _demote_and_setup():
        # Child will run in its own session/process group (start_new_session=True)
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
            start_new_session=True,
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
        # First try to signal the whole process group (created via setsid in preexec)
        pgid = None
        try:
            pgid = os.getpgid(p.pid)
        except Exception:
            pgid = None

        signaled = False
        if pgid is not None:
            try:
                os.killpg(pgid, signal.SIGTERM)
                signaled = True
            except ProcessLookupError:
                pass
            except PermissionError:
                pass

        # Also send SIGTERM to the leader just in case
        try:
            p.terminate()
        except Exception:
            pass

        # Give some time to exit cleanly
        try:
            p.wait(timeout=5)
        except Exception:
            # escalate: kill group then leader
            if pgid is not None:
                try:
                    os.killpg(pgid, signal.SIGKILL)
                except Exception:
                    pass
            try:
                p.kill()
            except Exception:
                pass

        # As a final fallback, reap any remaining children recursively
        try:
            for child in p.children(recursive=True):
                try:
                    child.terminate()
                except Exception:
                    pass
            gone, alive = psutil.wait_procs(p.children(recursive=True), timeout=3)
            for child in alive:
                try:
                    child.kill()
                except Exception:
                    pass
        except Exception:
            pass
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
    group.add_argument(
        "--create-service",
        action="store_true",
        help="Create a systemd service for Sailor (and attempt to enable/start it)",
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Port for the service when used with --create-service (also updates config)",
    )
    args = parser.parse_args()

    conf = load_conf()
    if not conf:
        first_run_prompt()
        conf = load_conf()
        if not getattr(args, "create_service", False) and args.serve is None:
            print("First-time configuration complete. Now start the server with: sailor --serve 8001")
            sys.exit(0)

    # Handle service creation request
    if getattr(args, "create_service", False):
        # Require an explicit port when creating a service
        if args.port is None:
            print("Error: --port is required when using --create-service.")
            print("Example: sudo python3 sailor.py --create-service --port 8001")
            sys.exit(2)
        # If a port is provided, persist it into config before creating the service
        if args.port is not None:
            try:
                conf["port"] = int(args.port)
                _write_json(CONF_FILE, conf)
            except Exception as e:
                print(f"Warning: failed to update port in config: {e}")

        # Creating a system service requires root to write to /etc/systemd/system and to run systemctl
        is_root = hasattr(os, "geteuid") and os.geteuid() == 0
        if not is_root:
            print("Creating a system-wide systemd service requires root (sudo).")
            print("Re-run with sudo to write the unit to /etc/systemd/system and enable/start it:")
            print("  sudo \"%s\" --create-service --port %s" % (shlex.quote(sys.executable + ' ' + str(Path(__file__).resolve())), conf.get('port', 8001)))
            print("Alternatively, run without sudo to create a user unit if your system supports user systemd (this script will attempt that automatically).")
            # Still attempt the non-root fallback path in _create_systemd_service, which will write a user unit or local file.
        _create_systemd_service(conf)
        return

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

    port = args.serve if args.serve is not None else (args.port if args.port is not None else conf.get("port", 8001))
    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    logger.info(f"Sailor serving on 0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")


def _create_systemd_service(conf: Dict[str, Any]):
    """
    Create a systemd unit for Sailor and try to enable/start it.
    Works in system (root) mode when possible; otherwise attempts user mode.
    """
    name = str(conf.get("name") or "sailor").strip() or "sailor"
    port = int(conf.get("port", 8001))
    python = sys.executable or "/usr/bin/python3"
    script = str(Path(__file__).resolve())

    unit_name = f"sailor-{name}.service"

    def render_unit(user_mode: bool) -> str:
        lines = [
            "[Unit]",
            f"Description=Sailor worker ({name})",
            "After=network.target",
            "",
            "[Service]",
            "Type=simple",
            f"ExecStart={shlex.quote(python)} {shlex.quote(script)} --serve {port}",
            "Restart=always",
            "RestartSec=3",
            "Environment=PYTHONUNBUFFERED=1",
            f"WorkingDirectory={shlex.quote(str(Path(script).parent))}",
        ]
        if not user_mode:
            # In system mode, run as root to allow switching to target job owners
            lines.append("User=root")
        lines += [
            "",
            "[Install]",
            "WantedBy=default.target" if user_mode else "WantedBy=multi-user.target",
            "",
        ]
        return "\n".join(lines)

    def _write(path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            f.write(unit_content)
        return path

    has_systemctl = bool(shutil.which("systemctl"))
    has_systemd = os.path.isdir("/run/systemd/system")
    is_root = (hasattr(os, "geteuid") and os.geteuid() == 0)

    if is_root and has_systemctl and has_systemd:
        unit_path = Path("/etc/systemd/system") / unit_name
        unit_content = render_unit(user_mode=False)
        _write(unit_path)
        print(f"Wrote system unit: {unit_path}")
        # Try to enable and start
        try:
            subprocess.run(["systemctl", "daemon-reload"], check=True)
            subprocess.run(["systemctl", "enable", unit_name], check=True)
            subprocess.run(["systemctl", "restart", unit_name], check=True)
            print(f"Service enabled and started: {unit_name}")
        except Exception as e:
            print(f"Created unit, but failed to enable/start automatically: {e}")
            print(f"You can run: sudo systemctl daemon-reload && sudo systemctl enable {unit_name} && sudo systemctl start {unit_name}")
        return

    # Fallback: user mode (no root or no system systemd)
    if has_systemctl and has_systemd:
        unit_path = Path.home() / ".config" / "systemd" / "user" / unit_name
        unit_content = render_unit(user_mode=True)
        _write(unit_path)
        print(f"Wrote user unit: {unit_path}")
        # Try to enable and start in --user scope
        try:
            subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
            subprocess.run(["systemctl", "--user", "enable", unit_name], check=True)
            subprocess.run(["systemctl", "--user", "restart", unit_name], check=True)
            print(f"User service enabled and started: {unit_name}")
            print("Note: ensure lingering is enabled for your user if you want it active without login: sudo loginctl enable-linger $USER")
        except Exception as e:
            print(f"Created user unit, but failed to enable/start automatically: {e}")
            print(f"You can run: systemctl --user daemon-reload && systemctl --user enable {unit_name} && systemctl --user start {unit_name}")
        return

    # Last fallback: write unit file next to script and show instructions
    unit_path = Path(script).parent / unit_name
    unit_content = render_unit(user_mode=not is_root)
    _write(unit_path)
    print("Systemd not detected or systemctl unavailable.")
    print(f"Wrote unit file locally: {unit_path}")
    print("If your system uses systemd, move it to /etc/systemd/system (as root) then run:\n  systemctl daemon-reload && systemctl enable " + unit_name + " && systemctl start " + unit_name)


if __name__ == "__main__":
    cli()
