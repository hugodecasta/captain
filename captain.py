#!/usr/bin/env python3
import argparse
import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

import requests
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
from contextlib import asynccontextmanager
import secrets
import pwd
try:
    import pam  # type: ignore
except Exception:
    pam = None  # PAM not available

# Data paths
ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data" / "captain"
CREW_FILE = DATA_DIR / "crew.json"
CHORES_FILE = DATA_DIR / "chores.json"
USERS_FILE = DATA_DIR / "users.json"
FRONT_DIR = ROOT / "front"

DATA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
logger = logging.getLogger("captain")

# JSON persistence helpers


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

# Data models (dicts as per spec)


def load_crew() -> Dict[str, Dict[str, Any]]:
    return _read_json(CREW_FILE, {})


def save_crew(crew: Dict[str, Dict[str, Any]]):
    _write_json(CREW_FILE, crew)


def load_chores() -> Dict[str, Dict[str, Any]]:
    return _read_json(CHORES_FILE, {})


def save_chores(chores: Dict[str, Dict[str, Any]]):
    _write_json(CHORES_FILE, chores)


def load_users() -> Dict[str, Dict[str, Any]]:
    """Load users registry keyed by UID (string).
    Schema: {<UID>: {name:str, uid:str, time_limit:str, chores_limit:int, notes:str}}
    """
    return _read_json(USERS_FILE, {})


def save_users(users: Dict[str, Dict[str, Any]]):
    _write_json(USERS_FILE, users)


# FastAPI app
cleanup_stop_event: Optional[threading.Event] = None
cleanup_thread: Optional[threading.Thread] = None
CAPTAIN_CLEANUP_TTL = int(os.getenv("CAPTAIN_CLEANUP_TTL", "120"))  # seconds
CAPTAIN_CANCEL_REQUESTED_TTL = int(os.getenv("CAPTAIN_CANCEL_REQUESTED_TTL", "300"))  # seconds


def _cleanup_loop(stop_event: threading.Event):
    TTL = CAPTAIN_CLEANUP_TTL
    while not stop_event.is_set():
        try:
            chores = load_chores()
            crew = load_crew()
            users = load_users()
            changed = False
            now = now_ts()
            # 1) Enforce user time_limit across active chores (assigned/running)
            # Build per-owner active chore durations and cancel newest beyond budget
            # Only consider chores not in terminal status
            # We compute durations from run_start/assigned_at/start to now
            by_owner: Dict[int, List[Tuple[str, Dict[str, Any], int]]] = {}
            for cid, chore in chores.items():
                status = str(chore.get("status", "")).lower()
                if status in TERMINAL_STATUSES:
                    continue
                owner = int(chore.get("owner") or 0)
                # pending chores don't consume time yet
                if status in (None, "pending"):
                    dur = 0
                else:
                    t0 = int(chore.get("run_start") or chore.get("assigned_at") or chore.get("start") or 0)
                    dur = max(0, now - t0) if t0 else 0
                by_owner.setdefault(owner, []).append((cid, chore, dur))
            for owner, items in by_owner.items():
                rec = users.get(str(owner)) or {}
                limit_s = _parse_max_time(rec.get("time_limit"))
                if limit_s <= 0:
                    continue
                # sort by start time (oldest first) to preserve older chores
                items_sorted = sorted(items, key=lambda x: int(x[1].get("run_start") or x[1].get("assigned_at") or x[1].get("start") or now))
                total = 0
                keep: List[str] = []
                cancel: List[Tuple[str, Dict[str, Any]]] = []
                for cid, chore, dur in items_sorted:
                    if total + dur <= limit_s:
                        total += dur
                        keep.append(cid)
                    else:
                        cancel.append((cid, chore))
                # mark excess chores for cancel, contacting sailor if assigned/running
                for cid, chore in cancel:
                    # mark cancel requested first to persist reason before any sailor report
                    chore["status"] = "cancel_requested"
                    chore["reason"] = "exceeded user time limit"
                    chore["cancel_requested_at"] = now
                    chore["cancel_source"] = "user_time_limit"
                    chores[cid] = chore
                    changed = True
                    save_chores(chores)
                    # then attempt to notify sailor if applicable
                    if str(chore.get("status", "")).lower() in ("assigned", "running"):
                        sailor_name = chore.get("sailor")
                        if sailor_name:
                            sailor = crew.get(sailor_name) or {}
                            try:
                                url = f"http://{sailor.get('ip', '127.0.0.1')}:{sailor.get('port', 8001)}/captain_cancels"
                                requests.post(url, json={"chore_id": cid}, timeout=3)
                            except Exception:
                                pass
            for cid, chore in list(chores.items()):
                status = str(chore.get("status", "")).lower()
                # enforce max_time while running or assigned
                if status not in TERMINAL_STATUSES:
                    sailor_name = chore.get("sailor")
                    if sailor_name:
                        sailor = crew.get(sailor_name) or {}
                        max_time_s = _parse_max_time(sailor.get("max_time"))
                        if max_time_s > 0:
                            # we count from when the chore started actually running if known, otherwise assigned time
                            t0 = int(chore.get("run_start") or chore.get("assigned_at") or chore.get("start") or 0)
                            if t0 and (now - t0) > max_time_s:
                                # mark cancel requested first (persist reason), then notify sailor
                                chore["status"] = "cancel_requested"
                                chore["reason"] = "exceeded time limit"
                                chore["cancel_requested_at"] = now
                                chore["cancel_source"] = "sailor_max_time"
                                chores[cid] = chore
                                changed = True
                                save_chores(chores)
                                try:
                                    url = f"http://{sailor.get('ip', '127.0.0.1')}:{sailor.get('port', 8001)}/captain_cancels"
                                    requests.post(url, json={"chore_id": cid}, timeout=3)
                                except Exception:
                                    pass
                # finalize long-standing cancel_requested
                if status == "cancel_requested":
                    cr_at = int(chore.get("cancel_requested_at") or 0)
                    if not cr_at:
                        # estimate from best-known start and stamp, so future loops can finalize
                        est = int(chore.get("run_start") or chore.get("assigned_at") or chore.get("start") or now)
                        chore["cancel_requested_at"] = est
                        chores[cid] = chore
                        changed = True
                        cr_at = est
                    if cr_at and (now - cr_at) >= CAPTAIN_CANCEL_REQUESTED_TTL:
                        # attempt one more cancel poke
                        sailor_name = chore.get("sailor")
                        if sailor_name:
                            sailor = crew.get(sailor_name) or {}
                            try:
                                url = f"http://{sailor.get('ip', '127.0.0.1')}:{sailor.get('port', 8001)}/captain_cancels"
                                requests.post(url, json={"chore_id": cid}, timeout=3)
                            except Exception:
                                pass
                            # free resources optimistically
                            need_cpus = int(chore.get("ressources", {}).get("cpus", 1) or 1)
                            need_gpus = int(chore.get("ressources", {}).get("gpus", 0) or 0)
                            s = crew.get(sailor_name)
                            if s:
                                s["used_cpus"] = max(0, int(s.get("used_cpus", 0) or 0) - need_cpus)
                                s["used_gpus"] = max(0, int(s.get("used_gpus", 0) or 0) - need_gpus)
                                s["status"] = "idle" if (s.get("used_cpus", 0) == 0 and s.get("used_gpus", 0) == 0) else "busy"
                                crew[sailor_name] = s
                                save_crew(crew)
                        # finalize chore as canceled
                        chore["status"] = "canceled"
                        chore["end"] = now
                        if not chore.get("reason"):
                            src = chore.get("cancel_source")
                            if src == "sailor_max_time":
                                chore["reason"] = "exceeded time limit"
                            elif src == "user_time_limit":
                                chore["reason"] = "exceeded user time limit"
                            elif src == "user":
                                chore["reason"] = "canceled by user"
                            else:
                                chore["reason"] = "canceled by timeout"
                        chores[cid] = chore
                        changed = True
                if status in TERMINAL_STATUSES:
                    end_ts = int(chore.get("end") or chore.get("terminated_at") or 0)
                    if end_ts and (now - end_ts) >= TTL:
                        chores.pop(cid, None)
                        changed = True
            if changed:
                save_chores(chores)
        except Exception as e:
            logger.error(f"cleanup loop error: {e}")
        # sleep with stop awareness
        stop_event.wait(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global cleanup_stop_event, cleanup_thread
    cleanup_stop_event = threading.Event()
    cleanup_thread = threading.Thread(target=_cleanup_loop, args=(cleanup_stop_event,), daemon=True)
    cleanup_thread.start()
    try:
        yield
    finally:
        if cleanup_stop_event:
            cleanup_stop_event.set()
        if cleanup_thread:
            cleanup_thread.join(timeout=2)


app = FastAPI(title="Captain", lifespan=lifespan)

# Static frontend
if FRONT_DIR.exists():
    app.mount("/front", StaticFiles(directory=str(FRONT_DIR), html=False), name="front")

# Utility


def now_ts() -> int:
    return int(time.time())


def _fmt_bytes(n: int) -> str:
    try:
        n = int(n)
    except Exception:
        return str(n)
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    f = float(n)
    while f >= 1024 and i < len(units) - 1:
        f /= 1024.0
        i += 1
    return f"{f:.1f} {units[i]}"


def _derive_status(s: Dict[str, Any]) -> str:
    last_seen = int(s.get("last_seen", 0) or 0)
    if now_ts() - last_seen > 10:
        return "down"
    used_cpus = int(s.get("used_cpus", 0) or 0)
    total_cpus = int(s.get("cpus", 0) or 0)
    if total_cpus and used_cpus >= total_cpus:
        return "full"
    if used_cpus > 0:
        return "busy"
    return "idle"


TERMINAL_STATUSES = {"done", "failed", "canceled", "cancelled"}
ACTIVE_STATUSES = {"pending", "assigned", "running", "cancel_requested"}

# Simple in-memory auth tokens {token: {uid, username, created}}
TOKENS: Dict[str, Dict[str, Any]] = {}
TOKEN_TTL = int(os.getenv("CAPTAIN_TOKEN_TTL", "3600"))

# Serve flag file for local CLI discovery
SERVE_FLAG_FILE = Path(os.getenv("CAPTAIN_FLAG_FILE", str(DATA_DIR / "serve.json")))


def _write_serve_flag(port: int):
    try:
        SERVE_FLAG_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = SERVE_FLAG_FILE.with_suffix(SERVE_FLAG_FILE.suffix + ".tmp")
        with tmp.open("w") as f:
            json.dump({
                "port": int(port),
                "pid": os.getpid(),
                "started_at": now_ts(),
            }, f)
        os.replace(tmp, SERVE_FLAG_FILE)
    except Exception as e:
        logger.error(f"Failed to write serve flag: {e}")


def _remove_serve_flag():
    try:
        if SERVE_FLAG_FILE.exists():
            SERVE_FLAG_FILE.unlink()
    except Exception:
        # non-fatal
        pass


def _discover_base_url(timeout: float = 1.0) -> Optional[str]:
    """Read the serve flag to discover the local base URL and verify reachability.
    Returns base URL like 'http://127.0.0.1:<port>' or None if not reachable.
    """
    try:
        if not SERVE_FLAG_FILE.exists():
            return None
        with SERVE_FLAG_FILE.open("r") as f:
            data = json.load(f)
        port = int(data.get("port"))
        base = f"http://127.0.0.1:{port}"
        # quick reachability probe
        try:
            r = requests.get(f"{base}/", timeout=timeout)
            if r.status_code >= 200 and r.status_code < 500:
                return base
        except Exception:
            return None
    except Exception:
        return None
    return None


def _parse_max_time(s: Optional[str]) -> int:
    """
    Parse a duration string into seconds.
    Supported formats:
      - DD-hh:mm:ss
      - hh:mm:ss (days defaults to 0)
    Returns 0 if missing or invalid.
    """
    if not s or not isinstance(s, str):
        return 0
    try:
        days = 0
        rest = s
        if "-" in s:
            d, rest = s.split("-", 1)
            days = int(d)
        parts = [int(x) for x in rest.split(":")]
        while len(parts) < 3:
            parts.insert(0, 0)  # pad to hh:mm:ss
        hh, mm, ss = parts[-3], parts[-2], parts[-1]
        return days * 86400 + hh * 3600 + mm * 60 + ss
    except Exception:
        return 0


def eligible_sailors(crew: Dict[str, Dict[str, Any]], service: Optional[str]) -> List[Dict[str, Any]]:
    out = []
    for s in crew.values():
        if service:
            services = s.get("services", [])
            if isinstance(services, str):
                services = [x.strip() for x in services.split(",") if x.strip()]
            if service not in services:
                continue
        # status calculation based on last_seen
        last_seen = s.get("last_seen", 0)
        status = s.get("status", "down")
        if now_ts() - last_seen > 10:
            status = "down"
        if status in ("down",):
            continue
        out.append(s)
    return out

# Scheduling


def try_assign_pending():
    crew = load_crew()
    chores = load_chores()

    for chore in chores.values():
        if chore.get("sailor"):
            continue
        if chore.get("status") not in (None, "pending"):
            continue
        service = chore.get("service")
        need_cpus = int(chore.get("ressources", {}).get("cpus", 1) or 1)
        need_gpus = int(chore.get("ressources", {}).get("gpus", 0) or 0)
        candidates = eligible_sailors(crew, service)
        # filter by free resources
        best = None
        for s in candidates:
            total_cpus = int(s.get("cpus", 0) or 0)
            total_gpus = int(len(s.get("gpus", []) or []))
            used_cpus = int(s.get("used_cpus", 0) or 0)
            used_gpus = int(s.get("used_gpus", 0) or 0)
            free_cpus = max(total_cpus - used_cpus, 0)
            free_gpus = max(total_gpus - used_gpus, 0)
            if free_cpus >= need_cpus and free_gpus >= need_gpus and s.get("status") in ("idle", "busy"):
                # pick least used
                score = (free_cpus - need_cpus) + (free_gpus - need_gpus)
                if not best or score > best[0]:
                    best = (score, s)
        if best:
            sailor = best[1]
            sailor_name = sailor["name"]
            chore_id = chore["chore_id"]
            logger.info(f"Assigning chore {chore_id} to {sailor_name}")
            # optimistic allocation
            sailor["used_cpus"] = int(sailor.get("used_cpus", 0) or 0) + need_cpus
            sailor["used_gpus"] = int(sailor.get("used_gpus", 0) or 0) + need_gpus
            sailor["status"] = "busy"
            crew[sailor_name] = sailor
            chore["sailor"] = sailor_name
            chore["status"] = "assigned"
            chore["assigned_at"] = now_ts()
            chore["reason"] = None
            chores[chore_id] = chore
            save_crew(crew)
            save_chores(chores)
            # notify sailor
            try:
                url = f"http://{sailor['ip']}:{sailor.get('port', 8001)}/captain_request"
                resp = requests.post(url, json={
                    "chore_id": chore_id,
                    "script": chore.get("script"),
                    "ressources": chore.get("ressources", {}),
                    "owner": chore.get("owner"),
                }, timeout=5)
                resp.raise_for_status()
                logger.info(f"Sailor {sailor_name} accepted chore {chore_id}")
            except Exception as e:
                logger.error(f"Failed to send chore {chore_id} to {sailor_name}: {e}")
                # rollback assignment
                chore["sailor"] = None
                chore["status"] = "pending"
                chore["reason"] = "sailor unreachable"
                sailor["used_cpus"] = int(sailor.get("used_cpus", 0) or 0) - need_cpus
                sailor["used_gpus"] = int(sailor.get("used_gpus", 0) or 0) - need_gpus
                crew[sailor_name] = sailor
                chores[chore_id] = chore
                save_crew(crew)
                save_chores(chores)
        else:
            # no available sailor for this chore currently
            if (chore.get("status") in (None, "pending")) and (chore.get("reason") != "no available sailor"):
                chore["status"] = "pending"
                chore["reason"] = "no available sailor"
                chores[chore.get("chore_id")] = chore
                save_chores(chores)

# API Endpoints


@app.get("/")
def index():
    # Serve front/index.html if present, else basic info
    index_file = FRONT_DIR / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    return JSONResponse({"ok": True, "message": "Captain is running", "endpoints": ["/crew", "/user_consult", "/front/*"]})


def _make_token(uid: int, username: str) -> str:
    tok = secrets.token_urlsafe(24)
    TOKENS[tok] = {"uid": uid, "username": username, "created": now_ts()}
    return tok


def _get_token_uid(token: Optional[str]) -> Optional[int]:
    if not token:
        return None
    rec = TOKENS.get(token)
    if not rec:
        return None
    if now_ts() - int(rec.get("created", 0)) > TOKEN_TTL:
        TOKENS.pop(token, None)
        return None
    return int(rec.get("uid"))


@app.post("/login")
def login(payload: Dict[str, Any]):
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "")
    if not username or not password:
        raise HTTPException(400, "username and password required")
    if pam is None:
        raise HTTPException(501, "PAM authentication not available. Install 'python-pam'.")
    p = pam.pam()
    if not p.authenticate(username, password):
        raise HTTPException(401, "invalid credentials")
    try:
        uid = pwd.getpwnam(username).pw_uid
    except Exception:
        raise HTTPException(400, "unknown user")
    token = _make_token(uid, username)
    return {"ok": True, "token": token, "uid": uid, "username": username}


def _bearer_token(auth_header: Optional[str]) -> Optional[str]:
    if not auth_header:
        return None
    parts = auth_header.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None


@app.get("/me/chores")
def me_chores(Authorization: Optional[str] = Header(None)):  # noqa: N803
    token = _bearer_token(Authorization)
    uid = _get_token_uid(token)
    if uid is None:
        raise HTTPException(401, "unauthorized")
    chores = load_chores()
    res = [c for c in chores.values() if int(c.get("owner") or -1) == int(uid)]
    return JSONResponse(res)


@app.post("/me/cancel")
def me_cancel(payload: Dict[str, Any], Authorization: Optional[str] = Header(None)):  # noqa: N803
    token = _bearer_token(Authorization)
    uid = _get_token_uid(token)
    if uid is None:
        raise HTTPException(401, "unauthorized")
    chore_id = str(payload.get("chore_id") or "")
    if not chore_id:
        raise HTTPException(400, "chore_id required")
    chores = load_chores()
    chore = chores.get(chore_id)
    if not chore or int(chore.get("owner") or -1) != int(uid):
        raise HTTPException(404, "chore not found")
    # delegate to existing user_cancel logic
    return user_cancel({"chore_id": chore_id, "reason": payload.get("reason")})


# Startup handled via lifespan above


@app.post("/sailor_register")
def sailor_register(payload: Dict[str, Any]):
    name = payload.get("name")
    ip = payload.get("ip")
    port = payload.get("port", 8001)
    if not name or not ip:
        raise HTTPException(400, "name and ip required")
    crew = load_crew()
    if name not in crew:
        # prereg required
        raise HTTPException(403, "sailor not preregistered")
    # update resources
    s = crew[name]
    s.update({
        "name": name,
        "ip": s.get("ip", ip),
        "port": port,
        "cpus": int(payload.get("cpus", s.get("cpus", 0)) or 0),
        "gpus": payload.get("gpus", s.get("gpus", [])) or [],
        "ram": int(payload.get("ram", s.get("ram", 0)) or 0),
        "last_seen": now_ts(),
        "status": "idle",
        "used_cpus": 0,
        "used_gpus": 0,
    })
    # preserve any preregistered max_time if present
    if "max_time" in crew.get(name, {}):
        s["max_time"] = crew[name].get("max_time")
    crew[name] = s
    save_crew(crew)
    try_assign_pending()
    return {"ok": True}


@app.post("/sailor_awake")
def sailor_awake(payload: Dict[str, Any]):
    name = payload.get("name")
    if not name:
        raise HTTPException(400, "name required")
    crew = load_crew()
    if name not in crew:
        raise HTTPException(403, "sailor not preregistered")
    s = crew[name]
    s["last_seen"] = now_ts()
    # infer status by used resources
    s["status"] = "full" if (s.get("used_cpus", 0) >= s.get("cpus", 0)) else ("busy" if s.get("used_cpus", 0) > 0 else "idle")
    crew[name] = s
    save_crew(crew)
    return {"ok": True}


@app.post("/sailor_report")
def sailor_report(payload: Dict[str, Any]):
    chore_id = str(payload.get("chore_id"))
    status = payload.get("status")  # Done|Running|Canceled|Failed
    exit_code = payload.get("exit_code")
    name = payload.get("name")
    if not chore_id or not name:
        raise HTTPException(400, "chore_id and name required")
    chores = load_chores()
    crew = load_crew()
    chore = chores.get(chore_id)
    if not chore:
        return {"ok": True}  # nothing to do, idempotent
    # free resources if terminal
    terminal = status in ("Done", "Canceled", "Failed")
    if terminal:
        need_cpus = int(chore.get("ressources", {}).get("cpus", 1) or 1)
        need_gpus = int(chore.get("ressources", {}).get("gpus", 0) or 0)
        s = crew.get(name)
        if s:
            s["used_cpus"] = max(0, int(s.get("used_cpus", 0) or 0) - need_cpus)
            s["used_gpus"] = max(0, int(s.get("used_gpus", 0) or 0) - need_gpus)
            s["status"] = "idle" if (s.get("used_cpus", 0) == 0 and s.get("used_gpus", 0) == 0) else "busy"
            crew[name] = s
        chore["status"] = status.lower()
        chore["end"] = now_ts()
        chore["exit_code"] = exit_code
        # keep existing reason if set during cancel request (e.g. exceeded time limit)
        if str(status).lower() == "canceled" and not chore.get("reason"):
            src = chore.get("cancel_source")
            if src == "sailor_max_time":
                chore["reason"] = "exceeded time limit"
            elif src == "user_time_limit":
                chore["reason"] = "exceeded user time limit"
            elif src == "user":
                chore["reason"] = "canceled by user"
            else:
                chore["reason"] = "canceled"
        chores[chore_id] = chore
        save_crew(crew)
        save_chores(chores)
        # attempt to assign queued chores
        try_assign_pending()
    else:
        # update status only
        chore["status"] = str(status).lower()
        # record when the chore actually started running (first Running report)
        if str(status).lower() == "running" and not chore.get("run_start"):
            chore["run_start"] = now_ts()
        chores[chore_id] = chore
        save_chores(chores)
    return {"ok": True}


@app.post("/user_chore")
def user_chore(payload: Dict[str, Any]):
    # required params
    script = payload.get("script")
    if not script:
        raise HTTPException(400, "script required")
    owner = payload.get("owner") or os.getuid()
    service = payload.get("service")
    ressources = payload.get("ressources", {})
    cpus = int(ressources.get("cpus", payload.get("cpus", 1)) or 1)
    gpus = int(ressources.get("gpus", payload.get("gpus", 0)) or 0)

    # enforce user chores_limit if defined
    users = load_users()
    user_rec = users.get(str(owner)) or {}
    chores_limit = int(user_rec.get("chores_limit") or 0)
    if chores_limit > 0:
        # count active chores for this owner
        existing = load_chores()
        active_count = 0
        now = now_ts()
        for c in existing.values():
            if int(c.get("owner") or -1) != int(owner):
                continue
            st = str(c.get("status", "")).strip().lower()
            # if status not known, treat as non-active unless clearly active
            if st not in ACTIVE_STATUSES:
                continue
            # if cancel_requested older than TTL, don't count it against the limit
            if st == "cancel_requested":
                cr_at = int(c.get("cancel_requested_at") or 0)
                # fallback estimate if missing timestamp
                if not cr_at:
                    cr_at = int(c.get("run_start") or c.get("assigned_at") or c.get("start") or now)
                if cr_at and (now - cr_at) >= CAPTAIN_CANCEL_REQUESTED_TTL:
                    continue
            active_count += 1
        if active_count >= chores_limit:
            raise HTTPException(403, f"user chores limit reached ({active_count}/{chores_limit})")

    # create chore id
    chore_id = str(int(time.time() * 1000))
    chore = {
        "chore_id": chore_id,
        "script": script,
        "service": service,
        "ressources": {"cpus": cpus, "gpus": gpus},
        "sailor": None,
        "owner": owner,
        "status": "pending",
        "start": now_ts(),
        "reason": "no available sailor",
    }
    chores = load_chores()
    chores[chore_id] = chore
    save_chores(chores)

    try_assign_pending()
    return {"ok": True, "chore_id": chore_id}


@app.get("/user_consult")
def user_consult(owner: Optional[int] = None, all: bool = False):  # noqa: A002
    chores = load_chores()
    if not all:
        owner = owner if owner is not None else os.getuid()
        res = [c for c in chores.values() if c.get("owner") == owner]
    else:
        res = list(chores.values())
    return JSONResponse(res)


@app.get("/crew")
def crew_list():
    crew = load_crew()
    # enrich with derived status and last seen age for UI
    out = []
    now = now_ts()
    for s in crew.values():
        scopy = dict(s)
        scopy["derived_status"] = _derive_status(scopy)
        last_seen = int(scopy.get("last_seen", 0) or 0)
        scopy["seen_ago"] = (now - last_seen) if last_seen else None
        out.append(scopy)
    # return as list for stable ordering in clients
    return JSONResponse(sorted(out, key=lambda x: x.get("name", "")))


@app.get("/users")
def users_list():
    users = load_users()
    # return as list for stable ordering in clients
    return JSONResponse(list(users.values()))


@app.post("/user_upsert")
def user_upsert(payload: Dict[str, Any]):
    """Create/update a user record.
    Payload fields: uid (int|str, required), name (str), time_limit (str), chores_limit (int), notes (str)
    """
    uid = payload.get("uid")
    if uid is None:
        raise HTTPException(400, "uid required")
    # normalize to string key
    uid_s = str(int(uid)) if str(uid).isdigit() else str(uid)
    users = load_users()
    rec = users.get(uid_s) or {}
    # merge fields
    name = payload.get("name", rec.get("name"))
    time_limit = payload.get("time_limit", rec.get("time_limit"))
    chores_limit = payload.get("chores_limit", rec.get("chores_limit"))
    notes = payload.get("notes", rec.get("notes"))
    users[uid_s] = {
        "uid": uid_s,
        **({"name": name} if name is not None else {}),
        **({"time_limit": time_limit} if time_limit is not None else {}),
        **({"chores_limit": chores_limit} if chores_limit is not None else {}),
        **({"notes": notes} if notes is not None else {}),
    }
    save_users(users)
    return {"ok": True}


@app.post("/prereg")
def preregister_sailor(payload: Dict[str, Any]):
    name = payload.get("name")
    ip = payload.get("ip")
    services = payload.get("services", [])
    if isinstance(services, str):
        services = [s.strip() for s in services.split(",") if s.strip()]
    if not name or not ip:
        raise HTTPException(400, "name and ip required")
    crew = load_crew()
    max_time = payload.get("max_time")  # optional string DD-hh:mm:ss
    crew[name] = {
        "name": name,
        "ip": ip,
        "services": services,
        "status": "down",
        "last_seen": 0,
        "cpus": int(crew.get(name, {}).get("cpus", 0) or 0),
        "gpus": crew.get(name, {}).get("gpus", []) or [],
        "ram": int(crew.get(name, {}).get("ram", 0) or 0),
        "used_cpus": 0,
        "used_gpus": 0,
        **({"max_time": max_time} if max_time else {}),
    }
    save_crew(crew)
    return {"ok": True}


@app.post("/user_cancel")
def user_cancel(payload: Dict[str, Any]):
    chore_id = str(payload.get("chore_id"))
    if not chore_id:
        raise HTTPException(400, "chore_id required")
    chores = load_chores()
    chore = chores.get(chore_id)
    if not chore:
        raise HTTPException(404, "chore not found")
    sailor_name = chore.get("sailor")
    if sailor_name:
        crew = load_crew()
        s = crew.get(sailor_name)
        if s:
            # mark cancel requested first to ensure consistent reason
            chore["status"] = "cancel_requested"
            chore["reason"] = payload.get("reason") or "canceled by user"
            chore["cancel_requested_at"] = now_ts()
            chore["cancel_source"] = "user"
            chores[chore_id] = chore
            save_chores(chores)
            try:
                url = f"http://{s['ip']}:{s.get('port', 8001)}/captain_cancels"
                requests.post(url, json={"chore_id": chore_id}, timeout=5)
            except Exception as e:
                logger.error(f"Failed to cancel chore on sailor: {e}")
        # already stamped state above; return OK
        return {"ok": True}
    else:
        # no sailor attached -> mark as canceled to keep a record
        chore["status"] = "canceled"
        chore["end"] = now_ts()
        chore["reason"] = payload.get("reason") or "canceled by user"
        chores[chore_id] = chore
        save_chores(chores)
        return {"ok": True}

# CLI


def cli():
    parser = argparse.ArgumentParser(description="Captain server and CLI")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--serve", type=int, metavar="PORT", help="Run API server on PORT")
    group.add_argument("--chore", nargs="+", metavar="key=value", help="Submit a chore: script= service= cpus= gpus=")
    group.add_argument("--consult", action="store_true", help="Consult chores (use --all for all)")
    group.add_argument("--cancel", metavar="CHORE_ID", help="Cancel a chore by id")
    group.add_argument("--prereg", nargs=2, metavar=("NAME", "IP"), help="Preregister a sailor")
    group.add_argument("--crew", action="store_true", help="Display crew status and resources")
    group.add_argument("--users", action="store_true", help="Display users registry")
    group.add_argument("--user-set", nargs="+", metavar="key=value", help="Create/update a user: uid= name= time_limit= chores_limit= notes=")
    parser.add_argument("--services", default="", help="Comma-separated services for prereg (with --prereg)")
    parser.add_argument("--max-time", dest="max_time", default=None, help="Optional max time for each chore on this sailor (DD-hh:mm:ss) with --prereg")
    parser.add_argument("--all", action="store_true", help="With --consult, show all chores")
    parser.add_argument("--json", action="store_true", help="Output raw JSON (must be last argument)")
    parser.add_argument("--reason", default=None, help="Optional reason string (used with --cancel)")

    args = parser.parse_args()

    json_last = ("--json" in sys.argv and sys.argv[-1] == "--json" and getattr(args, "json", False))

    if args.serve is not None:
        port = args.serve
        logger.info(f"Captain serving on 0.0.0.0:{port}")
        _write_serve_flag(port)
        try:
            uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
        finally:
            _remove_serve_flag()
        return

    if args.chore is not None:
        # parse key=value
        kv = {}
        for item in args.chore:
            if "=" in item:
                k, v = item.split("=", 1)
                kv[k.strip()] = v.strip()
        base = _discover_base_url()
        if not base:
            msg = "Captain is not reachable."
            print(json.dumps({"ok": False, "error": msg}) if json_last else msg)
            return
        payload = {
            "script": kv.get("script"),
            "service": kv.get("service"),
            "ressources": {
                "cpus": int(kv.get("cpus", 1) or 1),
                "gpus": int(kv.get("gpus", 0) or 0),
            },
            "owner": os.getuid(),
        }
        url = f"{base}/user_chore"
        try:
            resp = requests.post(url, json=payload, timeout=5)
            data = resp.json()
            if json_last:
                print(json.dumps(data))
            else:
                if data.get("ok"):
                    print(f"Chore submitted: {data.get('chore_id')}")
                else:
                    print(f"Submission failed: {data}")
        except Exception as e:
            out = {"ok": False, "error": str(e)}
            print(json.dumps(out) if json_last else f"Error: {out['error']}")
        return

    if args.consult:
        base = _discover_base_url()
        if not base:
            msg = "Captain is not reachable."
            print(json.dumps({"ok": False, "error": msg}) if json_last else msg)
            return
        url = f"{base}/user_consult?all={'true' if args.all else 'false'}"
        try:
            resp = requests.get(url, timeout=5)
            data = resp.json()
            if json_last:
                print(json.dumps(data))
            else:
                # Beautify as table
                headers = ["CHORE_ID", "OWNER", "SERVICE", "STATUS", "SAILOR", "AGE", "CPUS", "GPUS", "EXIT", "REASON"]
                rows = []
                now = now_ts()
                for c in sorted(data, key=lambda x: x.get("start", 0)):
                    age = now - int(c.get("start", now) or now)
                    res = c.get("ressources", {}) or {}
                    rows.append([
                        c.get("chore_id", "?"),
                        c.get("owner", "-"),
                        c.get("service", "-") or "-",
                        c.get("status", "-") or "-",
                        c.get("sailor", "-") or "-",
                        f"{age}s",
                        str(res.get("cpus", "-")),
                        str(res.get("gpus", "-")),
                        str(c.get("exit_code", "-")),
                        str(c.get("reason", "-") or "-"),
                    ])
                widths = [len(h) for h in headers]
                for r in rows:
                    for i, col in enumerate(r):
                        widths[i] = max(widths[i], len(str(col)))

                def fmt_row(cols):
                    return "  ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols))

                print(fmt_row(headers))
                print("  ".join("-" * w for w in widths))
                for r in rows:
                    print(fmt_row(r))
        except Exception as e:
            out = {"ok": False, "error": str(e)}
            print(json.dumps(out) if json_last else f"Error: {out['error']}")
        return

    if args.crew:
        base = _discover_base_url()
        if not base:
            msg = "Captain is not reachable."
            print(json.dumps({"ok": False, "error": msg}) if json_last else msg)
            return
        url = f"{base}/crew"
        try:
            resp = requests.get(url, timeout=5)
            data = resp.json()
            if json_last:
                print(json.dumps(data))
            else:
                if not data:
                    print("No sailors in crew.")
                    return
                headers = ["NAME", "IP:PORT", "SERVICES", "STATUS", "SEEN", "CPUS(u/t)", "GPUS(u/t)", "RAM"]
                rows = []
                for s in sorted(data, key=lambda x: x.get("name", "")):
                    name = s.get("name", "?")
                    ipport = f"{s.get('ip', '?')}:{s.get('port', 8001)}"
                    services = s.get("services", [])
                    if isinstance(services, str):
                        services = [x.strip() for x in services.split(",") if x.strip()]
                    status = _derive_status(s)
                    last_seen = int(s.get("last_seen", 0) or 0)
                    ago = now_ts() - last_seen if last_seen else None
                    seen_str = f"{ago}s" if ago is not None else "never"
                    total_cpus = int(s.get("cpus", 0) or 0)
                    used_cpus = int(s.get("used_cpus", 0) or 0)
                    total_gpus = len(s.get("gpus", []) or [])
                    used_gpus = int(s.get("used_gpus", 0) or 0)
                    ram = _fmt_bytes(s.get("ram", 0))
                    rows.append([
                        name,
                        ipport,
                        ",".join(services) if services else "-",
                        status,
                        seen_str,
                        f"{used_cpus}/{total_cpus}",
                        f"{used_gpus}/{total_gpus}",
                        ram,
                    ])
                widths = [len(h) for h in headers]
                for r in rows:
                    for i, col in enumerate(r):
                        widths[i] = max(widths[i], len(str(col)))

                def fmt_row(cols):
                    return "  ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols))
                print(fmt_row(headers))
                print("  ".join("-" * w for w in widths))
                for r in rows:
                    print(fmt_row(r))
        except Exception as e:
            out = {"ok": False, "error": str(e)}
            print(json.dumps(out) if json_last else f"Error: {out['error']}")
        return

    if getattr(args, "users", False):
        base = _discover_base_url()
        if not base:
            msg = "Captain is not reachable."
            print(json.dumps({"ok": False, "error": msg}) if json_last else msg)
            return
        url = f"{base}/users"
        try:
            resp = requests.get(url, timeout=5)
            data = resp.json()
            if json_last:
                print(json.dumps(data))
            else:
                if not data:
                    print("No users in registry.")
                    return
                headers = ["UID", "NAME", "TIME_LIMIT", "CHORES_LIMIT", "NOTES"]
                rows = []
                for u in sorted(data, key=lambda x: str(x.get("uid", ""))):
                    rows.append([
                        str(u.get("uid", "")),
                        u.get("name", "-"),
                        u.get("time_limit", "-"),
                        str(u.get("chores_limit", "-")),
                        u.get("notes", "-") or "-",
                    ])
                widths = [len(h) for h in headers]
                for r in rows:
                    for i, col in enumerate(r):
                        widths[i] = max(widths[i], len(str(col)))

                def fmt_row(cols):
                    return "  ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols))
                print(fmt_row(headers))
                print("  ".join("-" * w for w in widths))
                for r in rows:
                    print(fmt_row(r))
        except Exception as e:
            out = {"ok": False, "error": str(e)}
            print(json.dumps(out) if json_last else f"Error: {out['error']}")
        return

    if getattr(args, "user_set", None) is not None:
        kv = {}
        for item in args.user_set:
            if "=" in item:
                k, v = item.split("=", 1)
                kv[k.strip()] = v.strip()
        if "uid" not in kv:
            out = {"ok": False, "error": "uid is required (uid=<uid>)"}
            print(json.dumps(out) if json_last else f"Error: {out['error']}")
            return
        base = _discover_base_url()
        if not base:
            msg = "Captain is not reachable."
            print(json.dumps({"ok": False, "error": msg}) if json_last else msg)
            return
        payload = {
            "uid": kv.get("uid"),
        }
        if "name" in kv:
            payload["name"] = kv["name"]
        if "time_limit" in kv:
            payload["time_limit"] = kv["time_limit"]
        if "chores_limit" in kv:
            try:
                payload["chores_limit"] = int(kv["chores_limit"]) if kv["chores_limit"] != "" else None
            except Exception:
                payload["chores_limit"] = kv["chores_limit"]
        if "notes" in kv:
            payload["notes"] = kv["notes"]
        url = f"{base}/user_upsert"
        try:
            resp = requests.post(url, json=payload, timeout=5)
            data = resp.json()
            if json_last:
                print(json.dumps(data))
            else:
                if data.get("ok"):
                    print(f"User {kv.get('uid')} updated")
                else:
                    print(f"User update failed: {data}")
        except Exception as e:
            out = {"ok": False, "error": str(e)}
            print(json.dumps(out) if json_last else f"Error: {out['error']}")
        return

    if args.cancel is not None:
        base = _discover_base_url()
        if not base:
            msg = "Captain is not reachable."
            print(json.dumps({"ok": False, "error": msg}) if json_last else msg)
            return
        url = f"{base}/user_cancel"
        try:
            payload = {"chore_id": args.cancel}
            if args.reason:
                payload["reason"] = args.reason
            resp = requests.post(url, json=payload, timeout=5)
            data = resp.json()
            if json_last:
                print(json.dumps(data))
            else:
                if data.get("ok"):
                    print(f"Cancel requested for chore {args.cancel}")
                else:
                    print(f"Cancel failed: {data}")
        except Exception as e:
            out = {"ok": False, "error": str(e)}
            print(json.dumps(out) if json_last else f"Error: {out['error']}")
        return

    if args.prereg is not None:
        name, ip = args.prereg
        services = [s.strip() for s in args.services.split(",") if s.strip()]
        base = _discover_base_url()
        if not base:
            msg = "Captain is not reachable."
            print(json.dumps({"ok": False, "error": msg}) if json_last else msg)
            return
        url = f"{base}/prereg"
        try:
            req = {"name": name, "ip": ip, "services": services}
            if args.max_time:
                req["max_time"] = args.max_time
            resp = requests.post(url, json=req, timeout=5)
            data = resp.json()
            if json_last:
                print(json.dumps(data))
            else:
                if data.get("ok"):
                    extra = f", max_time={args.max_time}" if args.max_time else ""
                    print(f"Preregistered sailor '{name}' at {ip} with services: {','.join(services) if services else '-'}{extra}")
                else:
                    print(f"Prereg failed: {data}")
        except Exception as e:
            out = {"ok": False, "error": str(e)}
            print(json.dumps(out) if json_last else f"Error: {out['error']}")
        return

    parser.print_help()


if __name__ == "__main__":
    cli()
