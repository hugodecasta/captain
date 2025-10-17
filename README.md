# Captain/Sailor

A minimal resource scheduler with a Captain (controller) and Sailors (workers).

## Setup

### One-line system-wide install (/opt)

This installs the project under `/opt/captain`, creates a dedicated virtualenv, installs dependencies, and exposes `captain` and `sailor` globally in `/usr/local/bin`.

  ./install.sh

You’ll be prompted for sudo if not already root.

Notes:
- Data lives under `/opt/captain/data`. By default the installer makes it world-writable for convenience across users. For stricter setups, chown/chgrp to a dedicated group and tighten permissions, then run via systemd.
- If your system lacks the venv module, install it first (Debian/Ubuntu): `sudo apt install python3-venv`.

### Developer/local setup

Use the project directly or the system-wide installer above. If you prefer a local venv, create one and install requirements:

  python3 -m venv .captainenv && . .captainenv/bin/activate && pip install -r requirements.txt

## Run

- Start Captain API:

  captain --serve 8000

- On each Sailor machine (first run prompts for config):

  sailor --serve 8001

- Preregister a sailor on the Captain (name/IP/services):

  captain --prereg bob 10.0.0.12 --services GPU,CPU

- Optional per-chore max time for a sailor (DD-hh:mm:ss):

  captain --prereg bob 10.0.0.12 --services GPU,CPU --max-time 00-00:10:00

- Submit a chore as a user (Captain must be running):

  captain --chore script=/mnt/boat/user/exec.sh service=GPU cpus=2 gpus=1

- Consult your chores:

  captain --consult

- Cancel a chore:

  captain --cancel 123456789 --reason "canceled by user"

### CLI server discovery

The `captain` CLI automatically discovers the running server in a few ways:

- CLI overrides (highest priority):
  - --url http://host:port
  - --host HOST --port PORT
- Environment variables:
  - CAPTAIN_URL=http://host:port
  - CAPTAIN_HOST and CAPTAIN_PORT
- Config file:
  - $XDG_CONFIG_HOME/captain/config.json or ~/.config/captain/config.json
    - {"url": "http://host:port"} or {"host": "host", "port": 8000}
- Serve flag files written by `--serve` in multiple standard locations:
  - data/captain/serve.json (in repo)
  - $XDG_STATE_HOME/captain/serve.json
  - $XDG_DATA_HOME/captain/serve.json
  - $XDG_RUNTIME_DIR/captain/serve.json
  - /var/run/captain/serve.json
  - /tmp/captain_serve.json

This allows running `captain --crew` from any directory after the server has been started, without needing to be in the repository folder.

## Systemd services

Create and start a Sailor system service (requires sudo; runs as root so it can switch to chore owners):

  sudo sailor --create-service --port 8001

Notes:
- If you installed via the one-line installer, the `sailor` CLI is in your PATH. In a dev checkout, you can also run:

    sudo python3 sailor.py --create-service --port 8001

- The command will run Sailor's first-time prompt if needed, write `/etc/systemd/system/sailor-<name>.service`, reload systemd, enable, and start the service.
- Replace `<name>` with your Sailor name from `data/sailor/resources.json` (set during first-run).

Check logs:

  sudo journalctl -u sailor-<name>.service -f

## Data

- Captain data: data/captain/crew.json and data/captain/chores.json
- Captain users: data/captain/users.json
- Sailor data: data/sailor/resources.json and data/sailor/running_chores.json

## Notes

- CPU/GPU constraints are advisory via env (OMP_NUM_THREADS, CUDA_VISIBLE_DEVICES). To hard-limit CPUs/GPUs, integrate cgroups or container runtime.
- Network/registration errors are logged and retried on next heartbeat.

## New Features

- max_time on sailors (optional): In `crew.json` each sailor may have a `max_time` string formatted as `DD-hh:mm:ss`. The Captain will ensure no chore runs on that sailor beyond this duration. If a running chore exceeds the time, the Captain requests cancellation and sets the chore `reason` to `"exceeded time limit"`.
- chore reason tracking: Chores now include a `reason` field.
  - Set at creation to `"no available sailor"` until assigned.
  - On successful assignment, reason is cleared.
  - On user cancel, reason defaults to `"canceled by user"` (overridable via `--reason`).
  - On time limit cancel, reason is `"exceeded time limit"`.
  - The `captain --consult` view includes a REASON column.

- users registry and limits (V1.3.0): `data/captain/users.json` contains records keyed by UID string:

  {"<UID>": {"uid": "<UID>", "name": "alice", "time_limit": "DD-hh:mm:ss", "chores_limit": 3, "notes": "..."}}

  - chores_limit: Maximum number of active chores per user. Submissions beyond the limit are rejected with HTTP 403.
  - time_limit: Maximum cumulative run/assigned time across a user's active chores. The cleanup loop enforces this by cancel-requesting excess chores and marking reason "exceeded user time limit". Older chores are preserved; newer ones are canceled first.
  - Manage via CLI:

    captain --users
    captain --user-set uid=1000 name=alice chores_limit=2 time_limit=00-00:30:00 notes="burst limited"
