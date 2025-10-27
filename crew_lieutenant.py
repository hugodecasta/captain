from boat_chest import get_chores, get_sailors
from boat_chest import get_sailors_by_service, get_sailor_available_cpus, get_sailor_available_gpus, get_sailor_by_name
from boat_chest import get_chore_requested_ressources, assign_chore_sailor, get_chore_status, set_chore_infos, archive_chore
from boat_chest import log_message
from boat_chest import CHORE_STATUS_PENDING, CHORE_STATUS_CANCEL_REQUESTED, CHORE_STATUS_ASSIGNED, CHORE_STATUS_RUNNING, CHORE_STATUS_COMPLETED, CHORE_STATUS_FAILED
from boat_chest import create_service
from boat_chest import SAILOR_STATUS_DOWN
from boat_chest import requires_root
from boat_chest import get_logs_by_owner, get_logs_unique_owners
from boat_chest import update_db
import time

import os
import pwd
import random


def log(message: str):
    log_message("lieutenant", message)


# region -------------------------------------------------------- FUNCTIONS
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------

def assign_chore(chore, sailors):
    status = get_chore_status(chore)
    if status != CHORE_STATUS_PENDING:
        return

    rcpus, rgpus = get_chore_requested_ressources(chore)

    requested_sailor = chore["RSailor"]
    requested_service = chore["RService"]

    if requested_sailor:
        candidates = [get_sailor_by_name(requested_sailor, sailors)]
    elif requested_service:
        candidates = get_sailors_by_service(requested_service, sailors)
    else:
        raise Exception("Chore has no RSailor or RService")

    candidates = [c for c in candidates if c is not None and c['CPUS'] is not None]
    candidates = [c for c in candidates if c["Status"] != SAILOR_STATUS_DOWN]

    random.shuffle(candidates)

    for candidate in candidates:
        sailor_name = candidate["Name"]
        available_cpus = get_sailor_available_cpus(candidate)
        available_gpus = get_sailor_available_gpus(candidate)

        print(sailor_name, available_cpus, available_gpus, rcpus, rgpus)

        if rcpus == -1:
            rcpus = available_cpus
        if rgpus == -1:
            rgpus = available_gpus

        if available_cpus < rcpus or available_gpus < rgpus:
            continue

        candidate["UsedCPUS"] += rcpus
        candidate["UsedGPUS"] += rgpus

        assign_chore_sailor(chore["ID"], sailor_name)
        set_chore_infos(chore["ID"], f"Assigned to sailor {sailor_name}")
        log(f"Chore {chore['ID']} assigned to sailor {sailor_name}")
        return sailor_name
    if chore["Infos"] != "No available sailor":
        log(f"No available sailor found for chore {chore['ID']}")
        set_chore_infos(chore["ID"], "No available sailor")
    return None


def assign_chores():
    sailors = get_sailors()
    chores = get_chores()
    for chore in chores:
        assign_chore(chore, sailors)


def archive_chores():
    chores = get_chores()
    current_time = time.time()
    for chore in chores:
        end = chore.get("End")
        if get_chore_status(chore) == CHORE_STATUS_CANCEL_REQUESTED:
            end = chore["Start"]
        if end is None or end == -2:
            continue
        if current_time - end > 60 * 2:  # 2 minutes
            chore_id = chore["ID"]
            log(f"Archiving chore {chore_id}")
            set_chore_infos(chore_id, "Archived")
            archive_chore(chore_id)


def create_service_lieutenant():
    create_service(
        "lieutenant",
        "Captain Lieutenant service",
        "/usr/local/bin/lieutenant",
    )


def verify_db_version():
    update_db()

# region -------------------------------------------------------- LOOP
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------


def loop():
    log("Lieutenant started")
    verify_db_version()
    while True:
        assign_chores()
        archive_chores()
        time.sleep(1)


# region -------------------------------------------------------- FRONT SERVER
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------

PORT = 9874


def start_front_server(port):
    from flask import Flask, request, jsonify

    app = Flask("Captain front")

    # serve all front file from ./front
    from flask import send_from_directory

    front_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'front')

    @app.route('/<path:path>')
    def send_front(path):
        return send_from_directory(front_dir, path)

    # serve index.html from current working dir ./front
    @app.route('/')
    def send_index():
        return send_from_directory(front_dir, 'index.html')

    # crew service
    @app.route('/api/crew/', methods=['GET'])
    def get_crew():
        crew = get_sailors()
        return jsonify(crew)

    @app.route('/api/logs/owners', methods=['GET'])
    def get_logs_owners():
        owners = get_logs_unique_owners()
        return jsonify(owners)

    @app.route('/api/logs/by_owner', methods=['GET'])
    def get_logs_by_owner_api():
        owner = request.args.get("owner")
        logs = get_logs_by_owner(owner)
        return jsonify(logs)

    # chores
    @app.route('/api/chores/', methods=['GET'])
    def get_chores_api():
        import os
        chores = get_chores()
        for chore in chores:
            status = get_chore_status(chore)
            chore["Status"] = status
            if chore["owner"] is not None:
                chore["owner"] = pwd.getpwuid(chore["owner"]).pw_name
        return jsonify(chores)

    app.run(host='0.0.0.0', port=port)

# region -------------------------------------------------------- MAIN
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Lieutenant CLI")
    parser.add_argument('--create-service', action='store_true', help='Create a new service')
    parser.add_argument('--create-web-service', action='store_true', help='Create a new web service')
    parser.add_argument('--web-server', action='store_true', help='Start the web server')

    args = parser.parse_args()
    if args.create_service:
        requires_root()
        create_service_lieutenant()
        exit(0)

    elif args.create_web_service:
        requires_root()
        create_service(
            "lieutenant-web",
            "Captain Lieutenant Web service",
            "/usr/local/bin/lieutenant --web-server",
        )
        exit(0)

    elif args.web_server:
        start_front_server(PORT)
        exit(0)

    loop()
