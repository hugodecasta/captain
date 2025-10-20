from boat_chest import get_chores, get_sailors
from boat_chest import get_sailors_by_service, get_sailor_available_cpus, get_sailor_available_gpus, get_sailor_by_name
from boat_chest import get_chore_requested_ressources, assign_chore_sailor, get_chore_status, set_chore_infos, archive_chore
from boat_chest import log_message
from boat_chest import CHORE_STATUS_PENDING, CHORE_STATUS_ASSIGNED, CHORE_STATUS_RUNNING, CHORE_STATUS_COMPLETED, CHORE_STATUS_FAILED
from boat_chest import create_service
from boat_chest import SAILOR_STATUS_DOWN
import time

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

        if available_cpus < rcpus or available_gpus < rgpus:
            continue
        assign_chore_sailor(chore["ID"], sailor_name)
        set_chore_infos(chore["ID"], f"Assigned to sailor {sailor_name}")
        log(f"Chore {chore['ID']} assigned to sailor {sailor_name}")
        return sailor_name
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

# region -------------------------------------------------------- LOOP
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------


def loop():
    log("Lieutenant started")
    while True:
        assign_chores()
        archive_chores()
        time.sleep(1)


# region -------------------------------------------------------- MAIN
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Lieutenant CLI")
    parser.add_argument('--create-service', action='store_true', help='Create a new service')

    args = parser.parse_args()
    if args.create_service:
        create_service_lieutenant()
        exit(0)

    loop()
