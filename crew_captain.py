import argparse
from boat_chest import get_chores_by_owner, add_chore, get_chore_status, get_chore_requested_ressources, cancel_chore
from boat_chest import get_version
from boat_chest import print_table
from boat_chest import log_message
from boat_chest import pre_register_sailor, get_sailors, remove_sailor
from boat_chest import CHORE_STATUS_PENDING, CHORE_STATUS_RUNNING, CHORE_STATUS_COMPLETED, CHORE_STATUS_FAILED, CHORE_STATUS_CANCEL_REQUESTED, CHORE_STATUS_CANCELED, CHORE_STATUS_ASSIGNED
import os
import json
import sys
from boat_chest import requires_root


def log(msg: str):
    log_message("captain", msg)

# region -------------------------------------------------------- FUNCTIONS
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------


def consult(owner: str):
    chores = get_chores_by_owner(owner)
    return chores


def request_chore(owner: str, rsailor: str, rservice: str, configuration: str):
    log(f"Requesting chore for owner {owner} with RSailor {rsailor} and RService {rservice}")
    return add_chore(owner, rsailor, rservice, configuration)


def preregister_sailor(sailor_name: str, services: str):
    log(f"Pre-registering sailor {sailor_name} with services: {services}")
    pre_register_sailor(sailor_name, services)


def captain_remove_sailor(sailor_name: str):
    log(f"Removing sailor {sailor_name}")
    remove_sailor(sailor_name)


def captain_cancel_chore(chore_id: int):
    log(f"Cancelling chore {chore_id}")
    cancel_chore(chore_id)

# region -------------------------------------------------------- ARGS
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------


def create_chore_row(chore, is_small):
    status = get_chore_status(chore)
    config = json.loads(chore["configuration"])
    cpus, gpus = get_chore_requested_ressources(chore)
    wd = config.get('working_directory', 'N/A')
    script = config.get('script', 'N/A')
    out = config.get('output_file', 'N/A')
    if is_small:
        if status == CHORE_STATUS_CANCEL_REQUESTED:
            status = "CR"
        elif status == CHORE_STATUS_PENDING:
            status = "P"
        elif status == CHORE_STATUS_RUNNING:
            status = "R"
        elif status == CHORE_STATUS_CANCELED:
            status = "Cld"
        elif status == CHORE_STATUS_COMPLETED:
            status = "done"
        elif status == CHORE_STATUS_FAILED:
            status = "F"
        elif status == CHORE_STATUS_ASSIGNED:
            status = "A"
        return [chore["ID"], cpus, gpus, script, status, chore["Infos"]]
    return [chore["ID"], chore["owner"], chore["RSailor"], chore["RService"], cpus, gpus, wd, script, out, status, chore["Sailor"], chore["Infos"]]


def create_sailor_row(sailor):
    gpus = sailor.get("GPUS", '-')
    gpus = '-' if gpus is None else gpus
    cpus = sailor.get("CPUS", '-')
    cpus = '-' if cpus is None else cpus
    used_gpus = sailor.get("UsedGPUS", '-')
    used_gpus = '-' if used_gpus is None else used_gpus
    used_cpus = sailor.get("UsedCPUS", '-')
    used_cpus = '-' if used_cpus is None else used_cpus
    ram = sailor.get("RAM", '-')
    ram = '-' if ram is None else ram
    cpu_disp = f"{used_cpus}/{cpus}"
    gpu_disp = f"{used_gpus}/{gpus}"
    ram_disp = ram
    return [sailor["ID"], sailor["Name"], sailor["Services"], sailor["Status"], cpu_disp, gpu_disp, ram_disp]


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Crew Captain")
    mode_group = parser.add_mutually_exclusive_group(required=True)

    parser.add_argument("--version", action="version", help="Show version information", version=f"Captain {get_version()}")
    mode_group.add_argument('--consult', dest="mode", required=False, action='store_const', const='consult', help='Consult chores for owner')
    mode_group.add_argument('--chore', dest="mode", required=False, action='store_const', const='chore', help='Request a chore')
    mode_group.add_argument('--crew', dest="mode", required=False, action='store_const', const='crew', help='Display crew members')
    mode_group.add_argument('--prereg', dest="mode", required=False, action='store_const', const='prereg', help='Display crew members')
    mode_group.add_argument('--rmsailor', dest="mode", required=False, action='store_const', const='rmsailor', help='Remove sailor')
    mode_group.add_argument('--cancel', dest="mode", required=False, action='store_const', const='cancel', help='Cancel chore')
    mode_group.add_argument('--sailor-offset', dest="mode", required=False, action='store_const', const='sailor-offset', help='Set sailor time offset')

    parser.add_argument('-slr', '--rsailor', type=str, required=False, help='RSailor name')
    parser.add_argument('-srv', '--rservice', type=str, required=False, help='RService name')
    parser.add_argument('-wd', '--working-directory', type=str, required=False, help='Working directory')
    parser.add_argument('-sc', '--script', type=str, required=False, help='Script to execute')
    parser.add_argument('-out', '--output-file', type=str, required=False, help='Output file')
    parser.add_argument('-c', '--cpus', type=int, required=False, help='CPUs required', default=1)
    parser.add_argument('-g', '--gpus', type=int, required=False, help='GPUs required', default=0)

    parser.add_argument('-n', '--name', type=str, required=False, help='Sailor name')
    parser.add_argument('-s', '--services', type=str, required=False, help='Comma separated list of services')

    parser.add_argument('-off', '--offset', type=int, required=False, help='Sailor time offset in seconds')

    parser.add_argument('-cid', type=int, required=False, help='Chore ID to cancel')

    parser.add_argument('--small', action='store_true', required=False, help='Small display')

    args = parser.parse_args()

    owner = os.getuid()

    # region .... consult
    if args.mode == 'consult':
        chores = consult(owner)
        is_small = args.small
        if len(chores) == 0:
            print("No chores found")
        else:
            headers = ["ID", "Owner", "RSlr", "RSrv", "CPUs", "GPUs", "WD", "SC", "Out", "Status", "Sailor", "Infos"]
            if is_small:
                headers = ["ID", "CPUs", "GPUs", "SC", "Status", "Infos"]  # Shortened for small display
            rows = [
                create_chore_row(chore, is_small)
                for chore in chores
            ]
            print_table(headers, rows)

    # region .... cancel chore
    elif args.mode == 'cancel':
        if not args.cid:
            parser.error("the following arguments are required for 'cancel' mode: -cid")
        chore_id = args.cid
        captain_cancel_chore(chore_id)
        print(f"Chore {chore_id} cancellation requested")

    # region .... chore
    elif args.mode == 'chore':
        if not args.working_directory or not args.script:
            parser.error("the following arguments are required for 'chore' mode: -wd/--working-directory, -sc/--script")
        if not args.rsailor and not args.rservice:
            parser.error("the following arguments are required for 'chore' mode: -slr/--rsailor, -srv/--rservice")

        wd = args.working_directory
        script = args.script
        cpus = args.cpus
        gpus = args.gpus
        output_file = args.output_file if args.output_file else None
        configuration = json.dumps({'cpus': cpus, 'gpus': gpus, 'working_directory': wd, 'script': script, 'output_file': output_file})

        rsailor = args.rsailor if args.rsailor else None
        rservice = args.rservice if args.rservice else None
        chore_id = request_chore(owner, rsailor, rservice, configuration)
        print(f"Chore requested with ID: {chore_id}")

    # region .... pre register sailor
    elif args.mode == 'prereg':
        requires_root()
        if not args.name:
            parser.error("the following arguments are required for 'prereg' mode: -n/--name")
        sailor_name = args.name
        services = args.services if args.services else ""
        preregister_sailor(sailor_name, services)
        print(f"Sailor {sailor_name} pre-registered with services: {services}")

    # region .... crew
    elif args.mode == 'crew':
        sailors = get_sailors()
        if len(sailors) == 0:
            print("No sailors found")
        else:
            headers = ["ID", "Name", "Services", "Status", "CPUS", "GPUS", "RAM"]
            rows = [
                create_sailor_row(sailor)
                for sailor in sailors
            ]
            print_table(headers, rows)

    # region .... remove sailor
    elif args.mode == 'rmsailor':
        requires_root()
        if not args.name:
            parser.error("the following arguments are required for 'rmsailor' mode: -n/--name")
        sailor_name = args.name
        captain_remove_sailor(sailor_name)
        print(f"Sailor {sailor_name} removed")

    # region .... sailor time offset
    elif args.mode == 'sailor-offset':
        requires_root()
        if not args.name or args.offset is None:
            parser.error("the following arguments are required for 'sailor-offset' mode: -n/--name, -off/--offset")
        sailor_name = args.name
        time_offset = args.offset
        from boat_chest import set_sailor_time_offset
        set_sailor_time_offset(sailor_name, time_offset)
        print(f"Sailor {sailor_name} time offset set to {time_offset} seconds")
