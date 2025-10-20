from boat_chest import get_chores_by_sailor_name, get_chore_requested_ressources, set_chore_pid
from boat_chest import set_sailor_data, get_sailor_by_name, set_sailor_use, set_chore_end
from boat_chest import get_chore_requested_ressources, assign_chore_sailor, get_chore_status, set_chore_infos
from boat_chest import log_message
from boat_chest import get_version
from boat_chest import CHORE_STATUS_PENDING, CHORE_STATUS_ASSIGNED, CHORE_STATUS_RUNNING, CHORE_STATUS_CANCEL_REQUESTED
from boat_chest import DATA_DIR

import subprocess
import psutil
import threading

import time
import json


def log(message: str):
    log_message("sailor", message)


CONFIG_PATH = DATA_DIR / "sailor_config.json"


# region -------------------------------------------------------- FUNCTIONS
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------

def get_config():
    if not CONFIG_PATH.exists():
        raise Exception("Sailor not setup. Please run setup first.")
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
        return config


def get_sailor(config=None):
    config = get_config() if config is None else config
    sailor = get_sailor_by_name(config.get("Name"))
    if not sailor:
        raise Exception(f"Sailor {config.get('Name')} not found in database.")
    return sailor


def set_sailor_ressource_infos():
    import psutil
    config = get_config()
    sailor = get_sailor(config)
    total_cpus = psutil.cpu_count(logical=True)
    total_gpus = config.get("GPUS", 0)
    ram = round(psutil.virtual_memory().total / (1024 ** 3), 2)  # in GB
    set_sailor_data(sailor.get("Name"), total_cpus, total_gpus, ram)


def setup_sailor(name: str, gpus: int):
    found_sailor = get_sailor_by_name(name)
    if not found_sailor:
        return f"Sailor {name} not preregistered."
    with open(CONFIG_PATH, 'w') as f:
        config = {"Name": name, "GPUS": gpus}
        json.dump(config, f)
    set_sailor_ressource_infos()
    return f"Sailor {name} setup completed."


def update_sailor_ressource_use(my_running_chores=None):
    if my_running_chores is None:
        config = get_config()
        sailor = get_sailor(config)
        all_chores = get_chores_by_sailor_name(sailor.get("Name"))
        my_running_chores = [c for c in all_chores if get_chore_status(c) == CHORE_STATUS_RUNNING]

    used_cpus = 0
    used_gpus = 0
    for chore in my_running_chores:
        cpus, gpus = get_chore_requested_ressources(chore)
        used_cpus += cpus
        used_gpus += gpus

    set_sailor_use(sailor.get("Name"), used_cpus, used_gpus)

# region .... set chore status


def set_chore_failed(chore_id: int):
    set_chore_infos(chore_id, "Failed")
    set_chore_end(chore_id, None)


def set_chore_completed(chore_id: int, pid=int):
    set_chore_infos(chore_id, "Completed")
    set_chore_end(chore_id, pid)


def set_chore_canceled(chore_id: int):
    set_chore_infos(chore_id, "Canceled")
    set_chore_end(chore_id, -1)


# region .... process cache
connected_processes: dict[int, subprocess.Popen] = {}


# region .... watch process
def watch_process(chore_id: int, pid: int):
    print('watching', pid)
    process = connected_processes.get(pid)
    try:
        exit_code = process.wait()
        if exit_code == 0:
            set_chore_completed(chore_id, pid=pid)
            set_chore_infos(chore_id, infos="Completed successfully")
            log(f"Chore with PID {pid} completed successfully.")
        else:
            set_chore_failed(chore_id)
            log(f"Chore with PID {pid} failed with exit code {exit_code}.")
        del connected_processes[int(pid)]
    except Exception:
        print('exception in watch')
        pass


# region .... attach process


def attach_process(chore_id: int, pid: int):
    try:
        proc = psutil.Process(pid)
        if proc is None:
            set_chore_failed(chore_id)
            log(f"Failed to attach to process {pid} for chore {chore_id}")
            return
        connected_processes[pid] = proc
        watch_thread = threading.Thread(target=watch_process, args=(chore_id, pid,))
        watch_thread.start()
    except Exception:
        set_chore_failed(chore_id)
        log(f"Failed to attach to process {pid} for chore {chore_id}")


# region .... recall processes
def recall_processes():
    config = get_config()
    sailor = get_sailor(config)
    all_chores = get_chores_by_sailor_name(sailor.get("Name"))
    running_chores = [c for c in all_chores if get_chore_status(c) == CHORE_STATUS_RUNNING]
    for chore in running_chores:
        chore_pid = int(chore.get("PID"))
        attach_process(chore["ID"], chore_pid)


# region .... create process
def create_process(chore_id: int, script: str, working_directory: str, output_file: str, cpus: int, gpus: int, owner: int):
    import os
    import psutil
    import shlex

    owner = int(owner)

    def build_env():
        env = os.environ.copy()
        env.update({
            "HOME": working_directory or "/",
            "LOGNAME": str(owner),
            "USER": str(owner),
            "SHELL": "/bin/sh",
            "PATH": os.environ.get(
                "PATH",
                "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            ),
            "LANG": os.environ.get("LANG", "C.UTF-8"),
            "LC_ALL": os.environ.get("LC_ALL") or os.environ.get("LANG") or "C.UTF-8",
        })
        for v in (
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
            "NUMEXPR_NUM_THREADS",
            "VECLIB_MAXIMUM_THREADS",
        ):
            env[v] = str(cpus)
        env["MKL_DYNAMIC"] = "FALSE"
        env["OMP_DYNAMIC"] = "FALSE"
        env["TORCH_NUM_THREADS"] = str(cpus)
        env["TORCH_NUM_INTEROP_THREADS"] = str(max(1, min(cpus, 8)))
        gpu_list = list(range(gpus))
        gpu_str = ",".join(str(i) for i in gpu_list)
        env["CUDA_VISIBLE_DEVICES"] = gpu_str
        env["NVIDIA_VISIBLE_DEVICES"] = gpu_str
        env["HIP_VISIBLE_DEVICES"] = gpu_str
        env["ROCR_VISIBLE_DEVICES"] = gpu_str
        return env

    def _demote_and_setup():
        try:
            cpu_set = set(range(cpus))
            os.sched_setaffinity(0, cpu_set)
            os.chdir(working_directory)
            os.setgroups([])
            os.setuid(owner)
            os.umask(0o022)
        except Exception as e:
            log(f"Error in preexec_fn: {e}")
            print(e)
            exit(1)

    stdout_target = None
    stderr_target = None
    if output_file:
        out_dir = os.path.dirname(output_file) or "."
        inner = (
            f"mkdir -p {shlex.quote(out_dir)}; "
            f"echo 'START CHORE::{chore_id}' > {shlex.quote(output_file)}; "
            f"( /bin/bash {shlex.quote(script)}; ret=$?; echo 'END CHORE::{chore_id}'; exit $ret ) >> {shlex.quote(output_file)} 2>&1"
        )
        cmd = ["/bin/bash", "-lc", inner]
    else:
        cmd = ["/bin/bash", script]
        stdout_target = subprocess.DEVNULL
        stderr_target = subprocess.DEVNULL

    popen = psutil.Popen(
        cmd,
        env=build_env(),
        preexec_fn=_demote_and_setup,
        start_new_session=True,
        stdout=stdout_target,
        stderr=stderr_target,
    )
    pid = popen.pid
    attach_process(chore_id, pid)
    return pid


# region .... run chore
def run_chore(chore):
    configuration = json.loads(chore["configuration"])
    cpus, gpus = get_chore_requested_ressources(chore)

    start_time = time.time()
    script = configuration.get("script")
    working_directory = configuration.get("working_directory", ".")
    output_file = configuration.get("output_file", "chore_output.txt")
    owner = chore["owner"]

    pid = create_process(chore["ID"], script, working_directory, output_file, cpus, gpus, owner)
    log(f"Chore {chore['ID']} started with PID {pid} after waiting {time.time() - start_time:.2f}s")

    set_chore_pid(chore["ID"], pid)
    update_sailor_ressource_use()


def cancel_chore(chore):
    chore_id = chore["ID"]
    pid = chore.get("PID")
    if pid is None:
        log(f"Chore {chore_id} has no PID, cannot cancel. Autocanceling.")
        set_chore_canceled(chore_id)
        return
    pid = int(pid)
    process = connected_processes.get(pid)
    if process is None:
        log(f"Chore {chore_id} process with PID {pid} not found, cannot cancel.")
        set_chore_canceled(chore_id)
        return
    try:
        import os
        import signal
        pgid = os.getpgid(pid)
        try:
            os.killpg(pgid, signal.SIGTERM)
        except Exception:
            pass
        process.terminate()
        try:
            process.wait(timeout=10)
        except psutil.TimeoutExpired:
            os.killpg(pgid, signal.SIGKILL)
            process.kill()
        try:
            for child in process.children(recursive=True):
                try:
                    child.terminate()
                except Exception:
                    pass
            gone, alive = psutil.wait_procs(process.children(recursive=True), timeout=3)
            for child in alive:
                try:
                    child.kill()
                except Exception:
                    pass
        except Exception:
            pass
        log(f"Chore {chore_id} with PID {pid} terminated.")
        set_chore_canceled(chore_id)
    except Exception as e:
        log(f"Error terminating chore {chore_id} with PID {pid}: {e}")

# region .... handle chores


def handle_chores():
    config = get_config()
    sailor_name = config.get("Name")
    chores = get_chores_by_sailor_name(sailor_name)

    for chore in chores:
        status = get_chore_status(chore)

        if status == CHORE_STATUS_ASSIGNED:
            run_chore(chore)

        if status == CHORE_STATUS_CANCEL_REQUESTED:
            cancel_chore(chore)


def create_service_sailor():
    from boat_chest import create_service
    create_service(
        "sailor",
        "Captain Sailor service",
        "/usr/local/bin/sailor --run",
    )

# region -------------------------------------------------------- LOOP
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------


def loop():
    log("Sailor started")
    recall_processes()
    while True:
        set_sailor_ressource_infos()
        handle_chores()
        update_sailor_ressource_use()
        time.sleep(1)


# region -------------------------------------------------------- MAIN
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sailor")

    need_setup = CONFIG_PATH.exists() == False
    parser.add_argument("--setup", type=str, required=need_setup, help="Setup sailor with the given name")
    parser.add_argument("-g", "--gpus", type=int, required=need_setup, help="Number of GPUs to use", default=0)

    parser.add_argument('--create-service', action='store_true', help='Create a new service')

    parser.add_argument("--run", action="store_true", required=False, help="Run sailor loop")

    args = parser.parse_args()

    # region .... setup
    if args.setup:
        ret_info = setup_sailor(args.setup, args.gpus)
        log(f"Setup trial results in: {ret_info}")
        print(ret_info)

    # region .... setup
    if args.create_service:
        create_service_sailor()
        exit(0)

    # region .... run
    if args.run:
        log("Starting sailor loop")
        loop()
