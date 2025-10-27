import argparse
import json
import uuid
import pathlib
from pathlib import Path
import os
import requests
import time
import datetime
import sqlite3

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"
DB_PATH_FILE = DATA_DIR / "db_path.txt"

# region -------------------------------------------------------- INSTALL
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------

"""

DB tables are

Logs : (ID(int), Timestamp(int), Owner(text), Message(text))
Chores : (ID(int), Owner(str), RSailor(text|null), RService(text|null), configuration(text), Infos(text), Sailor(text), PID(text), Start(int), End(int|null))
Chores_archive : (ID(int), choreID(int), ...same as Chores)
Sailors : (ID(int), Name(text), Services(text), CPUS(int), GPUS(int), RAM(int), LastSeen(int), UsedCPUS(int), UsedGPUS(text))

"""


def install_db(db_path: str):
    with open(DB_PATH_FILE, "w") as f:
        f.write(db_path)
    if os.path.exists(db_path):
        print(f"Database already exists at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE Logs (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Timestamp INTEGER,
        Owner TEXT,
        Message TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE Chores (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        owner TEXT,
        RSailor TEXT,
        RService TEXT,
        configuration TEXT,
        Infos TEXT,
        Sailor TEXT,
        PID TEXT,
        Start INTEGER,
        End INTEGER
    )
    """)

    cursor.execute("""
    CREATE TABLE Chores_archive (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        choreID INTEGER,
        owner TEXT,
        RSailor TEXT,
        RService TEXT,
        configuration TEXT,
        Infos TEXT,
        Sailor TEXT,
        PID TEXT,
        Start INTEGER,
        End INTEGER
    )
    """)

    cursor.execute("""
    CREATE TABLE Sailors (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Name TEXT,
        Services TEXT,
        CPUS INTEGER,
        GPUS INTEGER,
        RAM INTEGER,
        LastSeen INTEGER,
        UsedCPUS INTEGER,
        UsedGPUS TEXT
    )
    """)

    conn.commit()
    conn.close()
    print(f"Database installed at {db_path}")

# region -------------------------------------------------------- DB
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------


def get_db_version_file_path():
    folder = str(get_db_path()).split('/')[0:-1]
    path = os.path.join(*folder, "db_version.txt")
    return Path(path)


def get_db_version():
    path = get_db_version_file_path()
    print(path)
    if not path.exists():
        return None
    with open(path, "r") as f:
        version = f.read().strip()
    return version


def set_db_version(version: str):
    with open(get_db_version_file_path(), "w") as f:
        f.write(version)


def update_db():
    version = get_db_version()

    if version is None:
        version = "1.0.0"
        connection = get_db_connection()
        cursor = connection.cursor()
        # add timeoffset column to sailors
        cursor.execute("ALTER TABLE Sailors ADD COLUMN TimeOffset INTEGER DEFAULT 0")
        connection.commit()
        connection.close()

    set_db_version(version)


def get_db_path():
    if not DB_PATH_FILE.exists():
        raise Exception("DB not setup. Please install the database first.")
    with open(DB_PATH_FILE, "r") as f:
        db_path = f.read().strip()

    return db_path


def get_db_connection():
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    return conn

# region -------------------------------------------------------- LOGS
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------


def log_message(owner: str, message: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    timestamp = int(time.time())
    print(f"[{datetime.datetime.fromtimestamp(timestamp)}] [{owner}] {message}")
    cursor.execute("INSERT INTO Logs (Timestamp, Owner, Message) VALUES (?, ?, ?)", (timestamp, owner, message))
    conn.commit()
    conn.close()


def get_logs_unique_owners():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT Owner FROM Logs")
    owners = [row[0] for row in cursor.fetchall()]
    conn.close()
    return owners


def get_logs_by_owner(owner: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Logs WHERE Owner = ? ORDER BY Timestamp DESC", (owner,))
    logs = cursor.fetchall()
    conn.close()
    logs_json = [{
        "ID": l[0],
        "Timestamp": l[1],
        "Owner": l[2],
        "Message": l[3]
    } for l in logs]
    return logs_json

# region -------------------------------------------------------- UTILS
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------


def get_version():
    with open(ROOT / "version.txt", "r") as f:
        version = f.read().strip().split('\n')[0]
    return version


def print_table(headers, rows):
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


def create_service(name, description, command):
    import shlex
    # get user name
    lines = [
        "[Unit]",
        f"Description={description} - Service {name}",
        "After=network.target",
        "",
        "[Service]",
        "Type=simple",
        f"ExecStart={command}",
        "Restart=on-failure",
        "Environment=PYTHONUNBUFFERED=1",
        f"User=Root",
        "",
        "[Install]",
        "WantedBy=multi-user.target",
        "",
    ]
    service_path = f"/etc/systemd/system/{name}.service"
    with open(service_path, "w") as f:
        f.write("\n".join(lines))
    os.system(f"systemctl daemon-reload")
    os.system(f"systemctl enable {name}.service")
    os.system(f"systemctl start {name}.service")


def requires_root():
    if os.geteuid() != 0:
        print("This operation requires root privileges. Please run as root.")
        exit(1)

# region -------------------------------------------------------- CHORES
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------


CHORE_STATUS_PENDING = "PENDING"
CHORE_STATUS_ASSIGNED = "ASSIGNED"

CHORE_STATUS_RUNNING = "RUNNING"
CHORE_STATUS_CANCEL_REQUESTED = "CANCEL_REQUESTED"

CHORE_STATUS_COMPLETED = "COMPLETED"
CHORE_STATUS_FAILED = "FAILED"
CHORE_STATUS_CANCELED = "CANCELED"


# region .... for all


def get_chores():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Chores")
    chores = cursor.fetchall()
    conn.close()
    # json version
    chores_json = [{
        "ID": c[0],
        "owner": int(c[1]),
        "RSailor": c[2], "RService": c[3],
        "configuration": c[4],
        "Infos": c[5],
        "Sailor": c[6], "PID": int(c[7]) if c[7] is not None else None,
        "Start": c[8], "End": c[9]}
        for c in chores]
    return chores_json


# region .... for getters

def set_chore_infos(chore_id: int, infos: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE Chores SET Infos = ? WHERE ID = ?", (infos, chore_id))
    conn.commit()
    conn.close()


def get_chore_status(chore) -> str:
    if chore["End"] == -2:
        return CHORE_STATUS_CANCEL_REQUESTED
    if chore["End"] is not None and chore["PID"] == -1:
        return CHORE_STATUS_CANCELED
    if chore["Start"] is not None:
        if chore["End"] is not None:
            if chore["PID"] is None:
                return CHORE_STATUS_FAILED
            return CHORE_STATUS_COMPLETED
        return CHORE_STATUS_RUNNING
    if chore["Sailor"] is not None:
        return CHORE_STATUS_ASSIGNED
    return CHORE_STATUS_PENDING


def get_chores_by_owner(owner: str):
    chores = get_chores()
    owner_chores = [c for c in chores if c["owner"] == owner]
    return owner_chores


def get_chores_by_sailor(sailor_name: str):
    chores = get_chores()
    sailor_chores = [c for c in chores if c["Sailor"] == sailor_name]
    return sailor_chores


def get_chore_by_id(chore_id: int):
    chores = get_chores()
    for c in chores:
        if c["ID"] == chore_id:
            return c
    return None


def get_chore_requested_ressources(chore):
    config = json.loads(chore["configuration"])
    rcpus = config.get("cpus", 0)
    rgpus = config.get("gpus", 0)
    return rcpus, rgpus

# region .... for captain


def add_chore(owner: str, rsailor: str, rservice: str, configuration: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO Chores (Owner, RSailor, RService, configuration, infos) VALUES (?, ?, ?, ?, ?)",
                   (owner, rsailor, rservice, configuration, "in registry"))
    chore_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return chore_id


def cancel_chore(chore_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE Chores SET Start = ?, End = ? WHERE ID = ?", (time.time(), -2, chore_id))
    conn.commit()
    conn.close()


def set_sailor_time_offset(sailor_name: str, time_offset: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE Sailors SET TimeOffset = ? WHERE Name = ?", (time_offset, sailor_name))
    conn.commit()
    conn.close()

# region .... for lieutenant


def assign_chore_sailor(chore_id: int, sailor_name: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE Chores SET Sailor = ? WHERE ID = ?",
                   (sailor_name, chore_id))
    conn.commit()
    conn.close()


def archive_chore(chore_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Chores WHERE ID = ?", (chore_id,))
    chore = cursor.fetchone()
    if chore:
        cursor.execute("""
        INSERT INTO Chores_archive (choreID, owner, RSailor, RService, configuration, Infos, Sailor, PID, Start, End)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (chore[0], chore[1], chore[2], chore[3], chore[4], chore[5], chore[6], chore[7], chore[8], chore[9]))
        cursor.execute("DELETE FROM Chores WHERE ID = ?", (chore_id,))
        conn.commit()
    conn.close()


def remove_chore(chore_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM Chores WHERE ID = ?", (chore_id,))
    conn.commit()
    conn.close()

# region .... for sailor


def set_chore_pid(chore_id: int, pid: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    start = time.time()
    cursor.execute("UPDATE Chores SET PID = ?, Start = ? WHERE ID = ?", (pid, start, chore_id))
    conn.commit()
    conn.close()


def set_chore_end(chore_id: int, pid: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    end = time.time()
    print('SET END', chore_id, end, pid)
    cursor.execute("UPDATE Chores SET End = ?, PID = ? WHERE ID = ?", (end, pid, chore_id))
    conn.commit()
    conn.close()


def get_chores_by_sailor_name(sailor_name: str):
    chores = get_chores()
    sailor_chores = [c for c in chores if c["Sailor"] == sailor_name]
    return sailor_chores

# region -------------------------------------------------------- SAILORS
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------


SAILOR_STATUS_DOWN = "DOWN"
SAILOR_STATUS_READY = "READY"
SAILOR_STATUS_WORKING = "WORKING"

MIN_SAILOR_ALIVE_ESTIMATION = 10

# region .... for getters


def get_sailor_status(last_seen: int, used_cpus: int) -> str:
    if int(time.time()) - last_seen > MIN_SAILOR_ALIVE_ESTIMATION:
        return SAILOR_STATUS_DOWN
    elif used_cpus is not None and used_cpus > 0:
        return SAILOR_STATUS_WORKING
    else:
        return SAILOR_STATUS_READY


def parse_sailor(sailor_row):
    ID = sailor_row[0]
    name = sailor_row[1]
    services = sailor_row[2]
    cpus = sailor_row[3]
    gpus = sailor_row[4]
    ram = sailor_row[5]
    lastSeen = sailor_row[6]
    used_cpus = sailor_row[7]
    used_gpus = sailor_row[8]
    time_offset = sailor_row[9]
    return {
        "ID": ID,
        "Name": name,
        "Services": services.split(','),
        "CPUS": int(cpus),
        "GPUS": int(gpus),
        "RAM": int(ram),
        "LastSeen": int(lastSeen),
        "UsedCPUS": int(used_cpus),
        "UsedGPUS": int(used_gpus),
        "TimeOffset": int(time_offset),
        "Status": get_sailor_status(lastSeen + time_offset, used_cpus)
    }


def get_sailors():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Sailors")
    sailors = cursor.fetchall()
    conn.close()
    sailors_json = [parse_sailor(s) for s in sailors]
    return sailors_json


def get_sailors_by_service(service: str, sailors=None):
    sailors = sailors if sailors is not None else get_sailors()
    filtered = [s for s in sailors if service in s["Services"]]
    return filtered


def get_sailor_available_cpus(sailor):
    cpus = sailor["CPUS"]
    used_cpus = sailor["UsedCPUS"]
    used_cpus = 0 if used_cpus is None else used_cpus
    return cpus - used_cpus


def get_sailor_available_gpus(sailor):
    gpus = sailor["GPUS"]
    used_gpus = sailor["UsedGPUS"]
    used_gpus = 0 if used_gpus is None else used_gpus
    return gpus - used_gpus


def get_sailor_by_name(sailor_name: str, sailors=None):
    sailors = get_sailors() if sailors is None else sailors
    for s in sailors:
        if s["Name"] == sailor_name:
            return s
    return None

# region .... for captain


def pre_register_sailor(name: str, services: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    timestamp = 0
    cursor.execute("""
    INSERT INTO Sailors (Name, Services, CPUS, GPUS, RAM, LastSeen, UsedCPUS, UsedGPUS)
    VALUES (?, ?, 0, 0, 0, ?, 0, 0)
    """, (name, services, timestamp))
    conn.commit()
    conn.close()


def remove_sailor(name: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM Sailors WHERE Name = ?", (name,))
    conn.commit()
    conn.close()

# region .... for sailors


def set_sailor_data(name: str, cpus: int, gpus: int, ram: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    timestamp = int(time.time())
    cursor.execute("""
    UPDATE Sailors
    SET CPUS = ?, GPUS = ?, RAM = ?, LastSeen = ?
    WHERE Name = ?
    """, (cpus, gpus, ram, timestamp, name))
    conn.commit()
    conn.close()


def set_sailor_use(name: str, used_cpus: int, used_gpus: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    timestamp = int(time.time())
    cursor.execute("""
    UPDATE Sailors
    SET UsedCPUS = ?, UsedGPUS = ?, LastSeen = ?
    WHERE Name = ?
    """, (used_cpus, used_gpus, timestamp, name))
    conn.commit()
    conn.close()

# region -------------------------------------------------------- MAIN
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------
# region --------------------------------------------------------


if not os.path.exists(DB_PATH_FILE):
    parser = argparse.ArgumentParser(description="Boat Chest Database Manager")
    parser.add_argument("--install-db", type=str, help="Install the database at the specified path")
    args = parser.parse_args()

    if args.install_db:
        db_path = args.install_db
        install_db(db_path)
        print('Database installed successfully.')
        exit(0)
