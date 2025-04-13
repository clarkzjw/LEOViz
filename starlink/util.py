# flake8: noqa: E501

import time
import logging
import threading
import multiprocessing
from datetime import datetime, timezone
from pathlib import Path
from shutil import which
from skyfield.api import load


from config import DATA_DIR, TLE_DATA_DIR, TLE_URL

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s"
)

# flake8: noqa: E501
import re
from datetime import datetime

import pytz
import pandas as pd
from skyfield.api import load


def load_ping(filename):
    with open(filename, "r") as f:
        rtt_list = []
        timestamp_list = []
        for line in f.readlines():
            match = re.search(
                r"\[(\d+\.\d+)\].*icmp_seq=(\d+).*time=(\d+(\.\d+)?)", line
            )
            if match:
                # timestamp = datetime.fromtimestamp(float(match.group(1)), tz=pytz.utc)
                timestamp = float(match.group(1))
                rtt = float(match.group(3))
                timestamp_list.append(timestamp)
                rtt_list.append(rtt)

    return pd.DataFrame(
        {
            "timestamp": timestamp_list,
            "rtt": rtt_list,
        }
    )


def load_tle_from_file(filename):
    return load.tle_file(str(filename))


def load_connected_satellites(filename):
    df = pd.read_csv(filename)
    return df


def date_time_string() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M-%S")


def ensure_directory(name: str):
    return Path(name).mkdir(parents=True, exist_ok=True)


def ensure_data_directory(directory: str) -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ensure_directory(str(Path(directory).joinpath(today)))
    return today


def test_command(command: str) -> bool:
    return which(command) is not None


def failed(e: str) -> None:
    with open("{}/failed.txt".format(DATA_DIR), "a+") as f:
        f.write("{}: {}\n".format(time.time(), e))


def run(func):
    job = multiprocessing.Process(target=func)
    job.start()


def load_tle():
    global satellites
    directory = Path(TLE_DATA_DIR).joinpath(ensure_data_directory(TLE_DATA_DIR))
    satellites = load.tle_file(
        TLE_URL, True, "{}/starlink-tle-{}.txt".format(directory, date_time_string())
    )
    print("Loaded {} Starlink TLE satellites".format(len(satellites)))
