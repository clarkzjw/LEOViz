# flake8: noqa: E501

import time
import logging
import threading
import multiprocessing
from datetime import datetime, timezone
from pathlib import Path
from shutil import which
from skyfield.api import load
import os
import re
from typing import Optional, Tuple

from config import DATA_DIR, TLE_DATA_DIR, TLE_URL

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s"
)

# flake8: noqa: E501
import pytz
import pandas as pd
from skyfield.api import load

logger = logging.getLogger(__name__)

def get_latest_file(directory: str, pattern: str) -> Optional[str]:
    """Get the latest file matching the pattern in the directory."""
    try:
        files = [f for f in os.listdir(directory) if re.match(pattern, f)]
        if not files:
            return None
        return max(files, key=lambda x: os.path.getctime(os.path.join(directory, x)))
    except Exception as e:
        logger.error(f"Error getting latest file: {str(e)}", exc_info=True)
        return None

def parse_timestamp_from_filename(filename: str) -> Optional[datetime]:
    """Parse timestamp from filename."""
    try:
        match = re.search(r'(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})', filename)
        if match:
            return datetime.strptime(match.group(1), "%Y-%m-%d-%H-%M-%S")
        return None
    except Exception as e:
        logger.error(f"Error parsing timestamp from filename: {str(e)}", exc_info=True)
        return None

def get_timestamp_str() -> str:
    """Get current timestamp as string."""
    return datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

def get_date_str() -> str:
    """Get current date as string."""
    return datetime.now().strftime("%Y-%m-%d")

def ensure_directory(directory: str) -> None:
    """Ensure directory exists."""
    try:
        Path(directory).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"Error creating directory: {str(e)}", exc_info=True)

def get_file_info(filepath: str) -> Tuple[Optional[datetime], Optional[str]]:
    """Get timestamp and UUID from filepath."""
    try:
        filename = os.path.basename(filepath)
        timestamp = parse_timestamp_from_filename(filename)
        uuid_match = re.search(r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', filename)
        uuid = uuid_match.group(1) if uuid_match else None
        return timestamp, uuid
    except Exception as e:
        logger.error(f"Error getting file info: {str(e)}", exc_info=True)
        return None, None

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
