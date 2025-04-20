# flake8: noqa: E501

import logging
import subprocess
import threading

from config import (
    INTERVAL_MS,
    INTERVAL_SEC,
    DURATION,
    DURATION_SECONDS,
    IFCE,
    COUNT,
    STARLINK_DEFAULT_GW,
    LATENCY_DATA_DIR,
)
from util import date_time_string, ensure_data_directory

logger = logging.getLogger(__name__)


def icmp_ping() -> None:
    name = "ICMP_PING"
    logger.info("{}, {}".format(name, threading.current_thread()))

    FILENAME = "{}/{}/ping-{}-{}.txt".format(
        LATENCY_DATA_DIR,
        ensure_data_directory(LATENCY_DATA_DIR),
        INTERVAL_MS,
        date_time_string(),
    )

    cmd = [
        "ping",
        "-D",
        "-i",
        INTERVAL_SEC,
        "-c",
        str(COUNT),
        STARLINK_DEFAULT_GW,
    ]
    if IFCE != "":
        cmd += ["-I", IFCE]
    try:
        with open(FILENAME, "w") as outfile:
            subprocess.run(cmd, stdout=outfile, timeout=DURATION_SECONDS)
    except subprocess.TimeoutExpired:
        pass

    logger.info("Latency measurement saved to {}".format(FILENAME))
