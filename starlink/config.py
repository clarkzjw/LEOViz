# flake8: noqa: E501
import os
import re
from pathlib import Path
from datetime import timedelta


LATITUDE = None
LONGITUDE = None
ALTITUDE = None

IFCE = os.getenv("IFCE", "")

STARLINK_GRPC_ADDR_PORT = os.getenv("STARLINK_GRPC_ADDR_PORT", "192.168.100.1:9200")
STARLINK_DEFAULT_GW = os.getenv("STARLINK_DEFAULT_GW", "100.64.0.1")

DATA_DIR = os.getenv("DATA_DIR", "data")
TLE_DATA_DIR = Path(DATA_DIR).joinpath("TLE")
LATENCY_DATA_DIR = Path(DATA_DIR).joinpath("latency")

TLE_URL = "https://celestrak.org/NORAD/elements/gp.php?GROUP=starlink&FORMAT=tle"

INTERVAL_MS = os.getenv("INTERVAL", "10ms")
DURATION = os.getenv("DURATION", "60m")

TIMEDELTA_REGEX = (
    r"((?P<days>-?\d+)d)?" r"((?P<hours>-?\d+)h)?" r"((?P<minutes>-?\d+)m)?"
)
TIMEDELTA_PATTERN = re.compile(TIMEDELTA_REGEX, re.IGNORECASE)


def parse_delta(delta):
    """Parses a human readable timedelta (3d5h19m) into a datetime.timedelta.
    Delta includes:
    * Xd days
    * Xh hours
    * Xm minutes
    Values can be negative following timedelta's rules. Eg: -5h-30m
    """
    match = TIMEDELTA_PATTERN.match(delta)
    if match:
        parts = {k: int(v) for k, v in match.groupdict().items() if v}
        return timedelta(**parts)


DURATION_SECONDS = parse_delta(DURATION).seconds
COUNT = int(DURATION_SECONDS / (int(INTERVAL_MS[:-2]) / 1000.0))
INTERVAL_SEC = str(float(INTERVAL_MS[:-2]) / 1000.0)


def print_config():
    print("Starlink gRPC address: {}".format(STARLINK_GRPC_ADDR_PORT))
    print("Starlink default gateway: {}".format(STARLINK_DEFAULT_GW))
    print("Measurement interval: {}".format(INTERVAL_MS))
    print("Measurement duration: {}".format(DURATION))
