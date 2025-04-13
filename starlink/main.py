# flake8: noqa: E501
import time
import logging
import argparse
import schedule

import config
from latency import icmp_ping
from dish import grpc_get_status, get_sinr, get_obstruction_map
from util import run, load_tle
from config import print_config


logger = logging.getLogger(__name__)


schedule.every(1).hours.at(":00").do(run, icmp_ping).tag("Latency")
schedule.every(1).hours.at(":00").do(run, grpc_get_status).tag("gRPC")
schedule.every(1).hours.at(":00").do(run, get_obstruction_map).tag("gRPC")
schedule.every(1).hours.at(":00").do(run, get_sinr).tag("gRPC")
schedule.every(6).hours.at(":00").do(run, load_tle).tag("TLE")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LEOViz | Starlink metrics collection")

    parser.add_argument("--run-once", action="store_true", help="Run once and exit")
    parser.add_argument("--lat", type=float, required=False, help="Dish latitude")
    parser.add_argument("--lon", type=float, required=False, help="Dish longitude")
    parser.add_argument("--alt", type=float, required=False, help="Dish altitude")
    args = parser.parse_args()

    print_config()

    if args.lat and args.lon and args.alt:
        config.LATITUDE = args.lat
        config.LONGITUDE = args.lon
        config.ALTITUDE = args.alt
    else:
        logger.warning(
            "Latitude, Longitude and Altitude not provided. Won't estimate connected satellites."
        )

    if args.run_once:
        schedule.run_all()
    else:
        for job in schedule.get_jobs("Latency"):
            logger.info("[Latency]: {}".format(job.next_run))
        for job in schedule.get_jobs("TLE"):
            logger.info("[TLE]: {}".format(job.next_run))
        for job in schedule.get_jobs("gRPC"):
            logger.info("[gRPC]: {}".format(job.next_run))

        while True:
            schedule.run_pending()
            time.sleep(0.5)
