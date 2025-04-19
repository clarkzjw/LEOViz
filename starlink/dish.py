# flake8: noqa: E501

import os
import csv
import sys
import json
import time
import logging
import subprocess
import threading

import config

from satellites import convert_observed, process_intervals

from datetime import datetime, timezone
from pathlib import Path
from config import DATA_DIR, STARLINK_GRPC_ADDR_PORT, DURATION_SECONDS, TLE_DATA_DIR
from util import date_time_string, ensure_data_directory
from obstruction import process_obstruction_maps, create_obstruction_map_video

import numpy as np
import pandas as pd
from skyfield.api import load, wgs84, utc

logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path("./starlink-grpc-tools").resolve()))
import starlink_grpc

GRPC_DATA_DIR = "{}/grpc".format(DATA_DIR)
GRPC_TIMEOUT = 10


def grpc_get_status() -> None:
    name = "GRPC_GetStatus"
    logger.info("{}, {}".format(name, threading.current_thread()))

    FILENAME = "{}/{}/GetStatus-{}.txt".format(
        GRPC_DATA_DIR, ensure_data_directory(GRPC_DATA_DIR), date_time_string()
    )

    # grpcurl -plaintext -d {\"get_status\":{}} 192.168.100.1:9200 SpaceX.API.Device.Device/Handle
    cmd = [
        "grpcurl",
        "-plaintext",
        "-d",
        '{"get_status":{}}',
        STARLINK_GRPC_ADDR_PORT,
        "SpaceX.API.Device.Device/Handle",
    ]
    try:
        with open(FILENAME, "w") as outfile:
            subprocess.run(cmd, stdout=outfile, timeout=GRPC_TIMEOUT)
    except subprocess.TimeoutExpired:
        pass

    logger.info("save grpc status to {}".format(FILENAME))


def get_sinr():
    name = "GRPC_phyRxBeamSnrAvg"
    logger.info("{}, {}".format(name, threading.current_thread()))

    FILENAME = "{}/{}/GRPC_STATUS-{}.csv".format(
        GRPC_DATA_DIR, ensure_data_directory(GRPC_DATA_DIR), date_time_string()
    )

    cmd = [
        "grpcurl",
        "-plaintext",
        "-d",
        '{"get_status":{}}',
        STARLINK_GRPC_ADDR_PORT,
        "SpaceX.API.Device.Device/Handle",
    ]
    try:
        with open(FILENAME, "w") as outfile:
            start = time.time()
            csv_writer = csv.writer(outfile)
            csv_writer.writerow(
                [
                    "timestamp",
                    "sinr",
                    "popPingLatencyMs",
                    "downlinkThroughputBps",
                    "uplinkThroughputBps",
                    "tiltAngleDeg",
                    "boresightAzimuthDeg",
                    "boresightElevationDeg",
                    "attitudeEstimationState",
                    "attitudeUncertaintyDeg",
                    "desiredBoresightAzimuthDeg",
                    "desiredBoresightElevationDeg",
                ]
            )
            while time.time() < start + DURATION_SECONDS:
                output = subprocess.check_output(cmd, timeout=GRPC_TIMEOUT)
                data = json.loads(output.decode("utf-8"))
                if (
                    data["dishGetStatus"] is not None
                    # Starlink may have just rollbacked the firmware
                    # from 2025.04.08.cr53207 to 2025.03.28.mr52463.2
                    # thus removing phyRxBeamSnrAvg again
                    # and "phyRxBeamSnrAvg" in data["dishGetStatus"]
                    and "alignmentStats" in data["dishGetStatus"]
                ):
                    status = data["dishGetStatus"]
                    sinr = status.get("phyRxBeamSnrAvg", 0)
                    alignment = status["alignmentStats"]
                    popPingLatencyMs = status.get("popPingLatencyMs", 0)
                    dlThroughputBps = status.get("downlinkThroughputBps", 0)
                    upThroughputBps = status.get("uplinkThroughputBps", 0)
                    csv_writer.writerow(
                        [
                            time.time(),
                            sinr,
                            popPingLatencyMs,
                            dlThroughputBps,
                            upThroughputBps,
                            alignment.get("tiltAngleDeg", 0),
                            alignment.get("boresightAzimuthDeg", 0),
                            alignment.get("boresightElevationDeg", 0),
                            alignment.get("attitudeEstimationState", ""),
                            alignment.get("attitudeUncertaintyDeg", 0),
                            alignment.get("desiredBoresightAzimuthDeg", 0),
                            alignment.get("desiredBoresightElevationDeg", 0),
                        ]
                    )
                    outfile.flush()
                    time.sleep(0.5)
    except subprocess.TimeoutExpired:
        pass

    logger.info("save sinr measurement to {}".format(FILENAME))


def wait_until_target_time():
    target_seconds = {12, 27, 42, 57}
    while True:
        current_second = datetime.now(timezone.utc).second
        if current_second in target_seconds:
            break
        time.sleep(0.5)


def get_obstruction_map_frame_type():
    context = starlink_grpc.ChannelContext(target=STARLINK_GRPC_ADDR_PORT)
    map = starlink_grpc.get_obstruction_map(context)
    if map.map_reference_frame == 0:
        frame_type = "UNKNOWN"
    elif map.map_reference_frame == 1:
        frame_type = "FRAME_EARTH"
    elif map.map_reference_frame == 2:
        frame_type = "FRAME_UT"
    return map.map_reference_frame, frame_type


def get_obstruction_map():
    name = "GRPC_GetObstructionMap"
    logger.info("{}, {}".format(name, threading.current_thread()))

    dt_string = date_time_string()
    date = ensure_data_directory(GRPC_DATA_DIR)
    FILENAME = "{}/{}/obstruction_map-{}.parquet".format(GRPC_DATA_DIR, date, dt_string)
    TIMESLOT_DURATION = 14

    frame_type_int, frame_type_str = get_obstruction_map_frame_type()

    start = time.time()
    obstruction_data_array = []
    timestamp_array = []
    while time.time() < start + DURATION_SECONDS:
        try:
            context = starlink_grpc.ChannelContext(target=STARLINK_GRPC_ADDR_PORT)
            wait_until_target_time()

            starlink_grpc.reset_obstruction_map(context)
            logger.info("clearing obstruction map data")
            timeslot_start = time.time()

            while time.time() < timeslot_start + TIMESLOT_DURATION:
                obstruction_data = np.array(
                    starlink_grpc.obstruction_map(context), dtype=int
                )
                obstruction_data[obstruction_data == -1] = 0
                obstruction_data = obstruction_data.flatten()

                timestamp_array.append(time.time())
                obstruction_data_array.append(obstruction_data)
                time.sleep(1)

        except starlink_grpc.GrpcError as e:
            logger.error("Failed getting obstruction map data:", str(e))

    df = pd.DataFrame(
        {
            "timestamp": timestamp_array,
            "frame_type": frame_type_int,
            "obstruction_map": obstruction_data_array,
        }
    )
    pd.DataFrame(df).to_parquet(
        FILENAME,
        engine="pyarrow",
        compression="zstd",
    )
    logger.info("saved obstruction map data to {}".format(FILENAME))

    process_obstruction_maps(df, dt_string)
    create_obstruction_map_video(df, dt_string, 5)

    print("start: ", df.iloc[0]["timestamp"])
    print("end: ", df.iloc[-1]["timestamp"])

    if config.LATITUDE and config.LONGITUDE and config.ALTITUDE:
        SINR_FILENAME = "{}/{}/GRPC_STATUS-{}.csv".format(
            GRPC_DATA_DIR, date, dt_string
        )
        df_sinr = pd.read_csv(SINR_FILENAME)
        estimate_connected_satellites(
            dt_string,
            date,
            frame_type_int,
            df_sinr,
            df.iloc[0]["timestamp"],
            df.iloc[-1]["timestamp"],
        )


def estimate_connected_satellites(uuid, date, frame_type, df_sinr, start, end):
    start_ts = datetime.fromtimestamp(start, tz=timezone.utc)
    end_ts = datetime.fromtimestamp(end, tz=timezone.utc)

    convert_observed(DATA_DIR, f"obstruction-data-{uuid}.csv", frame_type, df_sinr)

    filename = f"{DATA_DIR}/obstruction-data-{uuid}.csv"
    merged_data_file = f"{DATA_DIR}/processed_obstruction-data-{uuid}.csv"

    satellites = load.tle_file(
        "{}/{}/starlink-tle-{}.txt".format(TLE_DATA_DIR, date, uuid)
    )

    result_df = process_intervals(
        filename,
        start_ts.year,
        start_ts.month,
        start_ts.day,
        start_ts.hour,
        start_ts.minute,
        start_ts.second,
        end_ts.year,
        end_ts.month,
        end_ts.day,
        end_ts.hour,
        end_ts.minute,
        end_ts.second,
        merged_data_file,
        satellites,
        frame_type,
    )

    merged_data_df = pd.read_csv(merged_data_file, parse_dates=["Timestamp"])

    if os.path.exists(f"{DATA_DIR}/serving_satellite_data-{uuid}.csv"):
        existing_df = pd.read_csv(
            f"{DATA_DIR}/serving_satellite_data-{uuid}.csv", parse_dates=["Timestamp"]
        )
    else:
        existing_df = pd.DataFrame()

    merged_df = pd.merge(merged_data_df, result_df, on="Timestamp", how="inner")

    updated_df = pd.concat([existing_df, merged_df]).drop_duplicates(
        subset=["Timestamp"], keep="last"
    )

    updated_df.to_csv(f"{DATA_DIR}/serving_satellite_data-{uuid}.csv", index=False)

    print(f"Updated data saved to '{DATA_DIR}/serving_satellite_data-{uuid}.csv'")
