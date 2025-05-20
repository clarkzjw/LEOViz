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

from datetime import datetime, timezone, timedelta
from pathlib import Path
from config import DATA_DIR, STARLINK_GRPC_ADDR_PORT, DURATION_SECONDS, TLE_DATA_DIR
from util import date_time_string, ensure_data_directory
from obstruction import create_obstruction_map_video, process_obstruction_timeslot

import numpy as np
import pandas as pd
from skyfield.api import load

logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path("./starlink-grpc-tools").resolve()))
import starlink_grpc

GRPC_DATA_DIR = "{}/grpc".format(DATA_DIR)
GRPC_TIMEOUT = 10

def grpc_get_location() -> None:
    name = "GRPC_GetLocation"
    logger.info("{}, {}".format(name, threading.current_thread()))

    FILENAME = "{}/{}/GetLocation-{}.txt".format(
        GRPC_DATA_DIR, ensure_data_directory(GRPC_DATA_DIR), date_time_string()
    )

    # grpcurl -plaintext -d {\"get_location\":{}} 192.168.100.1:9200 SpaceX.API.Device.Device/Handle
    cmd = [
        "grpcurl",
        "-plaintext",
        "-d",
        '{"get_location":{}}',
        STARLINK_GRPC_ADDR_PORT,
        "SpaceX.API.Device.Device/Handle",
    ]
    try:
        with open(FILENAME, "w") as outfile:
            subprocess.run(cmd, stdout=outfile, timeout=GRPC_TIMEOUT)
    except subprocess.TimeoutExpired:
        pass

    logger.info("Saved gRPC dish location to {}".format(FILENAME))
    
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

    logger.info("Saved gRPC dish status to {}".format(FILENAME))

def write_csv_header(csv_writer, mobile: bool) -> None:
    base_columns = [
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
    mobile_columns = [
        "latitude",
        "longitude",
        "altitude",
        "qScalar",
        "qX",
        "qY",
        "qZ",
    ]
    header = base_columns + (mobile_columns if mobile else [])
    csv_writer.writerow(header)


def extract_status_fields(status: dict) -> list:
    alignment = status.get("alignmentStats", {})
    return [
        time.time(),
        status.get("phyRxBeamSnrAvg", 0),
        status.get("popPingLatencyMs", 0),
        status.get("downlinkThroughputBps", 0),
        status.get("uplinkThroughputBps", 0),
        alignment.get("tiltAngleDeg", 0),
        alignment.get("boresightAzimuthDeg", 0),
        alignment.get("boresightElevationDeg", 0),
        alignment.get("attitudeEstimationState", ""),
        alignment.get("attitudeUncertaintyDeg", 0),
        alignment.get("desiredBoresightAzimuthDeg", 0),
        alignment.get("desiredBoresightElevationDeg", 0),
    ]


def extract_location_fields(location_data: dict, status: dict) -> list:
    lla = location_data.get("getLocation", {}).get("lla", {})
    quaternion = status.get("ned2dishQuaternion", {})
    return [
        lla.get("lat", 0),
        lla.get("lon", 0),
        lla.get("alt", 0),
        quaternion.get("qScalar", 0),
        quaternion.get("qX", 0),
        quaternion.get("qY", 0),
        quaternion.get("qZ", 0),
    ]


def get_sinr() -> None:
    name = "GRPC_phyRxBeamSnrAvg"
    logger.info(f"{name}, {threading.current_thread()}")

    FILENAME = "{}/{}/GRPC_STATUS-{}.csv".format(
        GRPC_DATA_DIR, ensure_data_directory(GRPC_DATA_DIR), date_time_string()
    )

    status_cmd = [
        "grpcurl",
        "-plaintext",
        "-d",
        '{"get_status":{}}',
        STARLINK_GRPC_ADDR_PORT,
        "SpaceX.API.Device.Device/Handle",
    ]
    location_cmd = [
        "grpcurl",
        "-plaintext",
        "-d",
        '{"get_location":{}}',
        STARLINK_GRPC_ADDR_PORT,
        "SpaceX.API.Device.Device/Handle",
    ]

    with open(FILENAME, "w", newline="") as outfile:
        csv_writer = csv.writer(outfile)
        write_csv_header(csv_writer, config.MOBILE)

        start_time = time.time()
        while time.time() < start_time + DURATION_SECONDS:
            try:
                status_output = subprocess.check_output(status_cmd, timeout=GRPC_TIMEOUT)
                status_data = json.loads(status_output.decode("utf-8"))
                dish_status = status_data.get("dishGetStatus")

                if not dish_status or "alignmentStats" not in dish_status:
                    logger.warning("Missing dishGetStatus or alignmentStats in status data")
                    time.sleep(0.5)
                    continue

                row_data = extract_status_fields(dish_status)

                if config.MOBILE:
                    location_output = subprocess.check_output(location_cmd, timeout=GRPC_TIMEOUT)
                    location_data = json.loads(location_output.decode("utf-8"))
                    row_data.extend(extract_location_fields(location_data, dish_status))

                csv_writer.writerow(row_data)
                outfile.flush()
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Error in get_sinr: {str(e)}")
                time.sleep(0.5)

    logger.info(f"SNR measurement saved to {FILENAME}")

def wait_until_target_time(last_timeslot_second):
    while True:
        current_second = datetime.now(timezone.utc).second
        if current_second >= 12 and current_second < 27 and last_timeslot_second != 12:
            last_timeslot_second = 12
            break
        elif (
            current_second >= 27 and current_second < 42 and last_timeslot_second != 27
        ):
            last_timeslot_second = 27
            break
        elif (
            current_second >= 42 and current_second < 57 and last_timeslot_second != 42
        ):
            last_timeslot_second = 42
            break
        elif (
            current_second >= 57 and current_second < 60 and last_timeslot_second != 57
        ):
            last_timeslot_second = 57
            break
        elif current_second >= 0 and current_second < 12 and last_timeslot_second != 57:
            last_timeslot_second = 57
            break
        time.sleep(0.1)
    logger.info("Current timeslot starts at second: {}".format(last_timeslot_second))
    return last_timeslot_second


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


def process_obstruction_estimate_satellites_per_timeslot(
    timeslot_df, writer, csvfile, filename, dt_string, date, frame_type_int
):
    logger.info("Processing obstruction map for the past timeslot")
    try:
        process_obstruction_timeslot(timeslot_df, writer)
        csvfile.flush()
        write_obstruction_map_parquet(filename, timeslot_df)

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
                timeslot_df.iloc[0]["timestamp"],
                timeslot_df.iloc[-1]["timestamp"],
            )
    except Exception as e:
        logger.error(f"Error in processing thread: {str(e)}")


def get_obstruction_map():
    name = "GRPC_GetObstructionMap"
    logger.info("{}, {}".format(name, threading.current_thread()))

    dt_string = date_time_string()
    date = ensure_data_directory(GRPC_DATA_DIR)
    FILENAME = f"{GRPC_DATA_DIR}/{date}/obstruction_map-{dt_string}.parquet"
    OBSTRUCTION_DATA_FILENAME = f"{DATA_DIR}/obstruction-data-{dt_string}.csv"
    TIMESLOT_DURATION = 14

    frame_type_int, frame_type_str = get_obstruction_map_frame_type()

    start = time.time()
    thread_pool = []

    with open(OBSTRUCTION_DATA_FILENAME, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        context = starlink_grpc.ChannelContext(target=STARLINK_GRPC_ADDR_PORT)
        last_timeslot_second = None

        while time.time() < start + DURATION_SECONDS:
            try:
                if last_timeslot_second is None:
                    now = datetime.now(timezone.utc)
                    if now.second >= 12 and now.second < 27:
                        start_time = now.replace(microsecond=0).replace(second=27)
                        last_timeslot_second = 27
                    elif now.second >= 27 and now.second < 42:
                        start_time = now.replace(microsecond=0).replace(second=42)
                        last_timeslot_second = 42
                    elif now.second >= 42 and now.second < 57:
                        start_time = now.replace(microsecond=0).replace(second=57)
                        last_timeslot_second = 57
                    elif now.second >= 57 and now.second < 60:
                        start_time = now.replace(microsecond=0).replace(
                            second=12
                        ) + timedelta(minutes=1)
                        last_timeslot_second = 12
                    elif now.second >= 0 and now.second < 12:
                        start_time = now.replace(microsecond=0).replace(second=12)
                        last_timeslot_second = 12

                    while datetime.now(timezone.utc) < start_time:
                        time.sleep(0.1)
                else:
                    last_timeslot_second = wait_until_target_time(last_timeslot_second)

                starlink_grpc.reset_obstruction_map(context)
                logger.info("Resetting dish obstruction map")
                timeslot_start = time.time()

                obstruction_data_array = []
                timestamp_array = []

                while time.time() < timeslot_start + TIMESLOT_DURATION:
                    obstruction_data = np.array(
                        starlink_grpc.obstruction_map(context), dtype=int
                    )
                    obstruction_data[obstruction_data == -1] = 0
                    obstruction_data = obstruction_data.flatten()

                    timestamp_array.append(time.time())
                    obstruction_data_array.append(obstruction_data)
                    time.sleep(0.5)

                # obstruction data for the current 15-second timeslot
                timeslot_df = pd.DataFrame(
                    {
                        "timestamp": timestamp_array,
                        "frame_type": frame_type_int,
                        "obstruction_map": obstruction_data_array,
                    }
                )

                processing_thread = threading.Thread(
                    target=process_obstruction_estimate_satellites_per_timeslot,
                    args=(
                        timeslot_df,
                        writer,
                        csvfile,
                        FILENAME,
                        dt_string,
                        date,
                        frame_type_int,
                    ),
                )
                # processing_thread.daemon = True
                processing_thread.start()
                thread_pool.append(processing_thread)

            except starlink_grpc.GrpcError as e:
                logger.error("Failed getting obstruction map data:", str(e))

        for thread in thread_pool:
            thread.join()

    create_obstruction_map_video(FILENAME, dt_string, 5)


def write_obstruction_map_parquet(FILENAME, timeslot_df):
    if os.path.exists(FILENAME):
        existing_df = pd.read_parquet(FILENAME)
        combined_df = pd.concat([existing_df, timeslot_df], ignore_index=True)
        combined_df.to_parquet(
            FILENAME,
            engine="pyarrow",
            compression="zstd",
        )
    else:
        timeslot_df.to_parquet(
            FILENAME,
            engine="pyarrow",
            compression="zstd",
        )
    logger.info("Saved dish obstruction map to {}".format(FILENAME))


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
        df_sinr,
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

    logger.info(
        f"Connected satellites estimation for timeslot {start_ts} saved to '{DATA_DIR}/serving_satellite_data-{uuid}.csv'"
    )
