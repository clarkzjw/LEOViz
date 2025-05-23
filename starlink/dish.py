# flake8: noqa: E501

import os
import csv
import sys
import json
import time
import logging
import subprocess
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

import numpy as np
import pandas as pd
from skyfield.api import load

import config
from config import DATA_DIR, STARLINK_GRPC_ADDR_PORT, DURATION_SECONDS, TLE_DATA_DIR
from util import date_time_string, ensure_data_directory, ensure_directory, get_timestamp_str, get_date_str
from obstruction import create_obstruction_map_video, process_obstruction_timeslot
from data_processor import DataProcessor
from satellite_matching_estimation import SatelliteProcessor
from grpc_command import GrpcCommand
from timeslot_manager import TimeslotManager
from location_provider import LocationProvider

# Add starlink-grpc-tools to Python path
sys.path.insert(0, str(Path("./starlink-grpc-tools").resolve()))
import starlink_grpc

logger = logging.getLogger(__name__)

# Constants for GRPC operations
GRPC_DATA_DIR = f"{DATA_DIR}/grpc"
GRPC_TIMEOUT = 10

def grpc_status_job() -> None:
    """Collect dish status data over time."""
    name = "GRPC_DishStatus"
    logger.info(f"{name}, {threading.current_thread()}")

    # Generate filename with current timestamp
    dt_string = date_time_string()
    date = ensure_data_directory(GRPC_DATA_DIR)
    status_filename = f"{GRPC_DATA_DIR}/{date}/GRPC_STATUS-{dt_string}.csv"

    # Open CSV file for writing
    with open(status_filename, "w", newline="") as status_file:
        status_writer = csv.writer(status_file)
        DataProcessor.write_status_csv_header(status_writer)

        grpc = GrpcCommand()
        
        try:
            # Record start time for duration tracking
            start_time = time.time()
            
            # Collect data for specified duration
            while time.time() < start_time + DURATION_SECONDS:
                # Get status data with current time
                current_time = time.time()
                status_row = grpc.status(current_time)
                if status_row:
                    status_writer.writerow(status_row)
                    status_file.flush()
                
                time.sleep(0.5)

            logger.info(f"Dish status data saved to {status_filename}")

        except Exception as e:
            logger.error(f"Error monitoring dish status: {str(e)}", exc_info=True)

def grpc_gps_diagnostics_job() -> None:
    """Collect GPS diagnostics data over time."""
    if not config.MOBILE:
        logger.info("Skipping GPS diagnostics collection - not in mobile mode")
        return

    name = "GRPC_GPSDiagnostics"
    logger.info(f"{name}, {threading.current_thread()}")

    # Generate filename with current timestamp
    dt_string = date_time_string()
    date = ensure_data_directory(GRPC_DATA_DIR)
    gps_diagnostics = f"{GRPC_DATA_DIR}/{date}/GRPC_LOCATION-{dt_string}.csv"

    # Open CSV file for writing
    with open(gps_diagnostics, "w", newline="") as gps_diagnostics_file:
        gps_diagnostics_writer = csv.writer(gps_diagnostics_file)
        DataProcessor.write_location_csv_header(gps_diagnostics_writer)

        grpc = GrpcCommand()
        
        try:
            # Record start time for duration tracking
            start_time = time.time()
            
            # Collect data for specified duration
            while time.time() < start_time + DURATION_SECONDS:
                # Get GPS diagnostics data with current time
                current_time = time.time()
                gps_diagnostics_row = grpc.gps_diagnostics(current_time)
                if gps_diagnostics_row:
                    gps_diagnostics_writer.writerow(gps_diagnostics_row)
                    gps_diagnostics_file.flush()
                
                time.sleep(0.5)

            logger.info(f"Location data saved to {gps_diagnostics}")

        except Exception as e:
            logger.error(f"Error monitoring GPS diagnostics: {str(e)}", exc_info=True)


def get_obstruction_map() -> None:
    """Collect and process obstruction map data."""
    name = "GRPC_GetObstructionMap"
    logger.info(f"{name}, {threading.current_thread()}")

    # Validate location requirements for static installation
    if not config.MOBILE and not all([config.LATITUDE, config.LONGITUDE, config.ALTITUDE]):
        logger.error("Static installation requires LATITUDE, LONGITUDE, and ALTITUDE in config")
        return

    # Generate filenames with current timestamp
    dt_string = date_time_string()
    date = ensure_data_directory(GRPC_DATA_DIR)
    filename = f"{GRPC_DATA_DIR}/{date}/obstruction_map-{dt_string}.parquet"
    obstruction_data_filename = f"{DATA_DIR}/obstruction-data-{dt_string}.csv"

    # Get frame type for obstruction map
    frame_type_int, _ = get_obstruction_map_frame_type()
    start = time.time()
    thread_pool = []

    # Open CSV file for writing obstruction data
    with open(obstruction_data_filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['timestamp', 'Y', 'X'])
        grpc = GrpcCommand()
        context = starlink_grpc.ChannelContext(target=config.STARLINK_GRPC_ADDR_PORT)
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

                # Reset obstruction map for new data collection
                grpc.reset_obstruction_map()
                timeslot_start = time.time()

                # Collect data for the duration of one timeslot
                timeslot_data = collect_timeslot_data(context, timeslot_start)
                if timeslot_data:
                    timeslot_df = pd.DataFrame(timeslot_data)
                    
                    # Start processing thread for the timeslot
                    processing_thread = threading.Thread(
                        target=process_obstruction_estimate_satellites_per_timeslot,
                        args=(timeslot_df, writer, csvfile, filename, dt_string, date, frame_type_int),
                    )
                    processing_thread.start()
                    thread_pool.append(processing_thread)

            except starlink_grpc.GrpcError as e:
                logger.error(f"Failed getting obstruction map data: {str(e)}")

        # Wait for all processing threads to complete
        for thread in thread_pool:
            thread.join()

    # Create video visualization of obstruction map
    create_obstruction_map_video(filename, dt_string, 5)

def collect_timeslot_data(context: Any, timeslot_start: float) -> Optional[Dict[str, List[Any]]]:
    """Collect obstruction data for a single timeslot."""
    obstruction_data_array = []
    timestamp_array = []

    while time.time() < timeslot_start + TimeslotManager.TIMESLOT_DURATION:
        # Get and process obstruction data
        obstruction_data = np.array(starlink_grpc.obstruction_map(context), dtype=int)
        obstruction_data[obstruction_data == -1] = 0
        obstruction_data = obstruction_data.flatten()

        # Store timestamp and data
        timestamp_array.append(time.time())
        obstruction_data_array.append(obstruction_data)
        time.sleep(0.5)

    if not timestamp_array:
        return None

    return {
        "timestamp": timestamp_array,
        "obstruction_map": obstruction_data_array,
    }

def process_obstruction_estimate_satellites_per_timeslot(timeslot_df: pd.DataFrame, writer: csv.writer,
                                                       csvfile: Any, filename: str, dt_string: str,
                                                       date: str, frame_type_int: int) -> None:
    """Process obstruction data and estimate satellites for a timeslot."""
    try:
        # Process obstruction data for the timeslot
        process_obstruction_timeslot(timeslot_df, writer)
        csvfile.flush()
        write_obstruction_map_parquet(filename, timeslot_df)

        # Get status and location data files
        status_filename = f"{GRPC_DATA_DIR}/{date}/GRPC_STATUS-{dt_string}.csv"
        gps_diagnostics_filename = f"{GRPC_DATA_DIR}/{date}/GRPC_LOCATION-{dt_string}.csv"

        if not os.path.exists(status_filename):
            logger.error(f"Status file not found: {status_filename}")
            return

        # Read status data
        df_status = pd.read_csv(status_filename)

        # Handle location data based on installation type
        gps_diagnostics_df = None
        if config.MOBILE:
            if not os.path.exists(gps_diagnostics_filename):
                logger.error(f"Location file not found: {gps_diagnostics_filename}")
                return
                
            gps_diagnostics_df = pd.read_csv(gps_diagnostics_filename)
            if not all(col in gps_diagnostics_df.columns for col in ['timestamp', 'lat', 'lon', 'alt']):
                logger.error("Missing required columns in location file for mobile installation")
                return

        # Estimate connected satellites
        merged_df = SatelliteProcessor.estimate_connected_satellites(
            dt_string, date, frame_type_int, df_status,
            timeslot_df.iloc[0]["timestamp"],
            timeslot_df.iloc[-1]["timestamp"],
        )

        if merged_df is None or merged_df.empty:
            logger.warning("No satellite data to save")

    except Exception as e:
        logger.error(f"Error in processing thread: {str(e)}", exc_info=True)

def write_obstruction_map_parquet(filename: str, timeslot_df: pd.DataFrame) -> None:
    """Write obstruction map data to parquet file."""
    # Check if file exists and append or create new
    if os.path.exists(filename):
        # Read existing data and combine with new data
        existing_df = pd.read_parquet(filename)
        combined_df = pd.concat([existing_df, timeslot_df], ignore_index=True)
        combined_df.to_parquet(filename, engine="pyarrow", compression="zstd")
    else:
        # Create new file with current data
        timeslot_df.to_parquet(filename, engine="pyarrow", compression="zstd")
    logger.info(f"Saved dish obstruction map to {filename}")

def get_obstruction_map_frame_type() -> Tuple[int, str]:
    """Get the obstruction map frame type."""
    # Create GRPC context
    context = starlink_grpc.ChannelContext(target=STARLINK_GRPC_ADDR_PORT)
    
    # Get obstruction map data
    map = starlink_grpc.get_obstruction_map(context)
    
    # Map frame type integer to string
    frame_type = {
        0: "UNKNOWN",
        1: "FRAME_EARTH",
        2: "FRAME_UT"
    }.get(map.map_reference_frame, "UNKNOWN")
    
    return map.map_reference_frame, frame_type

def wait_until_target_time(last_timeslot_second: int) -> int:
    """Wait until the next timeslot and return the next timeslot second."""
    now = datetime.now(timezone.utc)
    next_timeslot_second = None

    if last_timeslot_second == 12:
        next_timeslot_second = 27
    elif last_timeslot_second == 27:
        next_timeslot_second = 42
    elif last_timeslot_second == 42:
        next_timeslot_second = 57
    elif last_timeslot_second == 57:
        next_timeslot_second = 12
        # If we're moving to the next minute
        if now.second >= 57:
            now = now + timedelta(minutes=1)

    target_time = now.replace(microsecond=0).replace(second=next_timeslot_second)
    while datetime.now(timezone.utc) < target_time:
        time.sleep(0.1)

    return next_timeslot_second
