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
from util import date_time_string, ensure_data_directory
from obstruction import create_obstruction_map_video, process_obstruction_timeslot
from data_processor import DataProcessor
from satellite_estimator import SatelliteEstimator
from grpc_command import GrpcCommand
from dish_data_handler import DishDataHandler
from timeslot_manager import TimeslotManager

# Add starlink-grpc-tools to Python path
sys.path.insert(0, str(Path("./starlink-grpc-tools").resolve()))
import starlink_grpc

logger = logging.getLogger(__name__)

# Constants for GRPC operations
GRPC_DATA_DIR = f"{DATA_DIR}/grpc"
GRPC_TIMEOUT = 10

def grpc_get_location() -> None:
    """Get and save dish location data."""
    name = "GRPC_GetLocation"
    logger.info(f"{name}, {threading.current_thread()}")

    # Generate filename with current timestamp
    filename = f"{GRPC_DATA_DIR}/{ensure_data_directory(GRPC_DATA_DIR)}/GetLocation-{date_time_string()}.txt"
    
    # Create and execute GRPC command to get location
    cmd = GrpcCommand("location", '{"get_location":{}}')
    cmd.save_to_file(filename)

def grpc_get_status() -> None:
    """Get and save dish status data."""
    name = "GRPC_GetStatus"
    logger.info(f"{name}, {threading.current_thread()}")

    filename = f"{GRPC_DATA_DIR}/{ensure_data_directory(GRPC_DATA_DIR)}/GetStatus-{date_time_string()}.txt"
    
    # Create and execute GRPC command to get status
    cmd = GrpcCommand("status", '{"get_status":{}}')
    cmd.save_to_file(filename)

def monitor_dish_state() -> None:
    """Collect SINR and location data over time."""
    name = "GRPC_phyRxBeamSnrAvg"
    logger.info(f"{name}, {threading.current_thread()}")

    filename = f"{GRPC_DATA_DIR}/{ensure_data_directory(GRPC_DATA_DIR)}/GRPC_STATUS-{date_time_string()}.csv"
    
    # Create GRPC commands for status and location (if mobile)
    status_cmd = GrpcCommand("status", '{"get_status":{}}')
    location_cmd = GrpcCommand("location", '{"get_location":{}}') if config.MOBILE else None

    # Open CSV file for writing
    with open(filename, "w", newline="") as outfile:
        csv_writer = csv.writer(outfile)
        DishDataHandler.write_csv_header(csv_writer, config.MOBILE)

        # Record start time for duration tracking
        start_time = time.time()
        
        # Collect data for specified duration
        while time.time() < start_time + DURATION_SECONDS:
            # Get SINR and location data
            row_data = DishDataHandler.get_sinr_data(status_cmd, location_cmd, config.MOBILE)
            
            if row_data:
                csv_writer.writerow(row_data)
                outfile.flush()
                time.sleep(0.5)

    logger.info(f"dish state data saved to {filename}")

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
        context = starlink_grpc.ChannelContext(target=STARLINK_GRPC_ADDR_PORT)
        last_timeslot_second = None

        # Collect data for specified duration
        while time.time() < start + DURATION_SECONDS:
            try:
                # Get next timeslot if not already tracking
                if last_timeslot_second is None:
                    last_timeslot_second, _ = TimeslotManager.get_next_timeslot()
                else:
                    last_timeslot_second = TimeslotManager.wait_until_target_time(last_timeslot_second)

                # Reset obstruction map for new data collection
                starlink_grpc.reset_obstruction_map(context)
                logger.info("Resetting dish obstruction map")
                timeslot_start = time.time()

                obstruction_data_array = []
                timestamp_array = []

                # Collect data for the duration of one timeslot
                while time.time() < timeslot_start + TimeslotManager.TIMESLOT_DURATION:
                    # Get and process obstruction data
                    obstruction_data = np.array(starlink_grpc.obstruction_map(context), dtype=int)
                    obstruction_data[obstruction_data == -1] = 0
                    obstruction_data = obstruction_data.flatten()

                    # Store timestamp and data
                    timestamp_array.append(time.time())
                    obstruction_data_array.append(obstruction_data)
                    time.sleep(0.5)

                timeslot_df = pd.DataFrame({
                    "timestamp": timestamp_array,
                    "frame_type": frame_type_int,
                    "obstruction_map": obstruction_data_array,
                })

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

def process_obstruction_estimate_satellites_per_timeslot(timeslot_df: pd.DataFrame, writer: csv.writer,
                                                       csvfile: Any, filename: str, dt_string: str,
                                                       date: str, frame_type_int: int) -> None:
    """Process obstruction data and estimate satellites for a timeslot."""
    try:
        # Process obstruction data for the timeslot
        process_obstruction_timeslot(timeslot_df, writer)
        csvfile.flush()
        write_obstruction_map_parquet(filename, timeslot_df)

        # Handle static installation case
        if not config.MOBILE and (config.LATITUDE and config.LONGITUDE and config.ALTITUDE):
            # get dish state data file
            sinr_filename = f"{GRPC_DATA_DIR}/{date}/GRPC_STATUS-{dt_string}.csv"
            if not os.path.exists(sinr_filename):
                logger.error(f"SINR file not found: {sinr_filename}")
                return
            df_sinr = pd.read_csv(sinr_filename)
            
            # Estimate connected satellites
            SatelliteEstimator.estimate_connected_satellites(
                dt_string, date, frame_type_int, df_sinr,
                timeslot_df.iloc[0]["timestamp"],
                timeslot_df.iloc[-1]["timestamp"],
            )
        # Handle mobile installation case
        elif config.MOBILE:
            # Get SINR data file
            sinr_filename = f"{GRPC_DATA_DIR}/{date}/GRPC_STATUS-{dt_string}.csv"
            if not os.path.exists(sinr_filename):
                logger.error(f"SINR file not found: {sinr_filename}")
                return
            df_sinr = pd.read_csv(sinr_filename)
            
            # Check for required location columns
            if all(col in df_sinr.columns for col in ['latitude', 'longitude', 'altitude']):
                # Estimate connected satellites
                SatelliteEstimator.estimate_connected_satellites(
                    dt_string, date, frame_type_int, df_sinr,
                    timeslot_df.iloc[0]["timestamp"],
                    timeslot_df.iloc[-1]["timestamp"],
                )
            else:
                logger.error("Missing location columns in SINR file for mobile installation")
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
