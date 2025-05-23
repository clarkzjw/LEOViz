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

def get_dish_data(status_cmd: GrpcCommand, diagnostics_cmd: Optional[GrpcCommand] = None) -> tuple[Optional[List[Any]], Optional[List[Any]]]:
    """Get dish status and location data for the current timestamp."""
    try:
        status_data = status_cmd.execute()
        if not status_data:
            return None, None

        dish_status = status_data.get("dishGetStatus")
        if not dish_status or "alignmentStats" not in dish_status:
            logger.warning("Missing dishGetStatus or alignmentStats in status data")
            return None, None

        current_time = time.time()
        status_row = DataProcessor.extract_status_fields(dish_status, current_time)
        location_row = None

        if diagnostics_cmd:
            diagnostics_data = diagnostics_cmd.execute()
            if diagnostics_data:
                location_row = DataProcessor.extract_location_fields(diagnostics_data, current_time)
            else:
                logger.error("Failed to get diagnostics data")

        return status_row, location_row
    except Exception as e:
        logger.error(f"Error getting dish data: {str(e)}")
        return None, None

def monitor_dish_state() -> None:
    """Collect dish status data over time."""
    name = "GRPC_DishState"
    logger.info(f"{name}, {threading.current_thread()}")

    # Generate filenames with current timestamp
    dt_string = date_time_string()
    date = ensure_data_directory(GRPC_DATA_DIR)
    status_filename = f"{GRPC_DATA_DIR}/{date}/GRPC_STATUS-{dt_string}.csv"
    location_filename = f"{GRPC_DATA_DIR}/{date}/GRPC_LOCATION-{dt_string}.csv" if config.MOBILE else None
    
    # Create GRPC command for status
    status_cmd = GrpcCommand("status", '{"get_status":{}}')
    location_cmd = GrpcCommand("diagnostics", '{"get_diagnostics":{}}') if config.MOBILE else None

    # Open CSV files for writing
    with open(status_filename, "w", newline="") as status_file:
        status_writer = csv.writer(status_file)
        DataProcessor.write_status_csv_header(status_writer)

        # Open location file if mobile installation
        location_file = None
        location_writer = None
        if config.MOBILE and location_filename:
            location_file = open(location_filename, "w", newline="")
            location_writer = csv.writer(location_file)
            DataProcessor.write_location_csv_header(location_writer)

        try:
            # Record start time for duration tracking
            start_time = time.time()
            
            # Collect data for specified duration
            while time.time() < start_time + DURATION_SECONDS:
                # Get status data
                status_row, location_row = get_dish_data(status_cmd, location_cmd)
                
                if status_row:
                    status_writer.writerow(status_row)
                    status_file.flush()

                if location_row and location_writer:
                    location_writer.writerow(location_row)
                    location_file.flush()
                
                time.sleep(0.5)

            logger.info(f"Dish status data saved to {status_filename}")
            if location_filename:
                logger.info(f"Location data saved to {location_filename}")

        finally:
            # Ensure location file is closed
            if location_file:
                location_file.close()

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
        context = starlink_grpc.ChannelContext(target=STARLINK_GRPC_ADDR_PORT)

        # Collect data for specified duration
        while time.time() < start + DURATION_SECONDS:
            try:
                # Reset obstruction map for new data collection
                starlink_grpc.reset_obstruction_map(context)
                logger.info("Resetting dish obstruction map")
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
        location_filename = f"{GRPC_DATA_DIR}/{date}/GRPC_LOCATION-{dt_string}.csv"

        if not os.path.exists(status_filename):
            logger.error(f"Status file not found: {status_filename}")
            return

        # Read status data
        df_status = pd.read_csv(status_filename)

        # Handle location data based on installation type
        df_location = None
        if config.MOBILE:
            if not os.path.exists(location_filename):
                logger.error(f"Location file not found: {location_filename}")
                return
            df_location = pd.read_csv(location_filename)
            if not all(col in df_location.columns for col in ['timestamp', 'lat', 'lon', 'alt']):
                logger.error("Missing required columns in location file for mobile installation")
                return

        # Estimate connected satellites
        merged_df = SatelliteProcessor.estimate_connected_satellites(
            dt_string, date, frame_type_int, df_status,
            timeslot_df.iloc[0]["timestamp"],
            timeslot_df.iloc[-1]["timestamp"],
        )

        if merged_df is not None and not merged_df.empty:
            # Save serving satellite data
            serving_satellite_file = f"{DATA_DIR}/serving_satellite_data-{dt_string}.csv"
            
            # Ensure the directory exists
            os.makedirs(os.path.dirname(serving_satellite_file), exist_ok=True)
            
            # Format the data
            merged_df['Timestamp'] = pd.to_datetime(merged_df['Timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S%z')
            
            # Write header if file doesn't exist
            if not os.path.exists(serving_satellite_file):
                with open(serving_satellite_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Timestamp', 'Y', 'X', 'Elevation', 'Azimuth', 'Connected_Satellite', 'Distance'])
            
            # Append data to CSV file
            merged_df.to_csv(serving_satellite_file, mode='a', header=False, index=False)
            logger.info(f"Saved serving satellite data to {serving_satellite_file}")
        else:
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
