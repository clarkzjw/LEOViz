import csv
import sys
import time
import logging
import threading

from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd

import config

from util import date_time_string, ensure_data_directory
from config import DATA_DIR, DURATION_SECONDS
from obstruction import ObstructionMap
from grpc_command import GrpcCommand
from timeslot_manager import TimeslotManager
from data_feature_extraction import DataFeatureExtraction


# Add starlink-grpc-tools to Python path
sys.path.insert(0, str(Path("./starlink-grpc-tools").resolve()))
import starlink_grpc

logger = logging.getLogger(__name__)


class JobManager:
    """Manages and executes various Starlink data collection jobs.

    This class coordinates the collection and processing of different types of data
    from the Starlink dish, including status information, GPS diagnostics, and
    obstruction maps.
    """

    def __init__(self):
        """Initialize the JobManager with necessary components.

        Sets up data directories, timeouts, and initializes required processors
        and extractors.
        """
        self.grpc_data_dir = f"{DATA_DIR}/grpc"
        self.grpc_timeout = 10
        self.grpc = GrpcCommand()
        self.data_extracter = DataFeatureExtraction()
        self.obstruction_map = ObstructionMap()

    def grpc_status_job(self) -> None:
        """Collect dish status data over time.

        This job continuously collects status information from the dish for a
        specified duration, including signal strength, throughput, and alignment
        data. Data is saved to a CSV file with timestamps.

        Note:
            The job runs for DURATION_SECONDS and collects data every 0.5 seconds.
        """
        name = "GRPC_DishStatus"
        logger.info(f"{name}, {threading.current_thread()}")

        # Generate filename with current timestamp
        dt_string = date_time_string()
        date = ensure_data_directory(self.grpc_data_dir)
        status_filename = f"{self.grpc_data_dir}/{date}/GRPC_STATUS-{dt_string}.csv"

        # Open CSV file for writing
        with open(status_filename, "w", newline="") as status_file:
            status_writer = csv.writer(status_file)
            self.data_extracter.write_status_csv_header(status_writer)

            try:
                # Record start time for duration tracking
                start_time = time.time()

                # Collect data for specified duration
                while time.time() < start_time + DURATION_SECONDS:
                    # Get status data with current time
                    current_time = time.time()
                    status_row = self.grpc.status(current_time)
                    if status_row:
                        status_writer.writerow(status_row)
                        status_file.flush()

                    time.sleep(0.5)

                logger.info(f"Dish status data saved to {status_filename}")

            except Exception as e:
                logger.error(f"Error monitoring dish status: {str(e)}", exc_info=True)

    def grpc_gps_diagnostics_job(self) -> None:
        """Collect GPS diagnostics data over time for mobile installations.

        This job continuously collects GPS location data from the dish for a
        specified duration. Data is saved to a CSV file with timestamps.

        Note:
            - Only runs when config.MOBILE is True
            - The job runs for DURATION_SECONDS and collects data every 0.5 seconds
        """
        if not config.MOBILE:
            logger.info("Skipping GPS diagnostics collection - not in mobile mode")
            return

        name = "GRPC_GPSDiagnostics"
        logger.info(f"{name}, {threading.current_thread()}")

        # Generate filename with current timestamp
        dt_string = date_time_string()
        date = ensure_data_directory(self.grpc_data_dir)
        gps_diagnostics = f"{self.grpc_data_dir}/{date}/GRPC_LOCATION-{dt_string}.csv"

        # Open CSV file for writing
        with open(gps_diagnostics, "w", newline="") as gps_diagnostics_file:
            gps_diagnostics_writer = csv.writer(gps_diagnostics_file)
            self.data_extracter.write_location_csv_header(gps_diagnostics_writer)

            try:
                # Record start time for duration tracking
                start_time = time.time()

                # Collect data for specified duration
                while time.time() < start_time + DURATION_SECONDS:
                    # Get GPS diagnostics data with current time
                    current_time = time.time()
                    gps_diagnostics_row = self.grpc.gps_diagnostics(current_time)
                    if gps_diagnostics_row:
                        gps_diagnostics_writer.writerow(gps_diagnostics_row)
                        gps_diagnostics_file.flush()

                    time.sleep(0.5)

                logger.info(f"Location data saved to {gps_diagnostics}")

            except Exception as e:
                logger.error(f"Error monitoring GPS diagnostics: {str(e)}", exc_info=True)

    def _collect_timeslot_data(self, timeslot_start: float) -> Optional[Dict[str, List[Any]]]:
        """Collect obstruction data for a single timeslot.

        Args:
            timeslot_start: Timestamp when the timeslot began.

        Returns:
            Optional[Dict[str, List[Any]]]: Dictionary containing:
                - timestamp: List of timestamps for each measurement
                - obstruction_map: List of obstruction map arrays

        Note:
            - Collects data for TimeslotManager.TIMESLOT_DURATION seconds
            - Measurements are taken every 0.5 seconds
            - Returns None if no data was collected
        """
        try:
            obstruction_data_array = []
            timestamp_array = []

            while time.time() < timeslot_start + TimeslotManager.TIMESLOT_DURATION:
                # Get and process obstruction data
                result = self.grpc.get_obstruction_data()
                if result:
                    timestamp, obstruction_data = result
                    timestamp_array.append(timestamp)
                    obstruction_data_array.append(obstruction_data)
                time.sleep(0.5)

            if not timestamp_array:
                return None

            return {
                "timestamp": timestamp_array,
                "obstruction_map": obstruction_data_array,
            }
        except Exception as e:
            logger.error(f"Error collecting timeslot data: {str(e)}")
            return None

    def get_obstruction_map_job(self) -> None:
        """Collect and process obstruction map data over time.

        This job continuously collects obstruction map data from the dish for a
        specified duration. Data is processed in timeslots and saved to both
        CSV and parquet files. A video visualization is also created.

        Note:
            - For static installations, requires LATITUDE, LONGITUDE, and ALTITUDE in config
            - The job runs for DURATION_SECONDS
            - Data is collected in 15-second timeslots
            - Each timeslot is processed in a separate thread
            - Creates both raw data files and a video visualization
        """
        name = "GRPC_GetObstructionMap"
        logger.info(f"{name}, {threading.current_thread()}")

        # Validate location requirements for static installation
        if not config.MOBILE and not all([config.LATITUDE, config.LONGITUDE, config.ALTITUDE]):
            logger.error("Static installation requires LATITUDE, LONGITUDE, and ALTITUDE in config")
            return

        # Generate filenames with current timestamp
        dt_string = date_time_string()
        date = ensure_data_directory(self.grpc_data_dir)
        filename = f"{self.grpc_data_dir}/{date}/obstruction_map-{dt_string}.parquet"
        obstruction_data_filename = f"{DATA_DIR}/obstruction-data-{dt_string}.csv"

        # Get frame type for obstruction map
        frame_type_int, _ = self.grpc.get_obstruction_map_frame_type()
        start = time.time()
        thread_pool = []

        # Open CSV file for writing obstruction data
        with open(obstruction_data_filename, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["timestamp", "Y", "X"])
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
                            start_time = now.replace(microsecond=0).replace(second=12) + timedelta(minutes=1)
                            last_timeslot_second = 12
                        elif now.second >= 0 and now.second < 12:
                            start_time = now.replace(microsecond=0).replace(second=12)
                            last_timeslot_second = 12

                        while datetime.now(timezone.utc) < start_time:
                            time.sleep(0.1)
                    else:
                        last_timeslot_second = TimeslotManager.wait_until_target_time(last_timeslot_second)

                    # Reset obstruction map for new data collection
                    self.grpc.reset_obstruction_map()
                    timeslot_start = time.time()

                    # Collect data for the duration of one timeslot
                    timeslot_data = self._collect_timeslot_data(timeslot_start)
                    if timeslot_data:
                        timeslot_df = pd.DataFrame(timeslot_data)
                        # although this is a bit redundant, as frame_type most likely change during a short term measurement
                        timeslot_df["frame_type"] = frame_type_int

                        # Start processing thread for the timeslot
                        processing_thread = threading.Thread(
                            target=self.data_extracter.process_obstruction_estimate_satellites_per_timeslot,
                            args=(
                                timeslot_df,
                                writer,
                                csvfile,
                                filename,
                                dt_string,
                                date,
                            ),
                        )
                        processing_thread.start()
                        thread_pool.append(processing_thread)

                except starlink_grpc.GrpcError as e:
                    logger.error(f"Failed getting obstruction map data: {str(e)}")

            # Wait for all processing threads to complete
            for thread in thread_pool:
                thread.join()

        # Create video visualization of obstruction map
        self.obstruction_map.create_video(filename, dt_string, 5)
