import sys
import json
import time
import logging
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import numpy as np

import config
from data_feature_extraction import DataFeatureExtraction
from timeslot_manager import TimeslotManager

sys.path.insert(0, str(Path("./starlink-grpc-tools").resolve()))
import starlink_grpc


logger = logging.getLogger(__name__)

GRPC_TIMEOUT = 10


class GrpcCommand:
    """Handles GRPC command execution and response parsing for Starlink dish communication.

    This class provides methods to interact with the Starlink dish through GRPC commands,
    including status checks, diagnostics, and obstruction map data collection.
    """

    def __init__(self):
        """Initialize GRPC commands and data extractor.

        Sets up the command strings for various GRPC operations and initializes
        the data extractor for processing responses.
        """
        self.status_cmd = [
            "grpcurl",
            "-plaintext",
            "-d",
            '{"get_status":{}}',
            config.STARLINK_GRPC_ADDR_PORT,
            "SpaceX.API.Device.Device/Handle",
        ]
        self.diagnostics_cmd = [
            "grpcurl",
            "-plaintext",
            "-d",
            '{"get_diagnostics":{}}',
            config.STARLINK_GRPC_ADDR_PORT,
            "SpaceX.API.Device.Device/Handle",
        ]
        self.reset_obstruction_cmd = [
            "grpcurl",
            "-plaintext",
            "-d",
            '{"dish_clear_obstruction_map":{}}',
            config.STARLINK_GRPC_ADDR_PORT,
            "SpaceX.API.Device.Device/Handle",
        ]
        self.data_extracter = DataFeatureExtraction()

    def reset_obstruction_map(self) -> None:
        """Reset the dish's obstruction map to clear previous measurements.

        Raises:
            Exception: If the reset command fails to execute successfully.
        """
        try:
            result = self.execute(self.reset_obstruction_cmd)
            if result is None:
                raise Exception("Failed to reset obstruction map")
            logger.info("Resetting dish obstruction map")
        except Exception as e:
            logger.error(f"Failed resetting obstruction map: {str(e)}")
            raise

    def execute(self, cmd: List[str]) -> Optional[Dict[str, Any]]:
        """Execute a grpcurl command and parse its JSON response.

        Args:
            cmd: List of command arguments to execute with grpcurl.

        Returns:
            Optional[Dict[str, Any]]: Parsed JSON response if successful, None otherwise.

        Note:
            Command execution is limited by GRPC_TIMEOUT seconds.
        """
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=GRPC_TIMEOUT)
            if result.returncode != 0:
                logger.error(f"Command failed with error: {result.stderr}")
                return None

            return json.loads(result.stdout)
        except subprocess.TimeoutExpired:
            logger.error(f"Command timed out after {GRPC_TIMEOUT} seconds")
            return None
        except json.JSONDecodeError:
            logger.error("Failed to parse command output as JSON")
            return None
        except Exception as e:
            logger.error(f"Error executing command: {str(e)}")
            return None

    def status(self, current_time: float) -> Optional[List[Any]]:
        """Get current dish status information.

        Args:
            current_time: Timestamp to associate with the status data.

        Returns:
            Optional[List[Any]]: List of status fields if successful, None otherwise.

        Note:
            Status includes signal strength, throughput, alignment, and attitude data.
        """
        try:
            status_data = self.execute(self.status_cmd)
            if not status_data:
                return None

            dish_status = status_data.get("dishGetStatus")
            if not dish_status or "alignmentStats" not in dish_status:
                logger.warning("Missing dishGetStatus or alignmentStats in status data")
                return None

            return self.data_extracter.extract_status_fields(dish_status, current_time)

        except Exception as e:
            logger.error(f"Error getting status data: {str(e)}")
            return None

    def gps_diagnostics(self, current_time: float) -> Optional[List[Any]]:
        """Get GPS diagnostics data for mobile installations.

        Args:
            current_time: Timestamp to associate with the GPS data.

        Returns:
            Optional[List[Any]]: List of GPS fields if successful, None otherwise.

        Note:
            This method is only active when config.MOBILE is True.
        """
        if not config.MOBILE:
            logger.info("Skipping GPS diagnostics - not in mobile mode")
            return None

        try:
            diagnostics_data = self.execute(self.diagnostics_cmd)
            if not diagnostics_data:
                logger.error("Failed to get diagnostics data")
                return None

            return self.data_extracter.extract_location_fields(diagnostics_data, current_time)

        except Exception as e:
            logger.error(f"Error getting GPS diagnostics data: {str(e)}")
            return None

    def get_obstruction_map_frame_type(self) -> Tuple[int, str]:
        """Get the reference frame type used by the obstruction map.

        Returns:
            Tuple[int, str]: A tuple containing:
                - int: Numeric frame type identifier
                - str: Human-readable frame type name

        Note:
            Frame types are:
            - 0: UNKNOWN
            - 1: FRAME_EARTH
            - 2: FRAME_UT
        """
        try:
            context = starlink_grpc.ChannelContext(target=config.STARLINK_GRPC_ADDR_PORT)
            map = starlink_grpc.get_obstruction_map(context)

            frame_type = {0: "UNKNOWN", 1: "FRAME_EARTH", 2: "FRAME_UT"}.get(map.map_reference_frame, "UNKNOWN")

            return map.map_reference_frame, frame_type
        except Exception as e:
            logger.error(f"Error getting obstruction map frame type: {str(e)}")
            return 0, "UNKNOWN"

    def get_obstruction_data(self) -> Optional[Tuple[float, np.ndarray]]:
        """Get a single obstruction map data point from the dish.

        Returns:
            Optional[Tuple[float, np.ndarray]]: A tuple containing:
                - float: Timestamp of the measurement
                - np.ndarray: Flattened obstruction map data

        Note:
            The obstruction map is a binary array where:
            - 0: No obstruction
            - 1: Obstruction detected
            - -1: Invalid measurement (converted to 0)
        """
        try:
            context = starlink_grpc.ChannelContext(target=config.STARLINK_GRPC_ADDR_PORT)
            obstruction_data = np.array(starlink_grpc.obstruction_map(context), dtype=int)
            obstruction_data[obstruction_data == -1] = 0
            obstruction_data = obstruction_data.flatten()

            return time.time(), obstruction_data
        except Exception as e:
            logger.error(f"Error getting obstruction data: {str(e)}")
            return None

    def collect_timeslot_data(self, timeslot_start: float) -> Optional[Dict[str, List[Any]]]:
        """Collect obstruction data for a single timeslot."""
        try:
            # Create GRPC context
            context = starlink_grpc.ChannelContext(target=config.STARLINK_GRPC_ADDR_PORT)

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
        except Exception as e:
            logger.error(f"Error collecting timeslot data: {str(e)}")
            return None
