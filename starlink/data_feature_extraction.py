import os
import csv
import time
import logging

from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime

import numpy as np
import pandas as pd

import config
from obstruction import ObstructionMap

logger = logging.getLogger(__name__)


class DataFeatureExtraction:
    """Extracts and processes features from Starlink dish data.

    This class handles the extraction and processing of various data features
    from Starlink dish data, including obstruction maps, status data, and
    location data. It provides methods for merging different data sources and
    processing observed positions.

    Attributes:
        obstruction_map (ObstructionMap): Instance for processing obstruction map data
    """

    def __init__(self):
        """Initialize the DataFeatureExtraction with required components."""
        self.status_columns = [
            "timestamp",
            "hardwareVersion",
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
            "quaternion_qScalar",
            "quaternion_qX",
            "quaternion_qY",
            "quaternion_qZ",
        ]
        self.location_columns = [
            "gps_time",
            "timestamp",
            "lat",
            "lon",
            "alt",
            "uncertainty_valid",
            "uncertainty",
        ]
        self.obstruction_map = ObstructionMap()

    def get_status_columns(self) -> List[str]:
        """Get the list of status columns."""
        return self.status_columns

    def get_location_columns(self) -> List[str]:
        """Get the list of location columns."""
        return self.location_columns

    def write_status_csv_header(self, csv_writer) -> None:
        """Write CSV header for status data."""
        csv_writer.writerow(self.status_columns)

    def write_location_csv_header(self, csv_writer) -> None:
        """Write CSV header for location data."""
        csv_writer.writerow(self.location_columns)

    def extract_status_fields(self, status: Dict[str, Any], current_time: Optional[float] = None) -> List[Any]:
        """Extract fields for dish status data."""
        alignment = status.get("alignmentStats", {})
        quaternion = status.get("ned2dishQuaternion", {})
        deviceInfo = status.get("deviceInfo", {})
        return [
            current_time if current_time is not None else time.time(),
            deviceInfo.get("hardwareVersion", ""),
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
            quaternion.get("qScalar", 0),
            quaternion.get("qX", 0),
            quaternion.get("qY", 0),
            quaternion.get("qZ", 0),
        ]

    def extract_location_fields(self, diagnostics: Dict[str, Any], current_time: Optional[float] = None) -> List[Any]:
        """Extract fields for location data from diagnostics."""
        location = diagnostics.get("dishGetDiagnostics", {}).get("location", {})
        gps_time = location.get("gpsTimeS", 0)
        return [
            gps_time,
            current_time if current_time is not None else time.time(),
            location.get("latitude", 0),
            location.get("longitude", 0),
            location.get("altitudeMeters", 0),
            location.get("uncertaintyMetersValid", False),
            location.get("uncertaintyMeters", 0),
        ]

    def pre_process_observed_data_by_frame_type(
        self,
        merged_df: pd.DataFrame,
        frame_type: int,
    ) -> pd.DataFrame:
        """
        Pre-process observed data by frame type to get the Elevation and Azimuth angle for connected satellites
        from the trajectory in the obstruction map
        """
        pixel_to_degrees = 80 / 62  # Conversion factor from pixel to degrees

        elevations = []
        azimuths = []

        for index, row in merged_df.iterrows():
            if frame_type == 1:  # FRAME_EARTH
                observer_x, observer_y = 62, 62  # Assume this is the observer's pixel location
                dx, dy = row["X"] - observer_x, (123 - row["Y"]) - observer_y
            elif frame_type == 2:  # FRAME_UT
                _tilt = row["tiltAngleDeg"]
                _rotation_az = row["boresightAzimuthDeg"]
                observer_x, observer_y = 62, 62 - (_tilt / (80 / 62))
                dx, dy = row["X"] - observer_x, row["Y"] - observer_y

            radius = np.sqrt(dx**2 + dy**2) * pixel_to_degrees
            azimuth = np.degrees(np.arctan2(dx, dy))

            if frame_type == 1:  # FRAME_EARTH
                azimuth = (azimuth + 360) % 360
            elif frame_type == 2:  # FRAME_UT
                azimuth = (azimuth + _rotation_az + 360) % 360
            # Normalize the azimuth to ensure it's within 0 to 360 degrees
            elevation = 90 - radius
            elevations.append(elevation)
            azimuths.append(azimuth)

        merged_df["Elevation"] = elevations
        merged_df["Azimuth"] = azimuths
        return merged_df

    def merge_obstruction_with_status_and_location(
        self,
        filename: str,
        frame_type: int,
        df_status: pd.DataFrame,
        df_location: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """Merge obstruction data with status and location data.

        Args:
            filename: Path to the obstruction data file
            frame_type: Reference frame type (1=FRAME_EARTH, 2=FRAME_UT)
            df_status: DataFrame containing dish status data
            df_location: Optional DataFrame containing location data for mobile installations

        Returns:
            pd.DataFrame: Merged DataFrame containing:
                - Timestamp
                - Obstruction map data
                - Status data
                - Location data (for mobile installations)

        Note:
            - For mobile installations, location data is required
            - Status data must contain timestamp column
            - Returns empty DataFrame if merge fails
        """
        try:
            # Read obstruction data
            df_obstruction = pd.read_csv(filename)
            df_obstruction["timestamp"] = pd.to_datetime(df_obstruction["timestamp"], format="%Y-%m-%d %H:%M:%S")
            df_obstruction = df_obstruction.set_index("timestamp").resample("1s").min().reset_index()

            # Convert status timestamps
            df_status["timestamp"] = pd.to_datetime(df_status["timestamp"], unit="s")
            df_status = df_status.drop(columns=["attitudeEstimationState"])
            df_status = df_status.set_index("timestamp").resample("1s").min().reset_index()

            # fill missing alignment stats
            alignment_cols = ["tiltAngleDeg", "boresightAzimuthDeg", "boresightElevationDeg"]
            df_status[alignment_cols] = (df_status[alignment_cols].ffill() + df_status[alignment_cols].bfill()) / 2

            # Merge obstruction and status data
            merged_df = pd.merge(
                df_obstruction,
                df_status,
                on="timestamp",
                how="inner",
            )

            # Add location data for mobile installations
            if config.MOBILE and df_location is not None:
                df_location["timestamp"] = pd.to_datetime(df_location["timestamp"], unit="s")
                df_location["timestamp"] = df_location["timestamp"].dt.tz_localize(None)
                df_location = df_location.set_index("timestamp").resample("1s").min().reset_index()

                # GPS data is collected every 0.5 second from the gRPC interface
                # In theory, there shouldn't be NaN values when resampling every 1 second
                # In certain scenarios, gRPC interface may respond slow, causing gaps in the collected GPS data
                # These gaps should be at most a few seconds, thus fill missing values by averaging
                # before and after NaN should be fine

                # Exclude timestamp column from arithmetic operations
                numeric_cols = df_location.select_dtypes(include=[np.number]).columns
                df_location[numeric_cols] = (df_location[numeric_cols].ffill() + df_location[numeric_cols].bfill()) / 2
                merged_df = pd.merge(
                    merged_df,
                    df_location,
                    on="timestamp",
                    how="inner",
                )
            else:
                # if in stationary mode, manually add lat, lon, alt columns with values from config
                merged_df["lat"] = config.LATITUDE
                merged_df["lon"] = config.LONGITUDE
                merged_df["alt"] = config.ALTITUDE

            # TODO:
            # only drop rows with NaN in X and Y columns (i.e., no obstruction data)
            # It is possible to fill missing obstruction data by averaging as well
            # but we need to handle timeslot boundaries, which is a bit tricky
            # a better solution is to ensure obstruction data is collected every second
            merged_df = merged_df[(merged_df["X"].notna()) & (merged_df["Y"].notna())].reset_index(drop=True)

            merged_df["timestamp"] = merged_df["timestamp"].dt.tz_localize("UTC")
            merged_df["X"] = merged_df["X"].astype(int)
            merged_df["Y"] = merged_df["Y"].astype(int)
            return merged_df

        except Exception as e:
            logger.error(f"Error merging data: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def process_observed_data(
        self,
        filename: str,
        timestamp: str,
        merged_data_file: str,
    ) -> Optional[List[Tuple[datetime, Tuple[float, float]]]]:
        """Process observed positions from obstruction data.

        Args:
            filename: Path to the obstruction data file
            timestamp: ISO format timestamp string
            merged_data_file: Path to the merged data file

        Returns:
            Optional[List[Tuple[datetime, Tuple[float, float]]]]: List of tuples containing:
                - datetime: Timestamp of the observation
                - Tuple[float, float]: (altitude, azimuth) in degrees

        Note:
            - Processes data in 15-second intervals
            - Returns None if processing fails
            - Requires valid obstruction map data
        """
        try:
            # Read obstruction data
            df_obstruction_merged = pd.read_csv(merged_data_file)
            # 2025-05-27 07:33:14+00:00
            df_obstruction_merged["timestamp"] = pd.to_datetime(
                df_obstruction_merged["timestamp"], format="%Y-%m-%d %H:%M:%S%z"
            ).dt.tz_localize(None)

            # Get data for the specified timestamp
            timestamp_dt = pd.to_datetime(timestamp).tz_localize(None)
            timeslot_df = df_obstruction_merged[
                (df_obstruction_merged["timestamp"] >= timestamp_dt)
                & (df_obstruction_merged["timestamp"] < timestamp_dt + pd.Timedelta(seconds=15))
            ]

            if timeslot_df.empty:
                logger.error(f"No data found for timestamp {timestamp}")
                return None

            # Process the timeslot
            observed_positions = []
            for _, row in timeslot_df.iterrows():
                timestamp_dt = pd.to_datetime(row["timestamp"])
                elevation = 90 - row["Elevation"]
                azimuth = row["Azimuth"] % 360
                observed_positions.append((timestamp_dt, (elevation, azimuth)))

            return observed_positions

        except Exception as e:
            logger.error(f"Error processing observed data: {str(e)}", exc_info=True)
            return None

    def process_obstruction_estimate_satellites_per_timeslot(
        self,
        timeslot_df: pd.DataFrame,
        writer: csv.writer,
        csvfile: Any,
        filename: str,
        dt_string: str,
        date: str,
    ) -> None:
        """Process obstruction data and estimate satellites for a timeslot."""
        try:
            # Import here to avoid circular dependency
            from satellite_matching_estimation import SatelliteProcessor

            # Process obstruction data for the timeslot
            self.obstruction_map.process_timeslot(timeslot_df, writer)
            csvfile.flush()
            self.obstruction_map.write_parquet(filename, timeslot_df)

            # Get status and location data files
            status_filename = f"{config.DATA_DIR}/grpc/{date}/GRPC_STATUS-{dt_string}.csv"
            gps_diagnostics_filename = f"{config.DATA_DIR}/grpc/{date}/GRPC_LOCATION-{dt_string}.csv"

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
                if not all(col in gps_diagnostics_df.columns for col in ["timestamp", "lat", "lon", "alt"]):
                    logger.error("Missing required columns in location file for mobile installation")
                    return

            # Estimate connected satellites
            satellite_processor = SatelliteProcessor()
            merged_df = satellite_processor.estimate_connected_satellites(
                dt_string,
                date,
                timeslot_df.iloc[0]["frame_type"],
                df_status,
                timeslot_df.iloc[0]["timestamp"],
                timeslot_df.iloc[-1]["timestamp"],
            )

            if merged_df is None or merged_df.empty:
                logger.warning("No satellite data to save")

        except Exception as e:
            logger.error(f"Error in processing thread: {str(e)}", exc_info=True)
