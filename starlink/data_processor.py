import os
import logging
import pandas as pd
import numpy as np
import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any

import config
from location_provider import LocationProvider
from grpc_command import GrpcCommand

logger = logging.getLogger(__name__)

class DataProcessor:
    """Processes and converts data for analysis."""

    @staticmethod
    def get_status_columns() -> List[str]:
        """Get columns for dish status data."""
        return [
            "timestamp", "sinr", "popPingLatencyMs", "downlinkThroughputBps",
            "uplinkThroughputBps", "tiltAngleDeg", "boresightAzimuthDeg",
            "boresightElevationDeg", "attitudeEstimationState",
            "attitudeUncertaintyDeg", "desiredBoresightAzimuthDeg",
            "desiredBoresightElevationDeg", "quaternion_qScalar", "quaternion_qX",
            "quaternion_qY", "quaternion_qZ"
        ]

    @staticmethod
    def get_location_columns() -> List[str]:
        """Get columns for location data."""
        return [
            "gps_time", "timestamp", "lat", "lon", "alt",
            "uncertainty_valid", "uncertainty"
        ]

    @staticmethod
    def write_status_csv_header(csv_writer) -> None:
        """Write CSV header for status data."""
        csv_writer.writerow(DataProcessor.get_status_columns())

    @staticmethod
    def write_location_csv_header(csv_writer) -> None:
        """Write CSV header for location data."""
        csv_writer.writerow(DataProcessor.get_location_columns())

    @staticmethod
    def extract_status_fields(status: Dict[str, Any], current_time: Optional[float] = None) -> List[Any]:
        """Extract fields for dish status data."""
        alignment = status.get("alignmentStats", {})
        quaternion = status.get("ned2dishQuaternion", {})
        return [
            current_time if current_time is not None else time.time(),
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
            quaternion.get("qZ", 0)
        ]

    @staticmethod
    def extract_location_fields(diagnostics: Dict[str, Any], current_time: Optional[float] = None) -> List[Any]:
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
            location.get("uncertaintyMeters", 0)
        ]

    @staticmethod
    def merge_obstruction_with_status_and_location(obstruction_filepath: str, frame_type: int, df_status: pd.DataFrame,
                                df_location: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Pre-process observed data with dish status and location information."""
        try:
            # Read CSV with headers and skip the header row
            df_obstruction = pd.read_csv(obstruction_filepath, names=['timestamp', 'Y', 'X'], skiprows=1)
            
            # Convert timestamp to datetime with UTC timezone
            df_obstruction['timestamp'] = pd.to_datetime(df_obstruction['timestamp'], utc=True)

            # Add dish status data with UTC timezone
            df_status['timestamp'] = pd.to_datetime(df_status['timestamp'], unit='s', utc=True)

            # Add location data for mobile installations with UTC timezone
            if config.MOBILE and df_location is not None:
                df_location['timestamp'] = pd.to_datetime(df_location['timestamp'], unit='s', utc=True)
            
            logger.info("Loaded the obstruction data, status data and the location data")

            # Match timestamps and add dish data
            for idx, point in df_obstruction.iterrows():
                # Get closest status data - but never go forward in time
                status_diffs = abs(df_status['timestamp'] - point['timestamp'])
                status_idx = status_diffs.idxmin() if status_diffs.idxmin() < idx else idx
                _tilt = df_status['tiltAngleDeg'].iloc[status_idx]
                _rotation_az = df_status['boresightAzimuthDeg'].iloc[status_idx]
                _rotation_el = df_status['boresightElevationDeg'].iloc[status_idx]

                # Add location data for mobile installations
                if config.MOBILE and df_location is not None:
                    location_diffs = abs(df_location['timestamp'] - point['timestamp'])
                    location_idx = location_diffs.idxmin()
                    _lat = df_location['lat'].iloc[location_idx]
                    _lon = df_location['lon'].iloc[location_idx]
                    _alt = df_location['alt'].iloc[location_idx]
                else:
                    # Use static location from config
                    _lat = config.LATITUDE
                    _lon = config.LONGITUDE
                    _alt = config.ALTITUDE

                # Add data to row
                df_obstruction.at[idx, 'Tilt'] = _tilt
                df_obstruction.at[idx, 'RotationAz'] = _rotation_az
                df_obstruction.at[idx, 'RotationEl'] = _rotation_el
                df_obstruction.at[idx, 'Latitude'] = _lat
                df_obstruction.at[idx, 'Longitude'] = _lon
                df_obstruction.at[idx, 'Altitude'] = _alt

            return df_obstruction

        except Exception as e:
            logger.error(f"Error pre-processing observed data: {str(e)}", exc_info=True)
            return pd.DataFrame()

    @staticmethod
    def process_observed_data(filename: str, start_time: str, 
                            merged_data_file: str) -> Optional[List[Tuple[datetime, Tuple[float, float]]]]:
        """Process observed data for a specific time interval."""
        data = pd.read_csv(filename, sep=",", header=None, names=["Timestamp", "Y", "X"])
        data["Timestamp"] = pd.to_datetime(data["Timestamp"], utc=True)
        
        interval_start_time = pd.to_datetime(start_time, utc=True)
        interval_end_time = interval_start_time + pd.Timedelta(seconds=14)
        
        filtered_data = data[
            (data["Timestamp"] >= interval_start_time) &
            (data["Timestamp"] < interval_end_time)
        ]
        
        if filtered_data.empty:
            logger.warning("No data found in the specified interval.")
            return None

        merged_data = pd.read_csv(merged_data_file, parse_dates=["Timestamp"])
        merged_data["Timestamp"] = pd.to_datetime(merged_data["Timestamp"], utc=True)
        
        merged_filtered_data = merged_data[
            (merged_data["Timestamp"] >= interval_start_time) &
            (merged_data["Timestamp"] < interval_end_time)
        ]

        if len(merged_filtered_data) < 3:
            logger.warning("Not enough data points in merged_filtered_data.")
            return None

        timestamps = [
            merged_filtered_data.iloc[0]["Timestamp"],
            merged_filtered_data.iloc[len(merged_filtered_data) // 2]["Timestamp"],
            merged_filtered_data.iloc[-1]["Timestamp"]
        ]
        
        # Add logging to show positions per timestamp
        positions = []
        for ts in timestamps:
            matching_rows = merged_filtered_data[merged_filtered_data["Timestamp"] == ts]
            logger.info(f"Timestamp {ts}: found {len(matching_rows)} positions")
            for _, row in matching_rows.iterrows():
                positions.append((ts, (90 - row["Elevation"], row["Azimuth"])))
        
        logger.info(f"Total positions found: {len(positions)}")
        return positions 