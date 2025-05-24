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

logger = logging.getLogger(__name__)

class DataFeatureExtraction:
    """Processes and converts data for analysis."""
    def __init__(self):
        self.status_columns = [
            "timestamp", "sinr", "popPingLatencyMs", "downlinkThroughputBps",
            "uplinkThroughputBps", "tiltAngleDeg", "boresightAzimuthDeg",
            "boresightElevationDeg", "attitudeEstimationState",
            "attitudeUncertaintyDeg", "desiredBoresightAzimuthDeg",
            "desiredBoresightElevationDeg", "quaternion_qScalar", "quaternion_qX",
            "quaternion_qY", "quaternion_qZ"
        ]
        self.location_columns = [
            "gps_time", "timestamp", "lat", "lon", "alt",
            "uncertainty_valid", "uncertainty"
        ]
    
    def get_status_columns(self) -> List[str]:
        """Get the list of status columns."""
        return self.status_columns

    def get_location_columns(self) -> List[str]:
        """Get the list of location columns."""
        return self.location_columns

    def write_status_csv_header(self, csv_writer) -> None:
        """Write CSV header for status data."""
        csv_writer.writerow(self.status_columns)

    def write_location_csv_header(self  , csv_writer) -> None:
        """Write CSV header for location data."""
        csv_writer.writerow(self.location_columns)

    def extract_status_fields(self, status: Dict[str, Any], current_time: Optional[float] = None) -> List[Any]:
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
            location.get("uncertaintyMeters", 0)
        ]

    def merge_obstruction_with_status_and_location(self, obstruction_filepath: str, frame_type: int, df_status: pd.DataFrame,
                                df_location: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Pre-process observed data with dish status and location information."""
        try:
            # Read CSV with headers and skip the header row
            df_obstruction = pd.read_csv(obstruction_filepath, names=['timestamp', 'Y', 'X'], skiprows=1)
            
            # Convert timestamp to datetime with UTC timezone (already in datetime string format)
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

                # Add data to row
                df_obstruction.at[idx, 'Tilt'] = _tilt
                df_obstruction.at[idx, 'RotationAz'] = _rotation_az
                df_obstruction.at[idx, 'RotationEl'] = _rotation_el

                # Only add location data if in mobile mode
                if config.MOBILE and df_location is not None:
                    location_diffs = abs(df_location['timestamp'] - point['timestamp'])
                    location_idx = location_diffs.idxmin()
                    df_obstruction.at[idx, 'Latitude'] = df_location['lat'].iloc[location_idx]
                    df_obstruction.at[idx, 'Longitude'] = df_location['lon'].iloc[location_idx]
                    df_obstruction.at[idx, 'Altitude'] = df_location['alt'].iloc[location_idx]

            return df_obstruction

        except Exception as e:
            logger.error(f"Error merging obstruction data with status and location data: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def process_observed_data(self, obstruction_filename: str, start_time: str, 
                            merged_data_file: str) -> Optional[List[Tuple[datetime, Tuple[float, float]]]]:
        """Process observed data for a specific time interval."""
        # Read the original obstruction data - skip the header row
        obstruction_data = pd.read_csv(obstruction_filename, sep=",", header=None, names=["Timestamp", "Y", "X"], skiprows=1)
        obstruction_data["Timestamp"] = pd.to_datetime(obstruction_data["Timestamp"], utc=True)
        
        interval_start_time = pd.to_datetime(start_time, utc=True)
        interval_end_time = interval_start_time + pd.Timedelta(seconds=14)
        
        filtered_data = obstruction_data[
            (obstruction_data["Timestamp"] >= interval_start_time) &
            (obstruction_data["Timestamp"] < interval_end_time)
        ]
        
        if filtered_data.empty:
            logger.warning("No data found in the specified interval.")
            return None

        # Read the merged data
        #timestamp, Tilt, RotationAz, RotationEl, Latitude?, Longitude?, Altitude?
        merged_data = pd.read_csv(merged_data_file)
        merged_data["timestamp"] = pd.to_datetime(merged_data["timestamp"], utc=True)
        
        merged_filtered_data = merged_data[
            (merged_data["timestamp"] >= interval_start_time) &
            (merged_data["timestamp"] < interval_end_time)
        ]

        if len(merged_filtered_data) < 3:
            logger.warning("Not enough data points in merged_filtered_data.")
            return None

        # Get exactly 3 positions: start, middle, and end
        start_idx = 0
        middle_idx = len(merged_filtered_data) // 2
        end_idx = len(merged_filtered_data) - 1

        positions = []
        for idx in [start_idx, middle_idx, end_idx]:
            row = merged_filtered_data.iloc[idx]
            positions.append((row["timestamp"], (90 - row["Y"], row["X"])))
            logger.info(f"Timestamp {row['timestamp']}: position ({90 - row['Y']}, {row['X']})")

        return positions 