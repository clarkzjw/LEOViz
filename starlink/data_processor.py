import logging
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional

logger = logging.getLogger(__name__)

class DataProcessor:
    """Handles data processing and conversion."""
    
    @staticmethod
    def pre_process_observed_data(filename: str, frame_type: int, df_sinr: pd.DataFrame) -> pd.DataFrame:
        """Pre-process observed data and convert to positions."""
        data = pd.read_csv(filename, sep=",", header=None, names=["Timestamp", "Y", "X"])
        data["Timestamp"] = pd.to_datetime(data["Timestamp"], utc=True)
        df_sinr["timestamp"] = pd.to_datetime(df_sinr["timestamp"], unit='s', utc=True)

        frame_type_str = {
            1: "FRAME_EARTH",
            2: "FRAME_UT"
        }.get(frame_type, "UNKNOWN")

        pixel_to_degrees = 80 / 62
        positions = []

        for _, point in data.iterrows():
            closest_idx = (df_sinr["timestamp"] - point["Timestamp"]).abs().idxmin()
            _tilt = df_sinr["tiltAngleDeg"].iloc[closest_idx]
            _rotation_az = df_sinr["boresightAzimuthDeg"].iloc[closest_idx]

            observer_x, observer_y = 62, 62
            if frame_type_str == "FRAME_UT":
                observer_y = 62 - (_tilt / (80 / 62))

            dx = point["X"] - observer_x
            dy = (123 - point["Y"] if frame_type_str == "FRAME_EARTH" else point["Y"]) - observer_y
            
            radius = np.sqrt(dx**2 + dy**2) * pixel_to_degrees
            azimuth = np.degrees(np.arctan2(dx, dy))
            
            if frame_type_str == "FRAME_EARTH":
                azimuth = (azimuth + 360) % 360
            elif frame_type_str == "FRAME_UT":
                azimuth = (azimuth + _rotation_az + 360) % 360

            elevation = 90 - radius
            positions.append((point["Timestamp"], point["Y"], point["X"], elevation, azimuth))

        return pd.DataFrame(
            positions, 
            columns=["Timestamp", "Y", "X", "Elevation", "Azimuth"]
        )

    @staticmethod
    def convert_observed(dir_path: str, filename: str, frame_type: int, 
                        df_sinr: pd.DataFrame) -> pd.DataFrame:
        """Convert observed data to processed format."""
        observed_positions = DataProcessor.pre_process_observed_data(
            Path(dir_path).joinpath(filename), frame_type, df_sinr
        )
        
        if not observed_positions.empty:
            output_path = Path(dir_path).joinpath(f"processed_{filename}")
            observed_positions.to_csv(output_path, index=False)
        else:
            logger.warning("No valid observed data found.")

        return observed_positions

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