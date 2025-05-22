import os
import logging
from datetime import datetime, timezone
from typing import List, Any, Optional

import pandas as pd
from skyfield.api import load

import config
from data_processor import DataProcessor
from satellites import process_intervals

logger = logging.getLogger(__name__)

class SatelliteEstimator:
    """Handles satellite estimation and data processing."""
    
    @staticmethod
    def estimate_connected_satellites(uuid: str, date: str, frame_type: int, df_sinr: pd.DataFrame, 
                                    start: float, end: float) -> None:
        """Estimate connected satellites for a given time period."""
        try:
            start_ts = datetime.fromtimestamp(start, tz=timezone.utc)
            end_ts = datetime.fromtimestamp(end, tz=timezone.utc)

            # Process obstruction data
            obstruction_file = f"obstruction-data-{uuid}.csv"
            DataProcessor.convert_observed(config.DATA_DIR, obstruction_file, frame_type, df_sinr)

            # Load TLE data
            tle_file = f"{config.TLE_DATA_DIR}/{date}/starlink-tle-{uuid}.txt"
            if not os.path.exists(tle_file):
                logger.error(f"TLE file not found: {tle_file}")
                return

            satellites = load.tle_file(tle_file)

            # Process data files
            filename = f"{config.DATA_DIR}/obstruction-data-{uuid}.csv"
            merged_data_file = f"{config.DATA_DIR}/processed_obstruction-data-{uuid}.csv"
            
            result_df = process_intervals(
                filename, start_ts.year, start_ts.month, start_ts.day,
                start_ts.hour, start_ts.minute, start_ts.second,
                end_ts.year, end_ts.month, end_ts.day,
                end_ts.hour, end_ts.minute, end_ts.second,
                merged_data_file, satellites, frame_type,
                df_sinr if config.MOBILE else None,
            )

            if result_df is None or result_df.empty:
                logger.error("No results returned from process_intervals")
                return

            # Update serving satellite data
            serving_satellite_file = f"{config.DATA_DIR}/serving_satellite_data-{uuid}.csv"
            merged_data_df = pd.read_csv(merged_data_file, parse_dates=["Timestamp"])
            merged_df = pd.merge(merged_data_df, result_df, on="Timestamp", how="inner")

            if os.path.exists(serving_satellite_file):
                existing_df = pd.read_csv(serving_satellite_file, parse_dates=["Timestamp"])
                updated_df = pd.concat([existing_df, merged_df]).drop_duplicates(
                    subset=["Timestamp"], keep="last"
                )
            else:
                updated_df = merged_df

            updated_df.to_csv(serving_satellite_file, index=False)
            logger.info(f"Satellite estimation complete for {start_ts} to {end_ts}")

        except Exception as e:
            logger.error(f"Error in estimate_connected_satellites: {str(e)}", exc_info=True) 