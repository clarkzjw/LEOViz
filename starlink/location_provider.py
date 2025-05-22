import logging
from datetime import datetime
from typing import Optional, Any

import pandas as pd
from skyfield.api import wgs84

import config

logger = logging.getLogger(__name__)

class LocationProvider:
    """Handles location data for both static and mobile installations."""
    
    @staticmethod
    def get_static_location() -> Any:
        """Get observer location from config for static installations."""
        return wgs84.latlon(
            latitude_degrees=config.LATITUDE,
            longitude_degrees=config.LONGITUDE,
            elevation_m=config.ALTITUDE
        )

    @staticmethod
    def get_mobile_location_at_time(df_sinr: pd.DataFrame, timestamp: Optional[datetime]) -> Optional[Any]:
        """Get observer location from df_sinr for mobile installations at a specific timestamp."""
        try:
            if not pd.api.types.is_datetime64_any_dtype(df_sinr['timestamp']):
                df_sinr['timestamp'] = pd.to_datetime(df_sinr['timestamp'], unit='s', utc=True)

            if timestamp is None:
                middle_idx = len(df_sinr) // 2
                middle_time = df_sinr['timestamp'].iloc[middle_idx]
                logger.info(f"Using median time: {middle_time}")
                return wgs84.latlon(
                    latitude_degrees=df_sinr['latitude'].iloc[middle_idx],
                    longitude_degrees=df_sinr['longitude'].iloc[middle_idx],
                    elevation_m=df_sinr['altitude'].iloc[middle_idx]
                )

            if isinstance(timestamp, (int, float)):
                timestamp = pd.to_datetime(timestamp, unit='s', utc=True)
            else:
                timestamp = pd.to_datetime(timestamp, utc=True)

            time_diffs = abs(df_sinr['timestamp'] - timestamp)
            closest_idx = time_diffs.idxmin()
            closest_time = df_sinr['timestamp'].iloc[closest_idx]
            time_diff_seconds = time_diffs.iloc[closest_idx].total_seconds()

            if time_diff_seconds > 60:
                logger.warning(
                    f"Large time gap: closest available time to {timestamp} is {closest_time} "
                    f"(diff: {time_diff_seconds:.2f}s)"
                )

            return wgs84.latlon(
                latitude_degrees=df_sinr['latitude'].iloc[closest_idx],
                longitude_degrees=df_sinr['longitude'].iloc[closest_idx],
                elevation_m=df_sinr['altitude'].iloc[closest_idx]
            )

        except (KeyError, AttributeError) as e:
            logger.error(f"Location data not found in df_sinr for timestamp {timestamp}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting mobile location for timestamp {timestamp}: {str(e)}", exc_info=True)
            return None

    @staticmethod
    def get_observer_location(df_sinr: Optional[pd.DataFrame] = None, 
                            timestamp: Optional[datetime] = None) -> Optional[Any]:
        """Get observer location based on installation type and timestamp."""
        if config.MOBILE:
            if df_sinr is None:
                logger.error("df_sinr is required for mobile installations")
                return None

            required_cols = ['latitude', 'longitude', 'altitude', 'timestamp']
            if not all(col in df_sinr.columns for col in required_cols):
                missing_cols = [col for col in required_cols if col not in df_sinr.columns]
                logger.error(f"Missing required columns in df_sinr: {missing_cols}")
                return None

            return LocationProvider.get_mobile_location_at_time(df_sinr, timestamp)
        
        return LocationProvider.get_static_location() 