import logging
from typing import Optional, Any, Tuple
import pandas as pd
from skyfield.api import load, wgs84, utc

import config

logger = logging.getLogger(__name__)

class LocationProvider:
    """Provides location data for satellite calculations."""

    @staticmethod
    def get_observer_location(df_location: Optional[pd.DataFrame] = None,
                            timestamp: Optional[float] = None) -> Optional[Any]:
        """Get observer location based on installation type."""
        try:
            if config.MOBILE:
                if df_location is None:
                    logger.error("Location data required for mobile installation")
                    return None

                if timestamp is not None:
                    return LocationProvider.get_mobile_location_at_time(df_location, timestamp)
                else:
                    # Get median time for location data
                    df_location['timestamp'] = pd.to_datetime(df_location['timestamp'], unit='s', utc=True)
                    median_time = df_location['timestamp'].median()
                    return LocationProvider.get_mobile_location_at_time(df_location, median_time.timestamp())
            else:
                # For fixed installations, use the configured location
                if not all([config.LATITUDE, config.LONGITUDE, config.ALTITUDE]):
                    logger.error("Static installation requires LATITUDE, LONGITUDE, and ALTITUDE in config")
                    return None
                return wgs84.latlon(
                    config.LATITUDE, config.LONGITUDE, config.ALTITUDE
                )

        except Exception as e:
            logger.error(f"Error getting observer location: {str(e)}", exc_info=True)
            return None

    @staticmethod
    def get_mobile_location_at_time(df_location: pd.DataFrame, timestamp: float) -> Optional[Any]:
        """Get mobile location at a specific time."""
        try:
            # Convert timestamp to datetime with UTC timezone
            if isinstance(timestamp, (int, float)):
                timestamp = pd.to_datetime(timestamp, unit='s', utc=True)
            else:
                timestamp = pd.to_datetime(timestamp, utc=True)

            # Find closest timestamp in location data
            df_location['timestamp'] = pd.to_datetime(df_location['timestamp'], unit='s', utc=True)
            time_diffs = abs(df_location['timestamp'] - timestamp)
            closest_idx = time_diffs.idxmin()

            # Get location data
            lat = df_location['lat'].iloc[closest_idx]
            lon = df_location['lon'].iloc[closest_idx]
            alt = df_location['alt'].iloc[closest_idx]

            return wgs84.latlon(lat, lon, alt)

        except Exception as e:
            logger.error(f"Error getting mobile location: {str(e)}", exc_info=True)
            return None 