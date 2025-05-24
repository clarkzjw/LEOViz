import logging
from typing import Optional, Any, Tuple
import pandas as pd
from skyfield.api import load, wgs84, utc

import config

logger = logging.getLogger(__name__)

class LocationProvider:
    """Provides location data for Starlink dish installations.
    
    This class handles location data for both static and mobile Starlink dish
    installations. For static installations, it uses a fixed location from
    configuration. For mobile installations, it uses GPS data from the dish.
    
    Attributes:
        None
    """

    def __init__(self):
        """Initialize the LocationProvider."""
        pass

    def get_observer_location(self, df_gps_diagnostics: Optional[pd.DataFrame] = None) -> Optional[Any]:
        """Get the observer location for satellite calculations.
        
        Args:
            df_gps_diagnostics: Optional DataFrame containing GPS diagnostics data
                for mobile installations. Required for mobile mode.
                
        Returns:
            Optional[Any]: Location object for satellite calculations, or None if
                location cannot be determined.
                
        Note:
            - For static installations, uses fixed location from config
            - For mobile installations, requires valid GPS data
            - Returns None if required data is missing or invalid
        """
        try:
            if config.MOBILE:
                if df_gps_diagnostics is None or df_gps_diagnostics.empty:
                    logger.error("GPS diagnostics data is required for mobile installations")
                    return None

                # Get the most recent GPS data
                latest_gps = df_gps_diagnostics.iloc[-1]
                latitude = latest_gps["latitude"]
                longitude = latest_gps["longitude"]
                altitude = latest_gps["altitude"]
            else:
                # Use fixed location from config
                latitude = config.LATITUDE
                longitude = config.LONGITUDE
                altitude = config.ALTITUDE

            # Create location object
            location = wgs84.latlon(latitude, longitude, altitude)
            return location

        except Exception as e:
            logger.error(f"Error getting observer location: {str(e)}", exc_info=True)
            return None

    def get_mobile_location_at_time(self, df_location: pd.DataFrame, timestamp: float) -> Optional[Any]:
        """Get location data for a specific timestamp."""
        try:
            # Convert timestamp to datetime
            target_time = pd.to_datetime(timestamp, unit='s', utc=True)
            
            # Find closest timestamp in location data
            df_location['timestamp'] = pd.to_datetime(df_location['timestamp'], unit='s', utc=True)
            closest_idx = (df_location['timestamp'] - target_time).abs().idxmin()
            location_data = df_location.iloc[closest_idx]
            
            return wgs84.latlon(
                location_data['lat'],
                location_data['lon'],
                location_data['alt']
            )

        except Exception as e:
            logger.error(f"Error getting mobile location: {str(e)}", exc_info=True)
            return None 