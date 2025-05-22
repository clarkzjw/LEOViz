import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Any, Optional

import pandas as pd
from skyfield.api import load

import config
from trajectory_analyzer import TrajectoryAnalyzer
from location_provider import LocationProvider

logger = logging.getLogger(__name__)

class SatelliteMatcher:
    """Handles satellite matching and distance calculations."""
    
    @staticmethod
    def find_matching_satellites(satellites: List[Any], observer_location: Any,
                               observed_positions_with_timestamps: List[Tuple[datetime, Tuple[float, float]]],
                               frame_type: int, df_sinr: Optional[pd.DataFrame] = None) -> List[str]:
        """Find matching satellites based on observed positions."""
        best_match = None
        closest_total_difference = float("inf")
        ts = load.timescale()

        for satellite in satellites:
            satellite_positions = []
            valid_positions = True

            for observed_time, observed_data in observed_positions_with_timestamps:
                current_location = (
                    LocationProvider.get_observer_location(df_sinr, observed_time)
                    if config.MOBILE else observer_location
                )
                
                if current_location is None:
                    valid_positions = False
                    break

                difference = satellite - current_location
                topocentric = difference.at(
                    ts.utc(
                        observed_time.year, observed_time.month, observed_time.day,
                        observed_time.hour, observed_time.minute, observed_time.second
                    )
                )
                alt, az, _ = topocentric.altaz()

                if alt.degrees <= 20:
                    valid_positions = False
                    break

                satellite_positions.append((alt.degrees, az.degrees))

            if valid_positions:
                observed_data = [(90 - data[0], data[1]) for _, data in observed_positions_with_timestamps]
                
                if frame_type == 1:  # FRAME_EARTH
                    total_difference = TrajectoryAnalyzer.calculate_total_difference(
                        observed_data, satellite_positions
                    )
                else:  # FRAME_UT
                    total_difference = TrajectoryAnalyzer.calculate_trajectory_distance_frame_ut(
                        observed_data, satellite_positions
                    )

                if total_difference < closest_total_difference:
                    closest_total_difference = total_difference
                    best_match = satellite.name

        return [best_match] if best_match else []

    @staticmethod
    def calculate_distance_for_best_match(satellite: Any, observer_location: Any,
                                        start_time: datetime, interval_seconds: int) -> List[float]:
        """Calculate distances for the best matching satellite."""
        distances = []
        for second in range(interval_seconds + 1):
            current_time = start_time + timedelta(seconds=second)
            difference = satellite - observer_location
            topocentric = difference.at(current_time)
            distances.append(topocentric.distance().km)
        return distances 