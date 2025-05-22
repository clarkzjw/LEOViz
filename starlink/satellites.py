# flake8: noqa:E501

import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Any

import pandas as pd
from skyfield.api import load, utc

from trajectory_analyzer import TrajectoryAnalyzer
from data_processor import DataProcessor
from satellite_matcher import SatelliteMatcher
from location_provider import LocationProvider

logger = logging.getLogger(__name__)

def process(filename: str, year: int, month: int, day: int, hour: int, minute: int, second: int,
           merged_data_file: str, satellites: List[Any], frame_type: int,
           df_sinr: Optional[pd.DataFrame] = None) -> Tuple[Optional[List[Tuple[datetime, Tuple[float, float]]]], 
                                                          Optional[List[str]], 
                                                          Optional[List[float]]]:
    """Process satellite data for a specific time period."""
    initial_time = load.timescale().utc(year, month, day, hour, minute, second)
    logger.info(f"Processing data for time: {initial_time.utc_strftime('%Y-%m-%dT%H:%M:%SZ')}")

    observer_location = LocationProvider.get_observer_location(df_sinr)
    if observer_location is None:
        logger.error("Failed to get observer location")
        return None, None, None

    logger.info(f"Observer location: {observer_location}")

    observed_positions = DataProcessor.process_observed_data(
        filename, initial_time.utc_strftime("%Y-%m-%dT%H:%M:%SZ"), merged_data_file
    )
    if observed_positions is None:
        logger.error("Failed to process observed data")
        return None, None, None

    logger.info(f"Found {len(observed_positions)} observed positions")

    matching_satellites = SatelliteMatcher.find_matching_satellites(
        satellites, observer_location, observed_positions, frame_type, df_sinr
    )
    if not matching_satellites:
        logger.warning("No matching satellites found")
        return observed_positions, [], []

    logger.info(f"Found matching satellites: {matching_satellites}")

    best_match_satellite = next(sat for sat in satellites if sat.name == matching_satellites[0])
    distances = SatelliteMatcher.calculate_distance_for_best_match(
        best_match_satellite, observer_location, initial_time, 14
    )

    return observed_positions, matching_satellites, distances


def process_intervals(filename: str, start_year: int, start_month: int, start_day: int,
                     start_hour: int, start_minute: int, start_second: int, end_year: int,
                     end_month: int, end_day: int, end_hour: int, end_minute: int,
                     end_second: int, merged_data_file: str, satellites: List[Any],
                     frame_type: int, df_sinr: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Process satellite data for multiple time intervals."""
    results = []
    start_time = datetime(start_year, start_month, start_day, start_hour, start_minute,
                         start_second, tzinfo=utc)
    end_time = datetime(end_year, end_month, end_day, end_hour, end_minute, end_second,
                       tzinfo=utc)
    current_time = start_time

    while current_time <= end_time:
        logger.info(f"Estimating connected satellites for timeslot {current_time}")
        observed_positions, matching_satellites, distances = process(
            filename, current_time.year, current_time.month, current_time.day,
            current_time.hour, current_time.minute, current_time.second,
            merged_data_file, satellites, frame_type, df_sinr
        )

        if matching_satellites:
            for second in range(15):
                if second < len(distances):
                    results.append({
                            "Timestamp": current_time + timedelta(seconds=second),
                            "Connected_Satellite": matching_satellites[0],
                            "Distance": distances[second],
                    })

        current_time += timedelta(seconds=15)

    return pd.DataFrame(results)


def get_observer_location(df_sinr: Optional[pd.DataFrame]) -> Optional[Any]:
    """Get observer location using the LocationProvider."""
    return LocationProvider.get_observer_location(df_sinr)
