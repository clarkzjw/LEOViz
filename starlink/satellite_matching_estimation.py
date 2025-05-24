import os
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Any, Optional

import pandas as pd
import numpy as np
from skyfield.api import load, utc

import config
from location_provider import LocationProvider
from data_feature_extraction import DataFeatureExtraction

logger = logging.getLogger(__name__)

class SatelliteProcessor:
    """Handles satellite estimation, matching, and distance calculations."""

    @staticmethod
    def estimate_connected_satellites(uuid: str, date: str, frame_type: int, df_status: pd.DataFrame,
                                    start_time: float, end_time: float) -> Optional[pd.DataFrame]:
        """Estimate connected satellites for a given time range."""
        try:
            # Convert timestamps to datetime
            start_ts = datetime.fromtimestamp(start_time, tz=timezone.utc)
            end_ts = datetime.fromtimestamp(end_time, tz=timezone.utc)

            # Get location data file for mobile installations
            df_location = None
            if config.MOBILE:
                location_filename = f"{config.DATA_DIR}/grpc/{date}/GRPC_LOCATION-{uuid}.csv"
                if not os.path.exists(location_filename):
                    logger.error(f"Location file not found: {location_filename}")
                    return None

                logger.info(f"Reading location file: {location_filename}")
                df_location = pd.read_csv(location_filename)
                df_location['timestamp'] = pd.to_datetime(df_location['timestamp'], unit='s', utc=True)

            # Get obstruction data file
            obstruction_file = f"obstruction-data-{uuid}.csv"
            if not os.path.exists(os.path.join(config.DATA_DIR, obstruction_file)):
                logger.error(f"Obstruction file not found: {obstruction_file}")
                return None

            # Process obstruction data
            filename = os.path.join(config.DATA_DIR, obstruction_file)
            merged_data_file = os.path.join(config.DATA_DIR, f"processed_obstruction-data-{uuid}.csv")
            
            # Merge obstruction data with status and location data
            logger.info(f"Merging obstruction data with status and location data")
            data_extracter = DataFeatureExtraction()
            merged_df = data_extracter.merge_obstruction_with_status_and_location(
                filename,
                frame_type,
                df_status,
                df_location if config.MOBILE else None
            )
            # The merged data should now contain:  
            if merged_df.empty:
                logger.error("Failed to merge data")
                return None

            # Save the merged data
            logger.info(f"Saving merged data to {merged_data_file}")
            merged_df.to_csv(merged_data_file, index=False)

            # Load TLE data
            tle_file = f"{config.TLE_DATA_DIR}/{date}/starlink-tle-{uuid}.txt"
            if not os.path.exists(tle_file):
                logger.error(f"TLE file not found: {tle_file}")
                return None

            satellites = load.tle_file(tle_file)
            logger.info(f"Loaded {len(satellites)} Starlink TLE satellites")

            # Process data in intervals
            result_df = SatelliteProcessor.process_intervals(
                filename, start_ts.year, start_ts.month, start_ts.day,
                start_ts.hour, start_ts.minute, start_ts.second,
                end_ts.year, end_ts.month, end_ts.day,
                end_ts.hour, end_ts.minute, end_ts.second,
                merged_data_file, satellites, frame_type,
                df_location if config.MOBILE else None
            )

            if result_df is None or result_df.empty:
                logger.error("No results returned from process_intervals")
                return None

            # Save the serving satellite data using the same UUID timestamp
            serving_satellite_file = f"{config.DATA_DIR}/serving_satellite_data-{uuid}.csv"
            os.makedirs(os.path.dirname(serving_satellite_file), exist_ok=True)
            
            # Check if file exists to determine if we need to write header
            file_exists = os.path.exists(serving_satellite_file)
            
            # Append to the file
            result_df.to_csv(serving_satellite_file, mode='a', header=not file_exists, index=False)
            logger.info(f"Satellite estimation complete for {start_ts} to {end_ts}")
            
            return result_df

        except Exception as e:
            logger.error(f"Error estimating connected satellites: {str(e)}", exc_info=True)
            return None

    @staticmethod
    def find_matching_satellites(satellites: List[Any], observer_location: Any,
                               observed_positions_with_timestamps: List[Tuple[datetime, Tuple[float, float]]],
                               frame_type: int) -> List[str]:
        """Find matching satellites based on observed positions.
        Compare the theree obsereved positions to all the satellites and calculate those in our elevation view.
        """

        best_match = None
        closest_total_difference = float("inf")
        ts = load.timescale()

        for satellite in satellites:
            satellite_positions = []
            valid_positions = True

            for observed_time, observed_data in observed_positions_with_timestamps:
                difference = satellite - observer_location
                topocentric = difference.at(
                    ts.utc(
                        observed_time.year,
                        observed_time.month,
                        observed_time.day,
                        observed_time.hour,
                        observed_time.minute,
                        observed_time.second,
                    )
                )
                alt, az, _ = topocentric.altaz()

                if alt.degrees <= 20:
                    valid_positions = False
                    break

                satellite_positions.append((alt.degrees, az.degrees))

            if valid_positions:
                if frame_type == 1:  # FRAME_EARTH
                    total_difference = SatelliteProcessor.calculate_total_difference(
                        [(90 - data[0], data[1]) for _, data in observed_positions_with_timestamps],
                        satellite_positions
                    )
                elif frame_type == 2:  # FRAME_UT
                    total_difference = SatelliteProcessor.calculate_trajectory_distance_frame_ut(
                        [(90 - data[0], data[1]) for _, data in observed_positions_with_timestamps],
                        satellite_positions
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
        ts = load.timescale()
        
        for second in range(interval_seconds + 1):
            current_time = start_time + timedelta(seconds=second)
            difference = satellite - observer_location
            topocentric = difference.at(ts.from_datetime(current_time))
            distances.append(topocentric.distance().km)
        return distances

    @staticmethod
    def process(
        filename: str,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
        second: int,
        merged_data_file: str,
        satellites: List[Any],
        frame_type: int,
        df_gps_diagnostics: Optional[pd.DataFrame] = None
    ) -> Tuple[Optional[List[Tuple[datetime, Tuple[float, float]]]], 
              Optional[List[str]], 
              Optional[List[float]]]:
        """Process a single time interval to find matching satellites."""
        initial_time = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
        
        # Get observer location based on installation type
        observer_location = LocationProvider.get_observer_location(df_gps_diagnostics)
        if observer_location is None:
            logger.error("Failed to get observer location")
            return None, None, None

        # Get observed positions using the existing method
        data_extracter = DataFeatureExtraction()
        observed_positions_with_timestamps = data_extracter.process_observed_data(
            filename, initial_time.strftime("%Y-%m-%dT%H:%M:%SZ"), merged_data_file
        )
        if observed_positions_with_timestamps is None:
            return None, None, None

        # Find matching satellites
        matching_satellites = SatelliteProcessor.find_matching_satellites(
            satellites, observer_location, observed_positions_with_timestamps, frame_type
        )
        if not matching_satellites:
            return observed_positions_with_timestamps, [], []

        # Calculate distances for the best match
        best_match_satellite = next(
            sat for sat in satellites if sat.name == matching_satellites[0]
        )
        distances = SatelliteProcessor.calculate_distance_for_best_match(
            best_match_satellite, observer_location, initial_time, 14
        )

        return observed_positions_with_timestamps, matching_satellites, distances

    @staticmethod
    def process_intervals(
        filename: str,
        start_year: int,
        start_month: int,
        start_day: int,
        start_hour: int,
        start_minute: int,
        start_second: int,
        end_year: int,
        end_month: int,
        end_day: int,
        end_hour: int,
        end_minute: int,
        end_second: int,
        merged_data_file: str,
        satellites: List[Any],
        frame_type: int,
        df_gps_diagnostics: Optional[pd.DataFrame] = None
    ) -> Optional[pd.DataFrame]:
        """Process data in intervals and find matching satellites."""
        try:
            # Create datetime objects for start and end times
            start_time = datetime(start_year, start_month, start_day, start_hour, start_minute, start_second, tzinfo=timezone.utc)
            end_time = datetime(end_year, end_month, end_day, end_hour, end_minute, end_second, tzinfo=timezone.utc)
            
            results = []
            current_time = start_time
            
            while current_time <= end_time:
                logger.info(f"Estimating connected satellites for timeslot {current_time}")
                
                observed_positions_with_timestamps, matching_satellites, distances = SatelliteProcessor.process(
                    filename,
                    current_time.year,
                    current_time.month,
                    current_time.day,
                    current_time.hour,
                    current_time.minute,
                    current_time.second,
                    merged_data_file,
                    satellites,
                    frame_type,
                    df_gps_diagnostics
                )
                
                if matching_satellites:
                    for second in range(15):
                        if second < len(distances):
                            results.append({
                                'Timestamp': current_time + timedelta(seconds=second),
                                'Connected_Satellite': matching_satellites[0],
                                'Distance': distances[second]
                            })
                
                current_time += timedelta(seconds=15)
            
            if not results:
                logger.warning("No data points processed")
                return None
            
            return pd.DataFrame(results)
            
        except Exception as e:
            logger.error(f"Error processing intervals: {str(e)}", exc_info=True)
            return None

    @staticmethod
    def calculate_total_difference(observed_positions, satellite_positions):
        """Calculate the total angular separation and bearing difference."""
        total_angular_separation = 0
        for i in range(len(observed_positions)):
            obs_alt, obs_az = observed_positions[i]
            sat_alt, sat_az = satellite_positions[i]
            separation = SatelliteProcessor.angular_separation(obs_alt, obs_az, sat_alt, sat_az)
            total_angular_separation += separation
        bearing_diff = SatelliteProcessor.calculate_bearing_difference(observed_positions, satellite_positions)
        total_difference = total_angular_separation + bearing_diff
        return total_difference

    @staticmethod
    def angular_separation(alt1, az1, alt2, az2):
        """Calculate the angular separation between two points on a sphere."""
        alt1, alt2 = np.radians(alt1), np.radians(alt2)
        az1 = (az1 + 360) % 360
        az2 = (az2 + 360) % 360
        az_diff = np.abs(az1 - az2)
        if az_diff > 180:
            az_diff = 360 - az_diff
        az_diff = np.radians(az_diff)
        separation = np.arccos(
            np.sin(alt1) * np.sin(alt2) + np.cos(alt1) * np.cos(alt2) * np.cos(az_diff)
        )
        return np.degrees(separation)

    @staticmethod
    def calculate_bearing(alt1, az1, alt2, az2):
        """Calculate bearing between two points."""
        alt1, alt2 = np.radians(alt1), np.radians(alt2)
        az1, az2 = np.radians(az1), np.radians(az2)
        x = np.sin(az2 - az1) * np.cos(alt2)
        y = np.cos(alt1) * np.sin(alt2) - np.sin(alt1) * np.cos(alt2) * np.cos(az2 - az1)
        bearing = np.arctan2(x, y)
        bearing = np.degrees(bearing)
        return (bearing + 360) % 360

    @staticmethod
    def calculate_bearing_difference(observed_trajectory, satellite_trajectory):
        """Calculate bearing difference between two trajectories."""
        observed_bearing = SatelliteProcessor.calculate_bearing(
            observed_trajectory[0][0],
            observed_trajectory[0][1],
            observed_trajectory[-1][0],
            observed_trajectory[-1][1],
        )
        satellite_bearing = SatelliteProcessor.calculate_bearing(
            satellite_trajectory[0][0],
            satellite_trajectory[0][1],
            satellite_trajectory[-1][0],
            satellite_trajectory[-1][1],
        )
        bearing_diff = abs(observed_bearing - satellite_bearing)
        if bearing_diff > 180:
            bearing_diff = 360 - bearing_diff
        return bearing_diff

    @staticmethod
    def calculate_trajectory_distance_frame_ut(observed_positions, satellite_positions):
        """Calculate the distance measure between observed and satellite trajectories."""
        altitude_range = 90.0  # Maximum possible altitude difference
        azimuth_range = 180.0  # Maximum possible azimuth difference
        direction_range = 2.0  # Maximum possible direction difference

        distance = 0
        for i in range(len(observed_positions)):
            # Calculate distance between points
            alt_deviation = abs(observed_positions[i][0] - satellite_positions[i][0]) / altitude_range
            az_deviation = SatelliteProcessor.azimuth_difference(observed_positions[i][1], satellite_positions[i][1]) / azimuth_range
            distance += alt_deviation + az_deviation

        # Calculate the overall direction vectors
        obs_dir_vector = SatelliteProcessor.calculate_direction_vector(observed_positions[0], observed_positions[-1])
        sat_dir_vector = SatelliteProcessor.calculate_direction_vector(satellite_positions[0], satellite_positions[-1])

        # Calculate direction difference
        direction_diff = np.sqrt(
            (obs_dir_vector[0] - sat_dir_vector[0]) ** 2 + 
            (obs_dir_vector[1] - sat_dir_vector[1]) ** 2
        ) / direction_range

        # Add the direction difference to the distance measure
        total_distance = distance + direction_diff

        return total_distance

    @staticmethod
    def azimuth_difference(az1, az2):
        """Calculate the smallest difference between two azimuth angles."""
        diff = abs(az1 - az2) % 360
        if diff > 180:
            diff = 360 - diff
        return diff

    @staticmethod
    def calculate_direction_vector(point1, point2):
        """Calculate the direction vector from point1 to point2."""
        alt_diff = point2[0] - point1[0]
        az_diff = SatelliteProcessor.azimuth_difference(point2[1], point1[1])
        magnitude = np.sqrt(alt_diff**2 + az_diff**2)
        return (alt_diff / magnitude, az_diff / magnitude) if magnitude != 0 else (0, 0) 