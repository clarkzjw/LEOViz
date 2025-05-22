import math
import numpy as np
from typing import List, Tuple

class TrajectoryAnalyzer:
    """Handles trajectory analysis and calculations."""
    
    @staticmethod
    def angular_separation(alt1: float, az1: float, alt2: float, az2: float) -> float:
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
    def calculate_bearing(alt1: float, az1: float, alt2: float, az2: float) -> float:
        """Calculate bearing (direction) between two points."""
        alt1, alt2 = np.radians(alt1), np.radians(alt2)
        az1, az2 = np.radians(az1), np.radians(az2)
        x = np.sin(az2 - az1) * np.cos(alt2)
        y = np.cos(alt1) * np.sin(alt2) - np.sin(alt1) * np.cos(alt2) * np.cos(az2 - az1)
        bearing = np.arctan2(x, y)
        bearing = np.degrees(bearing)
        return (bearing + 360) % 360

    @staticmethod
    def calculate_bearing_difference(observed_trajectory: List[Tuple[float, float]], 
                                   satellite_trajectory: List[Tuple[float, float]]) -> float:
        """Calculate bearing difference between two trajectories."""
        observed_bearing = TrajectoryAnalyzer.calculate_bearing(
            observed_trajectory[0][0], observed_trajectory[0][1],
            observed_trajectory[-1][0], observed_trajectory[-1][1]
        )
        satellite_bearing = TrajectoryAnalyzer.calculate_bearing(
            satellite_trajectory[0][0], satellite_trajectory[0][1],
            satellite_trajectory[-1][0], satellite_trajectory[-1][1]
        )
        bearing_diff = abs(observed_bearing - satellite_bearing)
        return 360 - bearing_diff if bearing_diff > 180 else bearing_diff

    @staticmethod
    def calculate_total_difference(observed_positions: List[Tuple[float, float]], 
                                 satellite_positions: List[Tuple[float, float]]) -> float:
        """Calculate total angular separation and bearing difference."""
        total_angular_separation = sum(
            TrajectoryAnalyzer.angular_separation(obs_alt, obs_az, sat_alt, sat_az)
            for (obs_alt, obs_az), (sat_alt, sat_az) in zip(observed_positions, satellite_positions)
        )
        bearing_diff = TrajectoryAnalyzer.calculate_bearing_difference(
            observed_positions, satellite_positions
        )
        return total_angular_separation + bearing_diff

    @staticmethod
    def azimuth_difference(az1: float, az2: float) -> float:
        """Calculate the smallest difference between two azimuth angles."""
        diff = abs(az1 - az2) % 360
        return 360 - diff if diff > 180 else diff

    @staticmethod
    def calculate_direction_vector(point1: Tuple[float, float], 
                                 point2: Tuple[float, float]) -> Tuple[float, float]:
        """Calculate the direction vector from point1 to point2."""
        alt_diff = point2[0] - point1[0]
        az_diff = TrajectoryAnalyzer.azimuth_difference(point2[1], point1[1])
        magnitude = math.sqrt(alt_diff**2 + az_diff**2)
        return (alt_diff / magnitude, az_diff / magnitude) if magnitude != 0 else (0, 0)

    @staticmethod
    def calculate_trajectory_distance_frame_ut(observed_positions: List[Tuple[float, float]], 
                                             satellite_positions: List[Tuple[float, float]]) -> float:
        """Calculate the distance measure between observed and satellite trajectories."""
        altitude_range = 90.0
        azimuth_range = 180.0
        direction_range = 2.0

        distance = sum(
            abs(obs[0] - sat[0]) / altitude_range + 
            TrajectoryAnalyzer.azimuth_difference(obs[1], sat[1]) / azimuth_range
            for obs, sat in zip(observed_positions, satellite_positions)
        )

        obs_dir_vector = TrajectoryAnalyzer.calculate_direction_vector(
            observed_positions[0], observed_positions[-1]
        )
        sat_dir_vector = TrajectoryAnalyzer.calculate_direction_vector(
            satellite_positions[0], satellite_positions[-1]
        )

        direction_diff = math.sqrt(
            (obs_dir_vector[0] - sat_dir_vector[0])**2 +
            (obs_dir_vector[1] - sat_dir_vector[1])**2
        ) / direction_range

        return distance + direction_diff 