# flake8: noqa:E501

import math
import logging
import config
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
from skyfield.api import load, wgs84, utc

logger = logging.getLogger(__name__)


def pre_process_observed_data(filename, frame_type, df_sinr):
    data = pd.read_csv(filename, sep=",", header=None, names=["Timestamp", "Y", "X"])
    data["Timestamp"] = pd.to_datetime(data["Timestamp"], utc=True)

    if frame_type == 1:
        frame_type_str = "FRAME_EARTH"
    elif frame_type == 2:
        frame_type_str = "FRAME_UT"
    else:
        frame_type_str = "UNKNOWN"

    _tilt = df_sinr["tiltAngleDeg"].iloc[0]
    _rotation_az = df_sinr["boresightAzimuthDeg"].iloc[0]

    if frame_type_str == "FRAME_EARTH":
        observer_x, observer_y = 62, 62  # Assume this is the observer's pixel location
    elif frame_type_str == "FRAME_UT":
        observer_x, observer_y = 62, 62 - (_tilt / (80 / 62))
    pixel_to_degrees = 80 / 62  # Conversion factor from pixel to degrees

    positions = []
    for index, point in data.iterrows():
        if frame_type_str == "FRAME_EARTH":
            dx, dy = point["X"] - observer_x, (123 - point["Y"]) - observer_y
        elif frame_type_str == "FRAME_UT":
            dx, dy = point["X"] - observer_x, point["Y"] - observer_y
        radius = np.sqrt(dx**2 + dy**2) * pixel_to_degrees
        azimuth = np.degrees(np.arctan2(dx, dy))
        if frame_type_str == "FRAME_EARTH":
            azimuth = (azimuth + 360) % 360
        elif frame_type_str == "FRAME_UT":
            azimuth = (azimuth + _rotation_az + 360) % 360
        # Normalize the azimuth to ensure it's within 0 to 360 degrees
        elevation = 90 - radius
        positions.append(
            (point["Timestamp"], point["Y"], point["X"], elevation, azimuth)
        )

    df_positions = pd.DataFrame(
        positions, columns=["Timestamp", "Y", "X", "Elevation", "Azimuth"]
    )
    return df_positions


def convert_observed(dir, filename, frame_type, df_sinr):
    observed_positions = pre_process_observed_data(
        Path(dir).joinpath(filename), frame_type, df_sinr
    )
    if not observed_positions.empty:
        observed_positions.to_csv(
            Path(dir).joinpath(f"processed_{filename}"), index=False
        )
    else:
        print("No valid observed data found.")

    return observed_positions


def set_observation_time(year, month, day, hour, minute, second):
    ts = load.timescale()
    return ts.utc(year, month, day, hour, minute, second)


def process_observed_data(filename, start_time, merged_data_file):
    data = pd.read_csv(filename, sep=",", header=None, names=["Timestamp", "Y", "X"])
    data["Timestamp"] = pd.to_datetime(data["Timestamp"], utc=True)
    interval_start_time = pd.to_datetime(start_time, utc=True)
    interval_end_time = interval_start_time + pd.Timedelta(seconds=14)
    filtered_data = data[
        (data["Timestamp"] >= interval_start_time)
        & (data["Timestamp"] < interval_end_time)
    ]
    if filtered_data.empty:
        print("No data found.")
        return None

    merged_data = pd.read_csv(merged_data_file, parse_dates=["Timestamp"])
    merged_data["Timestamp"] = pd.to_datetime(merged_data["Timestamp"], utc=True)
    merged_filtered_data = merged_data[
        (merged_data["Timestamp"] >= interval_start_time)
        & (merged_data["Timestamp"] < interval_end_time)
    ]

    if merged_filtered_data.empty:
        print("No matching data found in merged_data_file.")
        return None

    if len(merged_filtered_data) < 3:
        print("Not enough data points in merged_filtered_data.")
        return None

    start_data = merged_filtered_data.iloc[0]
    middle_data = merged_filtered_data.iloc[len(merged_filtered_data) // 2]
    end_data = merged_filtered_data.iloc[-2]
    rotation = 0
    positions = [
        (
            start_data["Timestamp"],
            (90 - start_data["Elevation"], (start_data["Azimuth"] + rotation) % 360),
        ),
        (
            middle_data["Timestamp"],
            (90 - middle_data["Elevation"], (middle_data["Azimuth"] + rotation) % 360),
        ),
        (
            end_data["Timestamp"],
            (90 - end_data["Elevation"], (end_data["Azimuth"] + rotation) % 360),
        ),
    ]

    return positions


# Calculate angular separation between two positions
def angular_separation(alt1, az1, alt2, az2):
    """Calculate the angular separation between two points on a sphere given by altitude and azimuth."""
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


# Calculate bearing (direction) between two points
def calculate_bearing(alt1, az1, alt2, az2):
    alt1, alt2 = np.radians(alt1), np.radians(alt2)
    az1, az2 = np.radians(az1), np.radians(az2)
    x = np.sin(az2 - az1) * np.cos(alt2)
    y = np.cos(alt1) * np.sin(alt2) - np.sin(alt1) * np.cos(alt2) * np.cos(az2 - az1)
    bearing = np.arctan2(x, y)
    bearing = np.degrees(bearing)
    return (bearing + 360) % 360


# Calculate bearing difference between two trajectories
def calculate_bearing_difference(observed_trajectory, satellite_trajectory):
    observed_bearing = calculate_bearing(
        observed_trajectory[0][0],
        observed_trajectory[0][1],
        observed_trajectory[-1][0],
        observed_trajectory[-1][1],
    )
    satellite_bearing = calculate_bearing(
        satellite_trajectory[0][0],
        satellite_trajectory[0][1],
        satellite_trajectory[-1][0],
        satellite_trajectory[-1][1],
    )
    bearing_diff = abs(observed_bearing - satellite_bearing)
    if bearing_diff > 180:
        bearing_diff = 360 - bearing_diff
    return bearing_diff


# Calculate the total angular separation and bearing difference
def calculate_total_difference(observed_positions, satellite_positions):
    total_angular_separation = 0
    for i in range(len(observed_positions)):
        obs_alt, obs_az = observed_positions[i]
        sat_alt, sat_az = satellite_positions[i]
        separation = angular_separation(obs_alt, obs_az, sat_alt, sat_az)
        total_angular_separation += separation
    bearing_diff = calculate_bearing_difference(observed_positions, satellite_positions)
    total_difference = total_angular_separation + bearing_diff
    return total_difference


def azimuth_difference(az1, az2):
    """Calculate the smallest difference between two azimuth angles."""
    diff = abs(az1 - az2) % 360
    if diff > 180:
        diff = 360 - diff
    return diff


def calculate_direction_vector(point1, point2):
    """Calculate the direction vector from point1 to point2."""
    alt_diff = point2[0] - point1[0]
    az_diff = azimuth_difference(point2[1], point1[1])
    magnitude = math.sqrt(alt_diff**2 + az_diff**2)
    return (alt_diff / magnitude, az_diff / magnitude) if magnitude != 0 else (0, 0)


def calculate_trajectory_distance_frame_ut(observed_positions, satellite_positions):
    """Calculate the distance measure between observed and satellite trajectories."""
    altitude_range = 90.0  # Maximum possible altitude difference
    azimuth_range = 180.0  # Maximum possible azimuth difference
    direction_range = 2.0  # Maximum possible direction difference

    distance = 0
    for i in range(len(observed_positions)):
        # Calculate distance between points
        alt_deviation = (
            abs(observed_positions[i][0] - satellite_positions[i][0]) / altitude_range
        )
        az_deviation = (
            azimuth_difference(observed_positions[i][1], satellite_positions[i][1])
            / azimuth_range
        )
        distance += alt_deviation + az_deviation

    # Calculate the overall direction vectors
    obs_dir_vector = calculate_direction_vector(
        observed_positions[0], observed_positions[-1]
    )
    sat_dir_vector = calculate_direction_vector(
        satellite_positions[0], satellite_positions[len(observed_positions) - 1]
    )

    # Calculate direction difference
    direction_diff = (
        math.sqrt(
            (obs_dir_vector[0] - sat_dir_vector[0]) ** 2
            + (obs_dir_vector[1] - sat_dir_vector[1]) ** 2
        )
        / direction_range
    )

    # Add the direction difference to the distance measure
    total_distance = distance + direction_diff

    return total_distance


def find_matching_satellites(
    satellites, observer_location, observed_positions_with_timestamps, frame_type
):
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
                total_difference = calculate_total_difference(
                    [
                        (90 - data[0], data[1])
                        for _, data in observed_positions_with_timestamps
                    ],
                    satellite_positions,
                )
            elif frame_type == 2:  # FRAME_UT
                total_difference = calculate_trajectory_distance_frame_ut(
                    [
                        (90 - data[0], data[1])
                        for _, data in observed_positions_with_timestamps
                    ],
                    satellite_positions,
                )
            # print(satellite.name, ": ", total_difference)
            if total_difference < closest_total_difference:
                closest_total_difference = total_difference
                best_match = satellite.name

    return [best_match] if best_match else []


def calculate_distance_for_best_match(
    satellite, observer_location, start_time, interval_seconds
):
    distances = []
    for second in range(0, interval_seconds + 1):
        current_time = start_time + timedelta(seconds=second)
        difference = satellite - observer_location
        topocentric = difference.at(current_time)
        distance = topocentric.distance().km
        distances.append(distance)
    return distances


def process(
    filename,
    year,
    month,
    day,
    hour,
    minute,
    second,
    merged_data_file,
    satellites,
    frame_type,
):
    initial_time = set_observation_time(year, month, day, hour, minute, second)
    observer_location = wgs84.latlon(
        latitude_degrees=config.LATITUDE,
        longitude_degrees=config.LONGITUDE,
        elevation_m=config.ALTITUDE,
    )
    observed_positions_with_timestamps = process_observed_data(
        filename, initial_time.utc_strftime("%Y-%m-%dT%H:%M:%SZ"), merged_data_file
    )
    if observed_positions_with_timestamps is None:
        return [], [], []

    matching_satellites = find_matching_satellites(
        satellites, observer_location, observed_positions_with_timestamps, frame_type
    )
    if not matching_satellites:
        return observed_positions_with_timestamps, [], []

    best_match_satellite = next(
        sat for sat in satellites if sat.name == matching_satellites[0]
    )
    distances = calculate_distance_for_best_match(
        best_match_satellite, observer_location, initial_time, 14
    )

    return observed_positions_with_timestamps, matching_satellites, distances


def process_intervals(
    filename,
    start_year,
    start_month,
    start_day,
    start_hour,
    start_minute,
    start_second,
    end_year,
    end_month,
    end_day,
    end_hour,
    end_minute,
    end_second,
    merged_data_file,
    satellites,
    frame_type,
):
    results = []

    start_time = datetime(
        start_year,
        start_month,
        start_day,
        start_hour,
        start_minute,
        start_second,
        tzinfo=utc,
    )
    end_time = datetime(
        end_year, end_month, end_day, end_hour, end_minute, end_second, tzinfo=utc
    )
    current_time = start_time

    while current_time <= end_time:
        logger.info(f"Estimating connected satellites for timeslot {current_time}")
        observed_positions_with_timestamps, matching_satellites, distances = process(
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
        )
        if matching_satellites:
            for second in range(15):
                if second < len(distances):
                    results.append(
                        {
                            "Timestamp": current_time + timedelta(seconds=second),
                            "Connected_Satellite": matching_satellites[0],
                            "Distance": distances[second],
                        }
                    )
        current_time += timedelta(seconds=15)

    result_df = pd.DataFrame(results)
    return result_df
