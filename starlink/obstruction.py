import csv
import logging
from datetime import datetime
from typing import List, Tuple, Optional
import pandas as pd
import numpy as np

from config import DATA_DIR
from timeslot_manager import TimeslotManager

import cv2

logger = logging.getLogger(__name__)


def process_obstruction_data(df_obstruction_map: pd.DataFrame) -> List[Tuple[datetime, float, float]]:
    """Process obstruction data and return list of timestamps and angles."""
    try:
        results = []
        for _, row in df_obstruction_map.iterrows():
            timestamp_dt = pd.to_datetime(row["timestamp"], unit='s')
            elevation = row["elevation"]
            azimuth = row["azimuth"]
            results.append((timestamp_dt, elevation, azimuth))
        return results
    except Exception as e:
        logger.error(f"Error processing obstruction data: {str(e)}", exc_info=True)
        return []


def get_time_range(df_obstruction_map: pd.DataFrame) -> Tuple[datetime, datetime]:
    """Get start and end times from obstruction map."""
    try:
        start_time = pd.to_datetime(
            df_obstruction_map.iloc[0]["timestamp"], unit='s'
        )
        end_time = pd.to_datetime(
            df_obstruction_map.iloc[-1]["timestamp"], unit='s'
        )
        return start_time, end_time
    except Exception as e:
        logger.error(f"Error getting time range: {str(e)}", exc_info=True)
        return None, None


def calculate_obstruction_angles(df_obstruction_map: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Calculate obstruction angles from obstruction map."""
    try:
        # Convert timestamps
        df_obstruction_map["timestamp"] = pd.to_datetime(
            df_obstruction_map["timestamp"], unit='s'
        )

        # Calculate angles
        df_obstruction_map["angle"] = np.arctan2(
            df_obstruction_map["elevation"],
            df_obstruction_map["azimuth"]
        )

        return df_obstruction_map

    except Exception as e:
        logger.error(f"Error calculating obstruction angles: {str(e)}", exc_info=True)
        return None


def process_obstruction_map(df_obstruction_map: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Process obstruction map data."""
    try:
        # Convert timestamps
        df_obstruction_map["timestamp"] = pd.to_datetime(
            df_obstruction_map["timestamp"], unit='s'
        )

        # Process data
        for _, row in df_obstruction_map.iterrows():
            timestamp_dt = pd.to_datetime(row["timestamp"], unit='s')
            # Process each row as needed
            pass

        return df_obstruction_map

    except Exception as e:
        logger.error(f"Error processing obstruction map: {str(e)}", exc_info=True)
        return None


def process_obstruction_timeslot(timeslot_df, writer):
    previous_obstruction_map = timeslot_df.iloc[0]["obstruction_map"]
    previous_obstruction_map = previous_obstruction_map.reshape(123, 123)

    hold_coord = None
    white_pixel_coords = []
    for index, row in timeslot_df.iterrows():
        timestamp_dt = pd.to_datetime(row["timestamp"], unit='s')
        obstruction_map = row["obstruction_map"].reshape(123, 123)
        xor_map = np.bitwise_xor(previous_obstruction_map, obstruction_map)
        coords = np.argwhere(xor_map == 1)

        if coords.size > 0:
            coord = coords[-1]  # Get the last occurrence
            hold_coord = coord  # Update hold_coord
        elif hold_coord is not None:
            coord = hold_coord  # Use the previous hold_coord if coords is empty
        else:
            continue  # If both coords is empty and hold_coord is None, skip this iteration

        white_pixel_coords.append((timestamp_dt, tuple(coord)))
        previous_obstruction_map = obstruction_map

    for coord in white_pixel_coords:
        writer.writerow(
            [
                coord[0].strftime("%Y-%m-%d %H:%M:%S"),
                coord[1][0],
                coord[1][1],
            ]
        )


def process_obstruction_maps(df_obstruction_map, uuid):
    start_time_dt = pd.to_datetime(
        df_obstruction_map.iloc[0]["timestamp"], unit='s'
    )
    end_time_dt = pd.to_datetime(
        df_obstruction_map.iloc[-1]["timestamp"], unit='s'
    )

    with open(
        f"{DATA_DIR}/obstruction-data-{uuid}.csv",
        "w",
        newline="",
    ) as csvfile:
        writer = csv.writer(csvfile)
        current_time = start_time_dt

        while current_time < end_time_dt:
            # Get next timeslot
            _, timeslot_endtime_dt = TimeslotManager.get_next_timeslot()
            
            # Adjust timeslot end time to match our data's timezone
            timeslot_endtime_dt = timeslot_endtime_dt.replace(tzinfo=current_time.tzinfo)

            # Get data for current timeslot
            timeslot_df = df_obstruction_map[
                (df_obstruction_map["timestamp"] >= current_time.timestamp())
                & (df_obstruction_map["timestamp"] < timeslot_endtime_dt.timestamp())
            ]

            if len(timeslot_df) == 0:
                current_time += pd.Timedelta(seconds=15)
                continue

            previous_obstruction_map = timeslot_df.iloc[0]["obstruction_map"]
            previous_obstruction_map = previous_obstruction_map.reshape(123, 123)

            hold_coord = None
            white_pixel_coords = []
            for index, row in timeslot_df.iterrows():
                timestamp_dt = pd.to_datetime(row["timestamp"], unit='s')
                obstruction_map = row["obstruction_map"].reshape(123, 123)
                xor_map = np.bitwise_xor(previous_obstruction_map, obstruction_map)
                coords = np.argwhere(xor_map == 1)

                if coords.size > 0:
                    coord = coords[-1]  # Get the last occurrence
                    hold_coord = coord  # Update hold_coord
                elif hold_coord is not None:
                    coord = hold_coord  # Use the previous hold_coord if coords is empty
                else:
                    continue  # If both coords is empty and hold_coord is None, skip this iteration

                white_pixel_coords.append((timestamp_dt, tuple(coord)))
                previous_obstruction_map = obstruction_map

            for coord in white_pixel_coords:
                writer.writerow(
                    [
                        coord[0].strftime("%Y-%m-%d %H:%M:%S"),
                        coord[1][0],
                        coord[1][1],
                    ]
                )

            current_time = timeslot_endtime_dt


def create_obstruction_map_video(FILENAME, uuid, fps):
    df = pd.read_parquet(FILENAME)
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(
        f"{DATA_DIR}/obstruction_map-{uuid}.mp4",
        fourcc,
        fps,
        (123, 123),
    )
    for index, row in df.iterrows():
        obstruction_map = row["obstruction_map"].reshape(123, 123)
        image_data = (obstruction_map * 255).astype(np.uint8)
        image_data_bgr = cv2.cvtColor(image_data, cv2.COLOR_GRAY2BGR)

        out.write(image_data_bgr)

    out.release()
