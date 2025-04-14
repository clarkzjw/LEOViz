# flake8: noqa: E501
import csv

from datetime import datetime, timezone

from config import DATA_DIR

import cv2
import pandas as pd
import numpy as np


def process_obstruction_maps(df_obstruction_map, uuid):
    start_time_dt = datetime.fromtimestamp(
        df_obstruction_map.iloc[0]["timestamp"], tz=timezone.utc
    )
    end_time_dt = datetime.fromtimestamp(
        df_obstruction_map.iloc[-1]["timestamp"], tz=timezone.utc
    )

    with open(
        f"{DATA_DIR}/obstruction-data-{uuid}.csv",
        "w",
        newline="",
    ) as csvfile:
        writer = csv.writer(csvfile)
        while start_time_dt < end_time_dt:
            if start_time_dt.second >= 12 and start_time_dt.second < 27:
                timeslot_endtime_dt = start_time_dt.replace(microsecond=0).replace(
                    second=27
                )
            elif start_time_dt.second >= 27 and start_time_dt.second < 42:
                timeslot_endtime_dt = start_time_dt.replace(microsecond=0).replace(
                    second=42
                )
            elif start_time_dt.second >= 42 and start_time_dt.second < 57:
                timeslot_endtime_dt = start_time_dt.replace(microsecond=0).replace(
                    second=57
                )
            elif start_time_dt.second >= 57 and start_time_dt.second < 60:
                timeslot_endtime_dt = start_time_dt.replace(microsecond=0).replace(
                    second=12
                ) + pd.Timedelta(minutes=1)
            elif start_time_dt.second >= 0 and start_time_dt.second < 12:
                timeslot_endtime_dt = start_time_dt.replace(microsecond=0).replace(
                    second=12
                )
            else:
                pass

            print(start_time_dt)
            print(end_time_dt)

            timeslot_df = df_obstruction_map[
                (df_obstruction_map["timestamp"] >= start_time_dt.timestamp())
                & (df_obstruction_map["timestamp"] < timeslot_endtime_dt.timestamp())
            ]

            if len(timeslot_df) == 0:
                start_time_dt += pd.Timedelta(seconds=15)
                continue

            previous_obstruction_map = timeslot_df.iloc[0]["obstruction_map"]
            previous_obstruction_map = previous_obstruction_map.reshape(123, 123)

            hold_coord = None
            white_pixel_coords = []
            for index, row in timeslot_df.iterrows():
                timestamp_dt = datetime.fromtimestamp(row["timestamp"], tz=timezone.utc)
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

            start_time_dt += pd.Timedelta(seconds=15)


def create_obstruction_map_video(df_obstruction_map, uuid, fps):
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(
        f"{DATA_DIR}/obstruction_map-{uuid}.mp4",
        fourcc,
        fps,
        (123, 123),
    )
    for index, row in df_obstruction_map.iterrows():
        obstruction_map = row["obstruction_map"].reshape(123, 123)
        image_data = (obstruction_map * 255).astype(np.uint8)
        image_data_bgr = cv2.cvtColor(image_data, cv2.COLOR_GRAY2BGR)

        out.write(image_data_bgr)

    out.release()
