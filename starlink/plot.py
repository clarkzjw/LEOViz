import os
import math
import logging
import argparse
import subprocess

from copy import deepcopy
from typing import List
from pathlib import Path
from datetime import datetime
from multiprocessing import Pool

import numpy as np
import pandas as pd
import cartopy
import cartopy.crs as ccrs
import cartopy.feature as cfeature

from skyfield.api import wgs84
from matplotlib import pyplot as plt
from matplotlib import dates as mdates
from matplotlib import gridspec
from skyfield.api import load, EarthSatellite

from util import load_ping, load_tle_from_file, load_connected_satellites
from pop import get_pop_data, get_home_pop

cartopy.config["data_dir"] = os.getenv("CARTOPY_DIR", cartopy.config.get("data_dir"))

logger = logging.getLogger(__name__)

POP_DATA = None
HOME_POP = None

centralLat = None
centralLon = None
offsetLon = 20
offsetLat = 10
resolution = "10m"

ts = load.timescale(builtin=True)
projStereographic = None
projPlateCarree = ccrs.PlateCarree()


def get_obstruction_map_by_timestamp(df_obstruction_map, timestamp):
    # 2025-04-12 06:43:14+00:00
    ts = pd.to_datetime(timestamp, format="%Y-%m-%d %H:%M:%S%z")
    closest_idx = (df_obstruction_map["timestamp"] - ts).abs().idxmin()
    closest_row = df_obstruction_map.iloc[closest_idx]
    return closest_row["obstruction_map"].reshape(123, 123)


def get_starlink_generation_by_norad_id(norad_id):
    # Exception sub-ranges known to be v2 Mini within v1.5 range
    v2mini_exceptions = [
        (57290, 57311),
        (56823, 56844),
        (56688, 56709),
        (56287, 56306),
    ]

    def in_ranges(id, ranges):
        return any(start <= id <= end for start, end in ranges)

    # Handle known exceptions first
    if in_ranges(norad_id, v2mini_exceptions):
        return "v2 Mini"

    # Default broader ranges
    if 44714 <= norad_id <= 48696:
        return "v1.0"
    elif 48880 <= norad_id <= 57381:
        return "v1.5"
    elif 57404 <= norad_id:
        return "v2 Mini"

    else:
        return "Unknown"


def rotate_points(x, y, angle):
    """Rotates points by the given angle."""
    x_rot = x * np.cos(angle) - y * np.sin(angle)
    y_rot = x * np.sin(angle) + y * np.cos(angle)
    return x_rot, y_rot


def plot_once(row, df_obstruction_map, df_cumulative_obstruction_map, df_rtt, df_merged, all_satellites):
    # TODO: make it as a configurable parameter to input dish FoV based on different antenna models
    base_radius = 60

    timestamp_str = row["Timestamp"].strftime("%Y-%m-%d %H:%M:%S%z")
    connected_sat_name = row["Connected_Satellite"]
    plot_current = pd.to_datetime(timestamp_str, format="%Y-%m-%d %H:%M:%S%z")

    if connected_sat_name is None:
        return

    print(timestamp_str, connected_sat_name)
    for sat in all_satellites:
        if sat.name == connected_sat_name:
            connected_sat_gen = get_starlink_generation_by_norad_id(sat.model.satnum)
            break

    fig = plt.figure(figsize=(20, 10))
    gs0 = gridspec.GridSpec(1, 2, figure=fig, width_ratios=[5, 5])

    gs00 = gs0[0].subgridspec(4, 2)
    axSat = fig.add_subplot(gs00[:3, :], projection=projStereographic)
    axObstructionMapInstantaneous = fig.add_subplot(gs00[3, 0])
    axObstructionMapCumulative = fig.add_subplot(gs00[3, 1])

    frame_type_int = df_obstruction_map["frame_type"].iloc[0]
    if frame_type_int == 0:
        FRAME_TYPE = "UNKNOWN"
    elif frame_type_int == 1:
        FRAME_TYPE = "FRAME_EARTH"
    elif frame_type_int == 2:
        FRAME_TYPE = "FRAME_UT"

    currentObstructionMap = get_obstruction_map_by_timestamp(df_obstruction_map, timestamp_str)
    axObstructionMapInstantaneous.imshow(currentObstructionMap, cmap="gray")
    axObstructionMapInstantaneous.set_title("Instantaneous satellite trajectory from gRPC")

    cumulativeObstructionMap = get_obstruction_map_by_timestamp(df_cumulative_obstruction_map, timestamp_str)
    axObstructionMapCumulative.imshow(cumulativeObstructionMap, cmap="gray")
    axObstructionMapCumulative.set_title(f"Cumulative obstruction map, frame type: {FRAME_TYPE}")

    axSat.set_extent(
        [
            centralLon - offsetLon,
            centralLon + offsetLon,
            centralLat - offsetLat,
            centralLat + offsetLat,
        ],
        crs=projPlateCarree,
    )
    axSat.coastlines(resolution=resolution, color="black")
    axSat.add_feature(cfeature.STATES, linewidth=0.3, edgecolor="brown")
    axSat.add_feature(cfeature.BORDERS, linewidth=0.5, edgecolor="blue")

    gs01 = gs0[1].subgridspec(4, 1)

    axFullRTT = fig.add_subplot(gs01[0, :])
    axRTT = fig.add_subplot(gs01[1, :])
    axFOV = fig.add_subplot(gs01[2:, :], projection="polar")

    axFOV.set_ylim(0, 90)
    axFOV.set_yticks(np.arange(0, 91, 10))
    axFOV.set_theta_zero_location("N")
    axFOV.set_theta_direction(-1)
    axFOV.grid(True)

    # FOV ellipse and axes
    df_filtered = df_merged[df_merged["timestamp"] == row["Timestamp"]]
    tiltAngleDeg = df_filtered["tiltAngleDeg"].iloc[0]
    boresightAzimuthDeg = df_filtered["boresightAzimuthDeg"].iloc[0]

    center_shift = tiltAngleDeg
    x_radius = base_radius
    y_radius = math.sqrt(base_radius**2 - tiltAngleDeg**2)

    theta = np.linspace(0, 2 * np.pi, 300)
    x = x_radius * np.cos(theta) + center_shift
    y = y_radius * np.sin(theta)
    r = np.sqrt(x**2 + y**2)
    angles = np.arctan2(y, x) + np.deg2rad(boresightAzimuthDeg)
    axFOV.plot(angles, r, "r", label=f"FOV (Base Radius: {base_radius})")

    major_axis_x = np.array([center_shift + x_radius, center_shift - x_radius])
    major_axis_y = np.array([0, 0])
    minor_axis_x = np.array([center_shift, center_shift])
    minor_axis_y = np.array([y_radius, -y_radius])
    major_axis_x_rot, major_axis_y_rot = rotate_points(major_axis_x, major_axis_y, np.deg2rad(boresightAzimuthDeg))
    minor_axis_x_rot, minor_axis_y_rot = rotate_points(minor_axis_x, minor_axis_y, np.deg2rad(boresightAzimuthDeg))
    axFOV.plot(
        np.arctan2(major_axis_y_rot, major_axis_x_rot),
        np.sqrt(major_axis_x_rot**2 + major_axis_y_rot**2),
        "red",
        linestyle="--",
        linewidth=1,
    )
    axFOV.plot(
        np.arctan2(minor_axis_y_rot, minor_axis_x_rot),
        np.sqrt(minor_axis_x_rot**2 + minor_axis_y_rot**2),
        "red",
        linestyle="--",
        linewidth=1,
    )

    axSat.scatter(centralLon, centralLat, transform=projPlateCarree, color="green", label="Dish", s=10)

    try:
        axSat.scatter(
            POP_DATA["lons"],
            POP_DATA["lats"],
            transform=projPlateCarree,
            color="purple",
            label="POP (Red = Home POP)",
            s=60,
            marker="x",
        )

        for lon, lat, name in zip(POP_DATA["lons"], POP_DATA["lats"], POP_DATA["names"]):
            if name == "sttlwax9":
                continue
            color = "green"

            if name == HOME_POP:
                color = "red"

            axSat.text(lon, lat, name, transform=projPlateCarree, fontsize=10, color=color, wrap=True, clip_on=True)
    except Exception as e:
        print(str(e))

    if not df_rtt.empty:
        axFullRTT.plot(
            df_rtt["timestamp"], df_rtt["rtt"], color="blue", label="RTT", linestyle="None", markersize=1, marker="."
        )
        axFullRTT.axvline(
            x=plot_current,
            color="red",
            linestyle="--",
        )
        axFullRTT.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))

    all_satellites_in_canvas, candidate_satellites, connected_sat_lat, connected_sat_lon = (
        get_connected_satellite_lat_lon(timestamp_str, connected_sat_name, all_satellites, df_merged)
    )
    axSat.scatter(
        connected_sat_lon, connected_sat_lat, transform=projPlateCarree, color="blue", label=connected_sat_name, s=30
    )
    axSat.text(
        connected_sat_lon, connected_sat_lat, connected_sat_name, transform=projPlateCarree, fontsize=10, color="red"
    )

    axSat.plot(
        [centralLon, connected_sat_lon],
        [centralLat, connected_sat_lat],
        transform=projPlateCarree,
        color="red",
        linewidth=2,
    )

    if all_satellites_in_canvas:
        satellite_lons = [s[1] for s in all_satellites_in_canvas]
        satellite_lats = [s[0] for s in all_satellites_in_canvas]
        axSat.scatter(satellite_lons, satellite_lats, transform=projPlateCarree, color="gray", s=30)

    print("Candidate satellites:", len(candidate_satellites))
    if candidate_satellites:
        for name, alt, az in candidate_satellites:
            axFOV.scatter(np.radians(az), 90 - alt, color="red", s=10)
            axFOV.text(
                np.radians(az),
                90 - alt,
                name,
                fontsize=8,
                color="black",
                ha="center",
                va="center",
            )

    axSat.set_title(f"Timestamp: {timestamp_str}, Connected satellite: {connected_sat_name}, {connected_sat_gen}")
    axSat.legend(loc="upper left")

    if not df_rtt.empty:
        axFullRTT.set_title("RTT")
        axFullRTT.set_ylabel("RTT (ms)")
        axFullRTT.set_xlim(df_rtt.iloc[0]["timestamp"], df_rtt.iloc[-1]["timestamp"])

    zoom_start = plot_current - pd.Timedelta(minutes=1)
    zoom_end = plot_current + pd.Timedelta(minutes=1)

    if not df_rtt.empty:
        df_rtt_zoomed = df_rtt[(df_rtt["timestamp"] >= zoom_start) & (df_rtt["timestamp"] <= zoom_end)]
        axRTT.plot(
            df_rtt_zoomed["timestamp"],
            df_rtt_zoomed["rtt"],
            color="blue",
            label="RTT",
            linestyle="None",
            markersize=1,
            marker=".",
        )
        axRTT.axvline(x=plot_current, color="red", linestyle="--")
        axRTT.set_ylim(0, 100)
        axRTT.set_title(f"RTT at {timestamp_str}")
        axRTT.set_ylabel("RTT (ms)")
        axRTT.set_xticklabels([])

    plt.tight_layout()
    plt.savefig(f"{FIGURE_DIR}/{timestamp_str}.png")
    plt.close()
    print(f"Saved figure for {timestamp_str}")


def cumulative_obstruction_map(df_obstruction_map: pd.DataFrame):
    df_cumulative = df_obstruction_map.copy()

    if len(df_obstruction_map) > 0:
        current_cumulative = deepcopy(df_obstruction_map.iloc[0]["obstruction_map"])
        df_cumulative.at[0, "obstruction_map"] = current_cumulative

        for index in range(1, len(df_obstruction_map)):
            current_cumulative = (
                current_cumulative.astype(bool) | df_obstruction_map.iloc[index]["obstruction_map"].astype(bool)
            ).astype(int)
            df_cumulative.at[index, "obstruction_map"] = deepcopy(current_cumulative)

    return df_cumulative


def plot():
    global projStereographic
    global centralLat
    global centralLon
    global POP_DATA
    global HOME_POP

    for file in [OBSTRUCTION_MAP_DATA, SINR_DATA, LATENCY_DATA, TLE_DATA]:
        if not file.exists():
            print(f"File {file} does not exist.")
            continue

    df_obstruction_map = pd.read_parquet(OBSTRUCTION_MAP_DATA)
    df_sinr = pd.read_csv(SINR_DATA)
    df_rtt = load_ping(LATENCY_DATA)
    df_processed = pd.read_csv(PROCESSED_DATA)
    all_satellites = load_tle_from_file(TLE_DATA)
    connected_satellites = load_connected_satellites(f"{DATA_DIR}/serving_satellite_data-{DATE_TIME}.csv")

    df_processed["timestamp"] = pd.to_datetime(df_processed["timestamp"]).dt.tz_localize("UTC")
    df_merged = pd.merge(df_processed, connected_satellites, left_on="timestamp", right_on="Timestamp", how="inner")

    centralLat = df_merged["lat"].mean()
    centralLon = df_merged["lon"].mean()
    projStereographic = ccrs.Stereographic(central_longitude=centralLon, central_latitude=centralLat)

    if not df_rtt.empty:
        df_rtt["timestamp"] = pd.to_datetime(df_rtt["timestamp"], unit="s", utc=True)
    if not df_sinr.empty:
        df_sinr["timestamp"] = pd.to_datetime(df_sinr["timestamp"], unit="s", utc=True)
    if not df_obstruction_map.empty:
        df_obstruction_map["timestamp"] = pd.to_datetime(df_obstruction_map["timestamp"], unit="s", utc=True)
        df_cumulative_obstruction_map = cumulative_obstruction_map(df_obstruction_map)

    HOME_POP = get_home_pop()
    CPU_COUNT = os.cpu_count()
    if CPU_COUNT is None or CPU_COUNT <= 2:
        CPU_COUNT = 1
    else:
        CPU_COUNT = CPU_COUNT - 1
    print(f"Process count: {CPU_COUNT}")

    POP_DATA = get_pop_data(centralLat, centralLon, offsetLat, offsetLon)
    with Pool(CPU_COUNT) as pool:
        results = []
        for index, row in connected_satellites.iterrows():
            # plot_once(row, df_obstruction_map, df_rtt, df_sinr, all_satellites)
            result = pool.apply_async(
                plot_once,
                args=(row, df_obstruction_map, df_cumulative_obstruction_map, df_rtt, df_merged, all_satellites),
            )
            results.append(result)

        for result in results:
            try:
                result.get()
            except Exception as e:
                print(f"Error in process: {e}")
                continue

        pool.close()
        pool.join()


def get_connected_satellite_lat_lon(
    timestamp_str, sat_name, all_satellites: List[EarthSatellite], df_merged: pd.DataFrame
):
    timestamp_dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S%z")

    all_satellites_in_canvas = []
    timescale = load.timescale(builtin=True)
    time_ts = timescale.utc(
        timestamp_dt.year,
        timestamp_dt.month,
        timestamp_dt.day,
        timestamp_dt.hour,
        timestamp_dt.minute,
        timestamp_dt.second,
    )

    # filter df_merged for the current timestamp
    df_filtered = df_merged[df_merged["timestamp"] == pd.to_datetime(timestamp_str, utc=True)]
    print(f"Filtered DataFrame length: {len(df_filtered)}")
    row = df_filtered.iloc[0]
    latitude = row["lat"]
    longitude = row["lon"]
    altitude = row["alt"]

    location = wgs84.latlon(latitude, longitude, altitude)

    candidate_satellites = []

    for sat in all_satellites:
        geocentric = sat.at(time_ts)
        subsat = geocentric.subpoint()

        difference = sat - location
        topocentric = difference.at(time_ts)
        alt, az, _ = topocentric.altaz()

        if alt.degrees <= 20:
            continue

        name = str.split(sat.name, "-")[1]
        candidate_satellites.append((name, alt.degrees, az.degrees))

        if sat.name == sat_name:
            connected_sat_lat = subsat.latitude.degrees
            connected_sat_lon = subsat.longitude.degrees
            connected_sat_name = sat.name
            print(connected_sat_lat, connected_sat_lon, connected_sat_name)
        else:
            if (
                subsat.latitude.degrees > centralLat - offsetLat * 1.5
                and subsat.latitude.degrees < centralLat + offsetLat * 1.5
                and subsat.longitude.degrees > centralLon - offsetLon
                and subsat.longitude.degrees < centralLon + offsetLon
            ):

                all_satellites_in_canvas.append((subsat.latitude.degrees, subsat.longitude.degrees, sat.name))
    return (
        all_satellites_in_canvas,
        candidate_satellites,
        connected_sat_lat,
        connected_sat_lon,
    )


def create_video(fps, filename):
    cmd = f"ffmpeg -framerate {fps} -pattern_type glob -i '{FIGURE_DIR}/*.png' -pix_fmt yuv420p -c:v libx264 {filename}.mp4 -y"
    subprocess.run(cmd, shell=True, check=True)
    print(f"Video created: {filename}.mp4")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LEOViz | Starlink metrics collection")

    parser.add_argument("--dir", type=str, default="./data", help="Directory with measurement results")
    parser.add_argument(
        "--id",
        type=str,
        required=True,
        help="Experiment ID in the data directory, format: YYYY-MM-DD-HH-mm-ss, e.g., 2025-04-13-04-00-00",
    )
    parser.add_argument("--fps", type=int, default=5, help="FPS for the generated video")
    args = parser.parse_args()

    DATA_DIR = args.dir
    DATE_TIME = args.id
    DATE = "-".join(args.id.split("-")[:3])

    OBSTRUCTION_MAP_DATA = Path(DATA_DIR).joinpath(f"grpc/{DATE}/obstruction_map-{DATE_TIME}.parquet")
    SINR_DATA = Path(DATA_DIR).joinpath(f"grpc/{DATE}/GRPC_STATUS-{DATE_TIME}.csv")
    PROCESSED_DATA = Path(DATA_DIR).joinpath(f"processed_obstruction-data-{DATE_TIME}.csv")
    LATENCY_DATA = Path(DATA_DIR).joinpath(f"latency/{DATE}/ping-10ms-{DATE_TIME}.txt")
    TLE_DATA = Path(DATA_DIR).joinpath(f"TLE/{DATE}/starlink-tle-{DATE_TIME}.txt")

    FIGURE_DIR = Path(f"{DATA_DIR}/figures-{DATE_TIME}")
    if not FIGURE_DIR.exists():
        os.makedirs(FIGURE_DIR, exist_ok=True)

    plot()
    create_video(args.fps, f"{DATA_DIR}/starlink-{DATE_TIME}")
