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
import cartopy.io.img_tiles as cimgt

from skyfield.api import wgs84
from matplotlib import pyplot as plt
from matplotlib import dates as mdates
from matplotlib import gridspec
from skyfield.api import load, EarthSatellite

from util import load_ping, load_tle_from_file, load_connected_satellites
from pop import get_pop_data, get_home_pop

cartopy.config["data_dir"] = os.getenv("CARTOPY_DIR", cartopy.config.get("data_dir"))

logger = logging.getLogger(__name__)

POP_DATA = {}
HOME_POP = None

centralLat = 0.0
centralLon = 0.0
offsetLon = 20
offsetLat = 10
resolution = "10m"

ts = load.timescale(builtin=True)
projStereographic = None
projPlateCarree = ccrs.PlateCarree()

# Add at the top with other globals
worker_satellites = None


def init_worker(tle_file):
    global worker_satellites
    worker_satellites = load_tle_from_file(tle_file)


def get_obstruction_map_by_timestamp(df_obstruction_map, timestamp):
    # 2025-04-12 06:43:14+00:00
    ts = pd.to_datetime(timestamp, format="%Y-%m-%d %H:%M:%S%z")
    closest_idx = (df_obstruction_map["timestamp"] - ts).abs().idxmin()
    closest_row = df_obstruction_map.iloc[closest_idx]
    return closest_row["obstruction_map"].reshape(123, 123)


def get_starlink_generation_by_norad_id(norad_id) -> str:
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


def rotate_points(x, y, angle) -> tuple[float, float]:
    """Rotates points by the given angle."""
    x_rot = x * np.cos(angle) - y * np.sin(angle)
    y_rot = x * np.sin(angle) + y * np.cos(angle)
    return x_rot, y_rot


def get_fov_degree_from_model(model: str) -> float:
    """
    Returns the field of view (FoV) in degrees based on the antenna model.
    # https://olegkutkov.me/forum/index.php?topic=35.0

    # REV1 - Original Starlink "Dishy"
    # rev1_pre_production, rev1_production, rev1_proto3

    # REV2 - First mass production Starlink "Dishy"
    # rev2_proto1, rev2_proto2, rev_proto3, rev2_proto4

    # https://api.starlink.com/public-files/specification_sheet_mini.pdf
    # Mini: 110
    # mini_prod1, mini_prod2, mini_prod3

    # https://api.starlink.com/public-files/Starlink%20Product%20Specifications_Standard.pdf
    # Standard Actuated (rev3): 100 (should be 110 as well?)
    # rev3_proto0, rev3_proto1, rev3_proto2

    # https://api.starlink.com/public-files/specification_sheet_standard.pdf
    # Standard, no actuated (rev4): 110
    # rev4_prod1, rev4_prod2, rev4_prod3, rev4_catapult_proto1

    # https://api.starlink.com/public-files/Starlink%20Product%20Specifications_HighPerformance.pdf
    # https://api.starlink.com/public-files/specification_sheet_flat_high_performance.pdf
    # High Performance: 140
    # Flat High Performance: 140
    # hp1_proto0, hp1_proto1, hp1_proto2

    # https://api.starlink.com/public-files/specification_sheet_enterprise.pdf
    # Enterprise: 110
    """

    if str.startswith(model, "mini_") or str.startswith(model, "rev3_") or str.startswith(model, "rev4_"):
        return 110.0
    elif str.startswith(model, "hp1_"):
        return 140.0
    else:
        return 110.0


def plot_once(row, df_obstruction_map, df_cumulative_obstruction_map, df_rtt, df_merged, is_mobile=False):
    global worker_satellites

    hardwareVersion = df_merged["hardwareVersion"].dropna().iloc[0]
    fov_degree = get_fov_degree_from_model(hardwareVersion)
    base_radius = fov_degree / 2

    timestamp_str = row["Timestamp"].strftime("%Y-%m-%d %H:%M:%S%z")
    connected_sat_name = row["Connected_Satellite"]
    plot_current = pd.to_datetime(timestamp_str, format="%Y-%m-%d %H:%M:%S%z")

    if connected_sat_name is None:
        return

    print(timestamp_str, connected_sat_name)
    for sat in worker_satellites:
        if sat.name == connected_sat_name:
            connected_sat_gen = get_starlink_generation_by_norad_id(sat.model.satnum)
            break

    # Adjust figure size based on mobile flag
    fig_width = 27 if is_mobile else 20
    fig_height = 15 if is_mobile else 11

    fig = plt.figure(figsize=(fig_width, fig_height))
    gs0 = gridspec.GridSpec(2, 2, figure=fig, height_ratios=[2, 1])

    if is_mobile:
        # Mobile layout with street map
        # Top row: Satellite map and RTT/Alt
        gs_top = gs0[0, :].subgridspec(1, 2)
        axSat = fig.add_subplot(gs_top[0, 0], projection=projStereographic)
        gs_right = gs_top[0, 1].subgridspec(4, 1)
        axFullRTT = fig.add_subplot(gs_right[0])
        axRTT = fig.add_subplot(gs_right[1])
        axFullAlt = fig.add_subplot(gs_right[2])
        axAlt = fig.add_subplot(gs_right[3])

        # Bottom row: FOV, Street Map, and Obstruction maps all in one line
        gs_middle = gs0[1, :].subgridspec(1, 4, width_ratios=[1.2, 1, 1, 1])  # Made FOV slightly wider
        axFOV = fig.add_subplot(gs_middle[0, 0], projection="polar")
        axStreetMap = fig.add_subplot(gs_middle[0, 1], projection=ccrs.PlateCarree())
        axStreetMapSat = fig.add_subplot(gs_middle[0, 2], projection=ccrs.PlateCarree())
        gs_obstruction = gs_middle[0, 3].subgridspec(1, 2)
        axObstructionMapInstantaneous = fig.add_subplot(gs_obstruction[0, 0])
        axObstructionMapCumulative = fig.add_subplot(gs_obstruction[0, 1])

        # Set up street map with increased zoom
        df_filtered = df_merged[df_merged["timestamp"] == row["Timestamp"]]
        if not df_filtered.empty:
            current_lat = df_filtered["lat"].iloc[0]
            current_lon = df_filtered["lon"].iloc[0]
            boresight_az = df_filtered["boresightAzimuthDeg"].iloc[0]

            axStreetMap.set_extent(
                [current_lon - 0.005, current_lon + 0.005, current_lat - 0.005, current_lat + 0.005],
                crs=projPlateCarree,
            )
            axStreetMapSat.set_extent(
                [current_lon - 0.005, current_lon + 0.005, current_lat - 0.005, current_lat + 0.005],
                crs=projPlateCarree,
            )

            osm_tiles = cimgt.OSM()
            sat_tiles = cimgt.GoogleTiles(style="satellite")
            zoom = 17
            axStreetMap.add_image(osm_tiles, zoom)
            axStreetMapSat.add_image(sat_tiles, zoom)

            # Plot dish location
            axStreetMap.scatter(current_lon, current_lat, transform=projPlateCarree, color="red", label="Dish", s=50)
            axStreetMapSat.scatter(current_lon, current_lat, transform=projPlateCarree, color="red", label="Dish", s=50)

            # Add direction arrow
            arrow_length = 0.001  # Adjust this value to change arrow length
            # Convert azimuth to radians and adjust for map coordinates
            # In map coordinates, 0 is East and 90 is North, so we need to subtract 90 from the azimuth
            angle_rad = np.radians(90 - boresight_az)
            end_lon = current_lon + arrow_length * np.cos(angle_rad)
            end_lat = current_lat + arrow_length * np.sin(angle_rad)
            axStreetMap.arrow(
                current_lon,
                current_lat,
                end_lon - current_lon,
                end_lat - current_lat,
                transform=projPlateCarree,
                color="red",
                width=0.0001,
                head_width=0.0003,
                head_length=0.0003,
                label="Boresight",
            )
            axStreetMapSat.arrow(
                current_lon,
                current_lat,
                end_lon - current_lon,
                end_lat - current_lat,
                transform=projPlateCarree,
                color="red",
                width=0.0001,
                head_width=0.0003,
                head_length=0.0003,
                label="Boresight",
            )

            axStreetMap.legend(loc="upper right")
            axStreetMap.set_title("Street Map View (OSM Tiles)")

            axStreetMapSat.legend(loc="upper right")
            axStreetMapSat.set_title("Satellite Map View (Google Tiles)")

    else:
        # Top row: Satellite map and RTT/Alt
        gs_top = gs0[0, :].subgridspec(1, 2)
        axSat = fig.add_subplot(gs_top[0, 0], projection=projStereographic)
        gs_right = gs_top[0, 1].subgridspec(4, 1)  # Changed to 4 rows for both current and cumulative
        axFullRTT = fig.add_subplot(gs_right[0])
        axRTT = fig.add_subplot(gs_right[1])
        axFullAlt = fig.add_subplot(gs_right[2])
        axAlt = fig.add_subplot(gs_right[3])

        # Bottom row: FOV and Obstruction maps
        gs_bottom = gs0[1, :].subgridspec(1, 2)
        axFOV = fig.add_subplot(gs_bottom[0, 0], projection="polar")
        gs_obstruction = gs_bottom[0, 1].subgridspec(1, 2)
        axObstructionMapInstantaneous = fig.add_subplot(gs_obstruction[0, 0])
        axObstructionMapCumulative = fig.add_subplot(gs_obstruction[0, 1])

    # Set up satellite map
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

    # Set up FOV plot
    axFOV.set_ylim(0, 90)
    axFOV.set_yticks(np.arange(0, 91, 10))
    axFOV.set_theta_zero_location("N")
    axFOV.set_theta_direction(-1)
    axFOV.grid(True)

    frame_type_int = df_obstruction_map["frame_type"].dropna().iloc[0] if not df_obstruction_map.empty else 0

    if frame_type_int == 0:
        FRAME_TYPE = "UNKNOWN"
    elif frame_type_int == 1:
        FRAME_TYPE = "FRAME_EARTH"
    elif frame_type_int == 2:
        FRAME_TYPE = "FRAME_UT"

    currentObstructionMap = get_obstruction_map_by_timestamp(df_obstruction_map, timestamp_str)
    axObstructionMapInstantaneous.imshow(currentObstructionMap, cmap="gray")
    axObstructionMapInstantaneous.set_title("Instantaneous satellite trajectory")

    cumulativeObstructionMap = get_obstruction_map_by_timestamp(df_cumulative_obstruction_map, timestamp_str)
    axObstructionMapCumulative.imshow(cumulativeObstructionMap, cmap="gray")
    axObstructionMapCumulative.set_title(f"Cumulative obstruction map\nFrame type: {FRAME_TYPE}")

    axFOV.set_ylim(0, 90)
    axFOV.set_yticks(np.arange(0, 91, 10))
    axFOV.set_theta_zero_location("N")
    axFOV.set_theta_direction(-1)
    axFOV.grid(True)

    # FOV ellipse and axes
    df_filtered = df_merged[df_merged["timestamp"] == row["Timestamp"]]
    if df_filtered.empty:
        print(f"No data for timestamp {timestamp_str}")
        return
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
        axFullRTT.axvline(x=plot_current, color="red", linestyle="--")
        axFullRTT.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        # Rotate and align the tick labels so they look better
        # Adjust the layout to prevent label cutoff
        plt.setp(axFullRTT.get_xticklabels(), ha="right")

    all_satellites_in_canvas, candidate_satellites, connected_sat_lat, connected_sat_lon = (
        get_connected_satellite_lat_lon(timestamp_str, connected_sat_name, worker_satellites, df_merged)
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

    if candidate_satellites:
        for name, alt, az in candidate_satellites:
            text_color = "black"
            sat_color = "gray"
            if name == connected_sat_name:
                text_color = "green"
                sat_color = "red"
            axFOV.scatter(np.radians(az), 90 - alt, color=sat_color, s=10)
            axFOV.text(
                np.radians(az),
                90 - alt + 5,
                str.split(name, "-")[1],
                fontsize=8,
                color=text_color,
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

    axFullAlt.plot(df_merged["timestamp"], df_merged["alt"], color="blue", label="Altitude", linewidth=1)
    axFullAlt.axvline(x=plot_current, color="red", linestyle="--")
    axFullAlt.set_ylim(df_merged["alt"].min() * 0.9, df_merged["alt"].max() * 1.1)
    axFullAlt.set_title("Altitude")
    axFullAlt.set_ylabel("Altitude (m)")
    axFullAlt.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    axFullAlt.set_xticklabels([])

    dfAltZoomed = df_merged[(df_merged["timestamp"] >= zoom_start) & (df_merged["timestamp"] <= zoom_end)]
    axAlt.plot(dfAltZoomed["timestamp"], dfAltZoomed["alt"], color="blue", label="Altitude", linewidth=1)
    axAlt.axvline(x=plot_current, color="red", linestyle="--")
    axAlt.set_ylim(dfAltZoomed["alt"].min() * 0.9, dfAltZoomed["alt"].max() * 1.1)
    axAlt.set_title(f"Altitude at {timestamp_str}")
    axAlt.set_ylabel("Altitude (m)")
    axAlt.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    # Rotate and align the tick labels so they look better
    axAlt.tick_params(axis="x", rotation=45)
    # Adjust the layout to prevent label cutoff
    plt.setp(axAlt.get_xticklabels(), ha="right")

    # Adjust FOV size and position
    axFOV.set_position(
        [
            axFOV.get_position().x0 - 0.02,  # Move left
            axFOV.get_position().y0,  # Keep same vertical position
            axFOV.get_position().width * 1.2,  # Make wider
            axFOV.get_position().height * 1.2,
        ]
    )  # Make taller

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
    connected_satellites = load_connected_satellites(f"{DATA_DIR}/serving_satellite_data-{DATE_TIME}.csv")

    df_processed["timestamp"] = pd.to_datetime(df_processed["timestamp"])
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
    with Pool(CPU_COUNT, initializer=init_worker, initargs=(TLE_DATA,)) as pool:
        results = []
        for index, row in connected_satellites.iterrows():
            result = pool.apply_async(
                plot_once,
                args=(row, df_obstruction_map, df_cumulative_obstruction_map, df_rtt, df_merged, IS_MOBILE),
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

        candidate_satellites.append((sat.name, alt.degrees, az.degrees))

        if sat.name == sat_name:
            connected_sat_lat = subsat.latitude.degrees
            connected_sat_lon = subsat.longitude.degrees
        else:
            if (
                subsat.latitude.degrees > centralLat - offsetLat
                and subsat.latitude.degrees < centralLat + offsetLat
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
    parser.add_argument("--mobile", action="store_true", help="Enable mobile layout with street map")
    args = parser.parse_args()

    DATA_DIR = args.dir
    DATE_TIME = args.id
    DATE = "-".join(args.id.split("-")[:3])
    IS_MOBILE = args.mobile

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
