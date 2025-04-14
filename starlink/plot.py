# flake8: noqa: E501
import os
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
from multiprocessing import Pool

import pandas as pd
import cartopy
import cartopy.crs as ccrs
import cartopy.feature as cfeature

from matplotlib import pyplot as plt
from matplotlib import dates as mdates
from matplotlib import gridspec
from skyfield.api import load

cartopy.config["data_dir"] = os.getenv("CARTOPY_DIR", cartopy.config.get("data_dir"))

from util import load_ping, load_tle_from_file, load_connected_satellites
from pop import get_pop_data
from pprint import pprint

POP_DATA = None

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


def plot_once(row, df_obstruction_map, df_rtt, df_sinr, all_satellites):
    timestamp_str = row["Timestamp"]
    connected_sat_name = row["Connected_Satellite"]
    plot_current = pd.to_datetime(timestamp_str, format="%Y-%m-%d %H:%M:%S%z")

    if connected_sat_name is None:
        return

    print(timestamp_str, connected_sat_name)

    fig = plt.figure(figsize=(20, 10))
    gs0 = gridspec.GridSpec(1, 2, figure=fig, width_ratios=[5, 5])

    gs00 = gs0[0].subgridspec(4, 1)
    axSat = fig.add_subplot(gs00[:3, :], projection=projStereographic)
    # axObstructionMapCumulative = fig.add_subplot(gs00[3, 0])
    axObstructionMapInstantaneous = fig.add_subplot(gs00[3, :])

    frame_type_int = df_obstruction_map["frame_type"].iloc[0]
    if frame_type_int == 0:
        FRAME_TYPE = "UNKNOWN"
    elif frame_type_int == 1:
        FRAME_TYPE = "FRAME_EARTH"
    elif frame_type_int == 2:
        FRAME_TYPE = "FRAME_UT"

    currentObstructionMap = get_obstruction_map_by_timestamp(
        df_obstruction_map, timestamp_str
    )
    axObstructionMapInstantaneous.imshow(
        currentObstructionMap,
        cmap="gray",
    )
    axObstructionMapInstantaneous.set_title(
        f"Instantaneous gRPC obstruction map in current timeslot, frame type: {FRAME_TYPE}"
    )

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
    axFullSINR = fig.add_subplot(gs01[1, :], sharex=axFullRTT)
    axRTT = fig.add_subplot(gs01[2, :])
    axSINR = fig.add_subplot(gs01[3, :], sharex=axRTT)

    axSat.scatter(
        centralLon,
        centralLat,
        transform=projPlateCarree,
        color="green",
        label="Dish",
        s=10,
    )

    try:
        axSat.scatter(
            POP_DATA["lons"],
            POP_DATA["lats"],
            transform=projPlateCarree,
            color="purple",
            label="POP",
            s=60,
            marker="x",
        )

        for lon, lat, name in zip(
            POP_DATA["lons"], POP_DATA["lats"], POP_DATA["names"]
        ):
            axSat.text(
                lon,
                lat,
                name,
                transform=projPlateCarree,
                fontsize=10,
                color="red",
                wrap=True,
                clip_on=True,
            )
    except Exception as e:
        print(str(e))

    axFullRTT.plot(
        df_rtt["timestamp"],
        df_rtt["rtt"],
        color="blue",
        label="RTT",
        linestyle="None",
        markersize=1,
        marker=".",
    )
    axFullRTT.axvline(
        x=plot_current,
        color="red",
        linestyle="--",
    )
    axFullSINR.plot(
        df_sinr["timestamp"],
        df_sinr["sinr"],
        color="blue",
        label="SINR",
        marker="x",
        markersize=2,
    )
    axFullSINR.axvline(
        x=plot_current,
        color="red",
        linestyle="--",
    )
    axFullRTT.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
    axFullRTT.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))

    all_satellites_in_canvas, connected_sat_lat, connected_sat_lon = (
        get_connected_satellite_lat_lon(
            timestamp_str, connected_sat_name, all_satellites
        )
    )
    axSat.scatter(
        connected_sat_lon,
        connected_sat_lat,
        transform=projPlateCarree,
        color="blue",
        label=connected_sat_name,
        s=30,
    )
    axSat.text(
        connected_sat_lon,
        connected_sat_lat,
        connected_sat_name,
        transform=projPlateCarree,
        fontsize=10,
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
        axSat.scatter(
            satellite_lons,
            satellite_lats,
            transform=projPlateCarree,
            color="gray",
            s=30,
        )

    axSat.set_title(
        f"Connected satellite: {connected_sat_name}, timestamp: {timestamp_str}"
    )

    axSat.legend(loc="upper left")

    axFullRTT.set_title("RTT")
    axFullRTT.set_ylabel("RTT (ms)")
    axFullRTT.set_xlim(
        df_rtt.iloc[0]["timestamp"],
        df_rtt.iloc[-1]["timestamp"],
    )

    axFullSINR.set_title("SINR")
    axFullSINR.set_ylabel("SINR (dB)")

    zoom_start = plot_current - pd.Timedelta(minutes=1)
    zoom_end = plot_current + pd.Timedelta(minutes=1)

    df_rtt_zoomed = df_rtt[
        (df_rtt["timestamp"] >= zoom_start) & (df_rtt["timestamp"] <= zoom_end)
    ]
    df_sinr_zoomed = df_sinr[
        (df_sinr["timestamp"] >= zoom_start) & (df_sinr["timestamp"] <= zoom_end)
    ]

    axRTT.plot(
        df_rtt_zoomed["timestamp"],
        df_rtt_zoomed["rtt"],
        color="blue",
        label="RTT",
        linestyle="None",
        markersize=1,
        marker=".",
    )
    axRTT.axvline(
        x=plot_current,
        color="red",
        linestyle="--",
    )
    axSINR.plot(
        df_sinr_zoomed["timestamp"],
        df_sinr_zoomed["sinr"],
        color="blue",
        label="SINR",
        marker="x",
        markersize=4,
    )
    axSINR.axvline(
        x=plot_current,
        color="red",
        linestyle="--",
    )

    axRTT.set_ylim(0, 100)
    axRTT.set_title(f"RTT at {timestamp_str}")
    axRTT.set_ylabel("RTT (ms)")
    axRTT.set_xticklabels([])

    axSINR.set_title(f"SINR at {timestamp_str}")
    axSINR.set_ylabel("SINR (dB)")
    axSINR.set_xticklabels([])

    plt.tight_layout()
    plt.savefig(f"{FIGURE_DIR}/{timestamp_str}.png")
    plt.close()
    print(f"Saved figure for {timestamp_str}")


def plot():
    global projStereographic
    global centralLat
    global centralLon
    global POP_DATA

    for file in [
        OBSTRUCTION_MAP_DATA,
        SINR_DATA,
        LATENCY_DATA,
        TLE_DATA,
    ]:
        if not file.exists():
            print(f"File {file} does not exist.")
            continue

    df_obstruction_map = pd.read_parquet(OBSTRUCTION_MAP_DATA)
    df_sinr = pd.read_csv(SINR_DATA)
    df_rtt = load_ping(LATENCY_DATA)
    all_satellites = load_tle_from_file(TLE_DATA)
    connected_satellites = load_connected_satellites(
        f"{DATA_DIR}/serving_satellite_data-{DATE_TIME}.csv"
    )

    df_rtt["timestamp"] = pd.to_datetime(df_rtt["timestamp"], unit="s", utc=True)
    df_sinr["timestamp"] = pd.to_datetime(df_sinr["timestamp"], unit="s", utc=True)
    df_obstruction_map["timestamp"] = pd.to_datetime(
        df_obstruction_map["timestamp"], unit="s", utc=True
    )

    CPU_COUNT = os.cpu_count() - 1 if os.cpu_count() > 1 else 1
    print(f"Process count: {CPU_COUNT}")

    POP_DATA = get_pop_data(centralLat, centralLon, offsetLat, offsetLon)
    with Pool(CPU_COUNT) as pool:
        for index, row in connected_satellites.iterrows():
            pool.apply_async(
                plot_once,
                args=(row, df_obstruction_map, df_rtt, df_sinr, all_satellites),
            )
        pool.close()
        pool.join()


def get_connected_satellite_lat_lon(timestamp_str, sat_name, all_satellites):
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

    for sat in all_satellites:
        geocentric = sat.at(time_ts)
        subsat = geocentric.subpoint()
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

                all_satellites_in_canvas.append(
                    (subsat.latitude.degrees, subsat.longitude.degrees, sat.name)
                )
    return (
        all_satellites_in_canvas,
        connected_sat_lat,
        connected_sat_lon,
    )


def create_video(fps, filename):
    cmd = f"ffmpeg -framerate {fps} -pattern_type glob -i '{FIGURE_DIR}/*.png' -pix_fmt yuv420p -c:v libx264 {filename}.mp4 -y"
    subprocess.run(cmd, shell=True, check=True)
    print(f"Video created: {filename}.mp4")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LEOViz | Starlink metrics collection")

    parser.add_argument(
        "--dir",
        type=str,
        default="./data",
        help="Directory with measurement results",
    )
    parser.add_argument(
        "--id",
        type=str,
        required=True,
        help="Experiment ID in the data directory, format: YYYY-MM-DD-HH-mm-ss, e.g., 2025-04-13-04-00-00",
    )
    parser.add_argument("--lat", type=float, required=True, help="Dish latitude")
    parser.add_argument("--lon", type=float, required=True, help="Dish longitude")
    parser.add_argument(
        "--fps", type=int, default=5, help="FPS for the generated video"
    )
    args = parser.parse_args()

    if args.lat and args.lon:
        centralLat = args.lat
        centralLon = args.lon
        projStereographic = ccrs.Stereographic(
            central_longitude=centralLon, central_latitude=centralLat
        )

    DATA_DIR = args.dir
    DATE_TIME = args.id
    DATE = "-".join(args.id.split("-")[:3])

    OBSTRUCTION_MAP_DATA = Path(DATA_DIR).joinpath(
        f"grpc/{DATE}/obstruction_map-{DATE_TIME}.parquet"
    )
    SINR_DATA = Path(DATA_DIR).joinpath(f"grpc/{DATE}/GRPC_STATUS-{DATE_TIME}.csv")
    LATENCY_DATA = Path(DATA_DIR).joinpath(f"latency/{DATE}/ping-10ms-{DATE_TIME}.txt")
    TLE_DATA = Path(DATA_DIR).joinpath(f"TLE/{DATE}/starlink-tle-{DATE_TIME}.txt")

    FIGURE_DIR = Path(f"{DATA_DIR}/figures-{DATE_TIME}")
    if not FIGURE_DIR.exists():
        os.makedirs(FIGURE_DIR, exist_ok=True)

    plot()
    create_video(args.fps, f"{DATA_DIR}/starlink-{DATE_TIME}")
