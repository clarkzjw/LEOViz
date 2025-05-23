# flake8: noqa: E501
import time
import logging
import argparse
import schedule
from typing import Optional, Callable, Any
from datetime import datetime, timedelta
import sys
from pathlib import Path

sys.path.insert(0, str(Path("./starlink-grpc-tools").resolve()))
import starlink_grpc

import config
from latency import icmp_ping
from dish import (
    grpc_status_job,
    grpc_gps_diagnostics_job,
    get_obstruction_map
)
from util import run, load_tle, ensure_directory, get_timestamp_str, get_date_str
from config import print_config
from data_processor import DataProcessor
from satellite_matching_estimation import SatelliteProcessor
from location_provider import LocationProvider
from timeslot_manager import TimeslotManager

logger = logging.getLogger(__name__)

class Scheduler:
    """Manages scheduling of various data collection tasks."""
    
    @staticmethod
    def setup_schedules() -> None:
        """Set up all scheduled tasks."""
        # Latency measurements
        schedule.every(1).hours.at(":00").do(run, icmp_ping).tag("Latency")
        
        # gRPC data collection
        schedule.every(1).hours.at(":00").do(run, get_obstruction_map).tag("gRPC")
        schedule.every(1).hours.at(":00").do(run, grpc_status_job).tag("gRPC")
        
        # TLE data updates
        schedule.every(1).hours.at(":00").do(run, load_tle).tag("TLE")

    @staticmethod
    def setup_mobile_schedule() -> None:
        """Set up additional schedules for mobile installations."""
        schedule.every().hour.at(":00").do(run, grpc_gps_diagnostics_job).tag("gRPC")

    @staticmethod
    def log_schedule_info() -> None:
        """Log information about scheduled tasks."""
        for category in ["Latency", "TLE", "gRPC"]:
            for job in schedule.get_jobs(category):
                logger.info(f"[{category}]: {job.next_run}")

    @staticmethod
    def run_scheduled_tasks() -> None:
        """Run all scheduled tasks."""
        while True:
            schedule.run_pending()
            time.sleep(0.5)

class ConfigManager:
    """Manages configuration settings."""
    
    @staticmethod
    def parse_arguments() -> argparse.Namespace:
        """Parse command line arguments."""
        parser = argparse.ArgumentParser(description="LEOViz | Starlink metrics collection")
        
        parser.add_argument(
            "--run-once",
            action="store_true",
            help="Run once and exit"
        )
        parser.add_argument(
            "--lat",
            type=float,
            required=False,
            help="Dish latitude"
        )
        parser.add_argument(
            "--lon",
            type=float,
            required=False,
            help="Dish longitude"
        )
        parser.add_argument(
            "--alt",
            type=float,
            required=False,
            help="Dish altitude"
        )
        parser.add_argument(
            "--mobile",
            type=bool,
            required=False,
            help="Dish is in mobile mode"
        )
        
        return parser.parse_args()

    @staticmethod
    def configure_mobile_mode(args: argparse.Namespace) -> None:
        """Configure settings for mobile installations."""
        config.MOBILE = bool(args.mobile)
        if not config.MOBILE:
            ConfigManager.configure_static_location(args)

    @staticmethod
    def configure_static_location(args: argparse.Namespace) -> None:
        """Configure settings for static installations."""
        if all([args.lat, args.lon, args.alt]):
            config.LATITUDE = args.lat
            config.LONGITUDE = args.lon
            config.ALTITUDE = args.alt
        else:
            logger.warning(
                "Latitude, Longitude and Altitude not provided. "
                "Won't estimate connected satellites."
            )

def main() -> None:
    """Main entry point for the application."""
    # Parse command line arguments
    args = ConfigManager.parse_arguments()
    
    # Print current configuration
    print_config()
    
    # Configure based on installation type
    ConfigManager.configure_mobile_mode(args)
    
    # Set up scheduled tasks
    Scheduler.setup_schedules()
    
    # Set up mobile-specific schedules if in mobile mode
    if config.MOBILE:
        Scheduler.setup_mobile_schedule()
    
    if args.run_once:
        schedule.run_all()
    else:
        Scheduler.log_schedule_info()
        Scheduler.run_scheduled_tasks()

if __name__ == "__main__":
    main()
