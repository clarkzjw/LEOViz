# flake8: noqa: E501
import time
import logging
import argparse
import schedule

import config
from latency import icmp_ping
from dish import (
    grpc_status_job,
    grpc_gps_diagnostics_job,
    get_obstruction_map
)
from util import run, load_tle
from config import print_config

logger = logging.getLogger(__name__)

class Application:
    """Main application class that manages configuration and scheduling."""
    def __init__(self):
        self.config = self._parse_arguments()
        self._configure_mobile_mode()
        self._configure_static_location()
        self.scheduler = Scheduler(self.config)
        
    def _parse_arguments(self) -> argparse.Namespace:
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
            type=float,required=False,
            help="Dish longitude"
        )
        parser.add_argument("--alt",type=float,required=False,help="Dish altitude")
        parser.add_argument("--mobile",type=bool,required=False,help="Dish is in mobile mode")
        parser.add_argument("--duration",type=int,required=False,help="Duration of the application in seconds",default=3600)
        
        return parser.parse_args()

    def _configure_mobile_mode(self) -> None:
        """Configure settings for mobile installations."""
        config.MOBILE = bool(self.config.mobile)
        if not config.MOBILE:
            self._configure_static_location()

    def _configure_static_location(self) -> None:
        """Configure settings for static installations."""
        if all([self.config.lat, self.config.lon, self.config.alt]):
            config.LATITUDE = self.config.lat
            config.LONGITUDE = self.config.lon
            config.ALTITUDE = self.config.alt
        else:
            logger.warning(
                "Latitude, Longitude and Altitude not provided. "
                "Won't estimate connected satellites."
            )

    def run(self) -> None:
        """Run the application."""
        # Print current configuration
        print_config()
        
        if self.config.run_once:
            schedule.run_all()
        else:
            self.scheduler.log_schedule_info()
            self.scheduler.run_scheduled_tasks()

class Scheduler:
    """Manages scheduling of various data collection tasks."""
    def __init__(self, config):
        self.config = config
        self._setup_schedules()

    def _setup_schedules(self) -> None:
        """Set up all scheduled tasks."""
        # Latency measurements
        schedule.every(1).hours.at(":00").do(run, icmp_ping).tag("Latency")
        
        # gRPC data collection
        schedule.every(1).hours.at(":00").do(run, get_obstruction_map).tag("gRPC")
        schedule.every(1).hours.at(":00").do(run, grpc_status_job).tag("gRPC")
        
        # TLE data updates
        schedule.every(1).hours.at(":00").do(run, load_tle).tag("TLE")

        if self.config.mobile:
            schedule.every().hour.at(":00").do(run, grpc_gps_diagnostics_job).tag("gRPC")

    def log_schedule_info(self) -> None:
        """Log information about scheduled tasks."""
        logger.info("Scheduled tasks:")
        for job in schedule.get_jobs():
            logger.info(f"- {job.tags[0]}: {job.next_run}")

    def run_scheduled_tasks(self) -> None:
        """Run all scheduled tasks."""
        while True:
            schedule.run_pending()
            time.sleep(0.5)

def main() -> None:
    """Main entry point for the application."""
    app = Application()
    app.run()

if __name__ == "__main__":
    main()
