# flake8: noqa: E501
import time
import logging
import argparse
import schedule

import config
from latency import icmp_ping
from jobs import JobManager
from util import run, load_tle
from config import print_config

logger = logging.getLogger(__name__)

class Application:
    """Main application class that manages configuration and scheduling."""
    def __init__(self):
        self.config = self._parse_arguments()
        self._configure_application()
        self.job_manager = JobManager()
        self.scheduler = Scheduler(self.config, self.job_manager)
        
    def _parse_arguments(self) -> argparse.Namespace:
        """Parse command line arguments."""
        parser = argparse.ArgumentParser(description="LEOViz | Starlink metrics collection")
        
        # Operation mode arguments
        parser.add_argument("--run-once", action="store_true", help="Run once and exit")
        parser.add_argument("--mobile", action="store_true", help="Dish is in mobile mode")
        parser.add_argument("--duration", type=int, default=5, help="Duration of the application in minutes")
        
        # Location arguments (required for static installations)
        location_group = parser.add_argument_group("Location Settings")
        location_group.add_argument("--lat", type=float, help="Dish latitude")
        location_group.add_argument("--lon", type=float, help="Dish longitude")
        location_group.add_argument("--alt", type=float, help="Dish altitude")
        
        return parser.parse_args()

    def _configure_application(self) -> None:
        """Configure application settings based on arguments."""
        # Configure mobile mode
        config.MOBILE = self.config.mobile
        
        # Configure location for static installations
        if not config.MOBILE:
            if all([self.config.lat, self.config.lon, self.config.alt]):
                config.LATITUDE = self.config.lat
                config.LONGITUDE = self.config.lon
                config.ALTITUDE = self.config.alt
            else:
                logger.warning("Latitude, Longitude and Altitude not provided. Won't estimate connected satellites.")

    def run(self) -> None:
        """Run the application."""
        print_config()
        
        if self.config.run_once:
            schedule.run_all()
        else:
            self.scheduler.log_schedule_info()
            self.scheduler.run_scheduled_tasks()

class Scheduler:
    """Manages scheduling of various data collection tasks."""
    def __init__(self, config, job_manager):
        self.config = config
        self.job_manager = job_manager
        self._setup_schedules()

    def _setup_schedules(self) -> None:
        """Set up all scheduled tasks."""
        # Schedule all tasks to run at the start of each hour
        schedule.every(1).hours.at(":00").do(run, icmp_ping).tag("Latency")
        schedule.every(1).hours.at(":00").do(run, self.job_manager.get_obstruction_map_job).tag("gRPC")
        schedule.every(1).hours.at(":00").do(run, self.job_manager.grpc_status_job).tag("gRPC")
        schedule.every(1).hours.at(":00").do(run, load_tle).tag("TLE")

        # Add mobile-specific tasks
        if self.config.mobile:
            schedule.every().hour.at(":00").do(run, self.job_manager.grpc_gps_diagnostics_job).tag("gRPC")

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
