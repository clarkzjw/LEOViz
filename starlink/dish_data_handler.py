import time
import logging
from typing import List, Any, Optional, Dict

from grpc_command import GrpcCommand

logger = logging.getLogger(__name__)

class DishDataHandler:
    """Handles dish data collection and processing."""
    
    @staticmethod
    def get_base_columns() -> List[str]:
        """Get base columns common to both mobile and static installations."""
        return [
            "timestamp", "sinr", "popPingLatencyMs", "downlinkThroughputBps",
            "uplinkThroughputBps", "tiltAngleDeg", "boresightAzimuthDeg",
            "boresightElevationDeg", "attitudeEstimationState",
            "attitudeUncertaintyDeg", "desiredBoresightAzimuthDeg",
            "desiredBoresightElevationDeg",
        ]

    @staticmethod
    def get_mobile_columns() -> List[str]:
        """Get additional columns for mobile installations."""
        return ["latitude", "longitude", "altitude", "qScalar", "qX", "qY", "qZ"]

    @staticmethod
    def write_csv_header(csv_writer, mobile: bool) -> None:
        """Write CSV header based on installation type."""
        header = DishDataHandler.get_base_columns()
        if mobile:
            header.extend(DishDataHandler.get_mobile_columns())
        csv_writer.writerow(header)

    @staticmethod
    def extract_base_fields(status: Dict[str, Any], current_time: Optional[float] = None) -> List[Any]:
        """Extract base fields common to both mobile and static installations."""
        alignment = status.get("alignmentStats", {})
        return [
            current_time if current_time is not None else time.time(),
            status.get("phyRxBeamSnrAvg", 0),
            status.get("popPingLatencyMs", 0),
            status.get("downlinkThroughputBps", 0),
            status.get("uplinkThroughputBps", 0),
            alignment.get("tiltAngleDeg", 0),
            alignment.get("boresightAzimuthDeg", 0),
            alignment.get("boresightElevationDeg", 0),
            alignment.get("attitudeEstimationState", ""),
            alignment.get("attitudeUncertaintyDeg", 0),
            alignment.get("desiredBoresightAzimuthDeg", 0),
            alignment.get("desiredBoresightElevationDeg", 0),
        ]

    @staticmethod
    def extract_mobile_fields(location_data: Dict[str, Any], status: Dict[str, Any]) -> List[Any]:
        """Extract additional fields for mobile installations."""
        lla = location_data.get("getLocation", {}).get("lla", {})
        quaternion = status.get("ned2dishQuaternion", {})
        return [
            lla.get("lat", 0), lla.get("lon", 0), lla.get("alt", 0),
            quaternion.get("qScalar", 0), quaternion.get("qX", 0),
            quaternion.get("qY", 0), quaternion.get("qZ", 0),
        ]

    @staticmethod
    def get_sinr_data(status_cmd: GrpcCommand, location_cmd: Optional[GrpcCommand] = None, mobile: bool = False) -> Optional[List[Any]]:
        """Get SINR and location data for the current timestamp."""
        try:
            status_data = status_cmd.execute()
            if not status_data:
                return None

            dish_status = status_data.get("dishGetStatus")
            if not dish_status or "alignmentStats" not in dish_status:
                logger.warning("Missing dishGetStatus or alignmentStats in status data")
                return None

            current_time = time.time()
            row_data = DishDataHandler.extract_base_fields(dish_status, current_time)

            if mobile and location_cmd:
                location_data = location_cmd.execute()
                if location_data:
                    row_data.extend(DishDataHandler.extract_mobile_fields(location_data, dish_status))
                else:
                    logger.error("Failed to get location data")
                    return None

            return row_data
        except Exception as e:
            logger.error(f"Error getting SINR data: {str(e)}")
            return None 