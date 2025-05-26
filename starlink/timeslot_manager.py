import time
import logging

from typing import Tuple
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


class TimeslotManager:
    """Manages time slots for data collection and processing.

    This class handles the creation and management of time slots for collecting
    and processing Starlink dish data. It provides methods for generating
    time slots and managing their boundaries.

    Attributes:
        None
    """

    def __init__(self):
        """Initialize the TimeslotManager."""
        pass

    @staticmethod
    def get_next_timeslot() -> Tuple[datetime, datetime]:
        """Get the next time slot for data collection.

        Returns:
            Tuple[datetime, datetime]: A tuple containing:
                - datetime: Start time of the next slot
                - datetime: End time of the next slot

        Note:
            - Time slots are 15 seconds long
            - Times are in UTC
            - End time is exclusive
        """
        current_time = datetime.now(timezone.utc)
        slot_start = current_time.replace(second=(current_time.second // 15) * 15, microsecond=0)
        slot_end = slot_start + timedelta(seconds=15)
        return slot_start, slot_end

    @staticmethod
    def get_timeslot_boundaries(timestamp: datetime) -> Tuple[datetime, datetime]:
        """Get the time slot boundaries for a given timestamp.

        Args:
            timestamp: The timestamp to get boundaries for

        Returns:
            Tuple[datetime, datetime]: A tuple containing:
                - datetime: Start time of the slot
                - datetime: End time of the slot

        Note:
            - Time slots are 15 seconds long
            - End time is exclusive
            - Times are in UTC
        """
        slot_start = timestamp.replace(second=(timestamp.second // 15) * 15, microsecond=0)
        slot_end = slot_start + timedelta(seconds=15)
        return slot_start, slot_end

    TIMESLOT_DURATION = 14
    TIMESLOT_INTERVALS = [(12, 27), (27, 42), (42, 57), (57, 12)]

    @staticmethod
    def wait_until_target_time(last_timeslot_second: int) -> int:
        """Wait until the next timeslot and return the next timeslot second."""
        now = datetime.now(timezone.utc)
        next_timeslot_second = None

        if last_timeslot_second == 12:
            next_timeslot_second = 27
        elif last_timeslot_second == 27:
            next_timeslot_second = 42
        elif last_timeslot_second == 42:
            next_timeslot_second = 57
        elif last_timeslot_second == 57:
            next_timeslot_second = 12
            # If we're moving to the next minute
            if now.second >= 57:
                now = now + timedelta(minutes=1)

        target_time = now.replace(microsecond=0).replace(second=next_timeslot_second)
        while datetime.now(timezone.utc) < target_time:
            time.sleep(0.1)

        return next_timeslot_second
