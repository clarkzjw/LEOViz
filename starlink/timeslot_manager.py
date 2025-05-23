import logging
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

logger = logging.getLogger(__name__)

class TimeslotManager:
    """Manages timeslot synchronization and data collection."""
    
    TIMESLOT_DURATION = 14
    TIMESLOT_INTERVALS = [(12, 27), (27, 42), (42, 57), (57, 12)]

    @staticmethod
    def get_next_timeslot() -> Tuple[int, datetime]:
        """Get the next timeslot start time."""
        now = datetime.now(timezone.utc)
        current_second = now.second

        for start, end in TimeslotManager.TIMESLOT_INTERVALS:
            if start <= current_second < end:
                next_start = now.replace(microsecond=0, second=end)
                if end < start:  # Handle wrap-around
                    next_start += timedelta(minutes=1)
                return end, next_start

        # If we're between slots, wait for the next one
        next_start = now.replace(microsecond=0, second=12)
        if current_second >= 57:
            next_start += timedelta(minutes=1)
        return 12, next_start