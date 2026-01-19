from abc import abstractmethod
import time
from typing import Optional, Tuple

from emissions_tracker.clients.price import PriceClient
from emissions_tracker.clients.wallet import WalletClientInterface

SECONDS_PER_DAY = 86400
RAO_PER_TAO = 10 ** 9


def _is_rate_limit_error(e: Exception) -> bool:
    """Check if an exception is a Google Sheets rate limit error."""
    error_str = str(e)
    error_type = type(e).__name__
    return '429' in error_str or 'Quota exceeded' in error_str or 'APIError' in error_type

class BittensorTracker:

    def __init__(self, wallet_client: WalletClientInterface, price_client: PriceClient):
        self.wallet_client = wallet_client
        self.price_client = price_client
        self._initialize()

    @abstractmethod
    def _initialize(self):
        ...

    @abstractmethod
    def run(self, start_time: int, end_time: Optional[int] = None):
        ...

    @staticmethod
    def _resolve_time_window(
        label: str,
        last_timestamp: int,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Tuple[Optional[int], Optional[int]]:
        """Determine the (start_time, end_time) timestamps for a processing window.
        
        Args:
            label: Description of what's being processed (for error messages)
            last_timestamp: Last processed timestamp from sheet state
            start_time: Explicit start time (overrides last_timestamp if provided)
            end_time: Explicit end time (defaults to now if not provided)
            
        Returns:
            Tuple of (start_time, end_time) as Unix timestamps, or (None, None) if 
            the requested range has already been fully processed
            
        Raises:
            ValueError: If no start_time provided and no last_timestamp exists
        """
        # End time defaults to now
        resolved_end = end_time if end_time is not None else int(time.time())

        # If explicit start_time provided, check if range overlaps with already processed data
        if start_time is not None:
            # If we've already processed data up to or past the requested end time, skip entirely
            if last_timestamp >= resolved_end:
                return None, None
            
            # If last_timestamp is within the requested range, continue from there
            if last_timestamp >= start_time:
                return last_timestamp + 1, resolved_end
            
            # Otherwise use the explicit start_time
            return start_time, resolved_end

        # Otherwise, continue from last processed timestamp
        if last_timestamp > 0:
            return last_timestamp + 1, resolved_end

        raise ValueError(
            f"No previous {label} timestamp found; please provide --start-date to seed the tracker."
        )