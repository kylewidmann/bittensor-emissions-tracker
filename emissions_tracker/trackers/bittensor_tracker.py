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
    def run(self, lookback_days: int = 1):
        ...

    @staticmethod
    def _resolve_time_window(
        label: str,
        last_timestamp: int,
        lookback_days: Optional[int],
        now: Optional[int] = None,
    ) -> Tuple[int, int]:
        """Determine the (start_time, end_time) timestamps for a processing window."""

        end_time = now if now is not None else int(time.time())

        if lookback_days is not None:
            if lookback_days <= 0:
                raise ValueError(f"lookback for {label} must be positive, got {lookback_days}")
            start_time = end_time - (lookback_days * SECONDS_PER_DAY)
            return start_time, end_time

        if last_timestamp > 0:
            return last_timestamp + 1, end_time

        raise ValueError(
            f"No previous {label} timestamp found; please rerun with --lookback <days> to seed the tracker."
        )