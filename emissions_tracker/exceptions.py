class PriceNotAvailableError(Exception):
    """Raised when price data is not available."""
    pass


class LotNotFoundError(Exception):
    """Raised when a lot cannot be found for consumption."""
    pass


class InsufficientLotsError(Exception):
    """Raised when there are insufficient lots to cover a disposal."""
    pass