class PriceNotAvailableError(Exception):
    """Raised when price data is not available."""


class LotNotFoundError(Exception):
    """Raised when a lot cannot be found for consumption."""


class InsufficientLotsError(Exception):
    """Raised when there are insufficient lots to cover a disposal."""


class DuplicateExtrinsicError(Exception):
    """Raised when a disposal event with the same extrinsic ID already exists."""
