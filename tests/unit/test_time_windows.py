import pytest

from emissions_tracker.trackers.bittensor_tracker import BittensorTracker


def test_explicit_start_takes_priority_over_last_timestamp():
    """When start_time is provided and last_timestamp is before it, start_time is used."""
    start, end = BittensorTracker._resolve_time_window(
        "contract income",
        last_timestamp=50,  # Before start_time
        start_time=100,
        end_time=1000,
    )

    assert start == 100
    assert end == 1000


def test_end_time_defaults_to_now_when_not_provided():
    """When end_time is None, it defaults to current time."""
    import time
    before = int(time.time())
    start, end = BittensorTracker._resolve_time_window(
        "contract income",
        last_timestamp=50,  # Before start_time
        start_time=100,
        end_time=None,
    )
    after = int(time.time())

    assert start == 100
    assert before <= end <= after


def test_prior_timestamp_used_when_start_time_missing():
    """When no start_time provided, uses last_timestamp + 1."""
    start, end = BittensorTracker._resolve_time_window(
        "sales",
        last_timestamp=12345,
        start_time=None,
        end_time=20000,
    )

    assert start == 12346
    assert end == 20000


def test_requires_start_time_when_no_prior_timestamp():
    """Raises ValueError when no start_time and no prior timestamp."""
    with pytest.raises(ValueError, match="No previous transfers timestamp found"):
        BittensorTracker._resolve_time_window(
            "transfers", last_timestamp=0, start_time=None, end_time=100
        )
