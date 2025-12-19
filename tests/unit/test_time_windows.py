import pytest

from emissions_tracker.tracker import SECONDS_PER_DAY, BittensorEmissionTracker


def test_lookback_takes_priority_over_last_timestamp():
    now = 10 * SECONDS_PER_DAY
    start, end = BittensorEmissionTracker._resolve_time_window(
        "contract income",
        last_timestamp=999999,
        lookback_days=5,
        now=now,
    )

    assert end == now
    assert start == now - (5 * SECONDS_PER_DAY)


def test_prior_timestamp_used_when_lookback_missing():
    start, end = BittensorEmissionTracker._resolve_time_window(
        "sales",
        last_timestamp=12345,
        lookback_days=None,
        now=20000,
    )

    assert start == 12346
    assert end == 20000


def test_requires_lookback_when_no_prior_timestamp():
    with pytest.raises(ValueError):
        BittensorEmissionTracker._resolve_time_window(
            "transfers", last_timestamp=0, lookback_days=None, now=100
        )


@pytest.mark.parametrize("invalid", [0, -1])
def test_rejects_non_positive_lookback_values(invalid):
    with pytest.raises(ValueError):
        BittensorEmissionTracker._resolve_time_window(
            "income", last_timestamp=123, lookback_days=invalid, now=1000
        )
