"""Unit tests for mining tracker emissions processing."""

from datetime import datetime

import pytest

from emissions_tracker.models import AlphaLot, SourceType


@pytest.fixture
def tracker(mining_tracker):
    """Create mining tracker instance with properly mocked dependencies."""
    return mining_tracker


@pytest.mark.parametrize(
    "start_date,end_date",
    [
        # Full December month - Nov 30 baseline available
        (datetime(2025, 12, 1), datetime(2025, 12, 31, 23, 59, 59)),
        # Dec 1-7 subset - first week with Nov 30 baseline
        (datetime(2025, 12, 1), datetime(2025, 12, 7, 23, 59, 59)),
    ],
)
def test_process_mining_staking_emissions(
    tracker, compute_expected_mining_staking_emission_lots, start_date, end_date
):
    """Test mining emissions processing."""
    # Compute expected emission lots from raw mining data
    expected_lots: list[AlphaLot] = compute_expected_mining_staking_emission_lots(
        start_date, end_date
    )

    start_time = int(start_date.timestamp())
    end_time = int(end_date.timestamp())

    new_lots: list[AlphaLot] = tracker.process_mining_emissions(
        start_time=start_time, end_time=end_time
    )

    # Get actual results from returned lots
    actual_count = len(new_lots)
    expected_count = len(expected_lots)

    # Verify we have emission lots
    assert actual_count > 0, "Should have created emission lots"

    # Verify count is close to expected (allow +/-1 for edge cases)
    assert (
        abs(actual_count - expected_count) <= 1
    ), f"Expected approximately {expected_count} emission lots, got {actual_count}"

    # Sort both lists by timestamp for comparison
    expected_lots_sorted = sorted(expected_lots, key=lambda x: x.timestamp)
    actual_lots_sorted = sorted(new_lots, key=lambda x: x.timestamp)

    # Compare each lot
    for i, (expected, actual) in enumerate(
        zip(expected_lots_sorted, actual_lots_sorted)
    ):
        # Verify timestamps are from the same day (not exact match due to different balance snapshot times)
        expected_date = datetime.fromtimestamp(expected.timestamp).date()
        actual_date = datetime.fromtimestamp(actual.timestamp).date()
        assert (
            actual_date == expected_date
        ), f"Lot {i+1} date mismatch: {actual_date} != {expected_date}"

        # Verify alpha quantity matches exactly
        assert (
            abs(actual.alpha - expected.alpha) < 0.001
        ), f"Lot {i+1} ALPHA quantity mismatch: {actual.alpha:.6f} != {expected.alpha:.6f}"

        # Verify USD values are positive and non-zero
        assert (
            actual.usd_fmv > 0
        ), f"Lot {i+1} has non-positive USD FMV: {actual.usd_fmv}"
        assert (
            actual.usd_per_alpha > 0
        ), f"Lot {i+1} has non-positive USD per alpha: {actual.usd_per_alpha}"

        # Verify USD values match expected values
        assert (
            actual.usd_fmv == expected.usd_fmv
        ), f"Lot {actual.lot_id} does not match expected USD FMV: {expected.usd_fmv}"
        assert (
            actual.usd_per_alpha == expected.usd_per_alpha
        ), f"Lot {actual.lot_id} does not match expected USD per alpha: {expected.usd_per_alpha}"

        # Verify source type is MINING
        assert (
            actual.source_type == SourceType.MINING
        ), f"Lot {i+1} should have MINING source type, got {actual.source_type}"

        # Verify usd_fmv = alpha * usd_per_alpha (within floating point tolerance)
        expected_fmv = actual.alpha * actual.usd_per_alpha
        assert (
            abs(actual.usd_fmv - expected_fmv) < 0.01
        ), f"Lot {actual.lot_id} FMV consistency check: {actual.usd_fmv} != {actual.alpha} * {actual.usd_per_alpha}"
