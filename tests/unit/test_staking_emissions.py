"""Unit tests for staking emissions processing."""
from curses import use_default_colors
from datetime import datetime
from unittest.mock import patch
import pytest
from emissions_tracker.models import AlphaLot, SourceType

@pytest.fixture
def tracker(contract_tracker):
    """Create tracker instance with properly mocked dependencies."""
    return contract_tracker


@pytest.mark.parametrize("start_date,end_date", [
    (datetime(2025, 11, 1), datetime(2025, 11, 30, 23, 59, 59)),
    (datetime(2025, 11, 1), datetime(2025, 11, 5, 23, 59, 59)),
    (datetime(2025, 10, 1), datetime(2025, 10, 31, 23, 59, 59)),
])
def test_process_staking_emissions(
    tracker, 
    compute_expected_staking_emission_lots,
    start_date, 
    end_date
):
    """Test staking emissions processing for a given date range."""
    # Compute expected emission lots from raw data
    expected_lots: list[AlphaLot] = compute_expected_staking_emission_lots(
        start_date,
        end_date
    )
    
    # Mock _resolve_time_window to return our test date range
    start_time = int(start_date.timestamp())
    end_time = int(end_date.timestamp())
    
    with patch.object(tracker, '_resolve_time_window', return_value=(start_time, end_time)):
        new_lots: list[AlphaLot] = tracker.process_staking_emissions()
    
    # Get actual results from returned lots
    actual_count = len(new_lots)
    expected_count = len(expected_lots)
    
    # Verify counts match
    assert actual_count == expected_count, \
        f"Expected {expected_count} emission lots, got {actual_count}"
    
    # Sort both lists by timestamp for comparison
    expected_lots_sorted = sorted(expected_lots, key=lambda x: x.timestamp)
    actual_lots_sorted = sorted(new_lots, key=lambda x: x.timestamp)
    
    # Compare each lot
    for i, (expected, actual) in enumerate(zip(expected_lots_sorted, actual_lots_sorted)):
        # Verify timestamps are from the same day (not exact match due to different balance snapshot times)
        from datetime import datetime
        expected_date = datetime.fromtimestamp(expected.timestamp).date()
        actual_date = datetime.fromtimestamp(actual.timestamp).date()
        assert actual_date == expected_date, \
            f"Lot {i+1} date mismatch: {actual_date} != {expected_date}"
        
        # Verify alpha quantity matches exactly
        assert abs(actual.alpha - expected.alpha) < 0.001, \
            f"Lot {i+1} ALPHA quantity mismatch: {actual.alpha:.6f} != {expected.alpha:.6f}"
        
        # Verify USD values are positive and non-zero (validates calculation is working)
        assert actual.usd_fmv > 0, \
            f"Lot {i+1} has non-positive USD FMV: {actual.usd_fmv}"
        assert actual.usd_per_alpha > 0, \
            f"Lot {i+1} has non-positive USD per alpha: {actual.usd_per_alpha}"
        
        # Verify USD values are expected values (validates calculation is working)
        assert actual.usd_fmv == expected.usd_fmv, \
            f"Lot {actual.lot_id} does not match expected USD FMV: {expected.usd_fmv}"
        assert actual.usd_per_alpha == expected.usd_per_alpha, \
            f"Lot {actual.lot_id} does not match expected USD per alpha: {expected.usd_per_alpha}"
        
        # Verify source type
        assert actual.source_type == SourceType.STAKING, \
            f"Lot {i+1} should have STAKING source type"
        
        # Verify usd_fmv = alpha * usd_per_alpha (within floating point tolerance)
        expected_fmv = actual.alpha * actual.usd_per_alpha
        assert abs(actual.usd_fmv - expected_fmv) < 0.01, \
            f"Lot {actual.lot_id} FMV consistency check: {actual.usd_fmv} != {actual.alpha} * {actual.usd_per_alpha}"
