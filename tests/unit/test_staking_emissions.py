"""Unit tests for staking emissions processing."""
from datetime import datetime
from unittest.mock import patch
import pytest
from emissions_tracker.models import SourceType

@pytest.fixture
def tracker(contract_tracker):
    """Create tracker instance with properly mocked dependencies."""
    return contract_tracker


@pytest.mark.parametrize("start_date,end_date", [
    (datetime(2025, 11, 1), datetime(2025, 11, 30, 23, 59, 59)),
    (datetime(2025, 10, 1), datetime(2025, 10, 31, 23, 59, 59)),
])
def test_process_staking_emissions(
    tracker, 
    compute_expected_staking_emissions,
    start_date, 
    end_date
):
    """Test staking emissions processing for a given date range."""
    # Compute expected values from raw data
    expected_count, expected_alpha_total = compute_expected_staking_emissions(
        start_date,
        end_date
    )
    
    # Mock _resolve_time_window to return our test date range
    start_time = int(start_date.timestamp())
    end_time = int(end_date.timestamp())
    lookback_days = (end_date - start_date).days + 1
    
    with patch.object(tracker, '_resolve_time_window', return_value=(start_time, end_time)):
        new_lots = tracker.process_staking_emissions(lookback_days=lookback_days)
    
    # Get actual results from returned lots
    actual_count = len(new_lots)
    actual_alpha_total = sum(lot.alpha_quantity for lot in new_lots)
    
    # Verify totals match
    assert actual_count == expected_count, \
        f"Expected {expected_count} emission lots, got {actual_count}"
    
    # Allow small floating point tolerance
    assert abs(actual_alpha_total - expected_alpha_total) < 0.001, \
        f"Expected {expected_alpha_total:.6f} ALPHA emitted, got {actual_alpha_total:.6f}"
    
    # Verify all lots have positive alpha quantity
    for lot in new_lots:
        assert lot.alpha_quantity > 0, f"Lot {lot.lot_id} has non-positive quantity: {lot.alpha_quantity}"
        assert lot.source_type == SourceType.CONTRACT
