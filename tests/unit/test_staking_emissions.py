"""Unit tests for staking emissions processing."""
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
import pytest

from emissions_tracker.tracker import BittensorEmissionTracker
from emissions_tracker.models import SourceType
from tests.utils import (
    filter_balances_by_date_range,
    group_balances_by_day,
    filter_delegation_events,
    group_events_by_day,
    calculate_daily_emissions
)


def compute_expected_staking_emissions(
    balance_history: list[dict],
    delegation_events: list[dict],
    start_date: datetime,
    end_date: datetime
) -> tuple[int, float]:
    """Compute expected staking emissions from raw data.
    
    This replicates the logic in process_staking_emissions:
    - For each balance point (skip first), calculate balance delta
    - Subtract DELEGATE events with is_transfer=True in window
    - Add back UNDELEGATE events in window
    - Sum up all positive emissions
    
    Args:
        balance_history: List of balance snapshots
        delegation_events: List of delegation/undelegation events
        start_date: Start of date range
        end_date: End of date range
        
    Returns:
        Tuple of (count of emission lots, total alpha emitted)
    """
    start_ts = int(start_date.timestamp())
    end_ts = int(end_date.timestamp())
    
    # Filter balance history to date range
    balances = filter_balances_by_date_range(balance_history, start_ts, end_ts)
    
    # Convert and filter delegation events
    events = filter_delegation_events(delegation_events, start_ts, end_ts)
    
    # Group balances by day, keeping only the last balance of each day
    daily_balances = group_balances_by_day(balances)
    
    # Group events by day
    events_by_day = group_events_by_day(events)
    
    # Calculate emissions per day (comparing consecutive days)
    _, emission_count, total_alpha_emitted = calculate_daily_emissions(
        daily_balances, 
        events_by_day,
        price_per_tao=20.0,  # Fixed price for emissions test
        emission_threshold=0.0001
    )
    
    return emission_count, total_alpha_emitted

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
    raw_stake_balance, 
    raw_stake_events, 
    start_date, 
    end_date
):
    """Test staking emissions processing for a given date range."""
    # Compute expected values from raw data
    expected_count, expected_alpha_total = compute_expected_staking_emissions(
        raw_stake_balance,
        raw_stake_events,
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
