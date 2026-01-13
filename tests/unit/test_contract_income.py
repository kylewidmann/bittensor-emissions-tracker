"""Unit tests for contract income processing."""
from datetime import datetime
from unittest.mock import patch
import pytest

from emissions_tracker.tracker import BittensorEmissionTracker
from emissions_tracker.models import SourceType
from tests.fixtures.mock_config import TEST_PAYOUT_COLDKEY_SS58, TEST_SMART_CONTRACT_SS58, TEST_SUBNET_ID, TEST_TRACKER_SHEET_ID, TEST_VALIDATOR_SS58
from tests.utils import filter_contract_income_events


@pytest.mark.parametrize("start_date,end_date", [
    (datetime(2025, 11, 1), datetime(2025, 11, 30, 23, 59, 59)),
    (datetime(2025, 10, 1), datetime(2025, 10, 31, 23, 59, 59)),
])
def test_process_contract_income(contract_tracker, raw_stake_events, start_date, end_date):
    """Test contract income processing for a given date range."""
    # Filter raw events for contract income in the date range
    # Must match the same filters that the API client uses
    filtered_events = filter_contract_income_events(
        raw_stake_events,
        int(start_date.timestamp()),
        int(end_date.timestamp()),
        contract_address=TEST_SMART_CONTRACT_SS58,
        netuid=TEST_SUBNET_ID,
        delegate=TEST_VALIDATOR_SS58,
        nominator=TEST_PAYOUT_COLDKEY_SS58
    )
    
    # Compute expected totals from raw data (alpha is in RAO)
    expected_count = len(filtered_events)
    expected_alpha_total = sum(int(event['alpha']) for event in filtered_events)
    expected_usd_total = sum(float(event['usd']) for event in filtered_events)
    
    # Mock _resolve_time_window to return our test date range
    start_time = int(start_date.timestamp())
    end_time = int(end_date.timestamp())
    lookback_days = (end_date - start_date).days + 1
    
    with patch.object(contract_tracker, '_resolve_time_window', return_value=(start_time, end_time)):
        new_lots = contract_tracker.process_contract_income(lookback_days=lookback_days)
    
    # Get actual results from returned lots
    actual_count = len(new_lots)
    actual_alpha_total = sum(lot.alpha_quantity for lot in new_lots)
    actual_usd_total = sum(lot.usd_fmv for lot in new_lots)
    
    # Verify totals match
    assert actual_count == expected_count, f"Expected {expected_count} lots, got {actual_count}"
    assert actual_alpha_total == expected_alpha_total, \
        f"Expected {expected_alpha_total} RAO alpha, got {actual_alpha_total}"
    assert abs(actual_usd_total - expected_usd_total) < 0.01, \
        f"Expected ${expected_usd_total} USD, got ${actual_usd_total}"

    assert abs(actual_usd_total - expected_usd_total) < 0.01, \
        f"Expected ${expected_usd_total} USD, got ${actual_usd_total}"
