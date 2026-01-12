"""Unit tests for contract income processing."""
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
import pytest

from emissions_tracker.tracker import BittensorEmissionTracker
from emissions_tracker.models import SourceType
from tests.fixtures.mock_config import TEST_PAYOUT_COLDKEY_SS58, TEST_SMART_CONTRACT_SS58, TEST_SUBNET_ID, TEST_TRACKER_SHEET_ID, TEST_VALIDATOR_SS58


def filter_contract_income_events(
    stake_events: list[dict], 
    start_date: datetime, 
    end_date: datetime,
    contract_address: str,
    netuid: int,
    delegate: str,
    nominator: str
) -> list[dict]:
    """Filter stake events for contract income by date range and contract address.
    
    Args:
        stake_events: Raw stake event data
        start_date: Filter events after this date (inclusive)
        end_date: Filter events before this date (inclusive)
        contract_address: Filter events for this contract address
        netuid: Subnet ID to filter by
        delegate: Delegate (hotkey) address to filter by
        nominator: Nominator (coldkey) address to filter by
        
    Returns:
        List of stake events matching contract income criteria
    """
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    
    filtered_events = []
    for event in stake_events:
        # Convert ISO timestamp to Unix timestamp for comparison
        event_timestamp = int(datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00')).timestamp())
        
        # Only include events that match ALL required filters:
        # - Correct netuid, delegate, and nominator (API required params)
        # - Transfer events to the contract address
        # - Within the date range
        if (event['netuid'] == netuid
            and event['delegate']['ss58'] == delegate
            and event['nominator']['ss58'] == nominator
            and event.get('is_transfer') == True
            and event.get('transfer_address', {}).get('ss58') == contract_address
            and start_timestamp <= event_timestamp <= end_timestamp):
            filtered_events.append(event)
    
    return filtered_events


@pytest.fixture
def tracker(mock_taostats_client) -> BittensorEmissionTracker   :
    """Create tracker instance with properly mocked dependencies."""
    # Create tracker normally through __init__
    tracker = BittensorEmissionTracker(
        price_client=mock_taostats_client,
        wallet_client=mock_taostats_client,
        tracking_hotkey=TEST_VALIDATOR_SS58,
        coldkey=TEST_PAYOUT_COLDKEY_SS58,
        sheet_id=TEST_TRACKER_SHEET_ID,
        label="Test Tracker",
        smart_contract_address=TEST_SMART_CONTRACT_SS58,
        income_source=SourceType.CONTRACT
    )
    
    return tracker


@pytest.mark.parametrize("start_date,end_date", [
    (datetime(2025, 11, 1), datetime(2025, 11, 30, 23, 59, 59)),
    (datetime(2025, 10, 1), datetime(2025, 10, 31, 23, 59, 59)),
])
def test_process_contract_income(tracker, raw_stake_events, start_date, end_date):
    """Test contract income processing for a given date range."""
    # Filter raw events for contract income in the date range
    # Must match the same filters that the API client uses
    filtered_events = filter_contract_income_events(
        raw_stake_events,
        start_date,
        end_date,
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
    
    with patch.object(tracker, '_resolve_time_window', return_value=(start_time, end_time)):
        new_lots = tracker.process_contract_income(lookback_days=lookback_days)
    
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
