"""Unit tests for contract income processing."""
from datetime import datetime
from unittest.mock import patch
import pytest

from emissions_tracker.models import AlphaLot, SourceType
from tests.fixtures.mock_config import (
    TEST_PAYOUT_COLDKEY_SS58, 
    TEST_SMART_CONTRACT_SS58, 
    TEST_SUBNET_ID, 
    TEST_VALIDATOR_SS58
)


@pytest.mark.parametrize("start_date,end_date", [
    (datetime(2025, 11, 1), datetime(2025, 11, 5, 23, 59, 59)),
    (datetime(2025, 11, 1), datetime(2025, 11, 30, 23, 59, 59)),
])
def test_process_contract_income(
    contract_tracker,
    compute_expected_contract_income_lots,
    start_date, 
    end_date
):
    """Test contract income processing for a given date range.
    
    This test verifies that the ContractTracker correctly:
    1. Filters delegation events for contract income (is_transfer=True, transfer_address=smart_contract)
    2. Creates ALPHA lots with correct amounts and cost basis
    3. Writes lots to the Income sheet
    4. Returns the created lots
    """
    # Filter raw events for contract income in the date range
    # Must match the same filters that the API client uses
    expected_lots = compute_expected_contract_income_lots(
        int(start_date.timestamp()),
        int(end_date.timestamp()),
        contract_address=TEST_SMART_CONTRACT_SS58,
        netuid=TEST_SUBNET_ID,
        delegate=TEST_VALIDATOR_SS58,
        nominator=TEST_PAYOUT_COLDKEY_SS58
    )
    
    # Compute expected totals from raw data (alpha is in RAO)
    expected_count = len(expected_lots)
    
    # Mock _resolve_time_window to return our test date range
    start_time = int(start_date.timestamp())
    end_time = int(end_date.timestamp())
    lookback_days = (end_date - start_date).days + 1
    
    with patch.object(contract_tracker, '_resolve_time_window', return_value=(start_time, end_time)):
        new_lots: list[AlphaLot] = contract_tracker.process_contract_income(lookback_days=lookback_days)
    
    # Verify count matches
    actual_count = len(new_lots)
    assert actual_count == expected_count, f"Expected {expected_count} lots, got {actual_count}"
    
    # Sort both lists by timestamp for comparison
    expected_sorted = sorted(expected_lots, key=lambda x: x.timestamp)
    actual_sorted = sorted(new_lots, key=lambda x: x.timestamp)
    
    # Compare each lot to expected values
    for i, (expected, actual) in enumerate(zip(expected_sorted, actual_sorted)):
        expected_ts = expected.timestamp
        expected_alpha_rao = expected.alpha_rao
        expected_usd_fmv = expected.usd_fmv
        
        # Verify timestamp matches exactly
        assert actual.timestamp == expected_ts, \
            f"Lot {i+1} timestamp mismatch: {actual.timestamp} != {expected_ts}"
        
        # Verify alpha RAO matches exactly
        assert actual.alpha_rao == expected_alpha_rao, \
            f"Lot {i+1} ALPHA RAO mismatch: {actual.alpha_rao} != {expected_alpha_rao}"
        
        # Verify alpha_rao_remaining equals alpha_rao (lot is open/unused)
        assert actual.alpha_rao_remaining == actual.alpha_rao, \
            f"Lot {i+1} should be fully open: {actual.alpha_rao_remaining} != {actual.alpha_rao}"
        
        # Verify USD FMV matches exactly
        assert actual.usd_fmv == expected_usd_fmv, \
            f"Lot {i+1} USD FMV mismatch: {actual.usd_fmv} != {expected_usd_fmv}"
        
        # Verify source type
        assert actual.source_type == SourceType.CONTRACT, \
            f"Lot {i+1} should have CONTRACT source type"
        
        # Verify lot ID format
        assert actual.lot_id.startswith('ALPHA-'), \
            f"Lot {i+1} ID should start with 'ALPHA-'"
