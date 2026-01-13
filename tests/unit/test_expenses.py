"""
Unit tests for process_expenses method.

Tests that ALPHA → expense events (UNDELEGATE with is_transfer=True to non-smart-contract)
are correctly processed, including USD proceeds, cost basis calculation, and realized gains.
"""
import pytest
from datetime import datetime
from unittest.mock import patch

from emissions_tracker.models import CostBasisMethod
from tests.fixtures.mock_config import (
    TEST_TRACKER_SHEET_ID,
)

@pytest.mark.parametrize(
    "emissions_start_date,expenses_start_date,expenses_end_date",
    [
        # Process emissions from Oct 15 to build up ALPHA lots, then test Nov expenses
        (datetime(2025, 10, 15), datetime(2025, 11, 1), datetime(2025, 11, 30, 23, 59, 59)),
    ]
)
def test_process_expenses(
    seed_contract_sheets,
    get_contract_tracker,
    compute_expected_expenses,
    emissions_start_date,
    expenses_start_date,
    expenses_end_date,
):
    """Test that process_expenses correctly processes ALPHA expense events."""
    # Seed historical ALPHA lots into mock sheets BEFORE creating tracker
    # This returns the seeded lots for expected value calculations
    seeded_alpha_lots = seed_contract_sheets(emissions_start_date, expenses_end_date, TEST_TRACKER_SHEET_ID)
    
    # Now create tracker - it will load the pre-seeded data
    tracker = get_contract_tracker()
    
    # Update tracker state to reflect that emissions have been processed
    tracker.last_staking_income_timestamp = int(expenses_end_date.timestamp())
    tracker.last_contract_income_timestamp = int(expenses_end_date.timestamp())
    tracker.last_expense_timestamp = int(expenses_start_date.timestamp()) - 1
    
    # Compute expected expenses using the same lots that were seeded
    expected_expenses = compute_expected_expenses(
        alpha_lots=seeded_alpha_lots,
        start_date=expenses_start_date,
        end_date=expenses_end_date,
        cost_basis_method=CostBasisMethod.HIFO
    )
    
    # Process expenses (only for November)
    expenses_lookback = (expenses_end_date - expenses_start_date).days + 1
    
    with patch.object(tracker, '_resolve_time_window', return_value=(
        int(expenses_start_date.timestamp()),
        int(expenses_end_date.timestamp())
    )):
        actual_expenses = tracker.process_expenses(lookback_days=expenses_lookback)
    
    # Don't sort - keep expenses in the order they were processed
    # Both actual and expected should be in the same order (raw JSON order)
    
    # Verify we got the expected number of expenses
    assert len(actual_expenses) == len(expected_expenses), \
        f"Expected {len(expected_expenses)} expenses, got {len(actual_expenses)}"
    
    # Verify each expense matches expected values
    for i, (actual, expected) in enumerate(zip(actual_expenses, expected_expenses)):
        print(f"\n=== Expense {i+1} ===")
        print(f"Timestamp: {actual.timestamp} (expected: {expected['timestamp']})")
        print(f"Transfer Address: {actual.transfer_address}")
        
        # Timestamp and block
        assert actual.timestamp == expected['timestamp'], \
            f"Expense {i+1}: timestamp mismatch"
        assert actual.block_number == expected['block_number'], \
            f"Expense {i+1}: block_number mismatch"
        
        # Transfer address
        assert actual.transfer_address == expected['transfer_address'], \
            f"Expense {i+1}: transfer_address mismatch"
        
        # ALPHA disposed (critical verification)
        assert abs(actual.alpha_disposed - expected['alpha_disposed']) < 1e-6, \
            f"Expense {i+1}: alpha_disposed mismatch - actual: {actual.alpha_disposed}, expected: {expected['alpha_disposed']}"
        
        # No TAO involved
        assert actual.tao_received == expected['tao_received'], \
            f"Expense {i+1}: tao_received should be 0"
        assert actual.tao_price_usd == expected['tao_price_usd'], \
            f"Expense {i+1}: tao_price_usd should be 0"
        assert actual.created_tao_lot_id == "", \
            f"Expense {i+1}: created_tao_lot_id should be empty"
        
        # USD proceeds (critical verification)
        assert abs(actual.usd_proceeds - expected['usd_proceeds']) < 0.01, \
            f"Expense {i+1}: usd_proceeds mismatch - actual: ${actual.usd_proceeds:.2f}, expected: ${expected['usd_proceeds']:.2f}"
        
        # Cost basis (critical verification)
        assert abs(actual.cost_basis - expected['cost_basis']) < 0.01, \
            f"Expense {i+1}: cost_basis mismatch - actual: ${actual.cost_basis:.2f}, expected: ${expected['cost_basis']:.2f}"
        
        # Realized gain/loss (critical verification)
        assert abs(actual.realized_gain_loss - expected['realized_gain_loss']) < 0.01, \
            f"Expense {i+1}: realized_gain_loss mismatch - actual: ${actual.realized_gain_loss:.2f}, expected: ${expected['realized_gain_loss']:.2f}"
        
        # Gain type
        assert actual.gain_type.value == expected['gain_type'], \
            f"Expense {i+1}: gain_type mismatch - actual: {actual.gain_type.value}, expected: {expected['gain_type']}"
        
        # Fees
        assert abs(actual.network_fee_usd - expected['network_fee_usd']) < 0.01, \
            f"Expense {i+1}: network_fee_usd mismatch"
        
        # Category should be empty (user must fill in)
        assert actual.category == "", \
            f"Expense {i+1}: category should be empty"
        
        # Verify consumed lots (critical verification)
        assert len(actual.consumed_lots) == len(expected['consumed_lots']), \
            f"Expense {i+1}: consumed_lots count mismatch - actual: {len(actual.consumed_lots)}, expected: {len(expected['consumed_lots'])}"
        
        # Verify each consumed lot
        total_consumed_alpha = 0.0
        total_consumed_basis = 0.0
        
        for j, (actual_lot, expected_lot) in enumerate(zip(actual.consumed_lots, expected['consumed_lots'])):
            assert actual_lot.lot_id == expected_lot['lot_id'], \
                f"Expense {i+1}, Lot {j+1}: lot_id mismatch"
            assert abs(actual_lot.alpha_consumed - expected_lot['alpha_consumed']) < 1e-6, \
                f"Expense {i+1}, Lot {j+1}: alpha_consumed mismatch"
            assert abs(actual_lot.cost_basis_consumed - expected_lot['cost_basis_consumed']) < 0.01, \
                f"Expense {i+1}, Lot {j+1}: cost_basis_consumed mismatch"
            
            total_consumed_alpha += actual_lot.alpha_consumed
            total_consumed_basis += actual_lot.cost_basis_consumed
        
        # Verify totals match (critical sanity check)
        assert abs(total_consumed_alpha - actual.alpha_disposed) < 1e-6, \
            f"Expense {i+1}: sum of consumed ALPHA doesn't match alpha_disposed"
        assert abs(total_consumed_basis - actual.cost_basis) < 0.01, \
            f"Expense {i+1}: sum of consumed cost basis doesn't match total cost_basis"
        
        print(f"✓ Expense {i+1} verified: {actual.alpha_disposed:.4f} ALPHA disposed, "
              f"FMV ${actual.usd_proceeds:.2f}, basis ${actual.cost_basis:.2f}, "
              f"{actual.gain_type.value} {'gain' if actual.realized_gain_loss >= 0 else 'loss'} ${abs(actual.realized_gain_loss):.2f}")
    
    print(f"\n✓ All {len(actual_expenses)} expenses verified successfully")