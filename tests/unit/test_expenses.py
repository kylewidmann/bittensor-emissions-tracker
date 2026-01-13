"""
Unit tests for process_expenses method.

Tests that ALPHA → expense events (UNDELEGATE with is_transfer=True to non-smart-contract)
are correctly processed, including USD proceeds, cost basis calculation, and realized gains.
"""
import json
import pytest
from datetime import datetime, timedelta
from typing import List, Dict, Any
from unittest.mock import patch
from pathlib import Path

from emissions_tracker.models import CostBasisMethod
from tests.fixtures.mock_config import (
    TEST_SMART_CONTRACT_SS58,
    TEST_TRACKER_SHEET_ID,
    TEST_PAYOUT_COLDKEY_SS58,
    TEST_VALIDATOR_SS58,
    TEST_SUBNET_ID
)


@pytest.fixture()
def seed_sheets(seed_historical_lots, mock_sheets):
    """
    Fixture that seeds mock sheets with historical data for expense tests.
    Returns a function that takes test params and seeds the sheets.
    Returns the seeded ALPHA lots for use in expected value calculations.
    """
    def _seed_sheets(emissions_start_date, expenses_end_date, sheet_id):
        """Seed the Income sheet with historical ALPHA lots for the test period."""
        # seed_historical_lots uses historical TAO prices, derives opening balance from account_history.json,
        # and includes contract income
        seed_historical_lots(
            sheet_id=sheet_id,
            start_date=emissions_start_date,
            end_date=expenses_end_date,
            include_opening_lot=True,
            contract_address=TEST_SMART_CONTRACT_SS58,
            netuid=TEST_SUBNET_ID,
            delegate=TEST_VALIDATOR_SS58,
            nominator=TEST_PAYOUT_COLDKEY_SS58
        )
        
        # Read back the seeded lots from the Income sheet
        spreadsheet = mock_sheets.client.spreadsheets.get(sheet_id)
        income_sheet = spreadsheet.worksheet("Income")
        lot_rows = income_sheet.get_all_records()
        
        # Convert to dict format for consumption tracking
        alpha_lots = []
        for row in lot_rows:
            alpha_lots.append({
                'lot_id': row['Lot ID'],
                'timestamp': int(datetime.fromisoformat(row['Date']).timestamp()) if isinstance(row['Date'], str) else row['Date'],
                'alpha_quantity': float(row['Alpha Quantity']),
                'alpha_remaining': float(row['Alpha Remaining']),
                'usd_fmv': float(row['USD FMV']),
                'usd_per_alpha': float(row['USD FMV']) / float(row['Alpha Quantity']) if float(row['Alpha Quantity']) > 0 else 0,
            })
        
        return alpha_lots
    
    return _seed_sheets


@pytest.fixture
def compute_expected_expenses(
    raw_stake_events: List[Dict[str, Any]],
):
    """
    Compute expected expenses from raw JSON data using seeded ALPHA lots.
    
    This mimics the tracker logic:
    1. Use the ALPHA lots already seeded (from seed_historical_lots)
    2. Process UNDELEGATE events with is_transfer=True to non-smart-contract as expenses
    3. Consume ALPHA lots using specified method to calculate cost basis
    4. Calculate FMV proceeds and realized gains
    
    Args:
        alpha_lots: The seeded ALPHA lots (from seed_sheets)
        start_date: Start date for processing
        end_date: End date for processing
        cost_basis_method: CostBasisMethod.FIFO or CostBasisMethod.HIFO
    
    Returns:
        List of expected expense dictionaries with computed values
    """

    def _compute_expected_expenses(
        alpha_lots: List[Dict[str, Any]],
        start_date: datetime,
        end_date: datetime,
        cost_basis_method: CostBasisMethod = CostBasisMethod.HIFO
        ):
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        
        # Use the provided ALPHA lots (already seeded with historical data)
        # Make a deep copy to avoid modifying the original
        import copy
        alpha_lots = copy.deepcopy(alpha_lots)
        
        # Sort lots for consumption based on method
        if cost_basis_method == CostBasisMethod.FIFO:
            # FIFO: Sort by timestamp (oldest first)
            sorted_lots = sorted(alpha_lots, key=lambda x: x['timestamp'])
        else:  # HIFO
            # HIFO: Sort by USD per ALPHA descending (highest cost first)
            sorted_lots = sorted(alpha_lots, key=lambda x: -x['usd_per_alpha'])
        
        # Load expense events from the same file (UNDELEGATE with is_transfer=True to non-smart-contract)
        expense_undelegates = []
        for e in raw_stake_events:
            # Convert ISO timestamp string to Unix timestamp if needed
            if isinstance(e.get('timestamp'), str):
                from datetime import datetime
                timestamp = int(datetime.fromisoformat(e['timestamp'].replace('Z', '+00:00')).timestamp())
            else:
                timestamp = e['timestamp']
            
            if (e['action'] == 'UNDELEGATE' and 
                e.get('is_transfer') == True and
                e.get('transfer_address', {}).get('ss58') != TEST_SMART_CONTRACT_SS58 and
                start_ts <= timestamp <= end_ts):
                # Store normalized data
                normalized_event = e.copy()
                normalized_event['timestamp'] = timestamp
                # Convert RAO to ALPHA if needed
                if isinstance(e.get('alpha'), str):
                    normalized_event['alpha'] = int(e['alpha']) / 1e9
                else:
                    normalized_event['alpha'] = e['alpha']
                # Convert string USD to float if needed
                if isinstance(e.get('usd'), str):
                    normalized_event['usd'] = float(e['usd'])
                # Convert string fee to float if needed
                if isinstance(e.get('fee'), str):
                    normalized_event['fee'] = int(e['fee']) / 1e9 if e['fee'] != "0" else 0.0
                
                expense_undelegates.append(normalized_event)
        
        # DON'T sort - keep in raw JSON order to match how the mock wallet client returns them
        # This ensures lot consumption order matches between expected and actual
        
        # Process each expense
        expected_expenses = []
        
        for event in expense_undelegates:
            alpha_disposed = event['alpha']
            
            # Extract transfer address
            transfer_address_data = event.get('transfer_address')
            if isinstance(transfer_address_data, dict):
                transfer_address = transfer_address_data.get('ss58', '')
            else:
                transfer_address = transfer_address_data or ''
            
            # Calculate USD proceeds based on ALPHA's FMV
            usd_proceeds = event.get('usd', 0.0)
            
            # Network fee (in USD if available, otherwise 0)
            fee_usd = event.get('fee_usd', 0.0)
            if not fee_usd and event.get('fee'):
                # If fee is in RAO, convert to ALPHA then to USD
                fee_alpha = event['fee'] if isinstance(event['fee'], float) else float(event['fee'])
                alpha_price_usd = event.get('alpha_price_in_usd', 0.0)
                if isinstance(alpha_price_usd, str):
                    alpha_price_usd = float(alpha_price_usd)
                if alpha_price_usd:
                    fee_usd = fee_alpha * alpha_price_usd
            
            # Consume ALPHA lots
            remaining_to_consume = alpha_disposed
            consumed_lots = []
            total_cost_basis = 0.0
            oldest_acquisition_ts = None
            
            for lot in sorted_lots:
                if remaining_to_consume <= 0:
                    break
                
                if lot['alpha_remaining'] > 0:
                    consume_amount = min(remaining_to_consume, lot['alpha_remaining'])
                    
                    # Calculate pro-rata cost basis
                    cost_basis_consumed = (consume_amount / lot['alpha_quantity']) * lot['usd_fmv']
                    
                    consumed_lots.append({
                        'lot_id': lot['lot_id'],
                        'alpha_consumed': consume_amount,
                        'cost_basis_consumed': cost_basis_consumed,
                        'acquisition_timestamp': lot['timestamp']
                    })
                    
                    total_cost_basis += cost_basis_consumed
                    lot['alpha_remaining'] -= consume_amount
                    remaining_to_consume -= consume_amount
                    
                    if oldest_acquisition_ts is None:
                        oldest_acquisition_ts = lot['timestamp']
            
            if remaining_to_consume > 0:
                raise ValueError(f"Insufficient ALPHA lots: need {alpha_disposed}, only have {alpha_disposed - remaining_to_consume}")
            
            # Determine gain type based on holding period (1 year)
            holding_period_days = (event['timestamp'] - oldest_acquisition_ts) / 86400
            gain_type = 'Long-term' if holding_period_days >= 365 else 'Short-term'
            
            # Calculate realized gain/loss: FMV - cost basis - fees
            realized_gain_loss = usd_proceeds - total_cost_basis - fee_usd
            
            expected_expense = {
                'timestamp': event['timestamp'],
                'block_number': event['block_number'],
                'transfer_address': transfer_address,
                'alpha_disposed': alpha_disposed,
                'tao_received': 0.0,  # No TAO involved in direct ALPHA transfers
                'tao_price_usd': 0.0,
                'usd_proceeds': usd_proceeds,
                'cost_basis': total_cost_basis,
                'realized_gain_loss': realized_gain_loss,
                'gain_type': gain_type,
                'network_fee_tao': 0.0,
                'network_fee_usd': fee_usd,
                'consumed_lots': consumed_lots,
                'extrinsic_id': event.get('extrinsic_id'),
            }
            
            expected_expenses.append(expected_expense)
        
        return expected_expenses

    return _compute_expected_expenses


@pytest.mark.parametrize(
    "emissions_start_date,expenses_start_date,expenses_end_date",
    [
        # Process emissions from Oct 15 to build up ALPHA lots, then test Nov expenses
        (datetime(2025, 10, 15), datetime(2025, 11, 1), datetime(2025, 11, 30, 23, 59, 59)),
    ]
)
def test_process_expenses(
    seed_sheets,
    get_contract_tracker,
    compute_expected_expenses,
    emissions_start_date,
    expenses_start_date,
    expenses_end_date,
):
    """Test that process_expenses correctly processes ALPHA expense events."""
    # Seed historical ALPHA lots into mock sheets BEFORE creating tracker
    # This returns the seeded lots for expected value calculations
    seeded_alpha_lots = seed_sheets(emissions_start_date, expenses_end_date, TEST_TRACKER_SHEET_ID)
    
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