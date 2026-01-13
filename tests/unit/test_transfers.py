"""
Unit tests for process_transfers method.

Tests that TAO → Kraken transfer events are correctly processed,
including TAO lot consumption, cost basis calculation, and realized gains.
"""
from datetime import datetime
from unittest.mock import patch

import pytest

from emissions_tracker.models import CostBasisMethod
from tests.fixtures.mock_config import (
    TEST_BROKER_SS58,
    TEST_PAYOUT_COLDKEY_SS58,
    TEST_TRACKER_SHEET_ID,
)

@pytest.mark.parametrize(
    "emissions_start_date,transfers_start_date,transfers_end_date",
    [
        # Process emissions from Oct 15 to build up ALPHA lots, then test Nov transfers
        (datetime(2025, 10, 15), datetime(2025, 11, 1), datetime(2025, 11, 30, 23, 59, 59)),
    ]
)
def test_process_transfers(
    seed_contract_sheets,
    contract_tracker,
    compute_expected_transfers,
    emissions_start_date,
    transfers_start_date,
    transfers_end_date,
):
    """Test that process_transfers correctly processes TAO → Kraken transfer events."""
    wallet_address = TEST_PAYOUT_COLDKEY_SS58  # Transfers come from coldkey
    
    # Seed historical ALPHA lots into mock sheets BEFORE creating tracker
    seed_contract_sheets(emissions_start_date, transfers_end_date, TEST_TRACKER_SHEET_ID)
    
    # Now create tracker - it will load the pre-seeded data
    tracker = contract_tracker
    
    # Update tracker state to reflect that income has been processed
    tracker.last_staking_income_timestamp = int(transfers_end_date.timestamp())
    tracker.last_contract_income_timestamp = int(transfers_end_date.timestamp())
    tracker.last_sale_timestamp = int(transfers_start_date.timestamp()) - 1
    tracker.last_transfer_timestamp = int(transfers_start_date.timestamp()) - 1
    
    # Compute expected transfers using the TAO lots that were just created
    expected_transfers = compute_expected_transfers(
        sheet_id=TEST_TRACKER_SHEET_ID,
        start_date=transfers_start_date,
        end_date=transfers_end_date,
        wallet_address=wallet_address,
        brokerage_address=TEST_BROKER_SS58,
        cost_basis_method=CostBasisMethod.HIFO
    )
    
    # Process transfers (patch _resolve_time_window)
    with patch.object(tracker, '_resolve_time_window', return_value=(
        int(transfers_start_date.timestamp()),
        int(transfers_end_date.timestamp())
    )):
        tracker.process_sales(lookback_days=(transfers_end_date - transfers_start_date).days + 1)
        actual_transfers = tracker.process_transfers(lookback_days=(transfers_end_date - transfers_start_date).days + 1)
    
    # Verify we got the expected number of transfers
    assert len(actual_transfers) == len(expected_transfers), \
        f"Expected {len(expected_transfers)} transfers, got {len(actual_transfers)}"
    
    # DON'T sort - keep in raw JSON order to match how the mock wallet client returns them
    # This ensures lot consumption order matches between expected and actual
    
    # Compare each transfer field by field
    for i, (actual, expected) in enumerate(zip(actual_transfers, expected_transfers)):
        # Timestamp and block
        assert actual.timestamp == expected['timestamp'], \
            f"Transfer {i+1}: timestamp mismatch"
        assert actual.block_number == expected['block_number'], \
            f"Transfer {i+1}: block_number mismatch"
        
        # Transfer ID
        assert actual.transfer_id == expected['transfer_id'], \
            f"Transfer {i+1}: transfer_id mismatch"
        
        # TAO amount (critical verification)
        assert abs(actual.tao_amount - expected['tao_amount']) < 1e-6, \
            f"Transfer {i+1}: tao_amount mismatch - actual: {actual.tao_amount}, expected: {expected['tao_amount']}"
        
        # TAO price
        assert abs(actual.tao_price_usd - expected['tao_price_usd']) < 0.01, \
            f"Transfer {i+1}: tao_price_usd mismatch"
        
        # USD proceeds (critical verification)
        assert abs(actual.usd_proceeds - expected['usd_proceeds']) < 0.01, \
            f"Transfer {i+1}: usd_proceeds mismatch - actual: ${actual.usd_proceeds:.2f}, expected: ${expected['usd_proceeds']:.2f}"
        
        # Cost basis (critical verification)
        # Should be exact since both tracker and test use same HIFO method
        assert abs(actual.cost_basis - expected['cost_basis']) < 0.01, \
            f"Transfer {i+1}: cost_basis mismatch - actual: ${actual.cost_basis:.2f}, expected: ${expected['cost_basis']:.2f}"
        
        # Realized gain/loss (critical verification)
        # Should be exact since derived from exact cost basis
        assert abs(actual.realized_gain_loss - expected['realized_gain_loss']) < 0.01, \
            f"Transfer {i+1}: realized_gain_loss mismatch - actual: ${actual.realized_gain_loss:.2f}, expected: ${expected['realized_gain_loss']:.2f}"
        
        # Gain type
        assert actual.gain_type.value == expected['gain_type'], \
            f"Transfer {i+1}: gain_type mismatch - actual: {actual.gain_type.value}, expected: {expected['gain_type']}"
        
        # Total outflow and fees
        assert abs(actual.total_outflow_tao - expected['total_outflow_tao']) < 1e-6, \
            f"Transfer {i+1}: total_outflow_tao mismatch"
        assert abs(actual.fee_tao - expected['fee_tao']) < 1e-6, \
            f"Transfer {i+1}: fee_tao mismatch"
        assert abs(actual.fee_cost_basis_usd - expected['fee_cost_basis_usd']) < 0.01, \
            f"Transfer {i+1}: fee_cost_basis_usd mismatch"
        
        # Transaction hash and extrinsic
        assert actual.transaction_hash == expected['transaction_hash'], \
            f"Transfer {i+1}: transaction_hash mismatch"
        
        # Verify consumed lots (critical verification)
        assert len(actual.consumed_tao_lots) == len(expected['consumed_lots']), \
            f"Transfer {i+1}: consumed_lots count mismatch - actual: {len(actual.consumed_tao_lots)}, expected: {len(expected['consumed_lots'])}"
        
        # Verify each consumed lot
        total_consumed_tao = 0.0
        total_consumed_basis = 0.0
        
        for j, (actual_lot, expected_lot) in enumerate(zip(actual.consumed_tao_lots, expected['consumed_lots'])):
            assert actual_lot.lot_id == expected_lot['lot_id'], \
                f"Transfer {i+1}, Lot {j+1}: lot_id mismatch - actual={actual_lot.lot_id}, expected={expected_lot['lot_id']}"
            # Note: consumed_tao_lots uses alpha_consumed field name (from LotConsumption class)
            # but contains TAO values
            assert abs(actual_lot.alpha_consumed - expected_lot['tao_consumed']) < 1e-6, \
                f"Transfer {i+1}, Lot {j+1}: tao_consumed mismatch"
            # Should be exact since both use same HIFO method
            assert abs(actual_lot.cost_basis_consumed - expected_lot['cost_basis_consumed']) < 0.01, \
                f"Transfer {i+1}, Lot {j+1}: cost_basis_consumed mismatch - actual: ${actual_lot.cost_basis_consumed:.2f}, expected: ${expected_lot['cost_basis_consumed']:.2f}"
            
            total_consumed_tao += actual_lot.alpha_consumed
            total_consumed_basis += actual_lot.cost_basis_consumed
        
        # Verify totals match (total outflow = brokerage + fees)
        assert abs(total_consumed_tao - actual.total_outflow_tao) < 1e-6, \
            f"Transfer {i+1}: sum of consumed TAO doesn't match total_outflow_tao"
        assert abs(total_consumed_basis - (actual.cost_basis + actual.fee_cost_basis_usd)) < 0.01, \
            f"Transfer {i+1}: sum of consumed cost basis doesn't match total cost basis"
        
        print(f"✓ Transfer {i+1} verified: {actual.tao_amount:.4f} TAO transferred, "
              f"${actual.usd_proceeds:.2f} proceeds, basis ${actual.cost_basis:.2f}, "
              f"{actual.gain_type.value} {'gain' if actual.realized_gain_loss >= 0 else 'loss'} ${abs(actual.realized_gain_loss):.2f}")
    
    print(f"\n✓ All {len(actual_transfers)} transfers verified successfully")
