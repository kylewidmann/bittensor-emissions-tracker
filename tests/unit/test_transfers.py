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
)

@pytest.mark.parametrize(
    "seed_date,start_date,end_date",
    [
        # Process emissions from Oct 15 to build up ALPHA lots, then test Nov transfers
        (datetime(2025, 10, 15), datetime(2025, 11, 1), datetime(2025, 11, 30, 23, 59, 59)),
    ]
)
def test_process_transfers(
    seed_contract_sheets,
    get_contract_tracker,
    compute_expected_transfers,
    seed_date,
    start_date,
    end_date,
):
    """Test that process_transfers correctly processes TAO → Kraken transfer events."""    
    # Seed historical ALPHA lots into mock sheets BEFORE creating tracker
    seed_contract_sheets(seed_date, end_date)
    
    # Now create tracker - it will load the pre-seeded data
    tracker = get_contract_tracker()

    # Process sales first to create TAO lots
    with patch.object(tracker, '_resolve_time_window', return_value=(
        int(start_date.timestamp()),
        int(end_date.timestamp())
    )):
        tracker.process_alpha_sales(lookback_days=(end_date - start_date).days + 1)
    

    # Now compute expected transfers using the TAO lots that were just created
    # Use same date range as tracker processed to ensure consistency
    # Pass seed_date as opening_lot_date so the opening TAO lot matches what was seeded
    expected_transfers = compute_expected_transfers(
        start_date=start_date,
        end_date=end_date,
        wallet_address=TEST_PAYOUT_COLDKEY_SS58,
        brokerage_address=TEST_BROKER_SS58,
        cost_basis_method=CostBasisMethod.HIFO,
        opening_lot_date=seed_date,
    )
    
    # Process transfers (with same time window patch)
    with patch.object(tracker, '_resolve_time_window', return_value=(
        int(start_date.timestamp()),
        int(end_date.timestamp())
    )):
        actual_transfers = tracker.process_tao_transfers(lookback_days=(end_date - start_date).days + 1)
    
    # Verify we got the expected number of transfers
    assert len(actual_transfers) == len(expected_transfers), \
        f"Expected {len(expected_transfers)} transfers, got {len(actual_transfers)}"
    
    # DON'T sort - keep in raw JSON order to match how the mock wallet client returns them
    # This ensures lot consumption order matches between expected and actual
    
    # Compare each transfer field by field
    for i, (actual, expected) in enumerate(zip(actual_transfers, expected_transfers)):
        # Timestamp and block
        assert actual.timestamp == expected.timestamp, \
            f"Transfer {i+1}: timestamp mismatch"
        assert actual.block_number == expected.block_number, \
            f"Transfer {i+1}: block_number mismatch"
        
        # Transfer ID
        assert actual.transfer_id == expected.transfer_id, \
            f"Transfer {i+1}: transfer_id mismatch"
        
        # TAO amount (critical verification)
        assert actual.tao_amount == expected.tao_amount, \
            f"Transfer {i+1}: tao_amount mismatch - actual: {actual.tao_amount}, expected: {expected.tao_amount}"
        
        # TAO price
        assert actual.tao_price_usd == expected.tao_price_usd, \
            f"Transfer {i+1}: tao_price_usd mismatch - actual: {actual.tao_price_usd}, expected: {expected.tao_price_usd}"
        
        # USD proceeds (critical verification)
        assert actual.usd_proceeds == expected.usd_proceeds, \
            f"Transfer {i+1}: usd_proceeds mismatch - actual: ${actual.usd_proceeds:.2f}, expected: ${expected.usd_proceeds:.2f}"
        
        # Cost basis (critical verification)
        assert actual.cost_basis == expected.cost_basis, \
            f"Transfer {i+1}: cost_basis mismatch - actual: ${actual.cost_basis:.2f}, expected: ${expected.cost_basis:.2f}"
        
        # Realized gain/loss (critical verification)
        assert actual.realized_gain_loss == expected.realized_gain_loss, \
            f"Transfer {i+1}: realized_gain_loss mismatch - actual: ${actual.realized_gain_loss:.2f}, expected: ${expected.realized_gain_loss:.2f}"
        
        # Gain type
        assert actual.gain_type == expected.gain_type, \
            f"Transfer {i+1}: gain_type mismatch - actual: {actual.gain_type}, expected: {expected.gain_type}"
        
        # Total outflow and fees
        assert actual.total_outflow_tao == expected.total_outflow_tao, \
            f"Transfer {i+1}: total_outflow_tao mismatch - actual: {actual.total_outflow_tao}, expected: {expected.total_outflow_tao}"
        assert actual.fee_tao == expected.fee_tao, \
            f"Transfer {i+1}: fee_tao mismatch - actual: {actual.fee_tao}, expected: {expected.fee_tao}"
        assert actual.fee_cost_basis_usd == expected.fee_cost_basis_usd, \
            f"Transfer {i+1}: fee_cost_basis_usd mismatch - actual: {actual.fee_cost_basis_usd}, expected: {expected.fee_cost_basis_usd}"
        
        # Transaction hash and extrinsic
        assert actual.transaction_hash == expected.transaction_hash, \
            f"Transfer {i+1}: transaction_hash mismatch - actual: {actual.transaction_hash}, expected: {expected.transaction_hash}"
        
        # Verify consumed lots totals (instead of exact lot breakdown due to potential opening lots)
        total_consumed_tao = sum(lot.tao_consumed for lot in actual.consumed_tao_lots)
        total_consumed_basis = sum(lot.cost_basis_consumed for lot in actual.consumed_tao_lots)
        
        # Verify totals match (total outflow = brokerage + fees) - use approx for floating point
        assert abs(total_consumed_tao - actual.total_outflow_tao) < 1e-9, \
            f"Transfer {i+1}: sum of consumed TAO doesn't match total_outflow_tao - {total_consumed_tao} vs {actual.total_outflow_tao}"
        # cost_basis is the TOTAL consumed basis (includes fee portion); fee_cost_basis_usd is carved out for reporting
        assert abs(total_consumed_basis - actual.cost_basis) < 1e-9, \
            f"Transfer {i+1}: sum of consumed cost basis doesn't match cost_basis - {total_consumed_basis} vs {actual.cost_basis}"
        
        print(f"✓ Transfer {i+1} verified: {actual.tao_amount:.4f} TAO transferred, "
              f"${actual.usd_proceeds:.2f} proceeds, basis ${actual.cost_basis:.2f}, "
              f"{actual.gain_type.value} {'gain' if actual.realized_gain_loss >= 0 else 'loss'} ${abs(actual.realized_gain_loss):.2f}")
    
    print(f"\n✓ All {len(actual_transfers)} transfers verified successfully")
