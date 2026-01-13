"""
Unit tests for process_transfers method.

Tests that TAO → Kraken transfer events are correctly processed,
including TAO lot consumption, cost basis calculation, and realized gains.
"""
from datetime import datetime
from typing import List, Dict, Any
from unittest.mock import patch

import pytest

from emissions_tracker.models import CostBasisMethod
from tests.fixtures.mock_config import (
    TEST_BROKER_SS58,
    TEST_PAYOUT_COLDKEY_SS58,
    TEST_SMART_CONTRACT_SS58,
    TEST_TRACKER_SHEET_ID,
    TEST_VALIDATOR_SS58,
    TEST_SUBNET_ID
)


@pytest.fixture()
def seed_sheets(seed_historical_lots):
    """
    Fixture that seeds mock sheets with historical data for transfer tests.
    Returns a function that takes test params and seeds the sheets.
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
    return _seed_sheets


@pytest.fixture
def compute_expected_transfers(
    raw_transfer_events,
    raw_historical_prices,
    mock_sheets,
) -> List[Dict[str, Any]]:
    """
    Compute expected transfers from raw JSON data using TAO lots from the tracker.
    
    This reads the TAO lots that the tracker created (from processing sales)
    and simulates transfer processing to calculate expected values.
    
    Args:
        raw_transfer_data: Raw transfer events
        raw_historical_prices: Historical TAO price data
        mock_sheets: Mock sheets environment to read TAO lots
    
    Returns:
        List of expected transfer dictionaries with computed values
    """

    def _compute_expected_transfers(
        sheet_id: str,
        start_date: datetime,
        end_date: datetime,
        wallet_address: str,
        brokerage_address: str,
        cost_basis_method: CostBasisMethod = CostBasisMethod.FIFO
    ):
        
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        historical_prices = raw_historical_prices
        
        # Helper function to get price for a specific date string (YYYY-MM-DD)
        def price_lookup(date_str: str) -> float:
            return historical_prices[date_str]['price']
        
        # Step 1: Read TAO lots from the tracker's TAO Lots sheet
        # These were created by the tracker when processing sales
        spreadsheet = mock_sheets.client.spreadsheets.get(sheet_id)
        tao_lots_sheet = spreadsheet.worksheet("TAO Lots")
        tao_lot_rows = tao_lots_sheet.get_all_records()
        
        # Convert to dict format for consumption tracking
        tao_lots = []
        for row in tao_lot_rows:
            tao_lots.append({
                'lot_id': row['TAO Lot ID'],
                'timestamp': int(datetime.fromisoformat(row['Date']).timestamp()) if isinstance(row['Date'], str) else row['Date'],
                'tao_quantity': float(row['TAO Quantity']),
                'tao_remaining': float(row['TAO Remaining']),
                'usd_basis': float(row['USD Basis']),
                'usd_per_tao': float(row['USD Basis']) / float(row['TAO Quantity']) if float(row['TAO Quantity']) > 0 else 0,
                'block_number': row.get('Block', 0),
                'extrinsic_id': row.get('Extrinsic ID', '')
            })
        
        # Step 2: Load and process transfer events
        # Step 2: Load and process transfer events
        # Filter transfers to brokerage in date range
        brokerage_transfers = []
        for t in raw_transfer_events:
            if isinstance(t.get('timestamp'), str):
                timestamp = int(datetime.fromisoformat(t['timestamp'].replace('Z', '+00:00')).timestamp())
            else:
                timestamp = t['timestamp']
            
            # Extract to address
            to_addr_data = t.get('to')
            if isinstance(to_addr_data, dict):
                to_addr = to_addr_data.get('ss58', '')
            else:
                to_addr = to_addr_data or ''
            
            # Extract from address
            from_addr_data = t.get('from')
            if isinstance(from_addr_data, dict):
                from_addr = from_addr_data.get('ss58', '')
            else:
                from_addr = from_addr_data or ''
            
            if (to_addr == brokerage_address and 
                from_addr == wallet_address and
                start_ts <= timestamp <= end_ts):
                
                # Convert amounts
                amount_tao = int(t['amount']) / 1e9 if isinstance(t.get('amount'), str) else t['amount']
                fee_tao = int(t['fee']) / 1e9 if isinstance(t.get('fee'), str) and t.get('fee') else 0.0
                
                brokerage_transfers.append({
                    'timestamp': timestamp,
                    'block_number': t['block_number'],
                    'brokerage_amount': amount_tao,
                    'fee_tao': fee_tao,
                    'total_outflow': amount_tao + fee_tao,
                    'transaction_hash': t.get('transaction_hash'),
                    'extrinsic_id': t.get('extrinsic_id')
                })
        
        # DON'T sort - keep in raw JSON order to match how the mock wallet client returns them
        # The tracker processes transfers in the order received from the API
        
        # Sort TAO lots for consumption based on cost basis method
        if cost_basis_method == CostBasisMethod.FIFO:
            sorted_tao_lots = sorted(tao_lots, key=lambda x: x['timestamp'])
        else:  # HIFO
            sorted_tao_lots = sorted(tao_lots, key=lambda x: -x['usd_per_tao'])
        
        # Step 5: Process each transfer chronologically
        expected_transfers = []
        transfer_counter = 1
        
        for transfer in brokerage_transfers:
            brokerage_amount = transfer['brokerage_amount']
            total_outflow = transfer['total_outflow']
            fee_tao = transfer['fee_tao']
            transfer_ts = transfer['timestamp']
            
            # Only consider TAO lots created BEFORE this transfer
            available_lots = [lot for lot in sorted_tao_lots if lot['timestamp'] <= transfer_ts and lot['tao_remaining'] > 0]
            available_tao = sum(lot['tao_remaining'] for lot in available_lots)
            
            # Verify sufficient TAO lots exist - if not, this indicates bad data or preprocessing
            assert available_tao >= total_outflow, (
                f"Insufficient TAO lots for transfer at {datetime.fromtimestamp(transfer_ts)}: "
                f"need {total_outflow:.4f} TAO but only {available_tao:.4f} available. "
                f"This suggests incorrect sales processing or missing TAO lots."
            )
            
            # Consume TAO lots for total outflow (brokerage + fees)
            remaining_to_consume = total_outflow
            consumed_lots = []
            total_cost_basis = 0.0
            oldest_acquisition_ts = None
            
            for lot in available_lots:  # Use available_lots instead of sorted_tao_lots
                if remaining_to_consume <= 0:
                    break
                
                if lot['tao_remaining'] > 0:
                    consume_amount = min(remaining_to_consume, lot['tao_remaining'])
                    
                    # Calculate pro-rata cost basis
                    cost_basis_consumed = (consume_amount / lot['tao_quantity']) * lot['usd_basis']
                    
                    consumed_lots.append({
                        'lot_id': lot['lot_id'],
                        'tao_consumed': consume_amount,
                        'cost_basis_consumed': cost_basis_consumed,
                        'acquisition_timestamp': lot['timestamp']
                    })
                    
                    total_cost_basis += cost_basis_consumed
                    lot['tao_remaining'] -= consume_amount
                    remaining_to_consume -= consume_amount
                    
                    if oldest_acquisition_ts is None:
                        oldest_acquisition_ts = lot['timestamp']
            
            # Allocate cost basis proportionally between brokerage and fees
            cost_basis_for_brokerage = (total_cost_basis * (brokerage_amount / total_outflow)) if total_outflow > 0 else 0.0
            fee_cost_basis = total_cost_basis - cost_basis_for_brokerage
            
            # Get price for this transfer date
            transfer_date = datetime.fromtimestamp(transfer['timestamp']).strftime('%Y-%m-%d')
            tao_price_at_transfer = price_lookup(transfer_date)
            
            # Calculate proceeds (only for brokerage amount)
            usd_proceeds = brokerage_amount * tao_price_at_transfer
            
            # Realized gain/loss
            realized_gain_loss = usd_proceeds - cost_basis_for_brokerage
            
            # Determine gain type
            holding_period_days = (transfer['timestamp'] - oldest_acquisition_ts) / 86400 if oldest_acquisition_ts else 0
            gain_type = 'Long-term' if holding_period_days >= 365 else 'Short-term'
            
            expected_transfer = {
                'transfer_id': f'XFER-{transfer_counter:04d}',
                'timestamp': transfer['timestamp'],
                'block_number': transfer['block_number'],
                'tao_amount': brokerage_amount,
                'tao_price_usd': tao_price_at_transfer,
                'usd_proceeds': usd_proceeds,
                'cost_basis': cost_basis_for_brokerage,
                'realized_gain_loss': realized_gain_loss,
                'gain_type': gain_type,
                'consumed_lots': consumed_lots,
                'transaction_hash': transfer['transaction_hash'],
                'extrinsic_id': transfer['extrinsic_id'],
                'total_outflow_tao': total_outflow,
                'fee_tao': fee_tao,
                'fee_cost_basis_usd': fee_cost_basis
            }
            
            expected_transfers.append(expected_transfer)
            transfer_counter += 1
    
        return expected_transfers

    return _compute_expected_transfers


@pytest.mark.parametrize(
    "emissions_start_date,transfers_start_date,transfers_end_date",
    [
        # Process emissions from Oct 15 to build up ALPHA lots, then test Nov transfers
        (datetime(2025, 10, 15), datetime(2025, 11, 1), datetime(2025, 11, 30, 23, 59, 59)),
    ]
)
def test_process_transfers(
    seed_sheets,
    contract_tracker,
    compute_expected_transfers,
    emissions_start_date,
    transfers_start_date,
    transfers_end_date,
):
    """Test that process_transfers correctly processes TAO → Kraken transfer events."""
    wallet_address = TEST_PAYOUT_COLDKEY_SS58  # Transfers come from coldkey
    
    # Seed historical ALPHA lots into mock sheets BEFORE creating tracker
    seed_sheets(emissions_start_date, transfers_end_date, TEST_TRACKER_SHEET_ID)
    
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
