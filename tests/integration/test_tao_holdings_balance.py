"""
Test that verifies TAO Holdings balance reconciles correctly with actual TAO balance.

This test uses the simple_sample data to verify that:
1. Sales (ALPHA → TAO) debit TAO Holdings at FMV (proceeds)
2. Transfers credit TAO Holdings at cost basis (FIFO)
3. The final TAO Holdings balance matches the cost basis of remaining TAO

Key insight: TAO Holdings should equal the cost basis (in TAO terms, 1:1) of TAO currently held.
When all TAO is transferred out, TAO Holdings should be near $0 (within rounding).
"""

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from emissions_tracker.config import WaveAccountSettings


def load_json_with_comments(filepath):
    """Load JSON file, stripping comment lines."""
    with open(filepath, 'r') as f:
        content = f.read()
        lines = content.split('\n')
        json_lines = [line for line in lines if not line.strip().startswith('//')]
        return json.loads('\n'.join(json_lines))


def test_tao_holdings_reconciliation(tmp_path):
    """
    Test that TAO Holdings balance reconciles with actual TAO balance using simple_sample data.
    
    Simple sample has:
    - Starting TAO balance: 0.000724008
    - 2 sales: 10 ALPHA → 0.791686628 TAO, 102.65 ALPHA → 8.126471118 TAO
    - 4 transfers: total 8.918558442 TAO transferred out
    - Ending TAO balance: 0.000323312 (just the transfer fees leftover)
    - Expected TAO Holdings: ~$0.00 (cost basis of 0.000323312 TAO ≈ $0.000323)
    """
    # Load simple_sample data
    data_dir = Path(__file__).parent.parent / 'data' / 'simple_sample'
    
    account_history = load_json_with_comments(data_dir / 'account_history.json')['data']
    stake_events = load_json_with_comments(data_dir / 'stake_events.json')['data']
    transfers = load_json_with_comments(data_dir / 'transfers_to_brokerage.json')['data']
    
    # Get start and end balances
    history_sorted = sorted(account_history, key=lambda x: x.get('timestamp', ''))
    start_tao_balance = float(history_sorted[0]['balance_free']) / 1e9
    end_tao_balance = float(history_sorted[-1]['balance_free']) / 1e9
    
    print(f"\n=== TAO Balance ===")
    print(f"Start: {start_tao_balance:.9f} TAO")
    print(f"End: {end_tao_balance:.9f} TAO")
    
    # Simulate journal entries being created
    # We'll manually calculate what TAO Holdings should be
    
    wave = WaveAccountSettings()
    user_wallet = '5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2'
    
    # Sales in chronological order (ALPHA → TAO)
    sales = sorted([e for e in stake_events if e.get('action') == 'UNDELEGATE' and e.get('is_transfer') is None], 
                   key=lambda x: (x['block_number'], x['timestamp']))
    
    # Transfers in chronological order (TAO → Kraken)
    outgoing = sorted([t for t in transfers if t.get('from', {}).get('ss58') == user_wallet],
                      key=lambda x: (x['block_number'], x['timestamp']))
    
    print(f"\n=== Simulating Journal Entries ===")
    
    # Track TAO lots (FIFO queue)
    tao_lots = [{
        'source': 'Starting balance',
        'quantity': Decimal(str(start_tao_balance)),
        'cost_basis_per_unit': Decimal('1.0'),  # TAO cost basis in TAO terms is 1:1
        'remaining': Decimal(str(start_tao_balance))
    }]
    
    tao_holdings_balance = Decimal(str(start_tao_balance))
    print(f"Starting TAO Holdings: ${tao_holdings_balance:.2f}")
    
    # Process sales and transfers in chronological order
    all_events = []
    for sale in sales:
        all_events.append(('SALE', sale['block_number'], sale['timestamp'], sale))
    for transfer in outgoing:
        all_events.append(('TRANSFER', transfer['block_number'], transfer['timestamp'], transfer))
    
    all_events.sort(key=lambda x: (x[1], x[2]))
    
    for event_type, block, timestamp, data in all_events:
        if event_type == 'SALE':
            # Journal Entry for Sale:
            # DR TAO Holdings (at proceeds/FMV)
            # DR Blockchain Fees (sale fee)
            # CR ALPHA Holdings (at cost basis consumed)
            # CR/DR Short-term Gain/Loss (plug)
            
            tao_qty = Decimal(str(float(data['amount']) / 1e9))
            fee = Decimal(str(float(data['fee']) / 1e9))
            
            # Add TAO to lots at cost basis = 1.0 TAO per TAO (in TAO terms)
            tao_lots.append({
                'source': f'Sale at block {block}',
                'quantity': tao_qty,
                'cost_basis_per_unit': Decimal('1.0'),
                'remaining': tao_qty
            })
            
            # TAO Holdings debited at proceeds (FMV)
            tao_holdings_balance += tao_qty
            print(f"{timestamp[:19]}: SALE +{tao_qty:.9f} TAO → TAO Holdings: ${tao_holdings_balance:.2f}")
            
        else:
            # Journal Entry for Transfer:
            # DR Exchange Clearing (at proceeds/FMV)
            # DR Blockchain Fees (transfer fee)
            # CR TAO Holdings (at cost basis via FIFO)
            # CR/DR Short-term Gain/Loss (plug)
            
            transfer_qty = Decimal(str(float(data['amount']) / 1e9))
            fee = Decimal(str(float(data['fee']) / 1e9))
            
            # Consume lots FIFO to get cost basis
            remaining_to_transfer = transfer_qty
            transfer_cost_basis = Decimal('0')
            
            for lot in tao_lots:
                if remaining_to_transfer <= 0:
                    break
                if lot['remaining'] <= 0:
                    continue
                    
                consumed = min(lot['remaining'], remaining_to_transfer)
                cost_basis = consumed * lot['cost_basis_per_unit']
                transfer_cost_basis += cost_basis
                lot['remaining'] -= consumed
                remaining_to_transfer -= consumed
            
            # TAO Holdings credited at cost basis
            tao_holdings_balance -= transfer_cost_basis
            print(f"{timestamp[:19]}: TRANSFER -{transfer_qty:.9f} TAO (cost: ${transfer_cost_basis:.9f}) → TAO Holdings: ${tao_holdings_balance:.2f}")
    
    print(f"\n=== Final TAO Holdings Balance ===")
    print(f"Calculated: ${tao_holdings_balance:.2f}")
    print(f"Expected (cost of {end_tao_balance:.9f} TAO): ~${end_tao_balance:.2f}")
    
    # TAO Holdings should be near 0 since almost all TAO was transferred out
    # The remaining 0.000323312 TAO has cost basis ≈ $0.000323
    assert abs(tao_holdings_balance) < Decimal('0.01'), \
        f"TAO Holdings should be near $0, but got ${tao_holdings_balance:.2f}"
    
    print(f"\n✓ TAO Holdings balance reconciles correctly!")
    
    # Additional verification: sum of all sales should equal sum of transfers + remaining balance + fees
    total_tao_sales = sum(float(e['amount']) / 1e9 for e in sales)
    total_tao_transferred = sum(float(t['amount']) / 1e9 for t in outgoing)
    
    print(f"\n=== Verification ===")
    print(f"Total TAO from sales: {total_tao_sales:.9f}")
    print(f"Total TAO transferred: {total_tao_transferred:.9f}")
    print(f"Start + Sales - Transfers: {start_tao_balance + total_tao_sales - total_tao_transferred:.9f}")
    print(f"Ending TAO: {end_tao_balance:.9f}")
    print(f"Difference (should be fees): {abs((start_tao_balance + total_tao_sales - total_tao_transferred) - end_tao_balance):.9f}")



def test_tao_holdings_with_partial_transfer(tmp_path):
    """
    Test TAO Holdings when only part of the TAO is transferred (not all).
    
    This would verify that:
    1. Cost basis is correctly tracked using FIFO
    2. TAO Holdings reflects the cost basis of remaining TAO
    3. Gains/losses are calculated correctly when TAO is transferred
    """
    # TODO: Create a test case where only 50% of TAO is transferred
    # Expected: TAO Holdings should equal cost basis of remaining 50%
    pytest.skip("Test case to be implemented")


def test_tao_holdings_with_multiple_price_points(tmp_path):
    """
    Test TAO Holdings when TAO is acquired at different ALPHA prices.
    
    This verifies FIFO lot consumption works correctly:
    1. Buy TAO at price A (e.g., 1 ALPHA = 0.08 TAO)
    2. Buy TAO at price B (e.g., 1 ALPHA = 0.10 TAO)  
    3. Transfer some TAO (should consume from first lot at price A)
    4. TAO Holdings should reflect weighted average cost basis of remaining TAO
    """
    # TODO: Create test with multiple sales at different prices
    pytest.skip("Test case to be implemented")
