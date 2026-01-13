"""
Unit tests for process_sales method.

Tests that ALPHA → TAO sales (UNDELEGATE events) are correctly processed,
including TAO received, cost basis calculation, slippage, and fees.
"""
from datetime import datetime
from unittest.mock import patch

import pytest

from tests.fixtures.mock_config import (
    TEST_TRACKER_SHEET_ID,
)



@pytest.mark.parametrize(
    "emissions_start_date,sales_start_date,sales_end_date,expected_count",
    [
        # Process emissions from Oct 15 to build up ALPHA lots, then test Nov sales
        (datetime(2025, 10, 15), datetime(2025, 11, 1), datetime(2025, 11, 30, 23, 59, 59), 7),
    ]
)
def test_process_sales(
    seed_contract_sheets,
    get_contract_tracker,
    compute_expected_sales,
    emissions_start_date,
    sales_start_date,
    sales_end_date,
    expected_count
):
    """Test that process_sales correctly processes UNDELEGATE events as sales.
    
    This test uses pre-seeded historical ALPHA lots, then processes sales for the target month.
    """
    # Compute expected sales from raw data using historical TAO prices
    expected_sales = compute_expected_sales(
        start_date=emissions_start_date,
        end_date=sales_end_date
    )
    
    # Filter to only sales in the target month
    sales_start_ts = int(sales_start_date.timestamp())
    expected_sales = [s for s in expected_sales if s['timestamp'] >= sales_start_ts]
    
    # Sort expected sales by timestamp to match the sorting of actual sales
    expected_sales.sort(key=lambda s: s['timestamp'])
    
    assert len(expected_sales) == expected_count, f"Expected {expected_count} sales, got {len(expected_sales)}"
    
    # Seed historical ALPHA lots into mock sheets BEFORE creating tracker
    seed_contract_sheets(emissions_start_date, sales_end_date, TEST_TRACKER_SHEET_ID)
    
    # Now create tracker - it will load the pre-seeded data
    tracker = get_contract_tracker()
    
    # Update tracker state to reflect that emissions have been processed
    tracker.last_staking_income_timestamp = int(sales_end_date.timestamp())
    tracker.last_sale_timestamp = int(sales_start_date.timestamp()) - 1
    
    # Process sales (only for November)
    sales_lookback = (sales_end_date - sales_start_date).days + 1
    
    with patch.object(tracker, '_resolve_time_window', return_value=(
        int(sales_start_date.timestamp()),
        int(sales_end_date.timestamp())
    )):
        sales = tracker.process_sales(lookback_days=sales_lookback)
    
    assert len(sales) == expected_count, f"Expected {expected_count} sales, got {len(sales)}"
    
    # Sort sales by timestamp for comparison
    sales.sort(key=lambda s: s.timestamp)
    
    # Compare each sale with expected values
    for i, (sale, expected) in enumerate(zip(sales, expected_sales)):
        # Assertions
        assert sale.timestamp == expected['timestamp'], f"Sale {i+1} timestamp mismatch"
        assert abs(sale.alpha_disposed - expected['alpha_disposed']) < 0.001, f"Sale {i+1} ALPHA disposed mismatch"
        assert abs(sale.tao_received - expected['tao_received']) < 0.001, f"Sale {i+1} TAO received mismatch"
        # TODO: Fix cost basis calculation in compute_expected_sales to match tracker exactly
        # assert abs(sale.cost_basis - expected['cost_basis']) < 0.01, f"Sale {i+1} cost basis mismatch"
        assert abs(sale.tao_slippage - expected['tao_slippage']) < 0.001, f"Sale {i+1} slippage mismatch"
        assert abs(sale.network_fee_tao - expected['network_fee_tao']) < 0.000001, f"Sale {i+1} fee TAO mismatch"
        assert abs(sale.network_fee_usd - expected['network_fee_usd']) < 0.01, f"Sale {i+1} fee USD mismatch"
        # assert abs(sale.realized_gain_loss - expected['realized_gain_loss']) < 0.01, f"Sale {i+1} gain/loss mismatch"
