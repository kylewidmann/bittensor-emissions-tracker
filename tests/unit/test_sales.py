"""
Unit tests for process_sales method.

Tests that ALPHA → TAO sales (UNDELEGATE events) are correctly processed,
including TAO received, cost basis calculation, slippage, and fees.
"""
from datetime import datetime
from unittest.mock import patch

import pytest

from emissions_tracker.models import AlphaSale, TaoLot
from tests.fixtures.mock_config import (
    TEST_TRACKER_SHEET_ID,
)



@pytest.mark.parametrize(
    "seed_date,start_date,end_date",
    [
        # Process emissions from Oct 15 to build up ALPHA lots, then test Nov sales
        (datetime(2025, 10, 15), datetime(2025, 11, 1), datetime(2025, 11, 30, 23, 59, 59)),
    ]
)
def test_process_sales(
    seed_contract_sheets,
    get_contract_tracker,
    compute_expected_sales,
    seed_date,
    start_date,
    end_date
):
    """Test that process_sales correctly processes UNDELEGATE events as sales.
    
    This test uses pre-seeded historical ALPHA lots, then processes sales for the target month.
    """
    # Compute expected sales from raw data using historical TAO prices
    expected_sales, expected_tao_lots = compute_expected_sales(
        start_date=seed_date,
        end_date=end_date
    )
    
    # Seed historical ALPHA lots into mock sheets BEFORE creating tracker
    seed_contract_sheets(seed_date, end_date)
    
    # Now create tracker - it will load the pre-seeded data
    tracker = get_contract_tracker()
    
    # Process sales (only for November)
    sales_lookback = (end_date - start_date).days + 1
    
    with patch.object(tracker, '_resolve_time_window', return_value=(
        int(start_date.timestamp()),
        int(end_date.timestamp())
    )):
        sales: list[AlphaSale] = tracker.process_alpha_sales(lookback_days=sales_lookback)
    
    expected_count = len(expected_sales)
    assert len(sales) == expected_count, f"Expected {expected_count} sales, got {len(sales)}"
    
    # Sort sales by timestamp for comparison
    sales.sort(key=lambda s: s.timestamp)
    
    # Compare each sale with expected values
    for i, (sale, expected) in enumerate(zip(sales, expected_sales)):
        # TODO: Update all of these assertions to be exact matches
        # Verify timestamp matches exactly
        assert sale.timestamp == expected.timestamp, \
            f"Sale {i+1} timestamp mismatch: {sale.timestamp} != {expected.timestamp}"
        
        # Verify ALPHA disposed matches exactly
        assert sale.alpha_disposed == expected.alpha_disposed, \
            f"Sale {i+1} ALPHA disposed mismatch: {sale.alpha_disposed} != {expected.alpha_disposed}"
        
        # Verify TAO received matches exactly
        assert sale.tao_received == expected.tao_received, \
            f"Sale {i+1} TAO received mismatch: {sale.tao_received} != {expected.tao_received}"
        
        # Verify slippage matches exactly
        assert sale.tao_slippage == expected.tao_slippage, \
            f"Sale {i+1} TAO slippage mismatch: {sale.tao_slippage} != {expected.tao_slippage}"
        
        # Verify network fee TAO matches exactly
        assert sale.network_fee_tao == expected.network_fee_tao, \
            f"Sale {i+1} network fee TAO mismatch: {sale.network_fee_tao} != {expected.network_fee_tao}"
        
        # Verify USD-based values match within accounting tolerance
        # USD values may differ due to:
        # - Intraday TAO price variations (test uses event USD, production uses price lookup)
        # - Rounding in multi-step floating point calculations
        # - Cumulative effects on realized_gain_loss (proceeds - basis)
        assert len(sale.consumed_lots) == len(expected.consumed_lots), \
            f"Sale {i+1} consumed lots count mismatch: {len(sale.consumed_lots)} != {len(expected.consumed_lots)}"
        assert sale.cost_basis == expected.cost_basis, \
            f"Sale {i+1} cost basis mismatch: {sale.cost_basis} != {expected.cost_basis}"
        assert sale.network_fee_usd == expected.network_fee_usd, \
            f"Sale {i+1} network fee USD mismatch: {sale.network_fee_usd} != {expected.network_fee_usd}"
        assert sale.realized_gain_loss == expected.realized_gain_loss, \
            f"Sale {i+1} realized gain/loss mismatch: {sale.realized_gain_loss} != {expected.realized_gain_loss}"