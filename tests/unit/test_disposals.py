"""
Unit tests for process_disposals method.

Tests that all disposal events (sales, expenses, transfers) are correctly processed
in chronological order. Note that exact lot consumption details (cost basis, consumed lots)
are not verified here because chronological processing order affects which lots get consumed.
Individual disposal type tests (if needed) would test those details in isolation.
"""

from datetime import datetime

import pytest

from emissions_tracker.models import CostBasisMethod
from tests.fixtures.mock_config import TEST_BROKER_SS58, TEST_PAYOUT_COLDKEY_SS58


@pytest.mark.parametrize(
    "seed_date,start_date,end_date",
    [
        # Process emissions from Oct 15 to build up ALPHA lots, then test Nov disposals
        (
            datetime(2025, 10, 15),
            datetime(2025, 11, 1),
            datetime(2025, 11, 30, 23, 59, 59),
        ),
    ],
)
def test_process_disposals(
    seed_contract_sheets,
    get_contract_tracker,
    compute_expected_sales,
    compute_expected_expenses,
    compute_expected_transfers,
    seed_date,
    start_date,
    end_date,
):
    """Test that process_disposals correctly processes all disposal events chronologically.

    This test verifies:
    1. Correct number of sales, expenses, and transfers are created
    2. Timestamps and amounts match expected values
    3. All disposals consume lots (has consumed_lots)

    Note: Exact lot consumption (cost_basis, consumed_lots contents) depends on chronological
    ordering and is not verified here because computing expected values independently would
    yield different results than interleaved processing.
    """
    # Seed historical ALPHA lots into mock sheets BEFORE creating tracker
    seeded_alpha_lots = seed_contract_sheets(seed_date, end_date)

    # Create tracker - it will load the pre-seeded data
    tracker = get_contract_tracker()

    # Compute expected values for all disposal types (for counts and amounts only)
    expected_sales, expected_tao_lots = compute_expected_sales(
        start_date=seed_date, end_date=end_date
    )

    expected_expenses = compute_expected_expenses(
        alpha_lots=seeded_alpha_lots,
        start_date=start_date,
        end_date=end_date,
        cost_basis_method=CostBasisMethod.HIFO,
    )

    expected_transfers = compute_expected_transfers(
        start_date=start_date,
        end_date=end_date,
        wallet_address=TEST_PAYOUT_COLDKEY_SS58,
        brokerage_address=TEST_BROKER_SS58,
        cost_basis_method=CostBasisMethod.HIFO,
        opening_lot_date=seed_date,
    )

    # Process all disposals chronologically
    tracker.process_disposals(
        start_time=int(start_date.timestamp()), end_time=int(end_date.timestamp())
    )

    # Get results from tracker
    actual_sales = tracker.sales
    actual_expenses = tracker.expenses
    actual_transfers = tracker.transfers

    # ==========================================================================
    # Verify Sales
    # ==========================================================================
    assert len(actual_sales) == len(
        expected_sales
    ), f"Expected {len(expected_sales)} sales, got {len(actual_sales)}"

    # Sort for comparison
    actual_sales_sorted = sorted(actual_sales, key=lambda s: s.timestamp)
    expected_sales_sorted = sorted(expected_sales, key=lambda s: s.timestamp)

    for i, (actual, expected) in enumerate(
        zip(actual_sales_sorted, expected_sales_sorted)
    ):
        assert (
            actual.timestamp == expected.timestamp
        ), f"Sale {i+1}: timestamp mismatch: {actual.timestamp} != {expected.timestamp}"
        assert (
            actual.alpha_disposed == expected.alpha_disposed
        ), f"Sale {i+1}: ALPHA disposed mismatch: {actual.alpha_disposed} != {expected.alpha_disposed}"
        assert (
            actual.tao_received == expected.tao_received
        ), f"Sale {i+1}: TAO received mismatch: {actual.tao_received} != {expected.tao_received}"
        assert (
            actual.tao_slippage == expected.tao_slippage
        ), f"Sale {i+1}: TAO slippage mismatch: {actual.tao_slippage} != {expected.tao_slippage}"
        assert (
            actual.network_fee_tao == expected.network_fee_tao
        ), f"Sale {i+1}: network fee TAO mismatch: {actual.network_fee_tao} != {expected.network_fee_tao}"
        # Verify lots were consumed (don't check exact counts - depends on chronological order)
        assert len(actual.consumed_lots) > 0, f"Sale {i+1}: should have consumed lots"

    print(f"✓ {len(actual_sales)} sales verified")

    # ==========================================================================
    # Verify Expenses
    # ==========================================================================
    assert len(actual_expenses) == len(
        expected_expenses
    ), f"Expected {len(expected_expenses)} expenses, got {len(actual_expenses)}"

    actual_expenses_sorted = sorted(actual_expenses, key=lambda e: e.timestamp)
    expected_expenses_sorted = sorted(expected_expenses, key=lambda e: e.timestamp)

    for i, (actual, expected) in enumerate(
        zip(actual_expenses_sorted, expected_expenses_sorted)
    ):
        assert (
            actual.timestamp == expected.timestamp
        ), f"Expense {i+1}: timestamp mismatch"
        assert (
            actual.block_number == expected.block_number
        ), f"Expense {i+1}: block_number mismatch"
        assert (
            actual.transfer_address == expected.transfer_address
        ), f"Expense {i+1}: transfer_address mismatch"
        assert (
            actual.alpha_disposed == expected.alpha_disposed
        ), f"Expense {i+1}: alpha_disposed mismatch - actual: {actual.alpha_disposed}, expected: {expected.alpha_disposed}"
        assert (
            actual.tao_received == expected.tao_received
        ), f"Expense {i+1}: tao_received should be 0"
        # Verify lots were consumed (don't check exact counts - depends on chronological order)
        assert (
            len(actual.consumed_lots) > 0
        ), f"Expense {i+1}: should have consumed lots"

    print(f"✓ {len(actual_expenses)} expenses verified")

    # ==========================================================================
    # Verify Transfers
    # ==========================================================================
    assert len(actual_transfers) == len(
        expected_transfers
    ), f"Expected {len(expected_transfers)} transfers, got {len(actual_transfers)}"

    actual_transfers_sorted = sorted(actual_transfers, key=lambda t: t.timestamp)
    expected_transfers_sorted = sorted(expected_transfers, key=lambda t: t.timestamp)

    for i, (actual, expected) in enumerate(
        zip(actual_transfers_sorted, expected_transfers_sorted)
    ):
        assert (
            actual.timestamp == expected.timestamp
        ), f"Transfer {i+1}: timestamp mismatch"
        assert (
            actual.block_number == expected.block_number
        ), f"Transfer {i+1}: block_number mismatch"
        assert (
            actual.tao_amount == expected.tao_amount
        ), f"Transfer {i+1}: tao_amount mismatch - actual: {actual.tao_amount}, expected: {expected.tao_amount}"
        assert (
            actual.tao_price_usd == expected.tao_price_usd
        ), f"Transfer {i+1}: tao_price_usd mismatch - actual: {actual.tao_price_usd}, expected: {expected.tao_price_usd}"
        assert (
            actual.usd_proceeds == expected.usd_proceeds
        ), f"Transfer {i+1}: usd_proceeds mismatch - actual: ${actual.usd_proceeds:.2f}, expected: ${expected.usd_proceeds:.2f}"
        assert (
            actual.total_outflow_tao == expected.total_outflow_tao
        ), f"Transfer {i+1}: total_outflow_tao mismatch"
        assert actual.fee_tao == expected.fee_tao, f"Transfer {i+1}: fee_tao mismatch"

        # Verify consumed lots totals (sum should match total_outflow_tao)
        total_consumed_tao = sum(lot.tao_consumed for lot in actual.consumed_tao_lots)
        assert (
            abs(total_consumed_tao - actual.total_outflow_tao) < 1e-9
        ), f"Transfer {i+1}: sum of consumed TAO doesn't match total_outflow_tao"
        assert (
            len(actual.consumed_tao_lots) > 0
        ), f"Transfer {i+1}: should have consumed TAO lots"

    print(f"✓ {len(actual_transfers)} transfers verified")

    print(
        f"\n✓ All disposals verified: {len(actual_sales)} sales, {len(actual_expenses)} expenses, {len(actual_transfers)} transfers"
    )
