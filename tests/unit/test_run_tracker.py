"""
Integration test for run() method processing full November 2025.

Tests the complete workflow:
1. Contract income processing
2. Staking emissions processing  
3. Sales processing (ALPHA → TAO)
4. Expenses processing
5. Transfers processing (TAO → Kraken)
6. Final balance reconciliation

Verifies final balances match the on-chain data from:
- stake_balance.json for ALPHA balance
- account_history.json for TAO balance (balance_free)
"""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest


@pytest.mark.parametrize(
    "seed_date,start_date,end_date,expected_alpha_rao,expected_tao_rao",
    [
        # Full November - verifies against Nov 30 on-chain balances
        (
            datetime(2025, 10, 15, tzinfo=timezone.utc),
            datetime(2025, 11, 1, tzinfo=timezone.utc),
            datetime(2025, 11, 30, 23, 59, 59, tzinfo=timezone.utc),
            728221510002,  # From stake_balance.json Nov 30
            233071424,  # From account_history.json Nov 30 balance_free
        ),
        # Nov 1-6 subset - verifies against Nov 6 on-chain balances
        # This is the well-tested subset with complete data
        (
            datetime(2025, 10, 15, tzinfo=timezone.utc),
            datetime(2025, 11, 1, tzinfo=timezone.utc),
            datetime(2025, 11, 6, 23, 59, 59, tzinfo=timezone.utc),
            180557853621,  # From stake_balance.json Nov 6
            0,  # From account_history.json Nov 6 balance_free
        ),
    ],
)
def test_run_tracker(
    get_contract_tracker,
    seed_contract_sheets,
    seed_date,
    start_date,
    end_date,
    expected_alpha_rao,
    expected_tao_rao,
):
    """Test the complete run() workflow for November 2025.

    Seeds sheets with opening balances from Oct 15 to Nov 1, then runs
    the tracker for the full period. Verifies final ALPHA and TAO balances
    match on-chain data from stake_balance.json and account_history.json.
    """

    # Seed sheets with opening balances (Oct 15 to start_date)
    seed_contract_sheets(seed_date, start_date)

    # Create tracker
    tracker = get_contract_tracker()

    # Get initial seeded state for logging
    income_records_before = tracker.income_sheet.get_all_records()
    tao_lots_before = tracker.tao_lots_sheet.get_all_records()

    initial_alpha = sum(float(r["Alpha Remaining"]) for r in income_records_before)
    initial_tao_rao = sum(int(r["TAO RAO Remaining"]) for r in tao_lots_before)

    print(f"\n=== Initial Seeded State ===")
    print(f"Opening ALPHA lots: {len(income_records_before)}")
    print(f"Opening TAO lots: {len(tao_lots_before)}")
    print(f"Opening ALPHA: {initial_alpha:.9f}")
    print(f"Opening TAO: {initial_tao_rao / 1e9:.9f} ({initial_tao_rao:,} RAO)")

    # Run tracker for the full period
    mock_now = int(end_date.timestamp())
    start_time = int(start_date.timestamp())
    with patch("time.time", return_value=mock_now):
        tracker.run(start_time=start_time, end_time=mock_now)

    # Get actual final state
    income_records = tracker.income_sheet.get_all_records()
    tao_lots_records = tracker.tao_lots_sheet.get_all_records()
    sales_records = tracker.sales_sheet.get_all_records()
    transfers_records = tracker.transfers_sheet.get_all_records()

    final_alpha_rao = sum(float(r["Alpha Remaining"]) * 1e9 for r in income_records)
    final_tao_rao = sum(int(r["TAO RAO Remaining"]) for r in tao_lots_records)

    print(f"\n=== Final State After Run ===")
    print(f"Income lots: {len(income_records)}")
    print(f"TAO lots: {len(tao_lots_records)}")
    print(f"Sales: {len(sales_records)}")
    print(f"Transfers: {len(transfers_records)}")
    print(f"Final ALPHA: {final_alpha_rao / 1e9:.9f} ({int(final_alpha_rao):,} RAO)")
    print(f"Final TAO: {final_tao_rao / 1e9:.9f} ({final_tao_rao:,} RAO)")

    # Verify final balances match on-chain data
    print(f"\n=== Verification ===")
    print(
        f"Expected ALPHA: {expected_alpha_rao / 1e9:.9f} ({expected_alpha_rao:,} RAO)"
    )
    print(f"Expected TAO: {expected_tao_rao / 1e9:.9f} ({expected_tao_rao:,} RAO)")

    # ALPHA balance verification (allow small tolerance for floating point)
    alpha_diff = abs(final_alpha_rao - expected_alpha_rao)
    alpha_tolerance = 1  # 1 RAO tolerance
    assert (
        alpha_diff <= alpha_tolerance
    ), f"ALPHA balance mismatch: {int(final_alpha_rao):,} vs expected {expected_alpha_rao:,} (diff: {alpha_diff:,} RAO)"
    print(
        f"✓ ALPHA balance: {int(final_alpha_rao):,} RAO (expected {expected_alpha_rao:,}, diff {alpha_diff:.0f})"
    )

    # TAO balance verification (allow tolerance for fee accumulation across many transfers)
    tao_diff = abs(final_tao_rao - expected_tao_rao)
    tao_tolerance = 500000  # 0.0005 TAO tolerance for accumulated fee differences
    assert (
        tao_diff <= tao_tolerance
    ), f"TAO balance mismatch: {final_tao_rao:,} vs expected {expected_tao_rao:,} (diff: {tao_diff:,} RAO)"
    print(
        f"✓ TAO balance: {final_tao_rao:,} RAO (expected {expected_tao_rao:,}, diff {tao_diff})"
    )

    print("\n✓ Test passed: Final balances match on-chain data!")
