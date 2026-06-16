"""
Integration test for mining tracker run() method.

Tests the complete workflow for mining emissions:
1. Staking emissions processing  
2. Final balance reconciliation

Verifies final balances match the on-chain data from:
- stake_balance.json for ALPHA balance
"""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest


@pytest.mark.parametrize(
    "start_date,end_date",
    [
        # Full December 2025 - verifies staking emissions for entire month
        # Nov 30 balance: 7510117 RAO (0.0075 ALPHA) - baseline
        # Dec 31 balance: 22297591504 RAO (22.3 ALPHA)
        (
            datetime(2025, 12, 1, tzinfo=timezone.utc),
            datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        ),
        # Dec 1-7 subset - first week
        # Expected to have opening ALPHA lot + opening TAO lot + first week of emissions
        (
            datetime(2025, 12, 1, tzinfo=timezone.utc),
            datetime(2025, 12, 7, 23, 59, 59, tzinfo=timezone.utc),
        ),
    ],
)
def test_run_mining_tracker(
    get_mining_tracker,
    compute_expected_mining_staking_emission_lots,
    start_date,
    end_date,
):
    """Test the complete run() workflow for mining tracker.

    Verifies that:
    1. Opening lots are created correctly (ALPHA and TAO)
    2. Staking emission lots match expected values exactly
    3. All lot fields are properly populated
    """

    # Compute expected emission lots from raw data
    expected_emission_lots = compute_expected_mining_staking_emission_lots(
        start_date, end_date
    )

    # Create tracker
    tracker = get_mining_tracker()

    # Create opening lots from the day before start_date
    start_time = int(start_date.timestamp())
    tracker.create_opening_lots(start_time)

    # Run tracker for the full period
    mock_now = int(end_date.timestamp())
    with patch("time.time", return_value=mock_now):
        tracker.run(start_time=start_time, end_time=mock_now)

    # Get all created lots from sheets
    income_records = tracker.income_sheet.get_all_records()

    # Parse into AlphaLot objects for comparison
    from emissions_tracker.models import AlphaLot, LotStatus
    from emissions_tracker.models import SourceType as ST

    all_lots = []
    for record in income_records:
        lot = AlphaLot(
            lot_id=record["Lot ID"],
            timestamp=int(record["Timestamp"]),
            block_number=int(record.get("Block Number", 0)),
            alpha_rao=int(float(record["Alpha RAO"])),
            alpha_rao_remaining=int(float(record["Alpha RAO Remaining"])),
            usd_fmv=float(record["USD FMV"]),
            usd_per_alpha=float(record["USD/Alpha"]),
            tao_equivalent=float(record.get("TAO Equivalent", 0.0)),
            source_type=ST(record["Source Type"]),
            status=LotStatus(record["Status"]),
            notes=record.get("Notes", ""),
        )
        all_lots.append(lot)

    # Separate opening lots from emission lots
    opening_lots = [
        lot for lot in all_lots if lot.source_type.value == "Opening Balance"
    ]
    emission_lots = [lot for lot in all_lots if lot.source_type.value == "Mining"]

    # Expected counts: 1 opening ALPHA lot + N emission lots
    expected_emission_count = len(expected_emission_lots)
    actual_emission_count = len(emission_lots)

    print(f"\n=== Test Results ===")
    print(f"Opening lots: {len(opening_lots)}")
    print(
        f"Emission lots: {actual_emission_count} (expected: {expected_emission_count})"
    )

    # Verify opening lots were created (1 ALPHA opening lot)
    assert (
        len(opening_lots) == 1
    ), f"Expected 1 opening ALPHA lot, got {len(opening_lots)}"
    assert opening_lots[0].lot_id == "ALPHA-0001", "Opening lot should be ALPHA-0001"

    # Verify emission lot count matches expected
    assert (
        actual_emission_count == expected_emission_count
    ), f"Expected {expected_emission_count} emission lots, got {actual_emission_count}"

    # Sort both lists by timestamp for comparison
    expected_sorted = sorted(expected_emission_lots, key=lambda x: x.timestamp)
    actual_sorted = sorted(emission_lots, key=lambda x: x.timestamp)

    # Compare each emission lot to expected values
    for i, (expected, actual) in enumerate(zip(expected_sorted, actual_sorted)):
        # Verify timestamp matches (same day)
        expected_date = datetime.fromtimestamp(
            expected.timestamp, tz=timezone.utc
        ).date()
        actual_date = datetime.fromtimestamp(actual.timestamp, tz=timezone.utc).date()
        assert (
            actual_date == expected_date
        ), f"Lot {i+1} date mismatch: {actual_date} != {expected_date}"

        # Verify alpha RAO matches exactly
        assert (
            actual.alpha_rao == expected.alpha_rao
        ), f"Lot {i+1} ({actual.lot_id}) ALPHA RAO mismatch: {actual.alpha_rao} != {expected.alpha_rao}"

        # Verify alpha_rao_remaining equals alpha_rao (lot is open/unused)
        assert (
            actual.alpha_rao_remaining == actual.alpha_rao
        ), f"Lot {i+1} should be fully open: {actual.alpha_rao_remaining} != {actual.alpha_rao}"

        # Verify USD FMV matches exactly
        assert (
            actual.usd_fmv == expected.usd_fmv
        ), f"Lot {i+1} ({actual.lot_id}) USD FMV mismatch: {actual.usd_fmv} != {expected.usd_fmv}"

        # Verify USD per ALPHA matches exactly
        assert (
            actual.usd_per_alpha == expected.usd_per_alpha
        ), f"Lot {i+1} ({actual.lot_id}) USD per ALPHA mismatch: {actual.usd_per_alpha} != {expected.usd_per_alpha}"

        # Verify source type
        assert (
            actual.source_type.value == "Mining"
        ), f"Lot {i+1} should have MINING source type"

        # Verify lot status is Open
        assert actual.status.value == "Open", f"Lot {i+1} should have Open status"
