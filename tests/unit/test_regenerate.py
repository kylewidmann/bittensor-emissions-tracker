"""Regression tests for --regenerate behavior.

Verifies that regenerate_from properly clears data and resets counters,
including opening balance lots that have timestamps before start_time.
"""

from datetime import datetime, timezone
from unittest.mock import patch


def test_regenerate_from_beginning_clears_everything(
    get_contract_tracker,
    seed_contract_sheets,
):
    """When regenerating from the same start_date used to create data,
    ALL lots (including opening balance lots) should be cleared and
    counters should reset to 1.

    This is the exact scenario: track-contract --start-date X --regenerate
    where X was the original start date.
    """
    start_date = datetime(2025, 11, 1, tzinfo=timezone.utc)
    end_date = datetime(2025, 11, 6, 23, 59, 59, tzinfo=timezone.utc)

    # Seed sheets with data starting from start_date (opening lot is day before)
    seed_contract_sheets(start_date, end_date)
    tracker = get_contract_tracker()

    start_time = int(start_date.timestamp())
    end_time = int(end_date.timestamp())

    # First run — populates sheets with lots/disposals
    with patch("time.time", return_value=end_time):
        tracker.run(start_time=start_time, end_time=end_time)

    lots_after_first = len(tracker.alpha_lots)
    tao_after_first = len(tracker.tao_lots)
    sales_after_first = len(tracker.sales)
    assert lots_after_first > 0, "First run should create lots"

    # Regenerate from the same start date — should clear EVERYTHING
    tracker.regenerate_from(start_time, end_time=end_time)

    assert (
        len(tracker.alpha_lots) == 0
    ), f"No ALPHA lots should survive full regeneration, got {len(tracker.alpha_lots)}"
    assert (
        len(tracker.tao_lots) == 0
    ), f"No TAO lots should survive full regeneration, got {len(tracker.tao_lots)}"
    assert (
        tracker.alpha_lot_counter == 1
    ), f"ALPHA counter should reset to 1, got {tracker.alpha_lot_counter}"
    assert (
        tracker.tao_lot_counter == 1
    ), f"TAO counter should reset to 1, got {tracker.tao_lot_counter}"
    assert (
        tracker.sale_counter == 1
    ), f"Sale counter should reset to 1, got {tracker.sale_counter}"

    # Re-create opening lots and run again
    tracker.create_opening_lots(start_time)
    with patch("time.time", return_value=end_time):
        tracker.run(start_time=start_time, end_time=end_time)

    # Second run should produce identical results
    assert (
        len(tracker.alpha_lots) == lots_after_first
    ), f"Second run lot count mismatch: {len(tracker.alpha_lots)} vs {lots_after_first}"
    assert (
        len(tracker.tao_lots) == tao_after_first
    ), f"Second run TAO lot count mismatch: {len(tracker.tao_lots)} vs {tao_after_first}"
    assert (
        len(tracker.sales) == sales_after_first
    ), f"Second run sale count mismatch: {len(tracker.sales)} vs {sales_after_first}"

    # No duplicate lot IDs
    alpha_ids = [lot.lot_id for lot in tracker.alpha_lots]
    assert len(alpha_ids) == len(set(alpha_ids)), "Duplicate ALPHA lot IDs found"
    tao_ids = [lot.lot_id for lot in tracker.tao_lots]
    assert len(tao_ids) == len(set(tao_ids)), "Duplicate TAO lot IDs found"


def test_partial_regenerate_preserves_history(
    get_contract_tracker,
    seed_contract_sheets,
):
    """When regenerating from a date AFTER the initial seed date,
    lots before that date should survive and counters should continue
    from the surviving data."""
    seed_date = datetime(2025, 10, 15, tzinfo=timezone.utc)
    start_date = datetime(2025, 11, 1, tzinfo=timezone.utc)
    end_date = datetime(2025, 11, 6, 23, 59, 59, tzinfo=timezone.utc)

    # Seed with historical data from Oct 15 through Nov 6
    seed_contract_sheets(seed_date, end_date)
    tracker = get_contract_tracker()

    start_time = int(start_date.timestamp())
    end_time = int(end_date.timestamp())

    # First run — processes Nov 1 through Nov 6
    with patch("time.time", return_value=end_time):
        tracker.run(start_time=start_time, end_time=end_time)

    lots_after_first = len(tracker.alpha_lots)

    # Regenerate from Nov 1 — should preserve lots BEFORE Nov 1
    tracker.regenerate_from(start_time, end_time=end_time)

    surviving_lots = len(tracker.alpha_lots)
    assert surviving_lots > 0, "Partial regeneration should preserve historical lots"
    assert (
        surviving_lots < lots_after_first
    ), "Partial regeneration should have fewer lots than before"
    assert (
        tracker.alpha_lot_counter == surviving_lots + 1
    ), f"Counter should be {surviving_lots + 1}, got {tracker.alpha_lot_counter}"

    # Run again from Nov 1 — should produce same total as first run
    with patch("time.time", return_value=end_time):
        tracker.run(start_time=start_time, end_time=end_time)

    assert len(tracker.alpha_lots) == lots_after_first, (
        f"After partial regenerate + run, lot count should match first run: "
        f"{len(tracker.alpha_lots)} vs {lots_after_first}"
    )

    # No duplicate lot IDs
    alpha_ids = [lot.lot_id for lot in tracker.alpha_lots]
    assert len(alpha_ids) == len(set(alpha_ids)), "Duplicate ALPHA lot IDs found"
