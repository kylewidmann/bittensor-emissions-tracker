"""Tests for the PaymentTracker module.

Covers:
- Payment income processing (inbound TAO transfers -> deposits + TAO lots)
- Disposal processing (outbound TAO transfers -> TaoTransfer with lot consumption)
- Full run (income + disposals)
- Journal generation with payment income accounting
"""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from emissions_tracker.journal import aggregate_monthly_journal_entries
from emissions_tracker.models import LotStatus, TaoDeposit, TaoLot, TaoTransfer
from emissions_tracker.trackers.payment_tracker import PaymentTracker
from tests.fixtures.mock_clients import MockTaoStatsClient
from tests.fixtures.mock_config import TEST_PAYMENT_TRACKER_SHEET_ID

PAYMENT_DATA_DIR = Path(__file__).parent.parent / "data" / "payment"

RAO_PER_TAO = 10**9

# Timestamps matching the test data
INBOUND_1_TS = int(datetime(2025, 11, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp())
INBOUND_2_TS = int(datetime(2025, 11, 20, 12, 0, 0, tzinfo=timezone.utc).timestamp())
OUTBOUND_1_TS = int(datetime(2025, 11, 25, 12, 0, 0, tzinfo=timezone.utc).timestamp())
OUTBOUND_2_TS = int(datetime(2025, 12, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())


@pytest.fixture
def payment_client():
    return MockTaoStatsClient(data_dir=PAYMENT_DATA_DIR)


@pytest.fixture
def payment_tracker(payment_client):
    return PaymentTracker(
        price_client=payment_client,
        wallet_client=payment_client,
    )


class TestPaymentIncome:
    """Test inbound TAO payment processing."""

    def test_process_payment_income_creates_deposits_and_lots(self, payment_tracker):
        start = int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2025, 11, 30, tzinfo=timezone.utc).timestamp())

        deposits = payment_tracker.process_payment_income(
            start_time=start, end_time=end
        )

        assert len(deposits) == 2
        assert len(payment_tracker.deposits) == 2
        assert len(payment_tracker.tao_lots) == 2

    def test_deposit_records_have_correct_fields(self, payment_tracker):
        start = int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2025, 11, 30, tzinfo=timezone.utc).timestamp())

        payment_tracker.process_payment_income(start_time=start, end_time=end)

        dep = payment_tracker.deposits[0]
        assert isinstance(dep, TaoDeposit)
        assert dep.deposit_id.startswith("DEP-")
        assert dep.tao_amount_rao == 5_000_000_000
        assert dep.tao_amount == pytest.approx(5.0)
        assert dep.category == "Payment"
        assert dep.usd_fmv > 0

    def test_tao_lots_created_from_deposits(self, payment_tracker):
        start = int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2025, 11, 30, tzinfo=timezone.utc).timestamp())

        payment_tracker.process_payment_income(start_time=start, end_time=end)

        lot = payment_tracker.tao_lots[0]
        assert isinstance(lot, TaoLot)
        assert lot.rao == 5_000_000_000
        assert lot.rao_remaining == 5_000_000_000
        assert lot.status == LotStatus.OPEN
        assert lot.usd_basis > 0
        assert lot.usd_per_tao > 0

    def test_deposit_linked_to_lot(self, payment_tracker):
        start = int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2025, 11, 30, tzinfo=timezone.utc).timestamp())

        payment_tracker.process_payment_income(start_time=start, end_time=end)

        dep = payment_tracker.deposits[0]
        lot = payment_tracker.tao_lots[0]
        assert dep.created_tao_lot_id == lot.lot_id

    def test_idempotent_no_duplicate_processing(self, payment_tracker):
        start = int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2025, 11, 30, tzinfo=timezone.utc).timestamp())

        payment_tracker.process_payment_income(start_time=start, end_time=end)
        first_count = len(payment_tracker.deposits)

        payment_tracker.process_payment_income(start_time=start, end_time=end)
        assert len(payment_tracker.deposits) == first_count


class TestPaymentDisposals:
    """Test outbound TAO transfer processing."""

    def test_process_disposals_creates_transfers(self, payment_tracker):
        start = int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2025, 12, 5, tzinfo=timezone.utc).timestamp())

        # First create income (TAO lots to consume)
        payment_tracker.process_payment_income(start_time=start, end_time=end)
        assert len(payment_tracker.tao_lots) == 2

        # Then process disposals
        payment_tracker.process_disposals(start_time=start, end_time=end)

        assert len(payment_tracker.transfers) == 2

    def test_transfer_consumes_tao_lots(self, payment_tracker):
        start = int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2025, 12, 5, tzinfo=timezone.utc).timestamp())

        payment_tracker.process_payment_income(start_time=start, end_time=end)
        payment_tracker.process_disposals(start_time=start, end_time=end)

        # Check that lots were consumed
        total_remaining = sum(lot.rao_remaining for lot in payment_tracker.tao_lots)
        total_original = sum(lot.rao for lot in payment_tracker.tao_lots)
        assert total_remaining < total_original

    def test_transfer_has_gain_loss(self, payment_tracker):
        start = int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2025, 12, 5, tzinfo=timezone.utc).timestamp())

        payment_tracker.process_payment_income(start_time=start, end_time=end)
        payment_tracker.process_disposals(start_time=start, end_time=end)

        xfer = payment_tracker.transfers[0]
        assert isinstance(xfer, TaoTransfer)
        assert xfer.transfer_id.startswith("XFER-")
        assert xfer.usd_proceeds > 0
        assert xfer.cost_basis > 0
        # Gain/loss = proceeds - basis (could be positive or negative)
        assert xfer.realized_gain_loss == pytest.approx(
            xfer.usd_proceeds - xfer.cost_basis
        )

    def test_only_brokerage_transfers_processed(self, payment_tracker):
        """Non-brokerage outbound transfers should be ignored."""
        start = int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2025, 12, 5, tzinfo=timezone.utc).timestamp())

        payment_tracker.process_payment_income(start_time=start, end_time=end)
        payment_tracker.process_disposals(start_time=start, end_time=end)

        for xfer in payment_tracker.transfers:
            assert "brokerage" in xfer.notes.lower() or "block" in xfer.notes.lower()


class TestPaymentRun:
    """Test the full run() lifecycle."""

    def test_run_processes_income_and_disposals(self, payment_tracker):
        start = int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2025, 12, 5, tzinfo=timezone.utc).timestamp())

        payment_tracker.run(start_time=start, end_time=end)

        assert len(payment_tracker.deposits) == 2
        assert len(payment_tracker.tao_lots) == 2
        assert len(payment_tracker.transfers) == 2


class TestPaymentJournalAccounting:
    """Test that journal entries correctly credit Payment Income."""

    def test_deposit_credits_payment_income_account(self, mock_wave_settings):
        """When deposit_income_account is provided, deposits credit that account."""
        deposit_records = [
            {
                "Deposit ID": "DEP-0001",
                "Timestamp": INBOUND_1_TS,
                "USD FMV": 2500.00,
            }
        ]

        entries, summary = aggregate_monthly_journal_entries(
            year_month="2025-11",
            income_records=[],
            sales_records=[],
            expense_records=[],
            transfer_records=[],
            deposit_records=deposit_records,
            wave_config=mock_wave_settings,
            start_ts=int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp()),
            end_ts=int(datetime(2025, 12, 1, tzinfo=timezone.utc).timestamp()),
            deposit_income_account=mock_wave_settings.payment_income_account,
        )

        accounts = {e.account: e for e in entries}
        assert mock_wave_settings.payment_income_account in accounts
        assert mock_wave_settings.business_checking_account not in accounts

        payment_entry = accounts[mock_wave_settings.payment_income_account]
        assert payment_entry.credit == pytest.approx(2500.00)

        tao_entry = accounts[mock_wave_settings.tao_asset_account]
        assert tao_entry.debit == pytest.approx(2500.00)

    def test_deposit_without_override_credits_business_checking(
        self, mock_wave_settings
    ):
        """Without deposit_income_account, deposits credit business checking."""
        deposit_records = [
            {
                "Deposit ID": "DEP-0001",
                "Timestamp": INBOUND_1_TS,
                "USD FMV": 2500.00,
            }
        ]

        entries, _summary = aggregate_monthly_journal_entries(
            year_month="2025-11",
            income_records=[],
            sales_records=[],
            expense_records=[],
            transfer_records=[],
            deposit_records=deposit_records,
            wave_config=mock_wave_settings,
            start_ts=int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp()),
            end_ts=int(datetime(2025, 12, 1, tzinfo=timezone.utc).timestamp()),
        )

        accounts = {e.account: e for e in entries}
        assert mock_wave_settings.business_checking_account in accounts
        assert mock_wave_settings.payment_income_account not in accounts

    def test_journal_debits_equal_credits(self, mock_wave_settings):
        """Journal entries must balance (debits == credits)."""
        deposit_records = [
            {
                "Deposit ID": "DEP-0001",
                "Timestamp": INBOUND_1_TS,
                "USD FMV": 2500.00,
            }
        ]
        transfer_records = [
            {
                "Transfer ID": "XFER-0001",
                "Timestamp": OUTBOUND_1_TS,
                "USD Proceeds": 2000.00,
                "Cost Basis": 2000.00,
                "Realized Gain/Loss": 0.00,
                "Gain Type": "Short-term",
                "Fee Cost Basis USD": 0.01,
            }
        ]

        entries, _summary = aggregate_monthly_journal_entries(
            year_month="2025-11",
            income_records=[],
            sales_records=[],
            expense_records=[],
            transfer_records=transfer_records,
            deposit_records=deposit_records,
            wave_config=mock_wave_settings,
            start_ts=int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp()),
            end_ts=int(datetime(2025, 12, 1, tzinfo=timezone.utc).timestamp()),
            deposit_income_account=mock_wave_settings.payment_income_account,
        )

        total_debits = sum(e.debit for e in entries)
        total_credits = sum(e.credit for e in entries)
        assert total_debits == pytest.approx(total_credits, abs=0.02)


class TestPaymentSheetManagement:
    """Test sheet write/clear operations."""

    def test_write_all_data_to_sheets(self, payment_tracker, mock_sheets):
        start = int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2025, 12, 5, tzinfo=timezone.utc).timestamp())

        payment_tracker.run(start_time=start, end_time=end)

        deposits_ws = mock_sheets.get_worksheet(
            TEST_PAYMENT_TRACKER_SHEET_ID, "Deposits"
        )
        tao_lots_ws = mock_sheets.get_worksheet(
            TEST_PAYMENT_TRACKER_SHEET_ID, "TAO Lots"
        )
        transfers_ws = mock_sheets.get_worksheet(
            TEST_PAYMENT_TRACKER_SHEET_ID, "Transfers"
        )

        # Rows = header + data rows
        assert deposits_ws is not None
        deposit_records = deposits_ws.get_all_records()
        assert len(deposit_records) == 2

        tao_records = tao_lots_ws.get_all_records()
        assert len(tao_records) == 2

        transfer_records = transfers_ws.get_all_records()
        assert len(transfer_records) == 2

    def test_clear_all_sheets(self, payment_tracker, mock_sheets):
        start = int(datetime(2025, 11, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2025, 12, 5, tzinfo=timezone.utc).timestamp())

        payment_tracker.run(start_time=start, end_time=end)
        assert len(payment_tracker.deposits) > 0

        payment_tracker.clear_all_sheets()
        assert len(payment_tracker.deposits) == 0
        assert len(payment_tracker.tao_lots) == 0
        assert len(payment_tracker.transfers) == 0
        assert payment_tracker.deposit_counter == 1
        assert payment_tracker.tao_lot_counter == 1
        assert payment_tracker.transfer_counter == 1
