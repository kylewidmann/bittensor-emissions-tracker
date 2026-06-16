"""Unit tests for BittensorTracker.verify_balances(), verify_balances_yearly(),
and extrinsic-ID duplicate detection."""

from unittest.mock import MagicMock

import pytest

from emissions_tracker.exceptions import DuplicateExtrinsicError
from emissions_tracker.models import (
    AlphaLot,
    AlphaSale,
    DisposalEvent,
    DisposalType,
    Expense,
    GainType,
    LotStatus,
    SourceType,
    TaoLot,
    TaoStatsAccountHistory,
    TaoStatsStakeBalance,
    TaoTransfer,
)
from emissions_tracker.trackers.bittensor_tracker import BittensorTracker

RAO_PER_TAO = 10**9

OCT_15 = 1760486400  # 2025-10-15 00:00 UTC
NOV_1 = 1761955200  # 2025-11-01 00:00 UTC
NOV_15 = 1763164800  # 2025-11-15 00:00 UTC
NOV_30 = 1764547199  # 2025-11-30 23:59:59 UTC
DEC_10 = 1765324800  # 2025-12-10 00:00 UTC


def _make_tracker(
    alpha_lots=None,
    tao_lots=None,
    sales=None,
    expenses=None,
    transfers=None,
    stake_balances=None,
    account_histories=None,
    hotkey="5FakeHotkey",
    subnet_id=64,
):
    """Build a minimal BittensorTracker-like object for testing."""
    tracker = object.__new__(BittensorTracker)
    tracker.alpha_lots = alpha_lots or []
    tracker.tao_lots = tao_lots or []
    tracker.sales = sales or []
    tracker.expenses = expenses or []
    tracker.transfers = transfers or []
    tracker.hotkey_ss58 = hotkey
    tracker.coldkey_ss58 = "5FakeColdkey"
    tracker.subnet_id = subnet_id
    tracker.last_disposal_timestamp = 0

    wallet_client = MagicMock()
    wallet_client.get_stake_balance_history.return_value = stake_balances or []
    wallet_client.get_account_history.return_value = account_histories or []
    tracker.wallet_client = wallet_client

    return tracker


def _alpha_lot(alpha_rao, timestamp=NOV_1, remaining=None):
    return AlphaLot(
        lot_id="ALPHA-TEST",
        timestamp=timestamp,
        block_number=1,
        source_type=SourceType.CONTRACT,
        alpha_rao=alpha_rao,
        alpha_rao_remaining=remaining if remaining is not None else alpha_rao,
        usd_fmv=100.0,
        usd_per_alpha=0.01,
        tao_equivalent=1.0,
        status=LotStatus.OPEN,
    )


def _tao_lot(rao, timestamp=NOV_1, remaining=None):
    return TaoLot(
        lot_id="TAO-TEST",
        timestamp=timestamp,
        block_number=1,
        rao=rao,
        rao_remaining=remaining if remaining is not None else rao,
        usd_basis=100.0,
        usd_per_tao=350.0,
        source_sale_id="",
        extrinsic_id="",
        status=LotStatus.OPEN,
    )


def _alpha_sale(alpha_disposed, timestamp=NOV_15, extrinsic_id=None):
    return AlphaSale(
        sale_id="SALE-TEST",
        timestamp=timestamp,
        block_number=1,
        alpha_disposed=alpha_disposed,
        tao_received=alpha_disposed * 0.5,
        tao_price_usd=350.0,
        usd_proceeds=alpha_disposed * 0.5 * 350.0,
        cost_basis=alpha_disposed * 0.01,
        realized_gain_loss=(alpha_disposed * 0.5 * 350.0) - (alpha_disposed * 0.01),
        gain_type=GainType.SHORT_TERM,
        consumed_lots=[],
        created_tao_lot_id="TAO-FROM-SALE",
        extrinsic_id=extrinsic_id,
    )


def _expense(alpha_disposed, timestamp=NOV_15, extrinsic_id=None):
    return Expense(
        expense_id="EXP-TEST",
        timestamp=timestamp,
        block_number=1,
        alpha_disposed=alpha_disposed,
        tao_received=alpha_disposed * 0.5,
        tao_price_usd=350.0,
        usd_proceeds=alpha_disposed * 0.5 * 350.0,
        cost_basis=alpha_disposed * 0.01,
        realized_gain_loss=0.0,
        gain_type=GainType.SHORT_TERM,
        consumed_lots=[],
        created_tao_lot_id="TAO-FROM-EXP",
        transfer_address="5SomeAddress",
        extrinsic_id=extrinsic_id,
    )


def _tao_transfer(total_outflow_tao, timestamp=NOV_15, extrinsic_id=None):
    return TaoTransfer(
        transfer_id="XFER-TEST",
        timestamp=timestamp,
        block_number=1,
        tao_amount=total_outflow_tao * 0.99,
        tao_price_usd=350.0,
        usd_proceeds=total_outflow_tao * 0.99 * 350.0,
        cost_basis=total_outflow_tao * 100.0,
        realized_gain_loss=0.0,
        gain_type=GainType.SHORT_TERM,
        consumed_tao_lots=[],
        total_outflow_tao=total_outflow_tao,
        fee_tao=total_outflow_tao * 0.01,
        extrinsic_id=extrinsic_id,
    )


def _stake_balance(alpha_rao, timestamp=NOV_30):
    b = MagicMock(spec=TaoStatsStakeBalance)
    b.timestamp_unix = timestamp
    b.balance_as_alpha_float = alpha_rao / RAO_PER_TAO
    b.balance_as_alpha_rao = alpha_rao
    return b


def _account_history(balance_free_rao, timestamp=NOV_30):
    h = MagicMock(spec=TaoStatsAccountHistory)
    h.timestamp_unix = timestamp
    h.balance_free_tao = balance_free_rao / RAO_PER_TAO
    return h


# ---------------------------------------------------------------------------
# verify_balances — event-stream reconstruction
# ---------------------------------------------------------------------------


class TestVerifyBalancesEventStream:
    """Tests that verify_balances uses acquired - disposed, not lot remaining."""

    def test_alpha_acquired_only(self):
        """No disposals — book balance equals total acquired alpha."""
        alpha_rao = 100_000_000_000  # 100 ALPHA
        tracker = _make_tracker(
            alpha_lots=[_alpha_lot(alpha_rao)],
            stake_balances=[_stake_balance(alpha_rao)],
            account_histories=[_account_history(0)],
        )
        assert tracker.verify_balances("2025-11") is True

    def test_alpha_with_sale(self):
        """Book = acquired - sold. On-chain matches the net."""
        acquired_rao = 100_000_000_000  # 100 ALPHA
        sold = 30.0  # 30 ALPHA sold
        net_rao = int((100 - 30) * RAO_PER_TAO)  # 70 ALPHA

        tracker = _make_tracker(
            alpha_lots=[_alpha_lot(acquired_rao, timestamp=OCT_15)],
            sales=[_alpha_sale(sold, timestamp=NOV_15)],
            stake_balances=[_stake_balance(net_rao)],
            account_histories=[_account_history(0)],
        )
        assert tracker.verify_balances("2025-11") is True

    def test_alpha_with_expense(self):
        """Book = acquired - expensed."""
        acquired_rao = 50_000_000_000  # 50 ALPHA
        expensed = 10.0
        net_rao = int((50 - 10) * RAO_PER_TAO)

        tracker = _make_tracker(
            alpha_lots=[_alpha_lot(acquired_rao, timestamp=OCT_15)],
            expenses=[_expense(expensed, timestamp=NOV_15)],
            stake_balances=[_stake_balance(net_rao)],
            account_histories=[_account_history(0)],
        )
        assert tracker.verify_balances("2025-11") is True

    def test_alpha_sale_in_future_excluded(self):
        """A sale in Dec should not affect Nov balance."""
        acquired_rao = 100_000_000_000  # 100 ALPHA
        tracker = _make_tracker(
            alpha_lots=[_alpha_lot(acquired_rao, timestamp=OCT_15)],
            sales=[_alpha_sale(30.0, timestamp=DEC_10)],
            stake_balances=[_stake_balance(acquired_rao)],
            account_histories=[_account_history(0)],
        )
        assert tracker.verify_balances("2025-11") is True

    def test_lot_remaining_does_not_matter(self):
        """Even with lot.alpha_rao_remaining reduced, book uses alpha_rao."""
        acquired_rao = 100_000_000_000
        remaining_rao = 70_000_000_000  # partially consumed
        net_rao = acquired_rao  # no sales in period

        tracker = _make_tracker(
            alpha_lots=[
                _alpha_lot(acquired_rao, timestamp=OCT_15, remaining=remaining_rao)
            ],
            stake_balances=[_stake_balance(net_rao)],
            account_histories=[_account_history(0)],
        )
        assert tracker.verify_balances("2025-11") is True

    def test_tao_acquired_minus_transferred(self):
        """Book TAO = lots created - total outflow from transfers."""
        tao_lot_rao = 5_000_000_000  # 5 TAO
        transferred = 2.0  # 2 TAO outflow
        net_rao = int((5.0 - 2.0) * RAO_PER_TAO)

        tracker = _make_tracker(
            alpha_lots=[],
            tao_lots=[_tao_lot(tao_lot_rao, timestamp=OCT_15)],
            transfers=[_tao_transfer(transferred, timestamp=NOV_15)],
            stake_balances=[],
            account_histories=[_account_history(net_rao)],
            hotkey="",
        )
        assert tracker.verify_balances("2025-11") is True

    def test_tao_transfer_in_future_excluded(self):
        """A Dec transfer should not affect Nov TAO balance."""
        tao_rao = 5_000_000_000
        tracker = _make_tracker(
            tao_lots=[_tao_lot(tao_rao, timestamp=OCT_15)],
            transfers=[_tao_transfer(2.0, timestamp=DEC_10)],
            stake_balances=[],
            account_histories=[_account_history(tao_rao)],
            hotkey="",
        )
        assert tracker.verify_balances("2025-11") is True


class TestVerifyBalancesMatching:
    def test_alpha_and_tao_match(self):
        alpha_rao = 125_432_000_000
        tao_rao = 16_000_000

        tracker = _make_tracker(
            alpha_lots=[_alpha_lot(alpha_rao)],
            tao_lots=[_tao_lot(tao_rao)],
            stake_balances=[_stake_balance(alpha_rao)],
            account_histories=[_account_history(tao_rao)],
        )
        result = tracker.verify_balances("2025-11", wallet_label="Contract Wallet")
        assert result is True

    def test_zero_balances_match(self):
        tracker = _make_tracker(
            stake_balances=[_stake_balance(0)],
            account_histories=[_account_history(0)],
        )
        result = tracker.verify_balances("2025-11")
        assert result is True


class TestVerifyBalancesMismatch:
    def test_alpha_mismatch_warns(self, capsys):
        tracker = _make_tracker(
            alpha_lots=[_alpha_lot(50_000_000_000)],
            stake_balances=[_stake_balance(50_500_000_000)],
            account_histories=[_account_history(0)],
        )
        result = tracker.verify_balances("2025-11", wallet_label="Mining Wallet")
        assert result is False
        output = capsys.readouterr().out
        assert "WARNING" in output
        assert "ALPHA" in output

    def test_tao_mismatch_warns(self, capsys):
        tracker = _make_tracker(
            tao_lots=[_tao_lot(1_250_000_000)],
            account_histories=[_account_history(750_000_000)],
            hotkey="",
        )
        result = tracker.verify_balances("2025-11")
        assert result is False
        output = capsys.readouterr().out
        assert "WARNING" in output
        assert "TAO" in output


class TestVerifyBalancesTolerance:
    def test_within_tolerance_passes(self):
        lots_rao = 1_000_000_000  # 1.0
        chain_rao = 1_000_500_000  # 1.0005 — diff = 0.0005, within 0.001

        tracker = _make_tracker(
            tao_lots=[_tao_lot(lots_rao)],
            account_histories=[_account_history(chain_rao)],
            hotkey="",
        )
        result = tracker.verify_balances("2025-11")
        assert result is True


class TestVerifyBalancesEdgeCases:
    def test_no_hotkey_skips_alpha(self, capsys):
        tracker = _make_tracker(
            tao_lots=[_tao_lot(1_000_000_000)],
            account_histories=[_account_history(1_000_000_000)],
            hotkey="",
        )
        result = tracker.verify_balances("2025-11")
        assert result is True
        output = capsys.readouterr().out
        assert "ALPHA" not in output

    def test_api_error_returns_false(self):
        tracker = _make_tracker(hotkey="5FakeHotkey")
        tracker.wallet_client.get_stake_balance_history.side_effect = Exception(
            "API timeout"
        )
        tracker.wallet_client.get_account_history.side_effect = Exception("API timeout")
        result = tracker.verify_balances("2025-11")
        assert result is False

    def test_future_lots_excluded(self):
        """Lots created after end-of-month should not be counted."""
        dec_ts = 1764633600  # 2025-12-02 00:00 UTC
        alpha_rao = 100_000_000_000

        tracker = _make_tracker(
            alpha_lots=[_alpha_lot(alpha_rao, timestamp=dec_ts)],
            stake_balances=[_stake_balance(0)],
            account_histories=[_account_history(0)],
        )
        result = tracker.verify_balances("2025-11")
        assert result is True

    def test_verbose_shows_breakdown(self, capsys):
        alpha_rao = 100_000_000_000
        tracker = _make_tracker(
            alpha_lots=[_alpha_lot(alpha_rao, timestamp=OCT_15)],
            stake_balances=[_stake_balance(alpha_rao)],
            account_histories=[_account_history(0)],
        )
        tracker.verify_balances("2025-11", verbose=True)
        output = capsys.readouterr().out
        assert "acquired:" in output
        assert "disposed:" in output


# ---------------------------------------------------------------------------
# _months_with_lot_data — includes disposals
# ---------------------------------------------------------------------------


class TestMonthsWithLotData:
    def test_empty_returns_empty(self):
        tracker = _make_tracker()
        assert tracker._months_with_lot_data(2025) == []

    def test_alpha_lots_detected(self):
        tracker = _make_tracker(alpha_lots=[_alpha_lot(1_000_000_000, timestamp=NOV_1)])
        assert tracker._months_with_lot_data(2025) == ["2025-11"]

    def test_tao_lots_detected(self):
        tracker = _make_tracker(tao_lots=[_tao_lot(1_000_000_000, timestamp=OCT_15)])
        assert tracker._months_with_lot_data(2025) == ["2025-10"]

    def test_sales_detected(self):
        tracker = _make_tracker(sales=[_alpha_sale(10.0, timestamp=DEC_10)])
        assert tracker._months_with_lot_data(2025) == ["2025-12"]

    def test_expenses_detected(self):
        tracker = _make_tracker(expenses=[_expense(5.0, timestamp=NOV_15)])
        assert tracker._months_with_lot_data(2025) == ["2025-11"]

    def test_transfers_detected(self):
        tracker = _make_tracker(transfers=[_tao_transfer(1.0, timestamp=OCT_15)])
        assert tracker._months_with_lot_data(2025) == ["2025-10"]

    def test_multiple_months_sorted(self):
        tracker = _make_tracker(
            alpha_lots=[_alpha_lot(1_000_000_000, timestamp=DEC_10)],
            tao_lots=[_tao_lot(1_000_000_000, timestamp=OCT_15)],
        )
        assert tracker._months_with_lot_data(2025) == ["2025-10", "2025-12"]

    def test_wrong_year_excluded(self):
        ts_2024 = 1704067200  # 2024-01-01 00:00 UTC
        tracker = _make_tracker(tao_lots=[_tao_lot(1_000_000_000, timestamp=ts_2024)])
        assert tracker._months_with_lot_data(2025) == []

    def test_deduplicates_months(self):
        tracker = _make_tracker(
            alpha_lots=[
                _alpha_lot(1_000_000_000, timestamp=NOV_1),
                _alpha_lot(2_000_000_000, timestamp=NOV_1 + 86400),
            ],
            tao_lots=[_tao_lot(500_000_000, timestamp=NOV_1 + 3600)],
            sales=[_alpha_sale(1.0, timestamp=NOV_15)],
        )
        assert tracker._months_with_lot_data(2025) == ["2025-11"]


# ---------------------------------------------------------------------------
# verify_balances_yearly
# ---------------------------------------------------------------------------


class TestVerifyBalancesYearly:
    def test_no_data_returns_true(self, capsys):
        tracker = _make_tracker()
        result = tracker.verify_balances_yearly(2025, wallet_label="Contract")
        assert result is True
        output = capsys.readouterr().out
        assert "No lot data found" in output

    def test_all_months_pass(self, capsys):
        alpha_rao = 50_000_000_000
        tao_rao = 1_000_000_000

        nov_eom_start = NOV_30 - 86399

        wallet_client = MagicMock()

        cumulative = {
            "oct": {"alpha": alpha_rao, "tao": tao_rao},
            "nov": {"alpha": alpha_rao * 2, "tao": tao_rao * 2},
        }

        def fake_stake_history(netuid, hotkey, coldkey, start_time, end_time):
            if start_time >= nov_eom_start:
                return [_stake_balance(cumulative["nov"]["alpha"], timestamp=end_time)]
            return [_stake_balance(cumulative["oct"]["alpha"], timestamp=end_time)]

        def fake_account_history(address, start_time, end_time):
            if start_time >= nov_eom_start:
                return [_account_history(cumulative["nov"]["tao"], timestamp=end_time)]
            return [_account_history(cumulative["oct"]["tao"], timestamp=end_time)]

        wallet_client.get_stake_balance_history.side_effect = fake_stake_history
        wallet_client.get_account_history.side_effect = fake_account_history

        tracker = _make_tracker(
            alpha_lots=[
                _alpha_lot(alpha_rao, timestamp=OCT_15),
                _alpha_lot(alpha_rao, timestamp=NOV_1),
            ],
            tao_lots=[
                _tao_lot(tao_rao, timestamp=OCT_15),
                _tao_lot(tao_rao, timestamp=NOV_1),
            ],
        )
        tracker.wallet_client = wallet_client

        result = tracker.verify_balances_yearly(2025, wallet_label="Contract")
        assert result is True
        output = capsys.readouterr().out
        assert "2 month(s) verified OK" in output

    def test_one_month_fails(self, capsys):
        alpha_rao = 50_000_000_000
        tao_rao = 1_000_000_000

        wallet_client = MagicMock()
        wallet_client.get_stake_balance_history.return_value = [
            _stake_balance(alpha_rao + 99_000_000_000)
        ]
        wallet_client.get_account_history.return_value = [_account_history(tao_rao)]

        tracker = _make_tracker(
            alpha_lots=[_alpha_lot(alpha_rao, timestamp=NOV_1)],
            tao_lots=[_tao_lot(tao_rao, timestamp=NOV_1)],
        )
        tracker.wallet_client = wallet_client

        result = tracker.verify_balances_yearly(2025, wallet_label="Mining")
        assert result is False
        output = capsys.readouterr().out
        assert "discrepancies" in output


# ---------------------------------------------------------------------------
# Extrinsic-ID duplicate detection
# ---------------------------------------------------------------------------


class TestExtrinsicDedup:
    """Verify _execute_disposal_events raises on duplicate extrinsic IDs (per-type)."""

    def _disposal(self, dtype, extrinsic_id, timestamp=NOV_15):
        return DisposalEvent(
            timestamp=timestamp,
            disposal_type=dtype,
            event=None,
            process=lambda: None,
            extrinsic_id=extrinsic_id,
        )

    def test_duplicate_sale_raises(self):
        tracker = _make_tracker(
            sales=[_alpha_sale(10.0, extrinsic_id="0xAAA")],
        )
        new_events = [self._disposal(DisposalType.SALE, "0xAAA")]

        with pytest.raises(DuplicateExtrinsicError, match="0xAAA"):
            tracker._execute_disposal_events(new_events)

    def test_duplicate_transfer_raises(self):
        tracker = _make_tracker(
            transfers=[_tao_transfer(1.0, extrinsic_id="0xBBB")],
        )
        new_events = [self._disposal(DisposalType.TRANSFER, "0xBBB")]

        with pytest.raises(DuplicateExtrinsicError, match="0xBBB"):
            tracker._execute_disposal_events(new_events)

    def test_duplicate_expense_raises(self):
        tracker = _make_tracker(
            expenses=[_expense(5.0, extrinsic_id="0xCCC")],
        )
        new_events = [self._disposal(DisposalType.EXPENSE, "0xCCC")]

        with pytest.raises(DuplicateExtrinsicError, match="0xCCC"):
            tracker._execute_disposal_events(new_events)

    def test_same_extrinsic_different_types_allowed(self):
        """A sale and transfer can share the same extrinsic_id (cross-type)."""
        tracker = _make_tracker(
            sales=[_alpha_sale(10.0, extrinsic_id="0xSHARED")],
        )
        transfer_event = self._disposal(DisposalType.TRANSFER, "0xSHARED")
        transfer_event.process = lambda: _tao_transfer(1.0, extrinsic_id="0xSHARED")

        # Should NOT raise — different type
        tracker._execute_disposal_events([transfer_event])

    def test_error_message_includes_regenerate(self):
        tracker = _make_tracker(
            sales=[_alpha_sale(10.0, extrinsic_id="0xDDD")],
        )
        new_events = [self._disposal(DisposalType.SALE, "0xDDD")]

        with pytest.raises(DuplicateExtrinsicError, match="--regenerate"):
            tracker._execute_disposal_events(new_events)

    def test_no_extrinsic_id_not_checked(self):
        """Events with None extrinsic_id should not trigger dedup."""
        tracker = _make_tracker(
            sales=[_alpha_sale(10.0, extrinsic_id=None)],
        )
        new_sale = self._disposal(DisposalType.SALE, None)
        new_sale.process = lambda: (_alpha_sale(5.0), _tao_lot(1_000_000_000))

        # Should NOT raise
        tracker._execute_disposal_events([new_sale])
