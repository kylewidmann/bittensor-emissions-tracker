"""Unit tests for Kraken reconciliation engine."""

import pytest

from emissions_tracker.clients.kraken_statement import (
    KrakenDeposit,
    KrakenMonthSummary,
    KrakenReward,
    KrakenTrade,
    KrakenUsdDeposit,
    KrakenWithdrawal,
)
from emissions_tracker.config import WaveAccountSettings
from emissions_tracker.entrypoints.kraken import (
    WAVE_EXCHANGE_FEE_ACCOUNT,
    WAVE_STAKING_INCOME_KRAKEN,
    ReconciliationResult,
    _filter_transfers_for_month,
    _generate_journal_entries,
    reconcile_month,
)
from emissions_tracker.models import GainType, TaoTransfer


def _make_transfer(
    transfer_id: str,
    timestamp: int,
    tao_amount: float,
    usd_proceeds: float,
) -> TaoTransfer:
    return TaoTransfer(
        transfer_id=transfer_id,
        timestamp=timestamp,
        block_number=1,
        tao_amount=tao_amount,
        tao_price_usd=usd_proceeds / tao_amount if tao_amount else 0,
        usd_proceeds=usd_proceeds,
        cost_basis=usd_proceeds * 0.98,
        realized_gain_loss=usd_proceeds * 0.02,
        gain_type=GainType.SHORT_TERM,
        consumed_tao_lots=[],
    )


def _make_kraken_month(
    year_month: str = "2025-07",
    deposits_tao: float = 10.0,
    trades_tao: float = 9.99,
    usd_received: float = 3481.01,
    cash_fees: float = 9.89,
    tao_fees: float = 0.0,
    withdrawn_usd: float = 3300.0,
    rewards_tao: float = 0.01,
    rewards_value_usd: float = 0.03,
    ending_cash: float = 150.5,
    ending_tao: float = 0.01,
    usd_deposited: float = 0.0,
) -> KrakenMonthSummary:
    return KrakenMonthSummary(
        year_month=year_month,
        deposits=[KrakenDeposit(date=f"{year_month}-01", tao_amount=deposits_tao)],
        trades=[
            KrakenTrade(
                date=f"{year_month}-01",
                tao_sold=trades_tao,
                usd_received=usd_received,
                fee_usd_cash=cash_fees,
                fee_usd_tao=tao_fees,
            )
        ],
        withdrawals=(
            [KrakenWithdrawal(date=f"{year_month}-15", usd_amount=withdrawn_usd)]
            if withdrawn_usd > 0
            else []
        ),
        rewards=(
            [
                KrakenReward(
                    date=f"{year_month}-20",
                    tao_amount=rewards_tao,
                    fee_tao=0.002,
                    value_usd=rewards_value_usd,
                )
            ]
            if rewards_tao > 0
            else []
        ),
        usd_deposits=(
            [KrakenUsdDeposit(date=f"{year_month}-05", usd_amount=usd_deposited)]
            if usd_deposited > 0
            else []
        ),
        ending_cash_usd=ending_cash,
        ending_tao=ending_tao,
        ending_tao_value_usd=ending_tao * 350.0,
    )


class TestFilterTransfersForMonth:
    def test_filters_correctly(self):
        t_jul = _make_transfer("X-1", 1751396400, 10.0, 3520.0)  # 2025-07-01
        t_aug = _make_transfer("X-2", 1754074800, 5.0, 1750.0)  # 2025-08-01

        result = _filter_transfers_for_month([t_jul, t_aug], "2025-07")
        assert len(result) == 1
        assert result[0].transfer_id == "X-1"

    def test_empty_month(self):
        t_jul = _make_transfer("X-1", 1751396400, 10.0, 3520.0)
        result = _filter_transfers_for_month([t_jul], "2025-08")
        assert len(result) == 0

    def test_utc_boundary_excludes_next_month(self):
        """Transfer on Dec 31 local that's Jan 1 UTC belongs to January.

        Regression: XFER-0018 was 2025-12-31 21:11 EST (2026-01-01 02:11 UTC).
        The filter must use UTC so Kraken's statement period boundaries match.
        """
        # 1767233484 = 2025-12-31 21:11:24 EST = 2026-01-01 02:11:24 UTC
        t_dec = _make_transfer("X-DEC", 1767200000, 6.0, 1400.0)  # safe Dec UTC
        t_boundary = _make_transfer("X-BOUNDARY", 1767233484, 9.53, 2103.24)

        dec_result = _filter_transfers_for_month([t_dec, t_boundary], "2025-12")
        assert len(dec_result) == 1
        assert dec_result[0].transfer_id == "X-DEC"

        jan_result = _filter_transfers_for_month([t_dec, t_boundary], "2026-01")
        assert len(jan_result) == 1
        assert jan_result[0].transfer_id == "X-BOUNDARY"


class TestReconcileMonth:
    @pytest.fixture
    def wave_config(self):
        return WaveAccountSettings()

    def test_price_loss(self, wave_config):
        """Sub-ledger priced higher than Kraken → price loss → positive price_diff."""
        transfers = [_make_transfer("X-1", 1751396400, 10.0, 3520.0)]
        kraken = _make_kraken_month(usd_received=3481.01, cash_fees=9.89)

        result = reconcile_month(transfers, kraken, wave_config)

        assert result.subledger_proceeds == pytest.approx(3520.0)
        assert result.gross_tao_to_usd == pytest.approx(3481.01)
        assert result.price_difference == pytest.approx(38.99)
        assert result.clearing_correction == pytest.approx(48.88)

    def test_price_gain(self, wave_config):
        """Kraken sold higher than sub-ledger → price gain → negative price_diff."""
        transfers = [_make_transfer("X-1", 1751396400, 10.0, 3400.0)]
        kraken = _make_kraken_month(usd_received=3481.01, cash_fees=9.89)

        result = reconcile_month(transfers, kraken, wave_config)

        assert result.subledger_proceeds == pytest.approx(3400.0)
        assert result.price_difference == pytest.approx(-81.01)
        assert result.clearing_correction == pytest.approx(-71.12)

    def test_zero_subledger(self, wave_config):
        """No sub-ledger transfers (e.g. --no-sheets mode)."""
        kraken = _make_kraken_month(usd_received=3481.01, cash_fees=9.89)
        result = reconcile_month([], kraken, wave_config)

        assert result.subledger_proceeds == 0.0
        assert result.cash_fees == pytest.approx(9.89)

    def test_no_withdrawal(self, wave_config):
        """Month with no withdrawal (e.g. December)."""
        transfers = [_make_transfer("X-1", 1751396400, 10.0, 3520.0)]
        kraken = _make_kraken_month(withdrawn_usd=0.0, ending_cash=3471.12)

        result = reconcile_month(transfers, kraken, wave_config)
        assert result.kraken_withdrawn_usd == 0.0
        assert result.ending_cash_usd == pytest.approx(3471.12)

    def test_multiple_transfers(self, wave_config):
        """Multiple sub-ledger transfers in one month are summed."""
        t1 = _make_transfer("X-1", 1751396400, 5.0, 1750.0)
        t2 = _make_transfer("X-2", 1751400000, 5.0, 1770.0)
        kraken = _make_kraken_month(usd_received=3481.01, cash_fees=9.89)

        result = reconcile_month([t1, t2], kraken, wave_config)
        assert result.subledger_proceeds == pytest.approx(3520.0)

    def test_split_fees(self, wave_config):
        """Cash fees and TAO fees are tracked separately."""
        transfers = [_make_transfer("X-1", 1751396400, 10.0, 3520.0)]
        kraken = _make_kraken_month(
            usd_received=3481.01, cash_fees=2.12, tao_fees=55.64
        )

        result = reconcile_month(transfers, kraken, wave_config)

        assert result.cash_fees == pytest.approx(2.12)
        assert result.tao_fees == pytest.approx(55.64)
        assert result.clearing_correction == pytest.approx(2.12 + 38.99)

    def test_usd_deposits_tracked(self, wave_config):
        """USD deposits are captured in the result."""
        kraken = _make_kraken_month(usd_deposited=10.0)
        result = reconcile_month([], kraken, wave_config)
        assert result.usd_deposited == pytest.approx(10.0)

    def test_subcent_proceeds_rounded(self, wave_config):
        """Sub-cent usd_proceeds from price*amount are rounded to 2dp.

        Regression: Google Sheets stores usd_proceeds = tao_amount * price
        which can produce sub-cent precision (e.g. 7.8918 * 392.11 = 3094.483...).
        Summing raw floats diverges from the accounting-rounded total.
        """
        t1 = _make_transfer("X-1", 1751396400, 7.8918, 3094.48398)
        t2 = _make_transfer("X-2", 1751400000, 4.0, 1561.28)
        kraken = _make_kraken_month(usd_received=4600.0, cash_fees=5.0)

        result = reconcile_month([t1, t2], kraken, wave_config)

        assert result.subledger_proceeds == 4655.76
        assert result.subledger_proceeds == round(result.subledger_proceeds, 2)


class TestGenerateJournalEntries:
    @pytest.fixture
    def wave_config(self):
        return WaveAccountSettings()

    def test_cash_fee_entries(self, wave_config):
        r = ReconciliationResult(
            year_month="2025-07",
            subledger_proceeds=3520.0,
            gross_tao_to_usd=3481.01,
            cash_fees=9.89,
            tao_fees=0.0,
            kraken_withdrawn_usd=3300.0,
            price_difference=38.99,
            clearing_correction=48.88,
            rewards_tao=0.0,
            rewards_value_usd=0.0,
            ending_cash_usd=150.0,
            ending_tao=0.01,
            ending_tao_value_usd=3.5,
            tao_deposited=10.0,
            tao_sold=9.99,
            usd_deposited=0.0,
        )
        entries = _generate_journal_entries(r, wave_config)
        fee_entries = [
            e
            for e in entries
            if e["account"] == WAVE_EXCHANGE_FEE_ACCOUNT
            and e.get("section") == "clearing"
        ]
        assert len(fee_entries) == 1
        assert fee_entries[0]["debit"] == pytest.approx(9.89)

    def test_price_loss_entries(self, wave_config):
        """Positive price_difference → sub-ledger priced higher → loss."""
        r = ReconciliationResult(
            year_month="2025-07",
            subledger_proceeds=3520.0,
            gross_tao_to_usd=3481.01,
            cash_fees=9.89,
            tao_fees=0.0,
            kraken_withdrawn_usd=3300.0,
            price_difference=38.99,
            clearing_correction=48.88,
            rewards_tao=0.0,
            rewards_value_usd=0.0,
            ending_cash_usd=150.0,
            ending_tao=0.01,
            ending_tao_value_usd=3.5,
            tao_deposited=10.0,
            tao_sold=9.99,
            usd_deposited=0.0,
        )
        entries = _generate_journal_entries(r, wave_config)

        stcg_debits = [
            e
            for e in entries
            if e["account"] == wave_config.short_term_gain_account and e["debit"] > 0
        ]
        assert len(stcg_debits) == 1
        assert stcg_debits[0]["debit"] == pytest.approx(38.99)

    def test_price_gain_entries(self, wave_config):
        """Negative price_difference → Kraken sold higher → gain."""
        r = ReconciliationResult(
            year_month="2025-07",
            subledger_proceeds=3400.0,
            gross_tao_to_usd=3481.01,
            cash_fees=9.89,
            tao_fees=0.0,
            kraken_withdrawn_usd=3300.0,
            price_difference=-81.01,
            clearing_correction=-71.12,
            rewards_tao=0.0,
            rewards_value_usd=0.0,
            ending_cash_usd=150.0,
            ending_tao=0.01,
            ending_tao_value_usd=3.5,
            tao_deposited=10.0,
            tao_sold=9.99,
            usd_deposited=0.0,
        )
        entries = _generate_journal_entries(r, wave_config)

        clearing_entries = [
            e for e in entries if e["account"] == wave_config.transfer_proceeds_account
        ]
        assert len(clearing_entries) == 1
        assert clearing_entries[0]["debit"] == pytest.approx(71.12)

        stcg_credits = [
            e
            for e in entries
            if e["account"] == wave_config.short_term_gain_account and e["credit"] > 0
        ]
        assert len(stcg_credits) == 1
        assert stcg_credits[0]["credit"] == pytest.approx(81.01)

    def test_tao_fee_entries(self, wave_config):
        """TAO fees generate TAO Holdings entries, not clearing entries."""
        r = ReconciliationResult(
            year_month="2025-09",
            subledger_proceeds=15100.99,
            gross_tao_to_usd=15073.34,
            cash_fees=2.12,
            tao_fees=55.64,
            kraken_withdrawn_usd=15070.0,
            price_difference=27.65,
            clearing_correction=29.77,
            rewards_tao=0.0,
            rewards_value_usd=0.0,
            ending_cash_usd=1.22,
            ending_tao=0.0,
            ending_tao_value_usd=0.0,
            tao_deposited=50.0,
            tao_sold=50.0,
            usd_deposited=0.0,
        )
        entries = _generate_journal_entries(r, wave_config)

        tao_fee_entries = [
            e
            for e in entries
            if e["account"] == WAVE_EXCHANGE_FEE_ACCOUNT
            and e.get("section") == "tao_holdings"
        ]
        assert len(tao_fee_entries) == 1
        assert tao_fee_entries[0]["debit"] == pytest.approx(55.64)

        tao_holding_entries = [
            e for e in entries if e["account"] == wave_config.tao_asset_account
        ]
        assert len(tao_holding_entries) == 1
        assert tao_holding_entries[0]["credit"] == pytest.approx(55.64)

    def test_staking_to_tao_holdings(self, wave_config):
        """Staking rewards go to TAO Holdings (cost basis at FMV), not clearing."""
        r = ReconciliationResult(
            year_month="2025-07",
            subledger_proceeds=0.0,
            gross_tao_to_usd=0.0,
            cash_fees=0.0,
            tao_fees=0.0,
            kraken_withdrawn_usd=0.0,
            price_difference=0.0,
            clearing_correction=0.0,
            rewards_tao=0.01,
            rewards_value_usd=3.55,
            ending_cash_usd=0.0,
            ending_tao=0.01,
            ending_tao_value_usd=3.5,
            tao_deposited=0.0,
            tao_sold=0.0,
            usd_deposited=0.0,
        )
        entries = _generate_journal_entries(r, wave_config)

        reward_entries = [
            e for e in entries if e["account"] == WAVE_STAKING_INCOME_KRAKEN
        ]
        assert len(reward_entries) == 1
        assert reward_entries[0]["credit"] == pytest.approx(3.55)

        tao_entries = [
            e for e in entries if e["account"] == wave_config.tao_asset_account
        ]
        assert len(tao_entries) == 1
        assert tao_entries[0]["debit"] == pytest.approx(3.55)

        clearing_entries = [
            e for e in entries if e["account"] == wave_config.transfer_proceeds_account
        ]
        assert len(clearing_entries) == 0

    def test_mixed_tao_fees_and_staking(self, wave_config):
        """TAO fees and staking rewards combine in TAO Holdings."""
        r = ReconciliationResult(
            year_month="2025-09",
            subledger_proceeds=0.0,
            gross_tao_to_usd=0.0,
            cash_fees=0.0,
            tao_fees=55.64,
            kraken_withdrawn_usd=0.0,
            price_difference=0.0,
            clearing_correction=0.0,
            rewards_tao=0.01,
            rewards_value_usd=3.55,
            ending_cash_usd=0.0,
            ending_tao=0.01,
            ending_tao_value_usd=3.5,
            tao_deposited=0.0,
            tao_sold=0.0,
            usd_deposited=0.0,
        )
        entries = _generate_journal_entries(r, wave_config)

        tao_section = [e for e in entries if e.get("section") == "tao_holdings"]
        assert len(tao_section) == 3

        tao_holding = [
            e for e in tao_section if e["account"] == wave_config.tao_asset_account
        ]
        assert len(tao_holding) == 1
        assert tao_holding[0]["credit"] == pytest.approx(52.09)

    def test_no_entries_for_zero_gap(self, wave_config):
        r = ReconciliationResult(
            year_month="2025-07",
            subledger_proceeds=0.0,
            gross_tao_to_usd=0.0,
            cash_fees=0.0,
            tao_fees=0.0,
            kraken_withdrawn_usd=0.0,
            price_difference=0.0,
            clearing_correction=0.0,
            rewards_tao=0.0,
            rewards_value_usd=0.0,
            ending_cash_usd=0.0,
            ending_tao=0.0,
            ending_tao_value_usd=0.0,
            tao_deposited=0.0,
            tao_sold=0.0,
            usd_deposited=0.0,
        )
        entries = _generate_journal_entries(r, wave_config)
        assert len(entries) == 0

    def test_clearing_entries_are_balanced(self, wave_config):
        """Clearing entries should have equal total debits and credits."""
        r = ReconciliationResult(
            year_month="2025-07",
            subledger_proceeds=3520.0,
            gross_tao_to_usd=3481.01,
            cash_fees=9.89,
            tao_fees=0.0,
            kraken_withdrawn_usd=3300.0,
            price_difference=38.99,
            clearing_correction=48.88,
            rewards_tao=0.0,
            rewards_value_usd=0.0,
            ending_cash_usd=150.0,
            ending_tao=0.01,
            ending_tao_value_usd=3.5,
            tao_deposited=10.0,
            tao_sold=9.99,
            usd_deposited=0.0,
        )
        entries = _generate_journal_entries(r, wave_config)
        clearing = [e for e in entries if e.get("section") == "clearing"]

        total_debits = sum(e["debit"] for e in clearing)
        total_credits = sum(e["credit"] for e in clearing)
        assert total_debits == pytest.approx(total_credits)

    def test_tao_entries_are_balanced(self, wave_config):
        """TAO Holdings entries should have equal total debits and credits."""
        r = ReconciliationResult(
            year_month="2025-09",
            subledger_proceeds=0.0,
            gross_tao_to_usd=0.0,
            cash_fees=0.0,
            tao_fees=55.64,
            kraken_withdrawn_usd=0.0,
            price_difference=0.0,
            clearing_correction=0.0,
            rewards_tao=0.01,
            rewards_value_usd=3.55,
            ending_cash_usd=0.0,
            ending_tao=0.01,
            ending_tao_value_usd=3.5,
            tao_deposited=0.0,
            tao_sold=0.0,
            usd_deposited=0.0,
        )
        entries = _generate_journal_entries(r, wave_config)
        tao = [e for e in entries if e.get("section") == "tao_holdings"]

        total_debits = sum(e["debit"] for e in tao)
        total_credits = sum(e["credit"] for e in tao)
        assert total_debits == pytest.approx(total_credits)

    def test_all_entries_balanced(self, wave_config):
        """All journal entries combined should have equal total debits and credits."""
        r = ReconciliationResult(
            year_month="2025-07",
            subledger_proceeds=3520.0,
            gross_tao_to_usd=3481.01,
            cash_fees=9.89,
            tao_fees=4.89,
            kraken_withdrawn_usd=3300.0,
            price_difference=38.99,
            clearing_correction=48.88,
            rewards_tao=0.01,
            rewards_value_usd=0.03,
            ending_cash_usd=150.0,
            ending_tao=0.01,
            ending_tao_value_usd=3.5,
            tao_deposited=10.0,
            tao_sold=9.99,
            usd_deposited=0.0,
        )
        entries = _generate_journal_entries(r, wave_config)

        total_debits = sum(e["debit"] for e in entries)
        total_credits = sum(e["credit"] for e in entries)
        assert total_debits == pytest.approx(total_credits)
