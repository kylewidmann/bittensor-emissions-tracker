"""Unit tests for Kraken statement parser."""

from pathlib import Path

import pytest

from emissions_tracker.clients.kraken_statement import (
    KrakenMonthSummary,
    KrakenTrade,
    _extract_four_numbers,
    _extract_portfolio_balances,
    _parse_number,
    parse_statement_pdf,
    parse_transactions_csv,
)

SYNTHETIC_PDF = (
    Path(__file__).parent.parent
    / "data"
    / "kraken"
    / "synthetic_statement_2025-07-01-2025-08-01.pdf"
)
SYNTHETIC_CSV = (
    Path(__file__).parent.parent / "data" / "kraken" / "synthetic_transactions.csv"
)


class TestParseNumber:
    def test_simple(self):
        assert _parse_number("123.45") == 123.45

    def test_commas(self):
        assert _parse_number("14,955.7290") == 14955.729

    def test_negative(self):
        assert _parse_number("-3,300.0000") == -3300.0

    def test_integer(self):
        assert _parse_number("0") == 0.0


class TestExtractFourNumbers:
    def test_deposit_line(self):
        line = "Spot / Main 47.00000000 318.207 0 14,955.7290"
        nums = _extract_four_numbers(line)
        assert len(nums) == 4
        assert nums[0] == pytest.approx(47.0)
        assert nums[1] == pytest.approx(318.207)
        assert nums[2] == pytest.approx(0.0)
        assert nums[3] == pytest.approx(14955.729)

    def test_trade_sell_line(self):
        line = "Earn / Liquid -2.00000000 314.39 2.5151 -628.7800"
        nums = _extract_four_numbers(line)
        assert len(nums) == 4
        assert nums[0] == pytest.approx(-2.0)
        assert nums[2] == pytest.approx(2.5151)

    def test_trade_buy_line(self):
        line = "Spot / Main 635.7112 1 2.2250 635.7112"
        nums = _extract_four_numbers(line)
        assert len(nums) == 4
        assert nums[0] == pytest.approx(635.7112)
        assert nums[2] == pytest.approx(2.225)

    def test_withdrawal_line(self):
        line = "Spot / Main -15,100.4300 1 0 -15,100.4300"
        nums = _extract_four_numbers(line)
        assert len(nums) == 4
        assert nums[0] == pytest.approx(-15100.43)


class TestExtractPortfolioBalances:
    def test_basic_portfolio(self):
        text = """Cash Portfolio
Currency Symbol Wallet Amount Value (USD)
US Dollar USD Spot 150.5000 150.5000
Total 150.5000

Crypto Portfolio
Currency Symbol Wallet Amount Value (USD)
Bittensor TAO Spot 0.01000000 3.5000
Total 3.5000

Stocks Portfolio"""
        cash, tao_qty, tao_val = _extract_portfolio_balances(text)
        assert cash == pytest.approx(150.5)
        assert tao_qty == pytest.approx(0.01)
        assert tao_val == pytest.approx(3.5)

    def test_zero_balances(self):
        text = """Cash Portfolio
Currency Symbol Wallet Amount Value (USD)
Total 0

Crypto Portfolio
Currency Symbol Wallet Amount Value (USD)
Total 0

Stocks Portfolio"""
        cash, tao_qty, tao_val = _extract_portfolio_balances(text)
        assert cash == 0.0
        assert tao_qty == 0.0
        assert tao_val == 0.0


class TestParseSyntheticPDF:
    @pytest.fixture
    def summary(self):
        return parse_statement_pdf(SYNTHETIC_PDF)

    def test_year_month(self, summary):
        assert summary.year_month == "2025-07"

    def test_deposits(self, summary):
        assert len(summary.deposits) == 1
        assert summary.total_tao_deposited == pytest.approx(10.0)
        assert summary.deposits[0].date == "2025-07-01"

    def test_trades(self, summary):
        assert len(summary.trades) == 3
        assert summary.total_tao_sold == pytest.approx(9.99)

    def test_trade_details(self, summary):
        t1 = summary.trades[0]
        assert t1.tao_sold == pytest.approx(5.0)
        assert t1.usd_received == pytest.approx(1745.0)
        assert t1.fee_usd == pytest.approx(5.0)

    def test_trade_with_sell_side_fee(self, summary):
        t2 = summary.trades[1]
        assert t2.tao_sold == pytest.approx(3.0)
        assert t2.usd_received == pytest.approx(1043.5)
        assert t2.fee_usd == pytest.approx(3.5)

    def test_total_fees(self, summary):
        assert summary.total_fees_usd == pytest.approx(9.893)

    def test_withdrawals(self, summary):
        assert len(summary.withdrawals) == 1
        assert summary.total_withdrawn_usd == pytest.approx(3300.0)

    def test_rewards(self, summary):
        assert len(summary.rewards) == 1
        assert summary.total_rewards_tao == pytest.approx(0.01)

    def test_ending_balances(self, summary):
        assert summary.ending_cash_usd == pytest.approx(150.5)
        assert summary.ending_tao == pytest.approx(0.01)
        assert summary.ending_tao_value_usd == pytest.approx(3.5)

    def test_auto_allocate_ignored(self, summary):
        assert len(summary.deposits) == 1

    def test_gross_proceeds(self, summary):
        assert summary.gross_trade_proceeds == pytest.approx(3481.01)

    def test_net_proceeds(self, summary):
        expected_net = 3481.01 - 9.893
        assert summary.net_trade_proceeds == pytest.approx(expected_net)


class TestParseSyntheticCSV:
    @pytest.fixture
    def summary(self):
        return parse_transactions_csv(SYNTHETIC_CSV, "2025-07")

    def test_year_month(self, summary):
        assert summary.year_month == "2025-07"

    def test_deposits(self, summary):
        assert len(summary.deposits) == 1
        assert summary.total_tao_deposited == pytest.approx(10.0)

    def test_trades(self, summary):
        assert len(summary.trades) == 3
        assert summary.total_tao_sold == pytest.approx(9.99)

    def test_usd_received_matches_pdf(self, summary):
        assert summary.total_usd_received == pytest.approx(3481.01)

    def test_withdrawals(self, summary):
        assert len(summary.withdrawals) == 1
        assert summary.total_withdrawn_usd == pytest.approx(3300.0)

    def test_rewards(self, summary):
        assert len(summary.rewards) == 1
        assert summary.total_rewards_tao == pytest.approx(0.01)

    def test_month_filtering(self):
        aug = parse_transactions_csv(SYNTHETIC_CSV, "2025-08")
        assert len(aug.deposits) == 1
        assert aug.total_tao_deposited == pytest.approx(5.0)
        assert len(aug.trades) == 0

    def test_empty_month(self):
        jan = parse_transactions_csv(SYNTHETIC_CSV, "2025-01")
        assert len(jan.deposits) == 0
        assert len(jan.trades) == 0


class TestKrakenMonthSummaryProperties:
    def test_empty_summary(self):
        s = KrakenMonthSummary(year_month="2025-01")
        assert s.total_tao_deposited == 0.0
        assert s.total_tao_sold == 0.0
        assert s.total_usd_received == 0.0
        assert s.total_fees_usd == 0.0
        assert s.total_withdrawn_usd == 0.0
        assert s.total_rewards_tao == 0.0
        assert s.gross_trade_proceeds == 0.0
        assert s.net_trade_proceeds == 0.0

    def test_single_trade(self):
        s = KrakenMonthSummary(
            year_month="2025-01",
            trades=[
                KrakenTrade(
                    date="2025-01-01",
                    tao_sold=1.0,
                    usd_received=100.0,
                    fee_usd_cash=2.0,
                    fee_usd_tao=0.0,
                )
            ],
        )
        assert s.total_tao_sold == 1.0
        assert s.total_usd_received == 100.0
        assert s.total_fees_usd == 2.0
        assert s.total_cash_fees_usd == 2.0
        assert s.total_tao_fees_usd == 0.0
        assert s.gross_trade_proceeds == 100.0
        assert s.gross_tao_to_usd == 100.0
        assert s.net_trade_proceeds == 98.0
