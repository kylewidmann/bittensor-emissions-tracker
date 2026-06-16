"""Kraken monthly statement parser.

Parses Kraken spot account statement PDFs and transactions.csv to produce
a unified KrakenMonthSummary for reconciliation against sub-ledger data.
"""

import csv
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pdfplumber


@dataclass
class KrakenTrade:
    """A single trade pair extracted from a statement.

    The fee is always USD regardless of which trade line it appears on.
    Pre-Sep statements show it on the Buy USD line (cash fee); Sep+ show
    it on the Sell TAO line (TAO consumed to pay fee).
    ``tao_side_fee_usd`` captures the sell-side portion so the reconciler
    can distinguish fees that reduced TAO vs fees that reduced USD.
    """

    date: str
    tao_sold: float
    usd_received: float
    fee_usd: float
    tao_side_fee_usd: float = 0.0


@dataclass
class KrakenDeposit:
    """A TAO deposit into Kraken."""

    date: str
    tao_amount: float


@dataclass
class KrakenUsdDeposit:
    """A USD deposit into Kraken."""

    date: str
    usd_amount: float


@dataclass
class KrakenWithdrawal:
    """A USD withdrawal from Kraken."""

    date: str
    usd_amount: float


@dataclass
class KrakenReward:
    """A staking reward from Kraken Earn."""

    date: str
    tao_amount: float
    fee_tao: float
    value_usd: float


@dataclass
class KrakenMonthSummary:
    """Aggregated Kraken activity for a single month."""

    year_month: str
    deposits: List[KrakenDeposit] = field(default_factory=list)
    trades: List[KrakenTrade] = field(default_factory=list)
    withdrawals: List[KrakenWithdrawal] = field(default_factory=list)
    rewards: List[KrakenReward] = field(default_factory=list)
    usd_deposits: List[KrakenUsdDeposit] = field(default_factory=list)
    ending_cash_usd: float = 0.0
    ending_tao: float = 0.0
    ending_tao_value_usd: float = 0.0

    @property
    def total_tao_deposited(self) -> float:
        return sum(d.tao_amount for d in self.deposits)

    @property
    def total_tao_sold(self) -> float:
        return sum(t.tao_sold for t in self.trades)

    @property
    def total_usd_received(self) -> float:
        return sum(t.usd_received for t in self.trades)

    @property
    def total_fees_usd(self) -> float:
        return sum(t.fee_usd for t in self.trades)

    @property
    def total_tao_side_fees_usd(self) -> float:
        """Fees from the sell (TAO) side — paid by consuming TAO, not USD."""
        return sum(t.tao_side_fee_usd for t in self.trades if t.tao_sold > 0)

    @property
    def total_cash_fees_usd(self) -> float:
        """Fees from the buy (USD) side — deducted from USD proceeds."""
        return self.total_fees_usd - self.total_tao_side_fees_usd

    @property
    def gross_tao_to_usd(self) -> float:
        """Sum of USD received from TAO→USD trades only (excludes buybacks)."""
        return sum(t.usd_received for t in self.trades if t.tao_sold > 0)

    @property
    def total_usd_deposited(self) -> float:
        return sum(d.usd_amount for d in self.usd_deposits)

    @property
    def total_withdrawn_usd(self) -> float:
        return sum(w.usd_amount for w in self.withdrawals)

    @property
    def total_rewards_tao(self) -> float:
        return sum(r.tao_amount for r in self.rewards)

    @property
    def total_rewards_fee_tao(self) -> float:
        return sum(r.fee_tao for r in self.rewards)

    @property
    def gross_trade_proceeds(self) -> float:
        """USD credited from all trades (fees tracked separately)."""
        return self.total_usd_received

    @property
    def net_trade_proceeds(self) -> float:
        """USD credited minus all fees."""
        return self.total_usd_received - self.total_fees_usd


def _parse_number(s: str) -> float:
    """Parse a number string that may contain commas."""
    return float(s.replace(",", ""))


def _extract_portfolio_balances(page_text: str) -> tuple[float, float, float]:
    """Extract ending cash USD, TAO quantity, and TAO value from page 1.

    Returns (cash_usd, tao_quantity, tao_value_usd).
    """
    cash_usd = 0.0
    tao_qty = 0.0
    tao_val = 0.0

    lines = page_text.split("\n")
    in_cash = False
    in_crypto = False
    for line in lines:
        if "Cash Portfolio" in line:
            in_cash = True
            in_crypto = False
            continue
        if "Crypto Portfolio" in line:
            in_cash = False
            in_crypto = True
            continue
        if "Stocks Portfolio" in line or "Activity" in line:
            in_cash = False
            in_crypto = False
            continue

        if in_cash and "US Dollar" in line:
            parts = line.split()
            try:
                cash_usd = _parse_number(parts[-1])
            except (ValueError, IndexError):
                pass

        if in_crypto and ("Bittensor" in line or "TAO" in line):
            parts = line.split()
            try:
                tao_val = _parse_number(parts[-1])
                tao_qty = _parse_number(parts[-2])
            except (ValueError, IndexError):
                pass

    return cash_usd, tao_qty, tao_val


def _extract_four_numbers(line: str) -> List[float]:
    """Extract the 4 trailing numbers (amount, price, fee, value) from a data line.

    The data line has format: 'Wallet_Part  Amount  Price  Fee  Value'
    e.g. 'Earn / Liquid -2.00000000 314.39 2.5151 -628.7800'
    e.g. 'Spot / Main 635.7112 1 2.2250 635.7112'
    """
    number_pattern = re.compile(r"-?[\d,]+\.?\d*")
    matches = number_pattern.findall(line)
    numbers = [_parse_number(m) for m in matches]
    if len(numbers) >= 4:
        return numbers[-4:]
    return numbers


def parse_statement_pdf(pdf_path: str | Path) -> KrakenMonthSummary:
    """Parse a Kraken monthly statement PDF into a KrakenMonthSummary.

    Each activity entry in the PDF spans 3 text lines:
      Line 1 (date):    '2025-07-01 Trade Sell Bittensor'
      Line 2 (data):    'Earn / Liquid -2.00000000 314.39 0 -636.4140'
      Line 3 (time):    '22:16:28 TAO'

    Trade pairs appear as consecutive Trade Sell (TAO) + Trade Buy (USD).
    Fees appear on the Buy side for May-Aug (USD fees) and on the Sell side
    for Sep+ (TAO fees converted to USD by Kraken).
    """
    pdf_path = Path(pdf_path)
    pdf = pdfplumber.open(pdf_path)

    filename = pdf_path.name
    period_match = re.search(r"(\d{4})-(\d{2})-\d{2}-\d{4}-\d{2}-\d{2}", filename)
    if period_match:
        year_month = f"{period_match.group(1)}-{period_match.group(2)}"
    else:
        year_month = "unknown"

    page0_text = pdf.pages[0].extract_text() or ""
    cash_usd, tao_qty, tao_val = _extract_portfolio_balances(page0_text)

    summary = KrakenMonthSummary(
        year_month=year_month,
        ending_cash_usd=cash_usd,
        ending_tao=tao_qty,
        ending_tao_value_usd=tao_val,
    )

    all_lines: List[str] = []
    for page in pdf.pages:
        text = page.extract_text() or ""
        for line in text.split("\n"):
            all_lines.append(line)

    _parse_activity_entries(all_lines, summary)
    pdf.close()
    return summary


def _parse_activity_entries(lines: List[str], summary: KrakenMonthSummary) -> None:
    """Group lines into 3-line entries and classify them.

    Activity entries are 3 lines:
      1) date line:  starts with YYYY-MM-DD
      2) data line:  wallet + 4 numbers (amount, price, fee, value)
      3) time line:  starts with HH:MM:SS + continuation text (ticker / type suffix)
    """
    activity_started = False
    entries: list[dict] = []

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if not activity_started:
            if line.startswith("Date (UTC)") and "Type" in line:
                activity_started = True
            i += 1
            continue

        if line.startswith("Date (UTC)") and "Type" in line:
            i += 1
            continue

        if line.startswith("Page ") and " of " in line:
            i += 1
            continue

        if line.startswith("DISCLAIMER"):
            break

        date_match = re.match(r"^(\d{4}-\d{2}-\d{2})\s+(.+)$", line)
        if date_match:
            date_str = date_match.group(1)
            type_text = date_match.group(2).strip()

            data_line = ""
            time_text = ""
            if i + 1 < len(lines):
                data_line = lines[i + 1].strip()
            if i + 2 < len(lines):
                time_text = lines[i + 2].strip()

            numbers = _extract_four_numbers(data_line)

            entries.append(
                {
                    "date": date_str,
                    "type_text": type_text,
                    "numbers": numbers,
                    "time_text": time_text,
                    "data_line": data_line,
                }
            )
            i += 3
            continue

        i += 1

    _classify_entries(entries, summary)


def _classify_entries(entries: list[dict], summary: KrakenMonthSummary) -> None:
    """Classify parsed entries and populate the summary."""
    pending_sell: Optional[dict] = None

    for entry in entries:
        type_text = entry["type_text"]
        date = entry["date"]
        nums = entry["numbers"]
        time_text = entry["time_text"]

        if "Auto Allocate" in time_text:
            pending_sell = None
            continue

        if "Reward" in time_text:
            if len(nums) >= 4:
                tao_amt = nums[0]
                price = nums[1]
                fee_usd = nums[2]
                val_usd = nums[3]
                fee_tao = fee_usd / price if price > 0 else 0.0
            else:
                tao_amt = nums[0] if nums else 0.0
                fee_tao = 0.0
                val_usd = 0.0
            summary.rewards.append(
                KrakenReward(
                    date=date,
                    tao_amount=tao_amt,
                    fee_tao=fee_tao,
                    value_usd=val_usd,
                )
            )
            pending_sell = None
            continue

        is_tao = "Bittensor" in type_text
        is_usd = "Dollar" in type_text or "USD" in type_text

        is_instant_spend = "Instant" in type_text and "Spend" in time_text
        is_instant_receive = "Instant" in type_text and "Receive" in time_text

        if "Deposit" in type_text and is_tao:
            tao_amount = nums[0] if nums else 0.0
            if tao_amount > 0:
                summary.deposits.append(KrakenDeposit(date=date, tao_amount=tao_amount))
            pending_sell = None

        elif "Trade Sell" in type_text or is_instant_spend:
            pending_sell = {
                "date": date,
                "numbers": nums,
                "is_tao": is_tao,
            }

        elif (
            "Trade Buy" in type_text or is_instant_receive
        ) and pending_sell is not None:
            sell_is_tao = pending_sell["is_tao"]
            buy_is_tao = is_tao

            sell_fee = (
                pending_sell["numbers"][2] if len(pending_sell["numbers"]) > 2 else 0.0
            )
            buy_fee = nums[2] if len(nums) > 2 else 0.0

            if sell_is_tao and not buy_is_tao:
                tao_sold = (
                    abs(pending_sell["numbers"][0]) if pending_sell["numbers"] else 0.0
                )
                usd_received = nums[0] if nums else 0.0
                summary.trades.append(
                    KrakenTrade(
                        date=pending_sell["date"],
                        tao_sold=tao_sold,
                        usd_received=usd_received,
                        fee_usd=buy_fee + sell_fee,
                        tao_side_fee_usd=sell_fee,
                    )
                )
            elif not sell_is_tao and buy_is_tao:
                tao_bought = nums[0] if nums else 0.0
                usd_spent = (
                    abs(pending_sell["numbers"][0]) if pending_sell["numbers"] else 0.0
                )
                summary.trades.append(
                    KrakenTrade(
                        date=pending_sell["date"],
                        tao_sold=-tao_bought,
                        usd_received=-usd_spent,
                        fee_usd=sell_fee + buy_fee,
                        tao_side_fee_usd=buy_fee,
                    )
                )

            pending_sell = None

        elif "Withdrawal" in type_text:
            usd_amount = abs(nums[0]) if nums else 0.0
            if usd_amount > 0:
                summary.withdrawals.append(
                    KrakenWithdrawal(date=date, usd_amount=usd_amount)
                )
            pending_sell = None

        elif "Deposit" in type_text:
            if is_usd:
                usd_amount = nums[0] if nums else 0.0
                if usd_amount > 0:
                    summary.usd_deposits.append(
                        KrakenUsdDeposit(date=date, usd_amount=usd_amount)
                    )
            pending_sell = None

        else:
            pending_sell = None


def parse_transactions_csv(
    csv_path: str | Path,
    year_month: str,
) -> KrakenMonthSummary:
    """Parse Kraken transactions.csv for a given month.

    This is the fallback input source when PDFs are unavailable.
    Produces the same KrakenMonthSummary structure as the PDF parser.
    """
    csv_path = Path(csv_path)
    year, month = year_month.split("-")
    target_year = int(year)
    target_month = int(month)

    summary = KrakenMonthSummary(year_month=year_month)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_str = row.get("Date", "")
            try:
                dt = datetime.fromisoformat(date_str)
            except ValueError:
                continue

            if dt.year != target_year or dt.month != target_month:
                continue

            date_short = dt.strftime("%Y-%m-%d")
            tx_type = row.get("Type", "").lower().strip()

            if tx_type == "deposit":
                currency = row.get("Received Currency", "").lower()
                qty = float(row.get("Received Quantity", 0) or 0)
                if currency == "tao" and qty > 0:
                    summary.deposits.append(
                        KrakenDeposit(date=date_short, tao_amount=qty)
                    )
                elif currency == "usd" and qty > 0:
                    summary.usd_deposits.append(
                        KrakenUsdDeposit(date=date_short, usd_amount=qty)
                    )

            elif tx_type == "trade":
                sent_currency = row.get("Sent Currency", "").lower()
                recv_currency = row.get("Received Currency", "").lower()
                fee_amount = float(row.get("Fee Amount", 0) or 0)
                fee_currency = row.get("Fee Currency", "").lower()

                if sent_currency == "tao" and recv_currency == "usd":
                    tao_sold = float(row.get("Sent Quantity", 0) or 0)
                    usd_received = float(row.get("Received Quantity", 0) or 0)

                    if fee_currency == "usd":
                        fee_usd = fee_amount
                    elif fee_currency == "tao" and tao_sold > 0:
                        fee_usd = fee_amount * (usd_received / tao_sold)
                    else:
                        fee_usd = 0.0

                    summary.trades.append(
                        KrakenTrade(
                            date=date_short,
                            tao_sold=tao_sold,
                            usd_received=usd_received,
                            fee_usd=fee_usd,
                        )
                    )
                elif sent_currency == "usd" and recv_currency == "tao":
                    tao_bought = float(row.get("Received Quantity", 0) or 0)
                    usd_spent = float(row.get("Sent Quantity", 0) or 0)
                    if fee_currency == "usd":
                        fee_usd = fee_amount
                    elif fee_currency == "tao" and tao_bought > 0:
                        fee_usd = fee_amount * (usd_spent / tao_bought)
                    else:
                        fee_usd = 0.0
                    summary.trades.append(
                        KrakenTrade(
                            date=date_short,
                            tao_sold=-tao_bought,
                            usd_received=-usd_spent,
                            fee_usd=fee_usd,
                        )
                    )

            elif tx_type == "withdrawal":
                usd_sent = float(row.get("Sent Quantity", 0) or 0)
                currency = row.get("Sent Currency", "").lower()
                if currency == "usd" and usd_sent > 0:
                    summary.withdrawals.append(
                        KrakenWithdrawal(date=date_short, usd_amount=usd_sent)
                    )

            elif tx_type == "income":
                tao_amt = float(row.get("Received Quantity", 0) or 0)
                fee_val = float(row.get("Fee Amount", 0) or 0)
                fee_curr = row.get("Fee Currency", "").lower()
                fee_tao = fee_val if fee_curr == "tao" else 0.0
                summary.rewards.append(
                    KrakenReward(
                        date=date_short,
                        tao_amount=tao_amt,
                        fee_tao=fee_tao,
                        value_usd=0.0,
                    )
                )

    return summary
