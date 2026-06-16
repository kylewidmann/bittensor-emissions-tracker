#!/usr/bin/env python3
"""Kraken Exchange Clearing Reconciliation Tool.

Reads sub-ledger Transfers from Google Sheets and parses Kraken monthly
statement PDFs (or transactions.csv) to compute the gap between the two,
then generates correcting journal entries for Wave accounting.

Each month is reconciled independently using only that month's statement data
and sub-ledger proceeds. No prior-month carry-forward is needed.
"""

import argparse
import csv
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from emissions_tracker.clients.kraken_statement import (
    KrakenMonthSummary,
    parse_statement_pdf,
    parse_transactions_csv,
)
from emissions_tracker.config import TrackerSettings, WaveAccountSettings
from emissions_tracker.models import TaoTransfer
from emissions_tracker.sheet_names import TRANSFERS_SHEET

WAVE_EXCHANGE_FEE_ACCOUNT = "Exchange Fees - Kraken"
WAVE_STAKING_INCOME_KRAKEN = "Staking Income - Kraken"


@dataclass
class ReconciliationResult:
    """Result of reconciling one month between sub-ledger and Kraken."""

    year_month: str
    subledger_proceeds: float
    gross_tao_to_usd: float
    cash_fees: float
    kraken_withdrawn_usd: float
    price_difference: float
    clearing_correction: float
    rewards_tao: float
    rewards_value_usd: float
    ending_cash_usd: float
    ending_tao: float
    ending_tao_value_usd: float
    tao_deposited: float
    tao_sold: float
    usd_deposited: float
    journal_entries: List[Dict] = field(default_factory=list)


def _read_subledger_transfers(
    config: TrackerSettings,
) -> List[TaoTransfer]:
    """Read TaoTransfer records from all three Google Sheets sub-ledgers."""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        config.tracker_google_credentials, scope
    )
    client = gspread.authorize(creds)

    transfers: List[TaoTransfer] = []
    sheet_ids = [
        ("Contract/Emissions", config.tracker_sheet_id),
        ("Mining", config.mining_tracker_sheet_id),
        ("Payment", config.payment_tracker_sheet_id),
    ]

    for label, sheet_id in sheet_ids:
        if not sheet_id:
            continue
        try:
            sheet = client.open_by_key(sheet_id)
            ws = sheet.worksheet(TRANSFERS_SHEET)
            records = ws.get_all_records()
            for record in records:
                try:
                    transfers.append(TaoTransfer.from_record(record))
                except (ValueError, KeyError) as e:
                    print(f"  Warning: skipping malformed {label} transfer: {e}")
            print(f"  Loaded {len(records)} transfers from {label} tracker")
        except Exception as e:
            print(f"  Warning: could not load {label} transfers: {e}")

    return transfers


def _filter_transfers_for_month(
    transfers: List[TaoTransfer], year_month: str
) -> List[TaoTransfer]:
    """Filter transfers to those whose UTC date falls within the given month.

    Kraken statement periods use UTC boundaries, so we must convert
    sub-ledger timestamps to UTC before checking the month.
    """
    year, month = year_month.split("-")
    target_year = int(year)
    target_month = int(month)

    result = []
    for t in transfers:
        dt = datetime.fromtimestamp(t.timestamp, tz=timezone.utc)
        if dt.year == target_year and dt.month == target_month:
            result.append(t)
    return result


def reconcile_month(
    transfers: List[TaoTransfer],
    kraken: KrakenMonthSummary,
    wave_config: WaveAccountSettings,
) -> ReconciliationResult:
    """Reconcile sub-ledger transfers against Kraken data for one month.

    Each month is standalone. The statement tracks which side each fee was
    on: sell-side fees consumed TAO (already reducing gross_tao_to_usd),
    buy-side fees reduced USD cash. We adjust the gross upward by the exact
    sell-side fee total so price_diff reflects only the true execution
    price difference, avoiding double-counting.
    """
    subledger_proceeds = round(sum(round(t.usd_proceeds, 2) for t in transfers), 2)

    gross_tao_to_usd = kraken.gross_tao_to_usd
    cash_fees = kraken.total_fees_usd
    tao_side_fees = round(kraken.total_tao_side_fees_usd, 2)

    adjusted_gross = gross_tao_to_usd + tao_side_fees
    price_diff = round(subledger_proceeds - adjusted_gross, 2)
    clearing_correction = round(cash_fees + price_diff, 2)

    result = ReconciliationResult(
        year_month=kraken.year_month,
        subledger_proceeds=subledger_proceeds,
        gross_tao_to_usd=gross_tao_to_usd,
        cash_fees=cash_fees,
        kraken_withdrawn_usd=kraken.total_withdrawn_usd,
        price_difference=price_diff,
        clearing_correction=clearing_correction,
        rewards_tao=kraken.total_rewards_tao,
        rewards_value_usd=sum(r.value_usd for r in kraken.rewards),
        ending_cash_usd=kraken.ending_cash_usd,
        ending_tao=kraken.ending_tao,
        ending_tao_value_usd=kraken.ending_tao_value_usd,
        tao_deposited=kraken.total_tao_deposited,
        tao_sold=kraken.total_tao_sold,
        usd_deposited=kraken.total_usd_deposited,
    )

    result.journal_entries = _generate_journal_entries(result, wave_config)
    return result


def _generate_journal_entries(
    r: ReconciliationResult,
    wave_config: WaveAccountSettings,
) -> List[Dict]:
    """Generate correcting journal entries for the reconciliation gap.

    All fees are a single entry (regardless of whether paid from USD or TAO).
    The double-counting is resolved upstream in reconcile_month by adjusting
    the gross for TAO consumed, so price_diff is clean.
    Staking rewards flow through TAO Holdings / Staking Income at FMV.
    """
    clearing = wave_config.transfer_proceeds_account
    stcg = wave_config.short_term_gain_account
    tao_holdings = wave_config.kraken_tao_asset_account
    month_str = r.year_month
    date = f"{month_str}-01"

    entries: List[Dict] = []
    components: List[Dict] = []

    if r.cash_fees > 0.005:
        components.append(
            {
                "account": WAVE_EXCHANGE_FEE_ACCOUNT,
                "amount": round(r.cash_fees, 2),
                "description": f"Kraken trading fees for {month_str}",
            }
        )

    if abs(r.price_difference) > 0.005:
        if r.price_difference > 0:
            desc = (
                f"Brokerage price loss for {month_str}"
                f" (sub-ledger priced higher than Kraken execution)"
            )
        else:
            desc = (
                f"Brokerage price gain for {month_str}"
                f" (Kraken execution higher than sub-ledger price)"
            )
        components.append(
            {
                "account": stcg,
                "amount": round(r.price_difference, 2),
                "description": desc,
            }
        )

    if components:
        clearing_net = 0.0
        for comp in components:
            amt = comp["amount"]
            clearing_net -= amt
            entries.append(
                {
                    "date": date,
                    "account": comp["account"],
                    "debit": amt if amt > 0 else 0.0,
                    "credit": abs(amt) if amt < 0 else 0.0,
                    "description": comp["description"],
                    "section": "clearing",
                }
            )

        clearing_net = round(clearing_net, 2)
        desc_parts = []
        if r.cash_fees > 0.005:
            desc_parts.append(f"fees ${r.cash_fees:,.2f}")
        if abs(r.price_difference) > 0.005:
            if r.price_difference > 0:
                desc_parts.append(f"price loss ${r.price_difference:,.2f}")
            else:
                desc_parts.append(f"price gain ${abs(r.price_difference):,.2f}")

        entries.append(
            {
                "date": date,
                "account": clearing,
                "debit": clearing_net if clearing_net > 0 else 0.0,
                "credit": abs(clearing_net) if clearing_net < 0 else 0.0,
                "description": (
                    f"Kraken clearing adjustment for {month_str}"
                    f" ({', '.join(desc_parts)})"
                ),
                "section": "clearing",
            }
        )

    rewards_usd = round(r.rewards_value_usd, 2)
    if rewards_usd > 0.005:
        entries.append(
            {
                "date": date,
                "account": tao_holdings,
                "debit": rewards_usd,
                "credit": 0.0,
                "description": (
                    f"Kraken staking rewards for {month_str}"
                    f" ({r.rewards_tao:.8f} TAO at FMV)"
                ),
                "section": "staking",
            }
        )
        entries.append(
            {
                "date": date,
                "account": WAVE_STAKING_INCOME_KRAKEN,
                "debit": 0.0,
                "credit": rewards_usd,
                "description": (
                    f"Kraken staking income for {month_str}"
                    f" ({r.rewards_tao:.8f} TAO at FMV)"
                ),
                "section": "staking",
            }
        )

    return entries


def print_reconciliation_report(results: List[ReconciliationResult]) -> None:
    """Print a human-readable reconciliation summary with journal entries."""
    print("\n" + "=" * 80)
    print("KRAKEN EXCHANGE CLEARING RECONCILIATION REPORT")
    print("=" * 80)

    total_fees = 0.0
    total_price_diff = 0.0
    total_clearing_corr = 0.0
    all_entries: List[Dict] = []

    for r in results:
        print(f"\n--- {r.year_month} ---")
        print(f"  TAO deposited:           {r.tao_deposited:>12.4f}")
        print(f"  TAO sold:                {r.tao_sold:>12.4f}")
        print(f"  Sub-ledger proceeds:     ${r.subledger_proceeds:>11,.2f}")
        print(f"  TAO->USD gross proceeds: ${r.gross_tao_to_usd:>11,.2f}")
        print(f"  Trading fees (USD):      ${r.cash_fees:>11,.2f}")
        print(f"  USD withdrawn:           ${r.kraken_withdrawn_usd:>11,.2f}")

        price_sign = "loss" if r.price_difference > 0 else "gain"
        print(
            f"  Price difference:        ${r.price_difference:>11,.2f}  ({price_sign})"
        )
        print(f"  Clearing correction:     ${r.clearing_correction:>11,.2f}")

        if r.rewards_tao > 0:
            print(
                f"  Staking rewards:         {r.rewards_tao:>12.8f} TAO"
                f" (${r.rewards_value_usd:,.4f})"
            )
        print(f"  Ending cash balance:     ${r.ending_cash_usd:>11,.2f}")
        print(
            f"  Ending TAO balance:      {r.ending_tao:>12.8f} TAO"
            f" (${r.ending_tao_value_usd:,.4f})"
        )

        if r.journal_entries:
            print(f"\n  Journal Entries for {r.year_month}:")
            print(f"  {'Account':<35s} {'Debit':>10s} {'Credit':>10s}   Description")
            print(f"  {'-' * 90}")
            for e in r.journal_entries:
                debit_str = f"${e['debit']:,.2f}" if e["debit"] else ""
                credit_str = f"${e['credit']:,.2f}" if e["credit"] else ""
                print(
                    f"  {e['account']:<35s} {debit_str:>10s} {credit_str:>10s}"
                    f"   {e['description']}"
                )

        if not r.journal_entries:
            print(f"\n  No correcting entries needed for {r.year_month}.")

        if r.usd_deposited > 0.005:
            print(
                f"\n  NOTE: ${r.usd_deposited:,.2f} in USD deposits detected."
                f" Book separately: DR Exchange Clearing / CR Business Checking."
            )

        all_entries.extend(r.journal_entries)
        total_fees += r.cash_fees
        total_price_diff += r.price_difference
        total_clearing_corr += r.clearing_correction

    print(f"\n{'=' * 80}")
    print("YEAR TOTALS")
    print(f"  Total trading fees:      ${total_fees:>11,.2f}")
    price_sign = "net loss" if total_price_diff > 0 else "net gain"
    print(f"  Total price difference:  ${total_price_diff:>11,.2f}  ({price_sign})")
    print(f"  Total clearing correct.: ${total_clearing_corr:>11,.2f}")

    if all_entries:
        print(f"\n{'=' * 120}")
        print("ALL JOURNAL ENTRIES")
        print(f"{'=' * 120}")
        print(
            f"  {'Date':<12s} {'Account':<35s}"
            f" {'Debit':>10s} {'Credit':>10s}   Description"
        )
        print(f"  {'-' * 116}")
        prev_month = None
        for e in all_entries:
            entry_month = e["date"][:7]
            if prev_month and entry_month != prev_month:
                print(f"  {'':.<116s}")
            prev_month = entry_month
            debit_str = f"${e['debit']:,.2f}" if e["debit"] else ""
            credit_str = f"${e['credit']:,.2f}" if e["credit"] else ""
            print(
                f"  {e['date']:<12s} {e['account']:<35s}"
                f" {debit_str:>10s} {credit_str:>10s}"
                f"   {e['description']}"
            )
        total_d = sum(e["debit"] for e in all_entries)
        total_c = sum(e["credit"] for e in all_entries)
        print(f"  {'-' * 116}")
        print(f"  {'TOTAL':<12s} {'':<35s} ${total_d:>9,.2f} ${total_c:>9,.2f}")

    print(f"{'=' * 120}\n")


def write_wave_journal_csv(
    results: List[ReconciliationResult],
    output_path: str | Path,
) -> None:
    """Write correcting journal entries to a Wave-importable CSV."""
    output_path = Path(output_path)
    all_entries: List[Dict] = []
    for r in results:
        all_entries.extend(r.journal_entries)

    if not all_entries:
        print("No journal entries to write.")
        return

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["date", "account", "debit", "credit", "description"],
            extrasaction="ignore",
        )
        writer.writeheader()
        for entry in all_entries:
            row = {
                "date": entry["date"],
                "account": entry["account"],
                "debit": f"{entry['debit']:.2f}" if entry["debit"] else "",
                "credit": f"{entry['credit']:.2f}" if entry["credit"] else "",
                "description": entry["description"],
            }
            writer.writerow(row)

    print(f"Wrote {len(all_entries)} journal entries to {output_path}")


def run():
    parser = argparse.ArgumentParser(
        description="Kraken Exchange Clearing Reconciliation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Reconcile a single month using a PDF statement
  track-kraken --month 2025-07 --pdf local/kraken/statements/kraken_spot_account_statement_2025-07-01-2025-08-01.pdf

  # Reconcile all months using the transactions CSV
  track-kraken --csv local/kraken/transactions.csv

  # Reconcile a range of months from CSV
  track-kraken --csv local/kraken/transactions.csv --start 2025-05 --end 2025-12

  # Reconcile using a directory of PDF statements
  track-kraken --pdf-dir local/kraken/statements/

  # Skip Google Sheets (sub-ledger) read and use only Kraken data
  track-kraken --csv local/kraken/transactions.csv --no-sheets

  # Output correcting journal entries to CSV
  track-kraken --pdf-dir local/kraken/statements/ --journal-csv output/kraken_journal.csv
        """,
    )

    input_group = parser.add_argument_group("Kraken data source (pick one)")
    input_group.add_argument(
        "--pdf",
        type=str,
        default=None,
        help="Path to a single Kraken monthly statement PDF",
    )
    input_group.add_argument(
        "--pdf-dir",
        type=str,
        default=None,
        help="Directory containing Kraken monthly statement PDFs",
    )
    input_group.add_argument(
        "--csv",
        type=str,
        default=None,
        help="Path to Kraken transactions.csv (fallback for batch reconciliation)",
    )

    parser.add_argument(
        "--month",
        type=str,
        default=None,
        help="Month to reconcile in YYYY-MM format (required with --pdf)",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start month in YYYY-MM format (inclusive, for --csv or --pdf-dir)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End month in YYYY-MM format (inclusive, for --csv or --pdf-dir)",
    )
    parser.add_argument(
        "--no-sheets",
        action="store_true",
        help="Skip Google Sheets reads (sub-ledger proceeds will be 0)",
    )
    parser.add_argument(
        "--journal-csv",
        type=str,
        default=None,
        help="Path to write correcting Wave journal entries CSV",
    )

    args = parser.parse_args()

    if not args.pdf and not args.pdf_dir and not args.csv:
        parser.error("At least one of --pdf, --pdf-dir, or --csv is required")

    transfers: List[TaoTransfer] = []
    if not args.no_sheets:
        try:
            config = TrackerSettings()
            print("Reading sub-ledger transfers from Google Sheets...")
            transfers = _read_subledger_transfers(config)
            print(f"  Total transfers loaded: {len(transfers)}")
        except Exception as e:
            print(f"Warning: Could not load sub-ledger data: {e}")
            print("Continuing with Kraken data only (sub-ledger proceeds = 0)")

    wave_config = WaveAccountSettings()
    summaries: List[KrakenMonthSummary] = []

    if args.pdf:
        s = parse_statement_pdf(args.pdf)
        summaries.append(s)

    elif args.pdf_dir:
        pdf_dir = Path(args.pdf_dir)
        pdfs = sorted(pdf_dir.glob("kraken_spot_account_statement_*.pdf"))
        if not pdfs:
            print(f"No statement PDFs found in {pdf_dir}")
            return 1
        for pdf_path in pdfs:
            s = parse_statement_pdf(pdf_path)
            if args.start and s.year_month < args.start:
                continue
            if args.end and s.year_month > args.end:
                continue
            summaries.append(s)

    elif args.csv:
        start = args.start or "2025-01"
        end = args.end or "2025-12"
        sy, sm = start.split("-")
        ey, em = end.split("-")
        y, m = int(sy), int(sm)
        end_y, end_m = int(ey), int(em)
        while (y, m) <= (end_y, end_m):
            ym = f"{y}-{m:02d}"
            s = parse_transactions_csv(args.csv, ym)
            if s.trades or s.deposits or s.withdrawals:
                summaries.append(s)
            m += 1
            if m > 12:
                m = 1
                y += 1

    if not summaries:
        print("No Kraken data found for the specified period.")
        return 1

    print(f"\nReconciling {len(summaries)} month(s)...")
    results: List[ReconciliationResult] = []
    for kraken_summary in summaries:
        month_transfers = _filter_transfers_for_month(
            transfers, kraken_summary.year_month
        )
        subledger_tao = round(sum(t.tao_amount for t in month_transfers), 4)
        kraken_tao = round(kraken_summary.total_tao_deposited, 4)
        if abs(subledger_tao - kraken_tao) > 0.001:
            print(
                f"  WARNING [{kraken_summary.year_month}]: Sub-ledger TAO "
                f"({subledger_tao:.4f}) != Kraken deposits "
                f"({kraken_tao:.4f}). Check for transfers that "
                f"crossed a UTC month boundary."
            )
        result = reconcile_month(month_transfers, kraken_summary, wave_config)
        results.append(result)

    print_reconciliation_report(results)

    if args.journal_csv:
        output_path = Path(args.journal_csv)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        write_wave_journal_csv(results, output_path)

    return 0


if __name__ == "__main__":
    sys.exit(run() or 0)
