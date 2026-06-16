#!/usr/bin/env python3
"""Reconciliation Summary Tool.

Queries TaoStats for on-chain wallet balances (ALPHA and TAO) at the end of
each month, and parses Kraken statement PDFs for ending USD balances.
Outputs tables for each tracker wallet and Kraken.
"""

import argparse
import contextlib
import io
import sys
from calendar import monthrange
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, TextIO, Tuple

from emissions_tracker.clients.kraken_statement import parse_statement_pdf
from emissions_tracker.clients.taostats import TaoStatsAPIClient
from emissions_tracker.config import TrackerSettings


def _end_of_month_utc(year: int, month: int) -> int:
    """Unix timestamp for 23:59:59 UTC on the last day of the month."""
    last_day = monthrange(year, month)[1]
    return int(
        datetime(year, month, last_day, 23, 59, 59, tzinfo=timezone.utc).timestamp()
    )


@contextlib.contextmanager
def _quiet():
    """Suppress stdout from noisy client calls."""
    old = sys.stdout
    sys.stdout = io.StringIO()
    try:
        yield
    finally:
        sys.stdout = old


def _get_tao_balance(
    client: TaoStatsAPIClient, address: str, year: int, month: int
) -> Optional[float]:
    """Get free TAO balance for an address at end of month."""
    eom = _end_of_month_utc(year, month)
    with _quiet():
        records = client.get_account_history(address, eom - 86400 * 2, eom)
    if not records:
        return None
    return records[-1].balance_free_tao


def _get_alpha_balance(
    client: TaoStatsAPIClient,
    netuid: int,
    hotkey: str,
    coldkey: str,
    year: int,
    month: int,
) -> Optional[Tuple[float, float]]:
    """Get staked ALPHA balance (ALPHA, TAO-equivalent) at end of month."""
    eom = _end_of_month_utc(year, month)
    with _quiet():
        records = client.get_stake_balance_history(
            netuid, hotkey, coldkey, eom - 86400 * 2, eom
        )
    if not records:
        return None
    last = records[-1]
    return (last.balance_as_alpha_float, last.balance_as_tao_float)


def _get_tao_price(client: TaoStatsAPIClient, year: int, month: int) -> float:
    """Get TAO price in USD at end of month."""
    eom = _end_of_month_utc(year, month)
    with _quiet():
        price = client.get_price_at_timestamp("TAO", eom)
    return price


def _emit(out: TextIO, line: str = ""):
    """Write a line to the output stream."""
    out.write(line + "\n")


def run():
    parser = argparse.ArgumentParser(
        description="Reconciliation summary: on-chain balances and Kraken statement balances"
    )
    parser.add_argument(
        "--year", type=int, default=2025, help="Year to reconcile (default: 2025)"
    )
    parser.add_argument(
        "--pdf-dir",
        type=str,
        default="local/kraken/statements",
        help="Directory containing Kraken statement PDFs",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Write tables to a text file (in addition to console output)",
    )
    args = parser.parse_args()
    year = args.year

    config = TrackerSettings()
    print("Initializing TaoStats API client...")
    client = TaoStatsAPIClient()

    wallets: List[Dict] = []

    wallets.append(
        {
            "label": "Contract",
            "coldkey": config.payout_coldkey_ss58,
            "hotkey": config.validator_ss58,
            "has_alpha": True,
        }
    )

    if config.miner_coldkey_ss58 and config.miner_hotkey_ss58:
        wallets.append(
            {
                "label": "Mining",
                "coldkey": config.miner_coldkey_ss58,
                "hotkey": config.miner_hotkey_ss58,
                "has_alpha": True,
            }
        )

    if config.payment_coldkey_ss58:
        wallets.append(
            {
                "label": "Payment",
                "coldkey": config.payment_coldkey_ss58,
                "hotkey": None,
                "has_alpha": False,
            }
        )

    months = list(range(1, 13))
    netuid = config.subnet_id
    price_cache: Dict[int, float] = {}

    # Collect all table lines so we can write them to console + file
    table_lines: List[str] = []

    def tbl(line: str = ""):
        table_lines.append(line)

    # --- Pre-fetch TAO prices (one per month, shared across wallets) ---
    print(f"\nFetching TAO prices for {year}...")
    for month in months:
        price_cache[month] = _get_tao_price(client, year, month)
        sys.stdout.write(".")
        sys.stdout.flush()
    print(" done\n")

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    tbl(f"Reconciliation Summary — {year}")
    tbl(f"Generated: {generated}")
    tbl()

    # --- On-Chain Wallet Balances ---
    for wallet in wallets:
        label = wallet["label"]
        coldkey = wallet["coldkey"]
        hotkey = wallet["hotkey"]
        has_alpha = wallet["has_alpha"]

        print(f"  Fetching {label} wallet balances...", end="", flush=True)

        rows: List[str] = []
        for month in months:
            month_str = f"{year}-{month:02d}"
            tao_price = price_cache[month]

            tao_balance = _get_tao_balance(client, coldkey, year, month)
            sys.stdout.write(".")
            sys.stdout.flush()

            if has_alpha:
                alpha_result = _get_alpha_balance(
                    client, netuid, hotkey, coldkey, year, month
                )
                if alpha_result is None and tao_balance is None:
                    continue

                alpha_qty = alpha_result[0] if alpha_result else 0.0
                alpha_tao = alpha_result[1] if alpha_result else 0.0
                alpha_usd = alpha_tao * tao_price
                tao_bal = tao_balance if tao_balance is not None else 0.0
                tao_usd = tao_bal * tao_price

                if alpha_qty < 0.0001 and tao_bal < 0.0001:
                    continue

                rows.append(
                    f"  {month_str:<10s}  {alpha_qty:>12.4f}  {tao_bal:>12.4f}"
                    f"  ${tao_price:>8,.2f}  ${alpha_usd:>10,.2f}  ${tao_usd:>8,.2f}"
                )
            else:
                if tao_balance is None:
                    continue
                tao_bal = tao_balance
                tao_usd = tao_bal * tao_price
                if tao_bal < 0.0001:
                    continue
                rows.append(
                    f"  {month_str:<10s}  {tao_bal:>12.4f}"
                    f"  ${tao_price:>8,.2f}  ${tao_usd:>8,.2f}"
                )

        print(" done")

        tbl("=" * 80)
        tbl(f"  {label} Wallet ({coldkey[:12]}...)")
        tbl("=" * 80)

        if has_alpha:
            tbl(
                f"  {'Month':<10s}  {'ALPHA':>12s}  {'TAO (free)':>12s}"
                f"  {'TAO Price':>10s}  {'ALPHA (USD)':>12s}  {'TAO (USD)':>10s}"
            )
        else:
            tbl(
                f"  {'Month':<10s}  {'TAO (free)':>12s}"
                f"  {'TAO Price':>10s}  {'TAO (USD)':>10s}"
            )
        tbl(f"  {'-' * 76}")

        for row in rows:
            tbl(row)

        tbl()

    # --- Kraken Statement Balances ---
    pdf_dir = Path(args.pdf_dir)
    if pdf_dir.exists():
        pdfs = sorted(pdf_dir.glob("kraken_spot_account_statement_*.pdf"))
        if pdfs:
            tbl("=" * 80)
            tbl("  Kraken Account (from statements)")
            tbl("=" * 80)
            tbl(
                f"  {'Month':<10s}  {'Cash (USD)':>12s}"
                f"  {'TAO':>14s}  {'TAO Val (USD)':>14s}"
            )
            tbl(f"  {'-' * 56}")

            for pdf_path in pdfs:
                summary = parse_statement_pdf(pdf_path)
                if not summary.year_month.startswith(str(year)):
                    continue
                tbl(
                    f"  {summary.year_month:<10s}"
                    f"  ${summary.ending_cash_usd:>10,.2f}"
                    f"  {summary.ending_tao:>14.8f}"
                    f"  ${summary.ending_tao_value_usd:>12,.2f}"
                )

            tbl()
        else:
            print(f"No Kraken statement PDFs found in {pdf_dir}")
    else:
        print(f"Kraken PDF directory not found: {pdf_dir}")

    # --- Output ---
    print()
    for line in table_lines:
        print(line)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            for line in table_lines:
                f.write(line + "\n")
        print(f"\n📄 Saved to {out_path}")

    print("✓ Done!")
    return 0


if __name__ == "__main__":
    sys.exit(run() or 0)
