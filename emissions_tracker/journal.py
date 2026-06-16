"""Journal entry generation for Wave accounting integration."""

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from emissions_tracker.config import WaveAccountSettings
from emissions_tracker.models import JournalEntry, SourceType


def aggregate_monthly_journal_entries(
    year_month: str,
    income_records: List[Dict[str, Any]],
    sales_records: List[Dict[str, Any]],
    expense_records: List[Dict[str, Any]],
    transfer_records: List[Dict[str, Any]],
    deposit_records: List[Dict[str, Any]],
    wave_config: WaveAccountSettings,
    start_ts: int,
    end_ts: int,
    tao_asset_account: Optional[str] = None,
    alpha_asset_account: Optional[str] = None,
) -> Tuple[List[JournalEntry], Dict[str, float]]:
    """Aggregate sheet data into monthly journal entries.

    Returns the list of ``JournalEntry`` rows plus summary metrics for logging.

    Args:
        tao_asset_account: Override for the TAO asset account name. Each
            tracker passes its own per-wallet account (e.g.
            ``"TAO Holdings - Contract"``).
        alpha_asset_account: Override for the ALPHA asset account name. Each
            tracker passes its own per-wallet account (e.g.
            ``"Alpha Holdings - Contract"``).
    """
    tao_account = tao_asset_account or "TAO Holdings"
    alpha_account = alpha_asset_account or "Alpha Holdings"

    account_totals: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"debit": 0.0, "credit": 0.0, "notes": []}
    )

    summary = {
        "contract_income": 0.0,
        "staking_income": 0.0,
        "sales_proceeds": 0.0,
        "sales_gain": 0.0,
        "sales_slippage": 0.0,
        "sales_fees": 0.0,
        "expense_total": 0.0,
        "expense_gain": 0.0,
        "transfer_gain": 0.0,
        "transfer_fees": 0.0,
        "deposit_total": 0.0,
    }

    gain_buckets: Dict[str, Dict[str, Any]] = {
        "Short-term": {"amount": 0.0, "notes": []},
        "Long-term": {"amount": 0.0, "notes": []},
    }

    def _add_amount(
        account: str, field: str, amount: float, note: Optional[str] = None
    ):
        if amount is None or amount == 0:
            return
        account_totals[account][field] += amount
        if note:
            account_totals[account]["notes"].append(note)

    # ------------------------- Income ---------------------------------------
    for record in income_records:
        ts = record.get("Timestamp")
        if ts is None:
            continue
        try:
            ts = int(ts)
        except (TypeError, ValueError):
            continue
        if ts < start_ts or ts >= end_ts:
            continue
        usd_fmv = record.get("USD FMV") or 0.0
        source_type = record.get("Source Type")
        note = record.get("Notes") or record.get("Lot ID")
        if source_type == SourceType.CONTRACT.value:
            summary["contract_income"] += usd_fmv
            _add_amount(
                alpha_account,
                "debit",
                usd_fmv,
                f"Contract lot {note}: ${usd_fmv:.2f}",
            )
            _add_amount(
                wave_config.contract_income_account,
                "credit",
                usd_fmv,
                f"Contract lot {note}: ${usd_fmv:.2f}",
            )
        elif source_type == SourceType.STAKING.value:
            summary["staking_income"] += usd_fmv
            _add_amount(
                alpha_account,
                "debit",
                usd_fmv,
                f"Staking lot {note}: ${usd_fmv:.2f}",
            )
            _add_amount(
                wave_config.staking_income_account,
                "credit",
                usd_fmv,
                f"Staking lot {note}: ${usd_fmv:.2f}",
            )
        elif source_type == SourceType.MINING.value:
            summary[
                "staking_income"
            ] += usd_fmv  # Add to staking_income summary for now
            _add_amount(
                alpha_account,
                "debit",
                usd_fmv,
                f"Mining lot {note}: ${usd_fmv:.2f}",
            )
            _add_amount(
                wave_config.mining_income_account,
                "credit",
                usd_fmv,
                f"Mining lot {note}: ${usd_fmv:.2f}",
            )
        elif source_type == SourceType.TRANSFER_IN.value:
            category = record.get("Category", "").strip()
            if not category:
                continue
            _add_amount(
                alpha_account,
                "debit",
                usd_fmv,
                f"Inbound transfer {note}: ${usd_fmv:.2f}",
            )
            _add_amount(
                category,
                "credit",
                usd_fmv,
                f"Inbound transfer {note}: ${usd_fmv:.2f}",
            )
        elif source_type == SourceType.OPENING_BALANCE.value:
            _add_amount(
                alpha_account,
                "debit",
                usd_fmv,
                f"Opening balance lot {note}: ${usd_fmv:.2f}",
            )
            _add_amount(
                "Opening Balance Equity",
                "credit",
                usd_fmv,
                f"Opening balance lot {note}: ${usd_fmv:.2f}",
            )

    # ------------------------- Sales (ALPHA -> TAO) -------------------------
    for sale in sales_records:
        ts = sale.get("Timestamp")
        if ts is None:
            continue
        try:
            ts = int(ts)
        except (TypeError, ValueError):
            continue
        if ts < start_ts or ts >= end_ts:
            continue
        proceeds = sale.get("USD Proceeds") or 0.0
        cost_basis = sale.get("Cost Basis") or 0.0
        gain_loss = sale.get("Realized Gain/Loss") or 0.0
        gain_type = sale.get("Gain Type") or "Short-term"
        sale_id = sale.get("Sale ID") or ""
        slippage_raw = sale.get("Slippage USD")
        try:
            slippage_usd = (
                float(slippage_raw) if slippage_raw not in (None, "") else 0.0
            )
        except (TypeError, ValueError):
            slippage_usd = 0.0
        fee_raw = sale.get("Network Fee (USD)")
        try:
            sale_fee_usd = float(fee_raw) if fee_raw not in (None, "") else 0.0
        except (TypeError, ValueError):
            sale_fee_usd = 0.0

        # Note: gain_loss already includes slippage (calculated in _record_alpha_sale)
        summary["sales_proceeds"] += proceeds
        summary["sales_gain"] += gain_loss
        summary["sales_slippage"] += slippage_usd

        _add_amount(
            tao_account,
            "debit",
            proceeds,
            f"Sale {sale_id}: TAO proceeds ${proceeds:.2f}",
        )
        _add_amount(
            alpha_account,
            "credit",
            cost_basis,
            f"Sale {sale_id}: ALPHA cost basis ${cost_basis:.2f}",
        )

        if sale_fee_usd:
            summary["sales_fees"] += sale_fee_usd
            fee_note = f"Sale {sale_id}: Network fee ${sale_fee_usd:.2f}"
            _add_amount(
                wave_config.blockchain_fee_account, "debit", sale_fee_usd, fee_note
            )
            _add_amount(tao_account, "credit", sale_fee_usd, fee_note)

        bucket = gain_buckets.setdefault(gain_type, {"amount": 0.0, "notes": []})
        bucket["amount"] += gain_loss
        note_parts = [f"Sale {sale_id}: ${gain_loss:.2f}"]
        if slippage_usd:
            note_parts.append(f"(incl. ${slippage_usd:.2f} slippage)")
        bucket["notes"].append(" ".join(note_parts))

    # ------------------------- Expenses (ALPHA payments) -------------------
    for expense in expense_records:
        ts = expense.get("Timestamp")
        if ts is None:
            continue
        try:
            ts = int(ts)
        except (TypeError, ValueError):
            continue
        if ts < start_ts or ts >= end_ts:
            continue

        category = expense.get("Category", "").strip()
        if not category:
            continue  # Should have been caught earlier, but skip uncategorized

        proceeds = expense.get("USD Proceeds") or 0.0
        cost_basis = expense.get("Cost Basis") or 0.0
        gain_loss = expense.get("Realized Gain/Loss") or 0.0
        gain_type = expense.get("Gain Type") or "Short-term"
        expense_id = expense.get("Expense ID") or ""
        fee_raw = expense.get("Network Fee (USD)")
        try:
            expense_fee_usd = float(fee_raw) if fee_raw not in (None, "") else 0.0
        except (TypeError, ValueError):
            expense_fee_usd = 0.0

        summary["expense_total"] += proceeds
        summary["expense_gain"] += gain_loss

        # Debit expense category (e.g., "Computer - Hosting")
        _add_amount(
            category, "debit", proceeds, f"Expense {expense_id}: ${proceeds:.2f}"
        )

        # Credit ALPHA asset for cost basis
        _add_amount(
            alpha_account,
            "credit",
            cost_basis,
            f"Expense {expense_id}: ALPHA cost basis ${cost_basis:.2f}",
        )

        # Handle network fees if any
        if expense_fee_usd:
            fee_note = f"Expense {expense_id}: Network fee ${expense_fee_usd:.2f}"
            _add_amount(
                wave_config.blockchain_fee_account, "debit", expense_fee_usd, fee_note
            )
            _add_amount(alpha_account, "credit", expense_fee_usd, fee_note)

        # Add gain/loss to appropriate bucket
        bucket = gain_buckets.setdefault(gain_type, {"amount": 0.0, "notes": []})
        bucket["amount"] += gain_loss
        bucket["notes"].append(f"Expense {expense_id}: ${gain_loss:.2f}")

    def _parse_fee_cost_basis(notes: str) -> float:
        if not notes:
            return 0.0
        for segment in notes.split("|"):
            token = segment.strip()
            if token.startswith("fee_cost_basis="):
                try:
                    return float(token.split("=", 1)[1])
                except ValueError:
                    return 0.0
        return 0.0

    def _get_transfer_fee_cost_basis(record: Dict[str, Any]) -> float:
        raw = record.get("Fee Cost Basis USD")
        if raw not in (None, ""):
            try:
                return float(raw)
            except (TypeError, ValueError):
                pass
        return _parse_fee_cost_basis(record.get("Notes") or "")

    # ------------------------- Transfers (TAO -> Kraken) --------------------
    for xfer in transfer_records:
        ts = xfer.get("Timestamp")
        if ts is None:
            continue
        try:
            ts = int(ts)
        except (TypeError, ValueError):
            continue
        if ts < start_ts or ts >= end_ts:
            continue
        proceeds = xfer.get("USD Proceeds") or 0.0
        cost_basis = xfer.get("Cost Basis") or 0.0
        gain_loss = xfer.get("Realized Gain/Loss") or 0.0
        gain_type = xfer.get("Gain Type") or "Short-term"
        transfer_id = xfer.get("Transfer ID") or ""
        fee_cost_basis = _get_transfer_fee_cost_basis(xfer)

        summary["transfer_gain"] += gain_loss

        _add_amount(
            wave_config.transfer_proceeds_account,
            "debit",
            proceeds,
            f"Transfer {transfer_id}: USD proceeds ${proceeds:.2f}",
        )
        _add_amount(
            tao_account,
            "credit",
            cost_basis,
            f"Transfer {transfer_id}: TAO disposed ${cost_basis:.2f}",
        )
        if fee_cost_basis:
            _add_amount(
                tao_account,
                "credit",
                fee_cost_basis,
                f"Transfer {transfer_id}: Fee cost basis ${fee_cost_basis:.2f}",
            )
            _add_amount(
                wave_config.blockchain_fee_account,
                "debit",
                fee_cost_basis,
                f"Transfer {transfer_id}: On-chain fees ${fee_cost_basis:.2f}",
            )
            summary["transfer_fees"] += fee_cost_basis

        bucket = gain_buckets.setdefault(gain_type, {"amount": 0.0, "notes": []})
        bucket["amount"] += gain_loss
        bucket["notes"].append(f"Transfer {transfer_id}: ${gain_loss:.2f}")

    # ------------------------- Deposits (TAO received) --------------------------
    for deposit in deposit_records:
        ts = deposit.get("Timestamp")
        if ts is None:
            continue
        try:
            ts = int(ts)
        except (TypeError, ValueError):
            continue
        if ts < start_ts or ts >= end_ts:
            continue

        category = deposit.get("Category", "").strip()
        if not category:
            continue
        usd_fmv = deposit.get("USD FMV") or 0.0
        deposit_id = deposit.get("Deposit ID") or ""

        summary["deposit_total"] += usd_fmv

        _add_amount(
            tao_account,
            "debit",
            usd_fmv,
            f"Deposit {deposit_id}: TAO deposit ${usd_fmv:.2f}",
        )

        _add_amount(
            category,
            "credit",
            usd_fmv,
            f"Deposit {deposit_id}: ${usd_fmv:.2f}",
        )

    gain_account_map = {
        "Short-term": wave_config.short_term_gain_account,
        "Long-term": wave_config.long_term_gain_account,
    }
    loss_account_map = {
        "Short-term": wave_config.short_term_loss_account,
        "Long-term": wave_config.long_term_loss_account,
    }

    for gain_type, data in gain_buckets.items():
        amount = round(data["amount"], 10)
        if abs(amount) < 0.00001:
            continue
        notes = ", ".join(data["notes"][:5])

        gain_account = gain_account_map.get(
            gain_type, wave_config.short_term_gain_account
        )
        loss_account = loss_account_map.get(
            gain_type, wave_config.short_term_loss_account
        )

        # If using the same account for gains and losses, record net amount once
        if gain_account == loss_account:
            if amount > 0:
                _add_amount(
                    gain_account,
                    "credit",
                    amount,
                    notes or f"{gain_type} net gain ${amount:.2f}",
                )
            else:
                _add_amount(
                    gain_account,
                    "debit",
                    abs(amount),
                    notes or f"{gain_type} net loss ${abs(amount):.2f}",
                )
        else:
            # Separate accounts: record gain or loss to appropriate account
            if amount > 0:
                _add_amount(
                    gain_account,
                    "credit",
                    amount,
                    notes or f"{gain_type} gain total ${amount:.2f}",
                )
            else:
                _add_amount(
                    loss_account,
                    "debit",
                    abs(amount),
                    notes or f"{gain_type} loss total ${abs(amount):.2f}",
                )

    account_desc_map = {
        alpha_account: f"Alpha asset activity for {year_month}",
        wave_config.contract_income_account: f"Contract income for {year_month}",
        wave_config.staking_income_account: f"Staking emissions income for {year_month}",
        wave_config.mining_income_account: f"Mining emissions income for {year_month}",
        tao_account: f"TAO activity for {year_month}",
        wave_config.transfer_proceeds_account: (
            f"TAO transfer proceeds to Kraken for {year_month}"
        ),
        wave_config.blockchain_fee_account: (
            f"On-chain transaction fees for {year_month}"
        ),
        wave_config.business_checking_account: f"Business checking for {year_month}",
        wave_config.short_term_gain_account: (
            f"Net short-term capital gains for {year_month}"
        ),
        wave_config.short_term_loss_account: (
            f"Net short-term capital losses for {year_month}"
        ),
        wave_config.long_term_gain_account: (
            f"Net long-term capital gains for {year_month}"
        ),
        wave_config.long_term_loss_account: (
            f"Net long-term capital losses for {year_month}"
        ),
        "Opening Balance Equity": f"Opening balance equity for {year_month}",
    }

    entries: List[JournalEntry] = []
    for account, values in sorted(account_totals.items()):
        debit = round(values["debit"], 2)
        credit = round(values["credit"], 2)
        net = round(debit - credit, 2)
        if abs(net) < 0.005:
            continue
        n_items = len(values["notes"])
        description = account_desc_map.get(account, f"{account} for {year_month}")
        if n_items:
            description += f" ({n_items} entries)"
        entries.append(
            JournalEntry(
                month=year_month,
                entry_type="Monthly",
                account=account,
                debit=net if net > 0 else 0.0,
                credit=abs(net) if net < 0 else 0.0,
                description=description,
            )
        )

    # Final rounding guard: Wave occasionally rejects entries that differ by pennies
    total_debits = sum(e.debit for e in entries)
    total_credits = sum(e.credit for e in entries)
    rounding_diff = round(total_debits - total_credits, 2)
    if abs(rounding_diff) >= 0.01:
        # Prefer to nudge the short-term gain account since it already absorbs net P/L
        target_account = wave_config.short_term_gain_account
        target_entry = next((e for e in entries if e.account == target_account), None)
        if target_entry is None:
            target_entry = JournalEntry(
                month=year_month,
                entry_type="Monthly",
                account=target_account,
                debit=0.0,
                credit=0.0,
                description=f"Rounding adjustment for {year_month}",
            )
            entries.append(target_entry)

        note = f"rounding adjustment {rounding_diff:+.2f}"
        # If debits > credits (positive diff), we need to increase credits
        # If credits > debits (negative diff), we need to increase debits
        # BUT: adjust the side that already has a value, not create both sides
        if rounding_diff > 0:
            # Debits exceed credits, so we need more credits
            if target_entry.credit > 0:
                # Already has credits, add to them
                target_entry.credit = round(target_entry.credit + rounding_diff, 2)
            elif target_entry.debit > 0:
                # Has debits, reduce them instead
                target_entry.debit = round(target_entry.debit - rounding_diff, 2)
            else:
                # Empty entry, add credit
                target_entry.credit = round(rounding_diff, 2)
        else:
            # Credits exceed debits, so we need more debits
            if target_entry.debit > 0:
                # Already has debits, add to them
                target_entry.debit = round(target_entry.debit + abs(rounding_diff), 2)
            elif target_entry.credit > 0:
                # Has credits, reduce them instead
                target_entry.credit = round(target_entry.credit - abs(rounding_diff), 2)
            else:
                # Empty entry, add debit
                target_entry.debit = round(abs(rounding_diff), 2)
        if note not in target_entry.description:
            target_entry.description += (
                "; " if target_entry.description else ""
            ) + note

    return entries, summary


class JournalGenerator:
    """Generates Wave accounting journal entries from tracker sheet data."""

    def __init__(
        self,
        wave_config: WaveAccountSettings,
        sheet_accessor,
        tao_asset_account: Optional[str] = None,
        alpha_asset_account: Optional[str] = None,
    ):
        """
        Initialize the journal generator.

        Args:
            wave_config: Wave accounting settings with account names
            sheet_accessor: Object with methods to access sheet data:
                - income_sheet.get_all_records()
                - sales_sheet.get_all_records()
                - expenses_sheet.get_all_records()
                - transfers_sheet.get_all_records()
                - journal_sheet (for writing)
                - _append_rows_with_retry(sheet, rows)
            tao_asset_account: Per-wallet TAO account name override
            alpha_asset_account: Per-wallet ALPHA account name override
        """
        self.wave_config = wave_config
        self.sheets = sheet_accessor
        self.tao_asset_account = tao_asset_account
        self.alpha_asset_account = alpha_asset_account

    def generate_monthly(self, year: int, month: int) -> List[JournalEntry]:
        """Generate journal entries for a single month.

        Args:
            year: The year (e.g., 2025)
            month: The month (1-12)

        Returns:
            List of JournalEntry objects for the month
        """
        year_month = f"{year}-{month:02d}"

        # Calculate timestamp range for the month
        start_dt = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            end_dt = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end_dt = datetime(year, month + 1, 1, tzinfo=timezone.utc)

        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        # Read all sheet data
        income_records = self.sheets.income_sheet.get_all_records()
        sales_records = self.sheets.sales_sheet.get_all_records()
        expense_records = self.sheets.expenses_sheet.get_all_records()
        transfer_records = self.sheets.transfers_sheet.get_all_records()

        entries, summary = aggregate_monthly_journal_entries(
            year_month=year_month,
            income_records=income_records,
            sales_records=sales_records,
            expense_records=expense_records,
            transfer_records=transfer_records,
            wave_config=self.wave_config,
            start_ts=start_ts,
            end_ts=end_ts,
            tao_asset_account=self.tao_asset_account,
            alpha_asset_account=self.alpha_asset_account,
        )

        self._print_summary(year_month, len(entries), summary)
        return entries

    def generate_yearly(
        self, year: int, write_to_sheet: bool = True
    ) -> List[JournalEntry]:
        """Generate journal entries for all months in a year.

        Args:
            year: The year to generate entries for
            write_to_sheet: If True, write entries to the journal sheet

        Returns:
            List of all JournalEntry objects for the year
        """
        print(f"\nGenerating journal entries for {year}...")

        # Read all sheet data once (more efficient than reading per-month)
        income_records = self.sheets.income_sheet.get_all_records()
        sales_records = self.sheets.sales_sheet.get_all_records()
        expense_records = self.sheets.expenses_sheet.get_all_records()
        transfer_records = self.sheets.transfers_sheet.get_all_records()

        all_entries: List[JournalEntry] = []
        all_rows: List[List[Any]] = []

        for month in range(1, 13):
            year_month = f"{year}-{month:02d}"

            # Calculate timestamp range
            start_dt = datetime(year, month, 1, tzinfo=timezone.utc)
            if month == 12:
                end_dt = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                end_dt = datetime(year, month + 1, 1, tzinfo=timezone.utc)

            start_ts = int(start_dt.timestamp())
            end_ts = int(end_dt.timestamp())

            try:
                entries, summary = aggregate_monthly_journal_entries(
                    year_month=year_month,
                    income_records=income_records,
                    sales_records=sales_records,
                    expense_records=expense_records,
                    transfer_records=transfer_records,
                    wave_config=self.wave_config,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    tao_asset_account=self.tao_asset_account,
                    alpha_asset_account=self.alpha_asset_account,
                )

                if not entries:
                    continue

                # Collect rows for batch writing
                for entry in entries:
                    all_rows.append(entry.to_row())
                    all_entries.append(entry)

                self._print_summary(year_month, len(entries), summary)

            except ValueError as e:
                print(f"  Skipping {year_month}: {e}")
                continue

        # Batch write all journal entries
        if write_to_sheet and all_rows:
            print(f"\nWriting {len(all_rows)} journal entries to sheet...")
            self.sheets._append_rows_with_retry(self.sheets.journal_sheet, all_rows)
            print("✓ Journal entries written")

        print(f"\n✓ Generated {len(all_entries)} total journal entries for {year}")
        return all_entries

    def clear_sheet(self):
        """Clear all journal entries from the sheet (for regeneration)."""
        print("  Clearing Journal Entries sheet...")
        try:
            all_values = self.sheets.journal_sheet.get_all_values()
            if len(all_values) > 1:
                last_row = len(all_values)
                self.sheets.journal_sheet.batch_clear([f"A2:Z{last_row}"])
            print("  ✓ Journal Entries sheet cleared")
        except Exception as e:
            print(f"  Warning: Could not clear journal sheet: {e}")

    def _print_summary(
        self, year_month: str, entry_count: int, summary: Dict[str, float]
    ):
        """Print a summary of generated journal entries."""
        print(f"✓ Generated {entry_count} aggregated journal entries for {year_month}")
        print(f"  Contract Income: ${summary['contract_income']:.2f}")
        print(f"  Staking Income: ${summary['staking_income']:.2f}")
        print(f"  Sales Proceeds: ${summary['sales_proceeds']:.2f}")
        print(f"  Sales Gain/Loss: ${summary['sales_gain']:.2f}")
        print(f"  Sales Slippage (USD): ${summary['sales_slippage']:.2f}")
        print(f"  Sales Fees: ${summary['sales_fees']:.2f}")
        print(f"  Expense Total: ${summary['expense_total']:.2f}")
        print(f"  Expense Gain/Loss: ${summary['expense_gain']:.2f}")
        print(f"  Transfer Gain/Loss: ${summary['transfer_gain']:.2f}")
        print(f"  Transfer Fees (cost basis): ${summary['transfer_fees']:.2f}")
