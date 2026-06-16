import time
from abc import abstractmethod
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

import backoff

from emissions_tracker.clients.price import PriceClient
from emissions_tracker.clients.wallet import WalletClientInterface
from emissions_tracker.exceptions import DuplicateExtrinsicError, PriceNotAvailableError
from emissions_tracker.models import (
    AlphaLot,
    AlphaLotConsumption,
    AlphaSale,
    CostBasisMethod,
    DisposalEvent,
    DisposalType,
    GainType,
    LotStatus,
    SourceType,
    TaoLot,
    TaoLotConsumption,
    TaoStatsDelegation,
    TaoStatsStakeBalance,
    TaoStatsTransfer,
    TaoTransfer,
)
from emissions_tracker.utils import col_idx_to_letter


def _row_ts(row: list, ts_idx: int) -> int:
    """Extract a timestamp integer from a raw row value list."""
    try:
        return int(row[ts_idx])
    except (IndexError, ValueError, TypeError):
        return 0


SECONDS_PER_DAY = 86400
RAO_PER_TAO = 10**9


def _is_rate_limit_error(e: Exception) -> bool:
    """Check if an exception is a Google Sheets rate limit error."""
    error_str = str(e)
    error_type = type(e).__name__
    return (
        "429" in error_str or "Quota exceeded" in error_str or "APIError" in error_type
    )


class BittensorTracker:

    def __init__(
        self,
        wallet_client: WalletClientInterface,
        price_client: PriceClient,
    ):
        self.wallet_client = wallet_client
        self.price_client = price_client
        self._initialize()

    @abstractmethod
    def _initialize(self): ...

    @abstractmethod
    def run(self, start_time: int, end_time: Optional[int] = None): ...

    @staticmethod
    def _resolve_time_window(
        label: str,
        last_timestamp: int,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Tuple[Optional[int], Optional[int]]:
        """Determine the (start_time, end_time) timestamps for a processing window.

        Args:
            label: Description of what's being processed (for error messages)
            last_timestamp: Last processed timestamp from sheet state
            start_time: Explicit start time (overrides last_timestamp if provided)
            end_time: Explicit end time (defaults to now if not provided)

        Returns:
            Tuple of (start_time, end_time) as Unix timestamps, or (None, None) if
            the requested range has already been fully processed

        Raises:
            ValueError: If no start_time provided and no last_timestamp exists
        """
        # End time defaults to now
        resolved_end = end_time if end_time is not None else int(time.time())

        # If explicit start_time provided, check if range overlaps with already processed data
        if start_time is not None:
            # If we've already processed data up to or past the requested end time, skip entirely
            if last_timestamp >= resolved_end:
                return None, None

            # If last_timestamp is within the requested range, continue from there
            if last_timestamp >= start_time:
                return last_timestamp + 1, resolved_end

            # Otherwise use the explicit start_time
            return start_time, resolved_end

        # Otherwise, continue from last processed timestamp
        if last_timestamp > 0:
            return last_timestamp + 1, resolved_end

        raise ValueError(
            f"No previous {label} timestamp found; please provide --start-date to seed the tracker."
        )

    # -------------------------------------------------------------------------
    # Sheet Operations (with retry logic for rate limiting)
    # -------------------------------------------------------------------------

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=5,
        max_time=180,
        base=10,
        factor=5,
        giveup=lambda e: not _is_rate_limit_error(e),
        on_backoff=lambda details: print(
            f"  Warning: opening sheet failed (attempt {details['tries']}), retrying in {details['wait']:.1f}s..."
        ),
    )
    def _open_sheet_with_retry(self, sheet_id: str):
        """Open a Google Sheet by ID with retry logic for rate limiting."""
        return self.sheets_client.open_by_key(sheet_id)

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=5,
        max_time=180,
        base=10,
        factor=5,
        giveup=lambda e: not _is_rate_limit_error(e),
        on_backoff=lambda details: print(
            f"  Warning: get records failed (attempt {details['tries']}), retrying in {details['wait']:.1f}s..."
        ),
    )
    def _get_records_with_retry(self, worksheet):
        """Get all records from a worksheet with retry logic for rate limiting."""
        return worksheet.get_all_records()

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=5,
        max_time=180,
        base=10,
        factor=5,
        giveup=lambda e: not _is_rate_limit_error(e),
        on_backoff=lambda details: print(
            f"  Warning: append rows failed (attempt {details['tries']}), retrying in {details['wait']:.1f}s..."
        ),
    )
    def _append_rows_with_retry(self, worksheet, rows: List[List[Any]]):
        """Append rows to a worksheet with retry logic for rate limiting."""
        worksheet.append_rows(rows, value_input_option="RAW")

    def _sort_sheet_by_timestamp(
        self, worksheet, timestamp_col: int, label: str, range_str: str = "A2:Z"
    ):
        """Sort a worksheet by a timestamp column (ascending) excluding header row."""
        try:
            worksheet.sort((timestamp_col, "asc"), range=range_str)
        except Exception as e:
            print(f"  Warning: Could not sort {label} sheet: {e}")

    # -------------------------------------------------------------------------
    # Regenerate helpers (shared by ContractTracker and MiningTracker)
    # -------------------------------------------------------------------------

    def _replace_sheet_data(
        self,
        worksheet,
        keep_rows: list,
        total: int,
        deleted: int,
        label: str,
        reason: str,
    ) -> int:
        """Clear a sheet and rewrite it with only the filtered rows.

        Args:
            worksheet: The gspread worksheet.
            keep_rows: Data rows to keep (headers excluded).
            total: Row count before filtering.
            deleted: Number of rows removed.
            label: Sheet name for logging.
            reason: Filter description for the log message.

        Returns:
            Number of rows deleted.
        """
        if deleted == 0:
            return 0

        headers = worksheet.row_values(1)
        worksheet.clear()
        if keep_rows:
            worksheet.update("A1", [headers] + keep_rows)
        else:
            worksheet.update("A1", [headers])
        print(f"  ✓ Cleared {deleted} {label.lower()} rows ({reason})")
        return deleted

    def _delete_sheet_rows_where_timestamp_gte(
        self, worksheet, timestamp_col: str, min_timestamp: int, label: str
    ) -> int:
        """Delete rows where record[timestamp_col] >= min_timestamp. Returns count deleted."""
        try:
            all_values = worksheet.get_all_values()
        except Exception as e:
            print(f"  Warning: Could not read {label} sheet: {e}")
            return 0
        if len(all_values) <= 1:
            return 0
        headers = all_values[0]
        try:
            ts_idx = headers.index(timestamp_col)
        except ValueError:
            print(f"  Warning: Column '{timestamp_col}' not found in {label}")
            return 0
        data_rows = all_values[1:]
        keep = [r for r in data_rows if _row_ts(r, ts_idx) < min_timestamp]
        deleted = len(data_rows) - len(keep)
        return self._replace_sheet_data(
            worksheet,
            keep,
            len(data_rows),
            deleted,
            label,
            f"timestamp >= {min_timestamp}",
        )

    def _delete_sheet_rows_where_timestamp_between(
        self, worksheet, timestamp_col: str, min_ts: int, max_ts: int, label: str
    ) -> int:
        """Delete rows where min_ts <= record[timestamp_col] <= max_ts. Returns count deleted."""
        try:
            all_values = worksheet.get_all_values()
        except Exception as e:
            print(f"  Warning: Could not read {label} sheet: {e}")
            return 0
        if len(all_values) <= 1:
            return 0
        headers = all_values[0]
        try:
            ts_idx = headers.index(timestamp_col)
        except ValueError:
            print(f"  Warning: Column '{timestamp_col}' not found in {label}")
            return 0
        data_rows = all_values[1:]
        keep = [r for r in data_rows if not (min_ts <= _row_ts(r, ts_idx) <= max_ts)]
        deleted = len(data_rows) - len(keep)
        return self._replace_sheet_data(
            worksheet, keep, len(data_rows), deleted, label, "timestamp in range"
        )

    def _delete_sheet_rows_where_timestamp_gt(
        self, worksheet, timestamp_col: str, max_timestamp: int, label: str
    ) -> int:
        """Delete rows where record[timestamp_col] > max_timestamp. Returns count deleted."""
        try:
            all_values = worksheet.get_all_values()
        except Exception as e:
            print(f"  Warning: Could not read {label} sheet: {e}")
            return 0
        if len(all_values) <= 1:
            return 0
        headers = all_values[0]
        try:
            ts_idx = headers.index(timestamp_col)
        except ValueError:
            print(f"  Warning: Column '{timestamp_col}' not found in {label}")
            return 0
        data_rows = all_values[1:]
        keep = [r for r in data_rows if _row_ts(r, ts_idx) <= max_timestamp]
        deleted = len(data_rows) - len(keep)
        return self._replace_sheet_data(
            worksheet,
            keep,
            len(data_rows),
            deleted,
            label,
            f"timestamp > {max_timestamp}",
        )

    def _reset_surviving_alpha_lots(self, income_sheet) -> int:
        """Reset all ALPHA lots on a sheet to full remaining / Open status.

        Used after disposals are cleared so that surviving lots reflect no
        consumption.  Returns the number of lots reset.
        """
        try:
            records = income_sheet.get_all_records()
        except Exception as e:
            print(f"  Warning: Could not load income sheet: {e}")
            return 0
        headers = AlphaLot.sheet_headers()
        rao_col = col_idx_to_letter("Alpha RAO Remaining", headers)
        status_col = col_idx_to_letter("Status", headers)
        updates = []
        for idx, rec in enumerate(records, start=2):
            if rec.get("Alpha RAO", 0) <= 0:
                continue
            updates.append({"range": f"{rao_col}{idx}", "values": [[rec["Alpha RAO"]]]})
            updates.append({"range": f"{status_col}{idx}", "values": [["Open"]]})
        if updates:
            income_sheet.batch_update(updates, value_input_option="RAW")
        return len(updates) // 2

    def _reset_surviving_tao_lots(self, tao_lots_sheet) -> int:
        """Reset all TAO lots on a sheet to full remaining / Open status.

        Returns the number of lots reset.
        """
        try:
            records = tao_lots_sheet.get_all_records()
        except Exception as e:
            print(f"  Warning: Could not load TAO lots sheet: {e}")
            return 0
        headers = TaoLot.sheet_headers()
        rao_col = col_idx_to_letter("TAO RAO Remaining", headers)
        status_col = col_idx_to_letter("Status", headers)
        updates = []
        for idx, rec in enumerate(records, start=2):
            if rec.get("TAO RAO", 0) <= 0:
                continue
            updates.append({"range": f"{rao_col}{idx}", "values": [[rec["TAO RAO"]]]})
            updates.append({"range": f"{status_col}{idx}", "values": [["Open"]]})
        if updates:
            tao_lots_sheet.batch_update(updates, value_input_option="RAW")
        return len(updates) // 2

    @abstractmethod
    def _get_regen_disposal_sheets(self) -> List[Tuple[Any, str, str]]:
        """Return list of (worksheet, label, timestamp_column_name) for disposal sheets to clear from start_time."""
        ...

    @abstractmethod
    def _get_regen_income_sheets(self) -> List[Tuple[Any, str]]:
        """Return (worksheet, label) pairs for ALPHA income sheets to clear from start_time."""
        ...

    @abstractmethod
    def _reset_regen_timestamps(self, start_time: int) -> None:
        """Set last_* timestamps to start_time - 1 so processing resumes from start_time."""
        ...

    def regenerate_from(self, start_time: int, end_time: Optional[int] = None) -> None:
        """Delete all data from ``start_time`` onward and reload in-memory state.

        Opening-balance lots are always stamped at ``start_time - 1``, so
        using ``start_time - 1`` as the lot-deletion threshold catches them
        without affecting any earlier historical data.

        1. Delete income/TAO lots >= ``start_time - 1``.
        2. Delete disposal/deposit rows >= ``start_time``.
        3. If ``end_time`` given, also delete income/TAO lots past it.
        4. Reset surviving lots to full remaining.
        5. Reload in-memory data and counters from the now-pruned sheets.
        """
        resolved_end = end_time if end_time is not None else int(time.time())
        print(
            f"\n⚠️  Regenerating from {datetime.fromtimestamp(start_time, tz=timezone.utc).date()} "
            f"to {datetime.fromtimestamp(resolved_end, tz=timezone.utc).date()}..."
        )

        # start_time - 1 catches the opening-balance lot (always at start_time - 1)
        lot_threshold = start_time - 1

        # Delete income lots >= lot_threshold
        for income_ws, label in self._get_regen_income_sheets():
            self._delete_sheet_rows_where_timestamp_gte(
                income_ws, "Timestamp", lot_threshold, label
            )

        # Delete TAO lots >= lot_threshold
        self._delete_sheet_rows_where_timestamp_gte(
            self.tao_lots_sheet, "Timestamp", lot_threshold, self.tao_lots_sheet.title
        )

        # Delete disposal/deposit rows >= start_time
        for worksheet, label, ts_col in self._get_regen_disposal_sheets():
            self._delete_sheet_rows_where_timestamp_gte(
                worksheet, ts_col, start_time, label
            )

        # If end_date given, also delete lots past it
        if end_time is not None:
            for income_ws, label in self._get_regen_income_sheets():
                self._delete_sheet_rows_where_timestamp_gt(
                    income_ws, "Timestamp", end_time, label
                )
            self._delete_sheet_rows_where_timestamp_gt(
                self.tao_lots_sheet, "Timestamp", end_time, self.tao_lots_sheet.title
            )

        # Reset surviving lots (before start_time) to undo consumption by
        # now-deleted disposals
        alpha_reset = 0
        for income_ws, _ in self._get_regen_income_sheets():
            alpha_reset += self._reset_surviving_alpha_lots(income_ws)
        tao_reset = self._reset_surviving_tao_lots(self.tao_lots_sheet)
        if alpha_reset:
            print(f"  ✓ Reset {alpha_reset} surviving ALPHA lots")
        if tao_reset:
            print(f"  ✓ Reset {tao_reset} surviving TAO lots")

        self._reset_regen_timestamps(start_time)

        # Reload in-memory state from the now-pruned sheets
        self._reload_after_regenerate()

        print("✓ Regenerate complete\n")

    def _reload_after_regenerate(self):
        """Reload in-memory data and counters from sheets after regeneration."""
        self.alpha_lots.clear()
        self.tao_lots.clear()
        self.sales.clear()
        if hasattr(self, "expenses"):
            self.expenses.clear()
        if hasattr(self, "deposits"):
            self.deposits.clear()
        self.transfers.clear()
        if hasattr(self, "transfers_in"):
            self.transfers_in.clear()
        self._load_all_data_from_sheets()
        self._load_state()
        self._load_counters()
        print("  ✓ Reloaded state from sheets")

    # -------------------------------------------------------------------------
    # Lot Consumption (FIFO/HIFO strategies)
    # -------------------------------------------------------------------------

    def _consume_alpha_lots(
        self, amount_rao: int, timestamp: int
    ) -> tuple[list[AlphaLotConsumption], float]:
        """Consume ALPHA lots according to configured strategy.

        Args:
            amount_rao: Amount to consume in RAO
            timestamp: Timestamp of the consumption event
        Returns:
            Tuple of (consumed_lots list, total_basis_consumed)
        """
        # Sort lots by strategy
        if self.config.lot_strategy == CostBasisMethod.FIFO:
            # First In First Out - oldest first
            sorted_lots = sorted(self.alpha_lots, key=lambda x: x.timestamp)
        else:  # HIFO
            # Highest In First Out - highest basis first
            sorted_lots = sorted(
                self.alpha_lots, key=lambda x: x.usd_per_alpha, reverse=True
            )

        available_lots = [
            l
            for l in sorted_lots
            if l.alpha_rao_remaining > 0 and l.timestamp <= timestamp
        ]

        consumed_lots = []
        total_basis = 0.0
        remaining_needed = amount_rao

        for lot in available_lots:
            if remaining_needed <= 0:
                break

            if lot.alpha_rao_remaining <= 0:
                continue

            # Consume from this lot
            consume_amount = min(lot.alpha_rao_remaining, remaining_needed)
            consume_alpha = consume_amount / RAO_PER_TAO

            # Calculate pro-rata basis
            basis_consumed = (consume_amount / lot.alpha_rao) * lot.usd_fmv

            consumed_lots.append(
                AlphaLotConsumption(
                    lot_id=lot.lot_id,
                    alpha_consumed=consume_alpha,
                    cost_basis_consumed=basis_consumed,
                    acquisition_timestamp=lot.timestamp,
                )
            )

            # Update lot remaining
            lot.alpha_rao_remaining -= consume_amount
            if lot.alpha_rao_remaining == 0:
                lot.status = LotStatus.CLOSED
            else:
                lot.status = LotStatus.PARTIAL

            total_basis += basis_consumed
            remaining_needed -= consume_amount

        if remaining_needed > 0:
            raise ValueError(
                f"Insufficient ALPHA lots to consume {amount_rao / RAO_PER_TAO:.4f} ALPHA at timestamp {timestamp}. "
                f"Shortfall of {remaining_needed / RAO_PER_TAO:.4f} ALPHA. "
                f"Available lots (acquired before disposal): {len(available_lots)}. "
                f"Total lots: {len(self.alpha_lots)}. "
                f"Lots with remaining balance: {len([l for l in self.alpha_lots if l.alpha_rao_remaining > 0])}."
            )

        return consumed_lots, total_basis

    def _consume_tao_lots(
        self, amount_rao: int, disposal_timestamp: int
    ) -> tuple[list[TaoLotConsumption], float]:
        """Consume TAO lots according to configured strategy.

        Args:
            amount_rao: Amount to consume in RAO
            disposal_timestamp: Timestamp of the disposal event

        Returns:
            Tuple of (consumed_lots list, total_basis_consumed)
        """
        # Sort lots by strategy
        if self.config.lot_strategy == CostBasisMethod.FIFO:
            # First In First Out - oldest first
            sorted_lots = sorted(self.tao_lots, key=lambda x: x.timestamp)
        else:  # HIFO
            # Highest In First Out - highest basis first
            sorted_lots = sorted(
                self.tao_lots, key=lambda x: x.usd_per_tao, reverse=True
            )

        consumed_lots = []
        total_basis = 0.0
        remaining_needed = amount_rao

        available_lots = [
            l
            for l in sorted_lots
            if l.rao_remaining > 0 and l.timestamp <= disposal_timestamp
        ]

        for lot in available_lots:
            if remaining_needed <= 0:
                break

            if lot.rao_remaining <= 0:
                continue

            # Consume from this lot
            consume_amount = min(lot.rao_remaining, remaining_needed)
            consume_tao = consume_amount / RAO_PER_TAO

            # Calculate pro-rata basis
            basis_consumed = (consume_amount / lot.rao) * lot.usd_basis

            consumed_lots.append(
                TaoLotConsumption(
                    lot_id=lot.lot_id,
                    tao_consumed=consume_tao,
                    cost_basis_consumed=basis_consumed,
                    acquisition_timestamp=lot.timestamp,
                )
            )

            # Update lot remaining
            lot.rao_remaining -= consume_amount
            if lot.rao_remaining == 0:
                lot.status = LotStatus.CLOSED
            else:
                lot.status = LotStatus.PARTIAL

            total_basis += basis_consumed
            remaining_needed -= consume_amount

        if remaining_needed > 0:
            # Not enough lots available
            return [], 0.0

        return consumed_lots, total_basis

    # -------------------------------------------------------------------------
    # Sale and Transfer Creation
    # -------------------------------------------------------------------------

    def _create_alpha_sale(
        self,
        undelegate: TaoStatsDelegation,
        transfers_by_extrinsic: dict[str, TaoStatsTransfer],
    ) -> tuple[AlphaSale, TaoLot]:
        """Create AlphaSale record from an UNDELEGATE event.

        Args:
            undelegate: UNDELEGATE delegation event
            transfers_by_extrinsic: Dict of transfers indexed by extrinsic_id for fee matching

        Returns:
            Tuple of (AlphaSale, TaoLot) objects
        """
        # Find matching fee transfer (may be absent when unstaking to same coldkey - no transfer)
        fee_transfer = transfers_by_extrinsic.get(undelegate.extrinsic_id)
        if undelegate.nominator.ss58 != self.coldkey_ss58 and not fee_transfer:
            raise ValueError(
                f"No fee transfer found for extrinsic {undelegate.extrinsic_id} "
                f"at block {undelegate.block_number}. This indicates a data integrity issue."
            )
        fee_amount = (
            fee_transfer.amount_rao + fee_transfer.fee_rao
            if fee_transfer
            else undelegate.fee or 0
        )

        # Consume ALPHA lots for this sale
        alpha_rao_needed = int(undelegate.alpha)
        consumed_lots, total_basis = self._consume_alpha_lots(
            alpha_rao_needed, undelegate.timestamp_unix
        )

        if not consumed_lots:
            raise ValueError(
                f"Insufficient ALPHA lots to cover sale of {alpha_rao_needed / RAO_PER_TAO:.4f} ALPHA "
                f"at block {undelegate.block_number}. This indicates missing income lots or incorrect lot consumption."
            )

        # Calculate TAO received: delegation.amount - transfer.amount - transfer.fee
        tao_received_rao = int(undelegate.amount) - fee_amount
        tao_received = tao_received_rao / RAO_PER_TAO

        # Network fee is the total amount deducted (transfer amount + transfer fee)
        network_fee_tao = fee_amount / RAO_PER_TAO

        # Get TAO price for valuation
        tao_price_usd = undelegate.usd / (undelegate.amount / RAO_PER_TAO)
        usd_proceeds = undelegate.usd
        network_fee_usd = network_fee_tao * tao_price_usd
        slippage_usd = undelegate.slippage * tao_price_usd

        # Calculate gain/loss
        realized_gain_loss = usd_proceeds - total_basis

        # Determine gain type (short-term if held < 1 year)
        oldest_lot_timestamp = min(c.acquisition_timestamp for c in consumed_lots)
        holding_period_days = (undelegate.timestamp_unix - oldest_lot_timestamp) / (
            24 * 60 * 60
        )
        gain_type = (
            GainType.LONG_TERM if holding_period_days >= 365 else GainType.SHORT_TERM
        )

        # Create TAO lot — basis excludes the network fee since that TAO
        # was deducted before reaching the wallet. The fee is journalized
        # separately as a Blockchain Fees expense.
        tao_lot_id = self._next_tao_lot_id()
        tao_lot = TaoLot(
            lot_id=tao_lot_id,
            timestamp=undelegate.timestamp_unix,
            block_number=undelegate.block_number,
            rao=tao_received_rao,
            rao_remaining=tao_received_rao,
            usd_basis=usd_proceeds - network_fee_usd,
            usd_per_tao=tao_price_usd,
            source_sale_id=self._next_sale_id(),
            extrinsic_id=undelegate.extrinsic_id,
            status=LotStatus.OPEN,
            notes=f"TAO from alpha sale at block {undelegate.block_number}",
        )

        # Create sale record
        sale = AlphaSale(
            sale_id=tao_lot.source_sale_id,
            timestamp=undelegate.timestamp_unix,
            block_number=undelegate.block_number,
            alpha_disposed=alpha_rao_needed / RAO_PER_TAO,
            tao_received=tao_received,
            tao_price_usd=tao_price_usd,
            usd_proceeds=usd_proceeds,
            cost_basis=total_basis,
            realized_gain_loss=realized_gain_loss,
            gain_type=gain_type,
            consumed_lots=consumed_lots,
            created_tao_lot_id=tao_lot_id,
            tao_slippage=undelegate.slippage,
            slippage_usd=slippage_usd,
            network_fee_tao=network_fee_tao,
            network_fee_usd=network_fee_usd,
            extrinsic_id=undelegate.extrinsic_id,
            notes=f"Alpha sale at block {undelegate.block_number}",
        )

        return sale, tao_lot

    def _create_tao_transfer(self, transfer: TaoStatsTransfer) -> TaoTransfer:
        """Create TaoTransfer record from a transfer event to brokerage.

        Args:
            transfer: Transfer event to brokerage

        Returns:
            TaoTransfer object
        """
        # Total outflow = transfer amount + fee (work in RAO to avoid floating point errors)
        total_outflow_rao = transfer.amount_rao + transfer.fee_rao

        # Consume TAO lots for total outflow (amount + fee)
        consumed_lots, total_basis = self._consume_tao_lots(
            total_outflow_rao, transfer.timestamp_unix
        )

        if not consumed_lots:
            raise ValueError(
                f"Insufficient TAO lots to cover transfer of {total_outflow_rao / RAO_PER_TAO:.4f} TAO "
                f"at block {transfer.block_number}. This indicates missing TAO lots or incorrect lot consumption."
            )

        # Get TAO price for valuation
        tao_price_usd = self.price_client.get_price_at_timestamp(
            "TAO", transfer.timestamp_unix
        )
        if not tao_price_usd:
            raise PriceNotAvailableError(
                f"Could not get TAO price for transfer at block {transfer.block_number} "
                f"(timestamp: {transfer.timestamp_unix})"
            )

        # Calculate proceeds (only for the amount transferred to brokerage, not fees)
        usd_proceeds = transfer.amount_tao * tao_price_usd

        # Split cost basis proportionally between transfer and fee
        fee_cost_basis = (
            (total_basis * (transfer.fee_rao / total_outflow_rao))
            if total_outflow_rao > 0
            else 0.0
        )

        # Calculate gain/loss
        realized_gain_loss = usd_proceeds - total_basis

        # Determine gain type (short-term if held < 1 year)
        oldest_lot_timestamp = min(c.acquisition_timestamp for c in consumed_lots)
        holding_period_days = (transfer.timestamp_unix - oldest_lot_timestamp) / (
            24 * 60 * 60
        )
        gain_type = (
            GainType.LONG_TERM if holding_period_days >= 365 else GainType.SHORT_TERM
        )

        # Create transfer record
        tao_transfer = TaoTransfer(
            transfer_id=self._next_transfer_id(),
            timestamp=transfer.timestamp_unix,
            block_number=transfer.block_number,
            tao_amount=transfer.amount_tao,
            tao_price_usd=tao_price_usd,
            usd_proceeds=usd_proceeds,
            cost_basis=total_basis,
            realized_gain_loss=realized_gain_loss,
            gain_type=gain_type,
            consumed_tao_lots=consumed_lots,
            transaction_hash=transfer.transaction_hash or "",
            extrinsic_id=transfer.extrinsic_id or "",
            total_outflow_tao=transfer.amount_tao + transfer.fee_tao,
            fee_tao=transfer.fee_tao,
            fee_cost_basis_usd=fee_cost_basis,
            notes=f"TAO transfer to brokerage at block {transfer.block_number}",
        )

        return tao_transfer

    # -------------------------------------------------------------------------
    # Staking Emissions Processing
    # -------------------------------------------------------------------------

    def _process_emissions(
        self,
        source_type: SourceType,
        label: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> list:
        """Fetch balance/delegation data, compute daily emission lots, and store them.

        Shared by staking and mining emission processing — the only
        differences are the ``source_type`` stamped on each lot and the
        ``label`` used in log messages.
        """
        start_time, end_time = self._resolve_time_window(
            f"{label} emissions",
            self.last_staking_income_timestamp,
            start_time,
            end_time,
        )

        if start_time is None:
            print(
                f"ℹ️  {label.capitalize()} emissions already fully processed for requested time range"
            )
            return []

        extended_start_time = start_time - SECONDS_PER_DAY

        stake_balances = self.wallet_client.get_stake_balance_history(
            netuid=self.subnet_id,
            hotkey=self.hotkey_ss58,
            coldkey=self.coldkey_ss58,
            start_time=extended_start_time,
            end_time=end_time,
        )

        if not stake_balances:
            print(f"ℹ️  No {label} balance history found")
            return []

        delegations = self.wallet_client.get_delegations(
            netuid=self.subnet_id,
            delegate=self.hotkey_ss58,
            nominator=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time,
        )

        min_ts = min(b.timestamp_unix for b in stake_balances)
        max_ts = max(b.timestamp_unix for b in stake_balances)
        print("  Pre-fetching TAO prices for actual event timestamps...")
        self.price_client.get_prices_in_range("TAO", min_ts, max_ts)

        alpha_lots = self._calculate_daily_emissions(stake_balances, delegations)

        for lot in alpha_lots:
            lot.source_type = source_type
            lot.notes = lot.notes.replace(
                "Staking emissions", f"{label.capitalize()} emissions"
            )

        if alpha_lots:
            self.alpha_lots.extend(alpha_lots)

            max_ts = max(lot.timestamp for lot in alpha_lots)
            self.last_staking_income_timestamp = max_ts
            self.last_income_timestamp = max(
                self.last_income_timestamp, self.last_staking_income_timestamp
            )

            print(f"\n✓ Created {len(alpha_lots)} {label} emission lots")
        else:
            print(f"ℹ️  No {label} emissions found")

        return alpha_lots

    def process_staking_emissions(
        self, start_time: Optional[int] = None, end_time: Optional[int] = None
    ) -> list:
        """Process staking emissions over the specified time period."""
        return self._process_emissions(
            SourceType.STAKING, "staking", start_time, end_time
        )

    def _calculate_daily_emissions(
        self,
        stake_balances: list[TaoStatsStakeBalance],
        delegations: list[TaoStatsDelegation],
    ) -> list[AlphaLot]:
        """Calculate daily staking emissions from balance changes.

        Formula: emissions = current_balance - prev_balance - SUM(DELEGATE.alpha) + SUM(UNDELEGATE.alpha)

        Args:
            stake_balances: List of stake balance snapshots
            delegations: List of DELEGATE/UNDELEGATE events

        Returns:
            List of AlphaLot objects for days with positive emissions
        """
        # Group balances by day (using date at 23:59:59)
        balances_by_day: defaultdict[str, TaoStatsStakeBalance] = defaultdict(
            TaoStatsStakeBalance
        )
        for balance in stake_balances:
            if balance.day in balances_by_day:
                if balance.timestamp_unix > balances_by_day[balance.day].timestamp_unix:
                    balances_by_day[balance.day] = balance
            else:
                balances_by_day[balance.day] = balance

        # Group delegation events by day
        delegations_by_day: defaultdict[str, list[TaoStatsDelegation]] = defaultdict(
            list
        )
        for delegation in delegations:
            delegations_by_day[delegation.day].append(delegation)

        # Calculate emissions for each day
        alpha_lots = []
        sorted_days = sorted(balances_by_day.keys())

        for i in range(1, len(sorted_days)):
            prev_day = sorted_days[i - 1]
            current_day = sorted_days[i]

            prev_balance = balances_by_day[prev_day]
            current_balance = balances_by_day[current_day]
            day_events = delegations_by_day.get(current_day, [])

            # Balance change in RAO
            balance_change_alpha_rao = (
                current_balance.balance_as_alpha_rao - prev_balance.balance_as_alpha_rao
            )

            # Adjust for DELEGATE (outflows - reduce emissions) and UNDELEGATE (inflows - already in balance)
            alpha_inflow_rao = sum(
                e.alpha for e in day_events if e.action == "DELEGATE"
            )
            alpha_outflow_rao = sum(
                e.alpha for e in day_events if e.action == "UNDELEGATE"
            )

            # Calculate net emissions
            # emissions = balance_change - delegates + undelegates
            emissions_alpha_rao = (
                balance_change_alpha_rao - alpha_inflow_rao + alpha_outflow_rao
            )

            # Only create lots for positive emissions
            if emissions_alpha_rao > 0:
                if current_balance.balance_as_alpha_rao == 0:
                    continue

                alpha_price_tao_rao = (
                    current_balance.balance_as_tao_rao
                    / current_balance.balance_as_alpha_rao
                )

                # Get TAO price for FMV calculation
                tao_price = self.price_client.get_price_at_timestamp(
                    "TAO", current_balance.timestamp_unix
                )
                if not tao_price:
                    raise PriceNotAvailableError(
                        f"Could not get TAO price for {current_day} (timestamp: {current_balance.timestamp_unix})"
                    )

                # Convert emissions to ALPHA float for calculations
                emissions_tao = (
                    emissions_alpha_rao * alpha_price_tao_rao
                ) / 1e9  # Convert new Alpha RAO to TAO RAO
                emissions_alpha = emissions_alpha_rao / 1e9  # Convert to TAO
                usd_fmv = emissions_tao * tao_price
                usd_per_alpha = usd_fmv / emissions_alpha if emissions_tao > 0 else 0

                # Use the current day's balance timestamp (latest timestamp of the day)
                lot = AlphaLot(
                    lot_id=self._next_alpha_lot_id(),
                    timestamp=current_balance.timestamp_unix,
                    block_number=current_balance.block_number,
                    source_type=SourceType.STAKING,
                    alpha_rao=emissions_alpha_rao,
                    alpha_rao_remaining=emissions_alpha_rao,
                    usd_fmv=usd_fmv,
                    usd_per_alpha=usd_per_alpha,
                    tao_equivalent=emissions_tao,
                    notes=f"Staking emissions for {current_day}",
                )
                alpha_lots.append(lot)

        return alpha_lots

    # -------------------------------------------------------------------------
    # Disposal Processing Framework
    # -------------------------------------------------------------------------

    def process_disposals(
        self, start_time: Optional[int] = None, end_time: Optional[int] = None
    ):
        """Process all disposal events (sales, expenses, transfers) in chronological order.

        This ensures correct lot consumption by processing events in the order they occurred,
        rather than by type. A sale on Dec 15 won't consume lots needed for an expense on Nov 20.

        Args:
            start_time: Start timestamp
            end_time: End timestamp
        """
        # Step 1: Calculate single time window for all disposals
        disposal_start, disposal_end = self._resolve_time_window(
            "disposals", self.last_disposal_timestamp, start_time, end_time
        )
        if disposal_start is None:
            print("ℹ️  No new disposal events to process")
            return

        # Step 2: Fetch all events for the time window
        all_delegations, all_transfers = self._fetch_disposal_events(
            disposal_start, disposal_end
        )

        # Step 3: Create disposal events from fetched data (subclass-specific)
        disposal_events = self._create_disposal_events(all_delegations, all_transfers)

        if not disposal_events:
            print("ℹ️  No new disposal events found")
            return

        # Step 4: Sort by timestamp and process
        disposal_events.sort(key=lambda x: x.timestamp)

        self._prefetch_disposal_prices(disposal_events)

        # Step 5: Process each event in chronological order
        self._execute_disposal_events(disposal_events)

    def _prefetch_disposal_prices(self, disposal_events: List[DisposalEvent]) -> None:
        """Bulk-fetch TAO prices covering all disposal events.

        Subclasses may override this to use per-event lookups when disposals
        are sparse and the time window is wide.
        """
        min_ts = min(e.timestamp for e in disposal_events)
        max_ts = max(e.timestamp for e in disposal_events)
        print(f"  Pre-fetching TAO prices for disposal events...")
        self.price_client.get_prices_in_range("TAO", min_ts, max_ts)

    def _fetch_disposal_events(
        self, start_time: int, end_time: int
    ) -> Tuple[List[TaoStatsDelegation], List[TaoStatsTransfer]]:
        """Fetch all delegations and transfers for the time range.

        Args:
            start_time: Start timestamp
            end_time: End timestamp

        Returns:
            Tuple of (all_delegations, all_transfers)
        """
        # Fetch all UNDELEGATE events (covers both sales and expenses)
        all_delegations = self.wallet_client.get_delegations(
            netuid=self.subnet_id,
            delegate=self.hotkey_ss58,
            nominator=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time,
            action="UNDELEGATE",
        )

        # Fetch all transfers (covers both fee transfers and brokerage transfers)
        all_transfers = self.wallet_client.get_transfers(
            account_address=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time,
            sender=self.coldkey_ss58,
        )

        return all_delegations, all_transfers

    @abstractmethod
    def _create_disposal_events(
        self,
        all_delegations: List[TaoStatsDelegation],
        all_transfers: List[TaoStatsTransfer],
    ) -> List[DisposalEvent]:
        """Create disposal events from fetched data.

        Subclasses implement this to define which disposal types apply to them.

        Args:
            all_delegations: All UNDELEGATE events in the time range
            all_transfers: All transfers in the time range

        Returns:
            List of DisposalEvent objects with process callbacks
        """
        ...

    def _execute_disposal_events(self, disposal_events: List[DisposalEvent]):
        """Execute disposal events and update state.

        Checks for duplicate extrinsic IDs per disposal type before processing.
        Raises DuplicateExtrinsicError if a duplicate is found.

        Args:
            disposal_events: Sorted list of disposal events to process
        """
        known_sale_extrinsics = {s.extrinsic_id for s in self.sales if s.extrinsic_id}
        known_expense_extrinsics = {
            e.extrinsic_id for e in getattr(self, "expenses", []) if e.extrinsic_id
        }
        known_transfer_extrinsics = {
            t.extrinsic_id for t in self.transfers if t.extrinsic_id
        }

        for disposal in disposal_events:
            eid = disposal.extrinsic_id
            if eid:
                dup_set = {
                    DisposalType.SALE: known_sale_extrinsics,
                    DisposalType.EXPENSE: known_expense_extrinsics,
                    DisposalType.TRANSFER: known_transfer_extrinsics,
                }.get(disposal.disposal_type)

                if dup_set is not None and eid in dup_set:
                    date_str = datetime.fromtimestamp(
                        disposal.timestamp, tz=timezone.utc
                    ).strftime("%Y-%m-%d")
                    raise DuplicateExtrinsicError(
                        f"Duplicate {disposal.disposal_type.value} extrinsic '{eid}' "
                        f"on {date_str}. To reprocess, run with "
                        f"--regenerate --start-date {date_str}"
                    )

        sales_created = 0
        expenses_created = 0
        transfers_created = 0

        for disposal in disposal_events:
            result = disposal.process()

            if disposal.disposal_type == DisposalType.SALE:
                sale, tao_lot = result
                self.sales.append(sale)
                self.tao_lots.append(tao_lot)
                known_sale_extrinsics.add(sale.extrinsic_id)
                sales_created += 1

            elif disposal.disposal_type == DisposalType.EXPENSE:
                self.expenses.append(result)
                known_expense_extrinsics.add(result.extrinsic_id)
                expenses_created += 1

            elif disposal.disposal_type == DisposalType.TRANSFER:
                self.transfers.append(result)
                known_transfer_extrinsics.add(result.extrinsic_id)
                transfers_created += 1

            # Update unified disposal timestamp
            self.last_disposal_timestamp = max(
                self.last_disposal_timestamp, disposal.timestamp
            )

        # Print summary
        if sales_created:
            print(f"\n✓ Created {sales_created} alpha sales")
        if expenses_created:
            print(f"✓ Created {expenses_created} expenses")
        if transfers_created:
            print(f"✓ Created {transfers_created} TAO transfers")

    # -------------------------------------------------------------------------
    # On-chain Balance Verification
    # -------------------------------------------------------------------------

    BALANCE_TOLERANCE_TAO = 0.001
    BALANCE_TOLERANCE_ALPHA = 0.001

    def _months_with_lot_data(self, year: int) -> list[str]:
        """Return sorted list of YYYY-MM strings with any lot or disposal activity."""
        months_seen: set[str] = set()

        def _add_timestamps(items, ts_attr: str = "timestamp"):
            for item in items:
                ts = getattr(item, ts_attr, 0)
                if ts:
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    if dt.year == year:
                        months_seen.add(f"{year}-{dt.month:02d}")

        _add_timestamps(getattr(self, "alpha_lots", []))
        _add_timestamps(getattr(self, "tao_lots", []))
        _add_timestamps(getattr(self, "sales", []))
        _add_timestamps(getattr(self, "expenses", []))
        _add_timestamps(getattr(self, "transfers", []))

        return sorted(months_seen)

    def verify_balances_yearly(
        self, year: int, wallet_label: str = "Wallet", verbose: bool = False
    ) -> bool:
        """Verify lot balances against on-chain data for every month with data.

        Scans in-memory lots to find which months in ``year`` have activity,
        then calls :meth:`verify_balances` for each. Prints a summary banner.

        Returns True if every month passes.
        """
        months = self._months_with_lot_data(year)
        if not months:
            print(f"\n  No lot data found for {year}, nothing to verify.")
            return True

        print(f"\n{'='*60}")
        print(f"Balance verification for {year} ({wallet_label})")
        print(f"{'='*60}")

        all_ok = True
        for ym in months:
            if not self.verify_balances(ym, wallet_label=wallet_label, verbose=verbose):
                all_ok = False

        if all_ok:
            print(f"\n  ✓ All {len(months)} month(s) verified OK")
        else:
            print(f"\n  ⚠ Some months have discrepancies — review warnings above")

        return all_ok

    def verify_balances(
        self, year_month: str, wallet_label: str = "Wallet", verbose: bool = False
    ) -> bool:
        """Compare reconstructed book balances against on-chain data at end of month.

        Uses event-stream reconstruction to compute the historical balance:
          ALPHA = SUM(acquired) - SUM(sold) - SUM(expensed)
          TAO   = SUM(lots created) - SUM(transferred out)

        This correctly reflects what the balance was at any point in time,
        independent of later disposals that consume lot remaining values.

        Returns True if all balances match within tolerance.
        """
        from calendar import monthrange

        parts = year_month.split("-")
        year, month = int(parts[0]), int(parts[1])
        last_day = monthrange(year, month)[1]
        eom_start = int(
            datetime(year, month, last_day, 0, 0, 0, tzinfo=timezone.utc).timestamp()
        )
        eom_end = int(
            datetime(year, month, last_day, 23, 59, 59, tzinfo=timezone.utc).timestamp()
        )

        all_ok = True

        print(f"\n  Balance verification for {year_month} ({wallet_label}):")

        # --- ALPHA (staked) via event-stream reconstruction ---
        if getattr(self, "hotkey_ss58", "") and getattr(self, "subnet_id", None):
            try:
                stake_balances = self.wallet_client.get_stake_balance_history(
                    netuid=self.subnet_id,
                    hotkey=self.hotkey_ss58,
                    coldkey=self.coldkey_ss58,
                    start_time=eom_start,
                    end_time=eom_end,
                )
                onchain_alpha = 0.0
                if stake_balances:
                    latest = max(stake_balances, key=lambda b: b.timestamp_unix)
                    onchain_alpha = latest.balance_as_alpha_float

                alpha_acquired = sum(
                    lot.alpha_rao / RAO_PER_TAO
                    for lot in getattr(self, "alpha_lots", [])
                    if lot.timestamp <= eom_end
                )
                alpha_sold = sum(
                    s.alpha_disposed
                    for s in getattr(self, "sales", [])
                    if s.timestamp <= eom_end
                )
                alpha_expensed = sum(
                    e.alpha_disposed
                    for e in getattr(self, "expenses", [])
                    if e.timestamp <= eom_end
                )
                book_alpha = alpha_acquired - alpha_sold - alpha_expensed

                diff = abs(book_alpha - onchain_alpha)
                if diff <= self.BALANCE_TOLERANCE_ALPHA:
                    print(
                        f"    ALPHA: book={book_alpha:<12.4f} on-chain={onchain_alpha:<12.4f} OK"
                    )
                else:
                    print(
                        f"    ALPHA: book={book_alpha:<12.4f} on-chain={onchain_alpha:<12.4f} "
                        f"WARNING: diff={diff:.4f}"
                    )
                    all_ok = False

                if verbose or diff > self.BALANCE_TOLERANCE_ALPHA:
                    n_lots = sum(
                        1
                        for l in getattr(self, "alpha_lots", [])
                        if l.timestamp <= eom_end
                    )
                    n_sales = sum(
                        1 for s in getattr(self, "sales", []) if s.timestamp <= eom_end
                    )
                    n_expenses = sum(
                        1
                        for e in getattr(self, "expenses", [])
                        if e.timestamp <= eom_end
                    )
                    print(
                        f"           acquired: {alpha_acquired:.4f} ({n_lots} lots through {year_month})"
                    )
                    print(
                        f"           disposed: {alpha_sold + alpha_expensed:.4f} "
                        f"({n_sales} sales, {n_expenses} expenses through {year_month})"
                    )

            except Exception as e:
                print(f"    ALPHA: could not verify — {e}")
                all_ok = False

        # --- TAO (free balance) via event-stream reconstruction ---
        try:
            account_histories = self.wallet_client.get_account_history(
                address=self.coldkey_ss58,
                start_time=eom_start,
                end_time=eom_end,
            )
            onchain_tao = 0.0
            if account_histories:
                latest = max(account_histories, key=lambda h: h.timestamp_unix)
                onchain_tao = latest.balance_free_tao

            tao_acquired = sum(
                lot.rao / RAO_PER_TAO
                for lot in getattr(self, "tao_lots", [])
                if lot.timestamp <= eom_end
            )
            tao_transferred = sum(
                t.total_outflow_tao
                for t in getattr(self, "transfers", [])
                if t.timestamp <= eom_end
            )
            book_tao = tao_acquired - tao_transferred

            diff = abs(book_tao - onchain_tao)
            if diff <= self.BALANCE_TOLERANCE_TAO:
                print(
                    f"    TAO:   book={book_tao:<12.4f} on-chain={onchain_tao:<12.4f} OK"
                )
            else:
                print(
                    f"    TAO:   book={book_tao:<12.4f} on-chain={onchain_tao:<12.4f} "
                    f"WARNING: diff={diff:.4f} TAO"
                )
                all_ok = False

            if verbose or diff > self.BALANCE_TOLERANCE_TAO:
                n_tao_lots = sum(
                    1 for l in getattr(self, "tao_lots", []) if l.timestamp <= eom_end
                )
                n_transfers = sum(
                    1 for t in getattr(self, "transfers", []) if t.timestamp <= eom_end
                )
                print(
                    f"           acquired: {tao_acquired:.4f} ({n_tao_lots} lots through {year_month})"
                )
                print(
                    f"           transferred: {tao_transferred:.4f} "
                    f"({n_transfers} transfers through {year_month})"
                )

        except Exception as e:
            print(f"    TAO:   could not verify — {e}")
            all_ok = False

        return all_ok
