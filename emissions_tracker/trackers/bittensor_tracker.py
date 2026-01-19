from abc import abstractmethod
import time
from typing import Any, List, Optional, Tuple

import backoff

from emissions_tracker.clients.price import PriceClient
from emissions_tracker.clients.wallet import WalletClientInterface
from emissions_tracker.exceptions import PriceNotAvailableError
from emissions_tracker.models import (
    AlphaLotConsumption, AlphaSale, CostBasisMethod, GainType, LotStatus,
    TaoLot, TaoLotConsumption, TaoStatsTransfer, TaoTransfer, TaoStatsDelegation
)

SECONDS_PER_DAY = 86400
RAO_PER_TAO = 10 ** 9


def _is_rate_limit_error(e: Exception) -> bool:
    """Check if an exception is a Google Sheets rate limit error."""
    error_str = str(e)
    error_type = type(e).__name__
    return '429' in error_str or 'Quota exceeded' in error_str or 'APIError' in error_type


class BittensorTracker:

    def __init__(self, wallet_client: WalletClientInterface, price_client: PriceClient):
        self.wallet_client = wallet_client
        self.price_client = price_client
        self._initialize()

    @abstractmethod
    def _initialize(self):
        ...

    @abstractmethod
    def run(self, start_time: int, end_time: Optional[int] = None):
        ...

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
        on_backoff=lambda details: print(f"  Warning: opening sheet failed (attempt {details['tries']}), retrying in {details['wait']:.1f}s...")
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
        on_backoff=lambda details: print(f"  Warning: get records failed (attempt {details['tries']}), retrying in {details['wait']:.1f}s...")
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
        on_backoff=lambda details: print(f"  Warning: append rows failed (attempt {details['tries']}), retrying in {details['wait']:.1f}s...")
    )
    def _append_rows_with_retry(self, worksheet, rows: List[List[Any]]):
        """Append rows to a worksheet with retry logic for rate limiting."""
        worksheet.append_rows(rows, value_input_option='RAW')

    def _sort_sheet_by_timestamp(self, worksheet, timestamp_col: int, label: str, range_str: str = "A2:Z"):
        """Sort a worksheet by a timestamp column (ascending) excluding header row."""
        try:
            worksheet.sort((timestamp_col, 'asc'), range=range_str)
        except Exception as e:
            print(f"  Warning: Could not sort {label} sheet: {e}")

    # -------------------------------------------------------------------------
    # Lot Consumption (FIFO/HIFO strategies)
    # -------------------------------------------------------------------------

    def _consume_alpha_lots(
        self, 
        amount_rao: int,
        timestamp: int
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
            sorted_lots = sorted(self.alpha_lots, key=lambda x: x.usd_per_alpha, reverse=True)

        available_lots = [
            l for l in sorted_lots 
            if l.alpha_rao_remaining > 0
            and l.timestamp <= timestamp
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

            consumed_lots.append(AlphaLotConsumption(
                lot_id=lot.lot_id,
                alpha_consumed=consume_alpha,
                cost_basis_consumed=basis_consumed,
                acquisition_timestamp=lot.timestamp
            ))

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
        self, 
        amount_rao: int, 
        disposal_timestamp: int
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
            sorted_lots = sorted(self.tao_lots, key=lambda x: x.usd_per_tao, reverse=True)

        consumed_lots = []
        total_basis = 0.0
        remaining_needed = amount_rao

        available_lots = [
            l for l in sorted_lots
            if l.rao_remaining > 0 
            and l.timestamp <= disposal_timestamp
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

            consumed_lots.append(TaoLotConsumption(
                lot_id=lot.lot_id,
                tao_consumed=consume_tao,
                cost_basis_consumed=basis_consumed,
                acquisition_timestamp=lot.timestamp
            ))

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
        transfers_by_extrinsic: dict[str, TaoStatsTransfer]
    ) -> tuple[AlphaSale, TaoLot]:
        """Create AlphaSale record from an UNDELEGATE event.
        
        Args:
            undelegate: UNDELEGATE delegation event
            transfers_by_extrinsic: Dict of transfers indexed by extrinsic_id for fee matching
            
        Returns:
            Tuple of (AlphaSale, TaoLot) objects
        """
        # Find matching fee transfer
        fee_transfer = transfers_by_extrinsic.get(undelegate.extrinsic_id)
        if not fee_transfer:
            raise ValueError(
                f"No fee transfer found for extrinsic {undelegate.extrinsic_id} "
                f"at block {undelegate.block_number}. This indicates a data integrity issue."
            )

        # Consume ALPHA lots for this sale
        alpha_rao_needed = int(undelegate.alpha)
        consumed_lots, total_basis = self._consume_alpha_lots(
            alpha_rao_needed,
            undelegate.timestamp_unix
        )

        if not consumed_lots:
            raise ValueError(
                f"Insufficient ALPHA lots to cover sale of {alpha_rao_needed / RAO_PER_TAO:.4f} ALPHA "
                f"at block {undelegate.block_number}. This indicates missing income lots or incorrect lot consumption."
            )

        # Calculate TAO received: delegation.amount - transfer.amount - transfer.fee
        tao_received_rao = int(undelegate.amount) - fee_transfer.amount_rao - fee_transfer.fee_rao
        tao_received = tao_received_rao / RAO_PER_TAO
        
        # Network fee is the total amount deducted (transfer amount + transfer fee)
        network_fee_tao = (fee_transfer.amount_rao + fee_transfer.fee_rao) / RAO_PER_TAO

        # Get TAO price for valuation
        tao_price_usd = undelegate.usd / (undelegate.amount / RAO_PER_TAO)
        usd_proceeds = undelegate.usd
        slippage_usd = undelegate.slippage * tao_price_usd

        # Calculate gain/loss
        realized_gain_loss = usd_proceeds - total_basis

        # Determine gain type (short-term if held < 1 year)
        oldest_lot_timestamp = min(c.acquisition_timestamp for c in consumed_lots)
        holding_period_days = (undelegate.timestamp_unix - oldest_lot_timestamp) / (24 * 60 * 60)
        gain_type = GainType.LONG_TERM if holding_period_days >= 365 else GainType.SHORT_TERM

        # Create TAO lot
        tao_lot_id = self._next_tao_lot_id()
        tao_lot = TaoLot(
            lot_id=tao_lot_id,
            timestamp=undelegate.timestamp_unix,
            block_number=undelegate.block_number,
            rao=tao_received_rao,
            rao_remaining=tao_received_rao,
            usd_basis=usd_proceeds,  # Use proceeds as basis
            usd_per_tao=tao_price_usd,
            source_sale_id=self._next_sale_id(),
            extrinsic_id=undelegate.extrinsic_id,
            status=LotStatus.OPEN,
            notes=f"TAO from alpha sale at block {undelegate.block_number}"
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
            network_fee_usd=network_fee_tao * tao_price_usd,
            extrinsic_id=undelegate.extrinsic_id,
            notes=f"Alpha sale at block {undelegate.block_number}"
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
            total_outflow_rao,
            transfer.timestamp_unix
        )

        if not consumed_lots:
            raise ValueError(
                f"Insufficient TAO lots to cover transfer of {total_outflow_rao / RAO_PER_TAO:.4f} TAO "
                f"at block {transfer.block_number}. This indicates missing TAO lots or incorrect lot consumption."
            )

        # Get TAO price for valuation
        tao_price_usd = self.price_client.get_price_at_timestamp('TAO', transfer.timestamp_unix)
        if not tao_price_usd:
            raise PriceNotAvailableError(
                f"Could not get TAO price for transfer at block {transfer.block_number} "
                f"(timestamp: {transfer.timestamp_unix})"
            )

        # Calculate proceeds (only for the amount transferred to brokerage, not fees)
        usd_proceeds = transfer.amount_tao * tao_price_usd

        # Split cost basis proportionally between transfer and fee
        fee_cost_basis = (total_basis * (transfer.fee_rao / total_outflow_rao)) if total_outflow_rao > 0 else 0.0

        # Calculate gain/loss
        realized_gain_loss = usd_proceeds - total_basis

        # Determine gain type (short-term if held < 1 year)
        oldest_lot_timestamp = min(c.acquisition_timestamp for c in consumed_lots)
        holding_period_days = (transfer.timestamp_unix - oldest_lot_timestamp) / (24 * 60 * 60)
        gain_type = GainType.LONG_TERM if holding_period_days >= 365 else GainType.SHORT_TERM

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
            notes=f"TAO transfer to brokerage at block {transfer.block_number}"
        )

        return tao_transfer