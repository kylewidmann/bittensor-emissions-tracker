from abc import abstractmethod
import time
from collections import defaultdict
from typing import Any, List, Optional, Tuple

import backoff

from emissions_tracker.clients.price import PriceClient
from emissions_tracker.clients.wallet import WalletClientInterface
from emissions_tracker.exceptions import PriceNotAvailableError
from emissions_tracker.models import (
    AlphaLot, AlphaLotConsumption, AlphaSale, CostBasisMethod, DisposalEvent,
    DisposalType, GainType, LotStatus, SourceType, TaoLot, TaoLotConsumption,
    TaoStatsStakeBalance, TaoStatsTransfer, TaoTransfer, TaoStatsDelegation
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

    # -------------------------------------------------------------------------
    # Staking Emissions Processing
    # -------------------------------------------------------------------------

    def process_staking_emissions(self, start_time: Optional[int] = None, end_time: Optional[int] = None) -> list:
        """Process staking emissions over the specified time period.

        Args:
            start_time: Start timestamp (uses last processed if None)
            end_time: End timestamp (defaults to now if None)

        Returns:
            list: List of processed emission lots.
        """
        start_time, end_time = self._resolve_time_window(
            "staking emissions",
            self.last_staking_income_timestamp,
            start_time,
            end_time
        )

        # Skip if already fully processed
        if start_time is None:
            print("ℹ️  Staking emissions already fully processed for requested time range")
            return []

        # For emission calculation, we need the previous day's balance to compute deltas
        # Extend start_time backward by 1 day to get comparison baseline
        extended_start_time = start_time - SECONDS_PER_DAY

        # Get stake balance history for the extended date range
        stake_balances = self.wallet_client.get_stake_balance_history(
            netuid=self.subnet_id,
            hotkey=self.hotkey_ss58,
            coldkey=self.coldkey_ss58,
            start_time=extended_start_time,
            end_time=end_time
        )

        if not stake_balances:
            print("ℹ️  No stake balance history found")
            return []

        # Get all delegation events (DELEGATE and UNDELEGATE) in the same period
        delegations = self.wallet_client.get_delegations(
            netuid=self.subnet_id,
            delegate=self.hotkey_ss58,
            nominator=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time
        )

        # Pre-fetch TAO prices for actual event timestamps to avoid individual API calls
        min_ts = min(b.timestamp_unix for b in stake_balances)
        max_ts = max(b.timestamp_unix for b in stake_balances)
        print(f"  Pre-fetching TAO prices for actual event timestamps...")
        self.price_client.get_prices_in_range('TAO', min_ts, max_ts)

        # Calculate daily emissions
        alpha_lots = self._calculate_daily_emissions(stake_balances, delegations)

        if alpha_lots:
            # Add to memory
            self.alpha_lots.extend(alpha_lots)
            
            max_ts = max(lot.timestamp for lot in alpha_lots)
            self.last_staking_income_timestamp = max_ts
            self.last_income_timestamp = max(self.last_income_timestamp, self.last_staking_income_timestamp)
            
            print(f"\n✓ Created {len(alpha_lots)} staking emission lots")
        else:
            print("ℹ️  No staking emissions found")
        
        return alpha_lots

    def _calculate_daily_emissions(
        self, 
        stake_balances: list[TaoStatsStakeBalance], 
        delegations: list[TaoStatsDelegation]
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
        balances_by_day: defaultdict[str, TaoStatsStakeBalance] = defaultdict(TaoStatsStakeBalance)
        for balance in stake_balances:
            if balance.day in balances_by_day:
                if balance.timestamp_unix > balances_by_day[balance.day].timestamp_unix:
                    balances_by_day[balance.day] = balance
            else:
                balances_by_day[balance.day] = balance      
        
        # Group delegation events by day
        delegations_by_day: defaultdict[str, list[TaoStatsDelegation]] = defaultdict(list)
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
            balance_change_alpha_rao = current_balance.balance_as_alpha_rao - prev_balance.balance_as_alpha_rao
            
            # Adjust for DELEGATE (outflows - reduce emissions) and UNDELEGATE (inflows - already in balance)
            alpha_inflow_rao = sum(e.alpha for e in day_events if e.action == 'DELEGATE')
            alpha_outflow_rao = sum(e.alpha for e in day_events if e.action == 'UNDELEGATE')
            
            # Calculate net emissions
            # emissions = balance_change - delegates + undelegates
            emissions_alpha_rao = balance_change_alpha_rao - alpha_inflow_rao + alpha_outflow_rao
            alpha_price_tao_rao = current_balance.balance_as_tao_rao / current_balance.balance_as_alpha_rao           
            
            # Only create lots for positive emissions
            if emissions_alpha_rao > 0:
                # Get TAO price for FMV calculation
                tao_price = self.price_client.get_price_at_timestamp('TAO', current_balance.timestamp_unix)
                if not tao_price:
                    raise PriceNotAvailableError(f"Could not get TAO price for {current_day} (timestamp: {current_balance.timestamp_unix})")

                # Convert emissions to ALPHA float for calculations
                emissions_tao = (emissions_alpha_rao * alpha_price_tao_rao) / 1e9  # Convert new Alpha RAO to TAO RAO
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
                    notes=f"Staking emissions for {current_day}"
                )
                alpha_lots.append(lot)
        
        return alpha_lots

    # -------------------------------------------------------------------------
    # Disposal Processing Framework
    # -------------------------------------------------------------------------

    def process_disposals(self, start_time: Optional[int] = None, end_time: Optional[int] = None):
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
        all_delegations, all_transfers = self._fetch_disposal_events(disposal_start, disposal_end)
        
        # Step 3: Create disposal events from fetched data (subclass-specific)
        disposal_events = self._create_disposal_events(all_delegations, all_transfers)
        
        if not disposal_events:
            print("ℹ️  No new disposal events found")
            return
        
        # Step 4: Sort by timestamp and process
        disposal_events.sort(key=lambda x: x.timestamp)
        
        # Pre-fetch TAO prices for all events
        min_ts = min(e.timestamp for e in disposal_events)
        max_ts = max(e.timestamp for e in disposal_events)
        print(f"  Pre-fetching TAO prices for disposal events...")
        self.price_client.get_prices_in_range('TAO', min_ts, max_ts)
        
        # Step 5: Process each event in chronological order
        self._execute_disposal_events(disposal_events)

    def _fetch_disposal_events(
        self,
        start_time: int,
        end_time: int
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
            action='UNDELEGATE'
        )
        
        # Fetch all transfers (covers both fee transfers and brokerage transfers)
        all_transfers = self.wallet_client.get_transfers(
            account_address=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time,
            sender=self.coldkey_ss58
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
        
        Args:
            disposal_events: Sorted list of disposal events to process
        """
        sales_created = 0
        expenses_created = 0
        transfers_created = 0
        
        for disposal in disposal_events:
            result = disposal.process()
            
            if disposal.disposal_type == DisposalType.SALE:
                sale, tao_lot = result
                self.sales.append(sale)
                self.tao_lots.append(tao_lot)
                sales_created += 1
                
            elif disposal.disposal_type == DisposalType.EXPENSE:
                self.expenses.append(result)
                expenses_created += 1
                
            elif disposal.disposal_type == DisposalType.TRANSFER:
                self.transfers.append(result)
                transfers_created += 1
            
            # Update unified disposal timestamp
            self.last_disposal_timestamp = max(self.last_disposal_timestamp, disposal.timestamp)
        
        # Print summary
        if sales_created:
            print(f"\n✓ Created {sales_created} alpha sales")
        if expenses_created:
            print(f"✓ Created {expenses_created} expenses")
        if transfers_created:
            print(f"✓ Created {transfers_created} TAO transfers")