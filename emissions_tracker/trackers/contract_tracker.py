import gspread
import backoff
import time
from collections import defaultdict
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone

from emissions_tracker.config import TrackerSettings, WaveAccountSettings
from emissions_tracker.exceptions import PriceNotAvailableError
from emissions_tracker.journal import JournalGenerator, aggregate_monthly_journal_entries
from emissions_tracker.models import (
    AlphaLot, AlphaLotRow, TaoLot, TaoLotConsumption, TaoLotRow, AlphaSale, Expense, TaoDeposit, TaoStatsStakeBalance, TaoStatsTransfer, TaoTransfer,
    SourceType, LotStatus, CostBasisMethod, TaoStatsDelegation, AlphaLotConsumption, GainType, JournalEntry
)
from emissions_tracker.trackers.bittensor_tracker import BittensorTracker, _is_rate_limit_error, SECONDS_PER_DAY
from oauth2client.service_account import ServiceAccountCredentials

from emissions_tracker.utils import col_idx_to_letter, initialize_sheets

RAO_PER_TAO = 10 ** 9
# Sheet names
INCOME_SHEET = "Income"
SALES_SHEET = "Sales"
EXPENSES_SHEET = "Expenses"
DEPOSITS_SHEET = "Deposits"
TRANSFERS_SHEET = "Transfers"
JOURNAL_SHEET = "Journal Entries"
TAO_LOTS_SHEET = "TAO Lots"
SHEET_CONFIGS = [
    (INCOME_SHEET, AlphaLot.sheet_headers()),
    (SALES_SHEET, AlphaSale.sheet_headers()),
    (EXPENSES_SHEET, Expense.sheet_headers()),
    (DEPOSITS_SHEET, TaoDeposit.sheet_headers()),
    (TAO_LOTS_SHEET, TaoLot.sheet_headers()),
    (TRANSFERS_SHEET, TaoTransfer.sheet_headers()),
    (JOURNAL_SHEET, JournalEntry.sheet_headers()),
]
class ContractTracker(BittensorTracker):
    """Tracker for smart contract emissions and related activities."""

    def _initialize(self):
        self.config = TrackerSettings()
        self.wave_config = WaveAccountSettings()
        
        # Tracker-specific configuration
        self.validator_ss58 = self.config.validator_ss58
        self.coldkey_ss58 = self.config.payout_coldkey_ss58
        self.sheet_id = self.config.tracker_sheet_id
        self.smart_contract_ss58 = self.config.smart_contract_ss58
        
        # Wallet addresses (from config)
        self.brokerage_ss58 = self.config.brokerage_ss58
        self.subnet_id = self.config.subnet_id
        
        print(f"Initializing Contract tracker:")
        print(f"  Tracking Hotkey: {self.validator_ss58}")
        print(f"  Coldkey: {self.coldkey_ss58}")
        print(f"  Brokerage: {self.brokerage_ss58}")
        print(f"  Smart Contract: {self.smart_contract_ss58}")
        
        # Connect to Google Sheets
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.config.tracker_google_credentials, scope
        )
        self.sheets_client = gspread.authorize(creds)
        self.sheet = self._open_sheet_with_retry(self.sheet_id)
        
        # Initialize sheets
        self._init_sheets()
        
        # Load state
        self._load_state()

        # If derived sheets were cleared, reopen income lots so they can be reprocessed
        self._reset_income_lots_if_sales_empty()
        
        # Counters for ID generation
        self._load_counters()

    # -------------------------------------------------------------------------
    # Sheet Infrastructure
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
        return self.sheets_client.open_by_key(sheet_id)

    def _init_sheets(self):
        """Initialize all tracking sheets with headers."""
        
        initialize_sheets(self.sheet, SHEET_CONFIGS)

        # Store worksheet references
        self.income_sheet = self.sheet.worksheet(INCOME_SHEET)
        self.sales_sheet = self.sheet.worksheet(SALES_SHEET)
        self.expenses_sheet = self.sheet.worksheet(EXPENSES_SHEET)
        self.deposits_sheet = self.sheet.worksheet(DEPOSITS_SHEET)
        self.tao_lots_sheet = self.sheet.worksheet(TAO_LOTS_SHEET)
        self.transfers_sheet = self.sheet.worksheet(TRANSFERS_SHEET)
        self.journal_sheet = self.sheet.worksheet(JOURNAL_SHEET)

    def _load_state(self):
        """Load last processed timestamps from sheets."""
        self.last_contract_income_timestamp = 0
        self.last_staking_income_timestamp = 0
        self.last_income_timestamp = 0
        self.last_sale_timestamp = 0
        self.last_expense_timestamp = 0
        self.last_deposit_timestamp = 0
        self.last_transfer_timestamp = 0
        
        try:
            records = self.income_sheet.get_all_records()
            if records:
                contract_income = [r for r in records if r.get('Source Type') == 'Contract']
                if contract_income:
                    self.last_contract_income_timestamp = max(r['Timestamp'] for r in contract_income)
                
                staking_income = [r for r in records if r.get('Source Type') in ('Staking', 'Mining')]
                if staking_income:
                    self.last_staking_income_timestamp = max(r['Timestamp'] for r in staking_income)
                
                self.last_income_timestamp = max(self.last_contract_income_timestamp, self.last_staking_income_timestamp)
        except Exception as e:
            print(f"  Warning: Could not load income state: {e}")
        
        try:
            records = self.sales_sheet.get_all_records()
            if records:
                self.last_sale_timestamp = max(r['Timestamp'] for r in records)
        except Exception as e:
            print(f"  Warning: Could not load sales state: {e}")
        
        try:
            records = self.expenses_sheet.get_all_records()
            if records:
                self.last_expense_timestamp = max(r['Timestamp'] for r in records)
        except Exception as e:
            print(f"  Warning: Could not load expense state: {e}")
        
        try:
            records = self.deposits_sheet.get_all_records()
            if records:
                self.last_deposit_timestamp = max(r['Timestamp'] for r in records)
        except Exception as e:
            print(f"  Warning: Could not load deposit state: {e}")
        
        try:
            records = self.transfers_sheet.get_all_records()
            if records:
                self.last_transfer_timestamp = max(r['Timestamp'] for r in records)
        except Exception as e:
            print(f"  Warning: Could not load transfer state: {e}")

    def _reset_income_lots_if_sales_empty(self):
        """Reset ALPHA lot remaining amounts/status if sales sheet is empty."""
        try:
            sales_records = self.sales_sheet.get_all_records()
        except Exception as e:
            print(f"  Warning: Could not check sales sheet: {e}")
            return

        if sales_records:
            return

        try:
            records = self.income_sheet.get_all_records()
        except Exception as e:
            print(f"  Warning: Could not load income records: {e}")
            return

        # Get column positions from AlphaLot headers
        headers = AlphaLot.sheet_headers()
        try:
            rao_remaining_idx = headers.index('Alpha RAO Remaining')
            status_idx = headers.index('Status')
        except ValueError as e:
            print(f"  Warning: Could not find required columns in headers: {e}")
            return

        rao_remaining_col = col_idx_to_letter(rao_remaining_idx)
        status_col = col_idx_to_letter(status_idx)

        updates = []
        
        for idx, record in enumerate(records, start=2):  # Start at 2 (row 1 is header)
            alpha_rao = record.get('Alpha RAO', 0)
            if alpha_rao > 0:
                updates.append({
                    'range': f'{rao_remaining_col}{idx}',
                    'values': [[alpha_rao]]
                })
                updates.append({
                    'range': f'{status_col}{idx}',
                    'values': [['Open']]
                })

        if not updates:
            return

        try:
            self.income_sheet.batch_update(updates, value_input_option='RAW')
            print(f"  Reset {len(updates)//2} income lots to Open status")
        except Exception as e:
            print(f"  Warning: Could not reset income lots: {e}")

    def _load_counters(self):
        """Load ID counters from existing data."""
        try:
            records = self.income_sheet.get_all_records()
            if records:
                lot_ids = [r['Lot ID'] for r in records if r.get('Lot ID', '').startswith('ALPHA-')]
                self.alpha_lot_counter = max([int(lid.split('-')[1]) for lid in lot_ids], default=0) + 1
            else:
                self.alpha_lot_counter = 1
        except:
            self.alpha_lot_counter = 1
        
        try:
            records = self.sales_sheet.get_all_records()
            if records:
                sale_ids = [r['Sale ID'] for r in records if r.get('Sale ID', '').startswith('SALE-')]
                self.sale_counter = max([int(sid.split('-')[1]) for sid in sale_ids], default=0) + 1
            else:
                self.sale_counter = 1
        except:
            self.sale_counter = 1
        
        try:
            records = self.expenses_sheet.get_all_records()
            if records:
                expense_ids = [r['Expense ID'] for r in records if r.get('Expense ID', '').startswith('EXP-')]
                self.expense_counter = max([int(eid.split('-')[1]) for eid in expense_ids], default=0) + 1
            else:
                self.expense_counter = 1
        except:
            self.expense_counter = 1
        
        try:
            records = self.deposits_sheet.get_all_records()
            if records:
                deposit_ids = [r['Deposit ID'] for r in records if r.get('Deposit ID', '').startswith('DEP-')]
                self.deposit_counter = max([int(did.split('-')[1]) for did in deposit_ids], default=0) + 1
            else:
                self.deposit_counter = 1
        except:
            self.deposit_counter = 1
        
        try:
            records = self.tao_lots_sheet.get_all_records()
            if records:
                lot_ids = [r['TAO Lot ID'] for r in records if r.get('TAO Lot ID', '').startswith('TAO-')]
                self.tao_lot_counter = max([int(lid.split('-')[1]) for lid in lot_ids], default=0) + 1
            else:
                self.tao_lot_counter = 1
        except:
            self.tao_lot_counter = 1
        
        try:
            records = self.transfers_sheet.get_all_records()
            if records:
                xfer_ids = [r['Transfer ID'] for r in records if r.get('Transfer ID', '').startswith('XFER-')]
                self.transfer_counter = max([int(xid.split('-')[1]) for xid in xfer_ids], default=0) + 1
            else:
                self.transfer_counter = 1
        except:
            self.transfer_counter = 1
        
        print(f"  Counters: ALPHA={self.alpha_lot_counter}, SALE={self.sale_counter}, EXPENSE={self.expense_counter}, DEP={self.deposit_counter}, TAO={self.tao_lot_counter}, XFER={self.transfer_counter}")

    # -------------------------------------------------------------------------
    # ID Generation
    # -------------------------------------------------------------------------

    def _next_alpha_lot_id(self) -> str:
        lot_id = f"ALPHA-{self.alpha_lot_counter:04d}"
        self.alpha_lot_counter += 1
        return lot_id
    
    def _next_sale_id(self) -> str:
        sale_id = f"SALE-{self.sale_counter:04d}"
        self.sale_counter += 1
        return sale_id
    
    def _next_expense_id(self) -> str:
        expense_id = f"EXP-{self.expense_counter:04d}"
        self.expense_counter += 1
        return expense_id
    
    def _next_deposit_id(self) -> str:
        deposit_id = f"DEP-{self.deposit_counter:04d}"
        self.deposit_counter += 1
        return deposit_id
    
    def _next_tao_lot_id(self) -> str:
        lot_id = f"TAO-{self.tao_lot_counter:04d}"
        self.tao_lot_counter += 1
        return lot_id
    
    def _next_transfer_id(self) -> str:
        transfer_id = f"XFER-{self.transfer_counter:04d}"
        self.transfer_counter += 1
        return transfer_id

    # -------------------------------------------------------------------------
    # Sheet Operations
    # -------------------------------------------------------------------------

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
        worksheet.append_rows(rows, value_input_option='RAW')

    def _sort_sheet_by_timestamp(self, worksheet, timestamp_col: int, label: str, range_str: str = "A2:Z"):
        """Sort a worksheet by a timestamp column (ascending) excluding header row."""
        try:
            worksheet.sort((timestamp_col, 'asc'), range=range_str)
        except Exception as e:
            print(f"  Warning: Could not sort {label} sheet: {e}")

    # -------------------------------------------------------------------------
    # Main Processing
    # -------------------------------------------------------------------------

    def run(self, start_time: Optional[int] = None, end_time: Optional[int] = None):
        """Run the contract tracker processing."""
        # Implementation for running the contract tracker
        self.process_contract_income(start_time=start_time, end_time=end_time)
        self.process_staking_emissions(start_time=start_time, end_time=end_time)
        self.process_alpha_sales(start_time=start_time, end_time=end_time)
        self.process_expenses(start_time=start_time, end_time=end_time)
        self.process_tao_deposits(start_time=start_time, end_time=end_time)
        self.process_tao_transfers(start_time=start_time, end_time=end_time)

    def process_contract_income(self, start_time: Optional[int] = None, end_time: Optional[int] = None) -> list:
        """Process contract income over the specified time period.

        Args:
            start_time: Start timestamp (uses last processed if None)
            end_time: End timestamp (defaults to now if None)

        Returns:
            list: List of processed emission lots.
        """

        start_time, end_time = self._resolve_time_window(
            "contract income",
            self.last_contract_income_timestamp,
            start_time,
            end_time
        )

        # Implementation for processing contract income
        delegation_events = self.wallet_client.get_delegations(
            netuid=self.subnet_id,
            delegate=self.validator_ss58,
            nominator=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time,
            is_transfer=True
        )

        alpha_lots = self._convert_delegations_to_alpha_lots(delegation_events)

        if alpha_lots:
            # Write all lots to sheet
            rows = [lot.to_sheet_row() for lot in alpha_lots]
            self._append_rows_with_retry(self.income_sheet, rows)
            
            max_ts = max(lot.timestamp for lot in alpha_lots)
            self.last_contract_income_timestamp = max_ts
            self.last_income_timestamp = max(self.last_contract_income_timestamp, self.last_staking_income_timestamp)
            
            # Keep sheet sorted by timestamp (column 3)
            self._sort_sheet_by_timestamp(self.income_sheet, timestamp_col=3, label="Income", range_str="A2:O")
            print(f"\n✓ Created {len(alpha_lots)} contract income lots")
        else:
            print("ℹ️  No new contract income found")
        
        return alpha_lots

    def _convert_delegations_to_alpha_lots(self, delegations: list[TaoStatsDelegation]) -> list[AlphaLot]:
        """Process delegation events related to contract income."""
        # Implementation for processing delegation events
        smart_contract_delegations = [
            d for d in delegations 
            if d.nominator.ss58 == self.coldkey_ss58 
            and d.delegate.ss58 == self.validator_ss58
            and d.transfer_address
            and d.transfer_address.ss58 == self.smart_contract_ss58
        ]
        
        alpha_lots = [
            AlphaLot(
                lot_id=self._next_alpha_lot_id(),
                timestamp=delegation.timestamp_unix,
                block_number=delegation.block_number,
                source_type=SourceType.CONTRACT,
                alpha_rao=delegation.alpha,
                alpha_rao_remaining=delegation.alpha,
                usd_fmv=delegation.usd,
                usd_per_alpha=delegation.alpha_price_in_usd,
                tao_equivalent=delegation.tao,
                notes=f"Smart contract delegation on block {delegation.block_number}"
            )
            for delegation in smart_contract_delegations
        ]

        return alpha_lots

    def process_alpha_sales(self, start_time: Optional[int] = None, end_time: Optional[int] = None) -> list:
        """Process ALPHA sales over the specified time period.

        Args:
            start_time: Start timestamp (uses last processed if None)
            end_time: End timestamp (defaults to now if None)

        Returns:
            list: List of processed sales.
        """
        start_time, end_time = self._resolve_time_window(
            "alpha sales",
            self.last_sale_timestamp,
            start_time,
            end_time
        )

        # Get UNDELEGATE events without transfers (these are alpha sales)
        undelegations = self.wallet_client.get_delegations(
            netuid=self.subnet_id,
            delegate=self.validator_ss58,
            nominator=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time,
            action='UNDELEGATE'
        )

        # Filter for sales: is_transfer=null, transfer_address=null
        sales_undelegations = [
            u for u in undelegations
            if not u.is_transfer and not u.transfer_address
        ]

        if not sales_undelegations:
            print("ℹ️  No new alpha sales found")
            return []

        # Get transfers to match fees
        transfers = self.wallet_client.get_transfers(
            account_address=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time,
            sender=self.coldkey_ss58
        )

        # Create sales
        sales, alpha_lots = self._create_alpha_sales(sales_undelegations, transfers)

        if sales:
            # Write sales to sheet
            sale_rows = [sale.to_sheet_row() for sale in sales]
            self._append_rows_with_retry(self.sales_sheet, sale_rows)

            # Write TAO lots to sheet
            tao_lot_rows = [lot.to_sheet_row() for lot in [s._tao_lot for s in sales]]
            self._append_rows_with_retry(self.tao_lots_sheet, tao_lot_rows)

            # Update income sheet with consumed lot amounts
            self._update_consumed_alpha_lots(sales, alpha_lots)

            max_ts = max(sale.timestamp for sale in sales)
            self.last_sale_timestamp = max_ts

            # Sort sheets
            self._sort_sheet_by_timestamp(self.sales_sheet, timestamp_col=3, label="Sales", range_str="A2:U")
            self._sort_sheet_by_timestamp(self.tao_lots_sheet, timestamp_col=3, label="TAO Lots", range_str="A2:N")

            print(f"\n✓ Created {len(sales)} alpha sales and {len(sales)} TAO lots")
        else:
            print("ℹ️  No valid alpha sales to process")

        return sales

    def _create_alpha_sales(
        self, 
        undelegations: list[TaoStatsDelegation], 
        transfers: list[TaoStatsTransfer]
    ) -> list[AlphaSale]:
        """Create AlphaSale records from UNDELEGATE events.
        
        Args:
            undelegations: List of UNDELEGATE events
            transfers: List of transfer events for fee matching
            
        Returns:
            List of AlphaSale objects with attached TaoLot objects
        """
        # Index transfers by extrinsic_id for quick lookup
        transfers_by_extrinsic = {t.extrinsic_id: t for t in transfers}

        # Load available ALPHA lots
        alpha_lots = self._load_alpha_lots()

        sales = []
        for undelegate in undelegations:
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
                alpha_lots,
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
            # This equals undelegate.fee (the fee specified in the delegation event)
            network_fee_tao = (fee_transfer.amount_rao + fee_transfer.fee_rao) / RAO_PER_TAO

            # Calculate slippage
            # TODO: Look at moving a bunch of these calculations into the models
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

            # Attach TAO lot to sale for later persistence
            sale._tao_lot = tao_lot
            sales.append(sale)

        return sales, alpha_lots

    def _load_alpha_lots(self) -> list:
        """Load available ALPHA lots from income sheet.
        
        Returns:
            List of AlphaLotRow objects with remaining balance > 0 and row numbers attached
        """
        records = self.income_sheet.get_all_records()
        alpha_lots = []

        for idx, record in enumerate(records, start=2):
            alpha_rao_remaining = int(record.get('Alpha RAO Remaining', 0))
            if alpha_rao_remaining > 0:
                lot = AlphaLotRow(
                    lot_id=record['Lot ID'],
                    timestamp=int(record['Timestamp']),
                    block_number=int(record['Block']),
                    source_type=SourceType(record['Source Type']),
                    alpha_rao=int(record['Alpha RAO']),
                    alpha_rao_remaining=alpha_rao_remaining,
                    usd_fmv=float(record['USD FMV']),
                    usd_per_alpha=float(record['USD/Alpha']),
                    tao_equivalent=float(record.get('TAO Equivalent', 0.0)),
                    extrinsic_id=record.get('Extrinsic ID') or None,
                    transfer_address=record.get('Transfer Address') or None,
                    status=LotStatus(record['Status']),
                    notes=record.get('Notes', ''),
                    row=idx
                )
                alpha_lots.append(lot)

        return alpha_lots

    def _consume_alpha_lots(
        self, 
        lots: list[AlphaLot], 
        amount_rao: int,
        timestamp: int
    ) -> tuple[list[AlphaLotConsumption], float]:
        """Consume ALPHA lots according to configured strategy.
        
        Args:
            lots: List of available AlphaLot objects
            amount_rao: Amount to consume in RAO
            timestamp: Timestamp of the consumption event
        Returns:
            Tuple of (consumed_lots list, total_basis_consumed)
        """
        # Sort lots by strategy
        if self.config.lot_strategy == CostBasisMethod.FIFO:
            # First In First Out - oldest first
            sorted_lots = sorted(lots, key=lambda x: x.timestamp)
        else:  # HIFO
            # Highest In First Out - highest basis first
            sorted_lots = sorted(lots, key=lambda x: x.usd_per_alpha, reverse=True)

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
                f"Insufficient ALPHA lots to consume {amount_rao / RAO_PER_TAO:.4f} ALPHA. "
                f"Shortfall of {remaining_needed / RAO_PER_TAO:.4f} ALPHA."
            )

        return consumed_lots, total_basis

    def _update_consumed_alpha_lots(self, sales: list, alpha_lots: list):
        """Update income sheet with consumed lot amounts.
        
        Args:
            sales: List of AlphaSale objects
            alpha_lots: List of AlphaLotRow objects with updated remaining amounts and row numbers
        """
        # Build lot lookup by ID for quick access
        lots_by_id = {lot.lot_id: lot for lot in alpha_lots}
        
        # Collect updates from modified lots that have row numbers
        updates = []
        updated_count = 0
        
        for sale in sales:
            for consumption in sale.consumed_lots:
                lot = lots_by_id.get(consumption.lot_id)
                if lot and hasattr(lot, 'row') and lot.row > 0:
                    # Use the updated values from the in-memory lot
                    new_remaining_rao = lot.alpha_rao_remaining
                    new_remaining = lot.alpha_remaining
                    new_status = lot.status.value
                    
                    updates.append({
                        'range': f'Income!I{lot.row}',  # Alpha RAO Remaining
                        'values': [[new_remaining_rao]]
                    })
                    updates.append({
                        'range': f'Income!K{lot.row}',  # Alpha Remaining (display)
                        'values': [[new_remaining]]
                    })
                    updates.append({
                        'range': f'Income!P{lot.row}',  # Status
                        'values': [[new_status]]
                    })
                    updated_count += 1

        if updates:
            body = {
                "valueInputOption": "RAW",
                "data": updates,
            }
            self.income_sheet.spreadsheet.batch_update(body)
            print(f"  Updated {updated_count} income lots")

    def process_expenses(self, start_time: Optional[int] = None, end_time: Optional[int] = None) -> list:
        """Process expenses over the specified time period.

        Args:
            start_time: Start timestamp (uses last processed if None)
            end_time: End timestamp (defaults to now if None)

        Returns:
            list: List of processed expense lots.
        """
        start_time, end_time = self._resolve_time_window(
            "expenses",
            self.last_expense_timestamp,
            start_time,
            end_time
        )

        # Get UNDELEGATE events with transfers (these are expenses)
        undelegations = self.wallet_client.get_delegations(
            netuid=self.subnet_id,
            delegate=self.validator_ss58,
            nominator=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time,
            action='UNDELEGATE',
            is_transfer=True
        )

        # Filter for expenses: transfer to address other than validator
        expense_undelegations = [
            u for u in undelegations
            if u.transfer_address and u.transfer_address.ss58 != self.validator_ss58
        ]

        if not expense_undelegations:
            print("ℹ️  No new expenses found")
            return []

        # Create expenses
        expenses, alpha_lots = self._create_expenses(expense_undelegations)

        if expenses:
            # Write expenses to sheet
            expense_rows = [expense.to_sheet_row() for expense in expenses]
            self._append_rows_with_retry(self.expenses_sheet, expense_rows)

            # Update income sheet with consumed lot amounts
            self._update_consumed_alpha_lots_for_expenses(expenses, alpha_lots)

            max_ts = max(expense.timestamp for expense in expenses)
            self.last_expense_timestamp = max_ts

            # Sort sheet
            self._sort_sheet_by_timestamp(self.expenses_sheet, timestamp_col=3, label="Expenses", range_str="A2:O")

            print(f"\n✓ Created {len(expenses)} expenses")
        else:
            print("ℹ️  No valid expenses to process")

        return expenses

    def _create_expenses(self, undelegations: list[TaoStatsDelegation]) -> tuple:
        """Create Expense records from UNDELEGATE events with transfers.
        
        Args:
            undelegations: List of UNDELEGATE events with is_transfer=True
            
        Returns:
            Tuple of (expenses list, alpha_lots list)
        """
        # Load available ALPHA lots
        alpha_lots = self._load_alpha_lots()

        expenses = []
        for undelegate in undelegations:
            # Consume ALPHA lots for this expense
            alpha_rao_needed = int(undelegate.alpha)
            consumed_lots, total_basis = self._consume_alpha_lots(
                alpha_lots,
                alpha_rao_needed,
                undelegate.timestamp_unix
            )

            if not consumed_lots:
                raise ValueError(
                    f"Insufficient ALPHA lots to cover expense of {alpha_rao_needed / RAO_PER_TAO:.4f} ALPHA "
                    f"at block {undelegate.block_number}. This indicates missing income lots or incorrect lot consumption."
                )
            
            # TODO: I don't think this accounts for slippage, should this be included in gain/loss?
            # Calculate network fee in USD
            network_fee_tao = 0.0  # No TAO fees for direct ALPHA transfers
            network_fee_usd = 0.0
            if undelegate.fee:
                # Fee is in ALPHA RAO
                network_fee_alpha = int(undelegate.fee) / RAO_PER_TAO
                # Calculate fee USD using alpha price
                if undelegate.alpha_price_in_usd:
                    network_fee_usd = network_fee_alpha * undelegate.alpha_price_in_usd

            # Calculate gain/loss
            realized_gain_loss = undelegate.usd - total_basis - network_fee_usd

            # Determine gain type (short-term if held < 1 year)
            newest_lot_timestamp = min(c.acquisition_timestamp for c in consumed_lots)
            holding_period_days = (undelegate.timestamp_unix - newest_lot_timestamp) / (24 * 60 * 60)
            gain_type = GainType.LONG_TERM if holding_period_days >= 365 else GainType.SHORT_TERM

            # Create expense record
            expense = Expense(
                expense_id=self._next_expense_id(),
                timestamp=undelegate.timestamp_unix,
                block_number=undelegate.block_number,
                transfer_address=undelegate.transfer_address.ss58 if undelegate.transfer_address else "",
                alpha_disposed=alpha_rao_needed / RAO_PER_TAO,
                tao_received=0.0,  # No TAO received for ALPHA expenses
                tao_price_usd=0.0,
                usd_proceeds=undelegate.usd,
                cost_basis=total_basis,
                realized_gain_loss=realized_gain_loss,
                gain_type=gain_type,
                consumed_lots=consumed_lots,
                created_tao_lot_id="",  # No TAO lot created for direct ALPHA expenses
                network_fee_tao=network_fee_tao,
                network_fee_usd=network_fee_usd,
                extrinsic_id=undelegate.extrinsic_id,
                notes=f"Alpha expense to {undelegate.transfer_address.ss58[:8]}... at block {undelegate.block_number}"
            )

            expenses.append(expense)

        return expenses, alpha_lots

    def _update_consumed_alpha_lots_for_expenses(self, expenses: list, alpha_lots: list):
        """Update income sheet with consumed lot amounts from expenses.
        
        Args:
            expenses: List of Expense objects
            alpha_lots: List of AlphaLotRow objects with updated remaining amounts and row numbers
        """
        # TODO: Possibly combine alpha lot consumption?
        # Build lot lookup by ID for quick access
        lots_by_id = {lot.lot_id: lot for lot in alpha_lots}
        
        # Collect updates from modified lots that have row numbers
        updates = []
        updated_count = 0
        
        for expense in expenses:
            for consumption in expense.consumed_lots:
                lot = lots_by_id.get(consumption.lot_id)
                if lot and hasattr(lot, 'row') and lot.row > 0:
                    # Use the updated values from the in-memory lot
                    new_remaining_rao = lot.alpha_rao_remaining
                    new_remaining = lot.alpha_remaining
                    new_status = lot.status.value
                    
                    updates.append({
                        'range': f'Income!I{lot.row}',  # Alpha RAO Remaining
                        'values': [[new_remaining_rao]]
                    })
                    updates.append({
                        'range': f'Income!K{lot.row}',  # Alpha Remaining (display)
                        'values': [[new_remaining]]
                    })
                    updates.append({
                        'range': f'Income!P{lot.row}',  # Status
                        'values': [[new_status]]
                    })
                    updated_count += 1

        if updates:
            body = {
                "valueInputOption": "RAW",
                "data": updates,
            }
            self.income_sheet.spreadsheet.batch_update(body)
            print(f"  Updated {updated_count} income lots")


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

        # For emission calculation, we need the previous day's balance to compute deltas
        # Extend start_time backward by 1 day to get comparison baseline
        extended_start_time = start_time - SECONDS_PER_DAY

        # Get stake balance history for the extended date range
        stake_balances = self.wallet_client.get_stake_balance_history(
            netuid=self.subnet_id,
            hotkey=self.validator_ss58,
            coldkey=self.coldkey_ss58,
            start_time=extended_start_time,
            end_time=end_time
        )

        if not stake_balances:
            print("ℹ️  No stake balance history found")
            return []

        # Get all delegation events (DELEGATE and UNDELEGATE) in the same period
        # TODO: Possibly just use stake balance differences without delegations?
        delegations = self.wallet_client.get_delegations(
            netuid=self.subnet_id,
            delegate=self.validator_ss58,
            nominator=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time
        )

        # Calculate daily emissions
        alpha_lots = self._calculate_daily_emissions(stake_balances, delegations)

        if alpha_lots:
            # Write all lots to sheet
            rows = [lot.to_sheet_row() for lot in alpha_lots]
            self._append_rows_with_retry(self.income_sheet, rows)
            
            max_ts = max(lot.timestamp for lot in alpha_lots)
            self.last_staking_income_timestamp = max_ts
            self.last_income_timestamp = max(self.last_contract_income_timestamp, self.last_staking_income_timestamp)
            
            # Keep sheet sorted by timestamp (column 3)
            self._sort_sheet_by_timestamp(self.income_sheet, timestamp_col=3, label="Income", range_str="A2:O")
            print(f"\n✓ Created {len(alpha_lots)} staking emission lots")
        else:
            print("ℹ️  No staking emissions found")
        
        return alpha_lots

    def _calculate_daily_emissions(self, stake_balances: list[TaoStatsStakeBalance], delegations: list[TaoStatsDelegation]) -> list:
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

    def process_tao_deposits(self, start_time: Optional[int] = None, end_time: Optional[int] = None) -> list:
        """Process incoming TAO transfers (deposits) over the specified time period.

        Creates TaoDeposit records and corresponding TAO lots for TAO received
        from external sources (excluding brokerage withdrawals).

        Args:
            start_time: Start timestamp (uses last processed if None)
            end_time: End timestamp (defaults to now if None)

        Returns:
            list: List of processed TaoDeposit records.
        """
        start_time, end_time = self._resolve_time_window(
            "TAO deposits",
            self.last_deposit_timestamp,
            start_time,
            end_time
        )

        # Get incoming transfers TO the coldkey (deposits)
        incoming_transfers = self.wallet_client.get_transfers(
            account_address=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time,
            receiver=self.coldkey_ss58  # Filter for transfers TO coldkey
        )

        # Filter out transfers from brokerage (those are withdrawals, not deposits we track)
        deposit_transfers = [
            t for t in incoming_transfers
            if t.from_address.ss58 != self.brokerage_ss58
        ]

        if not deposit_transfers:
            print("ℹ️  No new TAO deposits found")
            return []

        # Create deposits and TAO lots
        deposits, tao_lots = self._create_tao_deposits(deposit_transfers)

        if deposits:
            # Write deposits to sheet
            deposit_rows = [deposit.to_sheet_row() for deposit in deposits]
            self._append_rows_with_retry(self.deposits_sheet, deposit_rows)

            # Write TAO lots to sheet
            tao_lot_rows = [lot.to_sheet_row() for lot in tao_lots]
            self._append_rows_with_retry(self.tao_lots_sheet, tao_lot_rows)

            max_ts = max(deposit.timestamp for deposit in deposits)
            self.last_deposit_timestamp = max_ts

            # Sort sheets
            self._sort_sheet_by_timestamp(self.deposits_sheet, timestamp_col=3, label="Deposits", range_str="A2:M")
            self._sort_sheet_by_timestamp(self.tao_lots_sheet, timestamp_col=3, label="TAO Lots", range_str="A2:K")

            print(f"\n✓ Created {len(deposits)} TAO deposits and {len(tao_lots)} TAO lots")
        else:
            print("ℹ️  No valid TAO deposits to process")

        return deposits

    def _create_tao_deposits(self, transfers: list[TaoStatsTransfer]) -> tuple[list[TaoDeposit], list[TaoLot]]:
        """Create TaoDeposit records and corresponding TAO lots from incoming transfers.

        Args:
            transfers: List of incoming transfer events

        Returns:
            Tuple of (deposits list, tao_lots list)
        """
        deposits = []
        tao_lots = []

        for transfer in transfers:
            # Get TAO price at time of deposit
            try:
                tao_price = self.price_client.get_price_at_timestamp('TAO', transfer.timestamp_unix)
            except Exception as e:
                print(f"  Warning: Could not get price for deposit at {transfer.timestamp}: {e}")
                continue

            # Calculate USD FMV
            tao_amount = transfer.amount_rao / RAO_PER_TAO
            usd_fmv = tao_amount * tao_price

            # Create TAO lot for the deposit
            tao_lot_id = self._next_tao_lot_id()
            tao_lot = TaoLot(
                lot_id=tao_lot_id,
                timestamp=transfer.timestamp_unix,
                block_number=transfer.block_number,
                source_sale_id="",  # No sale associated with deposits
                rao=transfer.amount_rao,
                rao_remaining=transfer.amount_rao,
                usd_per_tao=tao_price,
                usd_basis=usd_fmv,
                status=LotStatus.OPEN,
                extrinsic_id=transfer.extrinsic_id,
                notes=f"Deposit from {transfer.from_address.ss58[:8]}..."
            )
            tao_lots.append(tao_lot)

            # Create deposit record
            deposit = TaoDeposit(
                deposit_id=self._next_deposit_id(),
                timestamp=transfer.timestamp_unix,
                block_number=transfer.block_number,
                from_address=transfer.from_address.ss58,
                tao_amount=tao_amount,
                tao_amount_rao=transfer.amount_rao,
                tao_price_usd=tao_price,
                usd_fmv=usd_fmv,
                created_tao_lot_id=tao_lot_id,
                extrinsic_id=transfer.extrinsic_id,
                notes=f"TAO deposit from {transfer.from_address.ss58[:8]}... at block {transfer.block_number}"
            )
            deposits.append(deposit)

        return deposits, tao_lots

    def process_tao_transfers(self, start_time: Optional[int] = None, end_time: Optional[int] = None) -> list:
        """Process TAO transfers over the specified time period.

        Args:
            start_time: Start timestamp (uses last processed if None)
            end_time: End timestamp (defaults to now if None)

        Returns:
            list: List of processed transfer lots.
        """
        start_time, end_time = self._resolve_time_window(
            "TAO transfers",
            self.last_transfer_timestamp,
            start_time,
            end_time
        )

        # Get transfers from wallet to brokerage (using receiver filter)
        brokerage_transfers = self.wallet_client.get_transfers(
            account_address=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time,
            sender=self.coldkey_ss58,
            receiver=self.brokerage_ss58
        )

        if not brokerage_transfers:
            print("ℹ️  No new TAO transfers found")
            return []

        # Create transfers
        tao_transfers, tao_lots = self._create_tao_transfers(brokerage_transfers)

        if tao_transfers:
            # Write transfers to sheet
            transfer_rows = [transfer.to_sheet_row() for transfer in tao_transfers]
            self._append_rows_with_retry(self.transfers_sheet, transfer_rows)

            # Update TAO lots sheet with consumed lot amounts
            self._update_consumed_tao_lots(tao_transfers, tao_lots)

            max_ts = max(transfer.timestamp for transfer in tao_transfers)
            self.last_transfer_timestamp = max_ts

            # Sort sheet
            self._sort_sheet_by_timestamp(self.transfers_sheet, timestamp_col=3, label="Transfers", range_str="A2:N")

            print(f"\n✓ Created {len(tao_transfers)} TAO transfers")
        else:
            print("ℹ️  No valid TAO transfers to process")

        return tao_transfers

    def _create_tao_transfers(self, transfers: list[TaoStatsTransfer]) -> tuple[list[TaoTransfer], list[TaoLotRow]]:
        """Create TaoTransfer records from transfer events.
        
        Args:
            transfers: List of transfer events to brokerage
            
        Returns:
            Tuple of (transfers list, tao_lots list)
        """
        # Load available TAO lots
        tao_lots = self._load_tao_lots()

        tao_transfers = []
        for transfer in transfers:
            # Total outflow = transfer amount + fee (work in RAO to avoid floating point errors)
            total_outflow_rao = transfer.amount_rao + transfer.fee_rao

            # Consume TAO lots for total outflow (amount + fee)
            # Both the transfer amount and fee reduce the wallet balance
            consumed_lots, total_basis = self._consume_tao_lots(
                tao_lots,
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

            tao_transfers.append(tao_transfer)

        return tao_transfers, tao_lots

    def _load_tao_lots(self) -> list:
        """Load available TAO lots from TAO Lots sheet.
        
        Returns:
            List of TaoLotRow objects with remaining balance > 0 and row numbers attached
        """
        records = self.tao_lots_sheet.get_all_records()
        tao_lots = []

        for idx, record in enumerate(records, start=2):  # Start at 2 (row 1 is header)
            rao_remaining = record.get('TAO RAO Remaining', 0)
            if rao_remaining > 0:
                lot = TaoLotRow(
                    lot_id=record['TAO Lot ID'],
                    timestamp=record['Timestamp'],
                    block_number=record['Block'],
                    rao=record['TAO RAO'],
                    rao_remaining=rao_remaining,
                    usd_basis=record['USD Basis'],
                    usd_per_tao=record['USD/TAO'],
                    source_sale_id=record.get('Source Sale ID') or "",
                    extrinsic_id=record.get('Extrinsic ID') or "",
                    status=LotStatus(record['Status']),
                    notes=record.get('Notes', ''),
                    row=idx  # Use the actual enumeration index (row number in sheet)
                )
                tao_lots.append(lot)

        return tao_lots

    def _consume_tao_lots(
        self, 
        lots: list[TaoLotRow], 
        amount_rao: int, 
        disposal_timestamp: int
    ) -> tuple[list[TaoLotConsumption], float]:
        """Consume TAO lots according to configured strategy.
        
        Args:
            lots: List of available TaoLot objects
            amount_rao: Amount to consume in RAO
            disposal_timestamp: Timestamp of the disposal event
            
        Returns:
            Tuple of (consumed_lots list, total_basis_consumed)
        """
        # Sort lots by strategy
        if self.config.lot_strategy == CostBasisMethod.FIFO:
            # First In First Out - oldest first
            sorted_lots = sorted(lots, key=lambda x: x.timestamp)
        else:  # HIFO
            # Highest In First Out - highest basis first
            sorted_lots = sorted(lots, key=lambda x: x.usd_per_tao, reverse=True)

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
                tao_consumed=consume_tao,  # Reusing alpha_consumed field for TAO amount
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

    def _update_consumed_tao_lots(self, transfers: list[TaoTransfer], tao_lots: list[TaoLotRow]):
        """Update TAO Lots sheet with consumed lot amounts.
        
        Args:
            transfers: List of TaoTransfer objects
            tao_lots: List of TaoLotRow objects with updated remaining amounts and row numbers
        """
        # Build lot lookup by ID for quick access
        lots_by_id = {lot.lot_id: lot for lot in tao_lots}
        
        # Collect updates from modified lots that have row numbers
        updates = []
        updated_count = 0
        
        for transfer in transfers:
            for consumption in transfer.consumed_tao_lots:
                lot = lots_by_id.get(consumption.lot_id)
                if lot and hasattr(lot, 'row') and lot.row > 0:
                    # Use the updated values from the in-memory lot
                    new_remaining_rao = lot.rao_remaining
                    new_remaining = lot.tao_remaining
                    new_status = lot.status.value
                    
                    updates.append({
                        'range': f'TAO Lots!F{lot.row}',  # TAO RAO Remaining
                        'values': [[new_remaining_rao]]
                    })
                    updates.append({
                        'range': f'TAO Lots!H{lot.row}',  # TAO Remaining (display)
                        'values': [[new_remaining]]
                    })
                    updates.append({
                        'range': f'TAO Lots!M{lot.row}',  # Status
                        'values': [[new_status]]
                    })
                    updated_count += 1

        if updates:
            body = {
                "valueInputOption": "RAW",
                "data": updates,
            }
            self.tao_lots_sheet.spreadsheet.batch_update(body)
            print(f"  Updated {updated_count} TAO lots")

    # -------------------------------------------------------------------------
    # Journal Entry Generation
    # -------------------------------------------------------------------------

    def generate_monthly_journal_entries(self, year_month: Optional[str] = None) -> List[JournalEntry]:
        """Generate aggregated Wave journal entries for a given month."""
        if not year_month:
            today = datetime.now()
            year_month = f"{today.year}-{today.month:02d}"

        try:
            period_start = datetime.strptime(year_month, "%Y-%m").replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise ValueError(f"Invalid month format '{year_month}', expected YYYY-MM") from exc

        first_day_next_month = (period_start.replace(day=28) + timedelta(days=4)).replace(day=1)

        start_ts = int(period_start.timestamp())
        end_ts = int(first_day_next_month.timestamp())

        print(f"\n{'='*60}")
        print(f"Generating journal entries for {year_month}...")
        print(f"{'='*60}")

        # Load all records once
        expense_records = self.expenses_sheet.get_all_records()
        income_records = self.income_sheet.get_all_records()
        sales_records = self.sales_sheet.get_all_records()
        transfer_records = self.transfers_sheet.get_all_records()

        # Check for uncategorized expenses
        self._check_uncategorized_expenses(expense_records, start_ts, end_ts, year_month)

        entries, summary = aggregate_monthly_journal_entries(
            year_month,
            income_records,
            sales_records,
            expense_records,
            transfer_records,
            self.wave_config,
            start_ts,
            end_ts,
        )

        for entry in entries:
            self.journal_sheet.append_row(entry.to_sheet_row())

        self._print_journal_summary(year_month, len(entries), summary)
        return entries

    def generate_yearly_journal_entries(self, year: int) -> List[JournalEntry]:
        """Generate journal entries for all months in a given year."""
        print(f"\n{'='*60}")
        print(f"Generating journal entries for entire year {year}")
        print(f"{'='*60}")

        # Read all sheets once at the start
        print("\nLoading data from sheets...")
        expense_records = self.expenses_sheet.get_all_records()
        income_records = self.income_sheet.get_all_records()
        sales_records = self.sales_sheet.get_all_records()
        transfer_records = self.transfers_sheet.get_all_records()
        print("✓ Data loaded\n")

        # Check for uncategorized expenses in the entire year
        year_start = datetime(year, 1, 1, tzinfo=timezone.utc)
        year_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        self._check_uncategorized_expenses(
            expense_records,
            int(year_start.timestamp()),
            int(year_end.timestamp()),
            str(year)
        )

        all_entries = []
        all_rows = []

        for month in range(1, 13):
            year_month = f"{year}-{month:02d}"

            try:
                period_start = datetime.strptime(year_month, "%Y-%m").replace(tzinfo=timezone.utc)
                first_day_next_month = (period_start.replace(day=28) + timedelta(days=4)).replace(day=1)
                start_ts = int(period_start.timestamp())
                end_ts = int(first_day_next_month.timestamp())
            except ValueError:
                continue

            print(f"\n{'='*60}")
            print(f"Generating journal entries for {year_month}...")
            print(f"{'='*60}")

            try:
                entries, summary = aggregate_monthly_journal_entries(
                    year_month,
                    income_records,
                    sales_records,
                    expense_records,
                    transfer_records,
                    self.wave_config,
                    start_ts,
                    end_ts,
                )

                for entry in entries:
                    all_rows.append(entry.to_sheet_row())
                    all_entries.append(entry)

                self._print_journal_summary(year_month, len(entries), summary)

            except ValueError as e:
                print(f"  Skipping {year_month}: {e}")
                continue

        # Batch write all journal entries
        if all_rows:
            print(f"\nWriting {len(all_rows)} journal entries to sheet...")
            self._append_rows_with_retry(self.journal_sheet, all_rows)
            print("✓ Journal entries written")

        print(f"\n✓ Generated {len(all_entries)} total journal entries for {year}")
        return all_entries

    def clear_journal_sheet(self):
        """Clear all journal entries (for regeneration)."""
        print("  Clearing Journal Entries sheet...")
        try:
            all_values = self.journal_sheet.get_all_values()
            if len(all_values) > 1:
                last_row = len(all_values)
                self.journal_sheet.batch_clear([f'A2:Z{last_row}'])
            print("  ✓ Journal Entries sheet cleared")
        except Exception as e:
            print(f"  Warning: Could not clear journal sheet: {e}")

    def _check_uncategorized_expenses(
        self,
        expense_records: List[Dict[str, Any]],
        start_ts: int,
        end_ts: int,
        period_name: str
    ):
        """Check for uncategorized expenses and raise an error if found."""
        uncategorized = [
            exp for exp in expense_records
            if start_ts <= exp['Timestamp'] < end_ts and not exp.get('Category', '').strip()
        ]

        if uncategorized:
            print(f"\n❌ ERROR: Found {len(uncategorized)} uncategorized expense(s) in {period_name}")
            print("Please categorize all expenses in the Expenses sheet before generating journal entries.")
            print("\nUncategorized expenses:")
            for exp in uncategorized:
                exp_date = datetime.fromtimestamp(exp['Timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                exp_id = exp.get('Expense ID', 'unknown')
                transfer_addr = exp.get('Transfer Address', 'unknown')
                alpha = exp.get('Alpha Disposed', 0)
                print(f"  - {exp_id} ({exp_date}): {alpha:.4f} ALPHA to {transfer_addr[:8]}...")
            raise ValueError(
                f"Cannot generate journal entries for {period_name}: "
                f"{len(uncategorized)} uncategorized expense(s) found. "
                "Please update the Category column in the Expenses sheet."
            )

    def _print_journal_summary(self, year_month: str, entry_count: int, summary: Dict[str, float]):
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
