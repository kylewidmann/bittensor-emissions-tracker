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
    SourceType, LotStatus, CostBasisMethod, TaoStatsDelegation, AlphaLotConsumption, GainType, JournalEntry,
    DisposalType, DisposalEvent
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
        self.hotkey_ss58 = self.config.validator_ss58
        self.coldkey_ss58 = self.config.payout_coldkey_ss58
        self.sheet_id = self.config.tracker_sheet_id
        self.smart_contract_ss58 = self.config.smart_contract_ss58
        
        # Wallet addresses (from config)
        self.brokerage_ss58 = self.config.brokerage_ss58
        self.subnet_id = self.config.subnet_id
        
        print(f"Initializing Contract tracker:")
        print(f"  Tracking Hotkey: {self.hotkey_ss58}")
        print(f"  Coldkey: {self.coldkey_ss58}")
        print(f"  Brokerage: {self.brokerage_ss58}")
        print(f"  Smart Contract: {self.smart_contract_ss58}")
        
        # Connect to Google Sheets
        print("  Connecting to Google Sheets...")
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.config.tracker_google_credentials, scope
        )
        self.sheets_client = gspread.authorize(creds)
        self.sheet = self._open_sheet_with_retry(self.sheet_id)
        print("  ✓ Connected to Google Sheets")
        
        # Initialize sheets
        print("  Initializing sheets...")
        self._init_sheets()
        print("  ✓ Sheets initialized")
        
        # Load state
        print("  Loading state from sheets...")
        self._load_state()
        print("  ✓ State loaded")

        # If derived sheets were cleared, reopen income lots so they can be reprocessed
        print("  Checking if income lots need reset...")
        self._reset_income_lots_if_sales_empty()
        print("  ✓ Income lots check complete")
        
        # Counters for ID generation
        print("  Loading counters...")
        self._load_counters()
        print("  ✓ Counters loaded")
        
        # In-memory storage for all data (loaded from sheets, modified during processing)
        print("  Loading data into memory...")
        self.alpha_lots: List[AlphaLot] = []
        self.tao_lots: List[TaoLot] = []
        self.sales: List[AlphaSale] = []
        self.expenses: List[Expense] = []
        self.deposits: List[TaoDeposit] = []
        self.transfers: List[TaoTransfer] = []
        self._load_all_data_from_sheets()
        print("  ✓ Data loaded into memory")

    # -------------------------------------------------------------------------
    # Sheet Infrastructure
    # -------------------------------------------------------------------------

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
        self.last_deposit_timestamp = 0
        self.last_disposal_timestamp = 0  # Unified timestamp for all disposal types
        
        try:
            records = self._get_records_with_retry(self.income_sheet)
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
            records = self._get_records_with_retry(self.deposits_sheet)
            if records:
                self.last_deposit_timestamp = max(r['Timestamp'] for r in records)
        except Exception as e:
            print(f"  Warning: Could not load deposit state: {e}")
        
        # Load last disposal timestamp from all disposal sheets (sales, expenses, transfers)
        disposal_timestamps = [0]
        try:
            records = self._get_records_with_retry(self.sales_sheet)
            if records:
                disposal_timestamps.append(max(r['Timestamp'] for r in records))
        except Exception as e:
            print(f"  Warning: Could not load sales state: {e}")
        
        try:
            records = self._get_records_with_retry(self.expenses_sheet)
            if records:
                disposal_timestamps.append(max(r['Timestamp'] for r in records))
        except Exception as e:
            print(f"  Warning: Could not load expense state: {e}")
        
        try:
            records = self._get_records_with_retry(self.transfers_sheet)
            if records:
                disposal_timestamps.append(max(r['Timestamp'] for r in records))
        except Exception as e:
            print(f"  Warning: Could not load transfer state: {e}")
        
        self.last_disposal_timestamp = max(disposal_timestamps)

    def _create_opening_lots_if_needed(self, start_time: int):
        """Create opening ALPHA and TAO lots if no lots exist.
        
        Args:
            start_time: The start time for processing - opening lots will be created from the day before
        """
        try:
            # Check if ALPHA lots exist
            income_records = self.income_sheet.get_all_records()
            if not income_records:
                self._create_opening_alpha_lot(start_time)
        except Exception as e:
            print(f"  Warning: Could not check/create opening ALPHA lot: {e}")
        
        try:
            # Check if TAO lots exist
            tao_lot_records = self.tao_lots_sheet.get_all_records()
            if not tao_lot_records:
                self._create_opening_tao_lot(start_time)
        except Exception as e:
            print(f"  Warning: Could not check/create opening TAO lot: {e}")

    def _create_opening_alpha_lot(self, start_time: int):
        """Create an opening ALPHA lot from the last stake balance before start_time.
        
        Args:
            start_time: The start time for processing - will fetch balance from the previous day
        """
        print("  Creating opening ALPHA lot from stake balance history...")
        
        # Get stake balance from the previous day (end of day)
        # Start: beginning of previous day (start_time - 2 days)
        # End: end of previous day (start_time - 1 second)
        prev_day_start = start_time - (2 * SECONDS_PER_DAY)
        prev_day_end = start_time - 1
        
        stake_balances = self.wallet_client.get_stake_balance_history(
            netuid=self.subnet_id,
            hotkey=self.hotkey_ss58,
            coldkey=self.coldkey_ss58,
            start_time=prev_day_start,
            end_time=prev_day_end
        )
        
        if not stake_balances:
            print("    No stake balance history found for previous day, skipping opening ALPHA lot")
            return
        
        # Use the last balance from that day as the opening lot
        opening_balance = stake_balances[-1]
        
        if opening_balance.balance_as_alpha_rao == 0:
            print("    Opening balance is zero, skipping opening ALPHA lot")
            return
        
        # Get TAO price at that time
        tao_price = self.price_client.get_price_at_timestamp('TAO', opening_balance.timestamp_unix)
        
        # Calculate USD values
        tao_equivalent = opening_balance.balance_as_tao_float
        usd_fmv = tao_equivalent * tao_price
        usd_per_alpha = usd_fmv / opening_balance.balance_as_alpha_float if opening_balance.balance_as_alpha_float > 0 else 0.0
        
        lot = AlphaLot(
            lot_id=self._next_alpha_lot_id(),
            timestamp=opening_balance.timestamp_unix,
            block_number=opening_balance.block_number,
            alpha_rao=opening_balance.balance_as_alpha_rao,
            alpha_rao_remaining=opening_balance.balance_as_alpha_rao,
            usd_per_alpha=usd_per_alpha,
            usd_fmv=usd_fmv,
            tao_equivalent=tao_equivalent,
            extrinsic_id="",
            transfer_address="",
            status=LotStatus.OPEN,
            source_type=SourceType.CONTRACT,
            notes="Opening balance lot"
        )
        
        self.alpha_lots.append(lot)
        print(f"    Created opening ALPHA lot: {lot.lot_id} with {opening_balance.balance_as_alpha_float:.4f} ALPHA (${usd_fmv:.2f})")
    
    def _create_opening_tao_lot(self, start_time: int):
        """Create an opening TAO lot from account history before start_time.
        
        Args:
            start_time: The start time for processing - will fetch balance from the previous day
        """
        print("  Creating opening TAO lot from account history...")
        
        # Get account balance from the previous day (end of day)
        # Start: beginning of previous day (start_time - 2 days)
        # End: end of previous day (start_time - 1 second)
        prev_day_start = start_time - (2 * SECONDS_PER_DAY)
        prev_day_end = start_time - 1
        
        account_histories = self.wallet_client.get_account_history(
            address=self.coldkey_ss58,
            start_time=prev_day_start,
            end_time=prev_day_end
        )
        
        if not account_histories:
            print("    No account history found for previous day, skipping opening TAO lot")
            return
        
        # Use the last balance from that day as the opening lot
        opening_history = account_histories[-1]
        tao_balance_rao = opening_history.balance_free_rao
        
        if tao_balance_rao == 0:
            print("    Opening balance is zero, skipping opening TAO lot")
            return
        
        # Get TAO price at that time
        tao_price = self.price_client.get_price_at_timestamp('TAO', opening_history.timestamp_unix)
        
        tao_amount = tao_balance_rao / RAO_PER_TAO
        usd_basis = tao_amount * tao_price
        
        lot = TaoLot(
            lot_id=self._next_tao_lot_id(),
            timestamp=opening_history.timestamp_unix,
            block_number=opening_history.block_number,
            rao=tao_balance_rao,
            rao_remaining=tao_balance_rao,
            usd_basis=usd_basis,
            usd_per_tao=tao_price,
            source_sale_id="",
            extrinsic_id="",
            status=LotStatus.OPEN,
            notes="Opening balance lot"
        )
        
        self.tao_lots.append(lot)
        print(f"    Created opening TAO lot: {lot.lot_id} with {tao_amount:.4f} TAO (${usd_basis:.2f})")

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

        rao_remaining_col = col_idx_to_letter('Alpha RAO Remaining', headers)
        status_col = col_idx_to_letter('Status', headers)

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
            records = self._get_records_with_retry(self.income_sheet)
            if records:
                lot_ids = [r['Lot ID'] for r in records if r.get('Lot ID', '').startswith('ALPHA-')]
                self.alpha_lot_counter = max([int(lid.split('-')[1]) for lid in lot_ids], default=0) + 1
            else:
                self.alpha_lot_counter = 1
        except:
            self.alpha_lot_counter = 1
        
        try:
            records = self._get_records_with_retry(self.sales_sheet)
            if records:
                sale_ids = [r['Sale ID'] for r in records if r.get('Sale ID', '').startswith('SALE-')]
                self.sale_counter = max([int(sid.split('-')[1]) for sid in sale_ids], default=0) + 1
            else:
                self.sale_counter = 1
        except:
            self.sale_counter = 1
        
        try:
            records = self._get_records_with_retry(self.expenses_sheet)
            if records:
                expense_ids = [r['Expense ID'] for r in records if r.get('Expense ID', '').startswith('EXP-')]
                self.expense_counter = max([int(eid.split('-')[1]) for eid in expense_ids], default=0) + 1
            else:
                self.expense_counter = 1
        except:
            self.expense_counter = 1
        
        try:
            records = self._get_records_with_retry(self.deposits_sheet)
            if records:
                deposit_ids = [r['Deposit ID'] for r in records if r.get('Deposit ID', '').startswith('DEP-')]
                self.deposit_counter = max([int(did.split('-')[1]) for did in deposit_ids], default=0) + 1
            else:
                self.deposit_counter = 1
        except:
            self.deposit_counter = 1
        
        try:
            records = self._get_records_with_retry(self.tao_lots_sheet)
            if records:
                lot_ids = [r['TAO Lot ID'] for r in records if r.get('TAO Lot ID', '').startswith('TAO-')]
                self.tao_lot_counter = max([int(lid.split('-')[1]) for lid in lot_ids], default=0) + 1
            else:
                self.tao_lot_counter = 1
        except:
            self.tao_lot_counter = 1
        
        try:
            records = self._get_records_with_retry(self.transfers_sheet)
            if records:
                xfer_ids = [r['Transfer ID'] for r in records if r.get('Transfer ID', '').startswith('XFER-')]
                self.transfer_counter = max([int(xid.split('-')[1]) for xid in xfer_ids], default=0) + 1
            else:
                self.transfer_counter = 1
        except:
            self.transfer_counter = 1
        
        print(f"  Counters: ALPHA={self.alpha_lot_counter}, SALE={self.sale_counter}, EXPENSE={self.expense_counter}, DEP={self.deposit_counter}, TAO={self.tao_lot_counter}, XFER={self.transfer_counter}")

    def _load_all_data_from_sheets(self):
        """Load all existing data from sheets into memory."""
        # Load ALPHA lots (income)
        try:
            records = self._get_records_with_retry(self.income_sheet)
            for record in records:
                lot = AlphaLot.from_record(record)
                self.alpha_lots.append(lot)
        except Exception as e:
            print(f"  Warning: Could not load income data: {e}")
        
        # Load TAO lots
        try:
            records = self._get_records_with_retry(self.tao_lots_sheet)
            for record in records:
                lot = TaoLot.from_record(record)
                self.tao_lots.append(lot)
        except Exception as e:
            print(f"  Warning: Could not load TAO lots data: {e}")
        
        # Load sales
        try:
            records = self._get_records_with_retry(self.sales_sheet)
            for record in records:
                sale = AlphaSale.from_record(record)
                self.sales.append(sale)
        except Exception as e:
            print(f"  Warning: Could not load sales data: {e}")
        
        # Load expenses
        try:
            records = self._get_records_with_retry(self.expenses_sheet)
            for record in records:
                expense = Expense.from_record(record)
                self.expenses.append(expense)
        except Exception as e:
            print(f"  Warning: Could not load expenses data: {e}")
        
        # Load deposits
        try:
            records = self._get_records_with_retry(self.deposits_sheet)
            for record in records:
                deposit = TaoDeposit.from_record(record)
                self.deposits.append(deposit)
        except Exception as e:
            print(f"  Warning: Could not load deposits data: {e}")
        
        # Load transfers
        try:
            records = self._get_records_with_retry(self.transfers_sheet)
            for record in records:
                transfer = TaoTransfer.from_record(record)
                self.transfers.append(transfer)
        except Exception as e:
            print(f"  Warning: Could not load transfers data: {e}")

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
    # Main Processing
    # -------------------------------------------------------------------------

    def run(self, start_time: Optional[int] = None, end_time: Optional[int] = None):
        """Run the contract tracker processing.
        
        Processing order:
        1. Income phase - creates lots (no consumption, order doesn't matter):
           - Contract income → ALPHA lots
           - Staking emissions → ALPHA lots  
           - TAO deposits → TAO lots
        
        2. Disposal phase - consumes lots (must be chronological):
           - Sales (consume ALPHA, create TAO)
           - Expenses (consume ALPHA)
           - Transfers (consume TAO)
           All processed together in timestamp order to ensure correct lot consumption.
        """
        # Phase 1: Process all income (creates lots, no consumption)
        self.process_contract_income(start_time=start_time, end_time=end_time)
        self.process_staking_emissions(start_time=start_time, end_time=end_time)
        self.process_tao_deposits(start_time=start_time, end_time=end_time)
        
        # Phase 2: Process all disposals chronologically
        self.process_disposals(start_time=start_time, end_time=end_time)
        
        # Write everything to sheets atomically
        self.write_all_data_to_sheets()

    def _create_disposal_events(
        self,
        all_delegations: List[TaoStatsDelegation],
        all_transfers: List[TaoStatsTransfer],
    ) -> List[DisposalEvent]:
        """Create disposal events from fetched data.
        
        Args:
            all_delegations: All UNDELEGATE events in the time range
            all_transfers: All transfers in the time range
        
        Returns:
            List of DisposalEvent objects with process callbacks
        """
        disposal_events: List[DisposalEvent] = []
        
        # Index transfers by extrinsic_id for sale fee matching
        transfers_by_extrinsic = {t.extrinsic_id: t for t in all_transfers}
        
        for d in all_delegations:
            ts = d.timestamp_unix
            
            # Sales: UNDELEGATE without transfer
            if not d.is_transfer and not d.transfer_address:
                disposal_events.append(DisposalEvent(
                    timestamp=ts,
                    disposal_type=DisposalType.SALE,
                    event=d,
                    process=lambda d=d: self._create_alpha_sale(d, transfers_by_extrinsic)
                ))
            
            # Expenses: UNDELEGATE with transfer to non-validator
            elif d.transfer_address and d.transfer_address.ss58 != self.hotkey_ss58:
                disposal_events.append(DisposalEvent(
                    timestamp=ts,
                    disposal_type=DisposalType.EXPENSE,
                    event=d,
                    process=lambda d=d: self._create_expense(d)
                ))
        
        # Transfers: to brokerage
        for t in all_transfers:
            if t.to_address and t.to_address.ss58 == self.brokerage_ss58:
                disposal_events.append(DisposalEvent(
                    timestamp=t.timestamp_unix,
                    disposal_type=DisposalType.TRANSFER,
                    event=t,
                    process=lambda t=t: self._create_tao_transfer(t)
                ))
        
        return disposal_events


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

        # Skip if already fully processed
        if start_time is None:
            print("ℹ️  Contract income already fully processed for requested time range")
            return []

        # Implementation for processing contract income
        delegation_events = self.wallet_client.get_delegations(
            netuid=self.subnet_id,
            delegate=self.hotkey_ss58,
            nominator=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time,
            is_transfer=True
        )

        alpha_lots = self._convert_delegations_to_alpha_lots(delegation_events)

        if alpha_lots:
            # Add to memory
            self.alpha_lots.extend(alpha_lots)
            
            max_ts = max(lot.timestamp for lot in alpha_lots)
            self.last_contract_income_timestamp = max_ts
            self.last_income_timestamp = max(self.last_contract_income_timestamp, self.last_staking_income_timestamp)
            
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
            and d.delegate.ss58 == self.hotkey_ss58
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


    def _update_consumed_alpha_lots(self, sales: list, alpha_lots: list):
        """Update income sheet with consumed lot amounts.
        
        Args:
            sales: List of AlphaSale objects
            alpha_lots: List of AlphaLotRow objects with updated remaining amounts and row numbers
        """
        # Get column positions from AlphaLot headers
        headers = AlphaLot.sheet_headers()
        
        rao_remaining_col = col_idx_to_letter('Alpha RAO Remaining', headers)
        remaining_col = col_idx_to_letter('Alpha Remaining', headers)
        status_col = col_idx_to_letter('Status', headers)
        
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
                        'range': f'{rao_remaining_col}{lot.row}',
                        'values': [[new_remaining_rao]]
                    })
                    updates.append({
                        'range': f'{remaining_col}{lot.row}',
                        'values': [[new_remaining]]
                    })
                    updates.append({
                        'range': f'{status_col}{lot.row}',
                        'values': [[new_status]]
                    })
                    updated_count += 1

        if updates:
            self.income_sheet.batch_update(updates, value_input_option='RAW')
            print(f"  Updated {updated_count} income lots")



    def _create_expense(self, undelegate: TaoStatsDelegation) -> Expense:
        """Create Expense records from UNDELEGATE events with transfers.
        
        Args:
            undelegations: List of UNDELEGATE events with is_transfer=True
            
        Returns:
            Tuple of (expenses list, alpha_lots list)
        """
        # Consume ALPHA lots for this expense
        alpha_rao_needed = int(undelegate.alpha)
        consumed_lots, total_basis = self._consume_alpha_lots(
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

        return expense

    def _update_consumed_alpha_lots_for_expenses(self, expenses: list, alpha_lots: list):
        """Update income sheet with consumed lot amounts from expenses.
        
        Args:
            expenses: List of Expense objects
            alpha_lots: List of AlphaLotRow objects with updated remaining amounts and row numbers
        """
        # Get column positions from AlphaLot headers
        headers = AlphaLot.sheet_headers()
        
        rao_remaining_col = col_idx_to_letter('Alpha RAO Remaining', headers)
        remaining_col = col_idx_to_letter('Alpha Remaining', headers)
        status_col = col_idx_to_letter('Status', headers)
        
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
                        'range': f'{rao_remaining_col}{lot.row}',
                        'values': [[new_remaining_rao]]
                    })
                    updates.append({
                        'range': f'{remaining_col}{lot.row}',
                        'values': [[new_remaining]]
                    })
                    updates.append({
                        'range': f'{status_col}{lot.row}',
                        'values': [[new_status]]
                    })
                    updated_count += 1

        if updates:
            self.income_sheet.batch_update(updates, value_input_option='RAW')
            print(f"  Updated {updated_count} income lots")


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

        # Skip if already fully processed
        if start_time is None:
            print("ℹ️  TAO deposits already fully processed for requested time range")
            return []

        # Get incoming transfers TO the coldkey (deposits)
        deposit_transfers = self.wallet_client.get_transfers(
            account_address=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time,
            receiver=self.coldkey_ss58  # Filter for transfers TO coldkey
        )

        if not deposit_transfers:
            print("ℹ️  No new TAO deposits found")
            return []

        # Pre-fetch TAO prices for actual transfer timestamps to avoid individual API calls
        min_ts = min(t.timestamp_unix for t in deposit_transfers)
        max_ts = max(t.timestamp_unix for t in deposit_transfers)
        print(f"  Pre-fetching TAO prices for actual event timestamps...")
        self.price_client.get_prices_in_range('TAO', min_ts, max_ts)

        # Create deposits and TAO lots
        deposits, tao_lots = self._create_tao_deposits(deposit_transfers)

        if deposits and tao_lots:
            # Add to memory
            self.deposits.extend(deposits)
            self.tao_lots.extend(tao_lots)

            max_ts = max(deposit.timestamp for deposit in deposits)
            self.last_deposit_timestamp = max_ts

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


    def _update_consumed_tao_lots(self, transfers: list[TaoTransfer], tao_lots: list[TaoLotRow]):
        """Update TAO Lots sheet with consumed lot amounts.
        
        Args:
            transfers: List of TaoTransfer objects
            tao_lots: List of TaoLotRow objects with updated remaining amounts and row numbers
        """
        # Get column positions from TaoLot headers
        headers = TaoLot.sheet_headers()
        
        rao_remaining_col = col_idx_to_letter('TAO RAO Remaining', headers)
        remaining_col = col_idx_to_letter('TAO Remaining', headers)
        status_col = col_idx_to_letter('Status', headers)
        
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
                        'range': f'{rao_remaining_col}{lot.row}',
                        'values': [[new_remaining_rao]]
                    })
                    updates.append({
                        'range': f'{remaining_col}{lot.row}',
                        'values': [[new_remaining]]
                    })
                    updates.append({
                        'range': f'{status_col}{lot.row}',
                        'values': [[new_status]]
                    })
                    updated_count += 1

        if updates:
            self.tao_lots_sheet.batch_update(updates, value_input_option='RAW')
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
        deposit_records = self.deposits_sheet.get_all_records()

        # Check for uncategorized expenses
        self._check_uncategorized_expenses(expense_records, start_ts, end_ts, year_month)

        entries, summary = aggregate_monthly_journal_entries(
            year_month,
            income_records,
            sales_records,
            expense_records,
            transfer_records,
            deposit_records,
            self.wave_config,
            start_ts,
            end_ts,
        )

        if entries:
            # Batch write all entries at once
            rows = [entry.to_sheet_row() for entry in entries]
            self._append_rows_with_retry(self.journal_sheet, rows)
            self._print_journal_summary(year_month, len(entries), summary)
        else:
            print(f"  No data for {year_month}, skipping")
        
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
        deposit_records = self.deposits_sheet.get_all_records()
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
                    deposit_records,
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

    def clear_all_sheets(self):
        """Clear all transaction sheets (for regeneration)."""
        print("\n⚠️  Clearing all transaction sheets...")
        
        sheets_to_clear = [
            (self.income_sheet, "Income"),
            (self.sales_sheet, "Sales"),
            (self.expenses_sheet, "Expenses"),
            (self.deposits_sheet, "Deposits"),
            (self.transfers_sheet, "Transfers"),
            (self.tao_lots_sheet, "TAO Lots"),
            (self.journal_sheet, "Journal Entries")
        ]
        
        for worksheet, name in sheets_to_clear:
            try:
                all_values = worksheet.get_all_values()
                if len(all_values) > 1:
                    last_row = len(all_values)
                    worksheet.batch_clear([f'A2:Z{last_row}'])
                    print(f"  ✓ {name} sheet cleared")
                else:
                    print(f"  ✓ {name} sheet already empty")
            except Exception as e:
                print(f"  Warning: Could not clear {name} sheet: {e}")
        
        # Clear in-memory data to match cleared sheets
        self.alpha_lots = []
        self.tao_lots = []
        self.sales = []
        self.expenses = []
        self.deposits = []
        self.transfers = []
        
        # Reset timestamps so processing starts fresh
        self.last_contract_income_timestamp = 0
        self.last_staking_income_timestamp = 0
        self.last_income_timestamp = 0
        self.last_deposit_timestamp = 0
        self.last_disposal_timestamp = 0
        
        # Reset ID counters
        self.alpha_lot_counter = 1
        self.sale_counter = 1
        self.expense_counter = 1
        self.deposit_counter = 1
        self.tao_lot_counter = 1
        self.transfer_counter = 1
        
        print("✓ All sheets cleared\n")

    def write_all_data_to_sheets(self):
        """Atomically write all in-memory data to sheets."""
        print("\n💾 Writing all data to sheets...")
        
        # Sort all data by timestamp before writing
        self.alpha_lots.sort(key=lambda x: x.timestamp)
        self.tao_lots.sort(key=lambda x: x.timestamp)
        self.sales.sort(key=lambda x: x.timestamp)
        self.expenses.sort(key=lambda x: x.timestamp)
        self.deposits.sort(key=lambda x: x.timestamp)
        self.transfers.sort(key=lambda x: x.timestamp)
        
        # Clear all sheets first
        sheets_to_clear = [
            (self.income_sheet, "Income", len(self.alpha_lots)),
            (self.tao_lots_sheet, "TAO Lots", len(self.tao_lots)),
            (self.sales_sheet, "Sales", len(self.sales)),
            (self.expenses_sheet, "Expenses", len(self.expenses)),
            (self.deposits_sheet, "Deposits", len(self.deposits)),
            (self.transfers_sheet, "Transfers", len(self.transfers)),
        ]
        
        for worksheet, name, count in sheets_to_clear:
            try:
                all_values = worksheet.get_all_values()
                if len(all_values) > 1:
                    last_row = len(all_values)
                    worksheet.batch_clear([f'A2:Z{last_row}'])
            except Exception as e:
                print(f"  Warning: Could not clear {name} sheet: {e}")
        
        # Write all data
        if self.alpha_lots:
            rows = [lot.to_sheet_row() for lot in self.alpha_lots]
            self._append_rows_with_retry(self.income_sheet, rows)
            print(f"  ✓ Wrote {len(rows)} income records")
        
        if self.tao_lots:
            rows = [lot.to_sheet_row() for lot in self.tao_lots]
            self._append_rows_with_retry(self.tao_lots_sheet, rows)
            print(f"  ✓ Wrote {len(rows)} TAO lot records")
        
        if self.sales:
            rows = [sale.to_sheet_row() for sale in self.sales]
            self._append_rows_with_retry(self.sales_sheet, rows)
            print(f"  ✓ Wrote {len(rows)} sales records")
        
        if self.expenses:
            rows = [expense.to_sheet_row() for expense in self.expenses]
            self._append_rows_with_retry(self.expenses_sheet, rows)
            print(f"  ✓ Wrote {len(rows)} expense records")
        
        if self.deposits:
            rows = [deposit.to_sheet_row() for deposit in self.deposits]
            self._append_rows_with_retry(self.deposits_sheet, rows)
            print(f"  ✓ Wrote {len(rows)} deposit records")
        
        if self.transfers:
            rows = [transfer.to_sheet_row() for transfer in self.transfers]
            self._append_rows_with_retry(self.transfers_sheet, rows)
            print(f"  ✓ Wrote {len(rows)} transfer records")
        
        print("✓ All data written to sheets\n")

    def create_opening_lots(self, start_time: int):
        """Create opening ALPHA and TAO lots based on balances from the day before start_time.
        
        Args:
            start_time: Unix timestamp of the first day to process.
                       Opening lots will be created from balances at end of previous day.
        """
        print(f"\nCreating opening lots for start date...")
        self._create_opening_alpha_lot(start_time)
        self._create_opening_tao_lot(start_time)
        
        # Write opening lots to sheets
        self.write_all_data_to_sheets()
        
        # Reset counters after creating opening lots
        self._load_counters()
        print("✓ Opening lots created\n")

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
        print(f"  TAO Deposits (Purchases): ${summary['deposit_total']:.2f}")
