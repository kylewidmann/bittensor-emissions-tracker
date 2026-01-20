import gspread
from typing import List, Optional
from datetime import datetime, timedelta, timezone

from emissions_tracker.config import TrackerSettings, WaveAccountSettings
from emissions_tracker.journal import JournalGenerator, aggregate_monthly_journal_entries
from emissions_tracker.models import (
    AlphaLot, TaoLot, AlphaSale, TaoTransfer,
    SourceType, LotStatus, CostBasisMethod, TaoStatsDelegation, TaoStatsTransfer,
    DisposalType, DisposalEvent, JournalEntry
)
from emissions_tracker.trackers.bittensor_tracker import BittensorTracker, SECONDS_PER_DAY
from oauth2client.service_account import ServiceAccountCredentials

from emissions_tracker.utils import initialize_sheets

RAO_PER_TAO = 10 ** 9

# Sheet names (no Expenses or Deposits for mining)
INCOME_SHEET = "Income"
SALES_SHEET = "Sales"
TRANSFERS_SHEET = "Transfers"
JOURNAL_SHEET = "Journal Entries"
TAO_LOTS_SHEET = "TAO Lots"

SHEET_CONFIGS = [
    (INCOME_SHEET, AlphaLot.sheet_headers()),
    (SALES_SHEET, AlphaSale.sheet_headers()),
    (TAO_LOTS_SHEET, TaoLot.sheet_headers()),
    (TRANSFERS_SHEET, TaoTransfer.sheet_headers()),
    (JOURNAL_SHEET, JournalEntry.sheet_headers()),
]


class MiningTracker(BittensorTracker):
    """Tracker for mining emissions and related activities.
    
    Unlike ContractTracker, MiningTracker:
    - Does NOT process contract income (miners don't receive contract payouts)
    - Does NOT process TAO deposits (miners don't receive TAO deposits)
    - Does NOT process expenses (miners don't do transfer undelegations)
    - Uses miner_hotkey_ss58 instead of validator_ss58
    - Uses mining_tracker_sheet_id instead of tracker_sheet_id
    """

    def _initialize(self):
        self.config = TrackerSettings()
        self.wave_config = WaveAccountSettings()
        
        # Tracker-specific configuration - map to base class variables
        self.hotkey_ss58 = self.config.miner_hotkey_ss58
        self.coldkey_ss58 = self.config.miner_coldkey_ss58 or self.config.payout_coldkey_ss58
        self.sheet_id = self.config.mining_tracker_sheet_id
        
        # Wallet addresses (from config)
        self.brokerage_ss58 = self.config.brokerage_ss58
        self.subnet_id = self.config.subnet_id
        
        print(f"Initializing Mining tracker:")
        print(f"  Miner Hotkey: {self.hotkey_ss58}")
        print(f"  Coldkey: {self.coldkey_ss58}")
        print(f"  Brokerage: {self.brokerage_ss58}")
        
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
        self.transfers: List[TaoTransfer] = []
        self._load_all_data_from_sheets()
        print(f"  ✓ Loaded {len(self.alpha_lots)} income lots, {len(self.tao_lots)} TAO lots, {len(self.sales)} sales, {len(self.transfers)} transfers")

    def _init_sheets(self):
        """Initialize all tracking sheets with headers."""
        initialize_sheets(self.sheet, SHEET_CONFIGS)

        # Store worksheet references
        self.income_sheet = self.sheet.worksheet(INCOME_SHEET)
        self.sales_sheet = self.sheet.worksheet(SALES_SHEET)
        self.tao_lots_sheet = self.sheet.worksheet(TAO_LOTS_SHEET)
        self.transfers_sheet = self.sheet.worksheet(TRANSFERS_SHEET)
        self.journal_sheet = self.sheet.worksheet(JOURNAL_SHEET)

    def _load_state(self):
        """Load last processed timestamps from sheets."""
        self.last_staking_income_timestamp = 0
        self.last_income_timestamp = 0
        self.last_disposal_timestamp = 0  # Unified timestamp for all disposal types
        
        try:
            records = self._get_records_with_retry(self.income_sheet)
            if records:
                # Mining income is stored with source type 'Mining'
                mining_income = [r for r in records if r.get('Source Type') in ('Staking', 'Mining')]
                if mining_income:
                    self.last_staking_income_timestamp = max(r['Timestamp'] for r in mining_income)
                
                self.last_income_timestamp = self.last_staking_income_timestamp
        except Exception as e:
            print(f"  Warning: Could not load income state: {e}")
        
        # Load last disposal timestamp from disposal sheets (sales, transfers - no expenses)
        disposal_timestamps = [0]
        try:
            records = self._get_records_with_retry(self.sales_sheet)
            if records:
                disposal_timestamps.append(max(r['Timestamp'] for r in records))
        except Exception as e:
            print(f"  Warning: Could not load sales state: {e}")
        
        try:
            records = self._get_records_with_retry(self.transfers_sheet)
            if records:
                disposal_timestamps.append(max(r['Timestamp'] for r in records))
        except Exception as e:
            print(f"  Warning: Could not load transfer state: {e}")
        
        self.last_disposal_timestamp = max(disposal_timestamps)

    def _reset_income_lots_if_sales_empty(self):
        """Reset income lots to OPEN status if sales sheet is empty.
        
        This allows reprocessing of lots if the sales sheet was cleared manually.
        """
        try:
            sales_records = self._get_records_with_retry(self.sales_sheet)
            if not sales_records:
                # Sales sheet is empty, reset all income lots to OPEN
                income_records = self._get_records_with_retry(self.income_sheet)
                if income_records:
                    # Check if any lots are not OPEN
                    has_consumed = any(r.get('Status') != 'OPEN' for r in income_records)
                    if has_consumed:
                        print("  ⚠️  Sales sheet empty but income lots have consumed status - resetting...")
                        # Clear and rewrite income sheet with all lots as OPEN
                        rows_to_write = []
                        for record in income_records:
                            # Reset status and consumption fields
                            record['Status'] = 'OPEN'
                            record['Remaining Alpha'] = record.get('Alpha Amount', 0)
                            lot = AlphaLot.from_record(record)
                            lot.status = LotStatus.OPEN
                            lot.remaining_alpha = lot.alpha_amount
                            rows_to_write.append(lot.to_sheet_row())
                        
                        # Clear and rewrite
                        all_values = self.income_sheet.get_all_values()
                        if len(all_values) > 1:
                            self.income_sheet.batch_clear([f'A2:Z{len(all_values)}'])
                        self._append_rows_with_retry(self.income_sheet, rows_to_write)
                        print(f"  ✓ Reset {len(rows_to_write)} income lots to OPEN")
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
        
        print(f"  Counters: ALPHA={self.alpha_lot_counter}, SALE={self.sale_counter}, TAO={self.tao_lot_counter}, XFER={self.transfer_counter}")

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
            print(f"  Warning: Could not load TAO lots: {e}")
        
        # Load sales
        try:
            records = self._get_records_with_retry(self.sales_sheet)
            for record in records:
                sale = AlphaSale.from_record(record)
                self.sales.append(sale)
        except Exception as e:
            print(f"  Warning: Could not load sales data: {e}")
        
        # Load transfers
        try:
            records = self._get_records_with_retry(self.transfers_sheet)
            for record in records:
                transfer = TaoTransfer.from_record(record)
                self.transfers.append(transfer)
        except Exception as e:
            print(f"  Warning: Could not load transfer data: {e}")

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
        """Run the mining tracker processing.
        
        Processing order:
        1. Income phase - creates lots (no consumption):
           - Mining emissions → ALPHA lots (via staking emissions with Mining source)
        
        2. Disposal phase - consumes lots (must be chronological):
           - Sales (consume ALPHA, create TAO)
           - Transfers (consume TAO)
           All processed together in timestamp order to ensure correct lot consumption.
        """
        # Phase 1: Process mining emissions (creates ALPHA lots)
        self.process_staking_emissions(start_time=start_time, end_time=end_time)
        
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
        
        For mining, this is simpler than contract tracker:
        - Sales: UNDELEGATE without transfer (same as contract)
        - Transfers: TAO → brokerage (same as contract)
        - NO Expenses: Miners don't do transfer undelegations
        
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
            
            # Note: No expenses for mining - miners don't do transfer undelegations
        
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

        # Load all records once (no expenses or deposits for mining)
        income_records = self.income_sheet.get_all_records()
        sales_records = self.sales_sheet.get_all_records()
        transfer_records = self.transfers_sheet.get_all_records()

        # Mining has no expenses or deposits
        expense_records = []
        deposit_records = []

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
            self.journal_sheet.append_row(entry.to_sheet_row())

        self._print_journal_summary(year_month, len(entries), summary)
        return entries

    def _print_journal_summary(self, year_month: str, entry_count: int, summary: dict):
        """Print a summary of the generated journal entries."""
        print(f"\n📊 Journal Summary for {year_month}:")
        print(f"  Total entries: {entry_count}")
        if summary:
            for key, value in summary.items():
                if isinstance(value, (int, float)):
                    print(f"  {key}: ${value:,.2f}")
                else:
                    print(f"  {key}: {value}")

    # -------------------------------------------------------------------------
    # Sheet Management
    # -------------------------------------------------------------------------

    def write_all_data_to_sheets(self):
        """Atomically write all in-memory data to sheets."""
        print("\n💾 Writing all data to sheets...")
        
        # Sort all data by timestamp before writing
        self.alpha_lots.sort(key=lambda x: x.timestamp)
        self.tao_lots.sort(key=lambda x: x.timestamp)
        self.sales.sort(key=lambda x: x.timestamp)
        self.transfers.sort(key=lambda x: x.timestamp)
        
        # Clear all sheets first
        sheets_to_clear = [
            (self.income_sheet, "Income", len(self.alpha_lots)),
            (self.tao_lots_sheet, "TAO Lots", len(self.tao_lots)),
            (self.sales_sheet, "Sales", len(self.sales)),
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
        
        if self.transfers:
            rows = [transfer.to_sheet_row() for transfer in self.transfers]
            self._append_rows_with_retry(self.transfers_sheet, rows)
            print(f"  ✓ Wrote {len(rows)} transfer records")
        
        print("✓ All data written to sheets\n")

    def clear_all_sheets(self):
        """Clear all data from tracking sheets (except headers)."""
        print("\n🗑️  Clearing all sheets...")
        
        sheets_to_clear = [
            (self.income_sheet, "Income"),
            (self.tao_lots_sheet, "TAO Lots"),
            (self.sales_sheet, "Sales"),
            (self.transfers_sheet, "Transfers"),
            (self.journal_sheet, "Journal Entries"),
        ]
        
        for worksheet, name in sheets_to_clear:
            try:
                all_values = worksheet.get_all_values()
                if len(all_values) > 1:
                    last_row = len(all_values)
                    worksheet.batch_clear([f'A2:Z{last_row}'])
                    print(f"  ✓ Cleared {name} sheet ({last_row - 1} rows)")
                else:
                    print(f"  ✓ {name} sheet already empty")
            except Exception as e:
                print(f"  Warning: Could not clear {name} sheet: {e}")
        
        # Clear in-memory data too
        self.alpha_lots = []
        self.tao_lots = []
        self.sales = []
        self.transfers = []
        
        # Reset counters
        self.alpha_lot_counter = 1
        self.sale_counter = 1
        self.tao_lot_counter = 1
        self.transfer_counter = 1
        
        # Reset timestamps
        self.last_staking_income_timestamp = 0
        self.last_income_timestamp = 0
        self.last_disposal_timestamp = 0
        
        print("✓ All sheets cleared\n")

    def create_opening_lots(self, start_time: int):
        """Create opening ALPHA lot based on balance from the day before start_time.
        
        Args:
            start_time: Unix timestamp of the first day to process.
                       Opening lot will be created from balance at end of previous day.
        """
        print(f"\nCreating opening lots for start date...")
        self._create_opening_alpha_lot(start_time)
        print("✓ Opening lots created\n")

    def _create_opening_alpha_lot(self, start_time: int):
        """Create an opening ALPHA lot from the balance before start_time."""
        # Get balance at midnight before start_time
        previous_day_end = start_time - 1
        
        stake_balances = self.wallet_client.get_stake_balance_history(
            netuid=self.subnet_id,
            hotkey=self.hotkey_ss58,
            coldkey=self.coldkey_ss58,
            start_time=previous_day_end - SECONDS_PER_DAY,
            end_time=previous_day_end
        )
        
        if not stake_balances:
            print("  No stake balance found for opening lot")
            return
        
        # Get the last balance before start_time
        latest_balance = max(stake_balances, key=lambda x: x.timestamp_unix)
        alpha_balance = latest_balance.stake / RAO_PER_TAO
        
        if alpha_balance <= 0:
            print("  No alpha balance for opening lot")
            return
        
        # Get price at the balance timestamp
        tao_price = self.price_client.get_price_at_timestamp('TAO', latest_balance.timestamp_unix)
        
        # Create opening lot
        lot = AlphaLot(
            lot_id=self._next_alpha_lot_id(),
            timestamp=latest_balance.timestamp_unix,
            alpha_amount=alpha_balance,
            tao_amount=0,  # Opening balance - no TAO equivalent tracked
            usd_value=alpha_balance * tao_price,  # Approximate USD value
            tao_price_at_receipt=tao_price,
            source_type=SourceType.OPENING_BALANCE,
            status=LotStatus.OPEN,
            remaining_alpha=alpha_balance
        )
        
        self.alpha_lots.append(lot)
        print(f"  Created opening ALPHA lot: {lot.lot_id} with {alpha_balance:.4f} ALPHA")
