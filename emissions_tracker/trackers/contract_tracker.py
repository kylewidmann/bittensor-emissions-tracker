import gspread
import backoff
import time
from collections import defaultdict
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from emissions_tracker.config import TrackerSettings, WaveAccountSettings
from emissions_tracker.models import (
    AlphaLot, TaoLot, AlphaSale, Expense, TaoStatsStakeBalance, TaoTransfer,
    SourceType, LotStatus, CostBasisMethod, TaoStatsDelegation
)
from emissions_tracker.trackers.bittensor_tracker import BittensorTracker, _is_rate_limit_error
from oauth2client.service_account import ServiceAccountCredentials

RAO_PER_TAO = 10 ** 9


class ContractTracker(BittensorTracker):
    """Tracker for smart contract emissions and related activities."""
    
    # Sheet names
    INCOME_SHEET = "Income"
    SALES_SHEET = "Sales"
    EXPENSES_SHEET = "Expenses"
    TRANSFERS_SHEET = "Transfers"
    JOURNAL_SHEET = "Journal Entries"
    TAO_LOTS_SHEET = "TAO Lots"

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
        sheet_configs = [
            (self.INCOME_SHEET, AlphaLot.sheet_headers()),
            (self.SALES_SHEET, AlphaSale.sheet_headers()),
            (self.EXPENSES_SHEET, Expense.sheet_headers()),
            (self.TAO_LOTS_SHEET, TaoLot.sheet_headers()),
            (self.TRANSFERS_SHEET, TaoTransfer.sheet_headers()),
        ]
        
        for sheet_name, headers in sheet_configs:
            try:
                worksheet = self.sheet.worksheet(sheet_name)
                self._ensure_sheet_headers(worksheet, headers, sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = self.sheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
                worksheet.append_row(headers)
                print(f"  Created sheet: {sheet_name}")
        
        # Store worksheet references
        self.income_sheet = self.sheet.worksheet(self.INCOME_SHEET)
        self.sales_sheet = self.sheet.worksheet(self.SALES_SHEET)
        self.expenses_sheet = self.sheet.worksheet(self.EXPENSES_SHEET)
        self.tao_lots_sheet = self.sheet.worksheet(self.TAO_LOTS_SHEET)
        self.transfers_sheet = self.sheet.worksheet(self.TRANSFERS_SHEET)

    def _ensure_sheet_headers(self, worksheet, expected_headers, label: str):
        """Ensure worksheet header row matches expected schema."""
        try:
            existing_headers = worksheet.row_values(1)
            if existing_headers != expected_headers:
                worksheet.update('A1', [expected_headers])
                print(f"  Updated {label} headers")
        except Exception as e:
            print(f"  Warning: Could not verify {label} headers: {e}")

    def _load_state(self):
        """Load last processed timestamps from sheets."""
        self.last_contract_income_timestamp = 0
        self.last_staking_income_timestamp = 0
        self.last_income_timestamp = 0
        self.last_sale_timestamp = 0
        self.last_expense_timestamp = 0
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

        updates = []
        for idx, record in enumerate(records, start=2):
            alpha_rao = record.get('Alpha RAO', 0)
            if alpha_rao > 0:
                updates.append({
                    'range': f'I{idx}',  # Alpha RAO Remaining column
                    'values': [[alpha_rao]]
                })
                updates.append({
                    'range': f'M{idx}',  # Status column
                    'values': [['Open']]
                })

        if not updates:
            return

        body = {
            "valueInputOption": "RAW",
            "data": updates,
        }
        try:
            self.income_sheet.spreadsheet.batch_update(body)
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
            records = self.tao_lots_sheet.get_all_records()
            if records:
                lot_ids = [r['Lot ID'] for r in records if r.get('Lot ID', '').startswith('TAO-')]
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
        
        print(f"  Counters: ALPHA={self.alpha_lot_counter}, SALE={self.sale_counter}, EXPENSE={self.expense_counter}, TAO={self.tao_lot_counter}, XFER={self.transfer_counter}")

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

    def run(self, lookback_days: int = 1):
        """Run the contract tracker processing."""
        # Implementation for running the contract tracker
        self.process_contract_income(lookback_days=lookback_days)
        self.process_staking_emissions(lookback_days=lookback_days)
        self.process_alpha_sales(lookback_days=lookback_days)
        self.process_expenses(lookback_days=lookback_days)
        self.process_tao_transfers(lookback_days=lookback_days)

    def process_contract_income(self, lookback_days: int = 1) -> list:
        """Process contract income over the specified lookback period.

        Args:
            lookback_days (int): Number of days to look back for processing.

        Returns:
            list: List of processed emission lots.
        """

        start_time, end_time = self._resolve_time_window(
            "contract income",
            self.last_contract_income_timestamp,
            lookback_days
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

    def process_alpha_sales(self) -> list:
        """Process ALPHA sales over the specified lookback period.

        Args:
            lookback_days (int): Number of days to look back for processing.

        Returns:
            list: List of processed emission lots.
        """
        # Implementation for processing ALPHA sales
        ...

    def process_expenses(self) -> list:
        """Process expenses over the specified lookback period.

        Args:
            lookback_days (int): Number of days to look back for processing.

        Returns:
            list: List of processed expense lots.
        """
        # Implementation for processing expenses
        ...

    def process_staking_emissions(self, lookback_days: int = 1) -> list:
        """Process staking emissions over the specified lookback period.

        Returns:
            list: List of processed emission lots.
        """
        start_time, end_time = self._resolve_time_window(
            "staking emissions",
            self.last_staking_income_timestamp,
            lookback_days
        )

        # Get stake balance history for the date range
        stake_balances = self.wallet_client.get_stake_balance_history(
            netuid=self.subnet_id,
            hotkey=self.validator_ss58,
            coldkey=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time
        )

        if not stake_balances:
            print("ℹ️  No stake balance history found")
            return []

        # Get all delegation events (DELEGATE and UNDELEGATE) in the same period
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
        balances_by_day = defaultdict(list[TaoStatsStakeBalance])
        for balance in stake_balances:
            balances_by_day[balance.day].append(balance)
        
        # Get the last balance for each day (closest to 23:59:59)
        daily_balances = dict[str, TaoStatsStakeBalance]()  
        for day_key, balances in balances_by_day.items():
            # Sort by timestamp descending to get the latest balance of the day
            latest_balance = sorted(balances, key=lambda b: b.timestamp_unix, reverse=True)[0]
            daily_balances[day_key] = latest_balance
        
        # Group delegation events by day
        delegations_by_day = defaultdict(list[TaoStatsDelegation])
        for delegation in delegations:
            delegations_by_day[delegation.day].append(delegation)
        
        # Calculate emissions for each day
        alpha_lots = []
        sorted_days = sorted(daily_balances.keys())
        
        for i in range(1, len(sorted_days)):
            prev_day = sorted_days[i - 1]
            current_day = sorted_days[i]
            
            prev_balance = daily_balances[prev_day]
            current_balance = daily_balances[current_day]
            
            # Balance change in RAO
            balance_change_rao = int(current_balance.balance) - int(prev_balance.balance)
            
            # Adjust for DELEGATE (outflows - reduce emissions) and UNDELEGATE (inflows - already in balance)
            day_delegations = delegations_by_day.get(current_day, [])
            
            delegate_alpha_rao = sum(
                int(d.alpha) for d in day_delegations 
                if d.action == 'DELEGATE'
            )
            
            undelegate_alpha_rao = sum(
                int(d.alpha) for d in day_delegations 
                if d.action == 'UNDELEGATE'
            )
            
            # Calculate net emissions
            # emissions = balance_change - delegates + undelegates
            emissions_rao = balance_change_rao - delegate_alpha_rao + undelegate_alpha_rao
            
            # Only create lots for positive emissions
            if emissions_rao > 0:
                # Use the current day's balance timestamp (latest timestamp of the day)
                lot = AlphaLot(
                    lot_id=self._next_alpha_lot_id(),
                    timestamp=current_balance.timestamp_unix,
                    block_number=current_balance.block_number,
                    source_type=SourceType.STAKING,
                    alpha_rao=emissions_rao,
                    alpha_rao_remaining=emissions_rao,
                    usd_fmv=0.0,  # Will need TAO price to calculate
                    usd_per_alpha=0.0,  # Will need TAO price to calculate
                    tao_equivalent=0.0,  # Not applicable for staking emissions
                    notes=f"Staking emissions for {current_day}"
                )
                alpha_lots.append(lot)
        
        return alpha_lots

    def process_tao_transfers(self) -> list:
        """Process TAO transfers over the specified lookback period.

        Args:
            lookback_days (int): Number of days to look back for processing.

        Returns:
            list: List of processed transfer lots.
        """
        # Implementation for processing TAO transfers
        ...
