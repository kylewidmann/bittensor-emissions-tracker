import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, timezone
import time
from typing import List, Dict, Any, Optional, Tuple

from emissions_tracker.clients.price import PriceClient
from emissions_tracker.clients.wallet import WalletClientInterface
from emissions_tracker.config import TrackerSettings, WaveAccountSettings
from emissions_tracker.exceptions import PriceNotAvailableError, InsufficientLotsError
from emissions_tracker.models import (
    AlphaLot, TaoLot, AlphaSale, TaoTransfer, JournalEntry,
    LotConsumption, SourceType, LotStatus, GainType
)


class BittensorEmissionTracker:
    """
    Tracks Bittensor ALPHA/TAO transactions for tax accounting purposes.
    
    Implements:
    - ALPHA income tracking (Contract + Staking emissions)
    - FIFO lot consumption for disposals
    - Capital gains calculation for ALPHA → TAO and TAO → Kraken
    - Monthly Wave journal entry generation
    """
    
    # Sheet names
    INCOME_SHEET = "Income"
    SALES_SHEET = "Sales"  
    TRANSFERS_SHEET = "Transfers"
    JOURNAL_SHEET = "Journal Entries"
    TAO_LOTS_SHEET = "TAO Lots"  # Internal tracking sheet
    
    def __init__(self, price_client: PriceClient, wallet_client: WalletClientInterface):
        self.config = TrackerSettings()
        self.wave_config = WaveAccountSettings()
        self.price_client = price_client
        self.wallet_client = wallet_client
        
        # Wallet addresses
        self.wallet_address = self.config.wallet_ss58
        self.validator_address = self.config.validator_ss58
        self.brokerage_address = self.config.brokerage_ss58
        self.smart_contract_address = self.config.smart_contract_ss58
        self.subnet_id = self.config.subnet_id
        
        print(f"Initializing tracker:")
        print(f"  Wallet: {self.wallet_address}")
        print(f"  Validator: {self.validator_address}")
        print(f"  Brokerage (Kraken): {self.brokerage_address}")
        print(f"  Smart Contract: {self.smart_contract_address}")
        
        # Connect to Google Sheets
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.config.tracker_google_credentials, scope
        )
        self.sheets_client = gspread.authorize(creds)
        self.sheet = self.sheets_client.open_by_key(self.config.tracker_sheet_id)
        
        # Initialize sheets
        self._init_sheets()
        
        # Load state
        self._load_state()
        
        # Counters for ID generation
        self._load_counters()

    def _init_sheets(self):
        """Initialize all tracking sheets with headers."""
        sheet_configs = [
            (self.INCOME_SHEET, AlphaLot.sheet_headers()),
            (self.SALES_SHEET, AlphaSale.sheet_headers()),
            (self.TAO_LOTS_SHEET, TaoLot.sheet_headers()),
            (self.TRANSFERS_SHEET, TaoTransfer.sheet_headers()),
            (self.JOURNAL_SHEET, JournalEntry.sheet_headers()),
        ]
        
        for sheet_name, headers in sheet_configs:
            try:
                worksheet = self.sheet.worksheet(sheet_name)
                print(f"  ✓ Found sheet: {sheet_name}")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = self.sheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
                worksheet.append_row(headers)
                print(f"  ✓ Created sheet: {sheet_name}")
        
        # Store worksheet references
        self.income_sheet = self.sheet.worksheet(self.INCOME_SHEET)
        self.sales_sheet = self.sheet.worksheet(self.SALES_SHEET)
        self.tao_lots_sheet = self.sheet.worksheet(self.TAO_LOTS_SHEET)
        self.transfers_sheet = self.sheet.worksheet(self.TRANSFERS_SHEET)
        self.journal_sheet = self.sheet.worksheet(self.JOURNAL_SHEET)

    def _load_state(self):
        """Load last processed timestamps from sheets."""
        self.last_income_timestamp = 0  # Overall income watermark (legacy)
        self.last_contract_income_timestamp = 0
        self.last_staking_income_timestamp = 0
        self.last_sale_timestamp = 0
        self.last_transfer_timestamp = 0
        
        try:
            records = self.income_sheet.get_all_records()
            if records:
                contract_ts = [r['Timestamp'] for r in records if r['Source Type'] == SourceType.CONTRACT.value]
                staking_ts = [r['Timestamp'] for r in records if r['Source Type'] == SourceType.STAKING.value]
                
                if contract_ts:
                    self.last_contract_income_timestamp = max(contract_ts)
                if staking_ts:
                    self.last_staking_income_timestamp = max(staking_ts)
                
                # Maintain legacy aggregate for reference
                self.last_income_timestamp = max(contract_ts + staking_ts) if (contract_ts or staking_ts) else 0
                
                print(f"  Last contract income timestamp: {self.last_contract_income_timestamp}")
                print(f"  Last staking income timestamp:   {self.last_staking_income_timestamp}")
        except Exception as e:
            print(f"  Warning: Could not load income state: {e}")
        
        try:
            records = self.sales_sheet.get_all_records()
            if records:
                self.last_sale_timestamp = max(r['Timestamp'] for r in records)
                print(f"  Last sale timestamp: {self.last_sale_timestamp}")
        except Exception as e:
            print(f"  Warning: Could not load sales state: {e}")
        
        try:
            records = self.transfers_sheet.get_all_records()
            if records:
                self.last_transfer_timestamp = max(r['Timestamp'] for r in records)
                print(f"  Last transfer timestamp: {self.last_transfer_timestamp}")
        except Exception as e:
            print(f"  Warning: Could not load transfers state: {e}")

    def _load_counters(self):
        """Load ID counters from existing data."""
        try:
            records = self.income_sheet.get_all_records()
            if records:
                max_id = max(int(r['Lot ID'].split('-')[1]) for r in records if r['Lot ID'])
                self.alpha_lot_counter = max_id + 1
            else:
                self.alpha_lot_counter = 1
        except:
            self.alpha_lot_counter = 1
        
        try:
            records = self.sales_sheet.get_all_records()
            if records:
                max_id = max(int(r['Sale ID'].split('-')[1]) for r in records if r['Sale ID'])
                self.sale_counter = max_id + 1
            else:
                self.sale_counter = 1
        except:
            self.sale_counter = 1
        
        try:
            records = self.tao_lots_sheet.get_all_records()
            if records:
                max_id = max(int(r['TAO Lot ID'].split('-')[1]) for r in records if r['TAO Lot ID'])
                self.tao_lot_counter = max_id + 1
            else:
                self.tao_lot_counter = 1
        except:
            self.tao_lot_counter = 1
        
        try:
            records = self.transfers_sheet.get_all_records()
            if records:
                max_id = max(int(r['Transfer ID'].split('-')[1]) for r in records if r['Transfer ID'])
                self.transfer_counter = max_id + 1
            else:
                self.transfer_counter = 1
        except:
            self.transfer_counter = 1
        
        print(f"  Counters: ALPHA={self.alpha_lot_counter}, SALE={self.sale_counter}, TAO={self.tao_lot_counter}, XFER={self.transfer_counter}")

    # -------------------------------------------------------------------------
    # Lightweight logging / timing helpers
    # -------------------------------------------------------------------------
    def _log(self, msg: str):
        """Print a timestamped log message."""
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{ts}  {msg}")

    def _timed_call(self, label: str, func, *args, **kwargs):
        """Call func(*args, **kwargs) while logging start/end and elapsed time.

        If the result is a sequence, also log the number of items returned.
        """
        start = time.time()
        self._log(f"Fetching {label} — start")
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            elapsed = time.time() - start
            self._log(f"Fetching {label} — failed after {elapsed:.2f}s: {e}")
            raise

        elapsed = time.time() - start
        # Try to log length when available
        try:
            count = len(result) if result is not None else 0
        except Exception:
            count = None

        if count is not None:
            self._log(f"Fetching {label} — done ({count} items) in {elapsed:.2f}s")
        else:
            self._log(f"Fetching {label} — done in {elapsed:.2f}s")

        return result

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

    def get_tao_price(self, timestamp: int) -> Optional[float]:
        """Fetch TAO price at a given timestamp."""
        try:
            price = self._timed_call(f"price_at_timestamp {timestamp}", self.price_client.get_price_at_timestamp, 'TAO', timestamp)
            return price
        except PriceNotAvailableError as e:
            print(f"  Warning: Could not get TAO price: {e}")
            return None
        
    # -------------------------------------------------------------------------
    # ALPHA Lot Management (FIFO)
    # -------------------------------------------------------------------------
    
    def _get_alpha_records_with_rows(self) -> List[Dict[str, Any]]:
        """Return income sheet records with sheet row numbers included."""
        records = self.income_sheet.get_all_records()
        for idx, record in enumerate(records, start=2):  # +1 for header, +1 for 1-indexing
            record['_row_num'] = idx
        return records
    
    def get_open_alpha_lots(self) -> List[Dict[str, Any]]:
        """Get all ALPHA lots with remaining balance, sorted by timestamp (FIFO)."""
        records = self._get_alpha_records_with_rows()
        open_lots = [
            r for r in records 
            if r['Status'] in ('Open', 'Partial') and r['Alpha Remaining'] > 0
        ]
        # Support configurable lot consumption strategies. Default is FIFO (old behavior).
        strategy = getattr(self.config, 'lot_strategy', 'FIFO')
        if isinstance(strategy, str) and strategy.upper() == 'HIFO':
            # Highest cost-basis first: compute USD per ALPHA from sheet fields when available.
            def _unit_price(l):
                try:
                    qty = l.get('Alpha Quantity') or 0
                    fmv = l.get('USD FMV') or 0
                    return (fmv / qty) if qty else 0
                except Exception:
                    return 0
            # Sort by unit price desc (highest first), tiebreaker by timestamp asc
            return sorted(open_lots, key=lambda x: (-_unit_price(x), x['Timestamp']))
        # FIFO: sort by timestamp ascending
        return sorted(open_lots, key=lambda x: x['Timestamp'])
    
    def consume_alpha_lots_fifo(self, alpha_needed: float) -> Tuple[List[LotConsumption], float, GainType, List[Dict[str, Any]]]:
        """
        Consume ALPHA lots in FIFO order.
        
        Args:
            alpha_needed: Amount of ALPHA to consume
            
        Returns:
            Tuple of (consumed lots, total cost basis, gain type)
            
        Raises:
            InsufficientLotsError: If not enough ALPHA available
        """
        open_lots = self.get_open_alpha_lots()
        total_available = sum(lot['Alpha Remaining'] for lot in open_lots)
        
        if total_available < alpha_needed:
            raise InsufficientLotsError(
                f"Need {alpha_needed:.4f} ALPHA but only {total_available:.4f} available"
            )
        
        consumed = []
        total_basis = 0.0
        remaining_need = alpha_needed
        earliest_acquisition = None
        now = int(time.time())
        one_year_ago = now - (365 * 24 * 60 * 60)
        all_long_term = True
        lot_updates = []
        
        for lot in open_lots:
            if remaining_need <= 0:
                break
            
            lot_id = lot['Lot ID']
            available = lot['Alpha Remaining']
            original_qty = lot['Alpha Quantity']
            original_basis = lot['USD FMV']
            acquisition_ts = lot['Timestamp']
            
            # Track earliest acquisition for gain type determination
            if earliest_acquisition is None or acquisition_ts < earliest_acquisition:
                earliest_acquisition = acquisition_ts
            
            # Check if this lot is long-term
            if acquisition_ts > one_year_ago:
                all_long_term = False
            
            # Calculate consumption
            to_consume = min(available, remaining_need)
            basis_consumed = (to_consume / original_qty) * original_basis
            
            consumed.append(LotConsumption(
                lot_id=lot_id,
                alpha_consumed=to_consume,
                cost_basis_consumed=basis_consumed,
                acquisition_timestamp=acquisition_ts
            ))
            
            total_basis += basis_consumed
            remaining_need -= to_consume
            
            new_remaining = available - to_consume
            new_status = LotStatus.CLOSED.value if new_remaining == 0 else LotStatus.PARTIAL.value
            lot_updates.append({
                "lot_id": lot_id,
                "row_num": lot["_row_num"],
                "remaining": new_remaining,
                "status": new_status
            })
        
        gain_type = GainType.LONG_TERM if all_long_term else GainType.SHORT_TERM
        return consumed, total_basis, gain_type, lot_updates
    
    def _batch_update_alpha_lots(self, updates: List[Dict[str, Any]]):
        """Batch update ALPHA lot remaining amounts/status to reduce write calls."""
        if not updates:
            return
        
        data = []
        for upd in updates:
            row = upd["row_num"]
            data.append({
                "range": f"{self.INCOME_SHEET}!I{row}:I{row}",  # Alpha Remaining
                "values": [[upd["remaining"]]]
            })
            data.append({
                "range": f"{self.INCOME_SHEET}!N{row}:N{row}",  # Status
                "values": [[upd["status"]]]
            })
        
        body = {
            "valueInputOption": "RAW",
            "data": data
        }
        
        # Simple retry to handle transient 429s
        for attempt in range(3):
            try:
                self.sheet.values_batch_update(body)
                return
            except Exception as e:
                if attempt == 2:
                    raise
                sleep_for = 2 ** attempt
                print(f"  Warning: batch alpha update failed (attempt {attempt + 1}), retrying in {sleep_for}s: {e}")
                time.sleep(sleep_for)
    
    # -------------------------------------------------------------------------
    # TAO Lot Management (FIFO)
    # -------------------------------------------------------------------------
    
    def get_open_tao_lots(self) -> List[Dict[str, Any]]:
        """Get all TAO lots with remaining balance, sorted by timestamp (FIFO)."""
        records = self.tao_lots_sheet.get_all_records()
        open_lots = [
            r for r in records 
            if r['Status'] in ('Open', 'Partial') and r['TAO Remaining'] > 0
        ]
        return sorted(open_lots, key=lambda x: x['Timestamp'])
    
    def consume_tao_lots_fifo(self, tao_needed: float) -> Tuple[List[LotConsumption], float, GainType, List[Dict[str, Any]]]:
        """
        Consume TAO lots in FIFO order.
        
        Returns:
            Tuple of (consumed lots, total cost basis, gain type)
        """
        # Attach row numbers to avoid multiple lookups when updating
        records = self.tao_lots_sheet.get_all_records()
        open_lots = []
        for idx, lot in enumerate(records, start=2):
            if lot['Status'] in ('Open', 'Partial') and lot['TAO Remaining'] > 0:
                lot['_row_num'] = idx
                open_lots.append(lot)
        # Ordering depends on lot strategy: FIFO (by timestamp) or HIFO (by USD basis per TAO desc)
        strategy = getattr(self.config, 'lot_strategy', 'FIFO')
        if isinstance(strategy, str) and strategy.upper() == 'HIFO':
            def _unit_price_tao(l):
                try:
                    qty = l.get('TAO Quantity') or 0
                    basis = l.get('USD Basis') or 0
                    return (basis / qty) if qty else 0
                except Exception:
                    return 0
            open_lots = sorted(open_lots, key=lambda x: (-_unit_price_tao(x), x['Timestamp']))
        else:
            open_lots = sorted(open_lots, key=lambda x: x['Timestamp'])
        total_available = sum(lot['TAO Remaining'] for lot in open_lots)
        
        if total_available < tao_needed:
            raise InsufficientLotsError(
                f"Need {tao_needed:.4f} TAO but only {total_available:.4f} available"
            )
        
        consumed = []
        total_basis = 0.0
        remaining_need = tao_needed
        now = int(time.time())
        one_year_ago = now - (365 * 24 * 60 * 60)
        all_long_term = True
        lot_updates = []
        
        for lot in open_lots:
            if remaining_need <= 0:
                break
            
            lot_id = lot['TAO Lot ID']
            available = lot['TAO Remaining']
            original_qty = lot['TAO Quantity']
            original_basis = lot['USD Basis']
            acquisition_ts = lot['Timestamp']
            
            if acquisition_ts > one_year_ago:
                all_long_term = False
            
            to_consume = min(available, remaining_need)
            basis_consumed = (to_consume / original_qty) * original_basis
            
            consumed.append(LotConsumption(
                lot_id=lot_id,
                alpha_consumed=to_consume,  # Reusing field for TAO amount
                cost_basis_consumed=basis_consumed,
                acquisition_timestamp=acquisition_ts
            ))
            
            total_basis += basis_consumed
            remaining_need -= to_consume
            
            new_remaining = available - to_consume
            new_status = LotStatus.CLOSED.value if new_remaining == 0 else LotStatus.PARTIAL.value
            lot_updates.append({
                "lot_id": lot_id,
                "row_num": lot["_row_num"],
                "remaining": new_remaining,
                "status": new_status
            })
        
        gain_type = GainType.LONG_TERM if all_long_term else GainType.SHORT_TERM
        return consumed, total_basis, gain_type, lot_updates
    
    def _batch_update_tao_lots(self, updates: List[Dict[str, Any]]):
        """Batch update TAO lot remaining amounts/status to reduce write calls."""
        if not updates:
            return
        
        data = []
        for upd in updates:
            row = upd["row_num"]
            data.append({
                "range": f"{self.TAO_LOTS_SHEET}!F{row}:F{row}",  # TAO Remaining
                "values": [[upd["remaining"]]]
            })
            data.append({
                "range": f"{self.TAO_LOTS_SHEET}!K{row}:K{row}",  # Status
                "values": [[upd["status"]]]
            })
        
        body = {
            "valueInputOption": "RAW",
            "data": data
        }
        
        for attempt in range(3):
            try:
                self.sheet.values_batch_update(body)
                return
            except Exception as e:
                if attempt == 2:
                    raise
                sleep_for = 2 ** attempt
                print(f"  Warning: batch TAO lot update failed (attempt {attempt + 1}), retrying in {sleep_for}s: {e}")
                time.sleep(sleep_for)
    
    def _append_with_retry(self, worksheet, row_values: List[Any], label: str):
        """Append a single row with small retry/backoff to handle rate limiting."""
        for attempt in range(3):
            try:
                worksheet.append_row(row_values)
                return
            except Exception as e:
                if attempt == 2:
                    raise
                sleep_for = 2 ** attempt
                print(f"  Warning: append to {label} failed (attempt {attempt + 1}), retrying in {sleep_for}s: {e}")
                time.sleep(sleep_for)
    
    def _append_rows_with_retry(self, worksheet, rows: List[List[Any]], label: str):
        """Append multiple rows with retry/backoff to reduce API calls."""
        if not rows:
            return
        for attempt in range(3):
            try:
                worksheet.append_rows(rows)
                return
            except Exception as e:
                if attempt == 2:
                    raise
                sleep_for = 2 ** attempt
                print(f"  Warning: append rows to {label} failed (attempt {attempt + 1}), retrying in {sleep_for}s: {e}")
                time.sleep(sleep_for)

    def _sort_sheet_by_timestamp(self, worksheet, timestamp_col: int, label: str, range_str: str = "A2:Z"):
        """Sort a worksheet by a timestamp column (ascending) excluding header row."""
        try:
            worksheet.sort((timestamp_col, 'des'))
        except Exception as e:
            print(f"  Warning: could not sort {label} sheet: {e}")
            return
            
    # -------------------------------------------------------------------------
    # Income Processing (ALPHA Lot Creation)
    # -------------------------------------------------------------------------
    
    def process_contract_income(self, days_back: int = 7) -> List[AlphaLot]:
        """
        Process contract income from DELEGATE events with smart contract transfer address.
        
        Returns:
            List of newly created ALPHA lots
        """
        print(f"\n{'='*60}")
        print("Processing Contract Income")
        print(f"{'='*60}")
        
        end_time = int(time.time())
        default_start = end_time - (days_back * 86400)
        start_time = max(
            self.last_contract_income_timestamp + 1,
            default_start
        )
        
        print(f"Fetching delegations from {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}")

        delegations = self._timed_call(
            "delegations (contract)",
            self.wallet_client.get_delegations,
            netuid=self.subnet_id,
            delegate=self.validator_address,
            nominator=self.wallet_address,
            start_time=start_time,
            end_time=end_time
        )
        
        new_lots = []
        for d in delegations:
            # Filter: DELEGATE events from smart contract
            if (d['action'] == 'DELEGATE' and 
                d.get('is_transfer') == True and 
                d.get('transfer_address') == self.smart_contract_address and
                d['timestamp'] > self.last_contract_income_timestamp):
                
                lot = self._create_alpha_lot_from_delegation(d, SourceType.CONTRACT)
                if lot:
                    new_lots.append(lot)
        
        if new_lots:
            max_ts = max(lot.timestamp for lot in new_lots)
            self.last_contract_income_timestamp = max_ts
            self.last_income_timestamp = max(self.last_contract_income_timestamp, self.last_staking_income_timestamp)
            # Keep sheet sorted by timestamp (column 3)
            self._sort_sheet_by_timestamp(self.income_sheet, timestamp_col=3, label="Income", range_str="A2:O")
            print(f"\n✓ Created {len(new_lots)} contract income lots")
        else:
            print("ℹ️  No new contract income found")
        
        return new_lots
    
    def process_staking_emissions(self, days_back: int = 7) -> List[AlphaLot]:
        """
        Process staking emissions by comparing balance history with DELEGATE events.
        
        Emissions = Balance increase - DELEGATE inflows
        
        Returns:
            List of newly created ALPHA lots
        """
        print(f"\n{'='*60}")
        print("Processing Staking Emissions")
        print(f"{'='*60}")
        
        end_time = int(time.time())
        default_start = end_time - (days_back * 86400)
        if self.last_staking_income_timestamp > 0:
            start_time = max(self.last_staking_income_timestamp + 1, default_start)
        else:
            start_time = default_start
        
        # Get balance history
        balances = self._timed_call(
            "stake_balance_history",
            self.wallet_client.get_stake_balance_history,
            netuid=self.subnet_id,
            hotkey=self.validator_address,
            coldkey=self.wallet_address,
            start_time=start_time,
            end_time=end_time
        )
        # Guard against providers returning data outside requested window
        balances = [
            b for b in balances
            if start_time <= b.get('timestamp', 0) <= end_time
        ]
        
        if not balances:
            print("ℹ️  No balance history found")
            return []
        
        # Get all DELEGATE events in this period (to subtract from balance changes)
        # Also need to account for UNDELEGATE which reduces balance
        delegations = self._timed_call(
            "delegations (staking window)",
            self.wallet_client.get_delegations,
            netuid=self.subnet_id,
            delegate=self.validator_address,
            nominator=self.wallet_address,
            start_time=start_time,
            end_time=end_time
        )
        delegations = [
            d for d in delegations
            if start_time <= d.get('timestamp', 0) <= end_time
        ]
        
        # Build list of delegation events with timestamps for windowed lookup
        delegation_events = []
        for d in delegations:
            if d['action'] == 'DELEGATE' and d.get('is_transfer') == True:
                delegation_events.append({
                    'timestamp': d['timestamp'],
                    'alpha': d['alpha'],
                    'action': 'DELEGATE'
                })
            elif d['action'] == 'UNDELEGATE':
                delegation_events.append({
                    'timestamp': d['timestamp'],
                    'alpha': d['alpha'],
                    'action': 'UNDELEGATE'
                })
        
        delegation_events = sorted(delegation_events, key=lambda x: x['timestamp'])
        
        # Preload prices for the full window to minimize API calls (15m granularity)
        price_points = []
        try:
            price_points = self._timed_call(
                "price_range",
                self.price_client.get_prices_in_range,
                'TAO', start_time, end_time
            )
        except Exception as e:
            print(f"  Warning: bulk price fetch failed, falling back to per-timestamp lookup: {e}")
            price_points = []
        
        def get_price_for(ts: int) -> Optional[float]:
            if price_points:
                closest = min(price_points, key=lambda p: abs(p['timestamp'] - ts))
                return closest.get('price')
            return self.get_tao_price(ts)
        
        # Calculate emissions as balance increases not explained by DELEGATEs
        balances = sorted(balances, key=lambda b: b['timestamp'])
        new_lots = []
        total_balances = len(balances)
        pending_rows = []
        last_progress_log = time.time()
        
        for i, balance in enumerate(balances):
            if balance['timestamp'] <= self.last_staking_income_timestamp:
                # Skip already-processed windows
                self._log(f"Skipping balance at {datetime.fromtimestamp(balance['timestamp'])} because <= last_staking_income_timestamp ({self.last_staking_income_timestamp})")
                continue

            if i == 0:
                # Need previous balance to calculate delta
                self._log(f"Skipping first balance point at {datetime.fromtimestamp(balance['timestamp'])} (no previous sample)")
                continue
            
            prev_balance = balances[i - 1]
            alpha_now = balance['alpha_balance'] / 1e9
            alpha_prev = prev_balance['alpha_balance'] / 1e9
            balance_change = alpha_now - alpha_prev
            
            if balance_change == 0:
                continue
            
            # Find all delegation events between prev_balance and this balance timestamps
            window_start = prev_balance['timestamp']
            window_end = balance['timestamp']
            
            delegate_inflow = 0.0
            undelegate_outflow = 0.0
            for event in delegation_events:
                if window_start < event['timestamp'] <= window_end:
                    if event['action'] == 'DELEGATE':
                        delegate_inflow += event['alpha']
                    elif event['action'] == 'UNDELEGATE':
                        undelegate_outflow += event['alpha']
            
            # Emissions = balance_change - delegate_inflow + undelegate_outflow
            # (balance went up by balance_change, subtract delegates that contributed,
            #  add back undelegates since they reduced balance)
            emissions = balance_change - delegate_inflow + undelegate_outflow
            
            if emissions > 0.0001:  # Minimum threshold to avoid noise
                # Get TAO price for FMV calculation
                tao_price = get_price_for(balance['timestamp'])
                if not tao_price:
                    print(f"  Warning: Could not get price for block {balance['block_number']}, skipping")
                    continue
                
                # Calculate FMV using TAO equivalent ratio
                tao_ratio = balance['tao_equivalent'] / balance['alpha_balance'] if balance['alpha_balance'] > 0 else 0
                tao_equivalent = emissions * tao_ratio
                usd_fmv = tao_equivalent * tao_price
                usd_per_alpha = usd_fmv / emissions if emissions > 0 else 0
                
                # Log computed emission details for debugging/diagnosis
                self._log(
                    f"Emission detected — ts={balance['timestamp']} block={balance['block_number']} delta={balance_change:.6f} delegates_in={delegate_inflow:.6f} undelegates_out={undelegate_outflow:.6f} tao_eq={tao_equivalent:.6f} tao_price={tao_price:.4f} usd_fmv={usd_fmv:.2f}"
                )

                lot = AlphaLot(
                    lot_id=self._next_alpha_lot_id(),
                    timestamp=balance['timestamp'],
                    block_number=balance['block_number'],
                    source_type=SourceType.STAKING,
                    alpha_quantity=emissions,
                    alpha_remaining=emissions,
                    usd_fmv=usd_fmv,
                    usd_per_alpha=usd_per_alpha,
                    tao_equivalent=tao_equivalent,
                    notes=f"Staking emissions (balance delta: {balance_change:.4f}, delegates: {delegate_inflow:.4f}, undelegates: {undelegate_outflow:.4f})"
                )
                
                # Collect for batch append to avoid duplicate writes and rate limits
                pending_rows.append(lot.to_sheet_row())
                new_lots.append(lot)
                self._log(f"Prepared emission lot {lot.lot_id} — {emissions:.4f} ALPHA (${usd_fmv:.2f}) at block {balance['block_number']}")
            
            # Periodic progress indicator to show the loop is advancing
            if (i % 100 == 0) or (time.time() - last_progress_log > 5):
                pct = (i + 1) / total_balances * 100 if total_balances else 100
                ts_readable = datetime.fromtimestamp(balance['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                print(f"    Progress: {i + 1}/{total_balances} ({pct:.1f}%) — up to {ts_readable}")
                last_progress_log = time.time()
        
        if new_lots:
            # Batch append emissions to reduce write calls
            try:
                self._append_rows_with_retry(self.income_sheet, pending_rows, label="Income")
            except Exception as e:
                print(f"  Error writing emissions to sheet: {e}")
                raise
            
            # Keep sheet sorted by timestamp (column 3)
            self._sort_sheet_by_timestamp(self.income_sheet, timestamp_col=3, label="Income")
            
            max_ts = max(lot.timestamp for lot in new_lots)
            self.last_staking_income_timestamp = max(self.last_staking_income_timestamp, max_ts)
            self.last_income_timestamp = max(self.last_contract_income_timestamp, self.last_staking_income_timestamp)
            print(f"\n✓ Created {len(new_lots)} staking emission lots")
        else:
            print("ℹ️  No new staking emissions found")
        
        return new_lots
    
    def _create_alpha_lot_from_delegation(self, delegation: Dict[str, Any], source_type: SourceType) -> Optional[AlphaLot]:
        """Create an ALPHA lot from a delegation event."""
        alpha_amount = delegation['alpha']
        usd_value = delegation['usd']
        usd_per_alpha = delegation.get('alpha_price_in_usd') or (usd_value / alpha_amount if alpha_amount > 0 else 0)
        tao_equivalent = delegation['tao_amount']
        
        lot = AlphaLot(
            lot_id=self._next_alpha_lot_id(),
            timestamp=delegation['timestamp'],
            block_number=delegation['block_number'],
            source_type=source_type,
            alpha_quantity=alpha_amount,
            alpha_remaining=alpha_amount,
            usd_fmv=usd_value,
            usd_per_alpha=usd_per_alpha,
            tao_equivalent=tao_equivalent,
            extrinsic_id=delegation.get('extrinsic_id'),
            transfer_address=delegation.get('transfer_address'),
            notes=f"{source_type.value} income"
        )
        
        self.income_sheet.append_row(lot.to_sheet_row())
        print(f"  ✓ {source_type.value}: {alpha_amount:.4f} ALPHA (${usd_value:.2f}) at {lot.date}")
        
        return lot
    
    # -------------------------------------------------------------------------
    # Sales Processing (ALPHA → TAO)
    # -------------------------------------------------------------------------
    
    def process_sales(self, days_back: int = 7) -> List[AlphaSale]:
        """
        Process ALPHA → TAO conversions (UNDELEGATE events).
        
        Returns:
            List of newly created sales
        """
        print(f"\n{'='*60}")
        print("Processing ALPHA → TAO Sales")
        print(f"{'='*60}")
        
        end_time = int(time.time())
        start_time = max(
            self.last_sale_timestamp + 1,
            end_time - (days_back * 86400)
        )
        
        delegations = self.wallet_client.get_delegations(
            netuid=self.subnet_id,
            delegate=self.validator_address,
            nominator=self.wallet_address,
            start_time=start_time,
            end_time=end_time
        )
        
        
        new_sales = []
        for d in delegations:
            # Filter: UNDELEGATE events (user-initiated unstakes)
            # is_transfer=null indicates user-initiated (not a transfer to another address)
            if (d['action'] == 'UNDELEGATE' and 
                d.get('is_transfer') is None and
                d['timestamp'] > self.last_sale_timestamp):
                
                sale = self._process_undelegate(d)
                if sale:
                    new_sales.append(sale)
        
        if new_sales:
            max_ts = max(sale.timestamp for sale in new_sales)
            self.last_sale_timestamp = max_ts
            print(f"\n✓ Processed {len(new_sales)} ALPHA sales")
        else:
            print("ℹ️  No new UNDELEGATE events found")
        
        return new_sales
    
    def _process_undelegate(self, delegation: Dict[str, Any]) -> Optional[AlphaSale]:
        """Process an UNDELEGATE event into a sale."""
        alpha_disposed = delegation['alpha']
        tao_received = delegation['tao_amount']
        
        # Get TAO price at disposal time
        tao_price = self.get_tao_price(delegation['timestamp'])
        if not tao_price:
            print(f"  Warning: Could not get TAO price for UNDELEGATE at {delegation['timestamp']}")
            return None
        
        usd_proceeds = tao_received * tao_price
        
        # Consume ALPHA lots FIFO (collect updates for batch write)
        try:
            consumed_lots, cost_basis, gain_type, lot_updates = self.consume_alpha_lots_fifo(alpha_disposed)
        except InsufficientLotsError as e:
            print(f"  Error: {e}")
            return None
        
        realized_gain_loss = usd_proceeds - cost_basis
        
        # Create TAO lot
        tao_lot_id = self._next_tao_lot_id()
        sale_id = self._next_sale_id()
        
        tao_lot = TaoLot(
            lot_id=tao_lot_id,
            timestamp=delegation['timestamp'],
            block_number=delegation['block_number'],
            tao_quantity=tao_received,
            tao_remaining=tao_received,
            usd_basis=usd_proceeds,  # Basis is FMV at time of receipt
            usd_per_tao=tao_price,
            source_sale_id=sale_id,
            extrinsic_id=delegation.get('extrinsic_id')
        )
        # Create sale record
        sale = AlphaSale(
            sale_id=sale_id,
            timestamp=delegation['timestamp'],
            block_number=delegation['block_number'],
            alpha_disposed=alpha_disposed,
            tao_received=tao_received,
            tao_price_usd=tao_price,
            usd_proceeds=usd_proceeds,
            cost_basis=cost_basis,
            realized_gain_loss=realized_gain_loss,
            gain_type=gain_type,
            consumed_lots=consumed_lots,
            created_tao_lot_id=tao_lot_id,
            extrinsic_id=delegation.get('extrinsic_id')
        )
        
        # Apply updates in batches to avoid write limits
        self._batch_update_alpha_lots(lot_updates)
        self._append_with_retry(self.tao_lots_sheet, tao_lot.to_sheet_row(), label="TAO Lots")
        self._append_with_retry(self.sales_sheet, sale.to_sheet_row(), label="Sales")
        # Sort sheets by timestamp (column 3)
        self._sort_sheet_by_timestamp(self.income_sheet, timestamp_col=3, label="Income", range_str="A2:O")
        self._sort_sheet_by_timestamp(self.sales_sheet, timestamp_col=3, label="Sales", range_str="A2:O")
        self._sort_sheet_by_timestamp(self.tao_lots_sheet, timestamp_col=3, label="TAO Lots", range_str="A2:L")
        
        gain_str = "gain" if realized_gain_loss >= 0 else "loss"
        print(f"  ✓ Sale {sale_id}: {alpha_disposed:.4f} ALPHA → {tao_received:.4f} TAO")
        print(f"    Proceeds: ${usd_proceeds:.2f}, Basis: ${cost_basis:.2f}, {gain_type.value} {gain_str}: ${abs(realized_gain_loss):.2f}")
        
        return sale

    # -------------------------------------------------------------------------
    # Transfer Processing (TAO → Kraken)
    # -------------------------------------------------------------------------
    
    def process_transfers(self, days_back: int = 7) -> List[TaoTransfer]:
        """
        Process TAO → Kraken transfers.
        
        Returns:
            List of newly created transfers
        """
        print(f"\n{'='*60}")
        print("Processing TAO → Kraken Transfers")
        print(f"{'='*60}")
        
        end_time = int(time.time())
        start_time = max(
            self.last_transfer_timestamp + 1,
            end_time - (days_back * 86400)
        )
        
        # Fetch all outgoing transfers from the wallet in the window so we can
        # detect fee transfers that accompany a brokerage transfer (same extrinsic).
        all_transfers = self._timed_call(
            "transfers",
            self.wallet_client.get_transfers,
            account_address=self.wallet_address,
            start_time=start_time,
            end_time=end_time,
            sender=self.wallet_address,
            receiver=None
        )

        # Enforce window locally (some providers ignore filters)
        all_transfers = [t for t in all_transfers if start_time <= t.get('timestamp', 0) <= end_time]

        # Group transfers by extrinsic_id when available, otherwise by (block_number, timestamp)
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for t in all_transfers:
            key = t.get('extrinsic_id') or t.get('transaction_hash') or (t.get('block_number'), t.get('timestamp'))
            groups.setdefault(key, []).append(t)

        new_transfers = []
        for key, group in groups.items():
            # Find if this group contains a transfer to the brokerage address
            brokerage_amount = sum(g['amount'] for g in group if g.get('to') == self.brokerage_address or g.get('to') == self.brokerage_address)
            if brokerage_amount <= 0:
                # Not a brokerage transfer group; skip
                continue

            # Total outflow from the wallet in this group (brokerage + fees + other outs)
            total_outflow = sum(g['amount'] for g in group if g.get('from') == self.wallet_address or g.get('from') == self.wallet_address)

            # Prefer timestamp from brokerage transfer record
            primary = next((g for g in group if g.get('to') == self.brokerage_address), group[0])
            if primary['timestamp'] > self.last_transfer_timestamp:
                transfer = self._process_tao_transfer(primary, brokerage_amount=brokerage_amount, total_outflow=total_outflow, related_transfers=group)
                if transfer:
                    new_transfers.append(transfer)
        
        if new_transfers:
            max_ts = max(xfer.timestamp for xfer in new_transfers)
            self.last_transfer_timestamp = max_ts
            print(f"\n✓ Processed {len(new_transfers)} TAO transfers to Kraken")
        else:
            print("ℹ️  No new TAO transfers to Kraken found")
        
        return new_transfers
    
    def _process_tao_transfer(
        self,
        transfer: Dict[str, Any],
        brokerage_amount: Optional[float] = None,
        total_outflow: Optional[float] = None,
        related_transfers: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[TaoTransfer]:
        """Process a TAO transfer to Kraken.

        The Taostats data often shows the brokerage deposit and a separate fee transfer
        as separate outgoing transfers sharing the same extrinsic. To ensure TAO lots
        reflect the total TAO leaving the wallet, we consume lots equal to the
        total_outflow (brokerage + fees) and allocate cost basis proportionally.
        """
        # Determine amounts
        brokerage_amount = brokerage_amount if brokerage_amount is not None else transfer.get('amount')
        total_outflow = total_outflow if total_outflow is not None else brokerage_amount
        related_transfers = related_transfers or [transfer]

        # Get TAO price at transfer time (used for proceeds calculation only for brokerage amount)
        tao_price = self.get_tao_price(transfer['timestamp'])
        if not tao_price:
            print(f"  Warning: Could not get TAO price for transfer at {transfer['timestamp']}")
            return None

        usd_proceeds = brokerage_amount * tao_price

        # Consume TAO lots for the full outflow (brokerage + fees)
        try:
            consumed_lots, total_cost_basis, gain_type, lot_updates = self.consume_tao_lots_fifo(total_outflow)
        except InsufficientLotsError as e:
            print(f"  Error: {e}")
            return None

        # Allocate cost basis proportionally to the brokerage amount vs total outflow
        cost_basis_for_brokerage = (total_cost_basis * (brokerage_amount / total_outflow)) if total_outflow else 0.0
        realized_gain_loss = usd_proceeds - cost_basis_for_brokerage

        # Prepare a note summarizing related fee transfers
        fee_parts = []
        for g in related_transfers:
            to_addr = g.get('to')
            amt = g.get('amount')
            if to_addr != self.brokerage_address:
                fee_parts.append(f"{to_addr}:{amt:.4f}")
        notes = ""
        if fee_parts:
            notes = f"Related outflows: {', '.join(fee_parts)}"

        xfer = TaoTransfer(
            transfer_id=self._next_transfer_id(),
            timestamp=transfer['timestamp'],
            block_number=transfer.get('block_number'),
            tao_amount=brokerage_amount,
            tao_price_usd=tao_price,
            usd_proceeds=usd_proceeds,
            cost_basis=cost_basis_for_brokerage,
            realized_gain_loss=realized_gain_loss,
            gain_type=gain_type,
            consumed_tao_lots=consumed_lots,
            transaction_hash=transfer.get('transaction_hash'),
            extrinsic_id=transfer.get('extrinsic_id'),
            notes=notes
        )

        # Apply TAO lot updates (they reflect the total outflow consumption)
        self._batch_update_tao_lots(lot_updates)
        self._append_with_retry(self.transfers_sheet, xfer.to_sheet_row(), label="Transfers")
        self._sort_sheet_by_timestamp(self.tao_lots_sheet, timestamp_col=3, label="TAO Lots", range_str="A2:L")
        self._sort_sheet_by_timestamp(self.transfers_sheet, timestamp_col=3, label="Transfers", range_str="A2:L")

        gain_str = "gain" if realized_gain_loss >= 0 else "loss"
        print(f"  ✓ Transfer {xfer.transfer_id}: {brokerage_amount:.4f} TAO → Kraken (total outflow {total_outflow:.4f})")
        print(f"    Proceeds: ${usd_proceeds:.2f}, Basis: ${cost_basis_for_brokerage:.2f}, {gain_type.value} {gain_str}: ${abs(realized_gain_loss):.2f}")

        return xfer

    # -------------------------------------------------------------------------
    # Journal Entry Generation
    # -------------------------------------------------------------------------
    
    def generate_monthly_journal_entries(self, year_month: str = None) -> List[JournalEntry]:
        """
        Generate Wave journal entries for a specific month.
        
        Args:
            year_month: Month in YYYY-MM format (defaults to last month)
            
        Returns:
            List of journal entries
        """
        if not year_month:
            last_month = datetime.now().replace(day=1) - timedelta(days=1)
            year_month = last_month.strftime('%Y-%m')
        
        print(f"\n{'='*60}")
        print(f"Generating Journal Entries for {year_month}")
        print(f"{'='*60}")
        
        # Parse month boundaries
        year, month = map(int, year_month.split('-'))
        start_ts = int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp())
        if month == 12:
            end_ts = int(datetime(year + 1, 1, 1, tzinfo=timezone.utc).timestamp())
        else:
            end_ts = int(datetime(year, month + 1, 1, tzinfo=timezone.utc).timestamp())
        
        entries = []
        
        # Process Income
        income_records = self.income_sheet.get_all_records()
        contract_income = sum(
            r['USD FMV'] for r in income_records 
            if start_ts <= r['Timestamp'] < end_ts and r['Source Type'] == 'Contract'
        )
        staking_income = sum(
            r['USD FMV'] for r in income_records 
            if start_ts <= r['Timestamp'] < end_ts and r['Source Type'] == 'Staking'
        )
        
        if contract_income > 0:
            # Debit Asset, Credit Income
            entries.append(JournalEntry(
                month=year_month,
                entry_type="Income",
                account=self.wave_config.alpha_asset_account,
                debit=contract_income,
                credit=0,
                description=f"Contract ALPHA income for {year_month}"
            ))
            entries.append(JournalEntry(
                month=year_month,
                entry_type="Income",
                account=self.wave_config.contract_income_account,
                debit=0,
                credit=contract_income,
                description=f"Contract ALPHA income for {year_month}"
            ))
        
        if staking_income > 0:
            entries.append(JournalEntry(
                month=year_month,
                entry_type="Income",
                account=self.wave_config.alpha_asset_account,
                debit=staking_income,
                credit=0,
                description=f"Staking emissions income for {year_month}"
            ))
            entries.append(JournalEntry(
                month=year_month,
                entry_type="Income",
                account=self.wave_config.staking_income_account,
                debit=0,
                credit=staking_income,
                description=f"Staking emissions income for {year_month}"
            ))
        
        # Process Sales (ALPHA → TAO)
        sales_records = self.sales_sheet.get_all_records()
        month_sales = [r for r in sales_records if start_ts <= r['Timestamp'] < end_ts]
        
        for sale in month_sales:
            gain_loss = sale['Realized Gain/Loss']
            gain_type = sale['Gain Type']
            
            # Debit TAO asset for proceeds
            entries.append(JournalEntry(
                month=year_month,
                entry_type="Sale",
                account=self.wave_config.tao_asset_account,
                debit=sale['USD Proceeds'],
                credit=0,
                description=f"TAO received from ALPHA sale {sale['Sale ID']}"
            ))
            
            # Credit ALPHA asset for cost basis
            entries.append(JournalEntry(
                month=year_month,
                entry_type="Sale",
                account=self.wave_config.alpha_asset_account,
                debit=0,
                credit=sale['Cost Basis'],
                description=f"ALPHA disposed in sale {sale['Sale ID']}"
            ))
            
            # Record gain/loss
            if gain_loss >= 0:
                gain_account = (self.wave_config.long_term_gain_account 
                               if gain_type == 'Long-term' 
                               else self.wave_config.short_term_gain_account)
                entries.append(JournalEntry(
                    month=year_month,
                    entry_type="Sale",
                    account=gain_account,
                    debit=0,
                    credit=gain_loss,
                    description=f"{gain_type} gain on sale {sale['Sale ID']}"
                ))
            else:
                loss_account = (self.wave_config.long_term_loss_account 
                               if gain_type == 'Long-term' 
                               else self.wave_config.short_term_loss_account)
                entries.append(JournalEntry(
                    month=year_month,
                    entry_type="Sale",
                    account=loss_account,
                    debit=abs(gain_loss),
                    credit=0,
                    description=f"{gain_type} loss on sale {sale['Sale ID']}"
                ))
        
        # Process Transfers (TAO → Kraken)
        transfer_records = self.transfers_sheet.get_all_records()
        month_transfers = [r for r in transfer_records if start_ts <= r['Timestamp'] < end_ts]
        
        for xfer in month_transfers:
            gain_loss = xfer['Realized Gain/Loss']
            gain_type = xfer['Gain Type']
            
            # For transfer to Kraken, we're disposing TAO
            # The basis leaves our books, proceeds are the new Kraken basis (handled by Koinly)
            
            # Credit TAO asset for cost basis (leaving our books)
            entries.append(JournalEntry(
                month=year_month,
                entry_type="Transfer",
                account=self.wave_config.tao_asset_account,
                debit=0,
                credit=xfer['Cost Basis'],
                description=f"TAO transferred to Kraken {xfer['Transfer ID']}"
            ))
            
            # Record gain/loss from price movement between receiving TAO and sending to Kraken
            if gain_loss >= 0:
                gain_account = (self.wave_config.long_term_gain_account 
                               if gain_type == 'Long-term' 
                               else self.wave_config.short_term_gain_account)
                entries.append(JournalEntry(
                    month=year_month,
                    entry_type="Transfer",
                    account=gain_account,
                    debit=0,
                    credit=gain_loss,
                    description=f"{gain_type} gain on transfer {xfer['Transfer ID']}"
                ))
                # Balancing entry
                entries.append(JournalEntry(
                    month=year_month,
                    entry_type="Transfer",
                    account=self.wave_config.tao_asset_account,
                    debit=gain_loss,
                    credit=0,
                    description=f"Proceeds adjustment for transfer {xfer['Transfer ID']}"
                ))
            elif gain_loss < 0:
                loss_account = (self.wave_config.long_term_loss_account 
                               if gain_type == 'Long-term' 
                               else self.wave_config.short_term_loss_account)
                entries.append(JournalEntry(
                    month=year_month,
                    entry_type="Transfer",
                    account=loss_account,
                    debit=abs(gain_loss),
                    credit=0,
                    description=f"{gain_type} loss on transfer {xfer['Transfer ID']}"
                ))
                # Balancing entry - no additional debit needed as loss reduces TAO value
        
        # Write to sheet
        for entry in entries:
            self.journal_sheet.append_row(entry.to_sheet_row())
        
        print(f"✓ Generated {len(entries)} journal entries for {year_month}")
        
        # Summary
        total_income = contract_income + staking_income
        total_sales_proceeds = sum(s['USD Proceeds'] for s in month_sales)
        total_sales_gain = sum(s['Realized Gain/Loss'] for s in month_sales)
        total_transfer_gain = sum(t['Realized Gain/Loss'] for t in month_transfers)
        
        print(f"\nSummary for {year_month}:")
        print(f"  Contract Income: ${contract_income:.2f}")
        print(f"  Staking Income: ${staking_income:.2f}")
        print(f"  Sales Proceeds: ${total_sales_proceeds:.2f}")
        print(f"  Sales Gain/Loss: ${total_sales_gain:.2f}")
        print(f"  Transfer Gain/Loss: ${total_transfer_gain:.2f}")
        
        return entries

    # -------------------------------------------------------------------------
    # Main Entry Points
    # -------------------------------------------------------------------------
    
    def run_daily_check(self, days_back: int = 7):
        """Run daily check for all transaction types."""
        print(f"\n{'='*60}")
        print(f"Daily Check: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        # Process income
        contract_lots = self.process_contract_income(days_back)
        staking_lots = self.process_staking_emissions(days_back)
        
        # Process sales
        sales = self.process_sales(days_back)
        
        # Process transfers
        transfers = self.process_transfers(days_back)
        
        print(f"\n{'='*60}")
        print("Summary")
        print(f"{'='*60}")
        print(f"  Contract Income Lots: {len(contract_lots)}")
        print(f"  Staking Emission Lots: {len(staking_lots)}")
        print(f"  ALPHA Sales: {len(sales)}")
        print(f"  TAO Transfers: {len(transfers)}")
    
    def run_monthly_summary(self, year_month: str = None):
        """Generate monthly Wave journal entries."""
        self.generate_monthly_journal_entries(year_month)
