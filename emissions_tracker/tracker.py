try:
    import gspread
except ImportError:  # pragma: no cover - optional for unit tests
    gspread = None

try:
    from oauth2client.service_account import ServiceAccountCredentials
except ImportError:  # pragma: no cover - optional for unit tests
    ServiceAccountCredentials = None
from datetime import datetime, timedelta, timezone
import time
import backoff
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict
from collections import defaultdict

from emissions_tracker.clients.price import PriceClient
from emissions_tracker.clients.wallet import WalletClientInterface
from emissions_tracker.config import TrackerSettings, WaveAccountSettings
from emissions_tracker.exceptions import PriceNotAvailableError, InsufficientLotsError
from emissions_tracker.models import (
    AlphaLot, TaoLot, AlphaSale, TaoTransfer, Expense, JournalEntry,
    LotConsumption, SourceType, LotStatus, GainType
)

SECONDS_PER_DAY = 86400
RAO_PER_TAO = 10 ** 9


def _rao_to_tao(value: Any) -> float:
    """Convert rao-denominated values to TAO, tolerating None/strings."""
    if value in (None, ""):
        return 0.0
    try:
        return float(value) / RAO_PER_TAO
    except (TypeError, ValueError):
        return 0.0


def _is_rate_limit_error(e: Exception) -> bool:
    """Check if an exception is a Google Sheets rate limit error."""
    error_str = str(e)
    error_type = type(e).__name__
    return '429' in error_str or 'Quota exceeded' in error_str or 'APIError' in error_type


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
    EXPENSES_SHEET = "Expenses"
    TRANSFERS_SHEET = "Transfers"
    JOURNAL_SHEET = "Journal Entries"
    TAO_LOTS_SHEET = "TAO Lots"  # Internal tracking sheet
    
    def __init__(
        self, 
        price_client: PriceClient, 
        wallet_client: WalletClientInterface,
        tracking_hotkey: str,
        coldkey: str,
        sheet_id: str,
        label: str = "Tracker",
        smart_contract_address: Optional[str] = None,
        income_source: SourceType = SourceType.STAKING
    ):
        """
        Initialize emissions tracker.
        
        Args:
            price_client: Client for fetching TAO prices
            wallet_client: Client for fetching wallet/blockchain data
            tracking_hotkey: Hotkey address to track (validator or miner)
            coldkey: Coldkey address (nominator)
            sheet_id: Google Sheet ID for this tracker
            label: Label for logging (e.g., "Smart Contract", "Mining")
            smart_contract_address: Optional smart contract address for filtering contract income
            income_source: Default income source type (STAKING for validator, MINING for miner)
        """
        if gspread is None or ServiceAccountCredentials is None:
            raise ImportError(
                "gspread and oauth2client must be installed to instantiate BittensorEmissionTracker"
            )
        self.config = TrackerSettings()
        self.wave_config = WaveAccountSettings()
        self.price_client = price_client
        self.wallet_client = wallet_client
        
        # Tracker-specific configuration
        self.label = label
        self.tracking_hotkey = tracking_hotkey
        self.coldkey = coldkey
        self.sheet_id = sheet_id
        self.smart_contract_address = smart_contract_address
        self.income_source = income_source
        
        # Wallet addresses (from config)
        self.wallet_address = self.coldkey  # Use coldkey as wallet address
        self.brokerage_address = self.config.brokerage_ss58
        self.subnet_id = self.config.subnet_id
        
        print(f"Initializing {self.label} tracker:")
        print(f"  Tracking Hotkey: {self.tracking_hotkey}")
        print(f"  Coldkey: {self.coldkey}")
        print(f"  Brokerage: {self.brokerage_address}")
        if self.smart_contract_address:
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
        self.sheet = self._open_sheet_with_retry(self.sheet_id)
        
        # Initialize sheets
        self._init_sheets()
        
        # Load state
        self._load_state()

        # If derived sheets were cleared, reopen income lots so they can be reprocessed
        self._reset_income_lots_if_sales_empty()
        
        # Counters for ID generation
        self._load_counters()

    def _init_sheets(self):
        """Initialize all tracking sheets with headers."""
        sheet_configs = [
            (self.INCOME_SHEET, AlphaLot.sheet_headers()),
            (self.SALES_SHEET, AlphaSale.sheet_headers()),
            (self.EXPENSES_SHEET, Expense.sheet_headers()),
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
            self._ensure_sheet_headers(worksheet, headers, sheet_name)
        
        # Store worksheet references
        self.income_sheet = self.sheet.worksheet(self.INCOME_SHEET)
        self.sales_sheet = self.sheet.worksheet(self.SALES_SHEET)
        self.expenses_sheet = self.sheet.worksheet(self.EXPENSES_SHEET)
        self.tao_lots_sheet = self.sheet.worksheet(self.TAO_LOTS_SHEET)
        self.transfers_sheet = self.sheet.worksheet(self.TRANSFERS_SHEET)
        self.journal_sheet = self.sheet.worksheet(self.JOURNAL_SHEET)

    def _ensure_sheet_headers(self, worksheet, expected_headers, label: str):
        """Ensure worksheet header row matches expected schema."""
        try:
            current_headers = worksheet.row_values(1)
            needs_update = (
                len(current_headers) < len(expected_headers)
                or current_headers[:len(expected_headers)] != expected_headers
            )
            if needs_update:
                worksheet.update('A1', [expected_headers])
                print(f"  ✓ Updated {label} headers to latest schema")
        except Exception as e:
            print(f"  Warning: Could not verify headers for {label}: {e}")

    def _load_state(self):
        """Load last processed timestamps from sheets."""
        self.last_income_timestamp = 0  # Overall income watermark (legacy)
        self.last_contract_income_timestamp = 0
        self.last_staking_income_timestamp = 0
        self.last_sale_timestamp = 0
        self.last_expense_timestamp = 0
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
            records = self.expenses_sheet.get_all_records()
            if records:
                self.last_expense_timestamp = max(r['Timestamp'] for r in records)
                print(f"  Last expense timestamp: {self.last_expense_timestamp}")
        except Exception as e:
            print(f"  Warning: Could not load expenses state: {e}")
        
        try:
            records = self.transfers_sheet.get_all_records()
            if records:
                self.last_transfer_timestamp = max(r['Timestamp'] for r in records)
                print(f"  Last transfer timestamp: {self.last_transfer_timestamp}")
        except Exception as e:
            print(f"  Warning: Could not load transfers state: {e}")

    def _reset_income_lots_if_sales_empty(self):
        """Reset ALPHA lot remaining amounts/status if sales sheet is empty."""
        try:
            sales_records = self.sales_sheet.get_all_records()
        except Exception as e:
            print(f"  Warning: Could not inspect sales sheet for reset: {e}")
            return

        if sales_records:
            return  # Nothing to do when sales exist

        try:
            records = self._get_alpha_records_with_rows()
        except Exception as e:
            print(f"  Warning: Could not inspect income sheet for reset: {e}")
            return

        updates = []
        for record in records:
            qty = record.get('Alpha Quantity') or 0.0
            remaining = record.get('Alpha Remaining') or 0.0
            status = record.get('Status') or ''
            if abs(remaining - qty) > 1e-9 or status != LotStatus.OPEN.value:
                row = record['_row_num']
                updates.append({
                    "range": f"{self.INCOME_SHEET}!I{row}:I{row}",
                    "values": [[qty]]
                })
                updates.append({
                    "range": f"{self.INCOME_SHEET}!N{row}:N{row}",
                    "values": [[LotStatus.OPEN.value]]
                })

        if not updates:
            return

        body = {
            "valueInputOption": "RAW",
            "data": updates,
        }
        try:
            self.sheet.values_batch_update(body)
            print("  ✓ Reset income lots to Open because sales sheet is empty")
        except Exception as e:
            print(f"  Warning: Could not reset income lots: {e}")

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
            records = self.expenses_sheet.get_all_records()
            if records:
                max_id = max(int(r['Expense ID'].split('-')[1]) for r in records if r['Expense ID'])
                self.expense_counter = max_id + 1
            else:
                self.expense_counter = 1
        except:
            self.expense_counter = 1
        
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
        
        print(f"  Counters: ALPHA={self.alpha_lot_counter}, SALE={self.sale_counter}, EXPENSE={self.expense_counter}, TAO={self.tao_lot_counter}, XFER={self.transfer_counter}")

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

    @staticmethod
    def _resolve_time_window(
        label: str,
        last_timestamp: int,
        lookback_days: Optional[int],
        now: Optional[int] = None,
    ) -> Tuple[int, int]:
        """Determine the (start_time, end_time) timestamps for a processing window."""

        end_time = now if now is not None else int(time.time())

        if lookback_days is not None:
            if lookback_days <= 0:
                raise ValueError(f"lookback for {label} must be positive, got {lookback_days}")
            start_time = end_time - (lookback_days * SECONDS_PER_DAY)
            return start_time, end_time

        if last_timestamp > 0:
            return last_timestamp + 1, end_time

        raise ValueError(
            f"No previous {label} timestamp found; please rerun with --lookback <days> to seed the tracker."
        )

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
    
    def _consume_alpha_lots_from_cache(
        self, 
        alpha_needed: float, 
        lots_cache: List[Dict[str, Any]]
    ) -> Tuple[List[LotConsumption], float, GainType, List[Dict[str, Any]]]:
        """
        Consume ALPHA lots from an in-memory cache (for batch processing).
        
        Updates the cache in-place to reflect consumption, avoiding reads from sheets.
        
        Args:
            alpha_needed: Amount of ALPHA to consume
            lots_cache: In-memory list of lot dicts (will be modified)
            
        Returns:
            Tuple of (consumed lots, total cost basis, gain type, lot updates for sheet)
        """
        # Filter open lots with remaining balance
        open_lots = [
            lot for lot in lots_cache
            if lot['Status'] in ('Open', 'Partial') and lot['Alpha Remaining'] > 0
        ]
        
        # Sort by strategy (FIFO by default)
        strategy = getattr(self.config, 'lot_strategy', 'FIFO')
        if isinstance(strategy, str) and strategy.upper() == 'HIFO':
            def _unit_price(l):
                try:
                    qty = l.get('Alpha Quantity') or 0
                    fmv = l.get('USD FMV') or 0
                    return (fmv / qty) if qty else 0
                except Exception:
                    return 0
            open_lots.sort(key=lambda x: (-_unit_price(x), x['Timestamp']))
        else:
            open_lots.sort(key=lambda x: x['Timestamp'])
        
        total_available = sum(lot['Alpha Remaining'] for lot in open_lots)
        if total_available < alpha_needed:
            raise InsufficientLotsError(
                f"Need {alpha_needed:.4f} ALPHA but only {total_available:.4f} available"
            )
        
        consumed = []
        lot_updates = []
        total_basis = 0.0
        remaining_need = alpha_needed
        all_long_term = True
        now_ts = int(time.time())
        
        for lot in open_lots:
            if remaining_need <= 0:
                break
            
            lot_id = lot['Lot ID']
            available = lot['Alpha Remaining']
            original_qty = lot['Alpha Quantity']
            original_basis = lot['USD FMV']
            acquisition_ts = lot['Timestamp']
            
            holding_days = (now_ts - acquisition_ts) / SECONDS_PER_DAY
            if holding_days < 365:
                all_long_term = False
            
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
            
            # Update cache in-place
            new_remaining = available - to_consume
            new_status = LotStatus.CLOSED.value if new_remaining == 0 else LotStatus.PARTIAL.value
            lot['Alpha Remaining'] = new_remaining
            lot['Status'] = new_status
            
            # Track update for sheet write
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
        
        self._batch_update_with_backoff(body)
    
    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=5,
        max_time=180,
        base=10,
        factor=5,
        giveup=lambda e: not _is_rate_limit_error(e),
        on_backoff=lambda details: print(f"  Warning: batch alpha update failed (attempt {details['tries']}), retrying in {details['wait']:.1f}s...")
    )
    def _batch_update_with_backoff(self, body: Dict[str, Any]):
        """Batch update with exponential backoff for rate limits."""
        self.sheet.values_batch_update(body)
    
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
    
    def _get_tao_lots_with_rows(self) -> List[Dict[str, Any]]:
        """Return TAO lots sheet records with sheet row numbers included."""
        records = self.tao_lots_sheet.get_all_records()
        for idx, record in enumerate(records, start=2):
            record['_row_num'] = idx
        return records
    
    def _consume_tao_lots_from_cache(
        self,
        tao_needed: float,
        lots_cache: List[Dict[str, Any]],
        as_of_timestamp: Optional[int] = None
    ) -> Tuple[List[LotConsumption], float, GainType, List[Dict[str, Any]]]:
        """
        Consume TAO lots from an in-memory cache (for batch processing).
        
        Updates the cache in-place to reflect consumption, avoiding reads from sheets.
        
        Args:
            tao_needed: Amount of TAO to consume
            lots_cache: In-memory list of TAO lot dicts (will be modified)
            as_of_timestamp: Only consider lots created at or before this timestamp (prevents consuming future lots)
            
        Returns:
            Tuple of (consumed lots, total cost basis, gain type, lot updates for sheet)
        """
        # Filter open lots with remaining balance (and created before/at the as_of_timestamp if provided)
        open_lots = [
            lot for lot in lots_cache
            if lot['Status'] in ('Open', 'Partial') and lot['TAO Remaining'] > 0
            and (as_of_timestamp is None or lot['Timestamp'] <= as_of_timestamp)
        ]
        
        # Sort by strategy (FIFO by default)
        strategy = getattr(self.config, 'lot_strategy', 'FIFO')
        if isinstance(strategy, str) and strategy.upper() == 'HIFO':
            def _unit_price_tao(l):
                try:
                    qty = l.get('TAO Quantity') or 0
                    basis = l.get('USD Basis') or 0
                    return (basis / qty) if qty else 0
                except Exception:
                    return 0
            open_lots.sort(key=lambda x: (-_unit_price_tao(x), x['Timestamp']))
        else:
            open_lots.sort(key=lambda x: x['Timestamp'])
        
        total_available = sum(lot['TAO Remaining'] for lot in open_lots)
        if total_available < tao_needed:
            raise InsufficientLotsError(
                f"Need {tao_needed:.4f} TAO but only {total_available:.4f} available"
            )
        
        consumed = []
        lot_updates = []
        total_basis = 0.0
        remaining_need = tao_needed
        all_long_term = True
        now_ts = int(time.time())
        
        for lot in open_lots:
            if remaining_need <= 0:
                break
            
            lot_id = lot['TAO Lot ID']
            available = lot['TAO Remaining']
            original_qty = lot['TAO Quantity']
            original_basis = lot['USD Basis']
            acquisition_ts = lot['Timestamp']
            
            holding_days = (now_ts - acquisition_ts) / SECONDS_PER_DAY
            if holding_days < 365:
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
            
            # Update cache in-place
            new_remaining = available - to_consume
            new_status = LotStatus.CLOSED.value if new_remaining == 0 else LotStatus.PARTIAL.value
            lot['TAO Remaining'] = new_remaining
            lot['Status'] = new_status
            
            # Track update for sheet write
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
        
        self._batch_update_tao_lots_with_backoff(body)
    
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
        """Open a Google Sheet by key with exponential backoff for rate limiting."""
        return self.sheets_client.open_by_key(sheet_id)
    
    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=5,
        max_time=180,
        base=10,
        factor=5,
        giveup=lambda e: not _is_rate_limit_error(e),
        on_backoff=lambda details: print(f"  Warning: batch TAO lot update failed (attempt {details['tries']}), retrying in {details['wait']:.1f}s...")
    )
    def _batch_update_tao_lots_with_backoff(self, body: Dict[str, Any]):
        """Batch update TAO lots with exponential backoff for rate limits."""
        self.sheet.values_batch_update(body)
    
    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=5,
        max_time=180,
        base=10,
        factor=5,
        giveup=lambda e: not _is_rate_limit_error(e),
        on_backoff=lambda details: print(f"  Warning: append to {details['args'][2]} failed (attempt {details['tries']}), retrying in {details['wait']:.1f}s...")
    )
    def _append_with_retry(self, worksheet, row_values: List[Any], label: str):
        """Append a single row with exponential backoff for rate limiting."""
        worksheet.append_row(row_values)
    
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
        """Append multiple rows with exponential backoff to reduce API calls."""
        if rows:
            worksheet.append_rows(rows)

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
    
    def process_contract_income(self, lookback_days: Optional[int] = None) -> List[AlphaLot]:
        """
        Process contract income from DELEGATE events with smart contract transfer address.
        
        Returns:
            List of newly created ALPHA lots
        """
        print(f"\n{'='*60}")
        print("Processing Contract Income")
        print(f"{'='*60}")
        
        start_time, end_time = self._resolve_time_window(
            "contract income",
            self.last_contract_income_timestamp,
            lookback_days
        )
        
        print(f"Fetching delegations from {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}")

        delegations = self._timed_call(
            "delegations (contract)",
            self.wallet_client.get_delegations,
            netuid=self.subnet_id,
            delegate=self.tracking_hotkey,
            nominator=self.wallet_address,
            start_time=start_time,
            end_time=end_time,
            is_transfer=True
        )
        
        new_lots = []
        for d in delegations:
            # Filter: DELEGATE events from smart contract
            # is_transfer=True means ALPHA was transferred from another wallet (smart contract)
            # while remaining delegated to the validator
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
    
    def process_staking_emissions(self, lookback_days: Optional[int] = None) -> List[AlphaLot]:
        """
        Process emissions from balance increases (mining rewards or validator staking).
        
        For mining: Tracks balance increases on the miner's hotkey+coldkey.
        For validators: Tracks balance increases on the validator's hotkey+nominator coldkey.
        
        Emissions = Balance increase - DELEGATE inflows + UNDELEGATE outflows
        
        Returns:
            List of newly created ALPHA lots
        """
        print(f"\n{'='*60}")
        print("Processing Staking Emissions")
        print(f"{'='*60}")
        
        start_time, end_time = self._resolve_time_window(
            "staking emissions",
            self.last_staking_income_timestamp,
            lookback_days
        )
        
        # Get balance history
        balances = self._timed_call(
            "stake_balance_history",
            self.wallet_client.get_stake_balance_history,
            netuid=self.subnet_id,
            hotkey=self.tracking_hotkey,
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
            delegate=self.tracking_hotkey,
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
                    source_type=self.income_source,
                    alpha_quantity=emissions,
                    alpha_remaining=emissions,
                    usd_fmv=usd_fmv,
                    usd_per_alpha=usd_per_alpha,
                    tao_equivalent=tao_equivalent,
                    notes=f"{self.income_source.value} emissions (balance delta: {balance_change:.4f}, delegates: {delegate_inflow:.4f}, undelegates: {undelegate_outflow:.4f})"
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
                self._append_rows_with_retry(self.income_sheet, pending_rows)
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
        alpha_amount = delegation['alpha']  # in RAO
        usd_value = delegation['usd']
        # Calculate USD per RAO (not per full ALPHA token)
        # If API provides alpha_price_in_usd, it's per full ALPHA, so divide by 1e9 to get per RAO
        alpha_price_usd = delegation.get('alpha_price_in_usd')
        if alpha_price_usd:
            usd_per_alpha_rao = alpha_price_usd / RAO_PER_TAO  # Convert from per-ALPHA to per-RAO
        else:
            usd_per_alpha_rao = usd_value / alpha_amount if alpha_amount > 0 else 0
        tao_equivalent = delegation['tao_amount']
        
        lot = AlphaLot(
            lot_id=self._next_alpha_lot_id(),
            timestamp=delegation['timestamp'],
            block_number=delegation['block_number'],
            source_type=source_type,
            alpha_quantity=alpha_amount,
            alpha_remaining=alpha_amount,
            usd_fmv=usd_value,
            usd_per_alpha=usd_per_alpha_rao,
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
    
    def process_sales(self, lookback_days: Optional[int] = None) -> List[AlphaSale]:
        """
        Process ALPHA → TAO conversions (UNDELEGATE events).
        
        Returns:
            List of newly created sales
        """
        print(f"\n{'='*60}")
        print("Processing ALPHA → TAO Sales")
        print(f"{'='*60}")
        start_time, end_time = self._resolve_time_window(
            "sales",
            self.last_sale_timestamp,
            lookback_days
        )
        
        delegations = self.wallet_client.get_delegations(
            netuid=self.subnet_id,
            delegate=self.tracking_hotkey,
            nominator=self.wallet_address,
            start_time=start_time,
            end_time=end_time
        )
        
        # Load ALPHA lots once into memory cache for batch processing
        # This prevents each sale from reading stale data from sheets
        alpha_lots_cache = self._get_alpha_records_with_rows()
        
        new_sales = []
        tao_lot_rows = []
        sale_rows = []
        alpha_lot_updates = []
        
        for d in delegations:
            # Filter: UNDELEGATE events (user-initiated unstakes)
            # is_transfer=null indicates user-initiated (not a transfer to another address)
            if (d['action'] == 'UNDELEGATE' and 
                d.get('is_transfer') is None and
                d['timestamp'] > self.last_sale_timestamp):
                
                sale, tao_lot, lot_updates = self._process_undelegate(d, alpha_lots_cache=alpha_lots_cache)
                if sale:
                    new_sales.append(sale)
                    tao_lot_rows.append(tao_lot.to_sheet_row())
                    sale_rows.append(sale.to_sheet_row())
                    alpha_lot_updates.extend(lot_updates)
        
        # Batch write all updates at once
        if new_sales:
            self._batch_update_alpha_lots(alpha_lot_updates)
            self._append_rows_with_retry(self.tao_lots_sheet, tao_lot_rows)
            self._append_rows_with_retry(self.sales_sheet, sale_rows)
            self._sort_sheet_by_timestamp(self.income_sheet, timestamp_col=3, label="Income", range_str="A2:O")
            self._sort_sheet_by_timestamp(self.sales_sheet, timestamp_col=3, label="Sales", range_str="A2:O")
            self._sort_sheet_by_timestamp(self.tao_lots_sheet, timestamp_col=3, label="TAO Lots", range_str="A2:L")
            
            max_ts = max(sale.timestamp for sale in new_sales)
            self.last_sale_timestamp = max_ts
            print(f"\n✓ Processed {len(new_sales)} ALPHA sales")
        else:
            print("ℹ️  No new UNDELEGATE events found")
        
        return new_sales
    
    def _process_undelegate(self, delegation: Dict[str, Any], alpha_lots_cache: Optional[List[Dict[str, Any]]] = None) -> Optional[Tuple[AlphaSale, TaoLot, List[Dict[str, Any]]]]:
        """Process an UNDELEGATE event into a sale.
        
        Returns:
            Tuple of (sale, tao_lot, lot_updates) or None if processing fails
        """
        alpha_disposed = delegation['alpha']
        tao_received = delegation['tao_amount']
        slippage_ratio = delegation.get('slippage') or 0.0
        alpha_price_in_tao = delegation.get('alpha_price_in_tao')
        tao_expected_from_price = None
        if alpha_price_in_tao:
            tao_expected_from_price = alpha_disposed * alpha_price_in_tao
        tao_expected_from_slippage = None
        if tao_received and slippage_ratio and abs(1 - slippage_ratio) > 1e-9:
            divisor = 1 - slippage_ratio
            if abs(divisor) > 1e-9:
                tao_expected_from_slippage = tao_received / divisor
        tao_expected = None
        for candidate in (tao_expected_from_slippage, tao_expected_from_price):
            if candidate is not None:
                tao_expected = candidate
                break
        if tao_expected is None:
            tao_expected = tao_received
        tao_slippage = (tao_expected - tao_received) if (tao_expected is not None and tao_received is not None) else 0.0
        
        # Determine USD proceeds / TAO price using Taostats data first
        usd_proceeds = delegation.get('usd') or 0.0
        tao_price = 0.0
        if usd_proceeds and tao_received:
            tao_price = usd_proceeds / tao_received
        else:
            tao_price = self.get_tao_price(delegation['timestamp'])
            if not tao_price:
                print(f"  Warning: Could not get TAO price for UNDELEGATE at {delegation['timestamp']}")
                return None
            usd_proceeds = tao_received * tao_price

        slippage_usd = tao_slippage * tao_price if tao_price and tao_slippage else 0.0

        # Fee is already in TAO (converted by the client), don't convert again
        fee_tao = delegation.get('fee', 0)
        fee_usd = fee_tao * tao_price if tao_price else 0.0
        
        # Consume ALPHA lots - use cache if provided (batch mode), otherwise read from sheets
        try:
            if alpha_lots_cache is not None:
                consumed_lots, cost_basis, gain_type, lot_updates = self._consume_alpha_lots_from_cache(alpha_disposed, alpha_lots_cache)
            else:
                consumed_lots, cost_basis, gain_type, lot_updates = self.consume_alpha_lots_fifo(alpha_disposed)
        except InsufficientLotsError as e:
            print(f"  Error: {e}")
            return None
        
        # Realized gain/loss calculation
        # Note: slippage is already reflected in usd_proceeds (TAO received, not expected)
        # So we don't subtract it again here - that would double-count the loss
        realized_gain_loss = usd_proceeds - cost_basis - fee_usd
        
        # Create TAO lot
        # The network fee is paid FROM the TAO received, so the actual TAO entering
        # the wallet (and tracked in the lot) is the net amount after fees
        tao_lot_id = self._next_tao_lot_id()
        sale_id = self._next_sale_id()
        tao_net = tao_received - fee_tao  # Net TAO after network fee
        usd_basis_net = usd_proceeds - fee_usd  # Net USD basis after fee
        
        tao_lot = TaoLot(
            lot_id=tao_lot_id,
            timestamp=delegation['timestamp'],
            block_number=delegation['block_number'],
            tao_quantity=tao_net,
            tao_remaining=tao_net,
            usd_basis=usd_basis_net,  # Basis is FMV at time of receipt, minus fee
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
            tao_expected=tao_expected or 0.0,
            tao_slippage=tao_slippage,
            slippage_usd=slippage_usd,
            slippage_ratio=slippage_ratio,
            network_fee_tao=fee_tao,
            network_fee_usd=fee_usd,
            consumed_lots=consumed_lots,
            created_tao_lot_id=tao_lot_id,
            extrinsic_id=delegation.get('extrinsic_id')
        )
        
        # Batch mode: defer writes, return objects for caller to batch
        if alpha_lots_cache is not None:
            gain_str = "gain" if realized_gain_loss >= 0 else "loss"
            print(f"  ✓ Sale {sale_id}: {alpha_disposed:.4f} ALPHA → {tao_received:.4f} TAO")
            if abs(tao_slippage) > 0.0000001:
                print(
                    f"    Slippage: {tao_slippage:+.6f} TAO (${slippage_usd:+.2f}) ({slippage_ratio:+.6%})"
                )
            print(f"    Proceeds: ${usd_proceeds:.2f}, Basis: ${cost_basis:.2f}, {gain_type.value} {gain_str}: ${abs(realized_gain_loss):.2f}")
            return (sale, tao_lot, lot_updates)
        
        # Immediate write mode (single sale processing)
        self._batch_update_alpha_lots(lot_updates)
        self._append_with_retry(self.tao_lots_sheet, tao_lot.to_sheet_row(), label="TAO Lots")
        self._append_with_retry(self.sales_sheet, sale.to_sheet_row(), label="Sales")
        # Sort sheets by timestamp (column 3)
        self._sort_sheet_by_timestamp(self.income_sheet, timestamp_col=3, label="Income", range_str="A2:O")
        self._sort_sheet_by_timestamp(self.sales_sheet, timestamp_col=3, label="Sales", range_str="A2:O")
        self._sort_sheet_by_timestamp(self.tao_lots_sheet, timestamp_col=3, label="TAO Lots", range_str="A2:L")
        
        gain_str = "gain" if realized_gain_loss >= 0 else "loss"
        print(f"  ✓ Sale {sale_id}: {alpha_disposed:.4f} ALPHA → {tao_received:.4f} TAO")
        if abs(tao_slippage) > 0.0000001:
            print(
                f"    Slippage: {tao_slippage:+.6f} TAO (${slippage_usd:+.2f}) ({slippage_ratio:+.6%})"
            )
        print(f"    Proceeds: ${usd_proceeds:.2f}, Basis: ${cost_basis:.2f}, {gain_type.value} {gain_str}: ${abs(realized_gain_loss):.2f}")
        
        return (sale, tao_lot, lot_updates)

    # -------------------------------------------------------------------------
    # Expense Processing (ALPHA → TAO payments to other entities)
    # -------------------------------------------------------------------------
    
    def process_expenses(self, lookback_days: Optional[int] = None) -> List[Expense]:
        """
        Process ALPHA → TAO payment/expense events (UNDELEGATE with is_transfer=True to non-smart-contract).
        
        These are automatically added to the Expenses sheet with an empty category field.
        The user must categorize them before they can be included in journal entries.
        
        Returns:
            List of newly created expenses
        """
        print(f"\n{'='*60}")
        print("Processing ALPHA → TAO Expenses")
        print(f"{'='*60}")
        start_time, end_time = self._resolve_time_window(
            "expenses",
            self.last_expense_timestamp,
            lookback_days
        )
        
        delegations = self.wallet_client.get_delegations(
            netuid=self.subnet_id,
            delegate=self.tracking_hotkey,
            nominator=self.wallet_address,
            start_time=start_time,
            end_time=end_time
        )
        
        # Load ALPHA lots once into memory cache for batch processing
        alpha_lots_cache = self._get_alpha_records_with_rows()
        
        new_expenses = []
        expense_rows = []
        alpha_lot_updates = []
        
        for d in delegations:
            # Filter: UNDELEGATE events with is_transfer=True to a non-smart-contract address
            # These are payments/transfers to other entities (not user-initiated sales)
            if (d['action'] == 'UNDELEGATE' and 
                d.get('is_transfer') == True and
                d.get('transfer_address') != self.smart_contract_address and
                d['timestamp'] > self.last_expense_timestamp):
                
                expense, lot_updates = self._process_expense_undelegate(d, alpha_lots_cache=alpha_lots_cache)
                if expense:
                    new_expenses.append(expense)
                    expense_rows.append(expense.to_sheet_row())
                    alpha_lot_updates.extend(lot_updates)
        
        # Batch write all updates at once
        if new_expenses:
            self._batch_update_alpha_lots(alpha_lot_updates)
            self._append_rows_with_retry(self.expenses_sheet, expense_rows)
            self._sort_sheet_by_timestamp(self.income_sheet, timestamp_col=3, label="Income", range_str="A2:O")
            self._sort_sheet_by_timestamp(self.expenses_sheet, timestamp_col=3, label="Expenses", range_str="A2:W")
            self._sort_sheet_by_timestamp(self.tao_lots_sheet, timestamp_col=3, label="TAO Lots", range_str="A2:L")
            
            max_ts = max(expense.timestamp for expense in new_expenses)
            self.last_expense_timestamp = max_ts
            print(f"\n✓ Processed {len(new_expenses)} ALPHA expenses")
            print(f"⚠️  Please categorize these expenses in the Expenses sheet before running journal entries")
        else:
            print("ℹ️  No new expense UNDELEGATE events found")
        
        return new_expenses
    
    def _process_expense_undelegate(self, delegation: Dict[str, Any], alpha_lots_cache: Optional[List[Dict[str, Any]]] = None) -> Optional[Tuple[Expense, List[Dict[str, Any]]]]:
        """Process an UNDELEGATE expense event (direct ALPHA transfer payment to another entity).
        
        These are direct ALPHA transfers with NO TAO involved.
        Proceeds are calculated based on ALPHA's FMV in USD at time of transfer.
        
        Returns:
            Tuple of (expense, lot_updates) or None if processing fails
        """
        alpha_disposed = delegation['alpha']
        
        # Extract transfer address
        transfer_address_data = delegation.get('transfer_address')
        if isinstance(transfer_address_data, dict):
            transfer_address = transfer_address_data.get('ss58', '')
        else:
            transfer_address = transfer_address_data or ''
        
        # Calculate USD proceeds based on ALPHA's FMV
        # Use the 'usd' field if available (from taostats), otherwise calculate from ALPHA price
        usd_proceeds = delegation.get('usd') or 0.0
        alpha_price_usd = 0.0
        
        if usd_proceeds and alpha_disposed:
            alpha_price_usd = usd_proceeds / alpha_disposed
        else:
            # Get ALPHA price in USD via TAO
            # ALPHA price = (ALPHA/TAO ratio) × (TAO/USD price)
            alpha_price_in_tao = delegation.get('alpha_price_in_tao')
            if alpha_price_in_tao:
                tao_price = self.get_tao_price(delegation['timestamp'])
                if not tao_price:
                    print(f"  Warning: Could not get TAO price for expense at {delegation['timestamp']}")
                    return None
                alpha_price_usd = alpha_price_in_tao * tao_price
                usd_proceeds = alpha_disposed * alpha_price_usd
            else:
                print(f"  Warning: Could not determine ALPHA price for expense at {delegation['timestamp']}")
                return None

        # Network fee (in ALPHA, already provided in USD if available)
        fee_usd = delegation.get('fee_usd', 0.0)
        if not fee_usd:
            fee_alpha = delegation.get('fee', 0.0)
            fee_usd = fee_alpha * alpha_price_usd if alpha_price_usd else 0.0
        
        # Consume ALPHA lots
        try:
            if alpha_lots_cache is not None:
                consumed_lots, cost_basis, gain_type, lot_updates = self._consume_alpha_lots_from_cache(alpha_disposed, alpha_lots_cache)
            else:
                consumed_lots, cost_basis, gain_type, lot_updates = self.consume_alpha_lots_fifo(alpha_disposed)
        except InsufficientLotsError as e:
            print(f"  Error: {e}")
            return None
        
        # Realized gain/loss calculation: FMV - cost basis - fees
        realized_gain_loss = usd_proceeds - cost_basis - fee_usd
        
        expense_id = self._next_expense_id()
        
        # Create expense record (category is empty - user must fill it in)
        # No TAO involved, so tao_received=0, tao_price_usd=0, created_tao_lot_id=""
        expense = Expense(
            expense_id=expense_id,
            timestamp=delegation['timestamp'],
            block_number=delegation['block_number'],
            transfer_address=transfer_address,
            category="",  # User must categorize
            alpha_disposed=alpha_disposed,
            tao_received=0.0,  # No TAO involved in direct ALPHA transfer
            tao_price_usd=0.0,
            usd_proceeds=usd_proceeds,  # FMV of ALPHA at time of transfer
            cost_basis=cost_basis,
            realized_gain_loss=realized_gain_loss,
            gain_type=gain_type,
            tao_expected=0.0,
            tao_slippage=0.0,
            slippage_usd=0.0,
            slippage_ratio=0.0,
            network_fee_tao=0.0,
            network_fee_usd=fee_usd,
            consumed_lots=consumed_lots,
            created_tao_lot_id="",  # No TAO lot created
            extrinsic_id=delegation.get('extrinsic_id'),
            notes=f"Direct ALPHA transfer (no TAO involved)"
        )
        
        gain_str = "gain" if realized_gain_loss >= 0 else "loss"
        print(f"  ✓ Expense {expense_id}: {alpha_disposed:.4f} ALPHA (${usd_proceeds:.2f}) to {transfer_address[:8]}...")
        print(f"    FMV: ${usd_proceeds:.2f}, Basis: ${cost_basis:.2f}, {gain_type.value} {gain_str}: ${abs(realized_gain_loss):.2f}")
        print(f"    ⚠️  Category required: Please update Category column in Expenses sheet")
        
        return (expense, lot_updates)

    # -------------------------------------------------------------------------
    # Transfer Processing (TAO → Kraken)
    # -------------------------------------------------------------------------
    
    def process_transfers(self, lookback_days: Optional[int] = None) -> List[TaoTransfer]:
        """
        Process TAO → Kraken transfers.
        
        Returns:
            List of newly created transfers
        """
        print(f"\n{'='*60}")
        print("Processing TAO → Kraken Transfers")
        print(f"{'='*60}")
        
        start_time, end_time = self._resolve_time_window(
            "transfers",
            self.last_transfer_timestamp,
            lookback_days
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

        # Load TAO lots once into memory cache for batch processing
        tao_lots_cache = self._get_tao_lots_with_rows()
        all_lot_updates = []
        transfer_rows = []
        
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
                transfer = self._process_tao_transfer(primary, brokerage_amount=brokerage_amount, total_outflow=total_outflow, related_transfers=group, tao_lots_cache=tao_lots_cache)
                if transfer:
                    new_transfers.append(transfer)
                    transfer_rows.append(transfer.to_sheet_row())
                    # Accumulate lot updates from this transfer
                    if hasattr(transfer, '_lot_updates'):
                        all_lot_updates.extend(transfer._lot_updates)
        
        # Batch write all updates once at the end
        if new_transfers:
            self._batch_update_tao_lots(all_lot_updates)
            self._append_rows_with_retry(self.transfers_sheet, transfer_rows)
            self._sort_sheet_by_timestamp(self.tao_lots_sheet, timestamp_col=3, label="TAO Lots", range_str="A2:L")
            self._sort_sheet_by_timestamp(self.transfers_sheet, timestamp_col=3, label="Transfers", range_str="A2:L")
            
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
        related_transfers: Optional[List[Dict[str, Any]]] = None,
        tao_lots_cache: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[TaoTransfer]:
        """Process a TAO transfer to Kraken.

        The Taostats data often shows the brokerage deposit and a separate fee transfer
        as separate outgoing transfers sharing the same extrinsic. To ensure TAO lots
        reflect the total TAO leaving the wallet, we consume lots equal to the
        total_outflow (brokerage + fees) and allocate cost basis proportionally.
        """
        # Determine amounts
        brokerage_amount = brokerage_amount if brokerage_amount is not None else transfer.get('amount')
        base_outflow = total_outflow if total_outflow is not None else brokerage_amount
        related_transfers = related_transfers or [transfer]

        # Fee is already in TAO (converted by the client), don't convert again
        fee_tao = sum(rt.get('fee', 0) for rt in related_transfers)
        total_outflow_tao = (base_outflow or 0.0) + fee_tao
        tao_to_consume = total_outflow_tao if total_outflow_tao > 0 else (base_outflow or 0.0)

        # Get TAO price at transfer time (used for proceeds calculation only for brokerage amount)
        tao_price = self.get_tao_price(transfer['timestamp'])
        if not tao_price:
            print(f"  Warning: Could not get TAO price for transfer at {transfer['timestamp']}")
            return None

        usd_proceeds = brokerage_amount * tao_price

        # Consume TAO lots for the full outflow (brokerage + fees) - use cache if provided
        # Pass transfer timestamp to prevent consuming lots created after this transfer
        try:
            if tao_lots_cache is not None:
                consumed_lots, total_cost_basis, gain_type, lot_updates = self._consume_tao_lots_from_cache(
                    tao_to_consume, tao_lots_cache, as_of_timestamp=transfer['timestamp']
                )
            else:
                consumed_lots, total_cost_basis, gain_type, lot_updates = self.consume_tao_lots_fifo(tao_to_consume)
        except InsufficientLotsError as e:
            print(f"  Error: {e}")
            return None

        # Allocate cost basis proportionally to the brokerage amount vs total outflow
        cost_basis_for_brokerage = (total_cost_basis * (brokerage_amount / total_outflow_tao)) if total_outflow_tao else 0.0
        fee_cost_basis = max(total_cost_basis - cost_basis_for_brokerage, 0.0)
        realized_gain_loss = usd_proceeds - cost_basis_for_brokerage

        # Prepare a note summarizing related fee transfers
        fee_parts = []
        for g in related_transfers:
            to_addr = g.get('to')
            amt = g.get('amount')
            if to_addr != self.brokerage_address:
                fee_parts.append(f"{to_addr}:{amt:.4f}")
        notes_segments = []
        if fee_parts:
            notes_segments.append(f"Related outflows: {', '.join(fee_parts)}")
        if fee_tao > 0:
            notes_segments.append(f"Network fee: {fee_tao:.6f} TAO")
        notes = " | ".join(notes_segments)

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
            notes=notes,
            total_outflow_tao=total_outflow_tao,
            fee_tao=fee_tao,
            fee_cost_basis_usd=fee_cost_basis
        )

        gain_str = "gain" if realized_gain_loss >= 0 else "loss"
        print(f"  ✓ Transfer {xfer.transfer_id}: {brokerage_amount:.4f} TAO → Kraken (total outflow {total_outflow_tao:.4f})")
        print(f"    Proceeds: ${usd_proceeds:.2f}, Basis: ${cost_basis_for_brokerage:.2f}, {gain_type.value} {gain_str}: ${abs(realized_gain_loss):.2f}")

        # Batch mode: store lot updates for caller to batch write
        if tao_lots_cache is not None:
            xfer._lot_updates = lot_updates
            return xfer
        
        # Immediate write mode (single transfer processing)
        self._batch_update_tao_lots(lot_updates)
        self._append_with_retry(self.transfers_sheet, xfer.to_sheet_row(), label="Transfers")
        self._sort_sheet_by_timestamp(self.tao_lots_sheet, timestamp_col=3, label="TAO Lots", range_str="A2:L")
        self._sort_sheet_by_timestamp(self.transfers_sheet, timestamp_col=3, label="Transfers", range_str="A2:L")

        return xfer

    # -------------------------------------------------------------------------
    # Journal Entry Generation
    # -------------------------------------------------------------------------

    def generate_monthly_journal_entries(self, year_month: Optional[str] = None) -> List[JournalEntry]:
        """Generate aggregated Wave journal entries for a given month."""
        if not year_month:
            last_month = datetime.now(timezone.utc).replace(day=1) - timedelta(days=1)
            year_month = last_month.strftime('%Y-%m')

        try:
            period_start = datetime.strptime(year_month, "%Y-%m").replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise ValueError("year_month must be in YYYY-MM format") from exc

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
        
        return self._generate_monthly_journal_entries_from_records(
            year_month, start_ts, end_ts,
            income_records, sales_records, expense_records, transfer_records
        )
    
    def _generate_monthly_journal_entries_from_records(
        self,
        year_month: str,
        start_ts: int,
        end_ts: int,
        income_records: List[Dict[str, Any]],
        sales_records: List[Dict[str, Any]],
        expense_records: List[Dict[str, Any]],
        transfer_records: List[Dict[str, Any]]
    ) -> List[JournalEntry]:
        """Internal method to generate journal entries from pre-loaded records."""
        # Check for uncategorized expenses before proceeding
        uncategorized_expenses = [
            exp for exp in expense_records
            if start_ts <= exp['Timestamp'] < end_ts and not exp.get('Category', '').strip()
        ]
        
        if uncategorized_expenses:
            print(f"\n❌ ERROR: Found {len(uncategorized_expenses)} uncategorized expense(s) in {year_month}")
            print("Please categorize all expenses in the Expenses sheet before generating journal entries.")
            print("\nUncategorized expenses:")
            for exp in uncategorized_expenses:
                exp_date = datetime.fromtimestamp(exp['Timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                exp_id = exp.get('Expense ID', 'unknown')
                transfer_addr = exp.get('Transfer Address', 'unknown')
                alpha = exp.get('Alpha Disposed', 0)
                print(f"  - {exp_id} ({exp_date}): {alpha:.4f} ALPHA to {transfer_addr[:8]}...")
            raise ValueError(
                f"Cannot generate journal entries for {year_month}: "
                f"{len(uncategorized_expenses)} uncategorized expense(s) found. "
                "Please update the Category column in the Expenses sheet."
            )

        entries, summary = _aggregate_monthly_journal_entries(
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

        print(f"✓ Generated {len(entries)} aggregated journal entries for {year_month}")
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

        return entries

    # -------------------------------------------------------------------------
    # Main Entry Points
    # -------------------------------------------------------------------------

    def run_daily_check(self, lookback_days: Optional[int] = None):
        """Run daily check for all transaction types."""
        print(f"\n{'='*60}")
        print(f"Daily Check: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")

        # Process contract income only if we have a smart contract address (validator mode)
        contract_lots = []
        if self.smart_contract_address:
            contract_lots = self.process_contract_income(lookback_days)
        
        # Process emissions (mining or validator staking rewards)
        staking_lots = self.process_staking_emissions(lookback_days)
        sales = self.process_sales(lookback_days)
        expenses = self.process_expenses(lookback_days)
        transfers = self.process_transfers(lookback_days)

        print(f"\n{'='*60}")
        print("Summary")
        print(f"{'='*60}")
        if contract_lots:
            print(f"  Contract Income Lots: {len(contract_lots)}")
        print(f"  {self.income_source.value} Emission Lots: {len(staking_lots)}")
        print(f"  ALPHA Sales: {len(sales)}")
        print(f"  ALPHA Expenses: {len(expenses)}")
        print(f"  TAO Transfers: {len(transfers)}")

    def run_monthly_summary(self, year_month: str = None):
        """Generate monthly Wave journal entries."""
        self.generate_monthly_journal_entries(year_month)

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
        
        # Calculate year boundaries
        year_start = datetime(year, 1, 1, tzinfo=timezone.utc)
        year_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        year_start_ts = int(year_start.timestamp())
        year_end_ts = int(year_end.timestamp())
        
        # Check for uncategorized expenses in the entire year BEFORE processing any months
        uncategorized_expenses = [
            exp for exp in expense_records
            if year_start_ts <= exp['Timestamp'] < year_end_ts and not exp.get('Category', '').strip()
        ]
        
        if uncategorized_expenses:
            print(f"\n❌ ERROR: Found {len(uncategorized_expenses)} uncategorized expense(s) in {year}")
            print("Please categorize all expenses in the Expenses sheet before generating journal entries.")
            print("\nUncategorized expenses:")
            for exp in uncategorized_expenses:
                exp_date = datetime.fromtimestamp(exp['Timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                exp_id = exp.get('Expense ID', 'unknown')
                transfer_addr = exp.get('Transfer Address', 'unknown')
                alpha = exp.get('Alpha Disposed', 0)
                print(f"  - {exp_id} ({exp_date}): {alpha:.4f} ALPHA to {transfer_addr[:8]}...")
            raise ValueError(
                f"Cannot generate journal entries for {year}: "
                f"{len(uncategorized_expenses)} uncategorized expense(s) found. "
                "Please update the Category column in the Expenses sheet."
            )
        
        all_entries = []
        all_rows = []  # Collect all rows for batch write
        
        for month in range(1, 13):
            year_month = f"{year}-{month:02d}"
            
            # Calculate timestamps for this month
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
                # Use aggregation function directly (we already checked expenses above)
                entries, summary = _aggregate_monthly_journal_entries(
                    year_month,
                    income_records,
                    sales_records,
                    expense_records,
                    transfer_records,
                    self.wave_config,
                    start_ts,
                    end_ts,
                )
                
                # Collect rows for batch write
                for entry in entries:
                    all_rows.append(entry.to_sheet_row())
                    all_entries.append(entry)
                
                # Print summary
                print(f"✓ Generated {len(entries)} aggregated journal entries for {year_month}")
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
                
            except ValueError as e:
                # Skip months with errors (e.g., no data)
                print(f"  Skipping {year_month}: {e}")
                continue
        
        # Batch write all journal entries at once
        if all_rows:
            print(f"\nWriting {len(all_rows)} journal entries to sheet...")
            self._append_rows_with_retry(self.journal_sheet, all_rows)
            print("✓ Journal entries written")
        
        print(f"\n✓ Generated {len(all_entries)} total journal entries for {year}")
        return all_entries

    def clear_income_sheets(self):
        """Clear all income and ALPHA lot data (for regeneration)."""
        print("  Clearing Income sheet...")
        try:
            # Get all values and clear everything except header
            all_values = self.income_sheet.get_all_values()
            if len(all_values) > 1:  # If there's more than just the header
                # Clear from row 2 onward
                last_row = len(all_values)
                self.income_sheet.batch_clear([f'A2:Z{last_row}'])
            print("  ✓ Income sheet cleared")
        except Exception as e:
            print(f"  Warning: Could not clear income sheet: {e}")
        
        # Reset state to match cleared sheet
        self.last_contract_income_timestamp = 0
        self.last_staking_income_timestamp = 0
        self.last_income_timestamp = 0
        self.alpha_lot_counter = 1

    def clear_sales_sheet(self):
        """Clear all sales and TAO lot data (for regeneration)."""
        print("  Clearing Sales sheet...")
        try:
            all_values = self.sales_sheet.get_all_values()
            if len(all_values) > 1:
                last_row = len(all_values)
                self.sales_sheet.batch_clear([f'A2:Z{last_row}'])
            print("  ✓ Sales sheet cleared")
        except Exception as e:
            print(f"  Warning: Could not clear sales sheet: {e}")
        
        print("  Clearing TAO Lots sheet...")
        try:
            all_values = self.tao_lots_sheet.get_all_values()
            if len(all_values) > 1:
                last_row = len(all_values)
                self.tao_lots_sheet.batch_clear([f'A2:Z{last_row}'])
            print("  ✓ TAO Lots sheet cleared")
        except Exception as e:
            print(f"  Warning: Could not clear TAO lots sheet: {e}")
        
        # Reset state to match cleared sheets
        self.last_sale_timestamp = 0
        self.sale_counter = 1
        self.tao_lot_counter = 1

    def clear_transfers_sheet(self):
        """Clear all transfer data (for regeneration)."""
        print("  Clearing Transfers sheet...")
        try:
            all_values = self.transfers_sheet.get_all_values()
            if len(all_values) > 1:
                last_row = len(all_values)
                self.transfers_sheet.batch_clear([f'A2:Z{last_row}'])
            print("  ✓ Transfers sheet cleared")
        except Exception as e:
            print(f"  Warning: Could not clear transfers sheet: {e}")
        
        # Reset state to match cleared sheet
        self.last_transfer_timestamp = 0
        self.transfer_counter = 1

    def clear_expenses_sheet(self):
        """Clear all expense data (for regeneration)."""
        print("  Clearing Expenses sheet...")
        try:
            all_values = self.expenses_sheet.get_all_values()
            if len(all_values) > 1:
                last_row = len(all_values)
                self.expenses_sheet.batch_clear([f'A2:Z{last_row}'])
            print("  ✓ Expenses sheet cleared")
        except Exception as e:
            print(f"  Warning: Could not clear expenses sheet: {e}")
        
        # Reset state to match cleared sheet
        self.last_expense_timestamp = 0
        self.expense_counter = 1

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


def _aggregate_monthly_journal_entries(
    year_month: str,
    income_records: List[Dict[str, Any]],
    sales_records: List[Dict[str, Any]],
    expense_records: List[Dict[str, Any]],
    transfer_records: List[Dict[str, Any]],
    wave_config: WaveAccountSettings,
    start_ts: int,
    end_ts: int,
) -> Tuple[List[JournalEntry], Dict[str, float]]:
    """Aggregate sheet data into monthly journal entries.

    Returns the list of ``JournalEntry`` rows plus summary metrics for logging.
    """

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
    }

    gain_buckets: Dict[str, Dict[str, Any]] = {
        "Short-term": {"amount": 0.0, "notes": []},
        "Long-term": {"amount": 0.0, "notes": []},
    }

    def _add_amount(account: str, field: str, amount: float, note: Optional[str] = None):
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
            _add_amount(wave_config.alpha_asset_account, "debit", usd_fmv, f"Contract lot {note}: ${usd_fmv:.2f}")
            _add_amount(wave_config.contract_income_account, "credit", usd_fmv, f"Contract lot {note}: ${usd_fmv:.2f}")
        elif source_type == SourceType.STAKING.value:
            summary["staking_income"] += usd_fmv
            _add_amount(wave_config.alpha_asset_account, "debit", usd_fmv, f"Staking lot {note}: ${usd_fmv:.2f}")
            _add_amount(wave_config.staking_income_account, "credit", usd_fmv, f"Staking lot {note}: ${usd_fmv:.2f}")
        elif source_type == SourceType.MINING.value:
            summary["staking_income"] += usd_fmv  # Add to staking_income summary for now
            _add_amount(wave_config.alpha_asset_account, "debit", usd_fmv, f"Mining lot {note}: ${usd_fmv:.2f}")
            _add_amount(wave_config.mining_income_account, "credit", usd_fmv, f"Mining lot {note}: ${usd_fmv:.2f}")

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
            slippage_usd = float(slippage_raw) if slippage_raw not in (None, "") else 0.0
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
            wave_config.tao_asset_account,
            "debit",
            proceeds,
            f"Sale {sale_id}: TAO proceeds ${proceeds:.2f}"
        )
        _add_amount(
            wave_config.alpha_asset_account,
            "credit",
            cost_basis,
            f"Sale {sale_id}: ALPHA cost basis ${cost_basis:.2f}"
        )

        if sale_fee_usd:
            summary["sales_fees"] += sale_fee_usd
            fee_note = f"Sale {sale_id}: Network fee ${sale_fee_usd:.2f}"
            _add_amount(
                wave_config.blockchain_fee_account,
                "debit",
                sale_fee_usd,
                fee_note
            )
            _add_amount(
                wave_config.tao_asset_account,
                "credit",
                sale_fee_usd,
                fee_note
            )

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
            category,
            "debit",
            proceeds,
            f"Expense {expense_id}: ${proceeds:.2f}"
        )
        
        # Credit ALPHA asset for cost basis
        _add_amount(
            wave_config.alpha_asset_account,
            "credit",
            cost_basis,
            f"Expense {expense_id}: ALPHA cost basis ${cost_basis:.2f}"
        )
        
        # Handle network fees if any
        if expense_fee_usd:
            fee_note = f"Expense {expense_id}: Network fee ${expense_fee_usd:.2f}"
            _add_amount(
                wave_config.blockchain_fee_account,
                "debit",
                expense_fee_usd,
                fee_note
            )
            _add_amount(
                wave_config.alpha_asset_account,
                "credit",
                expense_fee_usd,
                fee_note
            )
        
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
            f"Transfer {transfer_id}: USD proceeds ${proceeds:.2f}"
        )
        _add_amount(
            wave_config.tao_asset_account,
            "credit",
            cost_basis,  # Use cost basis from consumed lots
            f"Transfer {transfer_id}: TAO disposed ${cost_basis:.2f}"
        )
        if fee_cost_basis:
            _add_amount(
                wave_config.tao_asset_account,
                "credit",
                fee_cost_basis,
                f"Transfer {transfer_id}: Fee cost basis ${fee_cost_basis:.2f}"
            )
            _add_amount(
                wave_config.blockchain_fee_account,
                "debit",
                fee_cost_basis,
                f"Transfer {transfer_id}: On-chain fees ${fee_cost_basis:.2f}"
            )
            summary["transfer_fees"] += fee_cost_basis

        bucket = gain_buckets.setdefault(gain_type, {"amount": 0.0, "notes": []})
        bucket["amount"] += gain_loss
        bucket["notes"].append(f"Transfer {transfer_id}: ${gain_loss:.2f}")

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
        
        gain_account = gain_account_map.get(gain_type, wave_config.short_term_gain_account)
        loss_account = loss_account_map.get(gain_type, wave_config.short_term_loss_account)
        
        # If using the same account for gains and losses, record net amount once
        if gain_account == loss_account:
            if amount > 0:
                _add_amount(gain_account, "credit", amount, notes or f"{gain_type} net gain ${amount:.2f}")
            else:
                _add_amount(gain_account, "debit", abs(amount), notes or f"{gain_type} net loss ${abs(amount):.2f}")
        else:
            # Separate accounts: record gain or loss to appropriate account
            if amount > 0:
                _add_amount(gain_account, "credit", amount, notes or f"{gain_type} gain total ${amount:.2f}")
            else:
                _add_amount(loss_account, "debit", abs(amount), notes or f"{gain_type} loss total ${abs(amount):.2f}")

    entries: List[JournalEntry] = []
    for account, values in sorted(account_totals.items()):
        debit = round(values["debit"], 2)
        credit = round(values["credit"], 2)
        if abs(debit) < 0.005 and abs(credit) < 0.005:
            continue
        description = f"Aggregated journal for {year_month}: "
        if values["notes"]:
            description += ", ".join(values["notes"][:5])
        else:
            description += account
        entries.append(JournalEntry(
            month=year_month,
            entry_type="Monthly",
            account=account,
            debit=debit if debit >= 0.005 else 0.0,
            credit=credit if credit >= 0.005 else 0.0,
            description=description
        ))

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
                description=f"Aggregated journal for {year_month}: rounding adjustment"
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
            target_entry.description += ("; " if target_entry.description else "") + note

    return entries, summary
