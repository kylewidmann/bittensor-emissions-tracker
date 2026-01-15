"""
Shared mock Google Sheets infrastructure for testing.

Provides in-memory worksheet and spreadsheet mocks that track data,
allowing tests to verify the tracker's sheet operations without actual API calls.

Usage:
    def test_something(mock_sheets):
        # Create tracker normally - it will use mocked sheets
        tracker = BittensorEmissionTracker(
            ...,
            sheet_id="test-sheet-123"
        )
        
        # Access sheets for verification
        income_sheet = mock_sheets.get_worksheet("test-sheet-123", "Income")
        assert income_sheet.row_count == 5
        assert income_sheet.append_row_calls == 3
"""

import pytest
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from unittest.mock import patch, MagicMock
from dataclasses import dataclass, field

from emissions_tracker.models import TaoLot
from tests.fixtures.mock_config import TEST_PAYOUT_COLDKEY_SS58, TEST_SMART_CONTRACT_SS58, TEST_SUBNET_ID, TEST_VALIDATOR_SS58


def column_letter_to_index(letters: str) -> int:
    """Convert Excel-style column letters to 0-based index."""
    value = 0
    for ch in letters.upper():
        if not ch.isalpha():
            continue
        value = value * 26 + (ord(ch) - ord("A") + 1)
    return value


@dataclass
class WorksheetOperation:
    """Record of a single worksheet operation."""
    operation_type: str  # "append_row", "append_rows", "batch_update", "clear", "sort"
    data: Any
    timestamp: float = field(default_factory=lambda: __import__('time').time())


class MockWorksheet:
    """
    Mock worksheet that simulates Google Sheets behavior and tracks all operations.
    
    Tracks:
    - All append_row/append_rows calls
    - All batch_update operations
    - All clear/sort operations
    - Current state of rows
    """
    
    def __init__(self, name: str, headers: List[str]):
        """
        Initialize worksheet with name and headers.
        
        Args:
            name: Worksheet name
            headers: List of column names for the first row
        """
        self.name = name
        self.headers = headers
        self.rows: List[List[Any]] = []
        self.operations: List[WorksheetOperation] = []
        
        # Call counters
        self.append_row_calls = 0
        self.append_rows_calls = 0
        self.batch_update_calls = 0
        self.clear_calls = 0
        self.sort_calls = 0
        self.get_all_records_calls = 0
    
    def get_all_records(self) -> List[Dict[str, Any]]:
        """
        Get all rows as dictionaries (like gspread's get_all_records()).
        
        Uses the first row as headers (matching gspread behavior).
        
        Returns:
            List of dictionaries with header keys and row values
        """
        self.get_all_records_calls += 1
        
        # If no rows or only header row, return empty list
        if len(self.rows) <= 1:
            return []
        
        # Use first row as headers
        headers = self.rows[0]
        
        # Convert remaining rows to dicts
        results = []
        for row in self.rows[1:]:
            record = {}
            for idx, header in enumerate(headers):
                if idx < len(row):
                    record[header] = row[idx]
                else:
                    record[header] = ""
            results.append(record)
        return results
    
    def append_row(self, row: List[Any], **kwargs):
        """
        Append a single row to the worksheet.
        
        Args:
            row: List of values to append
            **kwargs: Additional arguments (ignored, for compatibility)
        """
        self.append_row_calls += 1
        
        # Special handling for header rows (first row on empty sheet)
        if not self.rows and len(row) > 1:
            # This looks like a header row - set it as headers
            self.headers = list(row)
        
        # Pad row to match current header length
        self.rows.append(row)
        self.operations.append(WorksheetOperation(
            operation_type="append_row",
            data=row
        ))
    
    def append_rows(self, rows: List[List[Any]], **kwargs):
        """
        Append multiple rows to the worksheet.
        
        Args:
            rows: List of row value lists
            **kwargs: Additional arguments (ignored, for compatibility)
        """
        self.append_rows_calls += 1
        for row in rows:
            padded = list(row) + [""] * max(0, len(self.headers) - len(row))
            self.rows.append(padded[:len(self.headers)])
        self.operations.append(WorksheetOperation(
            operation_type="append_rows",
            data=rows
        ))
    
    def batch_update(self, data: List[Dict[str, Any]], **kwargs):
        """
        Apply batch updates to cells.
        
        Args:
            data: List of update dictionaries with 'range' and 'values' keys
            **kwargs: Additional arguments (ignored, for compatibility)
        """
        self.batch_update_calls += 1
        for update in data:
            range_str = update['range']
            values = update['values']
            
            # Parse range like "Sheet!A2:B2" or "A2" or "A2:A2"
            if '!' in range_str:
                range_str = range_str.split('!')[1]
            
            # Parse single cell or single-cell range (A2 or A2:A2)
            if ':' in range_str:
                start_cell = range_str.split(':')[0]
            else:
                start_cell = range_str
            
            # Parse cell address
            col_letters = ''.join(c for c in start_cell if c.isalpha())
            row_num = int(''.join(c for c in start_cell if c.isdigit()))
            col_index = column_letter_to_index(col_letters) - 1
            row_index = row_num - 1  # Convert to 0-based (header is at rows[0])
            
            # Ensure row exists
            while len(self.rows) <= row_index:
                self.rows.append([""] * len(self.headers))
            
            # Update cell
            if 0 <= col_index < len(self.headers):
                self.rows[row_index][col_index] = values[0][0]
        
        self.operations.append(WorksheetOperation(
            operation_type="batch_update",
            data=data
        ))
    
    def sort(self, *args, **kwargs):
        """Mock sort operation (no-op for testing but tracked)."""
        self.sort_calls += 1
        self.operations.append(WorksheetOperation(
            operation_type="sort",
            data={"args": args, "kwargs": kwargs}
        ))
    
    def clear(self):
        """Clear all rows (keeps headers)."""
        self.clear_calls += 1
        self.rows = []
        self.operations.append(WorksheetOperation(
            operation_type="clear",
            data=None
        ))
    
    @property
    def row_count(self) -> int:
        """Number of data rows (excluding header)."""
        return len(self.rows)
    
    def seed_data(self, records: List[Dict[str, Any]]):
        """
        Seed the worksheet with initial data records (for test setup).
        
        Args:
            records: List of dictionaries with keys matching headers
        """
        for record in records:
            row = [record.get(header, "") for header in self.headers]
            self.rows.append(row)


class MockSpreadsheet:
    """
    Mock Google Spreadsheet that manages multiple worksheets and tracks operations.
    
    Provides worksheet() to access sheets by name and supports
    batch updates across multiple sheets.
    """
    
    def __init__(self, sheet_id: str):
        """
        Initialize spreadsheet.
        
        Args:
            sheet_id: Spreadsheet ID
        """
        self.sheet_id = sheet_id
        self.worksheets: Dict[str, MockWorksheet] = {}
        self.batch_update_calls = 0
        self.values_batch_update_calls = 0
    
    def worksheet(self, name: str) -> MockWorksheet:
        """
        Get a worksheet by name (creates if doesn't exist).
        
        Args:
            name: Sheet name
            
        Returns:
            MockWorksheet instance
        """
        if name not in self.worksheets:
            # Auto-create with appropriate headers based on sheet name
            from emissions_tracker.models import AlphaLot, TaoLot, AlphaSale, Expense, TaoTransfer, JournalEntry
            
            if name == "Income":
                headers = AlphaLot.sheet_headers()
            elif name == "TAO Lots":
                headers = TaoLot.sheet_headers()
            elif name == "Sales":
                headers = AlphaSale.sheet_headers()
            elif name == "Expenses":
                headers = Expense.sheet_headers()
            elif name == "Transfers":
                headers = TaoTransfer.sheet_headers()
            elif name == "Journal Entries":
                headers = JournalEntry.sheet_headers()
            else:
                raise AssertionError(f"Unknown sheet {name}")
            
            worksheet = MockWorksheet(name, headers)
            # Add header row as first row
            worksheet.append_row(headers)
            self.worksheets[name] = worksheet
        return self.worksheets[name]
    
    def add_worksheet(self, title: str, rows: int = 100, cols: int = 20) -> MockWorksheet:
        """
        Add a new worksheet (creates if doesn't exist).
        
        Args:
            title: Worksheet name
            rows: Number of rows (ignored)
            cols: Number of columns (ignored)
            
        Returns:
            MockWorksheet instance
        """
        if title not in self.worksheets:
            # Auto-create with appropriate headers based on sheet name
            from emissions_tracker.models import AlphaLot, TaoLot, AlphaSale, Expense, TaoTransfer, JournalEntry
            
            if title == "Income":
                headers = AlphaLot.sheet_headers()
            elif title == "TAO Lots":
                headers = TaoLot.sheet_headers()
            elif title == "Sales":
                headers = AlphaSale.sheet_headers()
            elif title == "Expenses":
                headers = Expense.sheet_headers()
            elif title == "Transfers":
                headers = TaoTransfer.sheet_headers()
            elif title == "Journal Entries":
                headers = JournalEntry.sheet_headers()
            else:
                raise AssertionError(f"Unknown sheet {title}")
            
            worksheet = MockWorksheet(title, headers)
            # Add header row as first row
            worksheet.append_row(headers)
            self.worksheets[title] = worksheet
        return self.worksheets[title]
    
    def values_batch_update(self, body: Dict[str, Any]):
        """
        Apply batch updates across multiple sheets.
        
        Args:
            body: Batch update request body with 'data' key containing updates
        """
        self.values_batch_update_calls += 1
        for update in body.get("data", []):
            range_str = update["range"]
            values = update["values"]
            
            # Parse "SheetName!A2:B2" format
            if '!' in range_str:
                sheet_name, cell_range = range_str.split('!', 1)
                if sheet_name in self.worksheets:
                    self.worksheets[sheet_name].batch_update([{
                        'range': cell_range,
                        'values': values
                    }])
    
    def batch_update(self, body: Dict[str, Any]):
        """Batch update (alias for values_batch_update)."""
        self.batch_update_calls += 1
        self.values_batch_update(body)
    
    def get_worksheet(self, name: str) -> Optional[MockWorksheet]:
        """Get worksheet by name without auto-creating."""
        return self.worksheets.get(name)


class MockSheetsClient:
    """
    Mock gspread client that creates and tracks spreadsheets.
    """
    
    def __init__(self):
        """Initialize client."""
        self.spreadsheets: Dict[str, MockSpreadsheet] = {}
        self.open_by_key_calls = 0
    
    def open_by_key(self, sheet_id: str) -> MockSpreadsheet:
        """
        Open a spreadsheet by ID (creates if doesn't exist).
        
        Args:
            sheet_id: Spreadsheet ID
            
        Returns:
            MockSpreadsheet instance
        """
        self.open_by_key_calls += 1
        if sheet_id not in self.spreadsheets:
            self.spreadsheets[sheet_id] = MockSpreadsheet(sheet_id)
        return self.spreadsheets[sheet_id]
    
    def get_spreadsheet(self, sheet_id: str) -> Optional[MockSpreadsheet]:
        """Get spreadsheet without auto-creating."""
        return self.spreadsheets.get(sheet_id)


class MockGspreadModule:
    """
    Mock gspread module that provides authorize() method.
    """
    
    def __init__(self):
        """Initialize module."""
        self.client = MockSheetsClient()
        self.authorize_calls = 0
    
    def authorize(self, credentials) -> MockSheetsClient:
        """
        Authorize and return client.
        
        Args:
            credentials: Credentials object (ignored)
            
        Returns:
            MockSheetsClient instance
        """
        self.authorize_calls += 1
        return self.client


class MockSheetsEnvironment:
    """
    Unified mock environment that tracks all sheets operations.
    
    Provides high-level inspection methods for test verification.
    """
    
    def __init__(self):
        """Initialize environment."""
        self.gspread_module = MockGspreadModule()
        self.client = self.gspread_module.client
    
    def get_spreadsheet(self, sheet_id: str) -> Optional[MockSpreadsheet]:
        """Get spreadsheet by ID."""
        return self.client.get_spreadsheet(sheet_id)
    
    def get_worksheet(self, sheet_id: str, worksheet_name: str) -> Optional[MockWorksheet]:
        """Get specific worksheet."""
        spreadsheet = self.get_spreadsheet(sheet_id)
        if spreadsheet:
            return spreadsheet.get_worksheet(worksheet_name)
        return None


@pytest.fixture(autouse=True)
def mock_sheets():
    """
    Pytest fixture that mocks `gspread for tracker tests.
    
    Usage:
        def test_something(mock_sheets):
            tracker = BittensorEmissionTracker(..., sheet_id="test-123")
            
            # Access sheets directly via nested properties
            income_sheet = mock_sheets.get_worksheet("test-123", "Income")
            assert income_sheet.row_count == 5
    """
    mock_env = MockSheetsEnvironment()
    
    # Patch gspread module in tracker
    with patch('emissions_tracker.trackers.contract_tracker.gspread', mock_env.gspread_module):
        # Patch ServiceAccountCredentials (tracker checks if it's None)
        mock_creds_class = MagicMock()
        mock_creds_class.from_json_keyfile_name.return_value = MagicMock()
        
        with patch('emissions_tracker.trackers.contract_tracker.ServiceAccountCredentials', mock_creds_class):
            yield mock_env


@pytest.fixture
def seed_historical_lots(mock_sheets, raw_stake_balance, raw_stake_events, raw_historical_prices):
    """
    Fixture that returns a function to seed historical ALPHA lots into mock sheets.
    
    This pre-populates the Income sheet with ALPHA emission lots computed from
    the test data using historical TAO prices, including both staking emissions
    and contract income.
    
    Usage:
        def test_something(seed_historical_lots):
            # Seed lots from Oct 1 through Nov 30 using historical TAO prices
            seed_historical_lots(
                sheet_id=tracker.sheet_id,
                start_date=datetime(2025, 10, 1),
                end_date=datetime(2025, 11, 30),
                contract_address='5F...',  # Optional
                netuid=0,  # Optional
                delegate='5F...',  # Optional  
                nominator='5F...'  # Optional
            )
            
            # Now tracker will see historical lots when it loads state
            # ...rest of test
    
    Args:
        mock_sheets: The mock sheets environment fixture
        raw_stake_balance: Fixture providing raw balance data
        raw_stake_events: Fixture providing raw event data (includes contract income)
        raw_historical_prices: Fixture providing historical TAO price data
    
    Returns:
        Callable that takes (sheet_id, start_date, end_date, optional contract params) 
        and seeds the Income sheet using historical TAO prices
    """
    from pathlib import Path
    import json
    from emissions_tracker.models import AlphaLot, SourceType
    from tests.utils import (
        filter_balances_by_date_range,
        group_balances_by_day,
        filter_delegation_events,
        group_events_by_day,
        calculate_daily_emissions
    )
    
    def _seed_lots(
        sheet_id: str,
        start_date: datetime,
        end_date: datetime,
        include_opening_lot: bool = True,
        contract_address: str = None,
        netuid: int = None,
        delegate: str = None,
        nominator: str = None
    ):
        """
        Seed historical ALPHA lots into the Income sheet using historical TAO prices.
        Includes both staking emissions and contract income (if contract params provided).
        
        Args:
            sheet_id: Google Sheet ID (used to access the mock sheet)
            start_date: Start date for computing emissions
            end_date: End date for computing emissions
            include_opening_lot: Whether to include an opening lot derived from account_history.json
            contract_address: Smart contract address for filtering contract income (optional)
            netuid: Subnet ID for filtering contract income (optional)
            delegate: Delegate address for filtering contract income (optional)
            nominator: Nominator address for filtering contract income (optional)
        """
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        
        # Load stake_balance to get actual opening balance (raw ALPHA, not TAO-equivalent)
        from pathlib import Path
        from datetime import timezone
        data_dir = Path(__file__).parent.parent / "data" / "all"
        with open(data_dir / "stake_balance.json") as f:
            stake_balance_data = json.load(f)['data']

        # Find balance on day BEFORE start_date (to get true opening balance)
        opening_alpha_amount = None
        # Make start_date timezone-aware for comparison
        start_date_aware = start_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        # We want the balance from the day BEFORE start_date
        day_before_start = start_date_aware - timedelta(days=1)

        for record in stake_balance_data:
            record_dt = datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
            record_date_only = record_dt.replace(hour=0, minute=0, second=0, microsecond=0)

            # Use the last balance from the day before start_date
            if record_date_only == day_before_start:
                opening_alpha_amount = int(record['balance']) / 1e9
                break

        if opening_alpha_amount is None:
            # Fallback: find any balance before start_date
            for record in stake_balance_data:
                record_dt = datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
                if record_dt < start_date_aware:
                    opening_alpha_amount = int(record['balance']) / 1e9
                    break

        if opening_alpha_amount is None:
            raise ValueError(f"No balance found before {start_date.strftime('%Y-%m-%d')}")
        
        # Create a price lookup function for historical TAO prices
        def price_lookup(day_str: str) -> float:
            """Look up TAO price for a specific day."""
            return raw_historical_prices.get(day_str, {}).get('price', 0.0)
        
        # Compute emissions using shared utilities and fixture data with historical prices
        balances = filter_balances_by_date_range(raw_stake_balance, start_ts, end_ts)
        daily_balances = group_balances_by_day(balances)
        events = filter_delegation_events(raw_stake_events, start_ts, end_ts)
        events_by_day = group_events_by_day(events)
        
        alpha_lots, _, _ = calculate_daily_emissions(
            daily_balances,
            events_by_day,
            price_lookup=price_lookup,
            emission_threshold=0.0001
        )
        
        # Get the Income sheet from mock environment (create spreadsheet if needed)
        spreadsheet = mock_sheets.client.spreadsheets.get(sheet_id)
        if not spreadsheet:
            spreadsheet = MockSpreadsheet(sheet_id)
            mock_sheets.client.spreadsheets[sheet_id] = spreadsheet
        
        # Get or create Income sheet
        income_sheet = spreadsheet.worksheet("Income")
        
        # Update headers attribute FIRST (before appending rows)
        if not income_sheet.headers:
            income_sheet.headers = AlphaLot.sheet_headers()
        
        # Ensure header row exists (append if sheet is empty)
        if not income_sheet.rows:
            income_sheet.append_row(AlphaLot.sheet_headers())
        
        # Add opening lot if requested (represents actual ALPHA balance at start_date)
        if include_opening_lot:
            opening_lot_date_str = start_date.strftime('%Y-%m-%d')
            opening_lot_ts = int(start_date.timestamp())
            
            # Get the actual TAO price from start_date (fail if not found)
            opening_price_data = raw_historical_prices.get(opening_lot_date_str)
            if not opening_price_data or 'price' not in opening_price_data:
                raise ValueError(f"No TAO price data found for opening lot date {opening_lot_date_str}")
            opening_tao_price = opening_price_data['price']
            # ALPHA price is ~8% of TAO price (1 ALPHA ~= 0.08 TAO)
            tao_alpha_ratio = 0.08
            opening_alpha_price = opening_tao_price * tao_alpha_ratio
            opening_usd = opening_alpha_amount * opening_alpha_price
            
            # Create AlphaLot for opening balance
            opening_alpha_rao = int(round(opening_alpha_amount * 1e9))
            opening_lot = AlphaLot(
                lot_id="ALPHA-0001",
                timestamp=opening_lot_ts,
                block_number=0,
                source_type=SourceType.STAKING,
                alpha_rao=opening_alpha_rao,
                alpha_rao_remaining=opening_alpha_rao,
                usd_fmv=opening_usd,
                usd_per_alpha=opening_alpha_price,
                tao_equivalent=opening_alpha_amount * tao_alpha_ratio,
                notes="Opening balance"
            )
            income_sheet.append_row(opening_lot.to_sheet_row())
        
        # Collect all lots (staking emissions + contract income) and sort chronologically
        all_lots = []
        
        # Add staking emission lots
        for lot_data in alpha_lots:
            all_lots.append({
                'timestamp': lot_data['timestamp'],
                'block_number': lot_data.get('block_number', 0),
                'source_type': SourceType.STAKING,
                'alpha_quantity': lot_data['alpha_quantity'],
                'alpha_rao': int(round(lot_data['alpha_quantity'] * 1e9)),
                'usd_fmv': lot_data['usd_fmv'],
                'usd_per_alpha': lot_data['usd_fmv'] / lot_data['alpha_quantity'] if lot_data['alpha_quantity'] > 0 else 0,
                'tao_equivalent': lot_data.get('tao_equivalent', lot_data['alpha_quantity'] * 0.08),
                'notes': "Staking emissions"
            })
        
        # Add contract income lots if contract parameters are provided
        if all([contract_address, netuid is not None, delegate, nominator]):
            from tests.utils import filter_contract_income_events
            
            contract_events = filter_contract_income_events(
                raw_stake_events,
                start_ts,
                end_ts,
                contract_address=contract_address,
                netuid=netuid,
                delegate=delegate,
                nominator=nominator
            )
            
            for event in contract_events:
                event_ts = int(datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00')).timestamp())
                event_date = datetime.fromtimestamp(event_ts).strftime('%Y-%m-%d')
                tao_price = raw_historical_prices.get(event_date, {}).get('price', 0.0)
                
                alpha_quantity = int(event['alpha']) / 1e9
                alpha_rao = int(event['alpha'])
                usd_fmv = float(event.get('usd', 0))
                
                all_lots.append({
                    'timestamp': event_ts,
                    'block_number': event.get('block_number', 0),
                    'source_type': SourceType.CONTRACT,
                    'alpha_quantity': alpha_quantity,
                    'alpha_rao': alpha_rao,
                    'usd_fmv': usd_fmv,
                    'usd_per_alpha': usd_fmv / alpha_quantity if alpha_quantity > 0 else 0,
                    'tao_equivalent': float(event.get('amount', 0)) / 1e9,
                    'notes': "Contract income"
                })
        
        # Sort all lots by timestamp to ensure true chronological order
        all_lots_sorted = sorted(all_lots, key=lambda x: x['timestamp'])
        
        # Create and append all lots in chronological order
        lot_counter = 2 if include_opening_lot else 1
        for lot_data in all_lots_sorted:
            lot = AlphaLot(
                lot_id=f"ALPHA-{lot_counter:04d}",
                timestamp=lot_data['timestamp'],
                block_number=lot_data['block_number'],
                source_type=lot_data['source_type'],
                alpha_rao=lot_data['alpha_rao'],
                alpha_rao_remaining=lot_data['alpha_rao'],
                usd_fmv=lot_data['usd_fmv'],
                usd_per_alpha=lot_data['usd_per_alpha'],
                tao_equivalent=lot_data['tao_equivalent'],
                notes=lot_data['notes']
            )
            income_sheet.append_row(lot.to_sheet_row())
            lot_counter += 1
    
    return _seed_lots


@pytest.fixture()
def seed_contract_sheets(
    raw_account_history,
    raw_stake_balance,
    raw_historical_prices,
    seed_historical_lots, 
    mock_sheets
):
    """
    Fixture that seeds mock sheets with historical data based on test parameters.
    Returns a function that takes test params and seeds the sheets.
    """
    def _seed_sheets(start_date, end_date, sheet_id):
        """Seed the Income sheet with historical ALPHA lots and TAO Lots sheet with opening balance."""
        
        # seed_historical_lots now uses historical TAO prices and derives opening balance from account_history.json
        # It also includes contract income when contract parameters are provided
        seed_historical_lots(
            sheet_id=sheet_id,
            start_date=start_date,
            end_date=end_date,
            include_opening_lot=True,
            contract_address=TEST_SMART_CONTRACT_SS58,
            netuid=TEST_SUBNET_ID,
            delegate=TEST_VALIDATOR_SS58,
            nominator=TEST_PAYOUT_COLDKEY_SS58
        )
        
        # Find balance_free from day before start_date
        start_date_aware = start_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        day_before_start = start_date_aware - timedelta(days=1)
        
        opening_tao_rao = None
        for record in raw_account_history:
            record_dt = datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
            record_date_only = record_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            
            if record_date_only == day_before_start:
                opening_tao_rao = int(record['balance_free'])
                break
        
        if opening_tao_rao and opening_tao_rao > 0:
            # Get TAO price for the opening date
            opening_date_str = start_date.strftime('%Y-%m-%d')
            opening_tao_price = raw_historical_prices.get(opening_date_str, {}).get('price', 0.0)
            
            if opening_tao_price:
                opening_tao_amount = opening_tao_rao / 1e9
                opening_tao_usd = opening_tao_amount * opening_tao_price
                
                spreadsheet = mock_sheets.client.spreadsheets.get(sheet_id)
                tao_lots_sheet = spreadsheet.worksheet("TAO Lots")
                
                opening_tao_lot = TaoLot(
                    lot_id="TAO-0001",
                    timestamp=int(start_date.timestamp()),
                    block_number=0,
                    rao=opening_tao_rao,
                    rao_remaining=opening_tao_rao,
                    usd_basis=opening_tao_usd,
                    usd_per_tao=opening_tao_price,
                    source_sale_id="",
                    extrinsic_id="",
                    notes="Opening balance"
                )
                tao_lots_sheet.append_row(opening_tao_lot.to_sheet_row())

        # Read back the seeded lots from the Income sheet
        spreadsheet = mock_sheets.client.spreadsheets.get(sheet_id)
        income_sheet = spreadsheet.worksheet("Income")
        lot_rows = income_sheet.get_all_records()
        
        # Convert to dict format for consumption tracking (using snake_case keys for compute fixtures)
        alpha_lots = []
        for row in lot_rows:
            alpha_lots.append({
                'lot_id': row['Lot ID'],
                'timestamp': int(datetime.fromisoformat(row['Date']).timestamp()) if isinstance(row['Date'], str) else row['Date'],
                'alpha_quantity': float(row['Alpha Quantity']),
                'alpha_remaining': float(row['Alpha Remaining']),
                'usd_fmv': float(row['USD FMV']),
                'usd_per_alpha': float(row['USD FMV']) / float(row['Alpha Quantity']) if float(row['Alpha Quantity']) > 0 else 0,
                'status': row['Status']
            })
        
        return alpha_lots
    
    return _seed_sheets