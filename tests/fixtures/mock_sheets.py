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

from datetime import datetime
import pytest
from typing import List, Dict, Any, Optional
from unittest.mock import patch, MagicMock
from dataclasses import dataclass, field

from emissions_tracker.models import TaoLot
from emissions_tracker.utils import initialize_sheets
from tests.fixtures.mock_config import TEST_PAYOUT_COLDKEY_SS58, TEST_SMART_CONTRACT_SS58, TEST_SUBNET_ID, TEST_TRACKER_SHEET_ID, TEST_VALIDATOR_SS58
from emissions_tracker.models import AlphaLot

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
    
    def __init__(self, name: str, spreadsheet=None):
        """
        Initialize worksheet with name and headers.
        
        Args:
            name: Worksheet name
            headers: List of column names for the first row
            spreadsheet: Parent MockSpreadsheet reference
        """
        self.name = name
        self.headers = []
        self.spreadsheet = spreadsheet
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

    def row_values(self, idx) -> List[Any]:
        """
        Get values of a specific row by index (1-based).
        
        Args:
            idx: 1-based row index
            
        Returns:
            List of cell values in the row
        """
        if 1 <= idx <= len(self.rows):
            return self.rows[idx - 1]
        else:
            return []
    
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

    def update(self, cell_range: str, values: List[List[Any]], **kwargs):
        """
        Update a range of cells.
        
        Args:
            cell_range: Range string like "A2:B2"
            values: 2D list of values to set
            **kwargs: Additional arguments (ignored, for compatibility)
        """
        self.batch_update_calls += 1
        
        # Parse range like "A2:B2"
        if ':' in cell_range:
            start_cell = cell_range.split(':')[0]
        else:
            start_cell = cell_range
        
        # Parse cell address
        col_letters = ''.join(c for c in start_cell if c.isalpha())
        row_num = int(''.join(c for c in start_cell if c.isdigit()))
        col_index = column_letter_to_index(col_letters) - 1
        row_index = row_num - 1  # Convert to 0-based (header is at rows[0])
        
        # Update cells for each row in values
        for row_offset, value_row in enumerate(values):
            target_row_index = row_index + row_offset
            
            # Ensure row exists
            while len(self.rows) <= target_row_index:
                self.rows.append([])
            
            # Ensure row has enough columns
            if len(self.rows[target_row_index]) < col_index + len(value_row):
                self.rows[target_row_index].extend([""] * (col_index + len(value_row) - len(self.rows[target_row_index])))
            
            # Update cells in this row
            for col_offset, value in enumerate(value_row):
                target_col_index = col_index + col_offset
                self.rows[target_row_index][target_col_index] = value

        if row_index == 0:
            self.headers = self.rows[0]
        
        self.operations.append(WorksheetOperation(
            operation_type="update",
            data={"range": cell_range, "values": values}
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
            worksheet = MockWorksheet(name, spreadsheet=self)
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
            worksheet = MockWorksheet(title, spreadsheet=self)
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

@pytest.fixture()
def mock_contract_sheet(mock_sheets):
    from emissions_tracker.trackers.contract_tracker import SHEET_CONFIGS
    spreadsheet =  mock_sheets.gspread_module.client.open_by_key(TEST_TRACKER_SHEET_ID)
    initialize_sheets(spreadsheet, SHEET_CONFIGS)

    yield spreadsheet

@pytest.fixture
def seed_historical_lots(
    get_opening_alpha_lot,
    get_opening_tao_lot,
    compute_expected_staking_emission_lots,
    compute_expected_contract_income_lots,
    compute_expected_deposit_lots,
    get_alpha_lot_id,
):
    """
    Fixture that returns a function to seed historical ALPHA and TAO lots into mock sheets.
    
    This pre-populates the Income and TAO Lots sheets with lots computed from
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
    
    def _seed_lots(
        spreadsheet: MockSpreadsheet,
        start_date: datetime,
        end_date: datetime,
        contract_address: str = None,
        netuid: int = None,
        delegate: str = None,
        nominator: str = None,
        wallet_address: str = None
    ):
        """
        Seed historical ALPHA and TAO lots into the sheets using historical TAO prices.
        Includes both staking emissions and contract income (if contract params provided).
        
        Args:
            spreadsheet: MockSpreadsheet instance representing the mock sheet
            start_date: Start date for computing emissions
            end_date: End date for computing emissions
            include_opening_lot: Whether to include opening lots from day before start_date
            contract_address: Smart contract address for filtering contract income (optional)
            netuid: Subnet ID for filtering contract income (optional)
            delegate: Delegate address for filtering contract income (optional)
            nominator: Nominator address for filtering contract income (optional)
            wallet_address: Wallet address for filtering deposits (optional)
        """
        from emissions_tracker.trackers.contract_tracker import INCOME_SHEET, TAO_LOTS_SHEET

        with get_alpha_lot_id.context():

            start_ts = int(start_date.timestamp())
            end_ts = int(end_date.timestamp())
            
            opening_alpha_lot: AlphaLot = get_opening_alpha_lot(start_date)
            opening_tao_lot: TaoLot = get_opening_tao_lot(start_date)

            income_lots: list[AlphaLot] = compute_expected_contract_income_lots(
                start_ts=start_ts,
                end_ts=end_ts,
                contract_address=contract_address or TEST_SMART_CONTRACT_SS58,
                netuid=netuid if netuid is not None else TEST_SUBNET_ID,
                delegate=delegate or TEST_PAYOUT_COLDKEY_SS58,
                nominator=nominator or TEST_VALIDATOR_SS58
            )
            emission_lots: list[AlphaLot] = compute_expected_staking_emission_lots(
                start_date=start_date,
                end_date=end_date
            )
            
            # Get TAO lots from deposits (incoming transfers)
            deposit_lots: list[TaoLot] = compute_expected_deposit_lots(
                start_date=start_date,
                end_date=end_date,
                wallet_address=wallet_address or TEST_PAYOUT_COLDKEY_SS58
            )
            
            # Collect all lots (staking emissions + contract income) and sort chronologically
            all_lots = [opening_alpha_lot] + income_lots + emission_lots
            all_lots.sort(key=lambda x: x.timestamp)
            
            # Create and append all lots in chronological order
            lot_counter = 1
            income_sheet = spreadsheet.get_worksheet(INCOME_SHEET)
            for lot in all_lots:
                # Overwrite alpha lot id so they are sequential
                lot.lot_id=f"ALPHA-{lot_counter:04d}"
                income_sheet.append_row(lot.to_sheet_row())
                lot_counter += 1
            
            # Seed TAO lots: opening lot + deposit lots
            tao_sheet = spreadsheet.get_worksheet(TAO_LOTS_SHEET)
            tao_sheet.append_row(opening_tao_lot.to_sheet_row())
            for deposit_lot in deposit_lots:
                tao_sheet.append_row(deposit_lot.to_sheet_row())

            return all_lots
    
    return _seed_lots


@pytest.fixture()
def seed_contract_sheets(
    seed_historical_lots,
    mock_contract_sheet
):
    """
    Fixture that seeds mock sheets with historical data based on test parameters.
    Returns a function that takes test params and seeds the sheets.
    """

    def _seed_sheets(start_date, end_date):
        """Seed sheets with opening balances + all emissions/contract income for the given date range.
        
        This prepares the sheets with historical data so the tracker can process transactions.
        seed_historical_lots now handles both ALPHA and TAO opening balances.
        """
        
        # Seed opening lots (ALPHA + TAO) + all emissions and contract income for the provided date range
        # Returns dict with 'alpha_lots' and optional 'tao_lot'
        alpha_lots = seed_historical_lots(
            spreadsheet=mock_contract_sheet,
            start_date=start_date,
            end_date=end_date,
            contract_address=TEST_SMART_CONTRACT_SS58,
            netuid=TEST_SUBNET_ID,
            delegate=TEST_VALIDATOR_SS58,
            nominator=TEST_PAYOUT_COLDKEY_SS58
        )
        
        # Return the alpha lots for consumption tracking by compute fixtures
        return alpha_lots
    
    return _seed_sheets