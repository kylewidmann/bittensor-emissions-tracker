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
from typing import List, Dict, Any, Optional
from unittest.mock import patch, MagicMock
from dataclasses import dataclass, field


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
        
        Returns:
            List of dictionaries with header keys and row values
        """
        self.get_all_records_calls += 1
        results = []
        for row in self.rows:
            record = {}
            for idx, header in enumerate(self.headers):
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
        padded = list(row) + [""] * max(0, len(self.headers) - len(row))
        self.rows.append(padded[:len(self.headers)])
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
            row_index = row_num - 2  # Skip header row, convert to 0-based
            
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
            # Auto-create with basic headers
            self.worksheets[name] = MockWorksheet(name, ["Column A"])
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
            self.worksheets[title] = MockWorksheet(title, ["Column A"])
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
    with patch('emissions_tracker.tracker.gspread', mock_env.gspread_module):
        # Patch ServiceAccountCredentials (tracker checks if it's None)
        mock_creds_class = MagicMock()
        mock_creds_class.from_json_keyfile_name.return_value = MagicMock()
        
        with patch('emissions_tracker.tracker.ServiceAccountCredentials', mock_creds_class):
            yield mock_env


# # Legacy compatibility (to be removed after test migration)

# def create_mock_sheets_for_tracker():
#     """
#     DEPRECATED: Legacy function for backward compatibility.
#     Use patch_gspread_for_tracker() instead.
#     """
#     from emissions_tracker.models import AlphaLot, TaoLot
    
#     income_sheet = InMemoryWorksheet(AlphaLot.sheet_headers())
#     tao_lots_sheet = InMemoryWorksheet(TaoLot.sheet_headers())
#     sales_sheet = InMemoryWorksheet([
#         "Sale ID", "Date", "Timestamp", "Block", "Alpha Disposed",
#         "TAO Received", "TAO Price USD", "USD Proceeds", "Cost Basis",
#         "Realized Gain/Loss", "Gain Type", "TAO Expected", "TAO Slippage",
#         "Slippage USD", "Slippage Ratio", "Network Fee (TAO)", "Network Fee (USD)",
#         "Consumed Lots", "Created TAO Lot ID", "Extrinsic ID", "Notes"
#     ])
#     expenses_sheet = InMemoryWorksheet([
#         "Expense ID", "Date", "Timestamp", "Block", "Transfer Address", "Category",
#         "Alpha Disposed", "TAO Received", "TAO Price USD", "USD Proceeds", 
#         "Cost Basis", "Realized Gain/Loss", "Gain Type", "TAO Expected", 
#         "TAO Slippage", "Slippage USD", "Slippage Ratio",
#         "Network Fee (TAO)", "Network Fee (USD)",
#         "Consumed Lots", "Created TAO Lot ID", "Extrinsic ID", "Notes"
#     ])
#     transfers_sheet = InMemoryWorksheet([
#         "Transfer ID", "Date", "Timestamp", "Block", "TAO Amount",
#         "TAO Price USD", "USD Proceeds", "Cost Basis", "Realized Gain/Loss",
#         "Gain Type", "Consumed TAO Lots", "Transaction Hash", "Extrinsic ID",
#         "Notes", "Total Outflow TAO", "Fee TAO", "Fee Cost Basis USD"
#     ])
#     journal_sheet = InMemoryWorksheet([
#         "Date", "Account", "Debit", "Credit", "Description", "Reference"
#     ])
    
#     spreadsheet = MockSpreadsheet()
#     spreadsheet.register("Income", income_sheet)
#     spreadsheet.register("TAO Lots", tao_lots_sheet)
#     spreadsheet.register("Sales", sales_sheet)
#     spreadsheet.register("Expenses", expenses_sheet)
#     spreadsheet.register("Transfers", transfers_sheet)
#     spreadsheet.register("Journal Entries", journal_sheet)
    
#     return (spreadsheet, income_sheet, tao_lots_sheet, sales_sheet, 
#             expenses_sheet, transfers_sheet, journal_sheet)


# class InMemoryWorksheet:
#     """
#     DEPRECATED: Legacy worksheet implementation.
#     Use MockWorksheet with patch_gspread_for_tracker() instead.
#     """
    
#     def __init__(self, headers: List[str]):
#         """
#         Initialize worksheet with headers.
        
#         Args:
#             headers: List of column names for the first row
#         """
#         self.headers = headers
#         self.rows = []
#         self.pending_updates = []  # Staged updates for batch operations
    
#     def seed_records(self, records: List[Dict[str, Any]]):
#         """
#         Seed the worksheet with initial data records.
        
#         Args:
#             records: List of dictionaries with keys matching headers
#         """
#         for record in records:
#             row = [record.get(header, "") for header in self.headers]
#             self.rows.append(row)
    
#     def get_all_records(self) -> List[Dict[str, Any]]:
#         """
#         Get all rows as dictionaries (like gspread's get_all_records()).
        
#         Returns:
#             List of dictionaries with header keys and row values
#         """
#         results = []
#         for row in self.rows:
#             record = {}
#             for idx, header in enumerate(self.headers):
#                 if idx < len(row):
#                     record[header] = row[idx]
#                 else:
#                     record[header] = ""
#             results.append(record)
#         return results
    
#     def append_row(self, row: List[Any]):
#         """
#         Append a single row to the worksheet.
        
#         Args:
#             row: List of values to append
#         """
#         # Pad or truncate to match header count
#         padded = list(row) + [""] * max(0, len(self.headers) - len(row))
#         self.rows.append(padded[:len(self.headers)])
    
#     def append_rows(self, rows: List[List[Any]]):
#         """
#         Append multiple rows to the worksheet.
        
#         Args:
#             rows: List of row value lists
#         """
#         for row in rows:
#             self.append_row(row)
    
#     def update_cell(self, row_num: int, column_letters: str, value: Any):
#         """
#         Update a single cell (used for batch updates).
        
#         Args:
#             row_num: 1-based row number
#             column_letters: Excel-style column letter (e.g., "A", "B", "AA")
#             value: New cell value
#         """
#         self.pending_updates.append({
#             'row_num': row_num,
#             'column_letters': column_letters,
#             'value': value
#         })
    
#     def batch_update(self, data: List[Dict[str, Any]]):
#         """
#         Apply batch updates to cells.
        
#         Args:
#             data: List of update dictionaries with 'range' and 'values' keys
#         """
#         for update in data:
#             range_str = update['range']
#             values = update['values']
            
#             # Parse range like "Sheet!A2:B2" or "A2" or "A2:A2"
#             if '!' in range_str:
#                 range_str = range_str.split('!')[1]
            
#             # Parse single cell or single-cell range (A2 or A2:A2)
#             # Extract start cell
#             if ':' in range_str:
#                 start_cell = range_str.split(':')[0]
#             else:
#                 start_cell = range_str
            
#             # Parse cell address
#             col_letters = ''.join(c for c in start_cell if c.isalpha())
#             row_num = int(''.join(c for c in start_cell if c.isdigit()))
#             col_index = column_letter_to_index(col_letters) - 1
#             row_index = row_num - 2  # Skip header row, convert to 0-based
            
#             if 0 <= row_index < len(self.rows) and 0 <= col_index < len(self.headers):
#                 self.rows[row_index][col_index] = values[0][0]
    
#     def flush_pending_updates(self):
#         """Apply all staged cell updates (for testing batch operations)."""
#         for update in self.pending_updates:
#             column_index = column_letter_to_index(update['column_letters']) - 1
#             row_index = update['row_num'] - 2  # Skip header row, convert to 0-based
            
#             if 0 <= row_index < len(self.rows) and 0 <= column_index < len(self.headers):
#                 self.rows[row_index][column_index] = update['value']
        
#         self.pending_updates.clear()
    
#     def sort(self, *args, **kwargs):
#         """Mock sort operation (no-op for testing)."""
#         pass
    
#     def clear(self):
#         """Clear all rows (keeps headers)."""
#         self.rows = []
#         self.pending_updates = []


# class MockSpreadsheet:
#     """
#     DEPRECATED: Legacy spreadsheet implementation.
#     Use MockSpreadsheet (new version) with patch_gspread_for_tracker() instead.
    
#     Mock Google Spreadsheet that manages multiple worksheets.
    
#     Provides worksheet() to access sheets by name and supports
#     batch updates across multiple sheets.
#     """
    
#     def __init__(self):
#         """Initialize empty spreadsheet."""
#         self.worksheets_dict: Dict[str, InMemoryWorksheet] = {}
    
#     def register(self, name: str, worksheet: InMemoryWorksheet):
#         """
#         Register a worksheet with this spreadsheet.
        
#         Args:
#             name: Sheet name
#             worksheet: InMemoryWorksheet instance
#         """
#         self.worksheets_dict[name] = worksheet
    
#     def worksheet(self, name: str) -> InMemoryWorksheet:
#         """
#         Get a worksheet by name.
        
#         Args:
#             name: Sheet name
            
#         Returns:
#             InMemoryWorksheet instance
            
#         Raises:
#             KeyError: If worksheet not found
#         """
#         if name not in self.worksheets_dict:
#             raise KeyError(f"Worksheet '{name}' not found")
#         return self.worksheets_dict[name]
    
#     def values_batch_update(self, body: Dict[str, Any]):
#         """
#         Apply batch updates across multiple sheets.
        
#         Args:
#             body: Batch update request body with 'data' key containing updates
#         """
#         for update in body.get("data", []):
#             range_str = update["range"]
#             values = update["values"]
            
#             # Parse "SheetName!A2:B2" format
#             if '!' in range_str:
#                 sheet_name, cell_range = range_str.split('!')
#                 if sheet_name in self.worksheets_dict:
#                     self.worksheets_dict[sheet_name].batch_update([{
#                         'range': cell_range,
#                         'values': values
#                     }])
    
#     def batch_update(self, body: Dict[str, Any]):
#         """Alias for values_batch_update."""
#         self.values_batch_update(body)
