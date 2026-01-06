"""
Shared mock Google Sheets infrastructure for testing.

Provides in-memory worksheet and spreadsheet mocks that track data,
allowing tests to verify the tracker's sheet operations without actual API calls.
"""

from typing import List, Dict, Any
from unittest.mock import Mock


def column_letter_to_index(letters: str) -> int:
    """Convert Excel-style column letters to 0-based index."""
    value = 0
    for ch in letters.upper():
        if not ch.isalpha():
            continue
        value = value * 26 + (ord(ch) - ord("A") + 1)
    return value


class InMemoryWorksheet:
    """
    Mock worksheet that simulates Google Sheets behavior in memory.
    
    Tracks rows, supports batch updates, and provides get_all_records()
    that returns data as dictionaries (like gspread).
    """
    
    def __init__(self, headers: List[str]):
        """
        Initialize worksheet with headers.
        
        Args:
            headers: List of column names for the first row
        """
        self.headers = headers
        self.rows = []
        self.pending_updates = []  # Staged updates for batch operations
    
    def seed_records(self, records: List[Dict[str, Any]]):
        """
        Seed the worksheet with initial data records.
        
        Args:
            records: List of dictionaries with keys matching headers
        """
        for record in records:
            row = [record.get(header, "") for header in self.headers]
            self.rows.append(row)
    
    def get_all_records(self) -> List[Dict[str, Any]]:
        """
        Get all rows as dictionaries (like gspread's get_all_records()).
        
        Returns:
            List of dictionaries with header keys and row values
        """
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
    
    def append_row(self, row: List[Any]):
        """
        Append a single row to the worksheet.
        
        Args:
            row: List of values to append
        """
        # Pad or truncate to match header count
        padded = list(row) + [""] * max(0, len(self.headers) - len(row))
        self.rows.append(padded[:len(self.headers)])
    
    def append_rows(self, rows: List[List[Any]]):
        """
        Append multiple rows to the worksheet.
        
        Args:
            rows: List of row value lists
        """
        for row in rows:
            self.append_row(row)
    
    def update_cell(self, row_num: int, column_letters: str, value: Any):
        """
        Update a single cell (used for batch updates).
        
        Args:
            row_num: 1-based row number
            column_letters: Excel-style column letter (e.g., "A", "B", "AA")
            value: New cell value
        """
        self.pending_updates.append({
            'row_num': row_num,
            'column_letters': column_letters,
            'value': value
        })
    
    def batch_update(self, data: List[Dict[str, Any]]):
        """
        Apply batch updates to cells.
        
        Args:
            data: List of update dictionaries with 'range' and 'values' keys
        """
        for update in data:
            range_str = update['range']
            values = update['values']
            
            # Parse range like "Sheet!A2:B2" or "A2" or "A2:A2"
            if '!' in range_str:
                range_str = range_str.split('!')[1]
            
            # Parse single cell or single-cell range (A2 or A2:A2)
            # Extract start cell
            if ':' in range_str:
                start_cell = range_str.split(':')[0]
            else:
                start_cell = range_str
            
            # Parse cell address
            col_letters = ''.join(c for c in start_cell if c.isalpha())
            row_num = int(''.join(c for c in start_cell if c.isdigit()))
            col_index = column_letter_to_index(col_letters) - 1
            row_index = row_num - 2  # Skip header row, convert to 0-based
            
            if 0 <= row_index < len(self.rows) and 0 <= col_index < len(self.headers):
                self.rows[row_index][col_index] = values[0][0]
    
    def flush_pending_updates(self):
        """Apply all staged cell updates (for testing batch operations)."""
        for update in self.pending_updates:
            column_index = column_letter_to_index(update['column_letters']) - 1
            row_index = update['row_num'] - 2  # Skip header row, convert to 0-based
            
            if 0 <= row_index < len(self.rows) and 0 <= column_index < len(self.headers):
                self.rows[row_index][column_index] = update['value']
        
        self.pending_updates.clear()
    
    def sort(self, *args, **kwargs):
        """Mock sort operation (no-op for testing)."""
        pass
    
    def clear(self):
        """Clear all rows (keeps headers)."""
        self.rows = []
        self.pending_updates = []


class MockSpreadsheet:
    """
    Mock Google Spreadsheet that manages multiple worksheets.
    
    Provides worksheet() to access sheets by name and supports
    batch updates across multiple sheets.
    """
    
    def __init__(self):
        """Initialize empty spreadsheet."""
        self.worksheets_dict: Dict[str, InMemoryWorksheet] = {}
    
    def register(self, name: str, worksheet: InMemoryWorksheet):
        """
        Register a worksheet with this spreadsheet.
        
        Args:
            name: Sheet name
            worksheet: InMemoryWorksheet instance
        """
        self.worksheets_dict[name] = worksheet
    
    def worksheet(self, name: str) -> InMemoryWorksheet:
        """
        Get a worksheet by name.
        
        Args:
            name: Sheet name
            
        Returns:
            InMemoryWorksheet instance
            
        Raises:
            KeyError: If worksheet not found
        """
        if name not in self.worksheets_dict:
            raise KeyError(f"Worksheet '{name}' not found")
        return self.worksheets_dict[name]
    
    def values_batch_update(self, body: Dict[str, Any]):
        """
        Apply batch updates across multiple sheets.
        
        Args:
            body: Batch update request body with 'data' key containing updates
        """
        for update in body.get("data", []):
            range_str = update["range"]
            values = update["values"]
            
            # Parse "SheetName!A2:B2" format
            if '!' in range_str:
                sheet_name, cell_range = range_str.split('!')
                if sheet_name in self.worksheets_dict:
                    self.worksheets_dict[sheet_name].batch_update([{
                        'range': cell_range,
                        'values': values
                    }])
    
    def batch_update(self, body: Dict[str, Any]):
        """Alias for values_batch_update."""
        self.values_batch_update(body)


def create_mock_sheets_for_tracker():
    """
    Create a complete set of mock sheets for BittensorEmissionTracker.
    
    Returns:
        Tuple of (spreadsheet, income_sheet, tao_lots_sheet, sales_sheet, 
                  expenses_sheet, transfers_sheet, journal_sheet)
    """
    from emissions_tracker.models import AlphaLot, TaoLot
    
    # Create individual worksheets with proper headers
    income_sheet = InMemoryWorksheet(AlphaLot.sheet_headers())
    
    tao_lots_sheet = InMemoryWorksheet(TaoLot.sheet_headers())
    
    sales_sheet = InMemoryWorksheet([
        "Sale ID", "Date", "Timestamp", "Block", "Alpha Disposed",
        "TAO Received", "TAO Price USD", "USD Proceeds", "Cost Basis",
        "Realized Gain/Loss", "Gain Type", "TAO Expected", "TAO Slippage",
        "Slippage USD", "Slippage Ratio", "Network Fee (TAO)", "Network Fee (USD)",
        "Consumed Lots", "Created TAO Lot ID", "Extrinsic ID", "Notes"
    ])
    
    expenses_sheet = InMemoryWorksheet([
        "Expense ID", "Date", "Timestamp", "Block", "Transfer Address", "Category",
        "Alpha Disposed", "TAO Received", "TAO Price USD", "USD Proceeds", 
        "Cost Basis", "Realized Gain/Loss", "Gain Type", "TAO Expected", 
        "TAO Slippage", "Slippage USD", "Slippage Ratio",
        "Network Fee (TAO)", "Network Fee (USD)",
        "Consumed Lots", "Created TAO Lot ID", "Extrinsic ID", "Notes"
    ])
    
    transfers_sheet = InMemoryWorksheet([
        "Transfer ID", "Date", "Timestamp", "Block", "TAO Amount",
        "TAO Price USD", "USD Proceeds", "Cost Basis", "Realized Gain/Loss",
        "Gain Type", "Consumed TAO Lots", "Transaction Hash", "Extrinsic ID",
        "Notes", "Total Outflow TAO", "Fee TAO", "Fee Cost Basis USD"
    ])
    
    journal_sheet = InMemoryWorksheet([
        "Date", "Account", "Debit", "Credit", "Description", "Reference"
    ])
    
    # Create spreadsheet and register all sheets
    spreadsheet = MockSpreadsheet()
    spreadsheet.register("Income", income_sheet)
    spreadsheet.register("TAO Lots", tao_lots_sheet)
    spreadsheet.register("Sales", sales_sheet)
    spreadsheet.register("Expenses", expenses_sheet)
    spreadsheet.register("Transfers", transfers_sheet)
    spreadsheet.register("Journal Entries", journal_sheet)
    
    return (spreadsheet, income_sheet, tao_lots_sheet, sales_sheet, 
            expenses_sheet, transfers_sheet, journal_sheet)
