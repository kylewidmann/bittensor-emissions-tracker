import gspread

def initialize_sheets(sheet: gspread.Spreadsheet, sheet_configs: dict[str, list[str]]):
    
    for sheet_name, headers in sheet_configs:
        try:
            worksheet = sheet.worksheet(sheet_name)
            _ensure_sheet_headers(worksheet, headers, sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
            worksheet.append_row(headers)
            print(f"  Created sheet: {sheet_name}")

def _ensure_sheet_headers(worksheet, expected_headers, label: str):
    """Ensure worksheet header row matches expected schema."""
    try:
        existing_headers = worksheet.row_values(1)
        if existing_headers != expected_headers:
            worksheet.update('A1', [expected_headers])
            print(f"  Updated {label} headers")
    except Exception as e:
        print(f"  Warning: Could not verify {label} headers: {e}")

# Convert indices to Excel column letters (0-indexed)
def col_idx_to_letter(col: str, headers: list[str]) -> str:
    """Convert 0-indexed column index to Excel column letter."""
    try:
        idx = headers.index(col)
    except ValueError as e:
        print(f"  Warning: Could not find required columns in headers: {e}")
        return
    
    result = ""
    idx += 1  # Excel columns are 1-indexed
    while idx > 0:
        idx -= 1
        result = chr(ord('A') + (idx % 26)) + result
        idx //= 26
    return result