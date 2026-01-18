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