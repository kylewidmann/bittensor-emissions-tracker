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
    """Ensure worksheet header row matches expected schema.

    When the header row differs from the expected schema, existing data rows
    are remapped so each cell stays associated with the correct column name.
    New columns get an empty-string default.
    """
    try:
        existing_headers = worksheet.row_values(1)
        if existing_headers == expected_headers:
            return

        all_values = worksheet.get_all_values()
        data_rows = all_values[1:] if len(all_values) > 1 else []

        if data_rows and existing_headers:
            col_map = {name: idx for idx, name in enumerate(existing_headers) if name}

            migrated_rows = []
            for row in data_rows:
                new_row = []
                for header in expected_headers:
                    old_idx = col_map.get(header)
                    if old_idx is not None and old_idx < len(row):
                        new_row.append(row[old_idx])
                    else:
                        new_row.append("")
                migrated_rows.append(new_row)

            worksheet.clear()
            worksheet.update("A1", [expected_headers] + migrated_rows)
        else:
            worksheet.update("A1", [expected_headers])

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
        result = chr(ord("A") + (idx % 26)) + result
        idx //= 26
    return result


def col_letter_to_idx(letters: str) -> int:
    """Convert Excel column letters to 0-indexed column index.

    Args:
        letters: Column letters like 'A', 'B', 'Z', 'AA', 'AB', etc.

    Returns:
        0-indexed column index (A=0, B=1, Z=25, AA=26, etc.)
    """
    result = 0
    for ch in letters.upper():
        if not ch.isalpha():
            continue
        result = result * 26 + (ord(ch) - ord("A") + 1)
    return result - 1  # Convert to 0-indexed
