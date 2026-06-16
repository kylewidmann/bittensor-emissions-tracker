"""Tests for _ensure_sheet_headers data migration when schema changes."""

from emissions_tracker.utils import _ensure_sheet_headers
from tests.fixtures.mock_sheets import MockWorksheet


def _ws_with_data(headers, rows):
    """Create a MockWorksheet pre-populated with a header row and data rows."""
    ws = MockWorksheet("Test")
    ws.rows.append(list(headers))
    ws.headers = list(headers)
    for row in rows:
        ws.rows.append(list(row))
    return ws


class TestEnsureSheetHeaders:

    def test_no_change_is_noop(self):
        ws = _ws_with_data(["A", "B", "C"], [["1", "2", "3"]])
        _ensure_sheet_headers(ws, ["A", "B", "C"], "Test")
        assert ws.rows == [["A", "B", "C"], ["1", "2", "3"]]

    def test_new_column_appended(self):
        ws = _ws_with_data(["A", "B"], [["1", "2"], ["x", "y"]])
        _ensure_sheet_headers(ws, ["A", "B", "C"], "Test")
        assert ws.rows[0] == ["A", "B", "C"]
        assert ws.rows[1] == ["1", "2", ""]
        assert ws.rows[2] == ["x", "y", ""]

    def test_column_reordered(self):
        ws = _ws_with_data(["A", "B", "C"], [["1", "2", "3"]])
        _ensure_sheet_headers(ws, ["C", "A", "B"], "Test")
        assert ws.rows[0] == ["C", "A", "B"]
        assert ws.rows[1] == ["3", "1", "2"]

    def test_column_removed(self):
        ws = _ws_with_data(["A", "B", "C"], [["1", "2", "3"]])
        _ensure_sheet_headers(ws, ["A", "C"], "Test")
        assert ws.rows[0] == ["A", "C"]
        assert ws.rows[1] == ["1", "3"]

    def test_column_added_in_middle(self):
        ws = _ws_with_data(["A", "C"], [["1", "3"]])
        _ensure_sheet_headers(ws, ["A", "B", "C"], "Test")
        assert ws.rows[0] == ["A", "B", "C"]
        assert ws.rows[1] == ["1", "", "3"]

    def test_empty_sheet_just_sets_headers(self):
        ws = MockWorksheet("Test")
        ws.rows.append(["Old"])
        ws.headers = ["Old"]
        _ensure_sheet_headers(ws, ["A", "B"], "Test")
        assert ws.rows[0] == ["A", "B"]

    def test_short_rows_padded(self):
        """Rows shorter than old headers still migrate correctly."""
        ws = _ws_with_data(["A", "B", "C"], [["1"]])
        _ensure_sheet_headers(ws, ["C", "A", "B"], "Test")
        assert ws.rows[0] == ["C", "A", "B"]
        assert ws.rows[1] == ["", "1", ""]
