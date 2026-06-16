"""Integration tests for the Kraken reconciliation entrypoint."""

import csv
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from emissions_tracker.entrypoints.kraken import run

SYNTHETIC_PDF = (
    Path(__file__).parent.parent
    / "data"
    / "kraken"
    / "synthetic_statement_2025-07-01-2025-08-01.pdf"
)
SYNTHETIC_CSV = (
    Path(__file__).parent.parent / "data" / "kraken" / "synthetic_transactions.csv"
)
SYNTHETIC_SUBLEDGER = (
    Path(__file__).parent.parent
    / "data"
    / "kraken"
    / "synthetic_subledger_transfers.json"
)


def _mock_sheets_for_transfers(subledger_path: Path):
    """Build mock gspread objects that return synthetic transfer records."""
    with open(subledger_path) as f:
        records = json.load(f)

    mock_worksheet = MagicMock()
    mock_worksheet.get_all_records.return_value = records

    mock_sheet = MagicMock()
    mock_sheet.worksheet.return_value = mock_worksheet

    mock_client = MagicMock()
    mock_client.open_by_key.return_value = mock_sheet

    return mock_client


class TestKrakenEntrypointCSV:
    """End-to-end tests using the CSV input path."""

    def test_csv_no_sheets(self, tmp_path):
        """Run with --csv --no-sheets and verify report is printed."""
        journal_out = tmp_path / "journal.csv"

        with patch(
            "sys.argv",
            [
                "track-kraken",
                "--csv",
                str(SYNTHETIC_CSV),
                "--no-sheets",
                "--start",
                "2025-07",
                "--end",
                "2025-07",
                "--journal-csv",
                str(journal_out),
            ],
        ):
            result = run()

        assert result == 0
        assert journal_out.exists()

        with open(journal_out) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) > 0
        total_debits = sum(float(r["debit"]) for r in rows if r["debit"])
        total_credits = sum(float(r["credit"]) for r in rows if r["credit"])
        assert total_debits == pytest.approx(total_credits)

    def test_csv_with_mocked_sheets(self, tmp_path):
        """Run with --csv and mocked Google Sheets transfers."""
        journal_out = tmp_path / "journal.csv"
        mock_client = _mock_sheets_for_transfers(SYNTHETIC_SUBLEDGER)

        with patch(
            "sys.argv",
            [
                "track-kraken",
                "--csv",
                str(SYNTHETIC_CSV),
                "--start",
                "2025-07",
                "--end",
                "2025-07",
                "--journal-csv",
                str(journal_out),
            ],
        ), patch(
            "emissions_tracker.entrypoints.kraken.ServiceAccountCredentials"
        ) as mock_creds, patch(
            "emissions_tracker.entrypoints.kraken.gspread"
        ) as mock_gspread, patch.dict(
            "os.environ",
            {
                "BROKER_SS58": "5FakeTest1111111111111111111111111111",
                "VALIDATOR_SS58": "5FakeTest2222222222222222222222222222",
                "PAYOUT_COLDKEY_SS58": "5FakeTest3333333333333333333333333333",
                "SMART_CONTRACT_SS58": "5FakeTest4444444444444444444444444444",
                "TRACKER_SHEET_ID": "test-sheet-id",
                "TRACKER_GOOGLE_CREDENTIALS": "/tmp/fake-creds.json",
            },
            clear=False,
        ):
            mock_gspread.authorize.return_value = mock_client
            result = run()

        assert result == 0
        assert journal_out.exists()

        with open(journal_out) as f:
            rows = list(csv.DictReader(f))

        assert len(rows) >= 3
        accounts = [r["account"] for r in rows]
        assert "Exchange Fees - Kraken" in accounts
        assert "Exchange Clearing - Kraken" in accounts

    def test_csv_journal_has_both_sections(self, tmp_path):
        """Journal CSV contains clearing entries (fees + price diff).

        The CSV parser sets rewards value_usd=0 (no FMV in CSV), so staking
        entries (TAO Holdings / Staking Income) only appear with PDF input.
        """
        journal_out = tmp_path / "journal.csv"
        mock_client = _mock_sheets_for_transfers(SYNTHETIC_SUBLEDGER)

        with patch(
            "sys.argv",
            [
                "track-kraken",
                "--csv",
                str(SYNTHETIC_CSV),
                "--start",
                "2025-07",
                "--end",
                "2025-07",
                "--journal-csv",
                str(journal_out),
            ],
        ), patch(
            "emissions_tracker.entrypoints.kraken.ServiceAccountCredentials"
        ), patch(
            "emissions_tracker.entrypoints.kraken.gspread"
        ) as mock_gspread, patch.dict(
            "os.environ",
            {
                "BROKER_SS58": "5FakeTest1111111111111111111111111111",
                "VALIDATOR_SS58": "5FakeTest2222222222222222222222222222",
                "PAYOUT_COLDKEY_SS58": "5FakeTest3333333333333333333333333333",
                "SMART_CONTRACT_SS58": "5FakeTest4444444444444444444444444444",
                "TRACKER_SHEET_ID": "test-sheet-id",
                "TRACKER_GOOGLE_CREDENTIALS": "/tmp/fake-creds.json",
            },
            clear=False,
        ):
            mock_gspread.authorize.return_value = mock_client
            result = run()

        assert result == 0

        with open(journal_out) as f:
            rows = list(csv.DictReader(f))

        accounts = [r["account"] for r in rows]
        assert "Exchange Clearing - Kraken" in accounts
        assert "Exchange Fees - Kraken" in accounts

    def test_empty_month(self, tmp_path):
        """An empty month should produce no journal entries."""
        journal_out = tmp_path / "journal.csv"

        with patch(
            "sys.argv",
            [
                "track-kraken",
                "--csv",
                str(SYNTHETIC_CSV),
                "--no-sheets",
                "--start",
                "2025-01",
                "--end",
                "2025-01",
            ],
        ):
            result = run()

        assert result == 1


class TestKrakenEntrypointPDF:
    """End-to-end tests using the PDF input path."""

    def test_pdf_single(self, tmp_path):
        """Run with --pdf pointing to the synthetic statement."""
        journal_out = tmp_path / "journal.csv"

        with patch(
            "sys.argv",
            [
                "track-kraken",
                "--pdf",
                str(SYNTHETIC_PDF),
                "--no-sheets",
                "--journal-csv",
                str(journal_out),
            ],
        ):
            result = run()

        assert result == 0
        assert journal_out.exists()

    def test_pdf_dir(self, tmp_path):
        """Run with --pdf-dir pointing to a directory with the synthetic PDF."""
        pdf_dir = tmp_path / "statements"
        pdf_dir.mkdir()
        import shutil

        shutil.copy(
            SYNTHETIC_PDF,
            pdf_dir / "kraken_spot_account_statement_2025-07-01-2025-08-01.pdf",
        )

        journal_out = tmp_path / "journal.csv"

        with patch(
            "sys.argv",
            [
                "track-kraken",
                "--pdf-dir",
                str(pdf_dir),
                "--no-sheets",
                "--journal-csv",
                str(journal_out),
            ],
        ):
            result = run()

        assert result == 0
        assert journal_out.exists()

    def test_pdf_split_fees(self, tmp_path):
        """PDF parser correctly splits fees into cash and TAO portions."""
        journal_out = tmp_path / "journal.csv"
        mock_client = _mock_sheets_for_transfers(SYNTHETIC_SUBLEDGER)

        with patch(
            "sys.argv",
            [
                "track-kraken",
                "--pdf",
                str(SYNTHETIC_PDF),
                "--journal-csv",
                str(journal_out),
            ],
        ), patch(
            "emissions_tracker.entrypoints.kraken.ServiceAccountCredentials"
        ), patch(
            "emissions_tracker.entrypoints.kraken.gspread"
        ) as mock_gspread, patch.dict(
            "os.environ",
            {
                "BROKER_SS58": "5FakeTest1111111111111111111111111111",
                "VALIDATOR_SS58": "5FakeTest2222222222222222222222222222",
                "PAYOUT_COLDKEY_SS58": "5FakeTest3333333333333333333333333333",
                "SMART_CONTRACT_SS58": "5FakeTest4444444444444444444444444444",
                "TRACKER_SHEET_ID": "test-sheet-id",
                "TRACKER_GOOGLE_CREDENTIALS": "/tmp/fake-creds.json",
            },
            clear=False,
        ):
            mock_gspread.authorize.return_value = mock_client
            result = run()

        assert result == 0
        assert journal_out.exists()

        with open(journal_out) as f:
            rows = list(csv.DictReader(f))

        total_debits = sum(float(r["debit"]) for r in rows if r["debit"])
        total_credits = sum(float(r["credit"]) for r in rows if r["credit"])
        assert total_debits == pytest.approx(total_credits)

        accounts = set(r["account"] for r in rows)
        assert "Exchange Clearing - Kraken" in accounts
        assert "Exchange Fees - Kraken" in accounts
