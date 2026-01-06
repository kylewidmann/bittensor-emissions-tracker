import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pytest

from emissions_tracker.config import WaveAccountSettings
from emissions_tracker.tracker import _aggregate_monthly_journal_entries

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "november_sample"


def _load_records(name: str):
    path = DATA_DIR / f"{name}.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _collect_account_totals(entries):
    totals = {}
    for entry in entries:
        account = entry.account
        bucket = totals.setdefault(account, {"debit": 0.0, "credit": 0.0})
        bucket["debit"] += entry.debit or 0.0
        bucket["credit"] += entry.credit or 0.0
    return totals


@pytest.fixture
def november_context():
    wave = WaveAccountSettings()
    period_start = datetime(2025, 11, 1, tzinfo=timezone.utc)
    period_end = datetime(2025, 12, 1, tzinfo=timezone.utc)
    year_month = "2025-11"
    start_ts = int(period_start.timestamp())
    end_ts = int(period_end.timestamp())
    return wave, year_month, start_ts, end_ts


def test_november_summary_totals_match_expectations(november_context):
    wave, year_month, start_ts, end_ts = november_context
    income_records = _load_records("income")
    sales_records = _load_records("sales")
    transfer_records = _load_records("transfers")

    entries, summary = _aggregate_monthly_journal_entries(
        year_month,
        income_records,
        sales_records,
        [],  # expense_records
        transfer_records,
        wave,
        start_ts,
        end_ts,
    )

    assert summary["contract_income"] == 150.0
    assert summary["staking_income"] == 95.0
    assert summary["sales_proceeds"] == 930.0
    assert summary["sales_gain"] == 151.5  # 178 - 26.5 = 151.5 (slippage already in gain/loss)
    assert summary["sales_slippage"] == 10.0
    assert summary["sales_fees"] == 6.5
    assert summary["transfer_gain"] == 10.0
    assert summary["transfer_fees"] == 9.5

    totals = _collect_account_totals(entries)

    alpha_account = totals[wave.alpha_asset_account]
    assert math.isclose(alpha_account["debit"], 245.0, rel_tol=1e-9)
    assert math.isclose(alpha_account["credit"], 762.5, rel_tol=1e-9)

    tao_account = totals[wave.tao_asset_account]
    assert math.isclose(tao_account["debit"], 930.0, rel_tol=1e-9)  # Sales proceeds
    assert math.isclose(tao_account["credit"], 686.0, rel_tol=1e-9)  # Transfer cost basis + fees

    # Both sale fees and transfer fees now go to the same consolidated account
    fee_account = totals[wave.blockchain_fee_account]
    assert math.isclose(fee_account["debit"], 16.0, rel_tol=1e-9)  # 6.5 (sale fees) + 9.5 (transfer fees)
    assert math.isclose(fee_account["credit"], 0.0, rel_tol=1e-9)


def test_transfer_fee_column_overrides_note_metadata(november_context):
    wave, year_month, start_ts, end_ts = november_context
    transfer_records = [
        {
            "Timestamp": start_ts + 100,
            "Transfer ID": "XFER-COLUMN",
            "USD Proceeds": 150.0,
            "Cost Basis": 120.0,
            "Realized Gain/Loss": 30.0,
            "Gain Type": "Short-term",
            "Fee Cost Basis USD": 7.0,
            "Notes": "fee_cost_basis=99.0",
        }
    ]

    entries, summary = _aggregate_monthly_journal_entries(
        year_month,
        [],
        [],
        [],  # expense_records
        transfer_records,
        wave,
        start_ts,
        end_ts,
    )

    assert summary["transfer_fees"] == 7.0

    totals = _collect_account_totals(entries)
    transfer_fee_account = totals[wave.blockchain_fee_account]
    assert math.isclose(transfer_fee_account["debit"], 7.0, rel_tol=1e-9)
    assert math.isclose(transfer_fee_account["credit"], 0.0, rel_tol=1e-9)

    tao_account = totals[wave.tao_asset_account]
    assert math.isclose(tao_account["credit"], 127.0, rel_tol=1e-9)  # Transfer cost basis + fee
