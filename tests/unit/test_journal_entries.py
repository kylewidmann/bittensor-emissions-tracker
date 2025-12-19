import math

from emissions_tracker.config import WaveAccountSettings
from emissions_tracker.tracker import _aggregate_monthly_journal_entries


def _collect_totals(entries):
    totals = {}
    total_debits = 0.0
    total_credits = 0.0
    for entry in entries:
        bucket = totals.setdefault(entry.account, {"debit": 0.0, "credit": 0.0})
        debit = entry.debit or 0.0
        credit = entry.credit or 0.0
        bucket["debit"] += debit
        bucket["credit"] += credit
        total_debits += debit
        total_credits += credit
    return totals, total_debits, total_credits


def test_aggregate_monthly_journal_entries_balances_double_entry():
    year_month = "2025-11"
    start_ts = 0
    end_ts = 200
    wave = WaveAccountSettings()

    income_records = [
        {"Timestamp": 10, "Source Type": "Contract", "USD FMV": 100.0, "Lot ID": "ALPHA-1"},
        {"Timestamp": 20, "Source Type": "Staking", "USD FMV": 50.0, "Lot ID": "ALPHA-2"},
        {"Timestamp": 500, "Source Type": "Contract", "USD FMV": 999.0, "Lot ID": "OUTSIDE"},
    ]

    sales_records = [
        {
            "Timestamp": 50,
            "Sale ID": "SALE-1",
            "USD Proceeds": 200.0,
            "Cost Basis": 150.0,
            "Realized Gain/Loss": 50.0,
            "Gain Type": "Short-term",
            "Slippage USD": 5.0,
        },
        {
            "Timestamp": 60,
            "Sale ID": "SALE-2",
            "USD Proceeds": 100.0,
            "Cost Basis": 130.0,
            "Realized Gain/Loss": -30.0,
            "Gain Type": "Short-term",
        },
        {
            "Timestamp": 400,
            "Sale ID": "SALE-OUT",
            "USD Proceeds": 500.0,
            "Cost Basis": 400.0,
            "Realized Gain/Loss": 100.0,
            "Gain Type": "Short-term",
        },
    ]

    transfer_records = [
        {
            "Timestamp": 70,
            "Transfer ID": "XFER-1",
            "USD Proceeds": 90.0,
            "Cost Basis": 70.0,
            "Realized Gain/Loss": 20.0,
            "Gain Type": "Short-term",
            "Notes": "fee_cost_basis=10.0",
        },
        {
            "Timestamp": 80,
            "Transfer ID": "XFER-2",
            "USD Proceeds": 80.0,
            "Cost Basis": 100.0,
            "Realized Gain/Loss": -20.0,
            "Gain Type": "Short-term",
        },
        {
            "Timestamp": 90,
            "Transfer ID": "XFER-3",
            "USD Proceeds": 200.0,
            "Cost Basis": 180.0,
            "Realized Gain/Loss": 20.0,
            "Gain Type": "Long-term",
        },
        {
            "Timestamp": 95,
            "Transfer ID": "XFER-4",
            "USD Proceeds": 60.0,
            "Cost Basis": 90.0,
            "Realized Gain/Loss": -30.0,
            "Gain Type": "Long-term",
        },
        {
            "Timestamp": 500,
            "Transfer ID": "XFER-OUT",
            "USD Proceeds": 999.0,
            "Cost Basis": 999.0,
            "Realized Gain/Loss": 0.0,
            "Gain Type": "Short-term",
        },
    ]

    entries, summary = _aggregate_monthly_journal_entries(
        year_month,
        income_records,
        sales_records,
        transfer_records,
        wave,
        start_ts,
        end_ts,
    )

    totals, total_debits, total_credits = _collect_totals(entries)
    assert math.isclose(total_debits, total_credits, rel_tol=1e-9)

    assert math.isclose(totals[wave.alpha_asset_account]["debit"], 150.0)
    assert math.isclose(totals[wave.alpha_asset_account]["credit"], 280.0)

    assert math.isclose(totals[wave.tao_asset_account]["debit"], 300.0)
    assert math.isclose(totals[wave.tao_asset_account]["credit"], 450.0)

    assert math.isclose(totals[wave.transfer_proceeds_account]["debit"], 430.0)
    assert math.isclose(totals[wave.transfer_fee_account]["debit"], 10.0)

    assert math.isclose(totals[wave.short_term_gain_account]["credit"], 20.0)
    assert math.isclose(totals[wave.long_term_loss_account]["debit"], 10.0)
    assert math.isclose(totals.get(wave.short_term_loss_account, {"debit": 0.0}).get("debit", 0.0), 0.0)
    assert math.isclose(totals.get(wave.long_term_gain_account, {"credit": 0.0}).get("credit", 0.0), 0.0)

    assert summary["contract_income"] == 100.0
    assert summary["staking_income"] == 50.0
    assert summary["sales_proceeds"] == 300.0
    assert summary["sales_gain"] == 20.0
    assert summary["sales_slippage"] == 5.0
    assert summary["sales_fees"] == 0.0
    assert summary["transfer_gain"] == -10.0
    assert summary["transfer_fees"] == 10.0


def test_rounding_adjustment_balances_totals_with_combined_accounts():
    wave = WaveAccountSettings()
    wave.short_term_gain_account = "Short-term Capital Gains"
    wave.short_term_loss_account = "Short-term Capital Gains"

    income_records = [
        {"Timestamp": 10, "Source Type": "Contract", "USD FMV": 0.8444218515, "Lot ID": "ALPHA-1"},
    ]
    sales_records = [
        {
            "Timestamp": 20,
            "Sale ID": "SALE-1",
            "USD Proceeds": 0.7579544029,
            "Cost Basis": 0.4205715808,
            "Realized Gain/Loss": -0.2410832497,
            "Gain Type": "Short-term",
        }
    ]

    entries, _ = _aggregate_monthly_journal_entries(
        "2025-11",
        income_records,
        sales_records,
        [],
        wave,
        0,
        100,
    )

    total_debits = round(sum(e.debit for e in entries), 2)
    total_credits = round(sum(e.credit for e in entries), 2)
    assert total_debits == total_credits

    rounding_entries = [e for e in entries if "rounding adjustment" in e.description]
    assert rounding_entries, "Expected rounding adjustment note to be recorded"
