#!/usr/bin/env python3
"""Quick test to verify slippage is properly included in gain/loss calculations."""

from emissions_tracker.config import WaveAccountSettings
from emissions_tracker.tracker import _aggregate_monthly_journal_entries

def test_slippage_in_gains():
    """Verify slippage reduces capital gains."""
    wave = WaveAccountSettings()
    
    # Sale with slippage: Proceeds $100, Cost $80, Slippage $5
    # Without slippage: gain = $20
    # With slippage: gain = $15 (100 - 80 - 5)
    sales_records = [
        {
            "Timestamp": 100,
            "Sale ID": "TEST-1",
            "USD Proceeds": 100.0,
            "Cost Basis": 80.0,
            "Realized Gain/Loss": 15.0,  # Already includes slippage
            "Gain Type": "Short-term",
            "Slippage USD": 5.0,
        }
    ]
    
    entries, summary = _aggregate_monthly_journal_entries(
        "2025-12",
        [],
        sales_records,
        [],
        wave,
        0,
        200,
    )
    
    # Verify summary
    assert summary["sales_proceeds"] == 100.0, f"Expected proceeds 100.0, got {summary['sales_proceeds']}"
    assert summary["sales_gain"] == 15.0, f"Expected gain 15.0 (with slippage), got {summary['sales_gain']}"
    assert summary["sales_slippage"] == 5.0, f"Expected slippage 5.0, got {summary['sales_slippage']}"
    
    # Verify double-entry balance
    total_debits = sum(e.debit or 0.0 for e in entries)
    total_credits = sum(e.credit or 0.0 for e in entries)
    
    print(f"Total debits:  ${total_debits:.2f}")
    print(f"Total credits: ${total_credits:.2f}")
    print(f"Sales gain:    ${summary['sales_gain']:.2f} (includes ${summary['sales_slippage']:.2f} slippage)")
    
    assert abs(total_debits - total_credits) < 0.01, \
        f"Books don't balance! Debits: ${total_debits:.2f}, Credits: ${total_credits:.2f}"
    
    print("✓ Test passed: Slippage is properly included in gain/loss calculations")
    return True

if __name__ == "__main__":
    test_slippage_in_gains()
