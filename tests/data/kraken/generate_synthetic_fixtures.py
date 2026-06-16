#!/usr/bin/env python3
"""Generate synthetic Kraken test fixtures.

Creates a fake PDF statement and transactions CSV with structurally identical
content but completely fabricated numbers, dates, and account info.
Run this once to create the fixtures; check the output files into the repo.
"""

import csv
from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

OUTPUT_DIR = Path(__file__).parent


def generate_synthetic_pdf():
    """Generate a fake Kraken monthly statement PDF for testing."""
    pdf_path = OUTPUT_DIR / "synthetic_statement_2025-07-01-2025-08-01.pdf"
    c = canvas.Canvas(str(pdf_path), pagesize=letter)
    width, height = letter
    y = height - 40

    def write_line(text, font="Helvetica", size=10):
        nonlocal y
        c.setFont(font, size)
        c.drawString(40, y, text)
        y -= 14

    # Page 1: Header + portfolio + first activity
    write_line("Statement Period: Jul 1, 2025 - Jul 31, 2025", size=11)
    write_line("All portfolio balances are recorded as of 2025-08-01 00:00:00 UTC")
    write_line("Synthetic Test Corp")
    write_line("Kraken Public ID:")
    write_line("XX00 Y00Z AAAA BB1C")
    write_line("")
    write_line("Cash Portfolio")
    write_line("Currency Symbol Wallet Amount Value (USD)")
    write_line("US Dollar USD Spot 150.5000 150.5000")
    write_line("Total 150.5000")
    write_line("")
    write_line("Crypto Portfolio")
    write_line("Currency Symbol Wallet Amount Value (USD)")
    write_line("Bittensor TAO Spot 0.01000000 3.5000")
    write_line("Total 3.5000")
    write_line("")
    write_line("Stocks Portfolio")
    write_line("Stock Symbol Wallet Amount Value (USD)")
    write_line("Total 0")
    write_line("")
    write_line("Activity")
    write_line(
        "Date (UTC) Type Instrument Wallet Amount Price (USD) Fee (USD) Value (USD)"
    )
    # Deposit
    write_line("2025-07-01 Deposit Bittensor")
    write_line("Spot / Main 10.00000000 350.00 0 3,500.0000")
    write_line("18:00:00 TAO")
    # Auto allocate pair (should be ignored)
    write_line("2025-07-01 Earn Bittensor")
    write_line("Spot / Main -10.00000000 350.00 0 -3,500.0000")
    write_line("18:00:01 Auto Allocate TAO")
    write_line("2025-07-01 Earn Bittensor")
    write_line("Earn / Liquid 10.00000000 350.00 0 3,500.0000")
    write_line("18:00:01 Auto Allocate TAO")
    write_line("Page 1 of 3")
    c.showPage()

    # Page 2: Trades
    y = height - 40
    write_line("Activity")
    write_line(
        "Date (UTC) Type Instrument Wallet Amount Price (USD) Fee (USD) Value (USD)"
    )
    # Trade 1: TAO → USD with USD fee (May-Aug style)
    write_line("2025-07-01 Trade Sell Bittensor")
    write_line("Earn / Liquid -5.00000000 350.00 0 -1,750.0000")
    write_line("18:05:00 TAO")
    write_line("2025-07-01 Trade Buy US Dollar")
    write_line("Spot / Main 1745.0000 1 5.0000 1745.0000")
    write_line("18:05:00 USD")
    # Trade 2: TAO → USD with fee on sell side (Sep+ style)
    write_line("2025-07-15 Trade Sell Bittensor")
    write_line("Earn / Liquid -3.00000000 350.00 3.5000 -1,050.0000")
    write_line("10:30:00 TAO")
    write_line("2025-07-15 Trade Buy US Dollar")
    write_line("Spot / Main 1043.5000 1 0 1043.5000")
    write_line("10:30:00 USD")
    # Trade 3: small remainder
    write_line("2025-07-15 Trade Sell Bittensor")
    write_line("Earn / Liquid -1.99000000 350.00 1.3930 -696.5000")
    write_line("10:35:00 TAO")
    write_line("2025-07-15 Trade Buy US Dollar")
    write_line("Spot / Main 692.5100 1 0 692.5100")
    write_line("10:35:00 USD")
    # Withdrawal
    write_line("2025-07-15 Withdrawal US Dollar")
    write_line("Spot / Main -3,300.0000 1 0 -3,300.0000")
    write_line("11:00:00 USD")
    # Reward
    write_line("2025-07-20 Earn Bittensor")
    write_line("Earn / Liquid 0.01000000 355.00 0.0071 0.0036")
    write_line("00:17:00 Reward TAO")
    write_line("Page 2 of 3")
    c.showPage()

    # Page 3: Disclaimer
    y = height - 40
    write_line("DISCLAIMER:")
    write_line("This is a synthetic test fixture, not a real Kraken statement.")
    write_line("Page 3 of 3")
    c.showPage()
    c.save()
    print(f"Generated {pdf_path}")


def generate_synthetic_csv():
    """Generate a fake transactions.csv for testing."""
    csv_path = OUTPUT_DIR / "synthetic_transactions.csv"
    rows = [
        # July deposit
        {
            "Date": "2025-07-01T18:00:00.000000+00:00",
            "Type": "deposit",
            "Transaction ID": "9990000000000000001",
            "Received Quantity": "10.00000000",
            "Received Currency": "tao",
            "Received Cost Basis (USD)": "0.00",
            "Received Wallet": "main_spotwallet",
            "Sent Quantity": "",
            "Sent Currency": "",
            "Sent Cost Basis (USD)": "",
            "Sent Wallet": "",
            "Fee Amount": "",
            "Fee Currency": "",
        },
        # Trade 1: TAO→USD (USD fee)
        {
            "Date": "2025-07-01T18:05:00.000000+00:00",
            "Type": "trade",
            "Transaction ID": "9990000000000000002",
            "Received Quantity": "1745.00000000",
            "Received Currency": "usd",
            "Received Cost Basis (USD)": "",
            "Received Wallet": "main_spotwallet",
            "Sent Quantity": "5.00000000",
            "Sent Currency": "tao",
            "Sent Cost Basis (USD)": "0.00",
            "Sent Wallet": "main_spotwallet",
            "Fee Amount": "5.00000000",
            "Fee Currency": "usd",
        },
        # Trade 2: TAO→USD (TAO fee)
        {
            "Date": "2025-07-15T10:30:00.000000+00:00",
            "Type": "trade",
            "Transaction ID": "9990000000000000003",
            "Received Quantity": "1043.50000000",
            "Received Currency": "usd",
            "Received Cost Basis (USD)": "",
            "Received Wallet": "main_spotwallet",
            "Sent Quantity": "3.00000000",
            "Sent Currency": "tao",
            "Sent Cost Basis (USD)": "0.00",
            "Sent Wallet": "main_spotwallet",
            "Fee Amount": "0.01000000",
            "Fee Currency": "tao",
        },
        # Trade 3: small remainder
        {
            "Date": "2025-07-15T10:35:00.000000+00:00",
            "Type": "trade",
            "Transaction ID": "9990000000000000004",
            "Received Quantity": "692.51000000",
            "Received Currency": "usd",
            "Received Cost Basis (USD)": "",
            "Received Wallet": "main_spotwallet",
            "Sent Quantity": "1.99000000",
            "Sent Currency": "tao",
            "Sent Cost Basis (USD)": "0.00",
            "Sent Wallet": "main_spotwallet",
            "Fee Amount": "0.00400000",
            "Fee Currency": "tao",
        },
        # Withdrawal
        {
            "Date": "2025-07-15T11:00:00.000000+00:00",
            "Type": "withdrawal",
            "Transaction ID": "9990000000000000005",
            "Received Quantity": "",
            "Received Currency": "",
            "Received Cost Basis (USD)": "",
            "Received Wallet": "",
            "Sent Quantity": "3300.00000000",
            "Sent Currency": "usd",
            "Sent Cost Basis (USD)": "",
            "Sent Wallet": "main_spotwallet",
            "Fee Amount": "",
            "Fee Currency": "",
        },
        # Staking reward
        {
            "Date": "2025-07-20T00:17:00.000000+00:00",
            "Type": "income",
            "Transaction ID": "9990000000000000006",
            "Received Quantity": "0.01000000",
            "Received Currency": "tao",
            "Received Cost Basis (USD)": "",
            "Received Wallet": "earn_liquid",
            "Sent Quantity": "",
            "Sent Currency": "",
            "Sent Cost Basis (USD)": "",
            "Sent Wallet": "",
            "Fee Amount": "0.00200000",
            "Fee Currency": "tao",
        },
        # August deposit (different month, should be excluded)
        {
            "Date": "2025-08-01T18:00:00.000000+00:00",
            "Type": "deposit",
            "Transaction ID": "9990000000000000010",
            "Received Quantity": "5.00000000",
            "Received Currency": "tao",
            "Received Cost Basis (USD)": "0.00",
            "Received Wallet": "main_spotwallet",
            "Sent Quantity": "",
            "Sent Currency": "",
            "Sent Cost Basis (USD)": "",
            "Sent Wallet": "",
            "Fee Amount": "",
            "Fee Currency": "",
        },
    ]

    fieldnames = [
        "Date",
        "Type",
        "Transaction ID",
        "Received Quantity",
        "Received Currency",
        "Received Cost Basis (USD)",
        "Received Wallet",
        "Sent Quantity",
        "Sent Currency",
        "Sent Cost Basis (USD)",
        "Sent Wallet",
        "Fee Amount",
        "Fee Currency",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Generated {csv_path}")


def generate_synthetic_subledger_json():
    """Generate fake sub-ledger transfer records as JSON."""
    import json

    records = [
        {
            "Transfer ID": "XFER-0001",
            "Timestamp": 1751396400,
            "Block": 100000,
            "TAO Amount": 10.0,
            "TAO Price USD": 352.00,
            "USD Proceeds": 3520.00,
            "Cost Basis": 3500.00,
            "Realized Gain/Loss": 20.00,
            "Gain Type": "Short-term",
            "Total Outflow TAO": 10.0,
            "Fee TAO": 0.0,
            "Fee Cost Basis USD": 0.0,
        },
    ]
    json_path = OUTPUT_DIR / "synthetic_subledger_transfers.json"
    with open(json_path, "w") as f:
        json.dump(records, f, indent=2)
    print(f"Generated {json_path}")


if __name__ == "__main__":
    generate_synthetic_pdf()
    generate_synthetic_csv()
    generate_synthetic_subledger_json()
