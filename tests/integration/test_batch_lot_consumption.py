"""
Integration test for batch lot consumption bug.

This test verifies that when processing multiple sales or transfers in a batch,
each transaction properly consumes lots sequentially instead of all seeing the
same initial lot state.

BUG: Before the fix, batch operations would read lots from sheets for each item,
seeing stale data. This caused all transactions in a batch to consume the same
lots, resulting in incorrect cost basis and negative balances.
"""

import pytest
from tests.integration.test_tracker_flow import (
    StubPriceClient,
    StubWalletClient,
    build_tracker
)


def test_multiple_sales_consume_lots_sequentially():
    """
    Test that multiple ALPHA sales in a batch consume lots correctly.
    
    BEFORE FIX: All 3 sales would try to consume from ALPHA-LOT-1 first,
    leading to incorrect lot tracking and potentially consuming more than available.
    
    AFTER FIX: Each sale consumes from the earliest available lot, properly
    updating state in memory before processing the next sale.
    """
    price_client = StubPriceClient({
        1000: 100.0,  # Sale 1
        2000: 100.0,  # Sale 2
        3000: 100.0,  # Sale 3
    })
    
    # Create 3 UNDELEGATE events happening in sequence
    delegations = [
        {
            "action": "UNDELEGATE",
            "is_transfer": None,
            "timestamp": 1000,
            "block_number": 100,
            "alpha": 10.0,
            "tao_amount": 1.0,
            "usd": 100.0,
            "slippage": 0.0,
            "extrinsic_id": "sale1",
            "fee": 0,
        },
        {
            "action": "UNDELEGATE",
            "is_transfer": None,
            "timestamp": 2000,
            "block_number": 200,
            "alpha": 10.0,
            "tao_amount": 1.0,
            "usd": 100.0,
            "slippage": 0.0,
            "extrinsic_id": "sale2",
            "fee": 0,
        },
        {
            "action": "UNDELEGATE",
            "is_transfer": None,
            "timestamp": 3000,
            "block_number": 300,
            "alpha": 10.0,
            "tao_amount": 1.0,
            "usd": 100.0,
            "slippage": 0.0,
            "extrinsic_id": "sale3",
            "fee": 0,
        },
    ]
    
    wallet_client = StubWalletClient(delegations, [])
    
    # Create 3 ALPHA lots of 10 each, with different cost bases
    alpha_records = [
        {
            "Lot ID": "ALPHA-LOT-1",
            "Date": "2025-01-01 00:00:00",
            "Timestamp": 100,
            "Block": 10,
            "Source Type": "Contract",
            "Transfer Address": "",
            "Extrinsic ID": "lot1",
            "Alpha Quantity": 10.0,
            "Alpha Remaining": 10.0,
            "USD FMV": 50.0,  # $5 per ALPHA cost basis
            "USD/Alpha": 5.0,
            "TAO Equivalent": 1.0,
            "Long Term Date": "2026-01-01",
            "Status": "Open",
            "Notes": "",
        },
        {
            "Lot ID": "ALPHA-LOT-2",
            "Date": "2025-01-02 00:00:00",
            "Timestamp": 200,
            "Block": 20,
            "Source Type": "Contract",
            "Transfer Address": "",
            "Extrinsic ID": "lot2",
            "Alpha Quantity": 10.0,
            "Alpha Remaining": 10.0,
            "USD FMV": 60.0,  # $6 per ALPHA cost basis
            "USD/Alpha": 6.0,
            "TAO Equivalent": 1.0,
            "Long Term Date": "2026-01-02",
            "Status": "Open",
            "Notes": "",
        },
        {
            "Lot ID": "ALPHA-LOT-3",
            "Date": "2025-01-03 00:00:00",
            "Timestamp": 300,
            "Block": 30,
            "Source Type": "Contract",
            "Transfer Address": "",
            "Extrinsic ID": "lot3",
            "Alpha Quantity": 10.0,
            "Alpha Remaining": 10.0,
            "USD FMV": 70.0,  # $7 per ALPHA cost basis
            "USD/Alpha": 7.0,
            "TAO Equivalent": 1.0,
            "Long Term Date": "2026-01-03",
            "Status": "Open",
            "Notes": "",
        },
    ]
    
    tracker = build_tracker(
        price_client,
        wallet_client,
        alpha_records,
        last_sale_ts=0,
        last_transfer_ts=0
    )
    
    # Process all 3 sales in one batch (use lookback to process all)
    sales = tracker.process_sales(lookback_days=365)
    
    assert len(sales) == 3, "Should have created 3 sales"
    
    # Verify each sale consumed from the correct lot with correct cost basis
    # Sale 1: 10 ALPHA from LOT-1 (basis $50)
    assert sales[0].alpha_disposed == 10.0
    assert sales[0].cost_basis == pytest.approx(50.0)
    assert sales[0].usd_proceeds == pytest.approx(100.0)
    assert sales[0].realized_gain_loss == pytest.approx(50.0)  # $100 proceeds - $50 basis
    assert any(lot.lot_id == "ALPHA-LOT-1" and lot.alpha_consumed == pytest.approx(10.0) for lot in sales[0].consumed_lots)
    
    # Sale 2: 10 ALPHA from LOT-2 (basis $60)
    assert sales[1].alpha_disposed == 10.0
    assert sales[1].cost_basis == pytest.approx(60.0)
    assert sales[1].usd_proceeds == pytest.approx(100.0)
    assert sales[1].realized_gain_loss == pytest.approx(40.0)  # $100 proceeds - $60 basis
    assert any(lot.lot_id == "ALPHA-LOT-2" and lot.alpha_consumed == pytest.approx(10.0) for lot in sales[1].consumed_lots)
    
    # Sale 3: 10 ALPHA from LOT-3 (basis $70)
    assert sales[2].alpha_disposed == 10.0
    assert sales[2].cost_basis == pytest.approx(70.0)
    assert sales[2].usd_proceeds == pytest.approx(100.0)
    assert sales[2].realized_gain_loss == pytest.approx(30.0)  # $100 proceeds - $70 basis
    assert any(lot.lot_id == "ALPHA-LOT-3" and lot.alpha_consumed == pytest.approx(10.0) for lot in sales[2].consumed_lots)
    
    # Verify sheet state: all lots should be closed
    income_records = tracker.income_sheet.get_all_records()
    assert len(income_records) == 3
    for record in income_records:
        assert record["Alpha Remaining"] == 0
        assert record["Status"] == "Closed"


def test_multiple_transfers_consume_tao_lots_sequentially():
    """
    Test that multiple TAO transfers in a batch consume TAO lots correctly.
    
    BEFORE FIX: All 3 transfers would try to consume from TAO-LOT-1 first,
    causing massive over-consumption and negative balances.
    
    AFTER FIX: Each transfer consumes from earliest available lot, updating
    state in memory before processing next transfer.
    """
    # Use recent timestamps (days ago from now)
    import time
    now = int(time.time())
    ts1 = now - (10 * 86400)  # 10 days ago
    ts2 = now - (5 * 86400)   # 5 days ago  
    ts3 = now - (1 * 86400)   # 1 day ago
    
    price_client = StubPriceClient({
        ts1: 200.0,  # Transfer 1
        ts2: 200.0,  # Transfer 2
        ts3: 200.0,  # Transfer 3
    })
    
    # Create 3 transfers to brokerage
    transfers = [
        {
            "timestamp": ts1,
            "block_number": 100,
            "from": "wallet-ss58",
            "to": "brokerage-ss58",
            "amount": 5.0,
            "transaction_hash": "0xTRANSFER1",
            "extrinsic_id": "xfer1",
            "fee": 0,
        },
        {
            "timestamp": ts2,
            "block_number": 200,
            "from": "wallet-ss58",
            "to": "brokerage-ss58",
            "amount": 5.0,
            "transaction_hash": "0xTRANSFER2",
            "extrinsic_id": "xfer2",
            "fee": 0,
        },
        {
            "timestamp": ts3,
            "block_number": 300,
            "from": "wallet-ss58",
            "to": "brokerage-ss58",
            "amount": 5.0,
            "transaction_hash": "0xTRANSFER3",
            "extrinsic_id": "xfer3",
            "fee": 0,
        },
    ]
    
    wallet_client = StubWalletClient([], transfers)
    
    # Create TAO lots from sales (these would normally come from UNDELEGATE events)
    # We'll directly populate the TAO Lots sheet
    alpha_records = []
    
    tracker = build_tracker(
        price_client,
        wallet_client,
        alpha_records,
        last_sale_ts=0,
        last_transfer_ts=0
    )
    
    # Manually add TAO lots to the sheet (simulating prior sales)
    # Use timestamps earlier than the transfers
    ts_lot1 = now - (30 * 86400)  # 30 days ago
    ts_lot2 = now - (25 * 86400)  # 25 days ago
    ts_lot3 = now - (20 * 86400)  # 20 days ago
    
    tao_lots = [
        ["TAO-LOT-1", "2025-01-01 00:00:00", ts_lot1, 10, 5.0, 5.0, 500.0, 100.0, "SALE-1", "sale1", "Open", ""],
        ["TAO-LOT-2", "2025-01-02 00:00:00", ts_lot2, 20, 5.0, 5.0, 600.0, 120.0, "SALE-2", "sale2", "Open", ""],
        ["TAO-LOT-3", "2025-01-03 00:00:00", ts_lot3, 30, 5.0, 5.0, 700.0, 140.0, "SALE-3", "sale3", "Open", ""],
    ]
    for lot in tao_lots:
        tracker.tao_lots_sheet.append_row(lot)
    
    # Process all 3 transfers in one batch (use lookback to process all)
    processed_transfers = tracker.process_transfers(lookback_days=365)
    
    assert len(processed_transfers) == 3, "Should have created 3 transfers"
    
    # Verify each transfer consumed from the correct TAO lot
    # Transfer 1: 5 TAO from LOT-1 (basis $500)
    assert processed_transfers[0].tao_amount == 5.0
    assert processed_transfers[0].cost_basis == pytest.approx(500.0)
    assert processed_transfers[0].usd_proceeds == pytest.approx(1000.0)  # 5 * $200
    assert processed_transfers[0].realized_gain_loss == pytest.approx(500.0)
    assert any(lot.lot_id == "TAO-LOT-1" and lot.alpha_consumed == pytest.approx(5.0) for lot in processed_transfers[0].consumed_tao_lots)
    
    # Transfer 2: 5 TAO from LOT-2 (basis $600)
    assert processed_transfers[1].tao_amount == 5.0
    assert processed_transfers[1].cost_basis == pytest.approx(600.0)
    assert processed_transfers[1].usd_proceeds == pytest.approx(1000.0)
    assert processed_transfers[1].realized_gain_loss == pytest.approx(400.0)
    assert any(lot.lot_id == "TAO-LOT-2" and lot.alpha_consumed == pytest.approx(5.0) for lot in processed_transfers[1].consumed_tao_lots)
    
    # Transfer 3: 5 TAO from LOT-3 (basis $700)
    assert processed_transfers[2].tao_amount == 5.0
    assert processed_transfers[2].cost_basis == pytest.approx(700.0)
    assert processed_transfers[2].usd_proceeds == pytest.approx(1000.0)
    assert processed_transfers[2].realized_gain_loss == pytest.approx(300.0)
    assert any(lot.lot_id == "TAO-LOT-3" and lot.alpha_consumed == pytest.approx(5.0) for lot in processed_transfers[2].consumed_tao_lots)
    
    # Verify TAO lot state: all should be closed
    tao_records = tracker.tao_lots_sheet.get_all_records()
    assert len(tao_records) == 3
    for record in tao_records:
        assert record["TAO Remaining"] == 0
        assert record["Status"] == "Closed"


def test_partial_lot_consumption_across_multiple_sales():
    """
    Test that partial lot consumption works correctly across multiple sales.
    
    This tests the edge case where sales consume fractions of lots.
    """
    price_client = StubPriceClient({
        1000: 100.0,
        2000: 100.0,
        3000: 100.0,
    })
    
    delegations = [
        {
            "action": "UNDELEGATE",
            "is_transfer": None,
            "timestamp": 1000,
            "block_number": 100,
            "alpha": 5.0,  # Partial consumption
            "tao_amount": 0.5,
            "usd": 50.0,
            "slippage": 0.0,
            "extrinsic_id": "sale1",
            "fee": 0,
        },
        {
            "action": "UNDELEGATE",
            "is_transfer": None,
            "timestamp": 2000,
            "block_number": 200,
            "alpha": 7.0,  # Finishes LOT-1, starts LOT-2
            "tao_amount": 0.7,
            "usd": 70.0,
            "slippage": 0.0,
            "extrinsic_id": "sale2",
            "fee": 0,
        },
        {
            "action": "UNDELEGATE",
            "is_transfer": None,
            "timestamp": 3000,
            "block_number": 300,
            "alpha": 8.0,  # Finishes LOT-2, starts LOT-3
            "tao_amount": 0.8,
            "usd": 80.0,
            "slippage": 0.0,
            "extrinsic_id": "sale3",
            "fee": 0,
        },
    ]
    
    wallet_client = StubWalletClient(delegations, [])
    
    alpha_records = [
        {
            "Lot ID": "ALPHA-LOT-1",
            "Date": "2025-01-01 00:00:00",
            "Timestamp": 100,
            "Block": 10,
            "Source Type": "Contract",
            "Transfer Address": "",
            "Extrinsic ID": "lot1",
            "Alpha Quantity": 10.0,
            "Alpha Remaining": 10.0,
            "USD FMV": 100.0,  # $10 per ALPHA
            "USD/Alpha": 10.0,
            "TAO Equivalent": 1.0,
            "Long Term Date": "2026-01-01",
            "Status": "Open",
            "Notes": "",
        },
        {
            "Lot ID": "ALPHA-LOT-2",
            "Date": "2025-01-02 00:00:00",
            "Timestamp": 200,
            "Block": 20,
            "Source Type": "Contract",
            "Transfer Address": "",
            "Extrinsic ID": "lot2",
            "Alpha Quantity": 10.0,
            "Alpha Remaining": 10.0,
            "USD FMV": 120.0,  # $12 per ALPHA
            "USD/Alpha": 12.0,
            "TAO Equivalent": 1.0,
            "Long Term Date": "2026-01-02",
            "Status": "Open",
            "Notes": "",
        },
        {
            "Lot ID": "ALPHA-LOT-3",
            "Date": "2025-01-03 00:00:00",
            "Timestamp": 300,
            "Block": 30,
            "Source Type": "Contract",
            "Transfer Address": "",
            "Extrinsic ID": "lot3",
            "Alpha Quantity": 10.0,
            "Alpha Remaining": 10.0,
            "USD FMV": 140.0,  # $14 per ALPHA
            "USD/Alpha": 14.0,
            "TAO Equivalent": 1.0,
            "Long Term Date": "2026-01-03",
            "Status": "Open",
            "Notes": "",
        },
    ]
    
    tracker = build_tracker(
        price_client,
        wallet_client,
        alpha_records,
        last_sale_ts=0,
        last_transfer_ts=0
    )
    
    sales = tracker.process_sales(lookback_days=365)
    assert len(sales) == 3
    
    # Sale 1: 5 ALPHA from LOT-1 (cost: 5 * $10 = $50)
    assert sales[0].cost_basis == pytest.approx(50.0)
    assert any(lot.lot_id == "ALPHA-LOT-1" and lot.alpha_consumed == pytest.approx(5.0) for lot in sales[0].consumed_lots)
    
    # Sale 2: 5 remaining from LOT-1 ($50) + 2 from LOT-2 (2 * $12 = $24) = $74
    assert sales[1].cost_basis == pytest.approx(74.0)
    assert any(lot.lot_id == "ALPHA-LOT-1" and lot.alpha_consumed == pytest.approx(5.0) for lot in sales[1].consumed_lots)
    assert any(lot.lot_id == "ALPHA-LOT-2" and lot.alpha_consumed == pytest.approx(2.0) for lot in sales[1].consumed_lots)
    
    # Sale 3: 8 from LOT-2 (8 * $12 = $96)
    assert sales[2].cost_basis == pytest.approx(96.0)
    assert any(lot.lot_id == "ALPHA-LOT-2" and lot.alpha_consumed == pytest.approx(8.0) for lot in sales[2].consumed_lots)
    
    # Verify final lot states
    income_records = tracker.income_sheet.get_all_records()
    lot1 = next(r for r in income_records if r["Lot ID"] == "ALPHA-LOT-1")
    lot2 = next(r for r in income_records if r["Lot ID"] == "ALPHA-LOT-2")
    lot3 = next(r for r in income_records if r["Lot ID"] == "ALPHA-LOT-3")
    
    assert lot1["Alpha Remaining"] == 0
    assert lot1["Status"] == "Closed"
    assert lot2["Alpha Remaining"] == 0
    assert lot2["Status"] == "Closed"
    assert lot3["Alpha Remaining"] == 10.0
    assert lot3["Status"] == "Open"
