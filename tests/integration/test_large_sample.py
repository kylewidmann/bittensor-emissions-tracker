"""
Integration tests for large November 2025 dataset.

This test suite validates the tracker's ability to process a full month
of real data with 22 contract income events and 7 brokerage transfers.

Data Period: November 1-30, 2025
Source: tests/data/large_sample/
"""
import json
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from emissions_tracker.tracker import BittensorEmissionTracker
from emissions_tracker.models import SourceType
from emissions_tracker.clients.wallet import WalletClientInterface
from emissions_tracker.clients.price import PriceClient
from tests.fixtures.mock_sheets import create_mock_sheets_for_tracker


# Test data location
DATA_DIR = Path(__file__).parent.parent / "data" / "large_sample"

# Expected values calculated from raw API responses
EXPECTED_CONTRACT_INCOME_COUNT = 20  # DELEGATE events with is_transfer=True to smart contract
EXPECTED_CONTRACT_INCOME_ALPHA = 1047.12  # Sum of 20 DELEGATE events
EXPECTED_SALES_COUNT = 7  # UNDELEGATE events with is_transfer=None (user-initiated)
EXPECTED_SALES_ALPHA = 442.65  # Total ALPHA sold in user-initiated UNDELEGATEs
EXPECTED_SALES_TAO = 37.2672  # Total TAO received from sales
EXPECTED_EXPENSE_COUNT = 2  # UNDELEGATE events with is_transfer=True to non-smart-contract
EXPECTED_EXPENSE_ALPHA = 19.0  # 18 + 1 ALPHA direct transfers (no TAO involved)
EXPECTED_CONTRACT_INCOME_TAO = 89.2879
EXPECTED_CONTRACT_INCOME_USD = 30499.84  # USD from 20 DELEGATE events only
EXPECTED_BROKERAGE_TRANSFER_COUNT = 7
EXPECTED_BROKERAGE_TRANSFER_TAO = 37.2218
EXPECTED_BROKERAGE_TRANSFER_FEES = 0.000096755

# Expense transfer address (payments to another entity)
EXPENSE_TRANSFER_ADDRESS = "5GpHCAdL1ooxwXfii2bDEwBQJBGN3cWRa6N928KCc6hVWKct"

# Test period
NOVEMBER_START = 1761969600  # 2025-11-01 00:00:00 UTC
NOVEMBER_END = 1764565200    # 2025-12-01 00:00:00 UTC
MOCK_CURRENT_TIME = NOVEMBER_END

# Key addresses
COLDKEY = "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2"
HOTKEY = "5Dt7HZ7Zpw4DppPxFM7Ke3Cm7sDAWhsZXmM5ZAmE7dSVJbcQ"
SMART_CONTRACT = "5F6D1yTyQDwqR8Hjawq733WSnZVpH3X3W2aQhAWyCZq47nrf"
BROKERAGE = "5Dw6RQTpoiks2hTA8BUMpQHeurJLPMEYDBokYhBuUG8Gef9J"


def load_json_with_comments(filepath):
    """Load JSON file, stripping // comments."""
    with open(filepath) as f:
        lines = []
        for line in f:
            # Remove // comments
            if '//' in line:
                line = line[:line.index('//')]
            lines.append(line)
        return json.loads(''.join(lines))


@pytest.fixture(scope="module", autouse=True)
def mock_time():
    """Mock time.time() to return Dec 1, 2025 (after all November events)."""
    with patch('time.time', return_value=MOCK_CURRENT_TIME):
        yield


@pytest.fixture
def contract_income_data():
    """Load contract income data from JSON file."""
    return load_json_with_comments(DATA_DIR / "smart_contract_income.json")


@pytest.fixture
def stake_events_data():
    """Load all stake events (DELEGATE and UNDELEGATE) from JSON file."""
    return load_json_with_comments(DATA_DIR / "stake_events.json")


@pytest.fixture
def transfer_events():
    """Load transfer events from JSON file."""
    return load_json_with_comments(DATA_DIR / "transfers_to_brokerage.json")


@pytest.fixture
def mock_wallet_client(stake_events_data, transfer_events):
    """Mock wallet client that returns data from JSON files."""
    client = Mock(spec=WalletClientInterface)
    
    # Convert stake events (both DELEGATE and UNDELEGATE) to match client format
    delegations = []
    for event in stake_events_data['data']:
        # For UNDELEGATE with is_transfer=True to non-smart-contract (expenses):
        # - These are direct ALPHA transfers with NO TAO involved
        # - The 'amount' field is just the value in TAO equivalent for accounting
        # - Should NOT set tao_amount (no actual TAO received)
        is_expense = (
            event['action'] == 'UNDELEGATE' 
            and event.get('is_transfer') == True
            and event.get('transfer_address')
            # Could add: and transfer_address != SMART_CONTRACT_ADDRESS
        )
        
        d = {
            'timestamp': int(datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00')).timestamp()),
            'action': event['action'],
            'alpha': int(event['alpha']) / 1e9,  # RAO to ALPHA
            'tao_amount': 0.0 if is_expense else int(event['amount']) / 1e9,  # Direct ALPHA transfers have no TAO
            'usd': float(event['usd']),
            'alpha_price_in_usd': float(event['alpha_price_in_usd']) if event.get('alpha_price_in_usd') else None,
            'alpha_price_in_tao': float(event['alpha_price_in_tao']) if event.get('alpha_price_in_tao') else None,
            'slippage': float(event.get('slippage')) if event.get('slippage') is not None else 0.0,
            'block_number': event['block_number'],
            'extrinsic_id': event['extrinsic_id'],
            'is_transfer': event.get('is_transfer'),
            'transfer_address': event.get('transfer_address', {}).get('ss58') if event.get('transfer_address') else None,
            'fee': int(event.get('fee', 0)) / 1e9
        }
        delegations.append(d)
    client.get_delegations = Mock(return_value=delegations)
    
    # Convert transfer events to match client format
    transfers = []
    for t in transfer_events['data']:
        transfer = t.copy()
        transfer['timestamp'] = int(datetime.fromisoformat(t['timestamp'].replace('Z', '+00:00')).timestamp())
        transfer['amount'] = int(t['amount']) / 1e9
        transfer['fee'] = int(t['fee']) / 1e9
        transfer['to'] = t['to']['ss58'] if isinstance(t.get('to'), dict) else t.get('to')
        transfer['from'] = t['from']['ss58'] if isinstance(t.get('from'), dict) else t.get('from')
        transfers.append(transfer)
    client.get_transfers = Mock(return_value=transfers)
    
    client.get_account_history = Mock(return_value=[])
    
    return client


@pytest.fixture
def mock_price_client():
    """Mock price client."""
    client = Mock(spec=PriceClient)
    # Return a reasonable TAO price
    client.get_price_at_timestamp = Mock(return_value=390.0)  # $390 per TAO
    return client


@pytest.fixture
def mock_sheets():
    """Create mock Google Sheets using shared infrastructure."""
    return create_mock_sheets_for_tracker()


@pytest.fixture
def tracker(mock_sheets, mock_wallet_client, mock_price_client):
    """Create tracker instance for testing."""
    spreadsheet, income_sheet, tao_lots_sheet, sales_sheet, expenses_sheet, transfers_sheet, journal_sheet = mock_sheets
    
    # Create tracker instance without calling __init__
    tracker = BittensorEmissionTracker.__new__(BittensorEmissionTracker)
    
    # Set required attributes
    tracker.config = SimpleNamespace(lot_strategy="FIFO", lookback_days=31)
    tracker.wave_config = SimpleNamespace(
        alpha_asset_account="Alpha Holdings",
        tao_asset_account="TAO Holdings",
        contract_income_account="Contractor Income - Alpha",
        staking_income_account="Staking Income - Alpha",
        mining_income_account="Mining Income - Alpha",
        transfer_proceeds_account="Exchange Clearing - Kraken",
        blockchain_fee_account="Blockchain Fees",
        short_term_gain_account="Short-term Capital Gains",
        short_term_loss_account="Short-term Capital Gains",
        long_term_gain_account="Long-term Capital Gains",
        long_term_loss_account="Long-term Capital Gains"
    )
    tracker.price_client = mock_price_client
    tracker.wallet_client = mock_wallet_client
    
    tracker.label = "Large Sample Test"
    tracker.tracking_hotkey = HOTKEY
    tracker.coldkey = COLDKEY
    tracker.wallet_address = COLDKEY
    tracker.brokerage_address = BROKERAGE
    tracker.smart_contract_address = SMART_CONTRACT
    tracker.sheet_id = "test_sheet_id"
    tracker.income_source = SourceType.CONTRACT
    tracker.subnet_id = 64
    
    # Initialize counters
    tracker.alpha_lot_counter = 1
    tracker.sale_counter = 1
    tracker.expense_counter = 1
    tracker.tao_lot_counter = 1
    tracker.transfer_counter = 1
    
    # Initialize timestamps - start before November to capture all events
    tracker.last_contract_income_timestamp = NOVEMBER_START - 1
    tracker.last_staking_income_timestamp = NOVEMBER_START - 1
    tracker.last_income_timestamp = NOVEMBER_START - 1
    tracker.last_sale_timestamp = NOVEMBER_START - 1
    tracker.last_expense_timestamp = NOVEMBER_START - 1
    tracker.last_transfer_timestamp = NOVEMBER_START - 1
    
    # Initialize data structures
    tracker.contract_income = []
    tracker.staking_income = []
    tracker.alpha_lots = []
    tracker.sales = []
    tracker.tao_lots = []
    tracker.transfers = []
    
    # Set the sheet references
    tracker.sheet = spreadsheet
    tracker.income_sheet = income_sheet
    tracker.tao_lots_sheet = tao_lots_sheet
    tracker.sales_sheet = sales_sheet
    tracker.expenses_sheet = expenses_sheet
    tracker.transfers_sheet = transfers_sheet
    tracker.journal_sheet = journal_sheet
    
    return tracker


class TestContractIncome:
    """Test smart contract income (automatic ALPHA→TAO swaps)."""
    
    def test_contract_income_count(self, tracker):
        """Verify correct number of contract income events detected."""
        income = tracker.process_contract_income()
        assert len(income) == EXPECTED_CONTRACT_INCOME_COUNT
    
    def test_contract_income_total_alpha(self, tracker):
        """Verify total ALPHA received from contract matches expected."""
        new_lots = tracker.process_contract_income()
        total_alpha = sum(lot.alpha_quantity for lot in new_lots)
        assert total_alpha == pytest.approx(EXPECTED_CONTRACT_INCOME_ALPHA, rel=0.01)
    
    def test_contract_income_total_usd(self, tracker):
        """Verify total USD value of contract income."""
        new_lots = tracker.process_contract_income()
        total_usd = sum(lot.usd_fmv for lot in new_lots)
        assert total_usd == pytest.approx(EXPECTED_CONTRACT_INCOME_USD, rel=0.01)
    
    def test_contract_income_chronological_order(self, tracker):
        """Verify contract income events are in reverse chronological order (newest first)."""
        new_lots = tracker.process_contract_income()
        timestamps = [lot.timestamp for lot in new_lots]
        # Tracker returns lots in reverse chronological order (newest first)
        assert timestamps == sorted(timestamps, reverse=True)
    
    def test_contract_income_all_have_transfer_address(self, tracker):
        """Verify all contract income events have transfer address (smart contract)."""
        new_lots = tracker.process_contract_income()
        for lot in new_lots:
            assert lot.transfer_address == SMART_CONTRACT


class TestBrokerageTransfers:
    """Test transfers to Kraken brokerage."""
    
    def test_brokerage_transfer_count(self, tracker):
        """Verify correct number of brokerage transfers."""
        tracker.process_contract_income()
        tracker.process_sales()
        transfers = tracker.process_transfers()
        assert len(transfers) == EXPECTED_BROKERAGE_TRANSFER_COUNT
    
    def test_brokerage_transfer_total_amount(self, tracker):
        """Verify total TAO transferred to brokerage."""
        tracker.process_contract_income()
        tracker.process_sales()
        transfers = tracker.process_transfers()
        total_tao = sum(xfer.tao_amount for xfer in transfers)
        assert total_tao == pytest.approx(EXPECTED_BROKERAGE_TRANSFER_TAO, rel=0.01)
    
    def test_brokerage_transfer_total_fees(self, tracker):
        """Verify total fees paid on transfers."""
        tracker.process_contract_income()
        tracker.process_sales()
        transfers = tracker.process_transfers()
        total_fees = sum(xfer.fee_tao for xfer in transfers)
        assert total_fees == pytest.approx(EXPECTED_BROKERAGE_TRANSFER_FEES, rel=0.01)
    
    def test_all_transfers_have_fees(self, tracker):
        """Verify all transfers have fee tracking."""
        records = tracker.transfers_sheet.get_all_records()
        for record in records:
            assert "Fee TAO" in record
            assert record.get("Fee TAO", 0) > 0
    
    def test_transfers_consume_tao_lots(self, tracker):
        """Verify transfers consume TAO lots correctly."""
        records = tracker.transfers_sheet.get_all_records()
        for record in records:
            consumed_lots = record.get("Consumed TAO Lots", "")
            assert consumed_lots, f"Transfer {record.get('Transfer ID')} has no consumed lots"
    
    def test_transfer_chronological_order(self, tracker):
        """Verify transfers are in chronological order."""
        records = tracker.transfers_sheet.get_all_records()
        timestamps = [r.get("Timestamp", 0) for r in records]
        assert timestamps == sorted(timestamps)
    
    def test_total_outflow_includes_fees(self, tracker):
        """Verify total outflow = amount + fees."""
        records = tracker.transfers_sheet.get_all_records()
        for record in records:
            amount = record.get("TAO Amount", 0)
            fee = record.get("Fee TAO", 0)
            total_outflow = record.get("Total Outflow TAO", 0)
            assert total_outflow == pytest.approx(amount + fee, rel=1e-9)


class TestSalesCalculations:
    """Test ALPHA → TAO sales cost basis and gain/loss calculations."""
    
    def test_sales_cost_basis_from_consumed_lots(self, tracker):
        """Verify sales cost basis equals sum of consumed ALPHA lot bases."""
        tracker.process_contract_income()
        sales = tracker.process_sales()
        
        for sale in sales:
            # Calculate expected cost basis from consumed lots
            expected_cost_basis = sum(lot.cost_basis_consumed for lot in sale.consumed_lots)
            assert sale.cost_basis == pytest.approx(expected_cost_basis, abs=0.01)
            
            # Verify consumed lots match ALPHA disposed
            total_alpha_consumed = sum(lot.alpha_consumed for lot in sale.consumed_lots)
            assert total_alpha_consumed == pytest.approx(sale.alpha_disposed, abs=0.0001)
    
    def test_sales_gain_calculation_formula(self, tracker):
        """Verify sales realized gain/loss = proceeds - cost_basis - fees."""
        tracker.process_contract_income()
        sales = tracker.process_sales()
        
        for sale in sales:
            # Calculate expected gain/loss
            expected_gain_loss = sale.usd_proceeds - sale.cost_basis - sale.network_fee_usd
            assert sale.realized_gain_loss == pytest.approx(expected_gain_loss, abs=0.01)
    
    def test_sales_fifo_lot_consumption(self, tracker):
        """Verify sales consume ALPHA lots in FIFO order."""
        income_lots = tracker.process_contract_income()
        sales = tracker.process_sales()
        
        # First sale should consume from oldest income lot(s) by timestamp
        if sales and income_lots:
            first_sale = sales[0]
            first_consumed_lot = first_sale.consumed_lots[0]
            
            # Find the oldest income lot by timestamp
            oldest_lot = min(income_lots, key=lambda lot: lot.timestamp)
            
            # First consumed lot should be from the oldest income lot
            assert first_consumed_lot.acquisition_timestamp == oldest_lot.timestamp
            
            # Verify acquisition timestamps are in ascending order within consumed lots
            for sale in sales:
                acquisition_times = [lot.acquisition_timestamp for lot in sale.consumed_lots]
                assert acquisition_times == sorted(acquisition_times), "Lots not consumed in FIFO order"
    
    def test_sales_proceeds_calculation(self, tracker):
        """Verify sales USD proceeds = TAO received × TAO price."""
        tracker.process_contract_income()
        sales = tracker.process_sales()
        
        for sale in sales:
            expected_proceeds = sale.tao_received * sale.tao_price_usd
            assert sale.usd_proceeds == pytest.approx(expected_proceeds, abs=0.01)
    
    def test_sales_create_tao_lots_with_correct_basis(self, tracker):
        """Verify TAO lots created from sales have proceeds as basis."""
        tracker.process_contract_income()
        sales = tracker.process_sales()
        
        for sale in sales:
            # Find the TAO lot created by this sale
            tao_lots = tracker.tao_lots_sheet.get_all_records()
            created_lot = next((lot for lot in tao_lots if lot.get("TAO Lot ID") == sale.created_tao_lot_id), None)
            assert created_lot is not None
            
            # TAO lot basis should equal sale proceeds (FMV at receipt)
            assert created_lot.get("USD Basis") == pytest.approx(sale.usd_proceeds, abs=0.01)
            assert created_lot.get("TAO Quantity") == pytest.approx(sale.tao_received, abs=0.0001)


class TestDataIntegrity:
    """Test data integrity and consistency."""
    
    def test_no_duplicate_contract_income(self, tracker):
        """Verify no duplicate contract income events."""
        records = tracker.income_sheet.get_all_records()
        contract_records = [r for r in records if r.get("Source Type") == "Contract"]
        extrinsic_ids = [r.get("Extrinsic ID") for r in contract_records]
        assert len(extrinsic_ids) == len(set(extrinsic_ids))
    
    def test_no_duplicate_transfers(self, tracker):
        """Verify no duplicate transfer events."""
        records = tracker.transfers_sheet.get_all_records()
        tx_hashes = [r.get("Transaction Hash") for r in records]
        assert len(tx_hashes) == len(set(tx_hashes))
    
    def test_contract_income_within_period(self, tracker):
        """Verify all contract income is within November 2025."""
        records = tracker.income_sheet.get_all_records()
        contract_records = [r for r in records if r.get("Source Type") == "Contract"]
        for record in contract_records:
            ts = record.get("Timestamp", 0)
            assert NOVEMBER_START <= ts < NOVEMBER_END
    
    def test_transfers_within_period(self, tracker):
        """Verify all transfers are within November 2025."""
        records = tracker.transfers_sheet.get_all_records()
        for record in records:
            ts = record.get("Timestamp", 0)
            assert NOVEMBER_START <= ts < NOVEMBER_END
    
    def test_all_tao_lots_have_basis(self, tracker):
        """Verify all TAO lots have cost basis."""
        tao_lots = tracker.tao_lots_sheet.get_all_records()
        for lot in tao_lots:
            assert lot.get("USD Basis", 0) > 0
    
    def test_all_income_has_usd_value(self, tracker):
        """Verify all income has USD value."""
        records = tracker.income_sheet.get_all_records()
        for record in records:
            assert record.get("USD FMV", 0) > 0


class TestTaoLotManagement:
    """Tests for TAO lot creation and consumption."""
    
    def test_tao_lots_balance_matches_sales_minus_transfers(self, tracker):
        """
        REGRESSION TEST: Verify TAO lot remaining balance equals sales minus transfers.
        
        This test would have caught the bug where expense processing incorrectly
        created TAO lots for direct ALPHA transfers, creating phantom TAO.
        
        Critical validation:
        - TAO lots created = TAO from sales only (not from expenses)
        - TAO lots consumed = TAO transferred + fees
        - Remaining balance = created - consumed
        
        If this test fails, it indicates:
        - Phantom TAO lots being created (e.g., from direct ALPHA transfers)
        - Incorrect TAO lot consumption
        - TAO lot accounting errors
        """
        # Process all transactions
        tracker.process_contract_income()
        tracker.process_sales()
        tracker.process_expenses()  # Should NOT create TAO lots
        tracker.process_transfers()
        
        # Get TAO lots
        tao_lots = tracker.tao_lots_sheet.get_all_records()
        
        # Calculate total TAO created from lots
        total_tao_created = sum(lot.get("TAO Quantity", 0) for lot in tao_lots)
        
        # Calculate total TAO remaining in lots
        total_tao_remaining = sum(lot.get("TAO Remaining", 0) for lot in tao_lots)
        
        # Calculate expected values from raw data
        # TAO created should equal TAO from sales ONLY (7 sales in test data)
        expected_tao_created = EXPECTED_SALES_TAO  # 37.2672 TAO from sales
        
        # TAO consumed should equal transfers + fees
        expected_tao_consumed = EXPECTED_BROKERAGE_TRANSFER_TAO + EXPECTED_BROKERAGE_TRANSFER_FEES
        
        # Expected remaining
        expected_tao_remaining = expected_tao_created - expected_tao_consumed
        
        # Verify TAO created matches sales (would catch phantom TAO lots)
        assert total_tao_created == pytest.approx(expected_tao_created, rel=0.01), \
            f"TAO lots created ({total_tao_created:.6f}) doesn't match sales ({expected_tao_created:.6f}). " \
            f"This may indicate phantom TAO lots being created (e.g., from expenses)."
        
        # Verify remaining balance is correct
        # Note: This may be negative if test data has more transfers than sales
        # (indicating transfers consumed pre-existing TAO not tracked by the system)
        assert total_tao_remaining == pytest.approx(expected_tao_remaining, rel=0.01), \
            f"TAO remaining ({total_tao_remaining:.6f}) doesn't match expected ({expected_tao_remaining:.6f}). " \
            f"Created: {total_tao_created:.6f}, Consumed: {expected_tao_consumed:.6f}"
        
        # Verify no TAO lots have "EXP-" source (expenses should not create TAO lots)
        expense_tao_lots = [lot for lot in tao_lots if "EXP-" in str(lot.get("Source Sale ID", ""))]
        assert len(expense_tao_lots) == 0, \
            f"Found {len(expense_tao_lots)} TAO lots from expenses. " \
            f"Expenses are direct ALPHA transfers and should NOT create TAO lots!"
    
    def test_tao_lots_created_from_sales(self, tracker):
        """Verify TAO lots are created from ALPHA sales (UNDELEGATEs)."""
        # Process contract income first to create ALPHA lots
        tracker.process_contract_income()
        # Process sales to create TAO lots
        sales = tracker.process_sales()
        # Should have sales creating TAO lots
        assert len(sales) == EXPECTED_SALES_COUNT
    
    def test_tao_lots_consumed_by_transfers(self, tracker):
        """Verify TAO lots are consumed by transfers."""
        tracker.process_contract_income()
        tracker.process_sales()
        tracker.process_transfers()
        # Read TAO lots from sheet to check consumption
        tao_lots = tracker.tao_lots_sheet.get_all_records()
        consumed_lots = [lot for lot in tao_lots if lot.get("TAO Remaining", 0) < lot.get("TAO Quantity", 0)]
        # At least some lots should be partially or fully consumed
        assert len(consumed_lots) > 0
    
    def test_tao_lot_fifo_ordering(self, tracker):
        """Verify TAO lots are consumed in FIFO order."""
        tao_lots = tracker.tao_lots_sheet.get_all_records()
        # First lots created should be consumed first
        for i, lot in enumerate(tao_lots[:-1]):  # All but last
            if lot.get("TAO Remaining", 0) > 0:
                # If this lot has remaining, all later lots should be unconsumed
                for later_lot in tao_lots[i+1:]:
                    assert later_lot.get("TAO Remaining", 0) == later_lot.get("TAO Quantity", 0)
                break


class TestComprehensiveAccounting:
    """Test comprehensive accounting across the full month."""
    
    def test_total_income_value(self, tracker):
        """Verify total income value from contract income."""
        new_lots = tracker.process_contract_income()
        total_usd = sum(lot.usd_fmv for lot in new_lots)
        # Should match expected contract income USD
        assert total_usd == pytest.approx(EXPECTED_CONTRACT_INCOME_USD, rel=0.01)
    
    def test_balance_sheet_integrity(self, tracker):
        """Verify TAO balance integrity: income - transfers = remaining lots."""
        # Total TAO received from swaps
        tao_lots = tracker.tao_lots_sheet.get_all_records()
        total_tao_received = sum(lot.get("TAO Quantity", 0) for lot in tao_lots)
        
        # Total TAO transferred out
        transfers = tracker.transfers_sheet.get_all_records()
        total_tao_sent = sum(xfer.get("Total Outflow TAO", 0) for xfer in transfers)
        
        # Total TAO remaining in lots
        total_tao_remaining = sum(lot.get("TAO Remaining", 0) for lot in tao_lots)
        
        # Balance equation: received - sent = remaining
        assert total_tao_received - total_tao_sent == pytest.approx(total_tao_remaining, abs=0.01)
    
    def test_cost_basis_tracking(self, tracker):
        """Verify cost basis is properly tracked through transfers."""
        transfers = tracker.transfers_sheet.get_all_records()
        for xfer in transfers:
            cost_basis = xfer.get("Cost Basis", 0)
            amount = xfer.get("TAO Amount", 0)
            # Cost basis should be reasonable (between $10-$50 per TAO in Nov 2025)
            if amount > 0:
                usd_per_tao = cost_basis / amount
                assert 10 < usd_per_tao < 50, f"Unreasonable cost basis: ${usd_per_tao:.2f}/TAO"
    
    def test_realized_gains_on_transfers(self, tracker):
        """Verify realized gains/losses are calculated on transfers."""
        transfers = tracker.transfers_sheet.get_all_records()
        for xfer in transfers:
            # All transfers should have realized gain/loss calculated
            assert "Realized Gain/Loss" in xfer
            # Should have gain type (short/long term)
            assert xfer.get("Gain Type") in ["Short-term", "Long-term", ""]


class TestExpenseProcessing:
    """Test ALPHA → TAO expense processing (payments to other entities)."""
    
    def test_expense_count(self, tracker):
        """Verify correct number of expenses are processed."""
        tracker.process_contract_income()
        expenses = tracker.process_expenses()
        assert len(expenses) == EXPECTED_EXPENSE_COUNT
    
    def test_expense_alpha_total(self, tracker):
        """Verify total ALPHA disposed in expenses."""
        tracker.process_contract_income()
        expenses = tracker.process_expenses()
        total_alpha = sum(exp.alpha_disposed for exp in expenses)
        assert total_alpha == pytest.approx(EXPECTED_EXPENSE_ALPHA, rel=0.01)
    
    def test_expense_tao_total(self, tracker):
        """Verify expenses are direct ALPHA transfers with no TAO."""
        tracker.process_contract_income()
        expenses = tracker.process_expenses()
        for exp in expenses:
            # Direct ALPHA transfers have no TAO
            assert exp.tao_received == 0.0
            assert exp.tao_price_usd == 0.0
            assert exp.created_tao_lot_id == ""
    
    def test_expense_transfer_addresses(self, tracker):
        """Verify expenses have correct transfer addresses."""
        tracker.process_contract_income()
        expenses = tracker.process_expenses()
        for exp in expenses:
            assert exp.transfer_address == EXPENSE_TRANSFER_ADDRESS
    
    def test_expense_category_empty(self, tracker):
        """Verify expenses start with empty category (user must fill)."""
        tracker.process_contract_income()
        expenses = tracker.process_expenses()
        for exp in expenses:
            assert exp.category == ""
    
    def test_expense_creates_tao_lots(self, tracker):
        """Verify expenses do NOT create TAO lots (direct ALPHA transfer)."""
        tracker.process_contract_income()
        tracker.process_expenses()
        tao_lots = tracker.tao_lots_sheet.get_all_records()
        expense_lot_count = sum(1 for lot in tao_lots if "EXP-" in lot.get("Source Sale ID", ""))
        assert expense_lot_count == 0  # No TAO lots should be created
    
    def test_expense_consumes_alpha_lots(self, tracker):
        """Verify expenses consume ALPHA lots via FIFO."""
        tracker.process_contract_income()
        tracker.process_expenses()
        # Check that some ALPHA lots were consumed
        alpha_lots = tracker.income_sheet.get_all_records()
        consumed_lots = [lot for lot in alpha_lots if lot.get("Alpha Remaining", 0) < lot.get("Alpha Quantity", 0)]
        assert len(consumed_lots) > 0
    
    def test_expense_realized_gains(self, tracker):
        """Verify expenses calculate realized gains/losses."""
        tracker.process_contract_income()
        expenses = tracker.process_expenses()
        for exp in expenses:
            # All expenses should have gain/loss calculated
            assert exp.realized_gain_loss is not None
            assert exp.gain_type.value in ["Short-term", "Long-term"]
    
    def test_expense_cost_basis_from_consumed_lots(self, tracker):
        """Verify expense cost basis equals sum of consumed ALPHA lot bases."""
        income_lots = tracker.process_contract_income()
        expenses = tracker.process_expenses()
        
        for exp in expenses:
            # Calculate expected cost basis from consumed lots
            expected_cost_basis = sum(lot.cost_basis_consumed for lot in exp.consumed_lots)
            assert exp.cost_basis == pytest.approx(expected_cost_basis, abs=0.01)
            
            # Verify consumed lots match ALPHA disposed
            total_alpha_consumed = sum(lot.alpha_consumed for lot in exp.consumed_lots)
            assert total_alpha_consumed == pytest.approx(exp.alpha_disposed, abs=0.0001)
    
    def test_expense_gain_calculation_formula(self, tracker):
        """Verify expense realized gain/loss = proceeds - cost_basis - fees."""
        tracker.process_contract_income()
        expenses = tracker.process_expenses()
        
        for exp in expenses:
            # Calculate expected gain/loss
            expected_gain_loss = exp.usd_proceeds - exp.cost_basis - exp.network_fee_usd
            assert exp.realized_gain_loss == pytest.approx(expected_gain_loss, abs=0.01)
    
    def test_expense_fifo_lot_consumption(self, tracker):
        """Verify expenses consume ALPHA lots in FIFO order."""
        income_lots = tracker.process_contract_income()
        expenses = tracker.process_expenses()
        
        # Expenses should occur after income, so they consume earliest lots first
        if expenses and income_lots:
            first_expense = expenses[0]
            first_consumed_lot = first_expense.consumed_lots[0]
            
            # Find the oldest income lot by timestamp
            oldest_lot = min(income_lots, key=lambda lot: lot.timestamp)
            
            # First consumed lot should be from the oldest income lot
            assert first_consumed_lot.acquisition_timestamp == oldest_lot.timestamp
            
            # Verify acquisition timestamps are in ascending order within consumed lots
            for exp in expenses:
                acquisition_times = [lot.acquisition_timestamp for lot in exp.consumed_lots]
                assert acquisition_times == sorted(acquisition_times), "Lots not consumed in FIFO order"
    
    def test_expense_proceeds_calculation(self, tracker):
        """Verify expense proceeds based on ALPHA FMV (direct transfer, no TAO)."""
        tracker.process_contract_income()
        expenses = tracker.process_expenses()
        
        for exp in expenses:
            # For direct ALPHA transfers, proceeds = FMV of ALPHA at time of transfer
            # Verify proceeds are positive and reasonable
            assert exp.usd_proceeds > 0
            # Verify no TAO involved
            assert exp.tao_received == 0.0
            assert exp.tao_price_usd == 0.0
    
    def test_uncategorized_expenses_block_journal_entries(self, tracker):
        """Verify journal entry generation fails with uncategorized expenses."""
        tracker.process_contract_income()
        tracker.process_expenses()
        
        # Try to generate journal entries - should fail
        with pytest.raises(ValueError, match="uncategorized expense"):
            tracker.generate_monthly_journal_entries("2025-11")
    
    def test_categorized_expenses_create_journal_entries(self, tracker):
        """Verify journal entries are created for categorized expenses."""
        tracker.process_contract_income()
        tracker.process_expenses()
        
        # Categorize the expenses by directly updating the data
        expense_records = tracker.expenses_sheet.get_all_records()
        assert len(expense_records) == EXPECTED_EXPENSE_COUNT
        
        # Update categories directly in the rows (column index 5 is Category, 0-indexed)
        for row in tracker.expenses_sheet.rows:
            row[5] = "Computer - Hosting"  # Column 5 is Category (0-indexed after headers)
        
        # Now generate journal entries - should succeed
        entries = tracker.generate_monthly_journal_entries("2025-11")
        
        # Verify expense-related entries exist
        expense_category_entries = [e for e in entries if e.account == "Computer - Hosting"]
        assert len(expense_category_entries) > 0, "Should have journal entries for expense category"
        
        # Verify debits to expense category
        expense_debits = sum(e.debit for e in expense_category_entries)
        assert expense_debits > 0, "Expense category should have debits"
        
        # Verify ALPHA asset credits for expense cost basis
        alpha_entries = [e for e in entries if e.account == tracker.wave_config.alpha_asset_account]
        alpha_credits = sum(e.credit for e in alpha_entries)
        assert alpha_credits > 0, "ALPHA asset should have credits from expenses"

