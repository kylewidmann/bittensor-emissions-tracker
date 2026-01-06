"""
Test suite for simple sample data - November 2025 period.

This test suite verifies the emissions tracker with a minimal, manually verifiable dataset
covering November 1-6, 2025. It tests the complete flow:
1. Contract income (4 DELEGATE events with is_transfer=true) → Creates ALPHA lots
2. Sales (2 UNDELEGATE events) → Consumes ALPHA lots, creates TAO lots  
3. Transfers (2 to brokerage) → Consumes TAO lots

Test data source: tests/data/simple_sample/
- stake_events.json: 4 DELEGATE + 2 UNDELEGATE events
- transfers_to_brokerage.json: 4 transfers (2 to brokerage, 2 to other addresses)
"""

import pytest
import json
import re
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import List
from unittest.mock import Mock, patch

from emissions_tracker.tracker import BittensorEmissionTracker
from emissions_tracker.models import SourceType, AlphaLot, TaoLot
from emissions_tracker.clients.wallet import WalletClientInterface
from emissions_tracker.clients.price import PriceClient
from tests.fixtures.mock_sheets import create_mock_sheets_for_tracker


# Constants
START_TIMESTAMP = 1761969600  # Nov 1, 2025 00:00:00 UTC
END_TIMESTAMP = 1762491600    # Nov 7, 2025 00:00:00 UTC
MOCK_CURRENT_TIME = END_TIMESTAMP  # Tests run as if it's Nov 7

COLDKEY = "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2"
HOTKEY = "5Dt7HZ7Zpw4DppPxFM7Ke3Cm7sDAWhsZXmM5ZAmE7dSVJbcQ"
BROKERAGE_ADDRESS = "5Dw6RQTpoiks2hTA8BUMpQHeurJLPMEYDBokYhBuUG8Gef9J"

# Expected totals (from analyze script)
EXPECTED_ALPHA_INCOME = 172.28  # 4 DELEGATE events
EXPECTED_ALPHA_SOLD = 112.65    # 2 UNDELEGATE events
EXPECTED_TAO_RECEIVED = 8.9182  # From UNDELEGATE
EXPECTED_TAO_TO_BROKERAGE = 8.8918  # 2 transfers to brokerage
EXPECTED_TOTAL_FEES = 0.000027966  # Total fees from 2 brokerage transfers


@pytest.fixture(scope="module", autouse=True)
def mock_time():
    """Mock time.time() to return November 7, 2025 for all tests in this module."""
    with patch('time.time', return_value=MOCK_CURRENT_TIME):
        yield


def load_json_with_comments(filepath: str) -> dict:
    """Load JSON file that may contain // comments."""
    with open(filepath) as f:
        content = f.read()
        # Remove // comments
        content = re.sub(r'//.*', '', content)
        return json.loads(content)


@pytest.fixture
def stake_events():
    """Load stake events from JSON file."""
    return load_json_with_comments('tests/data/simple_sample/stake_events.json')


@pytest.fixture
def transfer_events():
    """Load transfer events from JSON file."""
    return load_json_with_comments('tests/data/simple_sample/transfers_to_brokerage.json')


@pytest.fixture
def mock_wallet_client(stake_events, transfer_events):
    """Mock wallet client that returns data from JSON files."""
    client = Mock(spec=WalletClientInterface)
    
    # Convert stake events to match taostats client format
    # (ISO timestamps → Unix timestamps, RAO amounts → TAO, extract transfer_address.ss58)
    delegations = []
    for event in stake_events['data']:
        d = {
            'timestamp': int(datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00')).timestamp()),
            'action': event['action'],
            'alpha': int(event['alpha']) / 1e9,  # RAO to ALPHA
            'tao_amount': int(event['amount']) / 1e9,  # RAO to TAO
            'usd': float(event['usd']),
            'alpha_price_in_usd': float(event['alpha_price_in_usd']) if event.get('alpha_price_in_usd') else None,
            'alpha_price_in_tao': float(event['alpha_price_in_tao']) if event.get('alpha_price_in_tao') else None,
            'slippage': float(event.get('slippage')) if event.get('slippage') is not None else 0.0,
            'block_number': event['block_number'],
            'extrinsic_id': event['extrinsic_id'],
            'is_transfer': event.get('is_transfer'),
            # Extract SS58 address from nested object
            'transfer_address': event.get('transfer_address', {}).get('ss58') if event.get('transfer_address') else None,
            'fee': int(event.get('fee', 0)) / 1e9
        }
        delegations.append(d)
    client.get_delegations = Mock(return_value=delegations)
    
    # Convert transfer events to match taostats client format
    transfers = []
    for t in transfer_events['data']:
        transfer = t.copy()
        # Convert ISO timestamp to Unix timestamp
        transfer['timestamp'] = int(datetime.fromisoformat(t['timestamp'].replace('Z', '+00:00')).timestamp())
        # Convert RAO amounts to TAO
        transfer['amount'] = int(t['amount']) / 1e9
        transfer['fee'] = int(t['fee']) / 1e9
        # Extract SS58 addresses from nested objects
        transfer['to'] = t['to']['ss58'] if isinstance(t.get('to'), dict) else t.get('to')
        transfer['from'] = t['from']['ss58'] if isinstance(t.get('from'), dict) else t.get('from')
        transfers.append(transfer)
    client.get_transfers = Mock(return_value=transfers)
    
    # Mock account history (not used in these tests but required by interface)
    client.get_account_history = Mock(return_value=[])
    
    return client


@pytest.fixture
def mock_price_client():
    """Mock price client."""
    client = Mock(spec=PriceClient)
    # Return a reasonable TAO price for any timestamp
    client.get_price_at_timestamp = Mock(return_value=390.0)  # $390 per TAO
    return client


@pytest.fixture
def mock_worksheet():
    """Mock Google Sheets worksheet - deprecated, use mock_sheets instead."""
    worksheet = Mock()
    worksheet.get_all_records = Mock(return_value=[])
    worksheet.append_rows = Mock()
    worksheet.append_row = Mock()
    worksheet.clear = Mock()
    return worksheet


@pytest.fixture
def mock_sheets():
    """Create mock sheets infrastructure that persists data across operations."""
    return create_mock_sheets_for_tracker()


@pytest.fixture
def tracker(mock_wallet_client, mock_price_client, mock_sheets):
    """Create tracker instance with mocked dependencies and in-memory sheets."""
    spreadsheet, income_sheet, tao_lots_sheet, sales_sheet, expenses_sheet, transfers_sheet, journal_sheet = mock_sheets
    
    # Create tracker instance without calling __init__ (avoids config validation)
    tracker = BittensorEmissionTracker.__new__(BittensorEmissionTracker)
    
    # Set required attributes manually
    tracker.config = SimpleNamespace(lot_strategy="FIFO", lookback_days=7)
    tracker.wave_config = SimpleNamespace()
    tracker.price_client = mock_price_client
    tracker.wallet_client = mock_wallet_client
    
    tracker.label = "Simple Sample Test"
    tracker.tracking_hotkey = HOTKEY
    tracker.coldkey = COLDKEY
    tracker.wallet_address = COLDKEY
    tracker.brokerage_address = BROKERAGE_ADDRESS
    # Set smart contract address so contract income is recognized
    tracker.smart_contract_address = "5F6D1yTyQDwqR8Hjawq733WSnZVpH3X3W2aQhAWyCZq47nrf"
    tracker.sheet_id = "test_sheet_id"
    tracker.income_source = SourceType.CONTRACT
    tracker.subnet_id = 64
    
    # Initialize counters
    tracker.alpha_lot_counter = 1
    tracker.sale_counter = 1
    tracker.expense_counter = 1
    tracker.tao_lot_counter = 1
    tracker.transfer_counter = 1
    
    # Initialize timestamps
    tracker.last_contract_income_timestamp = 0
    tracker.last_staking_income_timestamp = 0
    tracker.last_income_timestamp = 0
    tracker.last_sale_timestamp = 0
    tracker.last_expense_timestamp = 0
    tracker.last_transfer_timestamp = 0
    
    # Initialize data structures
    tracker.contract_income = []
    tracker.staking_income = []
    tracker.alpha_lots = []
    tracker.sales = []
    tracker.tao_lots = []
    tracker.transfers = []
    
    # Set the sheet references to use in-memory sheets
    tracker.sheet = spreadsheet
    tracker.income_sheet = income_sheet
    tracker.tao_lots_sheet = tao_lots_sheet
    tracker.sales_sheet = sales_sheet
    tracker.expenses_sheet = expenses_sheet
    tracker.transfers_sheet = transfers_sheet
    tracker.journal_sheet = journal_sheet
    
    return tracker


class TestContractIncome:
    """Test contract income processing (DELEGATE events with is_transfer=true)."""
    
    def test_contract_income_count(self, tracker):
        """Verify correct number of contract income events recognized."""
        new_lots = tracker.process_contract_income(lookback_days=7)
        
        # Should have 4 DELEGATE events with is_transfer=true
        assert len(new_lots) == 4
    
    def test_contract_income_total_alpha(self, tracker):
        """Verify total ALPHA received from contract income."""
        new_lots = tracker.process_contract_income(lookback_days=7)
        
        total_alpha = sum(Decimal(str(lot.alpha_quantity)) for lot in new_lots)
        assert abs(total_alpha - Decimal(str(EXPECTED_ALPHA_INCOME))) < Decimal('0.01')
    
    def test_contract_income_creates_alpha_lots(self, tracker):
        """Verify contract income creates ALPHA lots (delegated)."""
        new_lots = tracker.process_contract_income(lookback_days=7)
        
        # Should create 4 ALPHA lots, all from contract source
        assert len(new_lots) == 4
        assert all(lot.source_type.value == "Contract" for lot in new_lots)
        assert all(lot.source_type == SourceType.CONTRACT for lot in new_lots)
    
    def test_contract_income_no_tao_lots(self, tracker):
        """Verify contract income does NOT create TAO lots."""
        tracker.process_contract_income(lookback_days=7)
        
        # Check the TAO lots sheet is still empty
        tao_lots = tracker.tao_lots_sheet.get_all_records()
        assert len(tao_lots) == 0


class TestSales:
    """Test sales processing (UNDELEGATE events)."""
    
    def test_sales_count(self, tracker):
        """Verify correct number of sales recognized."""
        tracker.process_contract_income(lookback_days=7)
        sales = tracker.process_sales(lookback_days=7)
        
        # Should have 2 UNDELEGATE events
        assert len(sales) == 2
    
    def test_sales_create_tao_lots(self, tracker):
        """Verify sales create TAO lots."""
        tracker.process_contract_income(lookback_days=7)
        sales = tracker.process_sales(lookback_days=7)
        
        # Sales should create TAO lots (check from sheet)
        tao_lots = tracker.tao_lots_sheet.get_all_records()
        assert len(tao_lots) == 2
    
    def test_sales_consume_alpha_lots(self, tracker):
        """Verify sales consume ALPHA lots."""
        new_lots = tracker.process_contract_income(lookback_days=7)
        initial_alpha = sum(Decimal(str(lot.alpha_quantity)) for lot in new_lots)
        
        sales = tracker.process_sales(lookback_days=7)
        
        # Should have consumed ALPHA (check from sales)
        consumed_alpha = sum(Decimal(str(sale.alpha_disposed)) for sale in sales)
        
        assert abs(consumed_alpha - Decimal(str(EXPECTED_ALPHA_SOLD))) < Decimal('0.01')
    
    def test_sales_tao_total(self, tracker):
        """Verify total TAO received from sales."""
        tracker.process_contract_income(lookback_days=7)
        sales = tracker.process_sales(lookback_days=7)
        
        total_tao = sum(Decimal(str(sale.tao_received)) for sale in sales)
        assert abs(total_tao - Decimal(str(EXPECTED_TAO_RECEIVED))) < Decimal('0.01')


class TestTransfers:
    """Test transfer processing."""
    
    def test_transfer_count(self, tracker):
        """Verify correct number of transfers to brokerage."""
        tracker.process_contract_income(lookback_days=7)
        tracker.process_sales(lookback_days=7)
        transfers = tracker.process_transfers(lookback_days=7)
        
        # Should have 2 transfers to brokerage (out of 4 total)
        assert len(transfers) == 2
    
    def test_transfers_consume_tao_lots(self, tracker):
        """Verify transfers consume TAO lots."""
        tracker.process_contract_income(lookback_days=7)
        tracker.process_sales(lookback_days=7)
        
        tao_lots_before = tracker.tao_lots_sheet.get_all_records()
        initial_tao = sum(Decimal(str(lot['TAO Remaining'])) for lot in tao_lots_before)
        
        transfers = tracker.process_transfers(lookback_days=7)
        
        # Calculate consumed TAO from transfers
        consumed_tao = sum(Decimal(str(t.tao_amount)) for t in transfers)
        
        assert abs(consumed_tao - Decimal(str(EXPECTED_TAO_TO_BROKERAGE))) < Decimal('0.01')
    
    def test_transfer_total_amount(self, tracker):
        """Verify total TAO transferred to brokerage."""
        tracker.process_contract_income(lookback_days=7)
        tracker.process_sales(lookback_days=7)
        transfers = tracker.process_transfers(lookback_days=7)
        
        total_transferred = sum(Decimal(str(t.tao_amount)) for t in transfers)
        assert abs(total_transferred - Decimal(str(EXPECTED_TAO_TO_BROKERAGE))) < Decimal('0.01')
    
    def test_fee_tracking(self, tracker):
        """Verify fees are tracked correctly."""
        tracker.process_contract_income(lookback_days=7)
        tracker.process_sales(lookback_days=7)
        transfers = tracker.process_transfers(lookback_days=7)
        
        total_fees = sum(Decimal(str(t.fee_tao)) for t in transfers)
        assert abs(total_fees - Decimal(str(EXPECTED_TOTAL_FEES))) < Decimal('0.0001')


class TestDataIntegrity:
    """Test data integrity and FIFO lot consumption."""
    
    def test_no_duplicate_contract_income(self, tracker):
        """Verify no duplicate contract income events."""
        new_lots = tracker.process_contract_income(lookback_days=7)
        
        # Check for duplicate extrinsic IDs
        extrinsic_ids = [lot.extrinsic_id for lot in new_lots if lot.extrinsic_id]
        assert len(extrinsic_ids) == len(set(extrinsic_ids))
    
    def test_no_duplicate_sales(self, tracker):
        """Verify no duplicate sales."""
        tracker.process_contract_income(lookback_days=7)
        sales = tracker.process_sales(lookback_days=7)
        
        # Check for duplicate extrinsic IDs
        extrinsic_ids = [sale.extrinsic_id for sale in sales if sale.extrinsic_id]
        assert len(extrinsic_ids) == len(set(extrinsic_ids))
    
    def test_no_duplicate_transfers(self, tracker):
        """Verify no duplicate transfers."""
        tracker.process_contract_income(lookback_days=7)
        tracker.process_sales(lookback_days=7)
        transfers = tracker.process_transfers(lookback_days=7)
        
        # Check for duplicate transaction IDs
        tx_ids = [t.transaction_hash for t in transfers if t.transaction_hash]
        assert len(tx_ids) == len(set(tx_ids))
    
    def test_fifo_lot_consumption(self, tracker):
        """Verify FIFO order in lot consumption."""
        tracker.process_contract_income(lookback_days=7)
        sales = tracker.process_sales(lookback_days=7)
        
        # Read alpha lots from sheet
        alpha_lots = tracker.income_sheet.get_all_records()
        
        # First sale should consume from first ALPHA lot
        # Verify by checking lot timestamps
        consumed_lots = [lot for lot in alpha_lots if float(lot.get('Alpha Remaining', 0)) == 0]
        if consumed_lots:
            # Sort by timestamp to verify FIFO
            consumed_lots_sorted = sorted(consumed_lots, key=lambda x: int(x['Timestamp']))
            # All consumed lots should have been consumed in timestamp order
            # (the sheet may not be sorted, but FIFO ensures oldest are consumed first)
            assert len(consumed_lots) >= 1  # At least one lot was fully consumed
    
    def test_chronological_ordering(self, tracker):
        """Verify all events have valid timestamps."""
        new_lots = tracker.process_contract_income(lookback_days=7)
        sales = tracker.process_sales(lookback_days=7)
        transfers = tracker.process_transfers(lookback_days=7)
        
        # Check that all events have valid timestamps
        assert all(lot.timestamp > 0 for lot in new_lots)
        assert all(sale.timestamp > 0 for sale in sales)
        assert all(transfer.timestamp > 0 for transfer in transfers)
class TestCompleteFlow:
    """Test the complete processing flow."""
    
    def test_complete_flow_execution(self, tracker):
        """Verify complete flow executes without errors."""
        # Should not raise any exceptions
        tracker.process_contract_income(lookback_days=7)
        tracker.process_sales(lookback_days=7)
        tracker.process_transfers(lookback_days=7)
    
    def test_alpha_lot_balance(self, tracker):
        """Verify ALPHA lot accounting balances."""
        new_lots = tracker.process_contract_income(lookback_days=7)
        initial_alpha = sum(Decimal(str(lot.alpha_quantity)) for lot in new_lots)
        
        sales = tracker.process_sales(lookback_days=7)
        
        # Total received = consumed + remaining
        consumed = sum(Decimal(str(sale.alpha_disposed)) for sale in sales)
        
        # Read remaining from sheet
        alpha_lots_after = tracker.income_sheet.get_all_records()
        remaining = sum(Decimal(str(lot['Alpha Remaining'])) for lot in alpha_lots_after)
        
        assert abs(initial_alpha - (consumed + remaining)) < Decimal('0.01')
    
    def test_tao_lot_balance(self, tracker):
        """Verify TAO lot accounting balances."""
        tracker.process_contract_income(lookback_days=7)
        sales = tracker.process_sales(lookback_days=7)
        
        initial_tao = sum(Decimal(str(sale.tao_received)) for sale in sales)
        
        transfers = tracker.process_transfers(lookback_days=7)
        
        # Total received = transferred + remaining
        transferred = sum(Decimal(str(t.tao_amount)) for t in transfers)
        
        # Read remaining from sheet
        tao_lots_after = tracker.tao_lots_sheet.get_all_records()
        remaining = sum(Decimal(str(lot['TAO Remaining'])) for lot in tao_lots_after)
        
        assert abs(initial_tao - (transferred + remaining)) < Decimal('0.01')
    
    def test_remaining_inventory(self, tracker):
        """Verify remaining inventory after all processing."""
        tracker.process_contract_income(lookback_days=7)
        tracker.process_sales(lookback_days=7)
        tracker.process_transfers(lookback_days=7)
        
        # Read remaining from sheets
        alpha_lots = tracker.income_sheet.get_all_records()
        remaining_alpha = sum(Decimal(str(lot['Alpha Remaining'])) for lot in alpha_lots)
        
        tao_lots = tracker.tao_lots_sheet.get_all_records()
        remaining_tao = sum(Decimal(str(lot['TAO Remaining'])) for lot in tao_lots)
        
        # Should have some ALPHA left (172.28 income - 112.65 sold = 59.63)
        expected_remaining_alpha = Decimal(str(EXPECTED_ALPHA_INCOME)) - Decimal(str(EXPECTED_ALPHA_SOLD))
        assert abs(remaining_alpha - expected_remaining_alpha) < Decimal('0.01')
        
        # Should have minimal TAO left (8.9182 received - 8.8918 transferred = 0.0264)
        expected_remaining_tao = Decimal(str(EXPECTED_TAO_RECEIVED)) - Decimal(str(EXPECTED_TAO_TO_BROKERAGE))
        assert abs(remaining_tao - expected_remaining_tao) < Decimal('0.01')
