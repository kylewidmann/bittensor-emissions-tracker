"""
Mock client fixtures for testing.

Provides mock implementations of API clients that use real test data
and filter it based on method arguments.
"""

import json
import pytest
from pathlib import Path
from typing import List, Optional
from datetime import datetime

from emissions_tracker.clients.wallet import WalletClientInterface
from emissions_tracker.clients.price import PriceClient
from emissions_tracker.models import (
    TaoStatsDelegation, TaoStatsTransfer, TaoStatsStakeBalance, TaoStatsAddress
)
from emissions_tracker.exceptions import PriceNotAvailableError


# Test data directory
TEST_DATA_DIR = Path(__file__).parent.parent / "data" / "all"


class MockTaoStatsClient(WalletClientInterface, PriceClient):
    """
    Mock TaoStats client that returns filtered data from test fixtures.
    
    Loads real test data and filters based on method arguments,
    returning properly typed model objects.
    """
    
    def __init__(self):
        """Initialize mock client and load test data."""
        self._load_test_data()
    
    def _load_test_data(self):
        """Load all test data files."""
        # Load delegations/stake events
        with open(TEST_DATA_DIR / "stake_events.json") as f:
            self._raw_delegations = json.load(f)["data"]
        
        # Load transfers
        with open(TEST_DATA_DIR / "transfers.json") as f:
            self._raw_transfers = json.load(f)["data"]
        
        # Load stake balance history
        with open(TEST_DATA_DIR / "stake_balance.json") as f:
            self._raw_stake_balance = json.load(f)["data"]
        
        # Load price data (dict with dates as keys)
        with open(TEST_DATA_DIR / "historical_tao_prices.json") as f:
            price_dict = json.load(f)
            # Convert dict to list for easier searching
            self._raw_prices = list(price_dict.values())
    
    @property
    def name(self) -> str:
        return "Mock TaoStats API"
    
    def get_delegations(
        self,
        netuid: int,
        delegate: str,
        nominator: str,
        start_time: int,
        end_time: int,
        is_transfer: Optional[bool] = None
    ) -> List[TaoStatsDelegation]:
        """Filter and return delegation events matching criteria.
        
        Matches the real TaoStats API behavior where netuid, delegate, and nominator
        are required filters.
        """
        filtered = []
        
        for event in self._raw_delegations:
            # Parse timestamp
            event_ts = int(datetime.fromisoformat(
                event['timestamp'].replace('Z', '+00:00')
            ).timestamp())
            
            # Apply time filter (inclusive on both ends)
            if event_ts < start_time or event_ts > end_time:
                continue
            
            # Apply netuid filter (required)
            if event['netuid'] != netuid:
                continue
            
            # Apply delegate filter (required)
            if event['delegate']['ss58'] != delegate:
                continue
            
            # Apply nominator filter (required)
            if event['nominator']['ss58'] != nominator:
                continue
            
            # Filter by is_transfer if specified
            # NOTE: Only filter if is_transfer is explicitly True or False
            # If None, include all events regardless of transfer status
            if is_transfer is not None:
                event_is_transfer = event.get('is_transfer', False)
                if is_transfer != event_is_transfer:
                    continue
            
            # Convert to TaoStatsDelegation model
            delegation = TaoStatsDelegation(
                block_number=int(event['block_number']),
                timestamp=event['timestamp'],
                action=event['action'],
                nominator=TaoStatsAddress(
                    ss58=event['nominator']['ss58'],
                    hex=event['nominator']['hex']
                ),
                delegate=TaoStatsAddress(
                    ss58=event['delegate']['ss58'],
                    hex=event['delegate']['hex']
                ),
                netuid=int(event['netuid']),
                amount=int(event['amount']),
                alpha=int(event['alpha']),
                usd=float(event['usd']),
                alpha_price_in_usd=event.get('alpha_price_in_usd'),
                alpha_price_in_tao=event.get('alpha_price_in_tao'),
                slippage=event.get('slippage'),
                extrinsic_id=event['extrinsic_id'],
                is_transfer=event.get('is_transfer'),
                transfer_address=TaoStatsAddress(
                    ss58=event['transfer_address']['ss58'],
                    hex=event['transfer_address']['hex']
                ) if event.get('transfer_address') else None,
                fee=event.get('fee')
            )
            filtered.append(delegation)
        
        return filtered
    
    def get_transfers(
        self,
        account_address: str,
        start_time: int,
        end_time: int,
        sender: Optional[str] = None,
        receiver: Optional[str] = None
    ) -> List[TaoStatsTransfer]:
        """Filter and return transfers matching criteria."""
        filtered = []
        
        for transfer in self._raw_transfers:
            # Parse timestamp
            transfer_ts = int(datetime.fromisoformat(
                transfer['timestamp'].replace('Z', '+00:00')
            ).timestamp())
            
            # Apply filters
            if transfer_ts < start_time or transfer_ts > end_time:
                continue
            
            # Check if account_address is sender or receiver
            is_sender = transfer['from']['ss58'] == account_address
            is_receiver = transfer['to']['ss58'] == account_address
            
            if not (is_sender or is_receiver):
                continue
            
            # Apply sender filter if specified
            if sender and transfer['from']['ss58'] != sender:
                continue
            
            # Apply receiver filter if specified
            if receiver and transfer['to']['ss58'] != receiver:
                continue
            
            # Convert to TaoStatsTransfer model
            transfer_obj = TaoStatsTransfer(
                block_number=transfer['block_number'],
                timestamp=transfer['timestamp'],
                transaction_hash=transfer['transaction_hash'],
                extrinsic_id=transfer['extrinsic_id'],
                amount=transfer['amount'],
                fee=transfer.get('fee'),
                from_address=TaoStatsAddress(
                    ss58=transfer['from']['ss58'],
                    hex=transfer['from']['hex']
                ),
                to_address=TaoStatsAddress(
                    ss58=transfer['to']['ss58'],
                    hex=transfer['to']['hex']
                )
            )
            filtered.append(transfer_obj)
        
        return filtered
    
    def get_stake_balance_history(
        self,
        netuid: int,
        hotkey: str,
        coldkey: str,
        start_time: int,
        end_time: int
    ) -> List[TaoStatsStakeBalance]:
        """Filter and return stake balance history matching criteria."""
        filtered = []
        
        for balance in self._raw_stake_balance:
            # Parse timestamp
            balance_ts = int(datetime.fromisoformat(
                balance['timestamp'].replace('Z', '+00:00')
            ).timestamp())
            
            # Apply filters
            if balance_ts < start_time or balance_ts > end_time:
                continue
            
            if balance['netuid'] != netuid:
                continue
            
            if balance['hotkey']['ss58'] != hotkey:
                continue
            
            if balance['coldkey']['ss58'] != coldkey:
                continue
            
            # Convert to TaoStatsStakeBalance model
            balance_obj = TaoStatsStakeBalance(
                block_number=balance['block_number'],
                timestamp=balance['timestamp'],
                hotkey_name=balance.get('hotkey_name', ''),
                hotkey=TaoStatsAddress(
                    ss58=balance['hotkey']['ss58'],
                    hex=balance['hotkey']['hex']
                ),
                coldkey=TaoStatsAddress(
                    ss58=balance['coldkey']['ss58'],
                    hex=balance['coldkey']['hex']
                ),
                netuid=balance['netuid'],
                balance=balance['balance'],
                balance_as_tao=balance['balance_as_tao']
            )
            filtered.append(balance_obj)
        
        return filtered
    
    def get_price_at_timestamp(self, symbol: str, timestamp: int) -> float:
        """Get price at specific timestamp (finds closest)."""
        if symbol != 'TAO':
            raise PriceNotAvailableError(f"Only TAO prices available, got {symbol}")
        
        if not self._raw_prices:
            raise PriceNotAvailableError("No price data available")
        
        # Find closest price by timestamp
        closest = min(
            self._raw_prices,
            key=lambda p: abs(p['timestamp'] - timestamp)
        )
        
        return float(closest['price'])
    
    def get_prices_in_range(self, symbol: str, start_time: int, end_time: int) -> List[dict]:
        """Get all prices within time range."""
        if symbol != 'TAO':
            raise PriceNotAvailableError(f"Only TAO prices available, got {symbol}")
        
        filtered = []
        for price_data in self._raw_prices:
            price_ts = price_data['timestamp']
            
            if start_time <= price_ts <= end_time:
                filtered.append({
                    'timestamp': price_ts,
                    'price': float(price_data['price'])
                })
        
        return sorted(filtered, key=lambda x: x['timestamp'])
    
    def get_current_price(self, symbol: str) -> float:
        """Get most recent price."""
        if symbol != 'TAO':
            raise PriceNotAvailableError(f"Only TAO prices available, got {symbol}")
        
        if not self._raw_prices:
            raise PriceNotAvailableError("No price data available")
        
        # Get most recent price
        latest = max(self._raw_prices, key=lambda p: p['timestamp'])
        
        return float(latest['price'])


@pytest.fixture
def mock_taostats_client():
    """
    Pytest fixture that provides a mock TaoStats client with test data.
    
    The client automatically filters data based on method arguments,
    so tests don't need to manually setup return values.
    
    Usage:
        def test_something(mock_taostats_client):
            # Client returns filtered data from test fixtures
            delegations = mock_taostats_client.get_delegations(
                netuid=64,
                delegate="5F...",
                nominator="5G...",
                start_time=1234567890,
                end_time=1234567999
            )
            # Returns only matching delegations from test data
    """
    return MockTaoStatsClient()
