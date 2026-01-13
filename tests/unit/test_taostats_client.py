"""Unit tests for TaoStatsAPIClient.

These tests verify that the client makes correct API calls with proper parameters
for each endpoint without actually making HTTP requests.
"""
import pytest
from unittest.mock import Mock, patch, call
from datetime import datetime

from emissions_tracker.clients.taostats import TaoStatsAPIClient
from emissions_tracker.exceptions import PriceNotAvailableError


@pytest.fixture
def mock_config():
    """Mock TaoStatsSettings configuration."""
    with patch('emissions_tracker.clients.taostats.TaoStatsSettings') as mock:
        config = Mock()
        config.api_key = "test-api-key"
        config.base_url = "https://api.taostats.io/api"
        config.rate_limit_seconds = 0.1
        mock.return_value = config
        yield config


@pytest.fixture
def client(mock_config):
    """Create a TaoStatsAPIClient instance with mocked config."""
    return TaoStatsAPIClient()


@pytest.fixture
def mock_requests_get():
    """Mock requests.get to avoid actual HTTP calls."""
    with patch('emissions_tracker.clients.taostats.requests.get') as mock_get:
        yield mock_get


# Tests for get_transfers method and /transfer/v1 endpoint

@pytest.mark.parametrize("address,start_time,end_time,sender,receiver,expected_params", [
    # Test with only required parameters
    (
        "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
        1700000000,
        1700100000,
        None,
        None,
        {
            "address": "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
            "timestamp_start": 1700000000,
            "timestamp_end": 1700100000,
            "order": "timestamp_asc",
            "page": 1,
            "limit": 500
        }
    ),
    # Test with sender parameter
    (
        "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
        1700000000,
        1700100000,
        "5EYCAe5ijiYfyeZ2JJCGq56LmPyNRAKzpG4QkoQkkQNB5e6Z",
        None,
        {
            "address": "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
            "timestamp_start": 1700000000,
            "timestamp_end": 1700100000,
            "order": "timestamp_asc",
            "sender": "5EYCAe5ijiYfyeZ2JJCGq56LmPyNRAKzpG4QkoQkkQNB5e6Z",
            "page": 1,
            "limit": 500
        }
    ),
    # Test with receiver parameter
    (
        "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
        1700000000,
        1700100000,
        None,
        "5EYCAe5ijiYfyeZ2JJCGq56LmPyNRAKzpG4QkoQkkQNB5e6Z",
        {
            "address": "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
            "timestamp_start": 1700000000,
            "timestamp_end": 1700100000,
            "order": "timestamp_asc",
            "receiver": "5EYCAe5ijiYfyeZ2JJCGq56LmPyNRAKzpG4QkoQkkQNB5e6Z",
            "page": 1,
            "limit": 500
        }
    ),
    # Test with both sender and receiver
    (
        "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
        1700000000,
        1700100000,
        "5EYCAe5ijiYfyeZ2JJCGq56LmPyNRAKzpG4QkoQkkQNB5e6Z",
        "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",
        {
            "address": "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
            "timestamp_start": 1700000000,
            "timestamp_end": 1700100000,
            "order": "timestamp_asc",
            "sender": "5EYCAe5ijiYfyeZ2JJCGq56LmPyNRAKzpG4QkoQkkQNB5e6Z",
            "receiver": "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",
            "page": 1,
            "limit": 500
        }
    ),
])
def test_get_transfers_params(
    client, mock_requests_get, address, start_time, end_time, 
    sender, receiver, expected_params
):
    """Test that get_transfers makes API calls with correct parameters."""
    # Mock response
    mock_response = Mock()
    mock_response.json.return_value = {
        "data": [
            {
                "timestamp": "2023-11-15T00:00:00Z",
                "from": {
                    "ss58": "5EYCAe5ijiYfyeZ2JJCGq56LmPyNRAKzpG4QkoQkkQNB5e6Z",
                    "hex": "0x1234567890abcdef"
                },
                "to": {
                    "ss58": "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
                    "hex": "0xabcdef1234567890"
                },
                "amount": "1000000000",
                "fee": "1000000",
                "block_number": 1234567,
                "transaction_hash": "0xabc123",
                "extrinsic_id": "1234567-2"
            }
        ],
        "pagination": {"next_page": None}
    }
    mock_requests_get.return_value = mock_response
    
    # Call the method
    result = client.get_transfers(address, start_time, end_time, sender, receiver)
    
    # Verify the request was made correctly
    expected_url = "https://api.taostats.io/api/transfer/v1"
    mock_requests_get.assert_called_once_with(
        expected_url,
        headers={
            "Authorization": "test-api-key",
            "Content-Type": "application/json"
        },
        params=expected_params
    )
    
    # Verify the result returns model objects
    assert len(result) == 1
    assert result[0].amount_tao == 1.0  # 1000000000 RAO = 1 TAO
    assert result[0].fee_tao == 0.001  # 1000000 RAO = 0.001 TAO


# Tests for get_delegations method and /delegation/v1 endpoint

@pytest.mark.parametrize("netuid,delegate,nominator,start_time,end_time,is_transfer,expected_params", [
    # Test with only required parameters (is_transfer=None)
    (
        64,
        "5Dt7HZ7Zpw4DppPxFM7Ke3Cm7sDAWhsZXmM5ZAmE7dSVJbcQ",
        "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
        1700000000,
        1700100000,
        None,
        {
            "action": "all",
            "netuid": 64,
            "delegate": "5Dt7HZ7Zpw4DppPxFM7Ke3Cm7sDAWhsZXmM5ZAmE7dSVJbcQ",
            "nominator": "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
            "timestamp_start": 1700000000,
            "timestamp_end": 1700100000,
            "order": "timestamp_asc",
            "page": 1,
            "limit": 500
        }
    ),
    # Test with is_transfer=True
    (
        64,
        "5Dt7HZ7Zpw4DppPxFM7Ke3Cm7sDAWhsZXmM5ZAmE7dSVJbcQ",
        "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
        1700000000,
        1700100000,
        True,
        {
            "action": "all",
            "netuid": 64,
            "delegate": "5Dt7HZ7Zpw4DppPxFM7Ke3Cm7sDAWhsZXmM5ZAmE7dSVJbcQ",
            "nominator": "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
            "timestamp_start": 1700000000,
            "timestamp_end": 1700100000,
            "order": "timestamp_asc",
            "is_transfer": "true",
            "page": 1,
            "limit": 500
        }
    ),
    # Test with is_transfer=False
    (
        64,
        "5Dt7HZ7Zpw4DppPxFM7Ke3Cm7sDAWhsZXmM5ZAmE7dSVJbcQ",
        "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
        1700000000,
        1700100000,
        False,
        {
            "action": "all",
            "netuid": 64,
            "delegate": "5Dt7HZ7Zpw4DppPxFM7Ke3Cm7sDAWhsZXmM5ZAmE7dSVJbcQ",
            "nominator": "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
            "timestamp_start": 1700000000,
            "timestamp_end": 1700100000,
            "order": "timestamp_asc",
            "is_transfer": "false",
            "page": 1,
            "limit": 500
        }
    ),
])
def test_get_delegations_params(
    client, mock_requests_get, netuid, delegate, nominator,
    start_time, end_time, is_transfer, expected_params
):
    """Test that get_delegations makes API calls with correct parameters."""
    # Mock response
    mock_response = Mock()
    mock_response.json.return_value = {
        "data": [
            {
                "timestamp": "2023-11-15T00:00:00Z",
                "action": "DELEGATE",
                "nominator": {
                    "ss58": "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
                    "hex": "0xnominator123"
                },
                "delegate": {
                    "ss58": "5Dt7HZ7Zpw4DppPxFM7Ke3Cm7sDAWhsZXmM5ZAmE7dSVJbcQ",
                    "hex": "0xdelegate456"
                },
                "netuid": 64,
                "alpha": "500000000",
                "amount": "1000000000",
                "usd": "350.0",
                "alpha_price_in_usd": "0.70",
                "alpha_price_in_tao": "0.5",
                "slippage": "0.01",
                "block_number": 1234567,
                "extrinsic_id": "1234567-2",
                "is_transfer": False,
                "transfer_address": None,
                "fee": "1000000"
            }
        ],
        "pagination": {"next_page": None}
    }
    mock_requests_get.return_value = mock_response
    
    # Call the method
    result = client.get_delegations(
        netuid, delegate, nominator, start_time, end_time, is_transfer
    )
    
    # Verify the request was made correctly
    expected_url = "https://api.taostats.io/api/delegation/v1"
    mock_requests_get.assert_called_once_with(
        expected_url,
        headers={
            "Authorization": "test-api-key",
            "Content-Type": "application/json"
        },
        params=expected_params
    )
    
    # Verify the result returns model objects
    assert len(result) == 1
    assert result[0].alpha_float == 0.5  # 500000000 RAO = 0.5 ALPHA
    assert result[0].tao == 1.0  # 1000000000 RAO = 1 TAO


# Tests for get_stake_balance_history method and /dtao/stake_balance/history/v1 endpoint

@pytest.mark.parametrize("netuid,hotkey,coldkey,start_time,end_time,expected_params", [
    # Test with required parameters
    (
        64,
        "5Dt7HZ7Zpw4DppPxFM7Ke3Cm7sDAWhsZXmM5ZAmE7dSVJbcQ",
        "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
        1700000000,
        1700100000,
        {
            "netuid": 64,
            "hotkey": "5Dt7HZ7Zpw4DppPxFM7Ke3Cm7sDAWhsZXmM5ZAmE7dSVJbcQ",
            "coldkey": "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
            "start_time": 1700000000,
            "end_time": 1700100000,
            "order": "timestamp_asc",
            "page": 1,
            "limit": 500
        }
    ),
])
def test_get_stake_balance_history_params(
    client, mock_requests_get, netuid, hotkey, coldkey,
    start_time, end_time, expected_params
):
        """Test that get_stake_balance_history makes API calls with correct parameters."""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [
                {
                    "timestamp": "2023-11-15T00:00:00Z",
                    "block_number": 1234567,
                    "hotkey_name": "test-hotkey",
                    "hotkey": {
                        "ss58": "5Dt7HZ7Zpw4DppPxFM7Ke3Cm7sDAWhsZXmM5ZAmE7dSVJbcQ",
                        "hex": "0xhotkey123"
                    },
                    "coldkey": {
                        "ss58": "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
                        "hex": "0xcoldkey456"
                    },
                    "netuid": 64,
                    "balance": "500000000",
                    "balance_as_tao": "1000000000"
                }
            ],
            "pagination": {"next_page": None}
        }
        mock_requests_get.return_value = mock_response
        
        # Call the method
        result = client.get_stake_balance_history(
            netuid, hotkey, coldkey, start_time, end_time
        )
        
        # Verify the request was made correctly
        expected_url = "https://api.taostats.io/api/dtao/stake_balance/history/v1"
        mock_requests_get.assert_called_once_with(
            expected_url,
            headers={
                "Authorization": "test-api-key",
                "Content-Type": "application/json"
            },
            params=expected_params
        )
        
        # Verify the result returns model objects
        assert len(result) == 1
        assert result[0].balance_rao == 500000000
        assert result[0].balance_as_tao_rao == 1000000000


# Tests for get_price_at_timestamp and get_prices_in_range methods

@pytest.mark.parametrize("symbol,timestamp,expected_params", [
    (
        "TAO",
        1700000000,
        {
            "asset": "TAO",
            "timestamp_start": 1700000000 - 1800,  # 30 min buffer
            "timestamp_end": 1700000000 + 1800,
            "order": "timestamp_asc",
            "per_page": 50,
            "page": 1,
            "limit": 50
        }
    ),
])
def test_get_price_at_timestamp_params(
    client, mock_requests_get, symbol, timestamp, expected_params
):
    """Test that get_price_at_timestamp makes API calls with correct parameters."""
    # Mock response
    mock_response = Mock()
    mock_response.json.return_value = {
        "data": [
            {
                "created_at": "2023-11-15T00:00:00Z",
                "price": "350.50"
            }
        ],
        "pagination": {"next_page": None}
    }
    mock_requests_get.return_value = mock_response
    
    # Call the method
    result = client.get_price_at_timestamp(symbol, timestamp)
    
    # Verify the request was made correctly
    expected_url = "https://api.taostats.io/api/price/history/v1"
    mock_requests_get.assert_called_once_with(
        expected_url,
        headers={
            "Authorization": "test-api-key",
            "Content-Type": "application/json"
        },
        params=expected_params
    )
    
    # Verify the result
    assert result == 350.50


def test_get_price_at_timestamp_invalid_symbol(client):
    """Test that get_price_at_timestamp raises error for non-TAO symbols."""
    with pytest.raises(PriceNotAvailableError, match="only supports TAO"):
        client.get_price_at_timestamp("BTC", 1700000000)


@pytest.mark.parametrize("symbol,start_time,end_time,expected_params", [
    (
        "TAO",
        1700000000,
        1700100000,
        {
            "asset": "TAO",
            "timestamp_start": 1700000000,
            "timestamp_end": 1700100000,
            "order": "timestamp_asc",
            "per_page": 500,
            "page": 1,
            "limit": 500
        }
    ),
])
def test_get_prices_in_range_params(
    client, mock_requests_get, symbol, start_time, end_time, expected_params
):
    """Test that get_prices_in_range makes API calls with correct parameters."""
    # Mock response
    mock_response = Mock()
    mock_response.json.return_value = {
        "data": [
            {
                "created_at": "2023-11-15T00:00:00Z",
                "price": "350.50"
            },
            {
                "created_at": "2023-11-15T01:00:00Z",
                "price": "351.00"
            }
        ],
        "pagination": {"next_page": None}
    }
    mock_requests_get.return_value = mock_response
    
    # Call the method
    result = client.get_prices_in_range(symbol, start_time, end_time)
    
    # Verify the request was made correctly
    expected_url = "https://api.taostats.io/api/price/history/v1"
    mock_requests_get.assert_called_once_with(
        expected_url,
        headers={
            "Authorization": "test-api-key",
            "Content-Type": "application/json"
        },
        params=expected_params
    )
    
    # Verify the result format
    assert len(result) == 2
    assert result[0]['price'] == 350.50
    assert result[1]['price'] == 351.00


def test_get_prices_in_range_invalid_symbol(client):
    """Test that get_prices_in_range raises error for non-TAO symbols."""
    with pytest.raises(PriceNotAvailableError, match="only supports TAO"):
        client.get_prices_in_range("BTC", 1700000000, 1700100000)


# Tests for get_current_price method and /price/latest/v1 endpoint

@pytest.mark.parametrize("symbol,expected_params", [
    (
        "TAO",
        {
            "asset": "TAO",
            "page": 1,
            "limit": 50
        }
    ),
])
def test_get_current_price_params(
    client, mock_requests_get, symbol, expected_params
):
    """Test that get_current_price makes API calls with correct parameters."""
    # Mock response
    mock_response = Mock()
    mock_response.json.return_value = {
        "data": [
            {
                "created_at": "2023-11-15T00:00:00Z",
                "price": "350.50"
            }
        ],
        "pagination": {"next_page": None}
    }
    mock_requests_get.return_value = mock_response
    
    # Call the method
    result = client.get_current_price(symbol)
    
    # Verify the request was made correctly
    expected_url = "https://api.taostats.io/api/price/latest/v1"
    mock_requests_get.assert_called_once_with(
        expected_url,
        headers={
            "Authorization": "test-api-key",
            "Content-Type": "application/json"
        },
        params=expected_params
    )
    
    # Verify the result
    assert result == 350.50


def test_get_current_price_invalid_symbol(client):
    """Test that get_current_price raises error for non-TAO symbols."""
    with pytest.raises(PriceNotAvailableError, match="only supports TAO"):
        client.get_current_price("BTC")


# Tests for error handling in client methods

def test_get_transfers_returns_empty_on_error(client, mock_requests_get):
    """Test that get_transfers returns empty list on API error."""
    mock_requests_get.side_effect = Exception("API Error")
    
    result = client.get_transfers(
        "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
        1700000000,
        1700100000
    )
    
    assert result == []


def test_get_delegations_returns_empty_on_error(client, mock_requests_get):
    """Test that get_delegations returns empty list on API error."""
    mock_requests_get.side_effect = Exception("API Error")
    
    result = client.get_delegations(
        64,
        "5Dt7HZ7Zpw4DppPxFM7Ke3Cm7sDAWhsZXmM5ZAmE7dSVJbcQ",
        "5DcjH2Y8eXskQ9cGgoProcS7S6m9FRg7bdMqvZs4VT2gNsR2",
        1700000000,
        1700100000
    )
    
    assert result == []


def test_get_price_raises_on_error(client, mock_requests_get):
    """Test that price methods raise PriceNotAvailableError on API error."""
    import requests
    # Use a RequestException which will be caught and re-raised as PriceNotAvailableError
    mock_requests_get.side_effect = requests.RequestException("API Error")
    
    with pytest.raises(PriceNotAvailableError, match="Taostats API error"):
        client.get_price_at_timestamp("TAO", 1700000000)
