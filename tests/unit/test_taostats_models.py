"""
Unit tests for TaoStats API response models.

These tests verify that the TaoStatsAPIClient correctly converts API JSON
responses into model objects. They mock API calls with real test data, call
the client methods, and verify that the returned models match the raw JSON.
"""
import json
import pytest
from pathlib import Path
from typing import Dict, Any
from unittest.mock import Mock, patch

from emissions_tracker.clients.taostats import TaoStatsAPIClient
from emissions_tracker.models import (
    TaoStatsAddress,
    TaoStatsStakeBalance,
    TaoStatsDelegation,
    TaoStatsTransfer,
)


def load_json_file(filename: str, test_dir: str = "all") -> Dict[str, Any]:
    """Load a JSON test data file."""
    data_dir = Path(__file__).parent.parent / "data" / test_dir
    with open(data_dir / filename, 'r') as f:
        return json.load(f)


@pytest.fixture
def mock_config():
    """Mock TaoStats configuration."""
    with patch('emissions_tracker.clients.taostats.TaoStatsSettings') as mock_settings:
        mock_instance = Mock()
        mock_instance.api_key = "test_api_key"
        mock_instance.base_url = "https://api.taostats.io"
        mock_instance.rate_limit_seconds = 0.1
        mock_settings.return_value = mock_instance
        yield mock_settings


@pytest.fixture
def client(mock_config):
    """Create a TaoStatsAPIClient instance with mocked config."""
    return TaoStatsAPIClient()


def verify_transfer_matches_raw(transfer: TaoStatsTransfer, raw: Dict[str, Any]):
    """Verify a TaoStatsTransfer model matches raw JSON."""
    assert transfer.block_number == raw['block_number']
    assert transfer.timestamp == raw['timestamp']
    assert transfer.transaction_hash == raw['transaction_hash']
    assert transfer.extrinsic_id == raw['extrinsic_id']
    assert transfer.amount == raw['amount']
    assert transfer.fee == raw.get('fee')
    assert transfer.from_address.ss58 == raw['from']['ss58']
    assert transfer.from_address.hex == raw['from']['hex']
    assert transfer.to_address.ss58 == raw['to']['ss58']
    assert transfer.to_address.hex == raw['to']['hex']


def verify_stake_balance_matches_raw(stake_balance: TaoStatsStakeBalance, raw: Dict[str, Any]):
    """Verify a TaoStatsStakeBalance model matches raw JSON."""
    assert stake_balance.block_number == raw['block_number']
    assert stake_balance.timestamp == raw['timestamp']
    assert stake_balance.hotkey_name == raw['hotkey_name']
    assert stake_balance.netuid == raw['netuid']
    assert stake_balance.balance == raw['balance']
    assert stake_balance.balance_as_tao == raw['balance_as_tao']
    assert stake_balance.hotkey.ss58 == raw['hotkey']['ss58']
    assert stake_balance.hotkey.hex == raw['hotkey']['hex']
    assert stake_balance.coldkey.ss58 == raw['coldkey']['ss58']
    assert stake_balance.coldkey.hex == raw['coldkey']['hex']


def verify_delegation_matches_raw(delegation: TaoStatsDelegation, raw: Dict[str, Any]):
    """Verify a TaoStatsDelegation model matches raw JSON."""
    assert delegation.block_number == raw['block_number']
    assert delegation.timestamp == raw['timestamp']
    assert delegation.action == raw['action']
    assert delegation.netuid == raw['netuid']
    # amount and alpha are converted from string to int by the model
    assert delegation.amount == int(raw['amount'])
    assert delegation.alpha == int(raw['alpha'])
    assert delegation.usd == raw['usd'] if isinstance(raw['usd'], float) else float(raw['usd'])
    assert delegation.extrinsic_id == raw['extrinsic_id']
    assert delegation.nominator.ss58 == raw['nominator']['ss58']
    assert delegation.nominator.hex == raw['nominator']['hex']
    assert delegation.delegate.ss58 == raw['delegate']['ss58']
    assert delegation.delegate.hex == raw['delegate']['hex']
    
    if raw.get('transfer_address'):
        assert delegation.transfer_address is not None
        assert delegation.transfer_address.ss58 == raw['transfer_address']['ss58']
        assert delegation.transfer_address.hex == raw['transfer_address']['hex']
    else:
        assert delegation.transfer_address is None


def test_get_transfers_returns_models_matching_raw_json(client):
    """Test that get_transfers returns TaoStatsTransfer models matching raw JSON."""
    raw_data = load_json_file('transfers.json')
    
    # Ensure pagination indicates this is the only page
    raw_data['pagination']['next_page'] = None
    
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = raw_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Call the actual client method
        transfers = client.get_transfers("test_address", 0, 9999999999)
        
        # Verify each returned model matches the raw JSON
        raw_records = raw_data['data']
        assert len(transfers) == len(raw_records)
        for transfer, raw in zip(transfers, raw_records):
            verify_transfer_matches_raw(transfer, raw)


def test_get_stake_balance_history_returns_models_matching_raw_json(client):
    """Test that get_stake_balance_history returns TaoStatsStakeBalance models matching raw JSON."""
    raw_data = load_json_file('stake_balance.json')
    
    # Ensure pagination indicates this is the only page
    raw_data['pagination']['next_page'] = None
    
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = raw_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Call the actual client method
        stake_balances = client.get_stake_balance_history(64, "hotkey", "coldkey", 0, 9999999999)
        
        # Verify each returned model matches the raw JSON
        raw_records = raw_data['data']
        assert len(stake_balances) == len(raw_records)
        for stake_balance, raw in zip(stake_balances, raw_records):
            verify_stake_balance_matches_raw(stake_balance, raw)


def test_get_delegations_returns_models_matching_raw_json(client):
    """Test that get_delegations returns TaoStatsDelegation models matching raw JSON."""
    raw_data = load_json_file('stake_events.json')
    
    # Ensure pagination indicates this is the only page
    raw_data['pagination']['next_page'] = None
    
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = raw_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Call the actual client method
        delegations = client.get_delegations(64, "delegate", "nominator", 0, 9999999999)
        
        # Verify each returned model matches the raw JSON
        raw_records = raw_data['data']
        assert len(delegations) == len(raw_records)
        for delegation, raw in zip(delegations, raw_records):
            verify_delegation_matches_raw(delegation, raw)


@pytest.mark.parametrize("test_dir", ["all", "bak"])
def test_transfer_models_work_across_datasets(test_dir, client):
    """Verify transfer model parsing works with both sanitized and backup datasets."""
    raw_data = load_json_file('transfers.json', test_dir)
    
    # Ensure pagination indicates this is the only page
    raw_data['pagination']['next_page'] = None
    
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = raw_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        transfers = client.get_transfers("test_address", 0, 9999999999)
        
        raw_records = raw_data['data']
        # Verify first and last
        for idx in [0, -1]:
            verify_transfer_matches_raw(transfers[idx], raw_records[idx])


@pytest.mark.parametrize("test_dir", ["all", "bak"])
def test_stake_balance_models_work_across_datasets(test_dir, client):
    """Verify stake balance model parsing works with both sanitized and backup datasets."""
    raw_data = load_json_file('stake_balance.json', test_dir)
    
    # Ensure pagination indicates this is the only page
    raw_data['pagination']['next_page'] = None
    
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = raw_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        stake_balances = client.get_stake_balance_history(64, "hotkey", "coldkey", 0, 9999999999)
        
        raw_records = raw_data['data']
        # Verify first and last
        for idx in [0, -1]:
            verify_stake_balance_matches_raw(stake_balances[idx], raw_records[idx])


@pytest.mark.parametrize("test_dir", ["all", "bak"])
def test_delegation_models_work_across_datasets(test_dir, client):
    """Verify delegation model parsing works with both sanitized and backup datasets."""
    raw_data = load_json_file('stake_events.json', test_dir)
    
    # Ensure pagination indicates this is the only page
    raw_data['pagination']['next_page'] = None
    
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = raw_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        delegations = client.get_delegations(64, "delegate", "nominator", 0, 9999999999)
        
        raw_records = raw_data['data']
        # Verify first and last
        for idx in [0, -1]:
            verify_delegation_matches_raw(delegations[idx], raw_records[idx])
