
import pytest
from tests.fixtures.mock_config import TEST_PAYOUT_COLDKEY_SS58, TEST_SMART_CONTRACT_SS58, TEST_TRACKER_SHEET_ID, TEST_VALIDATOR_SS58


@pytest.fixture
def contract_tracker(mock_taostats_client):
    """Create tracker instance with properly mocked dependencies."""
    # Create tracker normally through __init__
    from emissions_tracker.trackers.contract_tracker import ContractTracker
    tracker = ContractTracker(
        price_client=mock_taostats_client,
        wallet_client=mock_taostats_client,
    )
    
    return tracker

@pytest.fixture
def get_contract_tracker(mock_taostats_client):
    """Return a function that creates a contract tracker instance."""
    def _get_tracker():
        from emissions_tracker.trackers.contract_tracker import ContractTracker
        tracker = ContractTracker(
            price_client=mock_taostats_client,
            wallet_client=mock_taostats_client,
        )
        return tracker
    return _get_tracker


@pytest.fixture
def mining_tracker(mock_mining_taostats_client):
    """Create mining tracker instance with properly mocked dependencies."""
    from emissions_tracker.trackers.mining_tracker import MiningTracker 
    tracker = MiningTracker(
        price_client=mock_mining_taostats_client,
        wallet_client=mock_mining_taostats_client,
    )
    
    return tracker


@pytest.fixture
def get_mining_tracker(mock_mining_taostats_client):
    """Return a function that creates a mining tracker instance."""
    def _get_tracker():
        from emissions_tracker.trackers.mining_tracker import MiningTracker 
        tracker = MiningTracker(
            price_client=mock_mining_taostats_client,
            wallet_client=mock_mining_taostats_client,
        )
        return tracker
    return _get_tracker