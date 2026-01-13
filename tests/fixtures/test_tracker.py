
import pytest
from tests.fixtures.mock_config import TEST_PAYOUT_COLDKEY_SS58, TEST_SMART_CONTRACT_SS58, TEST_TRACKER_SHEET_ID, TEST_VALIDATOR_SS58


@pytest.fixture
def contract_tracker(mock_taostats_client):
    """Create tracker instance with properly mocked dependencies."""
    # Create tracker normally through __init__
    from emissions_tracker.tracker import BittensorEmissionTracker
    from emissions_tracker.models import SourceType
    tracker = BittensorEmissionTracker(
        price_client=mock_taostats_client,
        wallet_client=mock_taostats_client,
        tracking_hotkey=TEST_VALIDATOR_SS58,
        coldkey=TEST_PAYOUT_COLDKEY_SS58,
        sheet_id=TEST_TRACKER_SHEET_ID,
        label="Test Tracker",
        smart_contract_address=TEST_SMART_CONTRACT_SS58,
        income_source=SourceType.CONTRACT
    )
    
    return tracker

@pytest.fixture
def get_contract_tracker(mock_taostats_client):
    """Return a function that creates a contract tracker instance."""
    def _get_tracker():
        from emissions_tracker.tracker import BittensorEmissionTracker
        from emissions_tracker.models import SourceType
        tracker = BittensorEmissionTracker(
            price_client=mock_taostats_client,
            wallet_client=mock_taostats_client,
            tracking_hotkey=TEST_VALIDATOR_SS58,
            coldkey=TEST_PAYOUT_COLDKEY_SS58,
            sheet_id=TEST_TRACKER_SHEET_ID,
            label="Test Tracker",
            smart_contract_address=TEST_SMART_CONTRACT_SS58,
            income_source=SourceType.CONTRACT
        )
        return tracker
    return _get_tracker

@pytest.fixture
def mining_tracker(mock_taostats_client):
    """Create tracker instance with properly mocked dependencies."""
    # Create tracker normally through __init__
    from emissions_tracker.tracker import BittensorEmissionTracker
    from emissions_tracker.models import SourceType
    tracker = BittensorEmissionTracker(
        price_client=mock_taostats_client,
        wallet_client=mock_taostats_client,
        tracking_hotkey=TEST_VALIDATOR_SS58,
        coldkey=TEST_PAYOUT_COLDKEY_SS58,
        sheet_id=TEST_TRACKER_SHEET_ID,
        label="Test Tracker",
        smart_contract_address=TEST_SMART_CONTRACT_SS58,
        income_source=SourceType.MINING
    )
    
    return tracker