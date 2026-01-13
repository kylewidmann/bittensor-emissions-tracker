"""
Mock configuration fixtures for testing.

Provides consistent mock settings with constants that tests can reference.
All configuration values use test-safe defaults that can be overridden per-test.
"""

import pytest
from unittest.mock import patch
from emissions_tracker.config import TrackerSettings, WaveAccountSettings
from emissions_tracker.models import CostBasisMethod


# Test wallet addresses (constants for test assertions)
TEST_BROKER_SS58 = "5FBrokerTestAddress123456789ABCDEFGH"
TEST_VALIDATOR_SS58 = "5TestHotKey1111111111111111111111111111111DkcSpr"
TEST_PAYOUT_COLDKEY_SS58 = "5TestColdKey1111111111111111111111111111111BSti94"
TEST_SMART_CONTRACT_SS58 = "5TestHotKey1111111111111111111111111111111111111Zq47nrf"
TEST_MINER_HOTKEY_SS58 = "5FMinerHotkeyTestAddress123456789AB"
TEST_MINER_COLDKEY_SS58 = "5FMinerColdkeyTestAddress123456789"

# Test sheet IDs
TEST_TRACKER_SHEET_ID = "test-tracker-sheet-123"
TEST_MINING_TRACKER_SHEET_ID = "test-mining-sheet-456"
TEST_GOOGLE_CREDENTIALS_PATH = "/tmp/test-credentials.json"

# Test subnet configuration
TEST_SUBNET_ID = 64
TEST_LOT_STRATEGY = CostBasisMethod.HIFO
TEST_RATE_LIMIT_SECONDS = 0.1  # Fast for tests

# Wave account names (constants for test assertions)
TEST_CONTRACT_INCOME_ACCOUNT = "Test Contract Income - Alpha"
TEST_STAKING_INCOME_ACCOUNT = "Test Staking Income - Alpha"
TEST_MINING_INCOME_ACCOUNT = "Test Mining Income - Alpha"
TEST_ALPHA_ASSET_ACCOUNT = "Test Alpha Holdings"
TEST_TAO_ASSET_ACCOUNT = "Test TAO Holdings"
TEST_TRANSFER_PROCEEDS_ACCOUNT = "Test Exchange Clearing - Kraken"
TEST_BLOCKCHAIN_FEE_ACCOUNT = "Test Blockchain Fees"
TEST_SHORT_TERM_GAIN_ACCOUNT = "Test Short-term Capital Gains"
TEST_SHORT_TERM_LOSS_ACCOUNT = "Test Short-term Capital Losses"
TEST_LONG_TERM_GAIN_ACCOUNT = "Test Long-term Capital Gains"
TEST_LONG_TERM_LOSS_ACCOUNT = "Test Long-term Capital Losses"


@pytest.fixture
def mock_tracker_settings():
    """
    Pytest fixture that provides mocked TrackerSettings with test defaults.
    
    Usage:
        def test_something(mock_tracker_settings):
            # Use default values
            assert mock_tracker_settings.brokerage_ss58 == TEST_BROKER_SS58
            
            # Or modify for specific test
            mock_tracker_settings.subnet_id = 99
    """
    # Create mock settings without requiring environment variables
    with patch.dict('os.environ', {
        'BROKER_SS58': TEST_BROKER_SS58,
        'VALIDATOR_SS58': TEST_VALIDATOR_SS58,
        'PAYOUT_COLDKEY_SS58': TEST_PAYOUT_COLDKEY_SS58,
        'SMART_CONTRACT_SS58': TEST_SMART_CONTRACT_SS58,
        'MINER_HOTKEY_SS58': TEST_MINER_HOTKEY_SS58,
        'MINER_COLDKEY_SS58': TEST_MINER_COLDKEY_SS58,
        'TRACKER_SHEET_ID': TEST_TRACKER_SHEET_ID,
        'MINING_TRACKER_SHEET_ID': TEST_MINING_TRACKER_SHEET_ID,
        'TRACKER_GOOGLE_CREDENTIALS': TEST_GOOGLE_CREDENTIALS_PATH,
        'SUBNET_ID': str(TEST_SUBNET_ID),
        'LOT_STRATEGY': TEST_LOT_STRATEGY.name,
        'TAOSTATS_RATE_LIMIT_SECONDS': str(TEST_RATE_LIMIT_SECONDS),
    }, clear=False):
        yield TrackerSettings()


@pytest.fixture
def mock_wave_settings():
    """
    Pytest fixture that provides mocked WaveAccountSettings with test defaults.
    
    Usage:
        def test_something(mock_wave_settings):
            # Use default values
            assert mock_wave_settings.alpha_asset_account == TEST_ALPHA_ASSET_ACCOUNT
            
            # Or modify for specific test
            mock_wave_settings.short_term_gain_account = "Custom Account"
    """
    # Create mock settings with test defaults
    with patch.dict('os.environ', {
        'WAVE_CONTRACT_INCOME_ACCOUNT': TEST_CONTRACT_INCOME_ACCOUNT,
        'WAVE_STAKING_INCOME_ACCOUNT': TEST_STAKING_INCOME_ACCOUNT,
        'WAVE_MINING_INCOME_ACCOUNT': TEST_MINING_INCOME_ACCOUNT,
        'WAVE_ALPHA_ASSET_ACCOUNT': TEST_ALPHA_ASSET_ACCOUNT,
        'WAVE_TAO_ASSET_ACCOUNT': TEST_TAO_ASSET_ACCOUNT,
        'WAVE_TRANSFER_PROCEEDS_ACCOUNT': TEST_TRANSFER_PROCEEDS_ACCOUNT,
        'WAVE_BLOCKCHAIN_FEE_ACCOUNT': TEST_BLOCKCHAIN_FEE_ACCOUNT,
        'WAVE_SHORT_TERM_GAIN_ACCOUNT': TEST_SHORT_TERM_GAIN_ACCOUNT,
        'WAVE_SHORT_TERM_LOSS_ACCOUNT': TEST_SHORT_TERM_LOSS_ACCOUNT,
        'WAVE_LONG_TERM_GAIN_ACCOUNT': TEST_LONG_TERM_GAIN_ACCOUNT,
        'WAVE_LONG_TERM_LOSS_ACCOUNT': TEST_LONG_TERM_LOSS_ACCOUNT,
    }, clear=False):
        yield WaveAccountSettings()


@pytest.fixture(autouse=True)
def mock_all_settings(mock_tracker_settings, mock_wave_settings):
    """
    Convenience fixture that provides both tracker and wave settings.
    
    Usage:
        def test_something(mock_all_settings):
            tracker_settings, wave_settings = mock_all_settings
            # Both are available with test defaults
    """
    return mock_tracker_settings, mock_wave_settings
