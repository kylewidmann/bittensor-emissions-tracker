"""Shared test fixtures for emissions tracker tests."""
# Mock sheets fixtures
from .mock_sheets import mock_sheets

# Mock client fixtures
from .mock_clients import mock_taostats_client

# Mock config fixtures and constants
from .mock_config import (
    mock_tracker_settings,
    mock_wave_settings,
    mock_all_settings,
    # Test constants
    TEST_BROKER_SS58,
    TEST_VALIDATOR_SS58,
    TEST_PAYOUT_COLDKEY_SS58,
    TEST_SMART_CONTRACT_SS58,
    TEST_MINER_HOTKEY_SS58,
    TEST_MINER_COLDKEY_SS58,
    TEST_TRACKER_SHEET_ID,
    TEST_MINING_TRACKER_SHEET_ID,
    TEST_GOOGLE_CREDENTIALS_PATH,
    TEST_SUBNET_ID,
    TEST_LOT_STRATEGY,
    TEST_RATE_LIMIT_SECONDS,
    TEST_CONTRACT_INCOME_ACCOUNT,
    TEST_STAKING_INCOME_ACCOUNT,
    TEST_MINING_INCOME_ACCOUNT,
    TEST_ALPHA_ASSET_ACCOUNT,
    TEST_TAO_ASSET_ACCOUNT,
    TEST_TRANSFER_PROCEEDS_ACCOUNT,
    TEST_BLOCKCHAIN_FEE_ACCOUNT,
    TEST_SHORT_TERM_GAIN_ACCOUNT,
    TEST_SHORT_TERM_LOSS_ACCOUNT,
    TEST_LONG_TERM_GAIN_ACCOUNT,
    TEST_LONG_TERM_LOSS_ACCOUNT,
)

__all__ = [
    # Fixtures
    'mock_sheets',
    'mock_taostats_client',
    'mock_tracker_settings',
    'mock_wave_settings',
    'mock_all_settings',
    # Constants
    'TEST_BROKER_SS58',
    'TEST_VALIDATOR_SS58',
    'TEST_PAYOUT_COLDKEY_SS58',
    'TEST_SMART_CONTRACT_SS58',
    'TEST_MINER_HOTKEY_SS58',
    'TEST_MINER_COLDKEY_SS58',
    'TEST_TRACKER_SHEET_ID',
    'TEST_MINING_TRACKER_SHEET_ID',
    'TEST_GOOGLE_CREDENTIALS_PATH',
    'TEST_SUBNET_ID',
    'TEST_LOT_STRATEGY',
    'TEST_RATE_LIMIT_SECONDS',
    'TEST_CONTRACT_INCOME_ACCOUNT',
    'TEST_STAKING_INCOME_ACCOUNT',
    'TEST_MINING_INCOME_ACCOUNT',
    'TEST_ALPHA_ASSET_ACCOUNT',
    'TEST_TAO_ASSET_ACCOUNT',
    'TEST_TRANSFER_PROCEEDS_ACCOUNT',
    'TEST_BLOCKCHAIN_FEE_ACCOUNT',
    'TEST_SHORT_TERM_GAIN_ACCOUNT',
    'TEST_SHORT_TERM_LOSS_ACCOUNT',
    'TEST_LONG_TERM_GAIN_ACCOUNT',
    'TEST_LONG_TERM_LOSS_ACCOUNT',
]