import json
from pathlib import Path
import pytest

# Test data directory
TEST_DATA_DIR = Path(__file__).parent.parent / "data" / "all"


@pytest.fixture
def raw_stake_events():
    """Load raw stake events from test data."""
    data_path = TEST_DATA_DIR / "stake_events.json"
    with open(data_path) as f:
        return json.load(f)["data"]

@pytest.fixture
def raw_stake_balance():
    """Load raw stake balance history from test data."""
    data_path = TEST_DATA_DIR / "stake_balance.json"
    with open(data_path) as f:
        return json.load(f)["data"]
    
@pytest.fixture
def raw_historical_prices():
    """Load raw historical price data from test data."""
    data_path = TEST_DATA_DIR / "historical_tao_prices.json"
    with open(data_path) as f:
        return json.load(f)