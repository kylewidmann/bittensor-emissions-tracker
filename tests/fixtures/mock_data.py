import datetime
import json
from pathlib import Path
import pytest

# Test data directory
TEST_DATA_DIR = Path(__file__).parent.parent / "data" / "all"

@pytest.fixture
def raw_account_history():
    """Load raw account history data from test data."""
    data_path = TEST_DATA_DIR / "account_history.json"
    with open(data_path) as f:
        return json.load(f)["data"]

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
def raw_transfer_events():
    """Load raw transfer events from test data."""
    data_path = TEST_DATA_DIR / "transfers.json"
    with open(data_path) as f:
        return json.load(f)["data"]
    
@pytest.fixture
def raw_historical_prices():
    """Load raw historical price data from test data."""
    data_path = TEST_DATA_DIR / "historical_tao_prices.json"
    with open(data_path) as f:
        return json.load(f)
    
def get_tao_price_for_date(raw_historical_prices) -> float:
    """Get the historical TAO price for a given date.
    
    Args:
        dt: Datetime object
        
    Returns:
        TAO price in USD for that date
    """
    def _get_tao_price_for_date(dt: datetime) -> float:
        date_str = dt.strftime('%Y-%m-%d')
        if date_str in raw_historical_prices:
            return raw_historical_prices[date_str]['price']
        else:
            # Fallback to nearest available price
            raise ValueError(f"No historical price data for {date_str}")
        
    return _get_tao_price_for_date
