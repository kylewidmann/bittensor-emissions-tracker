import json
from pathlib import Path
import pytest


@pytest.fixture
def raw_stake_events():
    """Load raw stake events from test data."""
    data_path = Path(__file__).parent.parent / "data" / "all" / "stake_events.json"
    with open(data_path) as f:
        return json.load(f)["data"]