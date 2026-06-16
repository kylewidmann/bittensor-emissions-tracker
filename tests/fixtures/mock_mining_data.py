"""Mining-specific test data fixtures.

Loads test data from tests/data/mining/ for mining tracker tests.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict

import pytest

from emissions_tracker.models import AlphaLot, SourceType, TaoStatsStakeBalance

# Mining test data directory
MINING_TEST_DATA_DIR = Path(__file__).parent.parent / "data" / "mining"


@pytest.fixture
def mining_raw_stake_balance():
    """Load raw stake balance history from mining test data."""
    data_path = MINING_TEST_DATA_DIR / "stake_balance.json"
    with open(data_path) as f:
        return json.load(f)["data"]


@pytest.fixture
def mining_stake_balances(mining_raw_stake_balance):
    """Load stake balance data from mining test data."""
    stake_balances = []
    for balance in mining_raw_stake_balance:
        stake_balances.append(TaoStatsStakeBalance.from_json(balance))
    return stake_balances


@pytest.fixture
def mining_daily_stake_balances(
    mining_stake_balances: list[TaoStatsStakeBalance],
) -> Dict[str, TaoStatsStakeBalance]:
    """Group stake balances by day, keeping only the last balance of each day.

    Returns dict mapping day string to TaoStatsStakeBalance object.
    """
    balances_by_day: Dict[str, TaoStatsStakeBalance] = {}
    for b in mining_stake_balances:
        if b.day in balances_by_day:
            if b.timestamp > balances_by_day[b.day].timestamp:
                balances_by_day[b.day] = b
        else:
            balances_by_day[b.day] = b

    return balances_by_day


@pytest.fixture
def compute_expected_mining_staking_emission_lots(
    mining_daily_stake_balances: Dict[str, TaoStatsStakeBalance],
    historical_prices,  # Reuse from contract fixtures (same price data)
    get_alpha_lot_id: Callable[[], str],
) -> Callable[[datetime, datetime], list[AlphaLot]]:
    """Compute expected staking emissions for mining tracker from raw data.

    For mining, there are NO delegation/undelegation events to account for.
    Emissions are simply the balance increases from day to day.

    Returns:
        Function that computes emission lots for a given date range
    """

    def _compute_expected_mining_staking_emissions(
        start_date: datetime, end_date: datetime
    ) -> list[AlphaLot]:
        """Compute expected staking emissions from mining balance history."""
        from tests.utils import compute_staking_emissions_from_balances

        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())

        # For mining, pass empty dict for daily_stake_events (no delegation/undelegation events)
        return compute_staking_emissions_from_balances(
            daily_stake_balances=mining_daily_stake_balances,
            daily_stake_events={},  # Empty - mining has no delegation events
            start_ts=start_ts,
            end_ts=end_ts,
            get_tao_price_at_timestamp=historical_prices.get_price_for_timestamp,
            get_lot_id=get_alpha_lot_id,
            source_type=SourceType.MINING,
        )

    return _compute_expected_mining_staking_emissions
