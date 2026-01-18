from collections import defaultdict
import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from annotated_types import T
import pytest

from emissions_tracker.models import AlphaLot, AlphaSale, CostBasisMethod, Expense, GainType, AlphaLotConsumption, LotStatus, SourceType, TaoLot, TaoStatsStakeBalance, TaoStatsDelegation, TaoStatsTransfer, TaoStatsAccountHistory, TaoTransfer
from tests.fixtures.mock_config import TEST_BROKER_SS58, TEST_PAYOUT_COLDKEY_SS58, TEST_SMART_CONTRACT_SS58, TEST_SUBNET_ID, TEST_VALIDATOR_SS58
from tests.utils import consume_alpha_lots_for_expense, consume_alpha_lots_for_sale, consume_tao_lots, filter_balances_by_date_range, filter_delegation_events_by_date_range
from datetime import datetime, timezone, timedelta

# Test data directory
TEST_DATA_DIR = Path(__file__).parent.parent / "data" / "all"
SECONDS_PER_DAY = 86400


class HistoricalPrices:
    """Helper class to manage historical TAO price data for tests.
    
    Provides convenient methods to look up prices by date or datetime,
    avoiding duplicated price lookup logic throughout tests.
    """
    
    def __init__(self, price_data: Dict[str, Any]):
        """Initialize with raw price data dict.
        
        Args:
            price_data: Dict with date strings as keys (YYYY-MM-DD) and price dicts as values.
                       Each price dict should have: date, timestamp, price
        """
        self._data = price_data
    
    def get_price_for_date(self, date: str) -> float:
        """Get price for a specific date string.
        
        Args:
            date: Date string in 'YYYY-MM-DD' format
            
        Returns:
            Price as float
            
        Raises:
            ValueError: If date not found in historical data
        """
        if date in self._data:
            return float(self._data[date]['price'])
        raise ValueError(f"No historical price data for {date}")
    
    def get_price_for_datetime(self, dt: datetime) -> float:
        """Get price for a datetime object.
        
        Args:
            dt: Datetime object
            
        Returns:
            Price as float
            
        Raises:
            ValueError: If date not found in historical data
        """
        date_str = dt.strftime('%Y-%m-%d')
        return self.get_price_for_date(date_str)
    
    def get_price_for_timestamp(self, timestamp: int) -> float:
        """Get price for a Unix timestamp.
        
        Args:
            timestamp: Unix timestamp (seconds since epoch)
            
        Returns:
            Price as float
            
        Raises:
            ValueError: If date not found in historical data
        """
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return self.get_price_for_datetime(dt)
    
    def get_all_prices(self) -> Dict[str, Any]:
        """Get the raw price data dict.
        
        Returns:
            Dict with date strings as keys and price dicts as values
        """
        return self._data
    
    def __contains__(self, date: str) -> bool:
        """Check if a date exists in the historical data.
        
        Args:
            date: Date string in 'YYYY-MM-DD' format
            
        Returns:
            True if date exists, False otherwise
        """
        return date in self._data


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

@pytest.fixture
def account_histories(raw_account_history):
    """Load account history data from test data.
    
    Returns list of TaoStatsAccountHistory objects.
    """
    account_histories = []
    for record in raw_account_history:
        account_histories.append(TaoStatsAccountHistory.from_json(record))
    return account_histories

@pytest.fixture
def stake_events(raw_stake_events):
    """Load stake events data from test data."""
    stake_events = []
    for event in raw_stake_events:
        stake_events.append(TaoStatsDelegation.from_json(event))
    return stake_events

@pytest.fixture
def stake_balances(raw_stake_balance):
    """Load stake balance data from test data."""
    stake_balances = []
    for balance in raw_stake_balance:
        stake_balances.append(TaoStatsStakeBalance.from_json(balance))
    return stake_balances
    
@pytest.fixture
def transfer_events(raw_transfer_events):
    """Load transfer events data from test data."""
    transfer_events = []
    for transfer in raw_transfer_events:
        transfer_events.append(TaoStatsTransfer.from_json(transfer))
    return transfer_events

@pytest.fixture
def historical_prices(raw_historical_prices):
    """Load historical price data from test data.
    
    Returns HistoricalPrices instance with convenient lookup methods.
    """
    return HistoricalPrices(raw_historical_prices)

@pytest.fixture
def daily_stake_balances(stake_balances: list[TaoStatsStakeBalance]) -> Dict[str, TaoStatsStakeBalance]:
    """Group stake balances by day, keeping only the last balance of each day.
    
    Returns list of TaoStatsStakeBalance objects, one per day.
    """
    balances_by_day: Dict[str,TaoStatsStakeBalance] = {}
    for b in stake_balances:
        if b.day in balances_by_day:
            if b.timestamp > balances_by_day[b.day].timestamp:
                balances_by_day[b.day] = b
        else:
            balances_by_day[b.day] = b
    
    return balances_by_day

@pytest.fixture
def daily_stake_events(stake_events: list[TaoStatsDelegation]) -> Dict[str, list[TaoStatsDelegation]]:
    """Group stake events by day.
    
    Returns dict mapping day string to list of TaoStatsDelegation objects for that day.
    """
    events_by_day = defaultdict(list[TaoStatsDelegation])
    for e in stake_events:
        events_by_day[e.day].append(e)
    return events_by_day

# @pytest.fixture
# def get_tao_price_for_date(raw_historical_prices) -> float:
#     """Get the historical TAO price for a given date.
    
#     Args:
#         dt: Datetime object
        
#     Returns:
#         TAO price in USD for that date
#     """
#     def _get_tao_price_for_date(dt: datetime) -> float:
#         date_str = dt.strftime('%Y-%m-%d')
#         if date_str in raw_historical_prices:
#             return raw_historical_prices[date_str]['price']
#         else:
#             # Fallback to nearest available price
#             raise ValueError(f"No historical price data for {date_str}")
        
#     return _get_tao_price_for_date

@pytest.fixture
def get_alpha_lot_id():
    """Generate unique ALPHA lot IDs for tests with optional reset capability.
    
    Usage:
        # Normal usage - counter keeps incrementing
        lot_id = get_alpha_lot_id()  # "ALPHA-0001"
        lot_id2 = get_alpha_lot_id()  # "ALPHA-0002"
        
        # Temporarily reset counter within a context
        with get_alpha_lot_id.fresh():
            lot_id = get_alpha_lot_id()  # "ALPHA-0001" (reset)
            lot_id2 = get_alpha_lot_id()  # "ALPHA-0002"
        # Counter restored to previous value after context exits
        lot_id3 = get_alpha_lot_id()  # "ALPHA-0003" (continues from before context)
    """
    from contextlib import contextmanager
    
    state = {'counter': 0}

    def _get_alpha_lot_id():
        state['counter'] += 1
        return f"ALPHA-{state['counter']:04d}"
    
    @contextmanager
    def context():
        """Context manager that temporarily resets counter, then restores it."""
        original = state['counter']
        state['counter'] = 0
        try:
            yield
        finally:
            state['counter'] = original
    
    _get_alpha_lot_id.context = context
    return _get_alpha_lot_id

@pytest.fixture
def get_tao_lot_id():
    """Generate unique TAO lot IDs for tests with optional reset capability.
    
    Usage:
        # Normal usage - counter keeps incrementing
        lot_id = get_tao_lot_id()  # "TAO-0001"
        lot_id2 = get_tao_lot_id()  # "TAO-0002"
        
        # Temporarily reset counter within a context
        with get_tao_lot_id.context():
            lot_id = get_tao_lot_id()  # "TAO-0001" (reset)
            lot_id2 = get_tao_lot_id()  # "TAO-0002"
        # Counter restored to previous value after context exits
    """
    from contextlib import contextmanager
    
    state = {'counter': 0}

    def _get_tao_lot_id():
        state['counter'] += 1
        return f"TAO-{state['counter']:04d}"
    
    @contextmanager
    def context():
        """Context manager that temporarily resets counter, then restores it."""
        original = state['counter']
        state['counter'] = 0
        try:
            yield
        finally:
            state['counter'] = original
    
    _get_tao_lot_id.context = context
    return _get_tao_lot_id

@pytest.fixture
def id_context(get_alpha_lot_id, get_tao_lot_id):
    """Reentrant context manager to reset lot ID counters for both ALPHA and TAO lot ID generators.
    
    If already inside an id_context, nested calls simply yield without resetting counters.
    This allows functions to safely wrap themselves in id_context() whether called
    directly or from within another context.
    """
    from contextlib import contextmanager
    
    state = {'depth': 0}

    @contextmanager
    def _id_context():
        if state['depth'] > 0:
            # Already inside a context, just yield without resetting
            state['depth'] += 1
            try:
                yield
            finally:
                state['depth'] -= 1
        else:
            # First entry, reset counters
            state['depth'] += 1
            try:
                with get_alpha_lot_id.context():
                    with get_tao_lot_id.context():
                        yield
            finally:
                state['depth'] -= 1

    return _id_context

@pytest.fixture
def get_expense_id():
    """Generate unique TAOcompute_expected_sales lot IDs for tests."""
    lot_counter = 0

    def _get_expense_id():
        nonlocal lot_counter
        lot_counter += 1
        return f"EXP-{lot_counter:04d}"

    return _get_expense_id

@pytest.fixture
def get_sale_id():
    """Generate unique Sale IDs for tests."""
    lot_counter = 0

    def _get_sale_id():
        nonlocal lot_counter
        lot_counter += 1
        return f"SALE-{lot_counter:04d}"

    return _get_sale_id

@pytest.fixture
def get_transfer_id():
    """Generate unique XFER IDs for tests."""
    lot_counter = 0

    def _get_transfer_id():
        nonlocal lot_counter
        lot_counter += 1
        return f"XFER-{lot_counter:04d}"

    return _get_transfer_id

@pytest.fixture
def get_opening_tao_lot(
    get_tao_lot_id: Callable[[], str],
    account_histories: List[TaoStatsAccountHistory],
    historical_prices: HistoricalPrices,
):

    def _get_opening_tao_lot(
        date: datetime  
    ):
        # Get balance from previous day
        target_date_str = (date - timedelta(days=1)).strftime('%Y-%m-%d')
        account_history = next(ah for ah in account_histories if ah.day == target_date_str)
        tao_balance_rao = account_history.balance_free_rao
        tao_price = historical_prices.get_price_for_date(target_date_str)

        return TaoLot(
            lot_id=get_tao_lot_id(),
            timestamp=int(datetime.combine(date.date(), datetime.min.time(), tzinfo=timezone.utc).timestamp()),
            block_number=0,
            rao=tao_balance_rao,
            rao_remaining=tao_balance_rao,
            usd_basis=tao_balance_rao / 1e9 * tao_price,
            usd_per_tao=tao_price,
            source_sale_id="",
            extrinsic_id="",
            status=LotStatus.OPEN,
            notes="Opening balance lot",

        )

    return _get_opening_tao_lot

@pytest.fixture
def get_opening_alpha_lot(
    get_alpha_lot_id: Callable[[], str],
    daily_stake_balances: Dict[str, TaoStatsStakeBalance],
    historical_prices: HistoricalPrices,
):

    def _get_opening_alpha_lot(
        date: datetime  
    ):
        # Get balance from previous day
        target_date_str = (date - timedelta(days=1)).strftime('%Y-%m-%d')
        balance = next(b for b in daily_stake_balances.values() if b.day == target_date_str)
        tao_price = historical_prices.get_price_for_date(target_date_str)
        usd_fmv = balance.balance_as_tao_float * tao_price
        usd_per_alpha = (balance.balance_as_tao_float* tao_price) / balance.balance_as_alpha_float if balance.balance_as_alpha_rao > 0 else 0


        return AlphaLot(
                lot_id=get_alpha_lot_id(),
                timestamp=balance.timestamp_unix,
                block_number=balance.block_number,
                alpha_rao=balance.balance_as_alpha_rao,
                alpha_rao_remaining=balance.balance_as_alpha_rao,
                usd_per_alpha=usd_per_alpha,
                usd_fmv=usd_fmv,
                tao_equivalent=balance.balance_as_tao_float,
                extrinsic_id="",
                transfer_address='',
                status=LotStatus.OPEN,
                source_type=SourceType.CONTRACT
        )

    return _get_opening_alpha_lot

@pytest.fixture
def compute_expected_contract_income_lots(
    stake_events: List[TaoStatsDelegation],
    get_alpha_lot_id: Callable[[], str],
) -> Callable[[int, int, str, int, str, str], List[AlphaLot]]:
    """Filter stake events for contract income by date range and contract address.
    
    Args:
        stake_events: Raw stake event data
        start_ts: Start timestamp (unix seconds)
        end_ts: End timestamp (unix seconds)
        contract_address: Filter events for this contract address
        netuid: Subnet ID to filter by
        delegate: Delegate (hotkey) address to filter by
        nominator: Nominator (coldkey) address to filter by
        
    Returns:
        List of stake events matching contract income criteria
    """
    
    def _compute_expected_contract_income_lots(
        start_ts: int,
        end_ts: int,
        contract_address: str = TEST_SMART_CONTRACT_SS58,
        netuid: int = TEST_SUBNET_ID,
        delegate: str = TEST_VALIDATOR_SS58,
        nominator: str = TEST_PAYOUT_COLDKEY_SS58
    ) -> List[AlphaLot]:
        filtered_events = filter_delegation_events_by_date_range(stake_events, start_ts, end_ts)
        contract_income_events = [
            event for event in filtered_events
            if (event.netuid == netuid
                and event.delegate.ss58 == delegate
                and event.nominator.ss58 == nominator
                and event.is_transfer == True
                and event.transfer_address.ss58 == contract_address)
        ]
        lots = []
        for e in contract_income_events:
            lots.append(AlphaLot(
                lot_id=get_alpha_lot_id(),
                timestamp=e.timestamp_unix,
                block_number=e.block_number,
                alpha_rao=int(e.alpha),
                alpha_rao_remaining=int(e.alpha),
                usd_per_alpha=float(e.usd) / (int(e.alpha) / 1e9) if int(e.alpha) > 0 else 0,
                usd_fmv=float(e.usd),
                tao_equivalent=e.tao,
                extrinsic_id=e.extrinsic_id,
                transfer_address=e.transfer_address.ss58 if e.transfer_address else '',
                status=LotStatus.OPEN,
                source_type=SourceType.CONTRACT
            ))
        
        return lots
    return _compute_expected_contract_income_lots

@pytest.fixture
def compute_expected_staking_emission_lots(
    daily_stake_balances: Dict[str, TaoStatsStakeBalance],
    daily_stake_events: Dict[str, list[TaoStatsDelegation]],
    historical_prices: HistoricalPrices,
    get_alpha_lot_id: Callable[[], str],
) -> Callable[[datetime, datetime], list[AlphaLot]]:
    """Compute expected staking emissions from raw data.
    
    This replicates the logic in process_staking_emissions:
    - For each balance point (skip first), calculate balance delta
    - Subtract DELEGATE events with is_transfer=True in window
    - Add back UNDELEGATE events in window
    - Sum up all positive emissions
    
    Args:
        balance_history: List of balance snapshots
        delegation_events: List of delegation/undelegation events
        start_date: Start of date range
        end_date: End of date range
        
    Returns:
        Tuple of (count of emission lots, total alpha emitted)
    """

    def _compute_expected_staking_emissions(
        start_date: datetime,
        end_date: datetime
    ) -> list[AlphaLot]:
        """Compute expected staking emissions from raw data.
        
        This replicates the logic in process_staking_emissions:
        - For each balance point (skip first), calculate balance delta
        - Subtract DELEGATE events with is_transfer=True in window
        - Add back UNDELEGATE events in window
        - Sum up all positive emissions
        
        Args:
            balance_history: List of balance snapshots
            delegation_events: List of delegation/undelegation events
            start_date: Start of date range
            end_date: End of date range
            
        Returns:
            List of emission lot dictionaries with usd_fmv and other values
        """ 
        # Extend window back by 1 day to get previous day's balance (matches tracker behavior)
        start_ts = int(start_date.timestamp()) - SECONDS_PER_DAY
        end_ts = int(end_date.timestamp())
        
        # Filter balance history to date range
        daily_balances = filter_balances_by_date_range(daily_stake_balances, start_ts, end_ts)

        emission_lots = []
        for i in range(1, len(daily_balances)):
            prev_day = daily_balances[i - 1]
            curr_day = daily_balances[i]

            # Get all events for current day
            day_events = daily_stake_events.get(curr_day.day, [])
            
            alpha_inflow_rao = sum(e.alpha for e in day_events if e.action == 'DELEGATE')
            alpha_outflow_rao = sum(e.alpha for e in day_events if e.action == 'UNDELEGATE')
            
            # Calculate alpha emissions in RAO
            # Balance change from end of previous day to end of current day (in RAO)
            balance_change_alpha_rao = curr_day.balance_as_alpha_rao - prev_day.balance_as_alpha_rao
            
            alpha_price_tao_rao = curr_day.balance_as_tao_rao / curr_day.balance_as_alpha_rao           
 
            emissions_alpha_rao = balance_change_alpha_rao - alpha_inflow_rao + alpha_outflow_rao

            # Get TAO price for current day
            timestamp = curr_day.timestamp_unix + SECONDS_PER_DAY - 1  # End of day timestamp
            tao_price = historical_prices.get_price_for_timestamp(timestamp)
            emissions_tao = (emissions_alpha_rao * alpha_price_tao_rao) / 1e9  # Convert new Alpha RAO to TAO RAO
            emissions_alpha = emissions_alpha_rao / 1e9  # Convert to TAO
            usd_fmv = emissions_tao * tao_price
            usd_per_alpha = usd_fmv / emissions_alpha if emissions_tao > 0 else 0


            emission_lots.append(AlphaLot(
                lot_id=get_alpha_lot_id(),
                timestamp=curr_day.timestamp_unix,
                block_number=curr_day.block_number,
                alpha_rao=emissions_alpha_rao,
                alpha_rao_remaining=emissions_alpha_rao,
                tao_equivalent=emissions_tao,
                usd_per_alpha=usd_per_alpha,
                usd_fmv=usd_fmv,
                source_type=SourceType.STAKING
            ))
        
        return emission_lots
    
    return _compute_expected_staking_emissions

@pytest.fixture
def compute_expected_expenses(
    stake_events: List[TaoStatsDelegation],
    get_expense_id: Callable[[], str],
):
    """
    Compute expected expenses from raw JSON data using seeded ALPHA lots.
    
    This mimics the tracker logic:
    1. Use the ALPHA lots already seeded (from seed_historical_lots)
    2. Process UNDELEGATE events with is_transfer=True to non-smart-contract as expenses
    3. Consume ALPHA lots using specified method to calculate cost basis
    4. Calculate FMV proceeds and realized gains
    
    Args:
        alpha_lots: The seeded ALPHA lots (from seed_sheets)
        start_date: Start date for processing
        end_date: End date for processing
        cost_basis_method: CostBasisMethod.FIFO or CostBasisMethod.HIFO
    
    Returns:
        List of expected expense dictionaries with computed values
    """

    def _compute_expected_expenses(
        alpha_lots: list[AlphaLot],
        start_date: datetime,
        end_date: datetime,
        cost_basis_method: CostBasisMethod = CostBasisMethod.HIFO
        ):
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        
        # Use the provided ALPHA lots (already seeded with historical data)
        # Make a deep copy to avoid modifying the original
        import copy
        alpha_lots = copy.deepcopy(alpha_lots)
        
        # Load expense events from the same file (UNDELEGATE with is_transfer=True to non-smart-contract)
        expense_undelegates: list[TaoStatsDelegation] = []
        for e in stake_events:
            if (e.action == 'UNDELEGATE' and 
                e.is_transfer == True and
                e.transfer_address is not None and
                e.transfer_address.ss58 != TEST_SMART_CONTRACT_SS58 and
                start_ts <= e.timestamp_unix <= end_ts
            ):
                expense_undelegates.append(e)
        
        # Sort expenses chronologically to match MockTaoStatsClient behavior
        # The mock client now returns delegations in timestamp_asc order
        expense_undelegates.sort(key=lambda x: x.timestamp_unix)
        
        # Process each expense
        expected_expenses = []
        
        for event in expense_undelegates:
            expense = consume_alpha_lots_for_expense(
                get_expense_id(),
                alpha_lots,
                event,
                cost_basis_method
            )
            
            expected_expenses.append(expense)
        
        return expected_expenses

    return _compute_expected_expenses


@pytest.fixture
def compute_expected_deposit_lots(
    transfer_events: list[TaoStatsTransfer],
    historical_prices: HistoricalPrices,
    get_tao_lot_id: Callable[[], str],
) -> Callable[[datetime, datetime, str], list[TaoLot]]:
    """
    Compute TAO lots from incoming deposits (transfers TO the wallet).
    
    These are TAO transfers from external sources that increase the wallet's
    TAO balance but aren't from ALPHA sales.
    """
    
    def _compute_expected_deposit_lots(
        start_date: datetime,
        end_date: datetime,
        wallet_address: str,
    ) -> list[TaoLot]:
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        
        # Find incoming transfers TO the wallet (deposits)
        deposits = [
            t for t in transfer_events
            if t.to_address is not None
            and t.to_address.ss58 == wallet_address
            and start_ts <= t.timestamp_unix <= end_ts
        ]
        
        deposit_lots = []
        for deposit in deposits:
            # Get TAO price for the deposit date
            timestamp = ((deposit.timestamp_unix + SECONDS_PER_DAY // 2) // SECONDS_PER_DAY) * SECONDS_PER_DAY
            tao_price = historical_prices.get_price_for_timestamp(timestamp)
            
            deposit_lots.append(TaoLot(
                lot_id=get_tao_lot_id(),
                timestamp=deposit.timestamp_unix,
                block_number=deposit.block_number,
                rao=deposit.amount_rao,
                rao_remaining=deposit.amount_rao,
                usd_basis=deposit.amount_rao / 1e9 * tao_price,
                usd_per_tao=tao_price,
                source_sale_id="",
                extrinsic_id=deposit.extrinsic_id or "",
                status=LotStatus.OPEN,
                notes=f"Deposit from {deposit.from_address.ss58[:8]}..." if deposit.from_address else "Deposit",
            ))
        
        return deposit_lots
    
    return _compute_expected_deposit_lots


@pytest.fixture
def compute_expected_sales(
    stake_events: list[TaoStatsDelegation],
    transfer_events: list[TaoStatsTransfer],
    compute_expected_contract_income_lots,
    compute_expected_staking_emission_lots,
    get_opening_alpha_lot,
    get_sale_id,
    get_tao_lot_id,
    id_context,
) -> Callable[[datetime, datetime, Optional[CostBasisMethod]], tuple[list[AlphaSale], list[TaoLot]]]:
    """
    Compute expected sales from raw JSON data.
    
    This mimics the tracker logic:
    1. Create ALPHA lots from emissions (daily balance increases)
    2. Process UNDELEGATE events (is_transfer=None) as sales
    3. Consume ALPHA lots using specified method to calculate cost basis
    4. Calculate slippage, fees, and realized gains
    
    Args:
        balance_json_path: Path to stake balance JSON file
        events_json_path: Path to stake events JSON file
        prices_json_path: Path to historical TAO prices JSON file
        start_date: Start date for processing
        end_date: End date for processing
        cost_basis_method: CostBasisMethod.FIFO or CostBasisMethod.HIFO
    
    Returns:
        List of expected sale dicts with keys: timestamp, alpha_disposed, tao_received,
        cost_basis, tao_slippage, network_fee_tao, network_fee_usd, realized_gain_loss
    """

    def _compute_expected_sales(
        start_date: datetime,
        end_date: datetime,
        cost_basis_method: Optional[CostBasisMethod] = CostBasisMethod.HIFO,
        opening_lot_date: datetime = None,
    ) -> tuple[list[AlphaSale], list[TaoLot]]:
        
        # Use reentrant id_context - resets counters if called directly,
        # reuses existing context if called from compute_expected_transfers
        with id_context():
            start_ts = int(start_date.timestamp())
            end_ts = int(end_date.timestamp())
            
            # Use opening_lot_date for ALPHA lots if provided, otherwise fall back to start_date
            alpha_start_date = opening_lot_date if opening_lot_date is not None else start_date
            alpha_start_ts = int(alpha_start_date.timestamp())
            
            # Build ALPHA lots for sale consumption
            opening_alpha_lot = get_opening_alpha_lot(alpha_start_date)
            income_lots: list[AlphaLot] = compute_expected_contract_income_lots(
                alpha_start_ts,
                end_ts
            )
            staking_lots: list[AlphaLot] = compute_expected_staking_emission_lots(
                alpha_start_date,
                end_date
            )
            
            # Combine and sort by timestamp to match seed_historical_lots behavior
            alpha_lots = [opening_alpha_lot] + income_lots + staking_lots
            alpha_lots.sort(key=lambda x: x.timestamp)
            
            # Reassign lot IDs sequentially after sorting, matching seed_historical_lots
            for i, lot in enumerate(alpha_lots, start=1):
                lot.lot_id = f"ALPHA-{i:04d}"
                
            # Step 2: Process UNDELEGATE events as sales
            # Filter for UNDELEGATE events with is_transfer=None (user-initiated sales)
            sales = [
                e for e in stake_events
                if e.action == 'UNDELEGATE' 
                and e.is_transfer is None
                and start_ts <= e.timestamp_unix <= end_ts
            ]
            
            # Sort by timestamp
            sales.sort(key=lambda x: x.timestamp_unix)
            
            expected_sales = []
            tao_lots = []
            for sale in sales:
                # Find associated transfers in the same extrinsic (network fees to fee collector)
                # These ARE deducted from the TAO received
                associated_transfers = [
                    t for t in transfer_events
                    if t.extrinsic_id == sale.extrinsic_id
                ]

                alpha_sale, tao_lot = consume_alpha_lots_for_sale(
                    get_sale_id(),
                    get_tao_lot_id(),
                    alpha_lots,
                    sale,
                    associated_transfers,
                    cost_basis_method
                )

                expected_sales.append(alpha_sale)
                tao_lots.append(tao_lot)
            
            return expected_sales, tao_lots
    
    return _compute_expected_sales

@pytest.fixture
def compute_expected_transfers(
    transfer_events: list[TaoStatsTransfer],
    compute_expected_sales: Callable[[datetime, datetime, Optional[CostBasisMethod]], tuple[list[AlphaSale], list[TaoLot]]],
    compute_expected_deposit_lots: Callable[[datetime, datetime, str], list[TaoLot]],
    get_transfer_id: Callable[[], str],
    id_context,
    get_opening_tao_lot: Callable[[datetime], TaoLot],
    historical_prices: HistoricalPrices,
) -> Callable[[datetime, datetime, str, str, Optional[CostBasisMethod], Optional[datetime]], tuple[list[AlphaSale], list[TaoLot]]]:
    """
    Compute expected transfers from raw JSON data.
    
    Calculates TAO lots from raw sales data (UNDELEGATE events),
    then simulates transfer processing to calculate expected values.
    
    Args:
        transfer_events: Transfer events
    
    Returns:
        List of expected transfer dictionaries with computed values
    """

    def _compute_expected_transfers(
        start_date: datetime,
        end_date: datetime,
        wallet_address: str,
        brokerage_address: str,
        cost_basis_method: CostBasisMethod = CostBasisMethod.HIFO,
        opening_lot_date: datetime = None,
    ) -> tuple[list[AlphaSale], list[TaoLot]]:
        """
        Compute expected transfers for a date range.
        
        Args:
            start_date: Start date for transfers to process
            end_date: End date for transfers to process
            wallet_address: Wallet address to filter transfers from
            brokerage_address: Brokerage address to filter transfers to
            cost_basis_method: FIFO or HIFO lot consumption method
            opening_lot_date: Date to use for opening TAO lot (if None, uses start_date)
        """
        
        # Reset both ALPHA and TAO lot ID counters to match how seeding works
        # Opening lot gets TAO-0001, deposit gets TAO-0002, then sales get TAO-0003+
        with id_context():
            start_ts = int(start_date.timestamp())
            end_ts = int(end_date.timestamp())
            
            # Use opening_lot_date if provided, otherwise fall back to start_date
            opening_date = opening_lot_date if opening_lot_date is not None else start_date
            opening_tao_lot = get_opening_tao_lot(opening_date)
            
            # Get deposit TAO lots BEFORE sales - they were seeded first
            deposit_lots = compute_expected_deposit_lots(
                start_date,
                end_date,
                wallet_address,
            )
            
            # Now get sales - they will get IDs after opening and deposits
            # Pass opening_date so ALPHA lots are computed from the same date as seeding
            sales, sale_tao_lots = compute_expected_sales(
                start_date,
                end_date,
                opening_lot_date=opening_date,
            )
            
            tao_lots = [opening_tao_lot] + deposit_lots + sale_tao_lots
            
            # Sort sales by timestamp
            sales.sort(key=lambda x: x.timestamp)
            
            # Step 2: Load and process transfer events
            # Filter transfers to brokerage in date range
            brokerage_transfers = [
                e for e in transfer_events
                if e.to_address is not None
                and e.to_address.ss58 == brokerage_address 
                and e.from_address is not None
                and e.from_address.ss58 == wallet_address 
                and start_ts <= e.timestamp_unix <= end_ts
            ]

            # Sort transfers chronologically (oldest first)
            # Note: We're reading from raw JSON which is in reverse chronological order
            # The MockTaoStatsClient sorts its output, but this function reads raw JSON directly
            brokerage_transfers.sort(key=lambda x: x.timestamp_unix)
            
            # Sort TAO lots for consumption based on cost basis method
            if cost_basis_method == CostBasisMethod.FIFO:
                sorted_tao_lots: list[TaoLot] = sorted(tao_lots, key=lambda x: x.timestamp)
            else:  # HIFO
                sorted_tao_lots: list[TaoLot] = sorted(tao_lots, key=lambda x: -x.usd_per_tao)
            
            # Step 5: Process each transfer chronologically
            expected_transfers = []
            for taostats_transfer in brokerage_transfers:
                # Round timestamp to nearest day (add half day before truncating)
                timestamp = ((taostats_transfer.timestamp_unix + SECONDS_PER_DAY // 2) // SECONDS_PER_DAY) * SECONDS_PER_DAY
                tao_price = historical_prices.get_price_for_timestamp(timestamp)
                transfer = consume_tao_lots(get_transfer_id(), sorted_tao_lots, taostats_transfer, tao_price)
                expected_transfers.append(transfer)
        
            return expected_transfers

    return _compute_expected_transfers