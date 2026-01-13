import json
from pathlib import Path
from typing import Any, Dict
import pytest

from emissions_tracker.models import CostBasisMethod
from tests.fixtures.mock_config import TEST_SMART_CONTRACT_SS58
from tests.utils import calculate_daily_emissions, filter_balances_by_date_range, filter_delegation_events, group_balances_by_day, group_events_by_day
from datetime import datetime, timezone

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

@pytest.fixture
def compute_expected_staking_emissions(
    raw_stake_balance: list[dict],
    raw_stake_events: list[dict],
) -> tuple[int, float]:
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
    ) -> tuple[int, float]:
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
        balance_history = raw_stake_balance
        delegation_events = raw_stake_events 

        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        
        # Filter balance history to date range
        balances = filter_balances_by_date_range(balance_history, start_ts, end_ts)
        
        # Convert and filter delegation events
        events = filter_delegation_events(delegation_events, start_ts, end_ts)
        
        # Group balances by day, keeping only the last balance of each day
        daily_balances = group_balances_by_day(balances)
        
        # Group events by day
        events_by_day = group_events_by_day(events)
        
        # Calculate emissions per day (comparing consecutive days)
        _, emission_count, total_alpha_emitted = calculate_daily_emissions(
            daily_balances, 
            events_by_day,
            price_per_tao=20.0,  # Fixed price for emissions test
            emission_threshold=0.0001
        )
        
        return emission_count, total_alpha_emitted
    
    return _compute_expected_staking_emissions

@pytest.fixture
def compute_expected_expenses(
    raw_stake_events: list[Dict[str, Any]],
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
        alpha_lots: list[Dict[str, Any]],
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
        
        # Sort lots for consumption based on method
        if cost_basis_method == CostBasisMethod.FIFO:
            # FIFO: Sort by timestamp (oldest first)
            sorted_lots = sorted(alpha_lots, key=lambda x: x['timestamp'])
        else:  # HIFO
            # HIFO: Sort by USD per ALPHA descending (highest cost first)
            sorted_lots = sorted(alpha_lots, key=lambda x: -x['usd_per_alpha'])
        
        # Load expense events from the same file (UNDELEGATE with is_transfer=True to non-smart-contract)
        expense_undelegates = []
        for e in raw_stake_events:
            # Convert ISO timestamp string to Unix timestamp if needed
            if isinstance(e.get('timestamp'), str):
                from datetime import datetime
                timestamp = int(datetime.fromisoformat(e['timestamp'].replace('Z', '+00:00')).timestamp())
            else:
                timestamp = e['timestamp']
            
            if (e['action'] == 'UNDELEGATE' and 
                e.get('is_transfer') == True and
                e.get('transfer_address', {}).get('ss58') != TEST_SMART_CONTRACT_SS58 and
                start_ts <= timestamp <= end_ts):
                # Store normalized data
                normalized_event = e.copy()
                normalized_event['timestamp'] = timestamp
                # Convert RAO to ALPHA if needed
                if isinstance(e.get('alpha'), str):
                    normalized_event['alpha'] = int(e['alpha']) / 1e9
                else:
                    normalized_event['alpha'] = e['alpha']
                # Convert string USD to float if needed
                if isinstance(e.get('usd'), str):
                    normalized_event['usd'] = float(e['usd'])
                # Convert string fee to float if needed
                if isinstance(e.get('fee'), str):
                    normalized_event['fee'] = int(e['fee']) / 1e9 if e['fee'] != "0" else 0.0
                
                expense_undelegates.append(normalized_event)
        
        # DON'T sort - keep in raw JSON order to match how the mock wallet client returns them
        # This ensures lot consumption order matches between expected and actual
        
        # Process each expense
        expected_expenses = []
        
        for event in expense_undelegates:
            alpha_disposed = event['alpha']
            
            # Extract transfer address
            transfer_address_data = event.get('transfer_address')
            if isinstance(transfer_address_data, dict):
                transfer_address = transfer_address_data.get('ss58', '')
            else:
                transfer_address = transfer_address_data or ''
            
            # Calculate USD proceeds based on ALPHA's FMV
            usd_proceeds = event.get('usd', 0.0)
            
            # Network fee (in USD if available, otherwise 0)
            fee_usd = event.get('fee_usd', 0.0)
            if not fee_usd and event.get('fee'):
                # If fee is in RAO, convert to ALPHA then to USD
                fee_alpha = event['fee'] if isinstance(event['fee'], float) else float(event['fee'])
                alpha_price_usd = event.get('alpha_price_in_usd', 0.0)
                if isinstance(alpha_price_usd, str):
                    alpha_price_usd = float(alpha_price_usd)
                if alpha_price_usd:
                    fee_usd = fee_alpha * alpha_price_usd
            
            # Consume ALPHA lots
            remaining_to_consume = alpha_disposed
            consumed_lots = []
            total_cost_basis = 0.0
            oldest_acquisition_ts = None
            
            for lot in sorted_lots:
                if remaining_to_consume <= 0:
                    break
                
                if lot['alpha_remaining'] > 0:
                    consume_amount = min(remaining_to_consume, lot['alpha_remaining'])
                    
                    # Calculate pro-rata cost basis
                    cost_basis_consumed = (consume_amount / lot['alpha_quantity']) * lot['usd_fmv']
                    
                    consumed_lots.append({
                        'lot_id': lot['lot_id'],
                        'alpha_consumed': consume_amount,
                        'cost_basis_consumed': cost_basis_consumed,
                        'acquisition_timestamp': lot['timestamp']
                    })
                    
                    total_cost_basis += cost_basis_consumed
                    lot['alpha_remaining'] -= consume_amount
                    remaining_to_consume -= consume_amount
                    
                    if oldest_acquisition_ts is None:
                        oldest_acquisition_ts = lot['timestamp']
            
            if remaining_to_consume > 0:
                raise ValueError(f"Insufficient ALPHA lots: need {alpha_disposed}, only have {alpha_disposed - remaining_to_consume}")
            
            # Determine gain type based on holding period (1 year)
            holding_period_days = (event['timestamp'] - oldest_acquisition_ts) / 86400
            gain_type = 'Long-term' if holding_period_days >= 365 else 'Short-term'
            
            # Calculate realized gain/loss: FMV - cost basis - fees
            realized_gain_loss = usd_proceeds - total_cost_basis - fee_usd
            
            expected_expense = {
                'timestamp': event['timestamp'],
                'block_number': event['block_number'],
                'transfer_address': transfer_address,
                'alpha_disposed': alpha_disposed,
                'tao_received': 0.0,  # No TAO involved in direct ALPHA transfers
                'tao_price_usd': 0.0,
                'usd_proceeds': usd_proceeds,
                'cost_basis': total_cost_basis,
                'realized_gain_loss': realized_gain_loss,
                'gain_type': gain_type,
                'network_fee_tao': 0.0,
                'network_fee_usd': fee_usd,
                'consumed_lots': consumed_lots,
                'extrinsic_id': event.get('extrinsic_id'),
            }
            
            expected_expenses.append(expected_expense)
        
        return expected_expenses

    return _compute_expected_expenses

@pytest.fixture
def compute_expected_sales(
    raw_stake_balance: list[Dict[str, Any]],
    raw_stake_events: list[Dict[str, Any]],
    raw_historical_prices: Dict[str, Any]
) -> list[Dict[str, Any]]:
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
        cost_basis_method: CostBasisMethod = CostBasisMethod.HIFO
    ):
        
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        
        # Step 1: Build ALPHA lots from emissions using shared utilities
        balances = filter_balances_by_date_range(raw_stake_balance, start_ts, end_ts)
        daily_balances = group_balances_by_day(balances)
        events = filter_delegation_events(raw_stake_events, start_ts, end_ts)
        events_by_day = group_events_by_day(events)
        
        # Create a price lookup function for historical TAO prices
        def price_lookup(day_str: str) -> float:
            """Look up TAO price for a specific day."""
            return raw_historical_prices.get(day_str, {}).get('price', 0.0)
        
        # Create ALPHA lots from daily emissions using historical TAO prices
        alpha_lots, _, _ = calculate_daily_emissions(
            daily_balances,
            events_by_day,
            price_lookup=price_lookup,
            emission_threshold=0.0001
        )
        
        # Add opening lot from actual ALPHA balance in account_history.json
        data_dir = Path(__file__).parent.parent / "data" / "all"
        with open(data_dir / "account_history.json") as f:
            account_history = json.load(f)['data']
        
        # Find balance on or before start_date
        opening_alpha = None
        # Make start_date timezone-aware for comparison
        start_date_aware = start_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        
        for record in account_history:
            record_dt = datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
            record_date_only = record_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            
            if record_date_only <= start_date_aware:
                opening_alpha = int(record['balance_staked']) / 1e9
                break
        
        if opening_alpha is None:
            raise ValueError(f"No account balance found on or before {start_date.strftime('%Y-%m-%d')}")
        
        opening_lot_date = start_date
        opening_lot_date_str = opening_lot_date.strftime('%Y-%m-%d')
        opening_tao_price = raw_historical_prices.get(opening_lot_date_str, {}).get('price', 459.702244010299)
        
        opening_lot = {
            'timestamp': int(opening_lot_date.timestamp()),
            'alpha_quantity': opening_alpha,
            'alpha_remaining': opening_alpha,
            'usd_fmv': opening_alpha * opening_tao_price,
            'status': 'Open'
        }
        alpha_lots.insert(0, opening_lot)
        
        # Add contract income lots
        from tests.utils import filter_contract_income_events
        from tests.fixtures.mock_config import (
            TEST_SMART_CONTRACT_SS58,
            TEST_SUBNET_ID,
            TEST_VALIDATOR_SS58,
            TEST_PAYOUT_COLDKEY_SS58
        )
        
        contract_events = filter_contract_income_events(
            raw_stake_events,
            start_ts,
            end_ts,
            contract_address=TEST_SMART_CONTRACT_SS58,
            netuid=TEST_SUBNET_ID,
            delegate=TEST_VALIDATOR_SS58,
            nominator=TEST_PAYOUT_COLDKEY_SS58
        )
        
        for event in contract_events:
            event_ts = int(datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00')).timestamp())
            event_date = datetime.fromtimestamp(event_ts).strftime('%Y-%m-%d')
            
            alpha_quantity = int(event['alpha']) / 1e9
            usd_fmv = float(event.get('usd', 0))
            
            contract_lot = {
                'timestamp': event_ts,
                'alpha_quantity': alpha_quantity,
                'alpha_remaining': alpha_quantity,
                'usd_fmv': usd_fmv,
                'status': 'Open'
            }
            alpha_lots.append(contract_lot)
        
        # Sort lots based on cost basis method
        if cost_basis_method == CostBasisMethod.FIFO:
            # First In First Out - consume oldest lots first
            alpha_lots.sort(key=lambda x: x['timestamp'])
        elif cost_basis_method == CostBasisMethod.HIFO:
            # Highest In First Out - consume highest cost basis lots first
            alpha_lots.sort(key=lambda x: x['usd_fmv'] / x['alpha_quantity'], reverse=True)
        else:
            raise ValueError(f"Invalid cost_basis_method: {cost_basis_method}. Must be CostBasisMethod.FIFO or CostBasisMethod.HIFO")
        
        # Step 2: Process UNDELEGATE events as sales
        # Filter for UNDELEGATE events with is_transfer=None (user-initiated sales)
        undelegate_events = []
        for e in raw_stake_events:
            event_ts = int(datetime.fromisoformat(e['timestamp'].replace('Z', '+00:00')).timestamp())
            if (start_ts <= event_ts <= end_ts and 
                e['action'] == 'UNDELEGATE' and 
                e.get('is_transfer') is None):
                undelegate_events.append(e)
        
        # Sort by timestamp
        undelegate_events.sort(key=lambda x: datetime.fromisoformat(x['timestamp'].replace('Z', '+00:00')).timestamp())
        
        expected_sales = []
        
        for event in undelegate_events:
            timestamp = int(datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00')).timestamp())
            alpha_disposed = int(event['alpha']) / 1e9
            tao_amount = float(event['amount']) / 1e9
            fee_tao = float(event['fee']) / 1e9
            slippage_ratio = float(event.get('slippage', 0))
            usd_proceeds = float(event.get('usd', 0))
            
            # Calculate TAO price from USD proceeds (already in event data)
            tao_price = usd_proceeds / tao_amount if tao_amount > 0 else 0.0
            
            # Calculate expected TAO (before slippage)
            if slippage_ratio and abs(1 - slippage_ratio) > 1e-9:
                tao_expected = tao_amount / (1 - slippage_ratio)
            else:
                # Fallback: use alpha_price_in_tao if available
                alpha_price_in_tao = event.get('alpha_price_in_tao')
                if alpha_price_in_tao:
                    tao_expected = alpha_disposed * float(alpha_price_in_tao)
                else:
                    tao_expected = tao_amount
            
            tao_slippage = tao_expected - tao_amount
            slippage_usd = tao_slippage * tao_price
            fee_usd = fee_tao * tao_price
            
            # Consume ALPHA lots FIFO to calculate cost basis
            cost_basis = 0.0
            remaining_need = alpha_disposed
            
            for lot in alpha_lots:
                if remaining_need <= 0:
                    break
                
                if lot['alpha_remaining'] <= 0:
                    continue
                
                to_consume = min(lot['alpha_remaining'], remaining_need)
                basis_consumed = (to_consume / lot['alpha_quantity']) * lot['usd_fmv']
                
                cost_basis += basis_consumed
                lot['alpha_remaining'] -= to_consume
                remaining_need -= to_consume
                
                if lot['alpha_remaining'] == 0:
                    lot['status'] = 'Closed'
                else:
                    lot['status'] = 'Partial'
            
            # Calculate realized gain/loss
            realized_gain_loss = usd_proceeds - cost_basis - fee_usd
            
            expected_sales.append({
                'timestamp': timestamp,
                'alpha_disposed': alpha_disposed,
                'tao_received': tao_amount,
                'cost_basis': cost_basis,
                'tao_expected': tao_expected,
                'tao_slippage': tao_slippage,
                'slippage_usd': slippage_usd,
                'network_fee_tao': fee_tao,
                'network_fee_usd': fee_usd,
                'usd_proceeds': usd_proceeds,
                'realized_gain_loss': realized_gain_loss
            })
        
        return expected_sales
    
    return _compute_expected_sales

@pytest.fixture
def compute_expected_transfers(
    raw_transfer_events,
    raw_historical_prices,
    mock_sheets,
) -> list[Dict[str, Any]]:
    """
    Compute expected transfers from raw JSON data using TAO lots from the tracker.
    
    This reads the TAO lots that the tracker created (from processing sales)
    and simulates transfer processing to calculate expected values.
    
    Args:
        raw_transfer_data: Raw transfer events
        raw_historical_prices: Historical TAO price data
        mock_sheets: Mock sheets environment to read TAO lots
    
    Returns:
        List of expected transfer dictionaries with computed values
    """

    def _compute_expected_transfers(
        sheet_id: str,
        start_date: datetime,
        end_date: datetime,
        wallet_address: str,
        brokerage_address: str,
        cost_basis_method: CostBasisMethod = CostBasisMethod.FIFO
    ):
        
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        historical_prices = raw_historical_prices
        
        # Helper function to get price for a specific date string (YYYY-MM-DD)
        def price_lookup(date_str: str) -> float:
            return historical_prices[date_str]['price']
        
        # Step 1: Read TAO lots from the tracker's TAO Lots sheet
        # These were created by the tracker when processing sales
        spreadsheet = mock_sheets.client.spreadsheets.get(sheet_id)
        tao_lots_sheet = spreadsheet.worksheet("TAO Lots")
        tao_lot_rows = tao_lots_sheet.get_all_records()
        
        # Convert to dict format for consumption tracking
        tao_lots = []
        for row in tao_lot_rows:
            tao_lots.append({
                'lot_id': row['TAO Lot ID'],
                'timestamp': int(datetime.fromisoformat(row['Date']).timestamp()) if isinstance(row['Date'], str) else row['Date'],
                'tao_quantity': float(row['TAO Quantity']),
                'tao_remaining': float(row['TAO Remaining']),
                'usd_basis': float(row['USD Basis']),
                'usd_per_tao': float(row['USD Basis']) / float(row['TAO Quantity']) if float(row['TAO Quantity']) > 0 else 0,
                'block_number': row.get('Block', 0),
                'extrinsic_id': row.get('Extrinsic ID', '')
            })
        
        # Step 2: Load and process transfer events
        # Step 2: Load and process transfer events
        # Filter transfers to brokerage in date range
        brokerage_transfers = []
        for t in raw_transfer_events:
            if isinstance(t.get('timestamp'), str):
                timestamp = int(datetime.fromisoformat(t['timestamp'].replace('Z', '+00:00')).timestamp())
            else:
                timestamp = t['timestamp']
            
            # Extract to address
            to_addr_data = t.get('to')
            if isinstance(to_addr_data, dict):
                to_addr = to_addr_data.get('ss58', '')
            else:
                to_addr = to_addr_data or ''
            
            # Extract from address
            from_addr_data = t.get('from')
            if isinstance(from_addr_data, dict):
                from_addr = from_addr_data.get('ss58', '')
            else:
                from_addr = from_addr_data or ''
            
            if (to_addr == brokerage_address and 
                from_addr == wallet_address and
                start_ts <= timestamp <= end_ts):
                
                # Convert amounts
                amount_tao = int(t['amount']) / 1e9 if isinstance(t.get('amount'), str) else t['amount']
                fee_tao = int(t['fee']) / 1e9 if isinstance(t.get('fee'), str) and t.get('fee') else 0.0
                
                brokerage_transfers.append({
                    'timestamp': timestamp,
                    'block_number': t['block_number'],
                    'brokerage_amount': amount_tao,
                    'fee_tao': fee_tao,
                    'total_outflow': amount_tao + fee_tao,
                    'transaction_hash': t.get('transaction_hash'),
                    'extrinsic_id': t.get('extrinsic_id')
                })
        
        # DON'T sort - keep in raw JSON order to match how the mock wallet client returns them
        # The tracker processes transfers in the order received from the API
        
        # Sort TAO lots for consumption based on cost basis method
        if cost_basis_method == CostBasisMethod.FIFO:
            sorted_tao_lots = sorted(tao_lots, key=lambda x: x['timestamp'])
        else:  # HIFO
            sorted_tao_lots = sorted(tao_lots, key=lambda x: -x['usd_per_tao'])
        
        # Step 5: Process each transfer chronologically
        expected_transfers = []
        transfer_counter = 1
        
        for transfer in brokerage_transfers:
            brokerage_amount = transfer['brokerage_amount']
            total_outflow = transfer['total_outflow']
            fee_tao = transfer['fee_tao']
            transfer_ts = transfer['timestamp']
            
            # Only consider TAO lots created BEFORE this transfer
            available_lots = [lot for lot in sorted_tao_lots if lot['timestamp'] <= transfer_ts and lot['tao_remaining'] > 0]
            available_tao = sum(lot['tao_remaining'] for lot in available_lots)
            
            # Verify sufficient TAO lots exist - if not, this indicates bad data or preprocessing
            assert available_tao >= total_outflow, (
                f"Insufficient TAO lots for transfer at {datetime.fromtimestamp(transfer_ts)}: "
                f"need {total_outflow:.4f} TAO but only {available_tao:.4f} available. "
                f"This suggests incorrect sales processing or missing TAO lots."
            )
            
            # Consume TAO lots for total outflow (brokerage + fees)
            remaining_to_consume = total_outflow
            consumed_lots = []
            total_cost_basis = 0.0
            oldest_acquisition_ts = None
            
            for lot in available_lots:  # Use available_lots instead of sorted_tao_lots
                if remaining_to_consume <= 0:
                    break
                
                if lot['tao_remaining'] > 0:
                    consume_amount = min(remaining_to_consume, lot['tao_remaining'])
                    
                    # Calculate pro-rata cost basis
                    cost_basis_consumed = (consume_amount / lot['tao_quantity']) * lot['usd_basis']
                    
                    consumed_lots.append({
                        'lot_id': lot['lot_id'],
                        'tao_consumed': consume_amount,
                        'cost_basis_consumed': cost_basis_consumed,
                        'acquisition_timestamp': lot['timestamp']
                    })
                    
                    total_cost_basis += cost_basis_consumed
                    lot['tao_remaining'] -= consume_amount
                    remaining_to_consume -= consume_amount
                    
                    if oldest_acquisition_ts is None:
                        oldest_acquisition_ts = lot['timestamp']
            
            # Allocate cost basis proportionally between brokerage and fees
            cost_basis_for_brokerage = (total_cost_basis * (brokerage_amount / total_outflow)) if total_outflow > 0 else 0.0
            fee_cost_basis = total_cost_basis - cost_basis_for_brokerage
            
            # Get price for this transfer date
            transfer_date = datetime.fromtimestamp(transfer['timestamp']).strftime('%Y-%m-%d')
            tao_price_at_transfer = price_lookup(transfer_date)
            
            # Calculate proceeds (only for brokerage amount)
            usd_proceeds = brokerage_amount * tao_price_at_transfer
            
            # Realized gain/loss
            realized_gain_loss = usd_proceeds - cost_basis_for_brokerage
            
            # Determine gain type
            holding_period_days = (transfer['timestamp'] - oldest_acquisition_ts) / 86400 if oldest_acquisition_ts else 0
            gain_type = 'Long-term' if holding_period_days >= 365 else 'Short-term'
            
            expected_transfer = {
                'transfer_id': f'XFER-{transfer_counter:04d}',
                'timestamp': transfer['timestamp'],
                'block_number': transfer['block_number'],
                'tao_amount': brokerage_amount,
                'tao_price_usd': tao_price_at_transfer,
                'usd_proceeds': usd_proceeds,
                'cost_basis': cost_basis_for_brokerage,
                'realized_gain_loss': realized_gain_loss,
                'gain_type': gain_type,
                'consumed_lots': consumed_lots,
                'transaction_hash': transfer['transaction_hash'],
                'extrinsic_id': transfer['extrinsic_id'],
                'total_outflow_tao': total_outflow,
                'fee_tao': fee_tao,
                'fee_cost_basis_usd': fee_cost_basis
            }
            
            expected_transfers.append(expected_transfer)
            transfer_counter += 1
    
        return expected_transfers

    return _compute_expected_transfers