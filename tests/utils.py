"""
Shared utility functions for unit tests.

Contains common logic for processing balance history and delegation events
that is used across multiple test modules.
"""
import json
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Dict, Any, Tuple


def load_json_data(json_path: str) -> List[dict]:
    """Load data array from JSON file.
    
    Args:
        json_path: Path to JSON file with 'data' array
        
    Returns:
        List of data records from the JSON file
    """
    with open(json_path) as f:
        return json.load(f)['data']


def filter_balances_by_date_range(
    balance_data: List[dict],
    start_ts: int,
    end_ts: int
) -> List[dict]:
    """Filter balance records to a date range.
    
    Args:
        balance_data: List of balance records with 'timestamp' field
        start_ts: Start timestamp (unix seconds)
        end_ts: End timestamp (unix seconds)
        
    Returns:
        Filtered list of balance records
    """
    return [
        b for b in balance_data
        if start_ts <= int(datetime.fromisoformat(b['timestamp'].replace('Z', '+00:00')).timestamp()) <= end_ts
    ]


def group_balances_by_day(balances: List[dict]) -> List[Dict[str, Any]]:
    """Group balance records by day, keeping last balance of each day.
    
    Args:
        balances: List of balance records with 'timestamp' and 'balance' fields
        
    Returns:
        List of daily balance records with keys: day, timestamp, alpha, block
    """
    balances_by_day = defaultdict(list)
    for b in balances:
        ts = int(datetime.fromisoformat(b['timestamp'].replace('Z', '+00:00')).timestamp())
        dt = datetime.fromtimestamp(ts)
        day_key = dt.strftime('%Y-%m-%d')
        balances_by_day[day_key].append({
            'timestamp': ts,
            'alpha': int(b['balance']) / 1e9,
            'block': b['block_number']
        })
    
    daily_balances = []
    for day_key in sorted(balances_by_day.keys()):
        day_balances = sorted(balances_by_day[day_key], key=lambda x: x['timestamp'])
        daily_balances.append({
            'day': day_key,
            'timestamp': day_balances[-1]['timestamp'],
            'alpha': day_balances[-1]['alpha'],
            'block': day_balances[-1]['block']
        })
    
    return daily_balances


def filter_delegation_events(
    event_data: List[dict],
    start_ts: int,
    end_ts: int
) -> List[Dict[str, Any]]:
    """Filter and convert delegation events to simplified format.
    
    Args:
        event_data: List of raw event records
        start_ts: Start timestamp (unix seconds)
        end_ts: End timestamp (unix seconds)
        
    Returns:
        List of events with keys: timestamp, alpha, action
    """
    events = []
    for e in event_data:
        event_ts = int(datetime.fromisoformat(e['timestamp'].replace('Z', '+00:00')).timestamp())
        if start_ts <= event_ts <= end_ts:
            if e['action'] in ('DELEGATE', 'UNDELEGATE'):
                events.append({
                    'timestamp': event_ts,
                    'alpha': int(e['alpha']) / 1e9,
                    'action': e['action']
                })
    return sorted(events, key=lambda x: x['timestamp'])


def group_events_by_day(events: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group events by day.
    
    Args:
        events: List of events with 'timestamp' field
        
    Returns:
        Dictionary mapping day string (YYYY-MM-DD) to list of events
    """
    events_by_day = defaultdict(list)
    for event in events:
        dt = datetime.fromtimestamp(event['timestamp'])
        day_key = dt.strftime('%Y-%m-%d')
        events_by_day[day_key].append(event)
    return events_by_day


def calculate_daily_emissions(
    daily_balances: List[Dict[str, Any]],
    events_by_day: Dict[str, List[Dict[str, Any]]],
    price_per_tao: float = None,
    price_lookup: callable = None,
    emission_threshold: float = 0.0001
) -> Tuple[List[Dict[str, Any]], int, float]:
    """Calculate daily emissions from balance changes and events.
    
    For each day (comparing to previous day):
    - Calculate balance change
    - Subtract DELEGATE inflows
    - Add back UNDELEGATE outflows
    - Result is emissions for that day
    
    Args:
        daily_balances: List of daily balance records
        events_by_day: Dictionary of events grouped by day
        price_per_tao: Fixed price for calculating USD FMV (or None to use price_lookup)
        price_lookup: Callable that takes day string and returns price (or None to use price_per_tao)
        emission_threshold: Minimum emission amount to count
        
    Returns:
        Tuple of (emission_lots, count, total_alpha)
        - emission_lots: List of emission lot dicts
        - count: Number of emission lots created
        - total_alpha: Total ALPHA emitted
    """
    if price_per_tao is None and price_lookup is None:
        raise ValueError("Must provide either price_per_tao or price_lookup")
    
    emission_lots = []
    emission_count = 0
    total_alpha_emitted = 0.0
    
    for i in range(1, len(daily_balances)):
        prev_day = daily_balances[i - 1]
        curr_day = daily_balances[i]
        
        # Balance change from end of previous day to end of current day
        balance_change = curr_day['alpha'] - prev_day['alpha']
        
        # Get all events for current day
        day_events = events_by_day.get(curr_day['day'], [])
        
        delegate_inflow = sum(e['alpha'] for e in day_events if e['action'] == 'DELEGATE')
        undelegate_outflow = sum(e['alpha'] for e in day_events if e['action'] == 'UNDELEGATE')
        
        # Calculate emissions
        emissions = balance_change - delegate_inflow + undelegate_outflow
        
        if emissions > emission_threshold:
            # Get price for this day
            if price_lookup:
                tao_price = price_lookup(curr_day['day'])
            else:
                tao_price = price_per_tao
            
            usd_fmv = emissions * tao_price
            emission_lots.append({
                'timestamp': curr_day['timestamp'],
                'alpha_quantity': emissions,
                'alpha_remaining': emissions,
                'usd_fmv': usd_fmv,
                'status': 'Open'
            })
            emission_count += 1
            total_alpha_emitted += emissions
    
    return emission_lots, emission_count, total_alpha_emitted


def filter_contract_income_events(
    stake_events: List[dict],
    start_ts: int,
    end_ts: int,
    contract_address: str,
    netuid: int,
    delegate: str,
    nominator: str
) -> List[dict]:
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
    filtered_events = []
    for event in stake_events:
        # Convert ISO timestamp to Unix timestamp for comparison
        event_timestamp = int(datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00')).timestamp())
        
        # Only include events that match ALL required filters:
        # - Correct netuid, delegate, and nominator (API required params)
        # - Transfer events to the contract address
        # - Within the date range
        if (event['netuid'] == netuid
            and event['delegate']['ss58'] == delegate
            and event['nominator']['ss58'] == nominator
            and event.get('is_transfer') == True
            and event.get('transfer_address', {}).get('ss58') == contract_address
            and start_ts <= event_timestamp <= end_ts):
            filtered_events.append(event)
    
    return filtered_events


def create_emission_lots(
    balance_json_path: str,
    events_json_path: str,
    start_date: datetime,
    end_date: datetime,
    price_per_tao: float = None,
    price_lookup: callable = None,
    opening_lot_alpha: float = 0.0,
    opening_lot_usd_per_alpha: float = 20.0,
    emission_threshold: float = 1e-6
) -> List[Dict[str, Any]]:
    """Create ALPHA emission lots from raw balance and event data.
    
    This is a shared utility that creates ALPHA lots in the same way across
    all test modules. It:
    1. Loads balance history and delegation events
    2. Optionally creates an opening lot (1 day before start)
    3. Calculates daily emissions from balance changes
    4. Returns a list of lot dictionaries with lot_id, timestamp, quantities, etc.
    
    Args:
        balance_json_path: Path to stake balance JSON file
        events_json_path: Path to stake events JSON file (for DELEGATE/UNDELEGATE)
        start_date: Start date for emission processing
        end_date: End date for emission processing
        price_per_tao: Fixed TAO price for USD FMV calculations (or None to use price_lookup)
        price_lookup: Callable that takes day string (YYYY-MM-DD) and returns price
        opening_lot_alpha: If > 0, create an opening lot with this amount
        opening_lot_usd_per_alpha: USD cost basis per ALPHA for opening lot
        emission_threshold: Minimum emission amount to count
        
    Returns:
        List of ALPHA lot dictionaries with keys:
        - lot_id: Formatted as 'ALPHA-0001', 'ALPHA-0002', etc.
        - timestamp: Unix timestamp
        - alpha_quantity: Original ALPHA amount
        - alpha_remaining: Remaining ALPHA (starts same as quantity)
        - usd_per_alpha: USD cost basis per ALPHA
        - usd_fmv: Total USD FMV for the lot
    """
    start_ts = int(start_date.timestamp())
    end_ts = int(end_date.timestamp())
    
    # Load and process balance/event data
    balance_data = load_json_data(balance_json_path)
    event_data = load_json_data(events_json_path)
    
    balances = filter_balances_by_date_range(balance_data, start_ts, end_ts)
    daily_balances = group_balances_by_day(balances)
    
    delegations = filter_delegation_events(event_data, start_ts, end_ts)
    events_by_day = group_events_by_day(delegations)
    
    # Calculate daily emissions
    emission_lots, emission_count, total_alpha_emitted = calculate_daily_emissions(
        daily_balances,
        events_by_day,
        price_per_tao=price_per_tao,
        price_lookup=price_lookup,
        emission_threshold=emission_threshold
    )
    
    # Create lot list with IDs
    alpha_lots = []
    lot_counter = 1
    
    # Add opening lot if requested
    if opening_lot_alpha > 0:
        opening_lot_date = start_date - timedelta(days=1)
        alpha_lots.append({
            'lot_id': 'ALPHA-OPENING',  # Use same ID as tracker
            'timestamp': int(opening_lot_date.timestamp()),
            'alpha_quantity': opening_lot_alpha,
            'alpha_remaining': opening_lot_alpha,
            'usd_per_alpha': opening_lot_usd_per_alpha,
            'usd_fmv': opening_lot_alpha * opening_lot_usd_per_alpha,
        })
        # Don't increment counter for opening lot
    
    # Add emission lots with IDs
    for emission in emission_lots:
        usd_per_alpha = emission['usd_fmv'] / emission['alpha_quantity'] if emission['alpha_quantity'] > 0 else 0
        alpha_lots.append({
            'lot_id': f'ALPHA-{lot_counter:04d}',
            'timestamp': emission['timestamp'],
            'alpha_quantity': emission['alpha_quantity'],
            'alpha_remaining': emission['alpha_remaining'],
            'usd_per_alpha': usd_per_alpha,
            'usd_fmv': emission['usd_fmv'],
        })
        lot_counter += 1
    
    return alpha_lots
