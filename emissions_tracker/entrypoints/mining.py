#!/usr/bin/env python3
"""
Bittensor Mining Emissions Tracker

Tracks mining emissions for tax accounting:
- ALPHA mining emissions (balance increases on miner hotkey+coldkey)
- ALPHA → TAO conversions (undelegate events)
- TAO → Kraken transfers with capital gains tracking
- Monthly Wave journal entry generation

IMPORTANT: Mining emissions work differently than validator emissions:
- Mining rewards show up as balance increases in the stake_balance_history API
  when queried with the miner's own hotkey+coldkey combination
- There are NO delegation events for mining rewards - they are direct balance increases
- Undelegation events (ALPHA → TAO) work the same as validator mode
- The process_staking_emissions() method detects these balance increases by:
  1. Fetching stake balance history for the miner's hotkey+coldkey
  2. Calculating balance deltas between consecutive snapshots
  3. Accounting for manual DELEGATE/UNDELEGATE events
  4. Remaining increase = mining emissions
"""

import argparse
from datetime import datetime, timezone

from emissions_tracker.clients.taostats import TaoStatsAPIClient
from emissions_tracker.config import TrackerSettings
from emissions_tracker.trackers.mining_tracker import MiningTracker


def parse_date(date_str: str) -> int:
    """Parse a YYYY-MM-DD date string to Unix timestamp (start of day UTC)."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def run():
    parser = argparse.ArgumentParser(
        description='Bittensor Mining Emissions Tracker',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run daily check (continue from last processed timestamp)
  track-mining --mode auto

  # Process a specific date range
  track-mining --mode auto --start-date 2025-01-01 --end-date 2025-01-31

  # Process only mining income for a specific period
  track-mining --mode income --start-date 2025-01-01

  # Process only sales (ALPHA → TAO undelegations)
  track-mining --mode sales

  # Process only transfers  
  track-mining --mode transfers

  # Generate monthly journal entries
  track-mining --mode journal --month 2025-11
  
  # Generate journal entries for entire year
  track-mining --mode journal --year 2025

  # Initial seeding - process from specific start date
  track-mining --mode auto --start-date 2024-11-01
  
  # Regenerate all data (clear and reprocess from date)
  track-mining --mode auto --start-date 2024-01-01 --regenerate
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['auto', 'income', 'sales', 'transfers', 'journal'],
        default='auto',
        help='''Mode of operation:
            auto - Process all transaction types (default)
            income - Process only ALPHA mining emissions
            sales - Process only ALPHA → TAO conversions (undelegations)
            transfers - Process only TAO → Kraken transfers
            journal - Generate monthly Wave journal entries
        '''
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        default=None,
        help=('Start date in YYYY-MM-DD format. When omitted, continues from the '
              'last processed timestamp. Required for first-time runs.')
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='End date in YYYY-MM-DD format. Defaults to now if not specified.'
    )
    
    parser.add_argument(
        '--month',
        type=str,
        default=None,
        help='Month for journal entries in YYYY-MM format (default: last month)'
    )
    
    parser.add_argument(
        '--year',
        type=int,
        default=None,
        help='Year for journal entries (generates all 12 months)'
    )
    
    parser.add_argument(
        '--regenerate',
        action='store_true',
        help='Clear existing data before processing (forces full regeneration)'
    )
    
    args = parser.parse_args()
    
    # Parse date arguments
    start_time = parse_date(args.start_date) if args.start_date else None
    end_time = parse_date(args.end_date) if args.end_date else None
    
    # Load configuration
    config = TrackerSettings()
    
    # Validate mining configuration
    if not config.miner_hotkey_ss58:
        print("Error: MINER_HOTKEY_SS58 environment variable is required for mining tracker")
        print("Please set it in your .env file")
        return 1
    
    if not config.mining_tracker_sheet_id:
        print("Error: MINING_TRACKER_SHEET_ID environment variable is required for mining tracker")
        print("Please set it in your .env file")
        return 1
    
    # Use miner coldkey if specified, otherwise fall back to payout_coldkey_ss58
    miner_coldkey = config.miner_coldkey_ss58 or config.payout_coldkey_ss58
    
    if not miner_coldkey:
        print("Error: Either MINER_COLDKEY_SS58 or PAYOUT_COLDKEY_SS58 must be set")
        print("Please set one in your .env file")
        return 1
    
    # Initialize clients
    print("Initializing TaoStats API client...")
    taostats_client = TaoStatsAPIClient()
    
    # Initialize tracker for mining emissions
    print("Initializing Mining tracker...")
    tracker = MiningTracker(
        price_client=taostats_client,
        wallet_client=taostats_client
    )
    
    # Handle regeneration if requested
    if args.regenerate:
        if not start_time:
            print("Error: --start-date is required when using --regenerate to create opening lots")
            return 1
        
        tracker.clear_all_sheets()
        tracker.create_opening_lots(start_time)
    
    # Execute based on mode
    if args.mode == 'auto':
        tracker.run(start_time=start_time, end_time=end_time)
        
    elif args.mode == 'income':
        window_desc = _describe_window(args.start_date, args.end_date)
        print(f"\nProcessing mining emissions for {window_desc}...")
        tracker.process_staking_emissions(start_time=start_time, end_time=end_time)
        
    elif args.mode == 'sales':
        window_desc = _describe_window(args.start_date, args.end_date)
        print(f"\nProcessing undelegations (ALPHA → TAO) for {window_desc}...")
        tracker.process_disposals(start_time=start_time, end_time=end_time)
        
    elif args.mode == 'transfers':
        window_desc = _describe_window(args.start_date, args.end_date)
        print(f"\nProcessing transfers for {window_desc}...")
        tracker.process_disposals(start_time=start_time, end_time=end_time)
        
    elif args.mode == 'journal':
        if args.year:
            print(f"\nGenerating journal entries for all of {args.year}...")
            tracker.generate_yearly_journal_entries(args.year)
        else:
            month = args.month
            if not month:
                # Default to last month
                today = datetime.now()
                if today.month == 1:
                    month = f"{today.year - 1}-12"
                else:
                    month = f"{today.year}-{today.month - 1:02d}"
            print(f"\nGenerating journal entries for {month}...")
            tracker.generate_monthly_journal_entries(month)
    
    print("\n✓ Done!")


def _describe_window(start_date: str | None, end_date: str | None) -> str:
    """Generate a human-readable description of the time window."""
    if start_date and end_date:
        return f"{start_date} to {end_date}"
    elif start_date:
        return f"{start_date} to now"
    else:
        return "the period since your last run"


if __name__ == "__main__":
    run()
