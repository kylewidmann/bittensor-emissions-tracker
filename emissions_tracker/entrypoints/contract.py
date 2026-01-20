#!/usr/bin/env python3
"""
Bittensor Smart Contract Emissions Tracker

Tracks smart contract emissions for tax accounting:
- ALPHA income (Contract + Staking emissions)
- ALPHA → TAO sales with FIFO/HIFO lot consumption
- TAO deposits from external sources
- Expenses (ALPHA transfers to third parties)
- TAO → Kraken transfers with capital gains tracking
- Monthly Wave journal entry generation
"""

import argparse
from datetime import datetime, timezone

from emissions_tracker.clients.taostats import TaoStatsAPIClient
from emissions_tracker.config import TrackerSettings
from emissions_tracker.trackers.contract_tracker import ContractTracker


def parse_date(date_str: str) -> int:
    """Parse a YYYY-MM-DD date string to Unix timestamp (start of day UTC)."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def run():
    parser = argparse.ArgumentParser(
        description='Bittensor Smart Contract Emissions Tracker',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run daily check (continue from last processed timestamp)
  track-contract --mode auto

  # Process a specific date range
  track-contract --mode auto --start-date 2025-01-01 --end-date 2025-01-31

  # Process only income for a specific period
  track-contract --mode income --start-date 2025-01-01

  # Process only sales (continues from last processed)
  track-contract --mode sales

  # Process only transfers  
  track-contract --mode transfers

  # Generate monthly journal entries
  track-contract --mode journal --month 2025-11
  
  # Generate journal entries for entire year
  track-contract --mode journal --year 2025
  
  # Regenerate journal entries for entire year
  track-contract --mode journal --year 2025 --regenerate

  # Initial seeding - process from specific start date
  track-contract --mode auto --start-date 2024-11-01
  
  # Regenerate all data (clear and reprocess from date)
  track-contract --mode auto --start-date 2024-01-01 --regenerate
  
  # Regenerate only transfers from date
  track-contract --mode transfers --start-date 2024-01-01 --regenerate
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['auto', 'income', 'sales', 'transfers', 'journal'],
        default='auto',
        help='''Mode of operation:
            auto - Process all transaction types (default)
            income - Process only ALPHA income (Contract + Staking)
            sales - Process only ALPHA → TAO sales
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
    
    # Initialize clients
    print("Initializing TaoStats API client...")
    taostats_client = TaoStatsAPIClient()
    
    # Initialize tracker for smart contract emissions
    print("Initializing Smart Contract tracker...")
    tracker = ContractTracker(
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
        print(f"\nProcessing income for {window_desc}...")
        tracker.process_contract_income(start_time=start_time, end_time=end_time)
        tracker.process_staking_emissions(start_time=start_time, end_time=end_time)
        
    elif args.mode == 'sales':
        window_desc = _describe_window(args.start_date, args.end_date)
        print(f"\nProcessing sales for {window_desc}...")
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
