#!/usr/bin/env python3
"""
Bittensor ALPHA/TAO Subledger Tracker

Tracks cryptocurrency income and disposals for tax accounting:
- ALPHA income (Contract + Staking emissions)
- ALPHA → TAO sales with FIFO lot consumption
- TAO → Kraken transfers with capital gains tracking
- Monthly Wave journal entry generation
"""

import argparse
from datetime import datetime, timezone

from emissions_tracker.clients.taostats import TaoStatsAPIClient
from emissions_tracker.tracker import BittensorEmissionTracker
from emissions_tracker.config import TrackerSettings
from emissions_tracker.models import SourceType


def run():
    parser = argparse.ArgumentParser(
        description='Bittensor ALPHA/TAO Subledger Tracker',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run daily check (process all recent transactions)
  python -m emissions_tracker.main --mode auto

  # Process only income
  python -m emissions_tracker.main --mode income --lookback 30

  # Process only sales
  python -m emissions_tracker.main --mode sales --lookback 14

  # Process only transfers  
  python -m emissions_tracker.main --mode transfers --lookback 7

  # Generate monthly journal entries
  python -m emissions_tracker.main --mode journal --month 2025-11
  
  # Generate journal entries for entire year
  python -m emissions_tracker.main --mode journal --year 2025
  
  # Regenerate journal entries for entire year
  python -m emissions_tracker.main --mode journal --year 2025 --regenerate

  # Run with custom lookback period
  python -m emissions_tracker.main --mode auto --lookback 30
  
  # Regenerate all data (clear and reprocess)
  python -m emissions_tracker.main --mode auto --lookback 365 --regenerate
  
  # Regenerate only transfers
  python -m emissions_tracker.main --mode transfers --lookback 365 --regenerate
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
        '--lookback',
        type=int,
        default=None,
        help=('Days to look back for transactions. When omitted, the tracker '
              'continues from the last processed timestamp; required for first-time runs.')
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
    
    # Load configuration
    config = TrackerSettings()
    
    # Initialize clients
    print("Initializing TaoStats API client...")
    taostats_client = TaoStatsAPIClient()
    
    # Initialize tracker for smart contract emissions
    print("Initializing Smart Contract tracker...")
    tracker = BittensorEmissionTracker(
        price_client=taostats_client,
        wallet_client=taostats_client,
        tracking_hotkey=config.validator_ss58,
        coldkey=config.payout_coldkey_ss58,
        sheet_id=config.tracker_sheet_id,
        label="Smart Contract",
        smart_contract_address=config.smart_contract_ss58,
        income_source=SourceType.STAKING
    )
    
    # Handle regeneration if requested
    if args.regenerate:
        print("\n⚠️  REGENERATION MODE: Clearing existing data...")
        if args.mode in ['auto', 'income']:
            tracker.clear_income_sheets()
        if args.mode in ['auto', 'sales']:
            tracker.clear_sales_sheet()
            tracker.clear_expenses_sheet()
        if args.mode in ['auto', 'transfers']:
            tracker.clear_transfers_sheet()
        if args.mode == 'journal':
            tracker.clear_journal_sheet()
        print("✓ Sheets cleared\n")
    
    # Execute based on mode
    if args.mode == 'auto':
        tracker.run_daily_check(lookback_days=args.lookback)
        
    elif args.mode == 'income':
        income_window = (
            f"last {args.lookback} days" if args.lookback is not None else "the period since your last run"
        )
        print(f"\nProcessing income for {income_window}...")
        tracker.process_contract_income(lookback_days=args.lookback)
        tracker.process_staking_emissions(lookback_days=args.lookback)
        
    elif args.mode == 'sales':
        sales_window = (
            f"last {args.lookback} days" if args.lookback is not None else "the period since your last run"
        )
        print(f"\nProcessing sales for {sales_window}...")
        tracker.process_sales(lookback_days=args.lookback)
        
    elif args.mode == 'transfers':
        transfer_window = (
            f"last {args.lookback} days" if args.lookback is not None else "the period since your last run"
        )
        print(f"\nProcessing transfers for {transfer_window}...")
        tracker.process_transfers(lookback_days=args.lookback)
        
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

if __name__ == "__main__":
    run()