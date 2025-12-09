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
from datetime import datetime

from emissions_tracker.clients.taostats import TaoStatsAPIClient
from emissions_tracker.tracker import BittensorEmissionTracker


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

  # Run with custom lookback period
  python -m emissions_tracker.main --mode auto --lookback 30
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
        default=7,
        help='Days to look back for transactions (default: 7)'
    )
    
    parser.add_argument(
        '--month',
        type=str,
        default=None,
        help='Month for journal entries in YYYY-MM format (default: last month)'
    )
    
    args = parser.parse_args()
    
    # Initialize clients
    print("Initializing TaoStats API client...")
    taostats_client = TaoStatsAPIClient()
    
    # Initialize tracker
    print("Initializing tracker...")
    tracker = BittensorEmissionTracker(
        price_client=taostats_client,
        wallet_client=taostats_client
    )
    
    # Execute based on mode
    if args.mode == 'auto':
        tracker.run_daily_check(days_back=args.lookback)
        
    elif args.mode == 'income':
        print(f"\nProcessing income for last {args.lookback} days...")
        tracker.process_contract_income(days_back=args.lookback)
        tracker.process_staking_emissions(days_back=args.lookback)
        
    elif args.mode == 'sales':
        print(f"\nProcessing sales for last {args.lookback} days...")
        tracker.process_sales(days_back=args.lookback)
        
    elif args.mode == 'transfers':
        print(f"\nProcessing transfers for last {args.lookback} days...")
        tracker.process_transfers(days_back=args.lookback)
        
    elif args.mode == 'journal':
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