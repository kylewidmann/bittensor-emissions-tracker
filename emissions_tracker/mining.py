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
from datetime import datetime

from emissions_tracker.clients.taostats import TaoStatsAPIClient
from emissions_tracker.tracker import BittensorEmissionTracker
from emissions_tracker.config import TrackerSettings
from emissions_tracker.models import SourceType


def run():
    parser = argparse.ArgumentParser(
        description='Bittensor Mining Emissions Tracker',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run daily check (process all recent transactions)
  python -m emissions_tracker.mining --mode auto

  # Process only income
  python -m emissions_tracker.mining --mode income --lookback 30

  # Process only sales (ALPHA → TAO undelegations)
  python -m emissions_tracker.mining --mode sales --lookback 14

  # Process only transfers  
  python -m emissions_tracker.mining --mode transfers --lookback 7

  # Generate monthly journal entries
  python -m emissions_tracker.mining --mode journal --month 2025-11

  # Run with custom lookback period
  python -m emissions_tracker.mining --mode auto --lookback 30
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
    
    args = parser.parse_args()
    
    # Load configuration
    config = TrackerSettings()
    
    # Validate mining configuration
    if not config.miner_hotkey_ss58:
        print("Error: MINER_HOTKEY_SS58 environment variable is required for mining tracker")
        print("Please set it in your .env file")
        return
    
    if not config.mining_tracker_sheet_id:
        print("Error: MINING_TRACKER_SHEET_ID environment variable is required for mining tracker")
        print("Please set it in your .env file")
        return
    
    # Use miner coldkey if specified, otherwise fall back to payout_coldkey_ss58
    miner_coldkey = config.miner_coldkey_ss58 or config.payout_coldkey_ss58
    
    if not miner_coldkey:
        print("Error: Either MINER_COLDKEY_SS58 or PAYOUT_COLDKEY_SS58 must be set")
        print("Please set one in your .env file")
        return
    
    # Initialize clients
    print("Initializing TaoStats API client...")
    taostats_client = TaoStatsAPIClient()
    
    # Initialize tracker for mining emissions
    print("Initializing Mining tracker...")
    tracker = BittensorEmissionTracker(
        price_client=taostats_client,
        wallet_client=taostats_client,
        tracking_hotkey=config.miner_hotkey_ss58,
        coldkey=miner_coldkey,
        sheet_id=config.mining_tracker_sheet_id,
        label="Mining",
        smart_contract_address=None,
        income_source=SourceType.MINING
    )
    
    # Execute based on mode
    if args.mode == 'auto':
        tracker.run_daily_check(lookback_days=args.lookback)
        
    elif args.mode == 'income':
        income_window = (
            f"last {args.lookback} days" if args.lookback is not None else "the period since your last run"
        )
        print(f"\nProcessing mining emissions for {income_window}...")
        # Mining uses staking emissions method (same API, different address)
        tracker.process_staking_emissions(lookback_days=args.lookback)
        
    elif args.mode == 'sales':
        sales_window = (
            f"last {args.lookback} days" if args.lookback is not None else "the period since your last run"
        )
        print(f"\nProcessing undelegations (ALPHA → TAO) for {sales_window}...")
        tracker.process_sales(lookback_days=args.lookback)
        
    elif args.mode == 'transfers':
        transfer_window = (
            f"last {args.lookback} days" if args.lookback is not None else "the period since your last run"
        )
        print(f"\nProcessing transfers for {transfer_window}...")
        tracker.process_transfers(lookback_days=args.lookback)
        
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
