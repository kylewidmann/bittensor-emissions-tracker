import os
from emissions_tracker.client import CoinMarketCapClient
from emissions_tracker.config import TrackerSettings
from emissions_tracker.tracker import BittensorEmissionTracker


if __name__ == "__main__":
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Bittensor Emission Tracker')
    parser.add_argument('--mode', choices=['auto', 'manual', 'pending', 'liquidations', 'verify-prices'], default='auto',
                       help='Mode: auto (check emissions), manual (enter manually), pending (show pending), liquidations (process liquidations), verify-prices (review and update prices)')
    parser.add_argument('--lookback', type=int, default=1, help='Days to look back for emissions/liquidations')
    args = parser.parse_args()
    
    # Create price client based on configuration
    settings = TrackerSettings()
    price_client = CoinMarketCapClient(settings.cmc_api_key)
    print("Using CoinMarketCap API (5-minute intervals)")
    
    # Initialize tracker
    tracker = BittensorEmissionTracker(
        price_client=price_client
    )
    
    if args.mode == 'auto':
        # Run daily check (automated detection)
        tracker.run_daily_check()
        
        # Also check for liquidations
        print("\n" + "="*60)
        print("Checking for liquidations...")
        print("="*60)
        tracker.check_and_process_liquidations(lookback_days=args.lookback)
        
    elif args.mode == 'manual':
        # Manual entry mode
        tracker.manual_entry_mode()
        
    elif args.mode == 'pending':
        # Check pending liquidations
        tracker.get_pending_liquidations()
        
    elif args.mode == 'liquidations':
        # Check and process liquidations only
        tracker.check_and_process_liquidations(lookback_days=args.lookback)
    
    elif args.mode == 'verify-prices':
        # Verify and update prices for existing emissions
        tracker.verify_and_update_prices(start_date=args.start_date, end_date=args.end_date)
    