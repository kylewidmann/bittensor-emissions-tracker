import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import time
import os
import json

# Import price client
from emissions_tracker.config import TrackerSettings
from emissions_tracker.client import (
    PriceClient
)

from emissions_tracker.exceptions import PriceNotAvailableError
from emissions_tracker.wallet import WalletClient
from substrateinterface import SubstrateInterface

import bittensor as bt

class BittensorEmissionTracker:
    def __init__(self, wallet: WalletClient, price_client: PriceClient):
        """
        Initialize the tracker with wallet info and Google Sheets credentials
        
        Args:
            wallet_name: Your bittensor wallet name
            hotkey_name: Your hotkey name
            sheet_name: Google Sheets name
            credentials_path: Path to Google API credentials JSON
            brokerage_address: Address of your brokerage wallet (for detecting liquidations)
            price_client: PriceClient instance for getting historical prices
        """
        self.config = TrackerSettings()
        self.wallet = wallet
        self.price_client = price_client
        self.sheet_name = self.config.tracker_sheet
        self.brokerage_address = self.config.brokerage_ss58
        
        print(f"Initializing tracker for wallet: {self.wallet}")
        print(f"Tracking address: {self.wallet.address}")
        
        # Connect to Bittensor (for blockchain queries only, no wallet needed)
        self.subtensor = bt.subtensor(network="finney")
        
        # Connect to Google Sheets
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.config.tracker_google_credentials, scope)
        self.sheets_client = gspread.authorize(creds)
        
        self._init_tracking_sheet()
        self._init_liquidation_queue_sheet()
        self._init_capital_gains_sheet()

    def _init_tracking_sheet(self):
        # Main tracking sheet
        try:
            self.sheet = self.sheets_client.open(self.sheet_name).sheet1
        except:
            # Create new sheet if it doesn't exist
            spreadsheet = self.sheets_client.create(self.sheet_name)
            spreadsheet.share('', perm_type='anyone', role='writer')  # Make it accessible
            self.sheet = spreadsheet.sheet1

        self._init_headers()

    def _init_headers(self):
        # Initialize sheet headers if empty
        if self.sheet.row_count == 0 or self.sheet.cell(1, 1).value != 'Date':
            self.sheet.clear()
            self.sheet.append_row([
                'Date',
                'Timestamp',
                'Block Number',
                'ALPHA Tokens Received',
                'TAO Price (USD)',
                'Total Value (USD)',
                'Payroll to Liquidate (USD)',
                'Tax to Liquidate (USD)',
                'Total to Liquidate (USD)',
                'ALPHA to Liquidate',
                'Keep Amount (USD)',
                'ALPHA to Keep',
                'Liquidation Status',
                'Long-Term Eligible Date',
                'Notes'
            ])
            # Format header row
            self.sheet.format('A1:O1', {'textFormat': {'bold': True}})

    def _init_liquidation_queue_sheet(self):
        # Create or get liquidation queue sheet
        try:
            self.liquidation_sheet = self.sheets_client.open(self.sheet_name).worksheet('Liquidation Queue')
        except:
            spreadsheet = self.sheets_client.open(self.sheet_name)
            self.liquidation_sheet = spreadsheet.add_worksheet(title='Liquidation Queue', rows=100, cols=15)
            self.liquidation_sheet.append_row([
                'Priority',
                'Date Due',
                'Amount to Liquidate (USD)',
                'ALPHA to Liquidate',
                'Cost Basis per ALPHA',
                'Purpose',
                'Status',
                'Date Completed',
                'Actual Sale Price (USD)',
                'Actual ALPHA Sold',
                'Actual Price per ALPHA',
                'Capital Gain/Loss (USD)',
                'Gain/Loss %',
                'Receipt Date',
                'Notes'
            ])
            self.liquidation_sheet.format('A1:O1', {'textFormat': {'bold': True}})
    
    def _init_capital_gains_sheet(self):
        # Create or get capital gains summary sheet
        try:
            self.gains_sheet = self.sheets_client.open(self.sheet_name).worksheet('Capital Gains Summary')
        except:
            spreadsheet = self.sheets_client.open(self.sheet_name)
            self.gains_sheet = spreadsheet.add_worksheet(title='Capital Gains Summary', rows=100, cols=10)
            self.gains_sheet.append_row([
                'Year',
                'Quarter',
                'Short-Term Gains',
                'Short-Term Losses',
                'Long-Term Gains',
                'Long-Term Losses',
                'Net Short-Term',
                'Net Long-Term',
                'Total Net Gain/Loss',
                'Notes'
            ])
            self.gains_sheet.format('A1:J1', {'textFormat': {'bold': True}})
    
    def get_tao_price(self, timestamp):
        """
        Get TAO price at specific timestamp using configured price client
        
        Args:
            timestamp: Unix timestamp
            
        Returns:
            float: TAO price in USD or None if not available
        """
        try:
            return self.price_client.get_price_at_timestamp('TAO', timestamp)
        except PriceNotAvailableError as e:
            print(f"⚠️  Could not get TAO price: {e}")
            return None
    
    def get_current_tao_price(self):
        """Get current TAO price using configured price client"""
        try:
            return self.price_client.get_current_price('TAO')
        except PriceNotAvailableError as e:
            print(f"⚠️  Could not get current TAO price: {e}")
            return None
    
    def get_alpha_price_in_tao(self, with_slippage = False):
        """
        Get current ALPHA/TAO price for a specific subnet.
        Returns the price as TAO per alpha token (e.g., 0.2564).
        """
        # Connect to the Bittensor mainnet subtensor (use network='test' for testnet if needed)
        sub = bt.subtensor()
        
        # Get the DynamicInfo for the subnet
        dynamic_info = sub.subnet(netuid=self.config.subnet_id)
        
        if dynamic_info is None:
            raise ValueError(f"Could not fetch data for netuid {self.config.subnet_id}")
        
        # Get the TAO amount for 1 alpha token
        if with_slippage:
            tao_for_one_alpha = dynamic_info.alpha_to_tao_with_slippage(1)[0].tao
        else:
            tao_for_one_alpha = dynamic_info.alpha_to_tao(1).tao
        
        # Return as float (Balance object has .tao attribute)
        return tao_for_one_alpha

    def calculate_liquidation(self, total_value_usd, alpha_tokens):
        """
        Calculate how much to liquidate for payroll and taxes
        
        Args:
            total_value_usd: Total USD value of emission
            alpha_tokens: Number of ALPHA tokens received
            
        Returns:
            dict: Breakdown of amounts
        """
        payroll_amount = self.config.daily_payroll
        
        if total_value_usd < payroll_amount:
            print(f"WARNING: Emission value (${total_value_usd:.2f}) is less than payroll requirement (${payroll_amount:.2f})")
        
        remaining = max(0, total_value_usd - payroll_amount)
        tax_amount = remaining * 0.25
        keep_amount_usd = remaining - tax_amount
        total_liquidate_usd = payroll_amount + tax_amount
        
        # Calculate how many ALPHA tokens to liquidate vs keep
        if total_value_usd > 0:
            alpha_price_usd = total_value_usd / alpha_tokens
            alpha_to_liquidate = total_liquidate_usd / alpha_price_usd
            keep_alpha = alpha_tokens - alpha_to_liquidate
        else:
            alpha_to_liquidate = 0
            keep_alpha = alpha_tokens
        
        return {
            'total_usd': total_value_usd,
            'payroll': payroll_amount,
            'tax': tax_amount,
            'total_liquidate_usd': total_liquidate_usd,
            'alpha_to_liquidate': alpha_to_liquidate,
            'keep_usd': keep_amount_usd,
            'keep_alpha': keep_alpha
        }
    
    def get_recent_emissions(self, lookback_days=1):
        """
        Query blockchain for recent transfers to your wallet from the subnet contract
        
        Args:
            lookback_days: How many days back to check
            contract_address: The subnet smart contract address (optional filter)
            
        Returns:
            list: Emission events (transfers to your wallet)
        """
        try:
            contract_address = self.config.contract_ss58

            # Get current block
            current_block = self.subtensor.get_current_block()
            
            # Bittensor produces ~1 block every 12 seconds, so ~7200 blocks/day
            blocks_per_day = 7200
            start_block = max(0, current_block - (blocks_per_day * lookback_days))
            
            print(f"Scanning blocks {start_block} to {current_block}")
            print(f"Wallet address: {self.wallet.address}")
            
            # Connect to substrate
            substrate = SubstrateInterface(
                url="wss://entrypoint-finney.opentensor.ai:443",
                ss58_format=42,
                type_registry_preset='substrate-node-template'
            )
            
            emissions = []
            
            # Query transfer events
            # We're looking for balances.Transfer events to your address
            for block_num in range(start_block, current_block + 1):
                block_hash = substrate.get_block_hash(block_num)
                events = substrate.get_events(block_hash)
                
                for event in events:
                    # Look for Transfer events to your wallet
                    if event.value['event_id'] == 'Transfer' and event.value['module_id'] == 'Balances':
                        params = event.value['attributes']
                        
                        # Check if transfer is TO your address
                        if params.get('to') == self.wallet_address:
                            # Optionally filter by sender (contract address)
                            from_address = params.get('from')
                            if contract_address and from_address != contract_address:
                                continue
                            
                            # Get block timestamp
                            block = substrate.get_block(block_hash)
                            timestamp = block['extrinsics'][0].value['call']['call_args'][0]['value']
                            
                            # Amount is in RAO (1 TAO = 10^9 RAO)
                            amount_rao = params.get('amount', 0)
                            amount_alpha = amount_rao / 1e9  # Convert to ALPHA tokens
                            
                            emissions.append({
                                'block_number': block_num,
                                'timestamp': timestamp,
                                'from': from_address,
                                'amount': amount_alpha,
                                'amount_rao': amount_rao
                            })
                            
                            print(f"Found transfer: {amount_alpha:.4f} ALPHA at block {block_num}")
            
            return emissions
            
        except Exception as e:
            print(f"Error querying blockchain: {e}")
            print("Falling back to manual entry mode...")
            return []
    
    def add_to_liquidation_queue(self, emission_data):
        """
        Add liquidation tasks to the queue sheet for manual processing
        
        Args:
            emission_data: Dictionary with emission details
        """
        alpha_price_at_receipt = emission_data['total_value_usd'] / emission_data['alpha_tokens']
        
        # Add payroll liquidation
        self.liquidation_sheet.append_row([
            'HIGH',  # Priority
            emission_data['date'].split()[0],  # Date due (today)
            f"{emission_data['payroll']:.2f}",
            f"{emission_data['payroll_alpha']:.4f}",
            f"{alpha_price_at_receipt:.4f}",  # Cost basis per ALPHA
            'Payroll',
            'PENDING',
            '',  # Date completed
            '',  # Actual sale price
            '',  # Actual ALPHA sold
            '',  # Actual price per ALPHA
            '',  # Capital gain/loss
            '',  # Gain/loss %
            emission_data['date'],  # Receipt date
            f"From emission on {emission_data['date']}"
        ])
        
        # Add tax liquidation
        self.liquidation_sheet.append_row([
            'HIGH',  # Priority
            emission_data['date'].split()[0],  # Date due (today)
            f"{emission_data['tax']:.2f}",
            f"{emission_data['tax_alpha']:.4f}",
            f"{alpha_price_at_receipt:.4f}",  # Cost basis per ALPHA
            'Taxes',
            'PENDING',
            '',  # Date completed
            '',  # Actual sale price
            '',  # Actual ALPHA sold
            '',  # Actual price per ALPHA
            '',  # Capital gain/loss
            '',  # Gain/loss %
            emission_data['date'],  # Receipt date
            f"From emission on {emission_data['date']}"
        ])
        
        print(f"Added liquidation tasks to queue: ${emission_data['total_liquidate_usd']:.2f}")
        print(f"  Cost basis: ${alpha_price_at_receipt:.4f} per ALPHA")
    
    def log_emission(self, emission_data):
        """
        Log emission to Google Sheets
        
        Args:
            emission_data: Dictionary with emission details
        """
        row = [
            emission_data['date'],
            emission_data['timestamp'],
            emission_data.get('block_number', ''),
            f"{emission_data['alpha_tokens']:.4f}",
            f"{emission_data['tao_price']:.2f}",
            f"{emission_data['total_value_usd']:.2f}",
            f"{emission_data['payroll']:.2f}",
            f"{emission_data['tax']:.2f}",
            f"{emission_data['total_liquidate_usd']:.2f}",
            f"{emission_data['alpha_to_liquidate']:.4f}",
            f"{emission_data['keep_usd']:.2f}",
            f"{emission_data['keep_alpha']:.4f}",
            emission_data.get('status', 'Pending Manual Liquidation'),
            emission_data['long_term_date'],
            emission_data.get('notes', '')
        ]
        
        self.sheet.append_row(row)
        print(f"✓ Logged emission: {emission_data['date']} - ${emission_data['total_value_usd']:.2f}")
    
    def process_emission(self, alpha_tokens, timestamp, block_number=None, manual_tao_price=None):
        """
        Process a single emission event
        
        Args:
            alpha_tokens: Amount of ALPHA tokens received
            timestamp: Unix timestamp of emission
            block_number: Optional block number
            manual_tao_price: Optional manual TAO price override for accuracy
        """
        # Get TAO price at time of emission
        if manual_tao_price:
            tao_price = manual_tao_price
            print(f"Using manual TAO price: ${tao_price:.2f}")
        else:
            tao_price = self.get_tao_price(timestamp)
        
        if tao_price is None:
            print("Could not fetch TAO price, skipping this emission")
            print("💡 Tip: Use manual_entry_mode() to enter price manually")
            return
        
        # Get ALPHA/TAO conversion rate
        alpha_to_tao_rate = self.get_alpha_price_in_tao()
        
        # Calculate USD value
        alpha_value_in_tao = alpha_tokens * alpha_to_tao_rate
        total_value_usd = alpha_value_in_tao * tao_price
        
        print(f"\nProcessing emission:")
        print(f"  ALPHA tokens: {alpha_tokens:.4f}")
        print(f"  TAO price: ${tao_price:.2f}")
        print(f"  Total value: ${total_value_usd:.2f}")
        
        # Calculate liquidation amounts
        liquidation = self.calculate_liquidation(total_value_usd, alpha_tokens)
        
        # Calculate long-term capital gains eligible date (1 year from receipt)
        long_term_date = datetime.fromtimestamp(timestamp + 365*24*60*60).strftime('%Y-%m-%d')
        
        # Split the alpha to liquidate between payroll and tax
        alpha_price_usd = total_value_usd / alpha_tokens if alpha_tokens > 0 else 0
        payroll_alpha = liquidation['payroll'] / alpha_price_usd if alpha_price_usd > 0 else 0
        tax_alpha = liquidation['tax'] / alpha_price_usd if alpha_price_usd > 0 else 0
        
        # Prepare data for logging
        emission_data = {
            'date': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'),
            'timestamp': timestamp,
            'block_number': block_number,
            'alpha_tokens': alpha_tokens,
            'tao_price': tao_price,
            'total_value_usd': liquidation['total_usd'],
            'payroll': liquidation['payroll'],
            'tax': liquidation['tax'],
            'total_liquidate_usd': liquidation['total_liquidate_usd'],
            'alpha_to_liquidate': liquidation['alpha_to_liquidate'],
            'payroll_alpha': payroll_alpha,
            'tax_alpha': tax_alpha,
            'keep_usd': liquidation['keep_usd'],
            'keep_alpha': liquidation['keep_alpha'],
            'status': 'Pending Manual Liquidation',
            'long_term_date': long_term_date,
            'notes': f'Hold {liquidation["keep_alpha"]:.4f} ALPHA until {long_term_date} for long-term capital gains'
        }
        
        # Log to main sheet
        self.log_emission(emission_data)
        
        # Add to liquidation queue
        self.add_to_liquidation_queue(emission_data)
        
        print(f"\n📊 Summary:")
        print(f"  Liquidate: {liquidation['alpha_to_liquidate']:.4f} ALPHA (${liquidation['total_liquidate_usd']:.2f})")
        print(f"    - Payroll: {payroll_alpha:.4f} ALPHA (${liquidation['payroll']:.2f})")
        print(f"    - Taxes: {tax_alpha:.4f} ALPHA (${liquidation['tax']:.2f})")
        print(f"  Keep: {liquidation['keep_alpha']:.4f} ALPHA (${liquidation['keep_usd']:.2f})")
        print(f"  Hold until: {long_term_date}")
        
        return emission_data
    
    def run_daily_check(self):
        """
        Run daily check for new emissions
        """
        print(f"\n{'='*60}")
        print(f"Checking for emissions: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        # Get emissions from last 24 hours
        emissions = self.get_recent_emissions(lookback_days=1)
        
        if not emissions:
            print("ℹ️  No new emissions found")
            return
        
        for emission in emissions:
            self.process_emission(
                alpha_tokens=emission['amount'],
                timestamp=emission['timestamp'],
                block_number=emission.get('block_number')
            )
        
        print(f"\n✓ Processed {len(emissions)} emission(s)")
        print(f"📋 Check 'Liquidation Queue' sheet for manual tasks")
    
    def get_outgoing_transfers(self, lookback_days=7):
        """
        Query blockchain for outgoing transfers from your wallet to brokerage
        
        Args:
            lookback_days: How many days back to check
            
        Returns:
            list: Outgoing transfer events
        """
        try:
            from substrateinterface import SubstrateInterface
            
            # Get current block
            current_block = self.subtensor.get_current_block()
            blocks_per_day = 7200
            start_block = max(0, current_block - (blocks_per_day * lookback_days))
            
            print(f"Scanning blocks {start_block} to {current_block} for outgoing transfers")
            print(f"From: {self.wallet_address}")
            if self.brokerage_address:
                print(f"To: {self.brokerage_address}")
            
            # Connect to substrate
            substrate = SubstrateInterface(
                url="wss://entrypoint-finney.opentensor.ai:443",
                ss58_format=42,
                type_registry_preset='substrate-node-template'
            )
            
            transfers = []
            
            # Query transfer events
            for block_num in range(start_block, current_block + 1):
                block_hash = substrate.get_block_hash(block_num)
                events = substrate.get_events(block_hash)
                
                for event in events:
                    # Look for Transfer events FROM your wallet
                    if event.value['event_id'] == 'Transfer' and event.value['module_id'] == 'Balances':
                        params = event.value['attributes']
                        
                        from_address = params.get('from')
                        to_address = params.get('to')
                        
                        # Check if transfer is FROM your address
                        if from_address == self.wallet.address:
                            # Optionally filter by destination (brokerage address)
                            if self.brokerage_address and to_address != self.brokerage_address:
                                continue
                            
                            # Get block timestamp
                            block = substrate.get_block(block_hash)
                            timestamp = block['extrinsics'][0].value['call']['call_args'][0]['value']
                            
                            # Amount is in RAO (1 TAO = 10^9 RAO)
                            amount_rao = params.get('amount', 0)
                            amount_alpha = amount_rao / 1e9
                            
                            transfers.append({
                                'block_number': block_num,
                                'timestamp': timestamp,
                                'to': to_address,
                                'amount': amount_alpha,
                                'amount_rao': amount_rao
                            })
                            
                            print(f"Found outgoing transfer: {amount_alpha:.4f} ALPHA at block {block_num}")
            
            return transfers
            
        except Exception as e:
            print(f"Error querying blockchain for outgoing transfers: {e}")
            return []
    
    def manual_entry_mode(self):
        """
        Interactive mode to manually enter emission data
        Useful when automated detection isn't working or for manual price entry
        """
        print("\n" + "="*60)
        print("MANUAL EMISSION ENTRY MODE")
        print("="*60 + "\n")
        
        while True:
            try:
                print("Enter emission details (or 'q' to quit):")
                
                # Get ALPHA amount
                alpha_input = input("ALPHA tokens received: ").strip()
                if alpha_input.lower() == 'q':
                    break
                alpha_tokens = float(alpha_input)
                
                # Get timestamp (default to now)
                time_input = input("Date/time received (YYYY-MM-DD HH:MM:SS, or press Enter for now): ").strip()
                if time_input:
                    timestamp = int(datetime.strptime(time_input, "%Y-%m-%d %H:%M:%S").timestamp())
                else:
                    timestamp = int(time.time())
                
                receipt_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S UTC')
                print(f"Receipt time: {receipt_time}")
                
                # Get manual TAO price (optional but recommended)
                price_input = input("TAO price at receipt (USD, or press Enter to auto-fetch): ").strip()
                manual_tao_price = float(price_input) if price_input else None
                
                if not manual_tao_price:
                    print("\n⚠️  WARNING: Auto-fetching price may be inaccurate for tax purposes")
                    print("   CryptoCompare provides hourly prices (better than daily)")
                    print("   For maximum accuracy, check exchange price at exact time\n")
                
                # Get block number (optional)
                block_input = input("Block number (optional, press Enter to skip): ").strip()
                block_number = int(block_input) if block_input else None
                
                # Process the emission
                self.process_emission(alpha_tokens, timestamp, block_number, manual_tao_price)
                
                print("\n✓ Emission logged successfully!\n")
                
                continue_input = input("Add another emission? (y/n): ").strip().lower()
                if continue_input != 'y':
                    break
                    
            except ValueError as e:
                print(f"Invalid input: {e}. Please try again.\n")
            except KeyboardInterrupt:
                print("\nExiting manual entry mode...")
                break
        """
        Get a summary of pending liquidations
        """
        all_values = self.liquidation_sheet.get_all_values()
        
        if len(all_values) <= 1:
            print("No pending liquidations")
            return
        
        print(f"\n{'='*60}")
        print("PENDING LIQUIDATIONS (Manual Action Required)")
        print(f"{'='*60}\n")
        
        total_usd = 0
        total_alpha = 0
        
        for i, row in enumerate(all_values[1:], start=2):  # Skip header
            if len(row) >= 6 and row[5] == 'PENDING':
                priority = row[0]
                date_due = row[1]
                amount_usd = float(row[2]) if row[2] else 0
                alpha_amount = float(row[3]) if row[3] else 0
                purpose = row[4]
                
                total_usd += amount_usd
                total_alpha += alpha_amount
                
                print(f"[{priority}] {date_due} - {purpose}")
                print(f"    Amount: {alpha_amount:.4f} ALPHA (${amount_usd:.2f})")
                print(f"    Row: {i}")
                print()
        
        print(f"{'='*60}")
        print(f"TOTAL PENDING: {total_alpha:.4f} ALPHA (${total_usd:.2f})")
        print(f"{'='*60}\n")
        print("💡 After liquidating via Ledger, update the 'Status' column to 'COMPLETED'")
    
    def mark_liquidation_complete(self, row_number, actual_amount=None, notes=""):
        """
        Mark a liquidation as complete
        
        Args:
            row_number: Row number in liquidation sheet
            actual_amount: Actual amount liquidated (if different from planned)
            notes: Any notes about the liquidation
        """
        self.liquidation_sheet.update_cell(row_number, 6, 'COMPLETED')
        self.liquidation_sheet.update_cell(row_number, 7, datetime.now().strftime('%Y-%m-%d'))
        
        if actual_amount:
            self.liquidation_sheet.update_cell(row_number, 8, str(actual_amount))
        
        if notes:
            existing_notes = self.liquidation_sheet.cell(row_number, 9).value
            combined_notes = f"{existing_notes}; {notes}" if existing_notes else notes
            self.liquidation_sheet.update_cell(row_number, 9, combined_notes)
        
        print(f"✓ Marked row {row_number} as completed")