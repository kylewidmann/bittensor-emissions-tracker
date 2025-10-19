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
    def __init__(self, price_client: PriceClient):
        """
        Initialize the tracker with wallet info and Google Sheets credentials
        
        Args:
            price_client: PriceClient instance for getting historical prices
        """
        self.config = TrackerSettings()
        self.price_client = price_client
        self.sheet_id = self.config.tracker_sheet_id
        self.brokerage_address = self.config.brokerage_ss58
        self.wallet_address = self.config.wallet_ss58
        self.contract_address = self.config.contract_ss58
        
        print(f"Initializing tracker for wallet: {self.wallet_address}")
        
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
            self.sheet = self.sheets_client.open_by_key(self.sheet_id).sheet1
        except:
            raise RuntimeError("Sheet not found.  Create the sheet and set the ID for the `TRACKER_SHEET_ID` env variable.")

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
            self.liquidation_sheet = self.sheets_client.open_by_key(self.sheet_id).worksheet('Liquidation Queue')
        except:
            spreadsheet = self.sheets_client.open_by_key(self.sheet_id)
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
            self.gains_sheet = self.sheets_client.open_by_key(self.sheet_id).worksheet('Capital Gains Summary')
        except:
            spreadsheet = self.sheets_client.open_by_key(self.sheet_id)
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
            
        Returns:
            list: Emission events (transfers to your wallet)
        """
        try:
            # Get current block
            current_block = self.subtensor.get_current_block()
            
            # Bittensor produces ~1 block every 12 seconds, so ~7200 blocks/day
            blocks_per_day = 7200
            start_block = max(0, current_block - (blocks_per_day * lookback_days))
            
            print(f"Scanning blocks {start_block} to {current_block}")
            print(f"Wallet address: {self.wallet_address}")
            print(f"Contract address: {self.contract_address}")
            
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
                            if self.contract_address and from_address != self.contract_address:
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
    
    def get_tao_transfers_to_brokerage(self, lookback_days=7):
        """
        Query blockchain for TAO transfers from your wallet to brokerage.
        These would be from unstaking ALPHA tokens and converting to TAO.
        
        Args:
            lookback_days: How many days back to check
            
        Returns:
            list: TAO transfer events to brokerage
        """
        try:
            from substrateinterface import SubstrateInterface
            
            # Get current block
            current_block = self.subtensor.get_current_block()
            blocks_per_day = 7200
            start_block = max(0, current_block - (blocks_per_day * lookback_days))
            
            print(f"Scanning blocks {start_block} to {current_block} for TAO transfers to brokerage")
            print(f"From: {self.wallet_address}")
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
                    # Look for Transfer events FROM your wallet TO brokerage
                    if event.value['event_id'] == 'Transfer' and event.value['module_id'] == 'Balances':
                        params = event.value['attributes']
                        
                        from_address = params.get('from')
                        to_address = params.get('to')
                        
                        # Check if transfer is FROM your address TO brokerage
                        if (from_address == self.wallet_address and 
                            to_address == self.brokerage_address):
                            
                            # Get block timestamp
                            block = substrate.get_block(block_hash)
                            timestamp = block['extrinsics'][0].value['call']['call_args'][0]['value']
                            
                            # Amount is in RAO (1 TAO = 10^9 RAO)
                            amount_rao = params.get('amount', 0)
                            amount_tao = amount_rao / 1e9
                            
                            transfers.append({
                                'block_number': block_num,
                                'timestamp': timestamp,
                                'to': to_address,
                                'amount_tao': amount_tao,
                                'amount_rao': amount_rao
                            })
                            
                            print(f"Found TAO transfer to brokerage: {amount_tao:.4f} TAO at block {block_num}")
            
            return transfers
            
        except Exception as e:
            print(f"Error querying blockchain for TAO transfers: {e}")
            return []
    
    def check_and_process_liquidations(self, lookback_days=7):
        """
        Check for completed liquidations by looking for TAO transfers to brokerage.
        Match these with pending liquidation queue items.
        
        Args:
            lookback_days: How many days back to check for transfers
        """
        print(f"\n{'='*60}")
        print("CHECKING FOR COMPLETED LIQUIDATIONS")
        print(f"{'='*60}")
        
        # Get TAO transfers to brokerage
        tao_transfers = self.get_tao_transfers_to_brokerage(lookback_days)
        
        if not tao_transfers:
            print("No TAO transfers to brokerage found")
            return
        
        # Get pending liquidations
        pending_liquidations = self.get_pending_liquidations_data()
        
        if not pending_liquidations:
            print("No pending liquidations to match")
            return
        
        print(f"\nFound {len(tao_transfers)} TAO transfers and {len(pending_liquidations)} pending liquidations")
        
        # Try to match transfers with pending liquidations
        for transfer in tao_transfers:
            transfer_date = datetime.fromtimestamp(transfer['timestamp']).strftime('%Y-%m-%d')
            transfer_amount_tao = transfer['amount_tao']
            
            # Get TAO price at time of transfer for USD calculation
            tao_price = self.get_tao_price(transfer['timestamp'])
            if not tao_price:
                print(f"⚠️  Could not get TAO price for transfer on {transfer_date}")
                continue
                
            transfer_amount_usd = transfer_amount_tao * tao_price
            
            print(f"\nProcessing transfer: {transfer_amount_tao:.4f} TAO (${transfer_amount_usd:.2f}) on {transfer_date}")
            
            # Find matching pending liquidations
            # Look for liquidations due around the same date and similar USD amount
            matches = []
            for row_num, liquidation in pending_liquidations.items():
                expected_usd = float(liquidation['amount_usd'])
                date_due = liquidation['date_due']
                
                # Check if amounts are reasonably close (within 5% tolerance for price fluctuations)
                amount_diff_pct = abs(transfer_amount_usd - expected_usd) / expected_usd * 100
                
                if amount_diff_pct <= 5.0:  # 5% tolerance
                    matches.append((row_num, liquidation, amount_diff_pct))
            
            if matches:
                # Sort by closest amount match
                matches.sort(key=lambda x: x[2])
                best_match = matches[0]
                row_num, liquidation, diff_pct = best_match
                
                print(f"  ✓ Matched with liquidation row {row_num} ({liquidation['purpose']})")
                print(f"    Expected: ${liquidation['amount_usd']} vs Actual: ${transfer_amount_usd:.2f} ({diff_pct:.1f}% diff)")
                
                # Calculate gains/losses
                cost_basis = float(liquidation['cost_basis'])
                alpha_liquidated = float(liquidation['alpha_amount'])
                actual_price_per_alpha = transfer_amount_usd / alpha_liquidated
                
                gain_loss_usd = (actual_price_per_alpha - cost_basis) * alpha_liquidated
                gain_loss_pct = (actual_price_per_alpha - cost_basis) / cost_basis * 100
                
                # Update the liquidation sheet
                self.mark_liquidation_complete(
                    row_num, 
                    transfer_amount_usd, 
                    transfer_amount_tao, 
                    actual_price_per_alpha,
                    gain_loss_usd,
                    gain_loss_pct,
                    f"Auto-matched TAO transfer from block {transfer['block_number']}"
                )
                
                print(f"    Gain/Loss: ${gain_loss_usd:.2f} ({gain_loss_pct:.1f}%)")
                
                # Remove from pending list to avoid double-matching
                del pending_liquidations[row_num]
            else:
                print(f"  ⚠️  No matching pending liquidation found for this transfer")
    
    def get_pending_liquidations(self):
        """
        Get a summary of pending liquidations
        """
        pending_data = self.get_pending_liquidations_data()
        
        if not pending_data:
            print("No pending liquidations")
            return
        
        print(f"\n{'='*60}")
        print("PENDING LIQUIDATIONS (Manual Action Required)")
        print(f"{'='*60}\n")
        
        total_usd = 0
        total_alpha = 0
        
        for row_num, liquidation in pending_data.items():
            priority = liquidation['priority']
            date_due = liquidation['date_due']
            amount_usd = float(liquidation['amount_usd'])
            alpha_amount = float(liquidation['alpha_amount'])
            purpose = liquidation['purpose']
            
            total_usd += amount_usd
            total_alpha += alpha_amount
            
            print(f"[{priority}] {date_due} - {purpose}")
            print(f"    Amount: {alpha_amount:.4f} ALPHA (${amount_usd:.2f})")
            print(f"    Row: {row_num}")
            print()
        
        print(f"{'='*60}")
        print(f"TOTAL PENDING: {total_alpha:.4f} ALPHA (${total_usd:.2f})")
        print(f"{'='*60}\n")
        print("💡 Process by unstaking ALPHA, converting to TAO, and sending to brokerage")
        print("💡 After completed, run with --mode liquidations to auto-match transfers")
    
    def get_pending_liquidations_data(self):
        """
        Get pending liquidations as a dictionary for processing
        
        Returns:
            dict: {row_number: liquidation_data}
        """
        all_values = self.liquidation_sheet.get_all_values()
        
        if len(all_values) <= 1:
            return {}
        
        pending_liquidations = {}
        
        for i, row in enumerate(all_values[1:], start=2):  # Skip header
            if len(row) >= 7 and row[6] == 'PENDING':
                pending_liquidations[i] = {
                    'priority': row[0],
                    'date_due': row[1],
                    'amount_usd': row[2],
                    'alpha_amount': row[3],
                    'cost_basis': row[4],
                    'purpose': row[5],
                    'status': row[6],
                    'receipt_date': row[13] if len(row) > 13 else '',
                    'notes': row[14] if len(row) > 14 else ''
                }
        
        return pending_liquidations
    
    def verify_and_update_prices(self, start_date=None, end_date=None):
        """
        Review and update TAO prices for existing emissions.
        Useful for improving accuracy of historical records.
        
        Args:
            start_date: Start date for review (YYYY-MM-DD), defaults to 30 days ago
            end_date: End date for review (YYYY-MM-DD), defaults to today
        """
        print(f"\n{'='*60}")
        print("VERIFYING AND UPDATING TAO PRICES")
        print(f"{'='*60}")
        
        # Set default date range if not provided
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        print(f"Reviewing emissions from {start_date} to {end_date}")
        
        # Get all emissions from the main sheet
        all_values = self.sheet.get_all_values()
        
        if len(all_values) <= 1:
            print("No emissions found in tracking sheet")
            return
        
        headers = all_values[0]
        emissions_updated = 0
        
        for i, row in enumerate(all_values[1:], start=2):  # Skip header, start from row 2
            if len(row) < 6:  # Need at least date, timestamp, block, alpha, tao_price, total_value
                continue
            
            emission_date = row[0]  # Date column
            timestamp_str = row[1]  # Timestamp column
            alpha_tokens_str = row[3]  # ALPHA tokens column
            current_tao_price_str = row[4]  # Current TAO price column
            
            # Parse date to check if it's in our range
            try:
                emission_date_obj = datetime.strptime(emission_date.split()[0], '%Y-%m-%d')
                start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
                
                if not (start_date_obj <= emission_date_obj <= end_date_obj):
                    continue
                    
            except ValueError:
                print(f"⚠️  Skipping row {i}: Invalid date format '{emission_date}'")
                continue
            
            # Parse values
            try:
                timestamp = int(float(timestamp_str))
                alpha_tokens = float(alpha_tokens_str)
                current_tao_price = float(current_tao_price_str)
            except ValueError:
                print(f"⚠️  Skipping row {i}: Invalid numeric values")
                continue
            
            print(f"\nReviewing emission from {emission_date}:")
            print(f"  Current TAO price: ${current_tao_price:.2f}")
            
            # Get fresh TAO price
            fresh_tao_price = self.get_tao_price(timestamp)
            
            if fresh_tao_price is None:
                print(f"  ⚠️  Could not fetch fresh price, keeping current")
                continue
            
            print(f"  Fresh TAO price: ${fresh_tao_price:.2f}")
            
            # Check if price needs updating (more than 1% difference)
            price_diff_pct = abs(fresh_tao_price - current_tao_price) / current_tao_price * 100
            
            if price_diff_pct > 1.0:  # Update if more than 1% difference
                print(f"  📝 Updating price (difference: {price_diff_pct:.1f}%)")
                
                # Recalculate all USD values
                alpha_to_tao_rate = self.get_alpha_price_in_tao()
                alpha_value_in_tao = alpha_tokens * alpha_to_tao_rate
                new_total_value_usd = alpha_value_in_tao * fresh_tao_price
                
                # Recalculate liquidation amounts
                liquidation = self.calculate_liquidation(new_total_value_usd, alpha_tokens)
                
                # Update the row
                self.sheet.update_cell(i, 5, f"{fresh_tao_price:.2f}")  # TAO Price column
                self.sheet.update_cell(i, 6, f"{new_total_value_usd:.2f}")  # Total Value column
                self.sheet.update_cell(i, 7, f"{liquidation['payroll']:.2f}")  # Payroll column
                self.sheet.update_cell(i, 8, f"{liquidation['tax']:.2f}")  # Tax column
                self.sheet.update_cell(i, 9, f"{liquidation['total_liquidate_usd']:.2f}")  # Total liquidate column
                self.sheet.update_cell(i, 10, f"{liquidation['alpha_to_liquidate']:.4f}")  # ALPHA to liquidate
                self.sheet.update_cell(i, 11, f"{liquidation['keep_usd']:.2f}")  # Keep USD
                self.sheet.update_cell(i, 12, f"{liquidation['keep_alpha']:.4f}")  # Keep ALPHA
                
                print(f"    Old total value: ${current_tao_price * alpha_value_in_tao:.2f}")
                print(f"    New total value: ${new_total_value_usd:.2f}")
                
                emissions_updated += 1
                
                # Add a note about the update
                current_notes = self.sheet.cell(i, 15).value or ""  # Notes column
                update_note = f"Price updated on {datetime.now().strftime('%Y-%m-%d')}: ${current_tao_price:.2f} -> ${fresh_tao_price:.2f}"
                new_notes = f"{current_notes}; {update_note}" if current_notes else update_note
                self.sheet.update_cell(i, 15, new_notes)
                
                # Small delay to avoid hitting API rate limits
                time.sleep(0.5)
                
            else:
                print(f"  ✓ Price is accurate (difference: {price_diff_pct:.1f}%)")
        
        print(f"\n{'='*60}")
        print(f"PRICE VERIFICATION COMPLETE")
        print(f"Updated {emissions_updated} emission(s)")
        print(f"{'='*60}")
    
    def mark_liquidation_complete(self, row_number, actual_sale_price_usd=None, actual_tao_sold=None, 
                                 actual_price_per_alpha=None, gain_loss_usd=None, gain_loss_pct=None, notes=""):
        """
        Mark a liquidation as complete with detailed transaction info
        
        Args:
            row_number: Row number in liquidation sheet
            actual_sale_price_usd: Actual USD amount received from sale
            actual_tao_sold: Actual TAO amount sold
            actual_price_per_alpha: Actual USD price per ALPHA achieved
            gain_loss_usd: Capital gain/loss in USD
            gain_loss_pct: Capital gain/loss percentage
            notes: Any notes about the liquidation
        """
        # Update completion status and date
        self.liquidation_sheet.update_cell(row_number, 7, 'COMPLETED')  # Status
        self.liquidation_sheet.update_cell(row_number, 8, datetime.now().strftime('%Y-%m-%d'))  # Date completed
        
        # Update actual transaction details
        if actual_sale_price_usd:
            self.liquidation_sheet.update_cell(row_number, 9, f"{actual_sale_price_usd:.2f}")  # Actual sale price
        
        if actual_tao_sold:
            self.liquidation_sheet.update_cell(row_number, 10, f"{actual_tao_sold:.4f}")  # TAO amount (for reference)
        
        if actual_price_per_alpha:
            self.liquidation_sheet.update_cell(row_number, 11, f"{actual_price_per_alpha:.4f}")  # Actual price per ALPHA
        
        if gain_loss_usd:
            self.liquidation_sheet.update_cell(row_number, 12, f"{gain_loss_usd:.2f}")  # Capital gain/loss
        
        if gain_loss_pct:
            self.liquidation_sheet.update_cell(row_number, 13, f"{gain_loss_pct:.1f}%")  # Gain/loss percentage
        
        # Update notes
        if notes:
            existing_notes = self.liquidation_sheet.cell(row_number, 15).value or ""  # Notes column
            combined_notes = f"{existing_notes}; {notes}" if existing_notes else notes
            self.liquidation_sheet.update_cell(row_number, 15, combined_notes)
        
        print(f"✓ Marked liquidation row {row_number} as completed")
    
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