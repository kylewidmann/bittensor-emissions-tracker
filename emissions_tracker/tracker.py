import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, timezone
import time
import os
import json
import bittensor as bt
from abc import ABC, abstractmethod

# Import price client
from emissions_tracker.clients.price import PriceClient
from emissions_tracker.clients.wallet import WalletClientInterface
from emissions_tracker.config import TrackerSettings
from emissions_tracker.exceptions import PriceNotAvailableError

# ... (WalletClientInterface and TaostatsAPIClient unchanged from version 7d14030a-99cd-4b2b-9c4f-e32ad9840be9)

class BittensorEmissionTracker:
    def __init__(self, price_client: PriceClient, wallet_client: WalletClientInterface):
        """
        Initialize the tracker with wallet info and Google Sheets credentials
        
        Args:
            price_client: PriceClient instance for getting historical prices
            wallet_client: WalletClientInterface for fetching transfers/emissions
        """
        self.config = TrackerSettings()
        self.price_client = price_client
        self.wallet_client = wallet_client
        self.sheet_id = self.config.tracker_sheet_id
        self.brokerage_address = self.config.brokerage_ss58
        self.wallet_address = self.config.wallet_ss58
        self.validator_address = self.config.validator_ss58
        self.smart_contract_address = self.config.smart_contract_ss58  # Optional
        self.subnet_id = self.config.subnet_id  # e.g., 64 for Chutes
        
        print(f"Initializing tracker for wallet: {self.wallet_address}, validator: {self.validator_address}")
        
        # Connect to Bittensor (for price queries)
        self.subtensor = bt.subtensor(network="finney")
        
        # Connect to Google Sheets
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.config.tracker_google_credentials, scope)
        self.sheets_client = gspread.authorize(creds)
        
        self._init_tracking_sheet()
        self._init_liquidation_queue_sheet()
        self._init_capital_gains_sheet()
        
        self._load_last_timestamp()

    def _load_last_timestamp(self):
        try:
            # Emissions (Tracking sheet)
            records = self.tracking_worksheet.get_all_records()
            self.last_emission_timestamp = max(record['Timestamp'] for record in records) if records else 0
            print(f"Loaded last emission timestamp: {self.last_emission_timestamp}")
        except Exception as e:
            print(f"Error loading Tracking sheet timestamp: {e}")
            self.last_emission_timestamp = 0
        
        try:
            # Transfers (Liquidation Queue sheet)
            records = self.liquidation_worksheet.get_all_records()
            self.last_transfer_timestamp = max(record['Timestamp'] for record in records) if records else 0
            print(f"Loaded last transfer timestamp: {self.last_transfer_timestamp}")
        except Exception as e:
            print(f"Error loading Liquidation Queue timestamp: {e}")
            self.last_transfer_timestamp = 0
        
        try:
            # Balances (Capital Gains sheet)
            records = self.capital_gains_worksheet.get_all_records()
            self.last_balance_timestamp = max(record['Timestamp'] for record in records) if records else 0
            print(f"Loaded last balance timestamp: {self.last_balance_timestamp}")
        except Exception as e:
            print(f"Error loading Capital Gains sheet timestamp: {e}")
            self.last_balance_timestamp = 0

    def _load_last_alpha_balance(self):
        last_balance = 0
        try:
            # Balances (Capital Gains sheet)
            records = self.capital_gains_worksheet.get_all_records()
            if records:
                records = sorted(records, key=lambda r: r['Timestamp'], reverse=True)
                last_balance = records[0]['Alpha Tokens'] * 1e9
            print(f"Loaded last balance timestamp: {self.last_balance_timestamp}")
        except Exception as e:
            print(f"Error loading Capital Gains sheet timestamp: {e}")
        
        return last_balance

    def _save_last_timestamp(self, emission_timestamp: int = None, transfer_timestamp: int = None, balance_timestamp: int = None):
        try:
            if emission_timestamp:
                self.last_emission_timestamp = emission_timestamp
                print(f"Updated last emission timestamp: {emission_timestamp}")
            if transfer_timestamp:
                self.last_transfer_timestamp = transfer_timestamp
                print(f"Updated last transfer timestamp: {transfer_timestamp}")
            if balance_timestamp:
                self.last_balance_timestamp = balance_timestamp
                print(f"Updated last balance timestamp: {balance_timestamp}")
        except Exception as e:
            print(f"Error updating timestamps: {e}")

    def _init_tracking_sheet(self):
        """Initialize main tracking sheet."""
        try:
            self.sheet = self.sheets_client.open_by_key(self.sheet_id)
            try:
                self.tracking_worksheet = self.sheet.worksheet("Tracking")
            except gspread.exceptions.WorksheetNotFound:
                self.tracking_worksheet = self.sheet.add_worksheet(title="Tracking", rows=1000, cols=20)
                headers = ['Date', 'Timestamp', 'Block Number', 'Alpha Tokens', 'TAO Price (USD)', 
                           'Total Value (USD)', 'Payroll (USD)', 'Tax (USD)', 'Total Liquidate (USD)', 
                           'Alpha to Liquidate', 'Payroll Alpha', 'Tax Alpha', 'Keep (USD)', 
                           'Keep Alpha', 'Status', 'Long Term Date', 'Notes']
                self.tracking_worksheet.append_row(headers)
        except Exception as e:
            print(f"Error initializing tracking sheet: {e}")
            raise

    def _init_liquidation_queue_sheet(self):
        """Initialize liquidation queue sheet."""
        try:
            self.sheet = self.sheets_client.open_by_key(self.sheet_id)
            try:
                self.liquidation_worksheet = self.sheet.worksheet("Liquidation Queue")
            except gspread.exceptions.WorksheetNotFound:
                self.liquidation_worksheet = self.sheet.add_worksheet(title="Liquidation Queue", rows=1000, cols=20)
                headers = ['Date', 'Timestamp', 'Block Number', 'Alpha to Liquidate', 
                           'Payroll Alpha', 'Tax Alpha', 'Total Liquidate (USD)', 
                           'Payroll (USD)', 'Tax (USD)', 'Status']
                self.liquidation_worksheet.append_row(headers)
        except Exception as e:
            print(f"Error initializing liquidation queue sheet: {e}")
            raise

    def _init_capital_gains_sheet(self):
        """Initialize capital gains sheet for alpha hold periods."""
        try:
            self.sheet = self.sheets_client.open_by_key(self.sheet_id)
            try:
                self.capital_gains_worksheet = self.sheet.worksheet("Capital Gains")
            except gspread.exceptions.WorksheetNotFound:
                self.capital_gains_worksheet = self.sheet.add_worksheet(title="Capital Gains", rows=1000, cols=20)
                headers = ['Date', 'Timestamp', 'Block Number', 'Alpha Tokens', 'Alpha Emissions', 'TAO Equivalent', 'USD Value', 
                           'Long Term Date', 'Status', 'Acquisition Price (USD)', 'Notes']
                self.capital_gains_worksheet.append_row(headers)
        except Exception as e:
            print(f"Error initializing capital gains sheet: {e}")
            raise

    def _sort_sheets(self):

        try:
            self.tracking_worksheet.sort((2, 'des'))
            print("Sorted Tracking sheet by Timestamp descending")
        except Exception as e:
            print(f"Error sorting Tracking sheet: {e}")

        try:
            self.liquidation_worksheet.sort((2, 'des'))
            print("Sorted Liquidation sheet by Timestamp descending")
        except Exception as e:
            print(f"Error sorting Liquidation sheet: {e}")

        try:
            self.capital_gains_worksheet.sort((2, 'des'))
            print("Sorted Capital Gains sheet by Timestamp descending")
        except Exception as e:
            print(f"Error sorting Capital Gains sheet: {e}")

    def get_tao_price(self, timestamp: int):
        """Fetch TAO price at a given timestamp using PriceClient."""
        try:
            price = self.price_client.get_price_at_timestamp('TAO', timestamp)
            print(f"PriceClient: TAO price at {datetime.fromtimestamp(timestamp)}: ${price:.2f}")
            return price
        except PriceNotAvailableError as e:
            print(f"PriceClient error: {e}")
            return None

    def get_current_tao_price(self):
        """Fetch current TAO price using PriceClient."""
        try:
            price = self.price_client.get_current_price('TAO')
            print(f"PriceClient: Current TAO price: ${price:.2f}")
            return price
        except PriceNotAvailableError as e:
            print(f"PriceClient error: {e}")
            return None

    def get_alpha_price_in_tao(self, with_slippage: bool = False):
        """Fetch current alpha/TAO price for subnet."""
        try:
            dynamic_info = self.subtensor.subtensor(netuid=self.subnet_id)
            if with_slippage:
                return dynamic_info.alpha_to_tao(1, slippage=0.05).tao
            return dynamic_info.alpha_to_tao(1).tao
        except Exception as e:
            print(f"Error fetching alpha/TAO price: {e}")
            return 0.08  # Fallback default

    def calculate_liquidation(self, total_value_usd: float, alpha_tokens: float):
        """Calculate liquidation amounts for payroll and tax, using a fixed daily payroll amount.
        
        If total_value_usd is less than the fixed payroll, liquidate all alpha tokens for payroll.
        Otherwise, cover payroll and apply ~25% tax on emissions if funds remain.
        
        Args:
            total_value_usd: Total USD value of the emission
            alpha_tokens: Amount of ALPHA tokens received
            
        Returns:
            dict: Liquidation details with total_usd, payroll, tax, total_liquidate_usd,
                alpha_to_liquidate, keep_usd, keep_alpha
        """
        fixed_payroll_usd = self.config.fixed_payroll_usd  # e.g., 100.0 USD/day
        
        # Calculate price per alpha
        alpha_price_usd = total_value_usd / alpha_tokens if alpha_tokens > 0 else 0
        
        if total_value_usd < fixed_payroll_usd:
            # Insufficient funds: liquidate all alpha for payroll
            payroll = total_value_usd
            tax = 0
            total_liquidate_usd = payroll
            alpha_to_liquidate = alpha_tokens
            keep_usd = 0
            keep_alpha = 0
        else:
            # Sufficient funds: cover payroll, then tax
            payroll = fixed_payroll_usd
            # Calculate ~25% tax for new emissions
            tax = self.calculate_tax_liquidation(total_value_usd, alpha_tokens)
            total_liquidate_usd = payroll + tax['tax_usd']
            alpha_to_liquidate = total_liquidate_usd / alpha_price_usd if alpha_price_usd > 0 else 0
            keep_usd = total_value_usd - total_liquidate_usd
            keep_alpha = alpha_tokens - alpha_to_liquidate
        
        return {
            'total_usd': total_value_usd,
            'payroll': payroll,
            'tax_usd': tax['tax_usd'] if tax else 0,
            'tax_alpha': tax['tax_alpha'] if tax else 0,
            'total_liquidate_usd': total_liquidate_usd,
            'alpha_to_liquidate': alpha_to_liquidate,
            'keep_usd': keep_usd,
            'keep_alpha': keep_alpha
        }

    def calculate_tax_liquidation(self, total_value_usd: float, alpha_tokens: float):
        """Calculate ~25% tax liquidation for new alpha emissions."""
        tax_percentage = self.config.tax_percentage  # ~25% for immediate tax liability
        tax = total_value_usd * tax_percentage
        alpha_price_usd = total_value_usd / alpha_tokens if alpha_tokens > 0 else 0
        tax_alpha = tax / alpha_price_usd if alpha_price_usd > 0 else 0
        
        return {
            'tax_usd': tax,
            'tax_alpha': tax_alpha
        }

    def add_to_liquidation_queue(self, emission_data: dict):
        """Add emission to liquidation queue sheet."""
        try:
            row = [
                emission_data['date'],
                emission_data['timestamp'],
                emission_data.get('block_number', ''),
                emission_data['alpha_to_liquidate'],
                emission_data['payroll_alpha'],
                emission_data['tax_alpha'],
                emission_data['total_liquidate_usd'],
                emission_data['payroll'],
                emission_data['tax'],
                emission_data['status']
            ]
            self.liquidation_worksheet.append_row(row)
            print(f"Added to liquidation queue: {emission_data['date']}")
        except Exception as e:
            print(f"Error adding to liquidation queue: {e}")

    def log_emission(self, emission_data: dict):
        """Log emission to tracking sheet."""
        try:
            row = [
                emission_data['date'],
                emission_data['timestamp'],
                emission_data.get('block_number', ''),
                emission_data['alpha_tokens'],
                emission_data['tao_price'],
                emission_data['total_value_usd'],
                emission_data['payroll'],
                emission_data['tax'],
                emission_data['total_liquidate_usd'],
                emission_data['alpha_to_liquidate'],
                emission_data['payroll_alpha'],
                emission_data['tax_alpha'],
                emission_data['keep_usd'],
                emission_data['keep_alpha'],
                emission_data['status'],
                emission_data['long_term_date'],
                emission_data['notes']
            ]
            self.tracking_worksheet.append_row(row)
            print(f"Logged emission: {emission_data['date']}")
        except Exception as e:
            print(f"Error logging emission: {e}")

    def log_capital_gains(self, balance_data: dict):
        """Log alpha balance for capital gains tracking."""
        try:
            row = [
                datetime.fromtimestamp(balance_data['timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
                balance_data['timestamp'],
                balance_data['block_number'],
                balance_data['alpha_balance'],
                balance_data['incremental_alpha'],
                balance_data['tao_equivalent'],
                balance_data['usd_value'],
                datetime.fromtimestamp(balance_data['timestamp'] + 365*24*60*60).strftime('%Y-%m-%d'),
                'Held',
                balance_data['acquisition_price_usd'],
                balance_data.get('notes', '')
            ]
            self.capital_gains_worksheet.append_row(row)
            print(f"Logged capital gains balance: {datetime.fromtimestamp(balance_data['timestamp'])}")
        except Exception as e:
            print(f"Error logging capital gains: {e}")

    def get_recent_emissions(self, days_back=1):
        """
        Fetch recent incoming emissions from validator to wallet.
        
        Args:
            days_back: How many days back to check
            
        Returns:
            list: Emission events (delegations to your wallet)
        """
        try:
            end_time = int(time.time())
            start_time = self.last_emission_timestamp if self.last_emission_timestamp else  end_time - (days_back * 86400)
            
            print(f"Fetching emissions to {self.wallet_address} from {datetime.fromtimestamp(start_time)}")
            if self.smart_contract_address:
                print(f"Filtering by contract: {self.smart_contract_address}")
            
            emissions = []
            delegations = self.wallet_client.get_delegations(
                netuid=self.subnet_id,
                delegate=self.validator_address,
                nominator=self.wallet_address,
                start_time=start_time,
                end_time=end_time
            )

            for d in delegations:
                if (d['timestamp'] > self.last_emission_timestamp and 
                    d['unit'] == 'alpha' and 
                    d['action'] == 'DELEGATE' and 
                    (not self.smart_contract_address or d.get('transfer_address', None) == self.smart_contract_address)):
                    emissions.append({
                        'block_number': d['block_number'],
                        'timestamp': d['timestamp'],
                        'from': d['from'],
                        'amount': d['amount'],  # Alpha
                        'amount_rao': d['amount'] * 1e9,
                        'tao_price_usd': d['tao_price_usd'],
                        'tao_equivalent': d['tao_equivalent'],
                        'usd_value': d['usd']
                    })
                    print(f"Found emission: {d['amount']:.4f} ALPHA ({d['tao_equivalent']:.4f} TAO) at {datetime.fromtimestamp(d['timestamp'])}")
            
            if emissions:
                last_ts = max(em['timestamp'] for em in emissions)
                self._save_last_timestamp(emission_timestamp=last_ts)
            else:
                print(f"ℹ️  No new emissions found. Check Taostats: taostats.io/account/{self.wallet_address}/transactions")
            
            return emissions
        except Exception as e:
            print(f"Error fetching emissions: {e}")
            print("Falling back to manual entry mode...")
            return []

    def get_tao_transfers_to_brokerage(self, days_back=7):
        """
        Fetch outgoing TAO transfers from wallet to brokerage.
        
        Args:
            days_back: How many days back to check
            
        Returns:
            list: TAO transfer events to brokerage
        """
        try:
            end_time = int(time.time())
            start_time = self.last_transfer_timestamp if self.last_transfer_timestamp else end_time - (days_back * 86400)
            
            print(f"Fetching TAO transfers from {self.wallet_address} to {self.brokerage_address}")
            
            transfers = []
            api_transfers = self.wallet_client.get_transfers(
                account_address=self.wallet_address,
                start_time=start_time,
                end_time=end_time,
                receiver=self.brokerage_address
            )
            
            for t in api_transfers:
                if t['timestamp'] > self.last_transfer_timestamp and t['unit'] == 'tao':
                    tao_price_usd = t['tao_price_usd'] or self.get_tao_price(t['timestamp'])
                    transfers.append({
                        'block_number': t['block_number'],
                        'timestamp': t['timestamp'],
                        'to': t['to'],
                        'amount_tao': t['amount'],
                        'amount_rao': t['amount'] * 1e9,
                        'tao_price_usd': tao_price_usd
                    })
                    print(f"Found TAO transfer: {t['amount']:.4f} TAO at ${tao_price_usd:.2f}/TAO on {datetime.fromtimestamp(t['timestamp'])}")

            if transfers:
                last_ts = max(tr['timestamp'] for tr in transfers)
                self._save_last_timestamp(transfer_timestamp=last_ts)
            else:
                print(f"ℹ️  No TAO transfers found. Check Taostats: taostats.io/account/{self.brokerage_address}/transactions")
            
            return transfers
        except Exception as e:
            print(f"Error fetching TAO transfers: {e}")
            return []

    def get_remaining_alpha(self, days_back=365):
        """
        Fetch historical stake balances for capital gains tracking, logging incremental alpha.
        
        Args:
            days_back: How many days back to check (default: 365 for 1 year)
            
        Returns:
            list: Balance snapshots with timestamp, incremental_alpha, tao_equivalent, usd_value
        """
        try:
            end_time = int(time.time())
            start_time = self.last_balance_timestamp if self.last_balance_timestamp > 0 else end_time - (days_back * 86400)
            
            print(f"Fetching stake balance history for {self.wallet_address} from {datetime.fromtimestamp(start_time, tz=timezone.utc)}")
            
            balances = self.wallet_client.get_stake_balance_history(
                netuid=self.subnet_id,
                hotkey=self.validator_address,
                coldkey=self.wallet_address,
                start_time=start_time,
                end_time=end_time
            )
            

            balances = sorted(balances, key=lambda b: b['timestamp'])
            enriched_balances = []
            previous_balance = self._load_last_alpha_balance()
            for b in balances:
                if b['timestamp'] > self.last_balance_timestamp:
                    alpha_balance = b['alpha_balance'] / 1e9
                    new_alpha = alpha_balance - previous_balance if previous_balance > 0 else alpha_balance
                    if new_alpha > 0:  # Only log positive increments (emissions/rewards)
                        tao_price = self.get_tao_price(b['timestamp'])
                        usd_value = new_alpha * tao_price * (b['tao_equivalent'] / b['alpha_balance']) if tao_price and b['alpha_balance'] > 0 else None
                        # This is really just an approximation, but hard to get unless running against substrate every 30 minutes
                        acquisition_price_usd = usd_value / new_alpha if new_alpha > 0 and usd_value else None
                        enriched_balances.append({
                            'timestamp': b['timestamp'],
                            'block_number': b['block_number'],
                            'alpha_balance': alpha_balance,
                            'incremental_alpha': new_alpha,
                            'tao_equivalent': new_alpha * (b['tao_equivalent'] / b['alpha_balance']) if b['alpha_balance'] > 0 else 0,
                            'usd_value': usd_value,
                            'acquisition_price_usd': acquisition_price_usd,
                            'notes': f'Price source: {self.price_client.name}' if usd_value else 'Price source: Missing'
                        })
                        self.log_capital_gains(enriched_balances[-1])
                    previous_balance = alpha_balance
            
            if enriched_balances:
                last_ts = max(b['timestamp'] for b in enriched_balances)
                self._save_last_timestamp(balance_timestamp=last_ts)
                print(f"Found {len(enriched_balances)} new balance snapshots.")
            else:
                print("ℹ️  No new balance snapshots found.")
            
            return enriched_balances
        except Exception as e:
            print(f"Error fetching stake balance history: {e}")
            return []

    def process_emission(self, alpha_tokens, timestamp, block_number=None, tao_price=None, tao_equivalent=None, usd_value=None):
        """
        Process a single emission event, including ~25% tax liquidation.
        
        Args:
            alpha_tokens: Amount of ALPHA tokens received
            timestamp: Unix timestamp of emission
            block_number: Optional block number
            tao_price: Optional TAO price in USD (from API)
            tao_equivalent: Optional TAO equivalent of alpha
            usd_value: Optional USD value from API
        """
        # Prioritize Taostats API usd_value
        if usd_value and tao_equivalent:
            total_value_usd = usd_value
            tao_price = usd_value / tao_equivalent if tao_equivalent > 0 else self.get_tao_price(timestamp)
            print(f"Using Taostats API USD value: ${usd_value:.2f}, TAO equivalent: {tao_equivalent:.4f}")
        else:
            tao_price = tao_price or self.get_tao_price(timestamp)
            if tao_price is None:
                print("Could not fetch TAO price, skipping this emission")
                print("💡 Tip: Use manual_entry_mode() to enter price manually")
                return
            tao_equivalent = alpha_tokens * self.get_alpha_price_in_tao()
            total_value_usd = tao_equivalent * tao_price
            print(f"Using PriceClient for USD value: ${total_value_usd:.2f}")
        
        print(f"\nProcessing emission:")
        print(f"  ALPHA tokens: {alpha_tokens:.4f}")
        print(f"  TAO price: ${tao_price:.2f}")
        print(f"  Total value: ${total_value_usd:.2f}")
        
        # Calculate payroll/tax liquidation (existing logic)
        liquidation = self.calculate_liquidation(total_value_usd, alpha_tokens)
        
        # Calculate ~25% tax liquidation for new alpha emissions
        # tax_liquidation = self.calculate_tax_liquidation(total_value_usd, alpha_tokens)
        
        # Calculate long-term capital gains eligible date (1 year from receipt)
        long_term_date = datetime.fromtimestamp(timestamp + 365*24*60*60).strftime('%Y-%m-%d')
        
        # Combine liquidation amounts
        alpha_price_usd = total_value_usd / alpha_tokens if alpha_tokens > 0 else 0
        payroll_alpha = liquidation['payroll'] / alpha_price_usd if alpha_price_usd > 0 else 0
        tax_alpha = liquidation['tax_alpha']
        total_liquidate_alpha = payroll_alpha + tax_alpha
        keep_alpha = alpha_tokens - total_liquidate_alpha
        keep_usd = total_value_usd - (liquidation['total_liquidate_usd'] + liquidation['tax_usd'])
        
        # Prepare data for logging
        emission_data = {
            'date': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'),
            'timestamp': timestamp,
            'block_number': block_number,
            'alpha_tokens': alpha_tokens,
            'tao_price': tao_price,
            'total_value_usd': total_value_usd,
            'payroll': liquidation['payroll'],
            'tax': liquidation['tax_usd'],
            'total_liquidate_usd': liquidation['total_liquidate_usd'] + liquidation['tax_usd'],
            'alpha_to_liquidate': total_liquidate_alpha,
            'payroll_alpha': payroll_alpha,
            'tax_alpha': tax_alpha,
            'keep_usd': keep_usd,
            'keep_alpha': keep_alpha,
            'status': 'Pending Manual Liquidation',
            'long_term_date': long_term_date,
            'notes': f'Hold {keep_alpha:.4f} ALPHA until {long_term_date} for long-term capital gains; includes 25% tax liquidation'
        }
        
        # Log to main sheet
        self.log_emission(emission_data)
        
        # Add to liquidation queue
        self.add_to_liquidation_queue(emission_data)
        
        print(f"\n📊 Summary:")
        print(f"  Liquidate: {total_liquidate_alpha:.4f} ALPHA (${emission_data['total_liquidate_usd']:.2f})")
        print(f"    - Payroll: {payroll_alpha:.4f} ALPHA (${liquidation['payroll']:.2f})")
        print(f"    - Taxes: {tax_alpha:.4f} ALPHA (${emission_data['tax']:.2f})")
        print(f"  Keep: {keep_alpha:.4f} ALPHA (${keep_usd:.2f})")
        print(f"  Hold until: {long_term_date}")
        
        return emission_data

    def run_daily_check(self):
        """
        Run daily check for new emissions and capital gains tracking.
        """
        print(f"\n{'='*60}")
        print(f"Checking for emissions and balances: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        # Get emissions from last 24 hours
        emissions = self.get_recent_emissions(days_back=1)
        
        if not emissions:
            print("ℹ️  No new emissions found")
        else:
            for emission in emissions:
                self.process_emission(
                    alpha_tokens=emission['amount'],
                    timestamp=emission['timestamp'],
                    block_number=emission.get('block_number'),
                    tao_price=emission.get('tao_price_usd'),
                    tao_equivalent=emission.get('tao_equivalent'),
                    usd_value=emission.get('usd_value')
                )
        
        # Update capital gains tracking
        self.get_remaining_alpha(days_back=365)

        self._sort_sheets()

        print(f"\n✓ Processed {len(emissions)} emission(s)")
        print(f"📋 Check 'Liquidation Queue' and 'Capital Gains' sheets for manual tasks")

    def check_and_process_liquidations(self, lookback_days=7):
        """Check and process pending liquidations against brokerage transfers."""
        try:
            transfers = self.get_tao_transfers_to_brokerage(days_back=lookback_days)
            if transfers:
                pending_liquidations = self.get_pending_liquidations()
                
                for liquidation in pending_liquidations:
                    liquidation_timestamp = liquidation['timestamp']
                    
                    for transfer in transfers:
                        transfer_timestamp = transfer['timestamp']
                        transfer_amount = transfer['amount_tao']
                        tao_price_usd = transfer['tao_price_usd']
                        transfer_usd = transfer_amount * tao_price_usd if tao_price_usd else 0
                        
                        if abs(transfer_timestamp - liquidation_timestamp) < 86400:  # Within 24 hours
                            if abs(transfer_usd - liquidation['total_liquidate_usd']) < 0.1 * liquidation['total_liquidate_usd']:
                                self.mark_liquidation_complete(liquidation)
                                print(f"Matched liquidation: {liquidation['date']} with transfer: {transfer_amount:.4f} TAO")
                                break
        except Exception as e:
            print(f"Error processing liquidations: {e}")

    def get_pending_liquidations(self):
        """Fetch pending liquidations from queue."""
        try:
            records = self.liquidation_worksheet.get_all_records()
            return [r for r in records if r['Status'] == 'Pending Manual Liquidation']
        except Exception as e:
            print(f"Error fetching pending liquidations: {e}")
            return []

    def mark_liquidation_complete(self, liquidation: dict):
        """Mark a liquidation as complete in the queue."""
        try:
            records = self.liquidation_worksheet.get_all_records()
            for i, record in enumerate(records, 2):  # Start at row 2 (after header)
                if (record['Timestamp'] == liquidation['timestamp'] and
                    abs(record['Alpha to Liquidate'] - liquidation['alpha_to_liquidate']) < 0.0001):
                    self.liquidation_worksheet.update_cell(i, 10, 'Completed')
                    print(f"Marked liquidation complete: {liquidation['date']}")
                    break
        except Exception as e:
            print(f"Error marking liquidation complete: {e}")

    def manual_entry_mode(self):
        """Manual entry mode for emissions."""
        print("Entering manual entry mode...")
        date = input("Enter date (YYYY-MM-DD HH:MM:SS): ")
        alpha_tokens = float(input("Enter ALPHA tokens received: "))
        tao_price = float(input("Enter TAO price in USD: "))
        timestamp = int(datetime.strptime(date, "%Y-%m-%d %H:%M:%S").timestamp())
        
        self.process_emission(
            alpha_tokens=alpha_tokens,
            timestamp=timestamp,
            tao_price=tao_price
        )