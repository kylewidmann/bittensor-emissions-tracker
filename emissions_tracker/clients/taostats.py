from datetime import datetime, timezone
import time
import traceback

import requests
import backoff

from emissions_tracker.clients.price import PriceClient
from emissions_tracker.clients.wallet import WalletClientInterface
from emissions_tracker.config import TaoStatsSettings
from emissions_tracker.exceptions import PriceNotAvailableError

class TaoStatsAPIClient(WalletClientInterface, PriceClient):
    """Client for Taostats API account transactions."""
    
    def __init__(self):

        self.config = TaoStatsSettings()
        if not self.config.api_key:
            raise ValueError("TAOSTATS_API_KEY is required. Sign up at https://dash.taostats.io/")
        self.api_key = self.config.api_key
        self.base_url = self.config.base_url
        self.headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json"
        }
        self._last_call_time = None

    @property
    def name(self):
        return "TaoStats API"

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.HTTPError,
        max_tries=3,
        giveup=lambda e: e.response is None or e.response.status_code != 429,
        factor=6
    )
    def _fetch_with_pagination(self, url: str, params: dict):
        """Helper to fetch all pages from an endpoint."""
        all_data = []
        page = 1
        while True:
            if self._last_call_time and time.time() - self._last_call_time < 12:
                time.sleep(12)  # Pace at 12s to stay under 5 req/min
            params['page'] = page
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            items = data['data']
            all_data.extend(items)
            
            self._last_call_time = time.time()
            if data['pagination']['next_page'] is None:
                break

            page += 1
        return all_data

    def get_transfers(self, account_address: str, start_time: int, end_time: int, 
                     sender: str = None, receiver: str = None) -> list:
        """
        Fetch transfers via Taostats API.
        
        Args:
            account_address: SS58 address to query
            start_time: Unix timestamp start
            end_time: Unix timestamp end
            sender: Optional sender filter
            receiver: Optional receiver filter
            
        Returns:
            list: Transfers with timestamp, from, to, amount, unit, block_number, tao_price_usd, tao_equivalent
        """
        try:
            url = f"{self.base_url}/transfer/v1"
            params = {
                "address": account_address,
                "start_timestamp": start_time,
                "end_timestamp": end_time,
                "per_page": 50  # Adjust as needed
            }
            if sender:
                params["sender"] = sender
            if receiver:
                params["receiver"] = receiver

            transfer_data = self._fetch_with_pagination(url, params)
            
            transfers = []
            for t in transfer_data:
                timestamp = int(datetime.fromisoformat(t['timestamp'].replace('Z', '+00:00')).timestamp())
                amount = int(t['amount']) / 1e9  # RAO to TAO/alpha
                unit = 'tao'  # From your examples, transfers are TAO
                tao_price_usd = None  # Not in this endpoint; fallback to PriceClient
                tao_equivalent = amount
                
                transfers.append({
                    'timestamp': timestamp,
                    'from': t['from']['ss58'],
                    'to': t['to']['ss58'],
                    'amount': amount,
                    'unit': unit,
                    'block_number': t['block_number'],
                    'tao_price_usd': tao_price_usd,
                    'tao_equivalent': tao_equivalent
                })
            
            return transfers
        except Exception as e:
            print(f"Taostats API error in get_transfers: {e}")
            return []

    def get_delegations(self, netuid: int, delegate: str, nominator: str, start_time: int, end_time: int) -> list:
        """
        Fetch delegation/stake transfers via Taostats API.
        
        Returns:
            list: Delegation events with timestamp, action, amount, alpha, usd, etc.
        """
        try:
            url = f"{self.base_url}/delegation/v1"
            params = {
                "action": "all",
                "netuid": netuid,
                "delegate": delegate,
                "nominator": nominator,
                "start_timestamp": start_time,
                "end_timestamp": end_time,
                "per_page": 50
            }

            delegation_data = self._fetch_with_pagination(url, params)
            
            delegations = []
            for d in delegation_data:
                timestamp = int(datetime.fromisoformat(d['timestamp'].replace('Z', '+00:00')).timestamp())
                amount = float(d['amount']) / 1e9
                alpha = float(d['alpha'])
                unit = 'alpha' if alpha > 0 else 'tao'
                tao_price_usd = float(d['alpha_price_in_usd']) if d['alpha_price_in_usd'] else None
                tao_equivalent = amount if unit == 'tao' else float(d['amount']) / 1e9  # 'amount' is TAO
                
                delegations.append({
                    'timestamp': timestamp,
                    'from': d['delegate']['ss58'] if d['action'] == 'UNDELEGATE' else d['nominator']['ss58'],
                    'to': d['nominator']['ss58'] if d['action'] == 'UNDELEGATE' else d['delegate']['ss58'],
                    'amount': alpha if unit == 'alpha' else amount,
                    'unit': unit,
                    'block_number': d['block_number'],
                    'tao_price_usd': tao_price_usd,
                    'tao_equivalent': tao_equivalent,
                    'action': d['action'],
                    'usd': float(d['usd']),
                    "transfer_address": d.get("transfer_address") and d["transfer_address"].get("ss58", None)
                })
            
            return delegations
        except Exception as e:
            print(f"Taostats API error in get_delegations: {e}")
            print(traceback.format_exc())
            return []

    def get_stake_balance_history(self, netuid: int, hotkey: str, coldkey: str, start_time: int, end_time: int) -> list:
        """
        Fetch historical stake balances for alpha tracking via Taostats API.
        
        Returns:
            list: Balance snapshots with timestamp, alpha_balance, tao_equivalent
        """
        try:
            url = f"{self.base_url}/dtao/stake_balance/history/v1"
            params = {
                "netuid": netuid,
                "hotkey": hotkey,
                "coldkey": coldkey,
                "start_timestamp": start_time,
                "end_timestamp": end_time,
                "per_page": 50,
                "order":"timestamp_asc"
            }

            history_data = self._fetch_with_pagination(url, params)
            
            balances = []
            for h in history_data:
                timestamp = int(datetime.fromisoformat(h['timestamp'].replace('Z', '+00:00')).timestamp())
                alpha_balance = int(h['balance'])
                tao_equivalent = int(h['balance_as_tao'])
                
                balances.append({
                    'timestamp': timestamp,
                    'block_number': h['block_number'],
                    'alpha_balance': alpha_balance,
                    'tao_equivalent': tao_equivalent
                })
            
            return balances
        except Exception as e:
            print(f"Taostats API error in get_stake_balance_history: {e}")
            return []

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.HTTPError,
        max_tries=3,
        giveup=lambda e: e.response is None or e.response.status_code != 429,
        factor=6
    )
    def get_price_at_timestamp(self, symbol: str, timestamp: int) -> float:
        """
        Get the price of a cryptocurrency at a specific timestamp using Taostats API.
        Uses a ±30 minute buffer to handle gaps in price history and selects the closest price.
        
        Args:
            symbol: The cryptocurrency symbol (e.g., 'TAO')
            timestamp: Unix timestamp
            
        Returns:
            float: Price in USD
            
        Raises:
            PriceNotAvailableError: If price cannot be retrieved within ±30 minutes
        """
        try:
            if symbol != 'TAO':
                raise PriceNotAvailableError(f"Taostats API only supports TAO, got {symbol}")
            
            url = f"{self.base_url}/price/history/v1"
            buffer = 900  # 30 minutes in seconds
            params = {
                "asset": symbol,
                "timestamp_start": timestamp - buffer,
                "timestamp_end": timestamp + buffer,
                "order": "timestamp_asc",
                "per_page": 50
            }
            
            data = self._fetch_with_pagination(url, params)
            
            
            if data and len(data) > 0:
                # Find the closest price by timestamp
                closest_price = min(
                    data,
                    key=lambda x: abs(int(datetime.fromisoformat(x['created_at'].replace('Z', '+00:00')).timestamp()) - timestamp)
                )
                price = float(closest_price['price'])
                price_time = datetime.fromisoformat(closest_price['created_at'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
                print(f"✓ Got {symbol} price from Taostats: ${price:.2f} at {price_time} (closest to {datetime.fromtimestamp(timestamp, tz=timezone.utc)})")
                return price
            
            raise PriceNotAvailableError(f"No price data available for {symbol} within ±30 minutes of {datetime.fromtimestamp(timestamp, tz=timezone.utc)}")
            
        except requests.RequestException as e:
            raise PriceNotAvailableError(f"Taostats API error: {e}")
        except (KeyError, TypeError, ValueError) as e:
            raise PriceNotAvailableError(f"Unexpected response format: {e}")

    def get_current_price(self, symbol: str) -> float:
        """
        Get the current price of a cryptocurrency using Taostats API.
        
        Args:
            symbol: The cryptocurrency symbol
            
        Returns:
            float: Current price in USD
        """
        try:
            if symbol != 'TAO':
                raise PriceNotAvailableError(f"Taostats API only supports TAO, got {symbol}")
            
            url = f"{self.base_url}/price/latest/v1"
            params = {
                "asset": symbol
            }
            
            data = self._fetch_with_pagination(url, params)
            
            if data and len(data) > 0:
                price = float(data[0]['price'])
                price_time = datetime.fromisoformat(data[0]['created_at'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
                print(f"✓ Got current {symbol} price from Taostats: ${price:.2f} at {price_time}")
                return price
            
            raise PriceNotAvailableError(f"No current price available for {symbol}")
            
        except requests.RequestException as e:
            raise PriceNotAvailableError(f"Taostats API error: {e}")
        except (KeyError, TypeError, ValueError) as e:
            raise PriceNotAvailableError(f"Unexpected response format: {e}")