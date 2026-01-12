from datetime import datetime, timezone
import time
import traceback
from typing import List, Dict, Any, Optional

import requests
import backoff

from emissions_tracker.clients.price import PriceClient
from emissions_tracker.clients.wallet import WalletClientInterface
from emissions_tracker.config import TaoStatsSettings
from emissions_tracker.exceptions import PriceNotAvailableError
from emissions_tracker.models import (
    TaoStatsTransfer,
    TaoStatsDelegation,
    TaoStatsStakeBalance,
    TaoStatsAddress
)


class TaoStatsAPIClient(WalletClientInterface, PriceClient):
    """Client for Taostats API - provides wallet data and price information."""
    
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
        self._price_bucket_cache = {}   # keyed by 15m bucket
        self._price_window_cache = {}   # keyed by (start, end)
        self._rate_limit_seconds = self.config.rate_limit_seconds  # Configurable rate limit

    @property
    def name(self) -> str:
        return "TaoStats API"

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.HTTPError,
        max_tries=3,
        giveup=lambda e: e.response is None or e.response.status_code != 429,
        factor=6
    )
    def _fetch_with_pagination(self, url: str, params: dict, per_page: int = 50, context: str = "") -> List[Dict[str, Any]]:
        """Helper to fetch all pages from an endpoint with throttling and lightweight progress."""
        all_data = []
        page = 1
        while True:
            if self._last_call_time and time.time() - self._last_call_time < self._rate_limit_seconds:
                sleep_time = self._rate_limit_seconds - (time.time() - self._last_call_time)
                time.sleep(sleep_time)  # Enforce configured rate limit
            params['page'] = page
            params['limit'] = per_page
            if page == 1 or page % 5 == 0:
                label = context or url
                print(f"  Fetching {label}, page {page} (limit={per_page})")
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

    def get_transfers(
        self,
        account_address: str,
        start_time: int,
        end_time: int,
        sender: Optional[str] = None,
        receiver: Optional[str] = None
    ) -> List[TaoStatsTransfer]:
        """Fetch TAO transfers via Taostats API."""
        try:
            url = f"{self.base_url}/transfer/v1"
            params = {
                "address": account_address,
                "timestamp_start": start_time,
                "timestamp_end": end_time,
                "order": "timestamp_asc"
            }
            if sender:
                params["sender"] = sender
            if receiver:
                params["receiver"] = receiver
            
            transfer_data = self._fetch_with_pagination(url, params, per_page=500, context="transfers")
            
            transfers = []
            for t in transfer_data:
                transfer = TaoStatsTransfer(
                    block_number=t['block_number'],
                    timestamp=t['timestamp'],
                    transaction_hash=t['transaction_hash'],
                    extrinsic_id=t['extrinsic_id'],
                    amount=t['amount'],
                    fee=t.get('fee'),
                    from_address=TaoStatsAddress(ss58=t['from']['ss58'], hex=t['from']['hex']),
                    to_address=TaoStatsAddress(ss58=t['to']['ss58'], hex=t['to']['hex'])
                )
                transfers.append(transfer)
            
            return transfers
        except Exception as e:
            print(f"Taostats API error in get_transfers: {e}")
            return []

    def get_delegations(
        self,
        netuid: int,
        delegate: str,
        nominator: str,
        start_time: int,
        end_time: int,
        is_transfer: Optional[bool] = None
    ) -> List[TaoStatsDelegation]:
        """Fetch delegation/stake events via Taostats API.
        
        Args:
            is_transfer: If True, only return DELEGATE events with transfers to another address.
                        If False, only return events without transfers.
                        If None, return all events.
        """
        try:
            url = f"{self.base_url}/delegation/v1"
            params = {
                "action": "all",
                "netuid": netuid,
                "delegate": delegate,
                "nominator": nominator,
                "timestamp_start": start_time,
                "timestamp_end": end_time,
                "order": "timestamp_asc"
            }
            if is_transfer is not None:
                params["is_transfer"] = "true" if is_transfer else "false"

            delegation_data = self._fetch_with_pagination(url, params, per_page=500, context="delegations")
            
            delegations = []
            for d in delegation_data:
                delegation = TaoStatsDelegation(
                    block_number=int(d['block_number']),
                    timestamp=d['timestamp'],
                    action=d['action'],
                    nominator=TaoStatsAddress(ss58=d['nominator']['ss58'], hex=d['nominator']['hex']),
                    delegate=TaoStatsAddress(ss58=d['delegate']['ss58'], hex=d['delegate']['hex']),
                    netuid=int(d['netuid']),
                    amount=int(d['amount']),
                    alpha=int(d['alpha']),
                    usd=float(d['usd']),
                    alpha_price_in_usd=d.get('alpha_price_in_usd'),
                    alpha_price_in_tao=d.get('alpha_price_in_tao'),
                    slippage=d.get('slippage'),
                    extrinsic_id=d['extrinsic_id'],
                    is_transfer=d.get('is_transfer'),
                    transfer_address=TaoStatsAddress(ss58=d['transfer_address']['ss58'], hex=d['transfer_address']['hex']) if d.get('transfer_address') else None,
                    fee=d.get('fee')
                )
                delegations.append(delegation)
            
            return delegations
        except Exception as e:
            print(f"Taostats API error in get_delegations: {e}")
            print(traceback.format_exc())
            return []

    def get_stake_balance_history(
        self,
        netuid: int,
        hotkey: str,
        coldkey: str,
        start_time: int,
        end_time: int
    ) -> List[TaoStatsStakeBalance]:
        """Fetch historical stake balance snapshots via Taostats API."""
        try:
            url = f"{self.base_url}/dtao/stake_balance/history/v1"
            params = {
                "netuid": netuid,
                "hotkey": hotkey,
                "coldkey": coldkey,
                "start_time": start_time,
                "end_time": end_time,
                "order": "timestamp_asc"
            }

            history_data = self._fetch_with_pagination(url, params, per_page=500, context="stake_balance_history")
            
            balances = []
            for h in history_data:
                balance = TaoStatsStakeBalance(
                    block_number=h['block_number'],
                    timestamp=h['timestamp'],
                    hotkey_name=h['hotkey_name'],
                    hotkey=TaoStatsAddress(ss58=h['hotkey']['ss58'], hex=h['hotkey']['hex']),
                    coldkey=TaoStatsAddress(ss58=h['coldkey']['ss58'], hex=h['coldkey']['hex']),
                    netuid=h['netuid'],
                    balance=h['balance'],
                    balance_as_tao=h['balance_as_tao']
                )
                balances.append(balance)
            
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
        """Get the price of TAO at a specific timestamp."""
        try:
            if symbol != 'TAO':
                raise PriceNotAvailableError(f"Taostats API only supports TAO, got {symbol}")
            
            # Cache by 15-minute bucket to avoid repeat API calls while keeping throttle
            bucket = int(timestamp // 900)
            if bucket in self._price_bucket_cache:
                return self._price_bucket_cache[bucket]
            
            buffer = 1800  # 30 minutes in seconds
            url = f"{self.base_url}/price/history/v1"
            params = {
                "asset": symbol,
                "timestamp_start": timestamp - buffer,
                "timestamp_end": timestamp + buffer,
                "order": "timestamp_asc",
                "per_page": 50
            }
            
            data = self._fetch_with_pagination(url, params, context="price_at_timestamp")
            
            if data and len(data) > 0:
                # Find the closest price by timestamp
                closest_price = min(
                    data,
                    key=lambda x: abs(int(datetime.fromisoformat(x['created_at'].replace('Z', '+00:00')).timestamp()) - timestamp)
                )
                price = float(closest_price['price'])
                price_time = datetime.fromisoformat(closest_price['created_at'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
                print(f"✓ Got {symbol} price from Taostats: ${price:.2f} at {price_time}")
                self._price_bucket_cache[bucket] = price
                return price
            
            raise PriceNotAvailableError(f"No price data available for {symbol} within ±30 minutes of {datetime.fromtimestamp(timestamp, tz=timezone.utc)}")
            
        except requests.RequestException as e:
            raise PriceNotAvailableError(f"Taostats API error: {e}")
        except (KeyError, TypeError, ValueError) as e:
            raise PriceNotAvailableError(f"Unexpected response format: {e}")

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.HTTPError,
        max_tries=3,
        giveup=lambda e: e.response is None or e.response.status_code != 429,
        factor=6
    )
    def get_prices_in_range(self, symbol: str, start_time: int, end_time: int):
        """Fetch all prices within a range; used to avoid per-timestamp calls."""
        if symbol != 'TAO':
            raise PriceNotAvailableError(f"Taostats API only supports TAO, got {symbol}")
        
        cache_key = (start_time, end_time)
        if cache_key in self._price_window_cache:
            return self._price_window_cache[cache_key]
        
        url = f"{self.base_url}/price/history/v1"
        params = {
            "asset": symbol,
            "timestamp_start": start_time,
            "timestamp_end": end_time,
            "order": "timestamp_asc",
            "per_page": 500
        }

        # Fetch the full window in a single paginated flow; let _fetch_with_pagination
        # handle paging rather than chunking locally. This avoids overlapping
        # windows which can produce duplicate or repeated page-1 calls.
        data = self._fetch_with_pagination(url, params, per_page=500, context="price_range")

        prices = []
        for item in data:
            ts = int(datetime.fromisoformat(item['created_at'].replace('Z', '+00:00')).timestamp())
            prices.append({
                "timestamp": ts,
                "price": float(item['price'])
            })

        # Deduplicate by timestamp in case the API returns overlapping entries
        unique = {}
        for p in prices:
            unique[p['timestamp']] = p['price']

        prices = sorted([{"timestamp": ts, "price": unique[ts]} for ts in unique], key=lambda x: x['timestamp'])
        self._price_window_cache[cache_key] = prices
        return prices

    def get_current_price(self, symbol: str) -> float:
        """Get the current price of TAO."""
        try:
            if symbol != 'TAO':
                raise PriceNotAvailableError(f"Taostats API only supports TAO, got {symbol}")
            
            url = f"{self.base_url}/price/latest/v1"
            params = {"asset": symbol}
            
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
