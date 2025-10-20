"""
Price data clients for cryptocurrency historical prices.
All clients implement the PriceClient interface for easy swapping.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
import time
import requests
import backoff

from emissions_tracker.config import CoinMarketCapSettings
from emissions_tracker.exceptions import PriceNotAvailableError


class PriceClient(ABC):
    """Abstract base class for price data clients"""

    @property
    @abstractmethod
    def name(self):
        raise NotImplementedError()
    
    @abstractmethod
    def get_price_at_timestamp(self, symbol: str, timestamp: int) -> float:
        """
        Get the price of a cryptocurrency at a specific timestamp
        
        Args:
            symbol: The cryptocurrency symbol (e.g., 'TAO', 'BTC')
            timestamp: Unix timestamp
            
        Returns:
            float: Price in USD
            
        Raises:
            PriceNotAvailableError: If price cannot be retrieved
        """
        pass
    
    @abstractmethod
    def get_current_price(self, symbol: str) -> float:
        """
        Get the current price of a cryptocurrency
        
        Args:
            symbol: The cryptocurrency symbol
            
        Returns:
            float: Current price in USD
        """
        pass


class CoinMarketCapClient(PriceClient):
    """
    CoinMarketCap Pro API client
    Pricing: Starting at $29/month for 10,000 calls/month
    https://coinmarketcap.com/api/pricing/
    Features:
    - 5-minute interval data
    - Very accurate for tax purposes
    - High rate limits
    """
    
    def __init__(self):
        """
        Initialize CoinMarketCap client
        """
        self.config = CoinMarketCapSettings()
        self.api_key = self.config.cmc_api_key
        self.base_url = "https://pro-api.coinmarketcap.com"
        self.session = requests.Session()
        self.session.headers.update({
            'X-CMC_PRO_API_KEY': self.api_key,
            'Accept': 'application/json'
        })
        self.bittensor_id = 22974  # CoinMarketCap ID for Bittensor (TAO)
    
    @property
    def name(self):
        return "CoinMarketCap"

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.HTTPError,
        max_tries=3,
        giveup=lambda e: e.response is None or e.response.status_code != 429,
        factor=6
    )
    def get_price_at_timestamp(self, symbol: str, timestamp: int) -> float:
        """
        Get price at specific timestamp using 5-minute intervals
        
        Args:
            symbol: Cryptocurrency symbol (default: 'TAO')
            timestamp: Unix timestamp
            
        Returns:
            float: Price in USD
            
        Raises:
            PriceNotAvailableError: If price cannot be retrieved
        """
        try:
            # Convert timestamp to ISO format
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            time_start = dt.strftime('%Y-%m-%dT%H:%M:%S')
            
            url = f"{self.base_url}/v2/cryptocurrency/quotes/historical"
            symbol = "TAO"

            params = {
                'symbol': symbol,
                'time_start': time_start,
                'interval': '5m',
                'count': 1
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Find Bittensor (id: 22974) in the TAO list
            if 'data' in data and symbol in data['data']:
                for asset in data['data'][symbol]:
                    if asset['id'] == self.bittensor_id and asset['quotes']:
                        price = asset['quotes'][0]['quote']['USD']['price']
                        print(f"✓ Got {symbol} price from CoinMarketCap: ${price:.2f} at {time_start}")
                        time.sleep(2) # Sleep to avoid rate limit of 30req/s
                        return price
            
            raise PriceNotAvailableError(f"No price data available for {symbol} (Bittensor) at {time_start}")
            
        except requests.RequestException as e:
            raise PriceNotAvailableError(f"CoinMarketCap API error: {e}")
        except (KeyError, IndexError, TypeError) as e:
            raise PriceNotAvailableError(f"Unexpected response format: {e}")
    
    def get_current_price(self, symbol: str = "TAO") -> float:
        """
        Get current price
        
        Args:
            symbol: Cryptocurrency symbol (default: 'TAO')
            
        Returns:
            float: Current price in USD
        """
        try:
            url = f"{self.base_url}/v2/cryptocurrency/quotes/latest"
            
            params = {'symbol': symbol}
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'data' in data and symbol in data['data']:
                for asset in data['data'][symbol]:
                    if asset['id'] == self.bittensor_id:
                        price = asset['quote']['USD']['price']
                        print(f"✓ Got current {symbol} price: ${price:.2f}")
                        return price
            
            raise PriceNotAvailableError(f"No current price available for {symbol} (Bittensor)")
            
        except requests.RequestException as e:
            raise PriceNotAvailableError(f"CoinMarketCap API error: {e}")
        except (KeyError, TypeError) as e:
            raise PriceNotAvailableError(f"Unexpected response format: {e}")