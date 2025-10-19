"""
Price data clients for cryptocurrency historical prices.
All clients implement the PriceClient interface for easy swapping.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
import requests
import time

from emissions_tracker.exceptions import PriceNotAvailableError


class PriceClient(ABC):
    """Abstract base class for price data clients"""
    
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
    
    def __init__(self, api_key: str):
        """
        Initialize CoinMarketCap client
        
        Args:
            api_key: Your CoinMarketCap Pro API key
        """
        self.api_key = api_key
        self.base_url = "https://pro-api.coinmarketcap.com"
        self.session = requests.Session()
        self.session.headers.update({
            'X-CMC_PRO_API_KEY': api_key,
            'Accept': 'application/json'
        })
    
    def get_price_at_timestamp(self, symbol: str, timestamp: int) -> float:
        """
        Get price at specific timestamp using 5-minute intervals
        
        Args:
            symbol: Cryptocurrency symbol (e.g., 'TAO')
            timestamp: Unix timestamp
            
        Returns:
            float: Price in USD
        """
        try:
            # Convert timestamp to ISO format
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            time_start = dt.strftime('%Y-%m-%dT%H:%M:%S')
            
            url = f"{self.base_url}/v2/cryptocurrency/quotes/historical"
            
            params = {
                'symbol': symbol,
                'time_start': time_start,
                'interval': '5m',
                'count': 1  # Just need one data point
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Navigate the response structure
            if 'data' in data and symbol in data['data']:
                quotes = data['data'][symbol]['quotes']
                if quotes and len(quotes) > 0:
                    price = quotes[0]['quote']['USD']['price']
                    print(f"✓ Got {symbol} price from CoinMarketCap: ${price:.2f} at {time_start}")
                    return price
            
            raise PriceNotAvailableError(f"No price data available for {symbol} at {time_start}")
            
        except requests.RequestException as e:
            raise PriceNotAvailableError(f"CoinMarketCap API error: {e}")
        except (KeyError, IndexError, TypeError) as e:
            raise PriceNotAvailableError(f"Unexpected response format: {e}")
    
    def get_current_price(self, symbol: str) -> float:
        """
        Get current price
        
        Args:
            symbol: Cryptocurrency symbol
            
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
                price = data['data'][symbol]['quote']['USD']['price']
                print(f"✓ Got current {symbol} price: ${price:.2f}")
                return price
            
            raise PriceNotAvailableError(f"No current price available for {symbol}")
            
        except requests.RequestException as e:
            raise PriceNotAvailableError(f"CoinMarketCap API error: {e}")


class CryptoCompareClient(PriceClient):
    """
    CryptoCompare API client (FREE tier available)
    
    Pricing: Free for 100,000 calls/month
    https://www.cryptocompare.com/
    
    Features:
    - Hourly interval data (free tier)
    - Minute data available with paid plans
    - Good for most use cases
    """
    
    def __init__(self, api_key: str = None):
        """
        Initialize CryptoCompare client
        
        Args:
            api_key: Optional API key for higher rate limits
        """
        self.api_key = api_key
        self.base_url = "https://min-api.cryptocompare.com/data"
        self.session = requests.Session()
        if api_key:
            self.session.headers.update({'authorization': f'Apikey {api_key}'})
    
    def get_price_at_timestamp(self, symbol: str, timestamp: int) -> float:
        """
        Get price at specific timestamp (hourly granularity on free tier)
        
        Args:
            symbol: Cryptocurrency symbol
            timestamp: Unix timestamp
            
        Returns:
            float: Price in USD
        """
        try:
            url = f"{self.base_url}/pricehistorical"
            
            params = {
                'fsym': symbol,
                'tsyms': 'USD',
                'ts': timestamp
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if symbol in data and 'USD' in data[symbol]:
                price = data[symbol]['USD']
                dt = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                print(f"✓ Got {symbol} price from CryptoCompare: ${price:.2f} at {dt}")
                return price
            
            raise PriceNotAvailableError(f"No price data available for {symbol}")
            
        except requests.RequestException as e:
            raise PriceNotAvailableError(f"CryptoCompare API error: {e}")
    
    def get_current_price(self, symbol: str) -> float:
        """
        Get current price
        
        Args:
            symbol: Cryptocurrency symbol
            
        Returns:
            float: Current price in USD
        """
        try:
            url = f"{self.base_url}/price"
            
            params = {
                'fsym': symbol,
                'tsyms': 'USD'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'USD' in data:
                price = data['USD']
                print(f"✓ Got current {symbol} price: ${price:.2f}")
                return price
            
            raise PriceNotAvailableError(f"No current price available for {symbol}")
            
        except requests.RequestException as e:
            raise PriceNotAvailableError(f"CryptoCompare API error: {e}")


class CoinGeckoClient(PriceClient):
    """
    CoinGecko API client
    
    Free tier: Daily prices only (midnight UTC)
    Pro tier ($130/month): Minute-level data
    https://www.coingecko.com/en/api/pricing
    
    Features:
    - Wide coverage of cryptocurrencies
    - Free tier available (limited granularity)
    """
    
    def __init__(self, api_key: str = None):
        """
        Initialize CoinGecko client
        
        Args:
            api_key: Optional API key for Pro features
        """
        self.api_key = api_key
        self.base_url = "https://api.coingecko.com/api/v3"
        if api_key:
            self.base_url = "https://pro-api.coingecko.com/api/v3"
        self.session = requests.Session()
        if api_key:
            self.session.headers.update({'x-cg-pro-api-key': api_key})
        
        # Symbol to CoinGecko ID mapping
        self.symbol_to_id = {
            'TAO': 'bittensor',
            'BTC': 'bitcoin',
            'ETH': 'ethereum',
            # Add more as needed
        }
    
    def get_price_at_timestamp(self, symbol: str, timestamp: int) -> float:
        """
        Get price at specific timestamp
        Free tier: Daily prices only (midnight UTC)
        Pro tier: Minute-level data
        
        Args:
            symbol: Cryptocurrency symbol
            timestamp: Unix timestamp
            
        Returns:
            float: Price in USD
        """
        try:
            coin_id = self.symbol_to_id.get(symbol)
            if not coin_id:
                raise PriceNotAvailableError(f"Unknown symbol: {symbol}")
            
            # Free tier only has daily data
            date = datetime.utcfromtimestamp(timestamp).strftime('%d-%m-%Y')
            url = f"{self.base_url}/coins/{coin_id}/history"
            
            params = {'date': date}
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'market_data' in data and 'current_price' in data['market_data']:
                price = data['market_data']['current_price']['usd']
                receipt_time = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%H:%M:%S UTC')
                print(f"⚠️  Got {symbol} daily price from CoinGecko (00:00 UTC): ${price:.2f}")
                print(f"    Actual receipt time: {receipt_time}")
                print(f"    Consider using a more accurate price source")
                return price
            
            raise PriceNotAvailableError(f"No price data available for {symbol}")
            
        except requests.RequestException as e:
            raise PriceNotAvailableError(f"CoinGecko API error: {e}")
    
    def get_current_price(self, symbol: str) -> float:
        """
        Get current price
        
        Args:
            symbol: Cryptocurrency symbol
            
        Returns:
            float: Current price in USD
        """
        try:
            coin_id = self.symbol_to_id.get(symbol)
            if not coin_id:
                raise PriceNotAvailableError(f"Unknown symbol: {symbol}")
            
            url = f"{self.base_url}/simple/price"
            
            params = {
                'ids': coin_id,
                'vs_currencies': 'usd'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if coin_id in data and 'usd' in data[coin_id]:
                price = data[coin_id]['usd']
                print(f"✓ Got current {symbol} price: ${price:.2f}")
                return price
            
            raise PriceNotAvailableError(f"No current price available for {symbol}")
            
        except requests.RequestException as e:
            raise PriceNotAvailableError(f"CoinGecko API error: {e}")