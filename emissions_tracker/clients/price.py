from abc import ABC, abstractmethod


class PriceClient(ABC):
    """Abstract interface for cryptocurrency price clients."""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the client for logging purposes."""
        pass
    
    @abstractmethod
    def get_price_at_timestamp(self, symbol: str, timestamp: int) -> float:
        """
        Get the price of a cryptocurrency at a specific timestamp.
        
        Args:
            symbol: The cryptocurrency symbol (e.g., 'TAO')
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
        Get the current price of a cryptocurrency.
        
        Args:
            symbol: The cryptocurrency symbol
            
        Returns:
            float: Current price in USD
            
        Raises:
            PriceNotAvailableError: If price cannot be retrieved
        """
        pass
    
    @abstractmethod
    def get_prices_in_range(self, symbol: str, start_time: int, end_time: int):
        """
        Get prices for a symbol within a time range.
        
        Args:
            symbol: The cryptocurrency symbol
            start_time: Unix start timestamp (inclusive)
            end_time: Unix end timestamp (inclusive)
        
        Returns:
            Iterable of price points with timestamps (implementation-specific structure)
        
        Raises:
            PriceNotAvailableError: If price cannot be retrieved
        """
        pass
