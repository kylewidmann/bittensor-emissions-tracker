
"""
Wallet clients for accessing Bittensor addresses.
All clients implement the WalletClient interface for easy swapping.
"""

from abc import ABC, abstractmethod


class WalletClientInterface(ABC):
    """Interface for wallet-related data queries (transfers, emissions)."""
    
    @abstractmethod
    def get_transfers(self, account_address: str, start_time: int, end_time: int, 
                     sender: str = None, receiver: str = None) -> list:
        """
        Fetch transfers involving an account, filtered by sender/receiver and time.
        
        Args:
            account_address: SS58 address to query
            start_time: Unix timestamp for start of range
            end_time: Unix timestamp for end of range
            sender: Optional sender address filter
            receiver: Optional receiver address filter
            
        Returns:
            list: Transfer events with keys: timestamp, from, to, amount, unit (tao/alpha), 
                  block_number, tao_price_usd (optional)
        """
        pass

    @abstractmethod
    def get_delegations(self, netuid: int, delegate: str, nominator: str, start_time: int, end_time: int) -> list:
        """
        Fetch delegation/stake transfers.
        
        Returns:
            list: Delegation events with timestamp, action, amount, alpha, usd, etc.
        """
        pass

    @abstractmethod
    def get_stake_balance_history(self, netuid: int, hotkey: str, coldkey: str, start_time: int, end_time: int) -> list:
        """
        Fetch historical stake balances for alpha tracking.
        
        Returns:
            list: Balance snapshots with timestamp, balance (alpha), balance_as_tao
        """
        pass