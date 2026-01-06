from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class WalletClientInterface(ABC):
    """Abstract interface for wallet/blockchain data clients."""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the client for logging purposes."""
        pass
    
    @abstractmethod
    def get_transfers(
        self,
        account_address: str,
        start_time: int,
        end_time: int,
        sender: Optional[str] = None,
        receiver: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch TAO transfers for an account.
        
        Args:
            account_address: SS58 address to query
            start_time: Unix timestamp start
            end_time: Unix timestamp end
            sender: Optional sender filter
            receiver: Optional receiver filter
            
        Returns:
            List of transfer dicts with keys:
                - timestamp: int
                - from: str (SS58)
                - to: str (SS58)
                - amount: float (TAO)
                - block_number: int
                - transaction_hash: str
                - tao_price_usd: Optional[float]
        """
        pass
    
    @abstractmethod
    def get_delegations(
        self,
        netuid: int,
        delegate: str,
        nominator: str,
        start_time: int,
        end_time: int,
        is_transfer: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch delegation events (DELEGATE/UNDELEGATE).
        
        Args:
            netuid: Subnet ID
            delegate: Validator hotkey SS58
            nominator: Coldkey SS58
            start_time: Unix timestamp start
            end_time: Unix timestamp end
            is_transfer: If True, only return DELEGATE events with transfers.
                        If False, only return events without transfers.
                        If None, return all events.
            
        Returns:
            List of delegation dicts with keys:
                - timestamp: int
                - action: str ('DELEGATE' or 'UNDELEGATE')
                - alpha: float (ALPHA amount)
                - amount: float (TAO amount in RAO, divide by 1e9)
                - usd: float (USD value at time)
                - alpha_price_in_usd: float
                - alpha_price_in_tao: float
                - block_number: int
                - extrinsic_id: str
                - is_transfer: Optional[bool]
                - transfer_address: Optional[str] (SS58)
                - fee: float
        """
        pass
    
    @abstractmethod
    def get_stake_balance_history(
        self,
        netuid: int,
        hotkey: str,
        coldkey: str,
        start_time: int,
        end_time: int
    ) -> List[Dict[str, Any]]:
        """
        Fetch historical stake balance snapshots.
        
        Args:
            netuid: Subnet ID
            hotkey: Validator hotkey SS58
            coldkey: Coldkey SS58
            start_time: Unix timestamp start
            end_time: Unix timestamp end
            
        Returns:
            List of balance dicts with keys:
                - timestamp: int
                - block_number: int
                - alpha_balance: int (in RAO, divide by 1e9)
                - tao_equivalent: int (in RAO)
        """
        pass