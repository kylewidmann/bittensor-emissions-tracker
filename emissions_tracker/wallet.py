
"""
Wallet clients for accessing Bittensor addresses.
All clients implement the WalletClient interface for easy swapping.
"""

from abc import ABC, abstractmethod
from typing import Optional
import bittensor as bt


class WalletClient(ABC):
    """Abstract base class for wallet clients"""
    
    def __init__(self, address: str):
        self._address = address

    @property
    def address(self) -> str:
        """
        Get the SS58 address for this wallet
        
        Returns:
            str: SS58 formatted address
        """
        return self._address
     
    @property
    @abstractmethod
    def type(self) -> str:
        """
        Get the wallet type (e.g., 'bittensor', 'talisman', 'address-only')
        
        Returns:
            str: Wallet type identifier
        """
        raise NotImplementedError()
    
    def can_sign_transactions(self) -> bool:
        """
        Check if this wallet can sign transactions
        
        Returns:
            bool: True if wallet can sign, False for read-only wallets
        """
        return False


class AddressOnlyWallet(WalletClient):
    """
    Read-only wallet that only stores an address.
    Used for Talisman, Ledger, or any hardware wallet where you only need to track transactions.
    Cannot sign transactions - for tracking only.
    """
    
    def __init__(self, address: str):
        """
        Initialize with just an SS58 address
        
        Args:
            address: SS58 formatted Bittensor address
        """
        super().__init__(address)
        self._validate_address()
    
    def _validate_address(self):
        """Validate the address format"""
        # Basic validation - Bittensor addresses should start with '5'
        if not self.address or not isinstance(self.address, str):
            raise ValueError("Address must be a non-empty string")
        
        if not self.address.startswith('5'):
            print(f"⚠️  Warning: Address doesn't start with '5'. This may not be a valid Bittensor address.")
        
        # SS58 addresses are typically 47-48 characters
        if len(self.address) < 40 or len(self.address) > 50:
            print(f"⚠️  Warning: Address length unusual ({len(self.address)} chars). Typical SS58 addresses are 47-48 chars.")
    
    @property
    def type(self) -> str:
        """Identify as address-only wallet"""
        return "address-only"
    
    def can_sign_transactions(self) -> bool:
        """Cannot sign - read only"""
        return False
    
    def __str__(self):
        return f"AddressOnlyWallet({self.address[:8]}...{self.address[-6:]})"


class TalismanWallet(AddressOnlyWallet):
    """
    Wallet for Talisman browser extension users.
    Talisman manages the keys (possibly with Ledger hardware), we just track the address.
    """
    
    def __init__(self, address: str, wallet_name: Optional[str] = None):
        """
        Initialize Talisman wallet
        
        Args:
            address: SS58 address from Talisman
            wallet_name: Optional friendly name for the wallet
        """
        super().__init__(address)
        self.wallet_name = wallet_name or "Talisman Wallet"
    
    @property
    def type(self) -> str:
        """Identify as Talisman wallet"""
        return "talisman"
    
    def __str__(self):
        return f"TalismanWallet('{self.wallet_name}', {self.address[:8]}...{self.address[-6:]})"


class BittensorWallet(WalletClient):
    """
    Native Bittensor wallet using the bittensor library.
    Can optionally sign transactions if coldkey is available.
    """
    
    def __init__(self, wallet_name: str, hotkey_name: str, coldkey_name: Optional[str] = None):
        """
        Initialize Bittensor wallet
        
        Args:
            wallet_name: Name of the bittensor wallet
            hotkey_name: Name of the hotkey
            coldkey_name: Optional coldkey name (needed for signing)
        """
        self.wallet_name = wallet_name
        self.hotkey_name = hotkey_name
        self.coldkey_name = coldkey_name
        
        # Create bittensor wallet instance
        self.wallet = bt.wallet(name=wallet_name, hotkey=hotkey_name)
        super().__init__(self.wallet.hotkey.ss58_address)
    
    
    def type(self) -> str:
        """Identify as bittensor wallet"""
        return "bittensor"
    
    def can_sign_transactions(self) -> bool:
        """Can sign if coldkey is available"""
        return self.coldkey_name is not None
    
    def __str__(self):
        return f"BittensorWallet('{self.wallet_name}', hotkey='{self.hotkey_name}')"
