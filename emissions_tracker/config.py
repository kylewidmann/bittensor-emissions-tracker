from pathlib import Path
from typing import Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings

from emissions_tracker.models import CostBasisMethod


class TrackerSettings(BaseSettings):
    """Core tracker configuration for wallet addresses and Google Sheets."""
    
    # Wallet addresses
    brokerage_ss58: str = Field(..., alias="BROKER_SS58", description="Kraken deposit address")
    validator_ss58: str = Field(..., alias="VALIDATOR_SS58", description="Validator hotkey")
    payout_coldkey_ss58: str = Field(..., alias="PAYOUT_COLDKEY_SS58", description="Coldkey wallet that receives smart contract payouts")
    smart_contract_ss58: str = Field(..., alias="SMART_CONTRACT_SS58", description="Smart contract address for filtering contract income")
    
    # Mining tracker addresses (optional)
    miner_hotkey_ss58: str = Field(None, alias="MINER_HOTKEY_SS58", description="Miner hotkey for mining emissions tracking")
    miner_coldkey_ss58: str = Field(None, alias="MINER_COLDKEY_SS58", description="Miner coldkey for mining emissions tracking (if different from payout_coldkey_ss58)")
    
    # Google Sheets
    tracker_sheet_id: str = Field(..., alias="TRACKER_SHEET_ID", description="Google Sheet ID for smart contract tracking")
    mining_tracker_sheet_id: str = Field(None, alias="MINING_TRACKER_SHEET_ID", description="Google Sheet ID for mining emissions tracking")
    tracker_google_credentials: str = Field(..., alias="TRACKER_GOOGLE_CREDENTIALS", description="Path to Google service account credentials")
    
    # Subnet configuration
    subnet_id: int = Field(64, alias="SUBNET_ID", description="Bittensor subnet ID")
    # Lot consumption strategy: HIFO (default) or FIFO. HIFO = Highest cost-basis first.
    lot_strategy: CostBasisMethod = Field(CostBasisMethod.HIFO, alias="LOT_STRATEGY", description="Lot consumption strategy: CostBasisMethod.HIFO or CostBasisMethod.FIFO")
    
    @field_validator('lot_strategy', mode='before')
    @classmethod
    def parse_lot_strategy(cls, v):
        """Parse lot_strategy from string to enum if needed."""
        if isinstance(v, str):
            return CostBasisMethod[v.upper()]
        return v
    
    # API rate limiting
    taostats_rate_limit_seconds: float = Field(
        1.0, 
        alias="TAOSTATS_RATE_LIMIT_SECONDS",
        description="Minimum seconds between TaoStats API requests (default: 1.0 for 60 req/min)"
    )


class WaveAccountSettings(BaseSettings):
    """Configurable Wave account names for journal entries."""
    
    # Income accounts
    contract_income_account: str = Field(
        "Contractor Income - Alpha",
        alias="WAVE_CONTRACT_INCOME_ACCOUNT",
        description="Wave account for smart contract income"
    )
    staking_income_account: str = Field(
        "Staking Income - Alpha", 
        alias="WAVE_STAKING_INCOME_ACCOUNT",
        description="Wave account for staking/emissions income"
    )
    mining_income_account: str = Field(
        "Mining Income - Alpha",
        alias="WAVE_MINING_INCOME_ACCOUNT",
        description="Wave account for mining emissions income"
    )
    
    # Asset accounts
    alpha_asset_account: str = Field(
        "Alpha Holdings",
        alias="WAVE_ALPHA_ASSET_ACCOUNT",
        description="Wave account for ALPHA holdings"
    )
    tao_asset_account: str = Field(
        "TAO Holdings",
        alias="WAVE_TAO_ASSET_ACCOUNT",
        description="Wave account for TAO holdings"
    )
    transfer_proceeds_account: str = Field(
        "Exchange Clearing - Kraken",
        alias="WAVE_TRANSFER_PROCEEDS_ACCOUNT",
        description="Wave account for USD proceeds when TAO is transferred off-chain"
    )
    blockchain_fee_account: str = Field(
        "Blockchain Fees",
        alias="WAVE_BLOCKCHAIN_FEE_ACCOUNT",
        description="Wave account for on-chain transaction fees (paid in ALPHA or TAO)"
    )
    
    # Gain/Loss accounts
    short_term_gain_account: str = Field(
        "Short-term Capital Gains",
        alias="WAVE_SHORT_TERM_GAIN_ACCOUNT",
        description="Wave account for short-term gains"
    )
    short_term_loss_account: str = Field(
        "Short-term Capital Gains",
        alias="WAVE_SHORT_TERM_LOSS_ACCOUNT",
        description="Wave account for short-term losses"
    )
    long_term_gain_account: str = Field(
        "Long-term Capital Gains",
        alias="WAVE_LONG_TERM_GAIN_ACCOUNT",
        description="Wave account for long-term gains"
    )
    long_term_loss_account: str = Field(
        "Long-term Capital Gains",
        alias="WAVE_LONG_TERM_LOSS_ACCOUNT",
        description="Wave account for long-term losses"
    )


class TaoStatsSettings(BaseSettings):
    """TaoStats API configuration."""
    
    api_key: str = Field(None, alias="TAOSTATS_API_KEY", description="TaoStats API key")
    base_url: str = Field(
        "https://api.taostats.io/api",
        alias="TAOSTATS_BASE_URL",
        description="TaoStats API base URL"
    )
    rate_limit_seconds: float = Field(
        1.0,
        alias="TAOSTATS_RATE_LIMIT_SECONDS",
        description="Minimum seconds between API requests (default: 1.0 for 60 req/min)"
    )


class CoinMarketCapSettings(BaseSettings):
    """CoinMarketCap API configuration (optional fallback)."""
    
    cmc_api_key: Optional[str] = Field(None, alias="COINMARKETCAP_API_KEY", description="CoinMarketCap API key")