from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings

class TrackerSettings(BaseSettings):


    brokerage_ss58: str = Field(..., alias="BROKER_SS58", description="")
    validator_ss58: str = Field(..., alias="VALIDATOR_SS58", description="")
    wallet_ss58: str = Field(..., alias="WALLET_SS58", description="")
    smart_contract_ss58: str = Field(..., alias="SMART_CONTRACT_SS58", description="")
    tracker_sheet_id: str = Field(..., alias="TRACKER_SHEET_ID", description="")
    tracker_google_credentials: str = Field(..., alias="TRACKER_GOOGLE_CREDENTIALS", description="")

    # 493.15
    fixed_payroll_usd: float = Field(..., alias="FIXED_PAYROLL_USD", description="")
    tax_percentage: float = Field(0.25, alias="TAX_PERCENTAGE", description="")

    subnet_id: int = Field(64, alias="SUBNET_ID", description="")

class TaoStatsSettings(BaseSettings):
    api_key: str = Field(None,  alias="TAOSTATS_API_KEY", description="")
    base_url: str = Field("https://api.taostats.io/api", alias="TAOSTATS_BASE_URL", description="")

class CoinMarketCapSettings(BaseSettings):
    cmc_api_key: str = Field(..., alias="COINMARKETCAP_API_KEY", description="")    