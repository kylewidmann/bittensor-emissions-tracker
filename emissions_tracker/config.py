from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings

class TrackerSettings(BaseSettings):


    brokerage_ss58: str = Field(..., alias="BROKER_SS58", description="")
    contract_ss58: str = Field(..., alias="CONTRACT_SS58", description="")
    wallet_ss58: str = Field(..., alias="WALLET_SS58", description="")
    tracker_sheet_id: str = Field(..., alias="TRACKER_SHEET_ID", description="")
    tracker_google_credentials: str = Field(..., alias="TRACKER_GOOGLE_CREDENTIALS", description="")

    # Client API Keys
    cmc_api_key: str = Field(..., alias="COINMARKETCAP_API_KEY", description="")

    # 493.15
    daily_payroll: float = Field(..., alias="DAILY_PAYROLL", description="")

    subnet_id: int = Field(64, alias="SUBNET_ID", description="")
