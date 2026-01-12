from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
import json


# TaoStats API Response Models

@dataclass
class TaoStatsAddress:
    """Represents an address in TaoStats API responses."""
    ss58: str
    hex: str


@dataclass
class TaoStatsStakeBalance:
    """Represents a stake balance history entry from TaoStats API."""
    block_number: int
    timestamp: str
    hotkey_name: str
    hotkey: TaoStatsAddress
    coldkey: TaoStatsAddress
    netuid: int
    balance: str  # RAO as string
    balance_as_tao: str  # RAO as string
    
    @property
    def timestamp_unix(self) -> int:
        """Convert ISO timestamp to Unix timestamp."""
        return int(datetime.fromisoformat(self.timestamp.replace('Z', '+00:00')).timestamp())
    
    @property
    def balance_rao(self) -> int:
        """Balance in RAO as integer."""
        return int(self.balance)
    
    @property
    def balance_tao(self) -> float:
        """Balance in TAO (converted from RAO)."""
        return int(self.balance) / 1e9
    
    @property
    def balance_as_tao_rao(self) -> int:
        """Balance as TAO equivalent in RAO as integer."""
        return int(self.balance_as_tao)
    
    @property
    def balance_as_tao_float(self) -> float:
        """Balance as TAO equivalent (converted from RAO)."""
        return int(self.balance_as_tao) / 1e9


@dataclass
class TaoStatsDelegation:
    """Represents a delegation event from TaoStats API."""
    block_number: int
    timestamp: str
    action: str
    nominator: TaoStatsAddress
    delegate: TaoStatsAddress
    netuid: int
    amount: int  # RAO
    alpha: int  # RAO
    usd: float
    alpha_price_in_usd: Optional[float]
    alpha_price_in_tao: Optional[float]
    slippage: Optional[float]
    extrinsic_id: str
    is_transfer: Optional[bool]
    transfer_address: Optional[TaoStatsAddress]
    fee: Optional[int]  # RAO
    
    def __post_init__(self):
        """Convert optional numeric fields to proper types after initialization."""
        # Convert alpha_price_in_usd to float if not None
        if self.alpha_price_in_usd is not None and not isinstance(self.alpha_price_in_usd, float):
            self.alpha_price_in_usd = float(self.alpha_price_in_usd)
        
        # Convert alpha_price_in_tao to float if not None
        if self.alpha_price_in_tao is not None and not isinstance(self.alpha_price_in_tao, float):
            self.alpha_price_in_tao = float(self.alpha_price_in_tao)
        
        # Convert slippage to float if not None
        if self.slippage is not None and not isinstance(self.slippage, float):
            self.slippage = float(self.slippage)
        
        # Convert fee to int if not None
        if self.fee is not None and not isinstance(self.fee, int):
            self.fee = int(self.fee)
    
    @property
    def timestamp_unix(self) -> int:
        """Convert ISO timestamp to Unix timestamp."""
        return int(datetime.fromisoformat(self.timestamp.replace('Z', '+00:00')).timestamp())
    
    @property
    def rao(self) -> int:
        """Amount in RAO as integer."""
        return int(self.amount)
    
    @property
    def tao(self) -> float:
        """Amount in TAO (converted from RAO)."""
        return int(self.amount) / 1e9
    
    @property
    def alpha_float(self) -> float:
        """Alpha (converted from RAO)."""
        return int(self.alpha) / 1e9
    
    @property
    def fee_tao(self) -> float:
        """Fee in TAO (converted from RAO)."""
        return int(self.fee) / 1e9 if self.fee else 0.0


@dataclass
class TaoStatsTransfer:
    """Represents a transfer from TaoStats API."""
    block_number: int
    timestamp: str
    transaction_hash: str
    extrinsic_id: str
    amount: str  # RAO as string
    fee: Optional[str]  # RAO as string
    from_address: TaoStatsAddress  # Note: API uses 'from' key
    to_address: TaoStatsAddress  # Note: API uses 'to' key
    
    @property
    def timestamp_unix(self) -> int:
        """Convert ISO timestamp to Unix timestamp."""
        return int(datetime.fromisoformat(self.timestamp.replace('Z', '+00:00')).timestamp())
    
    @property
    def amount_rao(self) -> int:
        """Amount in RAO as integer."""
        return int(self.amount)
    
    @property
    def amount_tao(self) -> float:
        """Amount in TAO (converted from RAO)."""
        return int(self.amount) / 1e9
    
    @property
    def fee_rao(self) -> int:
        """Fee in RAO as integer."""
        return int(self.fee) if self.fee else 0
    
    @property
    def fee_tao(self) -> float:
        """Fee in TAO (converted from RAO)."""
        return int(self.fee) / 1e9 if self.fee else 0.0


# Business Logic Models

class SourceType(Enum):
    """Income source type for ALPHA lots."""
    CONTRACT = "Contract"
    STAKING = "Staking"
    MINING = "Mining"


class CostBasisMethod(Enum):
    """Cost basis calculation method for lot consumption."""
    FIFO = "FIFO"  # First In First Out
    HIFO = "HIFO"  # Highest In First Out


class LotStatus(Enum):
    """Status of a lot."""
    OPEN = "Open"
    PARTIAL = "Partial"
    CLOSED = "Closed"


class GainType(Enum):
    """Capital gain type based on holding period."""
    SHORT_TERM = "Short-term"
    LONG_TERM = "Long-term"


@dataclass
class AlphaLot:
    """Represents an ALPHA income lot for FIFO tracking."""
    lot_id: str
    timestamp: int
    block_number: int
    source_type: SourceType
    alpha_quantity: float  # Original amount
    alpha_remaining: float  # Remaining amount after partial consumption
    usd_fmv: float  # Total USD fair market value at receipt
    usd_per_alpha: float  # USD price per ALPHA at receipt
    tao_equivalent: float  # TAO equivalent at receipt
    extrinsic_id: Optional[str] = None
    transfer_address: Optional[str] = None
    status: LotStatus = LotStatus.OPEN
    notes: str = ""
    
    @property
    def date(self) -> str:
        return datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    @property
    def long_term_date(self) -> str:
        """Date when lot becomes eligible for long-term capital gains (1 year)."""
        return datetime.fromtimestamp(self.timestamp + 365 * 24 * 60 * 60).strftime('%Y-%m-%d')
    
    @property
    def cost_basis_remaining(self) -> float:
        """Pro-rata cost basis for remaining ALPHA."""
        if self.alpha_quantity == 0:
            return 0
        return (self.alpha_remaining / self.alpha_quantity) * self.usd_fmv
    
    def to_sheet_row(self) -> List[Any]:
        """Convert to Google Sheets row."""
        return [
            self.lot_id,
            self.date,
            self.timestamp,
            self.block_number,
            self.source_type.value,
            self.transfer_address or "",
            self.extrinsic_id or "",
            self.alpha_quantity,
            self.alpha_remaining,
            self.usd_fmv,
            self.usd_per_alpha,
            self.tao_equivalent,
            self.long_term_date,
            self.status.value,
            self.notes
        ]
    
    @classmethod
    def sheet_headers(cls) -> List[str]:
        return [
            "Lot ID", "Date", "Timestamp", "Block", "Source Type", 
            "Transfer Address", "Extrinsic ID", "Alpha Quantity", 
            "Alpha Remaining", "USD FMV", "USD/Alpha", "TAO Equivalent",
            "Long Term Date", "Status", "Notes"
        ]


@dataclass
class LotConsumption:
    """Records how much of a lot was consumed in a disposal."""
    lot_id: str
    alpha_consumed: float
    cost_basis_consumed: float
    acquisition_timestamp: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "lot_id": self.lot_id,
            "alpha": self.alpha_consumed,
            "basis": self.cost_basis_consumed,
            "acquired": self.acquisition_timestamp
        }


@dataclass
class TaoLot:
    """Represents a TAO lot created from ALPHA disposal."""
    lot_id: str
    timestamp: int
    block_number: int
    tao_quantity: float
    tao_remaining: float
    usd_basis: float  # Cost basis (proceeds from ALPHA disposal)
    usd_per_tao: float
    source_sale_id: str  # Link to the ALPHA disposal that created this
    extrinsic_id: Optional[str] = None
    status: LotStatus = LotStatus.OPEN
    notes: str = ""
    
    @property
    def date(self) -> str:
        return datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    @property
    def basis_remaining(self) -> float:
        """Pro-rata basis for remaining TAO."""
        if self.tao_quantity == 0:
            return 0
        return (self.tao_remaining / self.tao_quantity) * self.usd_basis
    
    def to_sheet_row(self) -> List[Any]:
        return [
            self.lot_id,
            self.date,
            self.timestamp,
            self.block_number,
            self.tao_quantity,
            self.tao_remaining,
            self.usd_basis,
            self.usd_per_tao,
            self.source_sale_id,
            self.extrinsic_id or "",
            self.status.value,
            self.notes
        ]
    
    @classmethod
    def sheet_headers(cls) -> List[str]:
        return [
            "TAO Lot ID", "Date", "Timestamp", "Block", "TAO Quantity",
            "TAO Remaining", "USD Basis", "USD/TAO", "Source Sale ID",
            "Extrinsic ID", "Status", "Notes"
        ]


@dataclass 
class AlphaSale:
    """Represents an ALPHA → TAO disposal event."""
    sale_id: str
    timestamp: int
    block_number: int
    alpha_disposed: float
    tao_received: float
    tao_price_usd: float
    usd_proceeds: float  # TAO received × TAO price
    cost_basis: float  # Sum of consumed lot bases
    realized_gain_loss: float
    gain_type: GainType
    consumed_lots: List[LotConsumption]
    created_tao_lot_id: str  # Link to TAO lot created
    tao_expected: float = 0.0
    tao_slippage: float = 0.0
    slippage_usd: float = 0.0
    slippage_ratio: float = 0.0
    network_fee_tao: float = 0.0
    network_fee_usd: float = 0.0
    extrinsic_id: Optional[str] = None
    notes: str = ""
    
    @property
    def date(self) -> str:
        return datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    def consumed_lots_json(self) -> str:
        """JSON representation of consumed lots for sheet storage."""
        return json.dumps([c.to_dict() for c in self.consumed_lots])
    
    def consumed_lots_summary(self) -> str:
        """Human-readable summary of consumed lots."""
        return ", ".join([f"{c.lot_id}:{c.alpha_consumed:.4f}" for c in self.consumed_lots])
    
    def to_sheet_row(self) -> List[Any]:
        return [
            self.sale_id,
            self.date,
            self.timestamp,
            self.block_number,
            self.alpha_disposed,
            self.tao_received,
            self.tao_price_usd,
            self.usd_proceeds,
            self.cost_basis,
            self.realized_gain_loss,
            self.gain_type.value,
            self.tao_expected,
            self.tao_slippage,
            self.slippage_usd,
            self.slippage_ratio,
            self.network_fee_tao,
            self.network_fee_usd,
            self.consumed_lots_summary(),
            self.created_tao_lot_id,
            self.extrinsic_id or "",
            self.notes
        ]
    
    @classmethod
    def sheet_headers(cls) -> List[str]:
        return [
            "Sale ID", "Date", "Timestamp", "Block", "Alpha Disposed",
            "TAO Received", "TAO Price USD", "USD Proceeds", "Cost Basis",
            "Realized Gain/Loss", "Gain Type", "TAO Expected", "TAO Slippage",
            "Slippage USD", "Slippage Ratio",
            "Network Fee (TAO)", "Network Fee (USD)",
            "Consumed Lots", "Created TAO Lot ID", "Extrinsic ID", "Notes"
        ]


@dataclass
class TaoTransfer:
    """Represents a TAO → Kraken transfer event."""
    transfer_id: str
    timestamp: int
    block_number: int
    tao_amount: float
    tao_price_usd: float
    usd_proceeds: float
    cost_basis: float  # From consumed TAO lots
    realized_gain_loss: float
    gain_type: GainType
    consumed_tao_lots: List[LotConsumption]
    transaction_hash: Optional[str] = None
    extrinsic_id: Optional[str] = None
    notes: str = ""
    total_outflow_tao: float = 0.0
    fee_tao: float = 0.0
    fee_cost_basis_usd: float = 0.0
    
    @property
    def date(self) -> str:
        return datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    def consumed_lots_summary(self) -> str:
        return ", ".join([f"{c.lot_id}:{c.alpha_consumed:.4f}" for c in self.consumed_tao_lots])
    
    def to_sheet_row(self) -> List[Any]:
        return [
            self.transfer_id,
            self.date,
            self.timestamp,
            self.block_number,
            self.tao_amount,
            self.tao_price_usd,
            self.usd_proceeds,
            self.cost_basis,
            self.realized_gain_loss,
            self.gain_type.value,
            self.consumed_lots_summary(),
            self.transaction_hash or "",
            self.extrinsic_id or "",
            self.notes,
            self.total_outflow_tao,
            self.fee_tao,
            self.fee_cost_basis_usd
        ]
    
    @classmethod
    def sheet_headers(cls) -> List[str]:
        return [
            "Transfer ID", "Date", "Timestamp", "Block", "TAO Amount",
            "TAO Price USD", "USD Proceeds", "Cost Basis", "Realized Gain/Loss",
            "Gain Type", "Consumed TAO Lots", "Transaction Hash", 
            "Extrinsic ID", "Notes", "Total Outflow TAO", "Fee TAO",
            "Fee Cost Basis USD"
        ]


@dataclass
class Expense:
    """Represents an ALPHA → TAO payment/expense event (transferred to another entity)."""
    expense_id: str
    timestamp: int
    block_number: int
    alpha_disposed: float
    tao_received: float
    tao_price_usd: float
    usd_proceeds: float  # TAO received × TAO price
    cost_basis: float  # Sum of consumed lot bases
    realized_gain_loss: float
    gain_type: GainType
    consumed_lots: List[LotConsumption]
    created_tao_lot_id: str  # Link to TAO lot created
    transfer_address: str  # Address the TAO was transferred to
    category: str = ""  # User fills this in (e.g., "Payment to Entity", "Distribution", etc.)
    tao_expected: float = 0.0
    tao_slippage: float = 0.0
    slippage_usd: float = 0.0
    slippage_ratio: float = 0.0
    network_fee_tao: float = 0.0
    network_fee_usd: float = 0.0
    extrinsic_id: Optional[str] = None
    notes: str = ""
    
    @property
    def date(self) -> str:
        return datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    def consumed_lots_json(self) -> str:
        """JSON representation of consumed lots for sheet storage."""
        return json.dumps([c.to_dict() for c in self.consumed_lots])
    
    def consumed_lots_summary(self) -> str:
        """Human-readable summary of consumed lots."""
        return ", ".join([f"{c.lot_id}:{c.alpha_consumed:.4f}" for c in self.consumed_lots])
    
    def to_sheet_row(self) -> List[Any]:
        return [
            self.expense_id,
            self.date,
            self.timestamp,
            self.block_number,
            self.transfer_address,
            self.category,
            self.alpha_disposed,
            self.tao_received,
            self.tao_price_usd,
            self.usd_proceeds,
            self.cost_basis,
            self.realized_gain_loss,
            self.gain_type.value,
            self.tao_expected,
            self.tao_slippage,
            self.slippage_usd,
            self.slippage_ratio,
            self.network_fee_tao,
            self.network_fee_usd,
            self.consumed_lots_summary(),
            self.created_tao_lot_id,
            self.extrinsic_id or "",
            self.notes
        ]
    
    @classmethod
    def sheet_headers(cls) -> List[str]:
        return [
            "Expense ID", "Date", "Timestamp", "Block", "Transfer Address", "Category",
            "Alpha Disposed", "TAO Received", "TAO Price USD", "USD Proceeds", 
            "Cost Basis", "Realized Gain/Loss", "Gain Type", "TAO Expected", 
            "TAO Slippage", "Slippage USD", "Slippage Ratio",
            "Network Fee (TAO)", "Network Fee (USD)",
            "Consumed Lots", "Created TAO Lot ID", "Extrinsic ID", "Notes"
        ]


@dataclass
class JournalEntry:
    """Represents a Wave journal entry row."""
    month: str  # YYYY-MM
    entry_type: str  # Income, Sale, Transfer
    account: str
    debit: float
    credit: float
    description: str
    
    def to_sheet_row(self) -> List[Any]:
        return [
            self.month,
            self.entry_type,
            self.account,
            self.debit if self.debit > 0 else "",
            self.credit if self.credit > 0 else "",
            self.description
        ]
    
    @classmethod
    def sheet_headers(cls) -> List[str]:
        return ["Month", "Entry Type", "Account", "Debit", "Credit", "Description"]