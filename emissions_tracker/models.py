from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any, Callable
import json


# TaoStats API Response Models

@dataclass
class TaoStatsAddress:
    """Represents an address in TaoStats API responses."""
    ss58: str
    hex: str

@dataclass
class DailyBalance:
    """Represents the last balance snapshot for a given day."""
    day: str  # Date in 'YYYY-MM-DD' format
    balance: 'TaoStatsStakeBalance'  # The balance snapshot for this day


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
    def day(self) -> str:
        """Extract day in 'YYYY-MM-DD' format from timestamp."""
        dt = datetime.fromisoformat(self.timestamp.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d')

    @property
    def balance_as_alpha_rao(self) -> int:
        """Balance in RAO as integer."""
        return int(self.balance)
    
    @property
    def balance_as_alpha_float(self) -> float:
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

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'TaoStatsStakeBalance':
        """Create a TaoStatsStakeBalance instance from JSON data."""
        return cls(
            block_number=data['block_number'],
            timestamp=data['timestamp'],
            hotkey_name=data['hotkey_name'],
            hotkey=TaoStatsAddress(**data['hotkey']),
            coldkey=TaoStatsAddress(**data['coldkey']),
            netuid=int(data['netuid']),
            balance=data['balance'],
            balance_as_tao=data['balance_as_tao']
        )

@dataclass
class TaoStatsDelegation:
    """Represents a delegation event from TaoStats API."""
    block_number: int
    timestamp: str
    action: str
    nominator: TaoStatsAddress
    delegate: TaoStatsAddress
    netuid: int
    amount: int  # TAO RAO
    alpha: int  # Alpha RAO
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
    def day(self) -> str:
        """Extract day in 'YYYY-MM-DD' format from timestamp."""
        dt = datetime.fromisoformat(self.timestamp.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d')

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

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'TaoStatsDelegation':
        """Create a TaoStatsDelegation instance from JSON data."""
        return cls(
            block_number=int(data['block_number']),
            timestamp=data['timestamp'],
            action=data['action'],
            nominator=TaoStatsAddress(**data['nominator']),
            delegate=TaoStatsAddress(**data['delegate']),
            netuid=int(data['netuid']),
            amount=int(data['amount']),
            alpha=int(data['alpha']),
            usd=float(data['usd']),
            alpha_price_in_usd=data.get('alpha_price_in_usd'),
            alpha_price_in_tao=data.get('alpha_price_in_tao'),
            slippage=data.get('slippage'),
            extrinsic_id=data['extrinsic_id'],
            is_transfer=data.get('is_transfer'),
            transfer_address=TaoStatsAddress(**data['transfer_address']) if data.get('transfer_address') else None,
            fee=data.get('fee')
        )


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

    @property
    def total_outflow_rao(self) -> int:
        """Total outflow (amount + fee) in RAO as integer."""
        return self.amount_rao + self.fee_rao

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'TaoStatsTransfer':
        """Create a TaoStatsTransfer instance from JSON data."""
        return cls(
            block_number=int(data['block_number']),
            timestamp=data['timestamp'],
            transaction_hash=data['transaction_hash'],
            extrinsic_id=data['extrinsic_id'],
            amount=data['amount'],
            fee=data.get('fee'),
            from_address=TaoStatsAddress(**data['from']),
            to_address=TaoStatsAddress(**data['to'])
        )


@dataclass
class TaoStatsAccountHistory:
    """Represents an account history snapshot from TaoStats API."""
    address: TaoStatsAddress
    network: str
    block_number: int
    timestamp: str
    rank: int
    balance_free: str  # RAO as string
    balance_reserved: str  # RAO as string
    balance_staked: str  # RAO as string
    balance_staked_alpha_as_tao: str  # RAO as string
    balance_staked_root: str  # RAO as string
    root_claim_type: str
    balance_liquidity: str  # RAO as string
    balance_total: str  # RAO as string
    created_on_date: str
    created_on_network: str
    coldkey_swap: Optional[str]
    
    @property
    def timestamp_unix(self) -> int:
        """Convert ISO timestamp to Unix timestamp."""
        return int(datetime.fromisoformat(self.timestamp.replace('Z', '+00:00')).timestamp())
    
    @property
    def day(self) -> str:
        """Extract day in 'YYYY-MM-DD' format from timestamp."""
        dt = datetime.fromisoformat(self.timestamp.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d')
    
    @property
    def balance_free_rao(self) -> int:
        """Free balance in RAO as integer."""
        return int(self.balance_free)
    
    @property
    def balance_free_tao(self) -> float:
        """Free balance in TAO (converted from RAO)."""
        return int(self.balance_free) / 1e9
    
    @property
    def balance_staked_rao(self) -> int:
        """Staked balance in RAO as integer."""
        return int(self.balance_staked)
    
    @property
    def balance_staked_tao(self) -> float:
        """Staked balance in TAO (converted from RAO)."""
        return int(self.balance_staked) / 1e9
    
    @property
    def balance_total_rao(self) -> int:
        """Total balance in RAO as integer."""
        return int(self.balance_total)
    
    @property
    def balance_total_tao(self) -> float:
        """Total balance in TAO (converted from RAO)."""
        return int(self.balance_total) / 1e9
    
    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'TaoStatsAccountHistory':
        """Create a TaoStatsAccountHistory instance from JSON data."""
        return cls(
            address=TaoStatsAddress(**data['address']),
            network=data['network'],
            block_number=int(data['block_number']),
            timestamp=data['timestamp'],
            rank=int(data['rank']),
            balance_free=data['balance_free'],
            balance_reserved=data['balance_reserved'],
            balance_staked=data['balance_staked'],
            balance_staked_alpha_as_tao=data['balance_staked_alpha_as_tao'],
            balance_staked_root=data['balance_staked_root'],
            root_claim_type=data['root_claim_type'],
            balance_liquidity=data['balance_liquidity'],
            balance_total=data['balance_total'],
            created_on_date=data['created_on_date'],
            created_on_network=data['created_on_network'],
            coldkey_swap=data.get('coldkey_swap')
        )


# Business Logic Models

class SourceType(Enum):
    """Income source type for ALPHA lots and TAO lots."""
    CONTRACT = "Contract"
    STAKING = "Staking"
    MINING = "Mining"
    SALE = "Sale"       # TAO lot from ALPHA sale
    DEPOSIT = "Deposit" # TAO lot from incoming TAO transfer


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


class DisposalType(Enum):
    """Type of disposal event for chronological processing."""
    SALE = "sale"
    EXPENSE = "expense"
    TRANSFER = "transfer"


@dataclass
class DisposalEvent:
    """Wrapper to sort different disposal types chronologically.
    
    Used to process sales, expenses, and transfers in timestamp order
    rather than by type, ensuring correct lot consumption.
    """
    timestamp: int
    disposal_type: DisposalType
    event: Any  # TaoStatsDelegation or TaoStatsTransfer
    process: Callable[[], Any]  # Callable that processes this event and returns the result


@dataclass
class AlphaLot:
    """Represents an ALPHA income lot for FIFO tracking.
    
    Uses RAO (integer) for all ALPHA amounts internally to avoid floating-point precision errors.
    1 ALPHA = 1e9 RAO (1,000,000,000 RAO).
    """
    lot_id: str
    timestamp: int
    block_number: int
    source_type: SourceType
    alpha_rao: int  # Original ALPHA amount in RAO (integer)
    alpha_rao_remaining: int  # Remaining ALPHA in RAO after consumption (integer)
    usd_fmv: float  # Total USD fair market value at receipt
    usd_per_alpha: float  # USD price per ALPHA at receipt
    tao_equivalent: float  # TAO equivalent at receipt
    extrinsic_id: Optional[str] = None
    transfer_address: Optional[str] = None
    status: LotStatus = LotStatus.OPEN
    notes: str = ""
    
    @property
    def alpha(self) -> float:
        """Original ALPHA amount (converted from RAO)."""
        return self.alpha_rao / 1e9
    
    @property
    def alpha_remaining(self) -> float:
        """Remaining ALPHA amount (converted from RAO)."""
        return self.alpha_rao_remaining / 1e9
    
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
        if self.alpha_rao == 0:
            return 0
        return (self.alpha_rao_remaining / self.alpha_rao) * self.usd_fmv
    
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
            self.alpha_rao,  # RAO for calculations (integer)
            self.alpha_rao_remaining,  # RAO remaining for calculations (integer)
            self.alpha,  # Display column (float)
            self.alpha_remaining,  # Display column (float)
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
            "Transfer Address", "Extrinsic ID", "Alpha RAO",
            "Alpha RAO Remaining", "Alpha Quantity", "Alpha Remaining",
            "USD FMV", "USD/Alpha", "TAO Equivalent",
            "Long Term Date", "Status", "Notes"
        ]


@dataclass
class AlphaLotRow(AlphaLot):
    """AlphaLot with sheet row number attached for batch updates."""
    row: int = 0  # Sheet row number (1-indexed, where 1 is header)


@dataclass
class AlphaLotConsumption:
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
class TaoLotConsumption:
    """Records how much of a lot was consumed in a disposal."""
    lot_id: str
    tao_consumed: float
    cost_basis_consumed: float
    acquisition_timestamp: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "lot_id": self.lot_id,
            "tao": self.tao_consumed,
            "basis": self.cost_basis_consumed,
            "acquired": self.acquisition_timestamp
        }


@dataclass
class TaoLot:
    """Represents a TAO lot created from ALPHA disposal.
    
    Uses RAO (integer) for all TAO amounts internally to avoid floating-point precision errors.
    1 TAO = 1e9 RAO (1,000,000,000 RAO).
    """
    lot_id: str
    timestamp: int
    block_number: int
    rao: int  # Original TAO amount in RAO (integer)
    rao_remaining: int  # Remaining TAO in RAO after consumption (integer)
    usd_basis: float  # Cost basis (proceeds from ALPHA disposal)
    usd_per_tao: float
    source_sale_id: str  # Link to the ALPHA disposal that created this
    extrinsic_id: Optional[str] = None
    status: LotStatus = LotStatus.OPEN
    notes: str = ""
    
    @property
    def tao(self) -> float:
        """Original TAO amount (converted from RAO)."""
        return self.rao / 1e9
    
    @property
    def tao_remaining(self) -> float:
        """Remaining TAO amount (converted from RAO)."""
        return self.rao_remaining / 1e9
    
    @property
    def date(self) -> str:
        return datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    @property
    def basis_remaining(self) -> float:
        """Pro-rata basis for remaining TAO."""
        if self.rao == 0:
            return 0
        return (self.rao_remaining / self.rao) * self.usd_basis
    
    def to_sheet_row(self) -> List[Any]:
        return [
            self.lot_id,
            self.date,
            self.timestamp,
            self.block_number,
            self.rao,  # RAO for calculations (integer)
            self.rao_remaining,  # RAO remaining for calculations (integer)
            self.tao,  # Display column (float)
            self.tao_remaining,  # Display column (float)
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
            "TAO Lot ID", "Date", "Timestamp", "Block", "TAO RAO",
            "TAO RAO Remaining", "TAO Quantity", "TAO Remaining",
            "USD Basis", "USD/TAO", "Source Sale ID",
            "Extrinsic ID", "Status", "Notes"
        ]


@dataclass
class TaoLotRow(TaoLot):
    """TaoLot with sheet row number attached for batch updates."""
    row: int = 0  # Sheet row number (1-indexed, where 1 is header)


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
    consumed_lots: List[AlphaLotConsumption]
    created_tao_lot_id: str  # Link to TAO lot created
    tao_slippage: float = 0.0
    slippage_usd: float = 0.0
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
            self.tao_slippage,
            self.slippage_usd,
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
    consumed_tao_lots: List[TaoLotConsumption]
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
        return ", ".join([f"{c.lot_id}:{c.tao_consumed:.4f}" for c in self.consumed_tao_lots])
    
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
    consumed_lots: List[AlphaLotConsumption]
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
class TaoDeposit:
    """Represents an incoming TAO transfer (deposit) that creates a TAO lot."""
    deposit_id: str
    timestamp: int
    block_number: int
    from_address: str
    tao_amount: float  # TAO received
    tao_amount_rao: int  # TAO in RAO for precision
    tao_price_usd: float
    usd_fmv: float  # Fair market value at time of receipt
    created_tao_lot_id: str  # Link to TAO lot created
    category: str = ""  # User fills this in (e.g., "Gift", "Payment Received", "Refund", etc.)
    extrinsic_id: Optional[str] = None
    notes: str = ""
    
    @property
    def date(self) -> str:
        return datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    def to_sheet_row(self) -> List[Any]:
        return [
            self.deposit_id,
            self.date,
            self.timestamp,
            self.block_number,
            self.from_address,
            self.category,
            self.tao_amount,
            self.tao_amount_rao,
            self.tao_price_usd,
            self.usd_fmv,
            self.created_tao_lot_id,
            self.extrinsic_id or "",
            self.notes
        ]
    
    @classmethod
    def sheet_headers(cls) -> List[str]:
        return [
            "Deposit ID", "Date", "Timestamp", "Block", "From Address", "Category",
            "TAO Amount", "TAO RAO", "TAO Price USD", "USD FMV",
            "Created TAO Lot ID", "Extrinsic ID", "Notes"
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