from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
import json


class SourceType(Enum):
    """Income source type for ALPHA lots."""
    CONTRACT = "Contract"
    STAKING = "Staking"


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
            "Realized Gain/Loss", "Gain Type", "Consumed Lots", 
            "Created TAO Lot ID", "Extrinsic ID", "Notes"
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
            self.notes
        ]
    
    @classmethod
    def sheet_headers(cls) -> List[str]:
        return [
            "Transfer ID", "Date", "Timestamp", "Block", "TAO Amount",
            "TAO Price USD", "USD Proceeds", "Cost Basis", "Realized Gain/Loss",
            "Gain Type", "Consumed TAO Lots", "Transaction Hash", 
            "Extrinsic ID", "Notes"
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