"""
Shared utility functions for unit tests.

Contains common logic for processing balance history and delegation events
that is used across multiple test modules.
"""
from typing import List, Dict

from emissions_tracker.models import AlphaLot, AlphaSale, CostBasisMethod, Expense, GainType, AlphaLotConsumption, LotStatus, SourceType, TaoLot, TaoLotConsumption, TaoStatsDelegation, TaoStatsStakeBalance, TaoStatsTransfer, TaoTransfer

SECONDS_PER_DAY = 86400


def compute_staking_emissions_from_balances(
    daily_stake_balances: Dict[str, TaoStatsStakeBalance],
    daily_stake_events: Dict[str, List[TaoStatsDelegation]],
    start_ts: int,
    end_ts: int,
    get_tao_price_at_timestamp,
    get_lot_id,
) -> List[AlphaLot]:
    """Generic function to compute staking emissions from balance history and events.
    
    Works for both contract and mining trackers. For mining, just pass empty daily_stake_events.
    
    Args:
        daily_stake_balances: Dict of daily stake balance records keyed by day string
        daily_stake_events: Dict of daily stake events keyed by day string (empty for mining)
        start_ts: Start timestamp (unix seconds)
        end_ts: End timestamp (unix seconds)
        get_tao_price_at_timestamp: Function that takes timestamp and returns TAO price
        get_lot_id: Function that returns next lot ID
        
    Returns:
        List of AlphaLot objects representing emissions
    """
    # Extend window back by 1 day to get previous day's balance (matches tracker behavior)
    extended_start = start_ts - SECONDS_PER_DAY
    
    # Filter balance history to date range
    daily_balances = filter_balances_by_date_range(daily_stake_balances, extended_start, end_ts)
    
    emission_lots = []
    for i in range(1, len(daily_balances)):
        prev_day = daily_balances[i - 1]
        curr_day = daily_balances[i]

        # Get all events for current day (empty list for mining)
        day_events = daily_stake_events.get(curr_day.day, [])
        
        alpha_inflow_rao = sum(e.alpha for e in day_events if e.action == 'DELEGATE')
        alpha_outflow_rao = sum(e.alpha for e in day_events if e.action == 'UNDELEGATE')
        
        # Calculate alpha emissions in RAO
        # Balance change from end of previous day to end of current day (in RAO)
        balance_change_alpha_rao = curr_day.balance_as_alpha_rao - prev_day.balance_as_alpha_rao
        
        alpha_price_tao_rao = curr_day.balance_as_tao_rao / curr_day.balance_as_alpha_rao
        
        emissions_alpha_rao = balance_change_alpha_rao - alpha_inflow_rao + alpha_outflow_rao
        
        # Skip if no emissions (or balance decreased)
        if emissions_alpha_rao <= 0:
            continue
        
        # Get TAO price for current day
        timestamp = curr_day.timestamp_unix + SECONDS_PER_DAY - 1  # End of day timestamp
        tao_price = get_tao_price_at_timestamp(timestamp)
        
        emissions_tao = (emissions_alpha_rao * alpha_price_tao_rao) / 1e9  # Convert new Alpha RAO to TAO
        emissions_alpha = emissions_alpha_rao / 1e9  # Convert to ALPHA
        usd_fmv = emissions_tao * tao_price
        usd_per_alpha = usd_fmv / emissions_alpha if emissions_alpha > 0 else 0

        emission_lots.append(AlphaLot(
            lot_id=get_lot_id(),
            timestamp=curr_day.timestamp_unix,
            block_number=curr_day.block_number,
            alpha_rao=emissions_alpha_rao,
            alpha_rao_remaining=emissions_alpha_rao,
            tao_equivalent=emissions_tao,
            usd_per_alpha=usd_per_alpha,
            usd_fmv=usd_fmv,
            source_type=SourceType.STAKING
        ))
    
    return emission_lots



def filter_balances_by_date_range(
    daily_stake_balances: Dict[str, TaoStatsStakeBalance],
    start_ts: int,
    end_ts: int
) -> List[TaoStatsStakeBalance]:
    """Filter balance records to a date range.
    
    Args:
        daily_stake_balances: Dict of daily stake balance records keyed by day string
        start_ts: Start timestamp (unix seconds)
        end_ts: End timestamp (unix seconds)
        
    Returns:
        Filtered list of balance records
    """
    balances = [
        b for b in daily_stake_balances.values()
        if start_ts <= b.timestamp_unix <= end_ts
    ]
    balances.sort(key=lambda x: x.timestamp_unix)
    return balances


def filter_delegation_events_by_date_range(
    event_data: List[TaoStatsDelegation],
    start_ts: int,
    end_ts: int
) -> List[TaoStatsDelegation]:
    """Filter and convert delegation events to simplified format.
    
    Args:
        event_data: List of raw event records
        start_ts: Start timestamp (unix seconds)
        end_ts: End timestamp (unix seconds)
        
    Returns:
        List of events with keys: timestamp, alpha_rao, action
    """
    events = [
        e for e in event_data
        if start_ts <= e.timestamp_unix <= end_ts
    ]
    return sorted(events, key=lambda x: x.timestamp_unix)

def _consume_alpha_lots(
    alpha_lots: list[AlphaLot], 
    amount_rao: int,
    timestamp: int,
    cost_basis_method: CostBasisMethod = CostBasisMethod.HIFO,
) -> tuple[list[AlphaLotConsumption], float]:
    # Consume ALPHA lots FIFO to calculate cost basis
    cost_basis = 0.0
    remaining_need = amount_rao
    
    consumed_lots: list[AlphaLotConsumption] = []
    available_lots = [
        lot for lot in alpha_lots 
        if lot.timestamp <= timestamp
        and lot.alpha_rao_remaining > 0
    ]

    if not available_lots:
        raise ValueError(f"No available ALPHA lots to consume at timestamp {timestamp}")

    # Sort lots based on cost basis method
    if cost_basis_method == CostBasisMethod.FIFO:
        # First In First Out - consume oldest lots first
        available_lots.sort(key=lambda x: x.timestamp)
    elif cost_basis_method == CostBasisMethod.HIFO:
        # Highest In First Out - consume highest cost basis lots first
        available_lots.sort(key=lambda x: x.usd_per_alpha, reverse=True)
    else:
        raise ValueError(f"Invalid cost_basis_method: {cost_basis_method}. Must be CostBasisMethod.FIFO or CostBasisMethod.HIFO")
    

    for lot in available_lots:
        if remaining_need == 0:
            break

        if lot.alpha_rao_remaining == 0:
            continue
        
        to_consume = min(lot.alpha_rao_remaining, remaining_need)
        basis_consumed = (to_consume / lot.alpha_rao) * lot.usd_fmv
        
        cost_basis += basis_consumed
        lot.alpha_rao_remaining -= to_consume
        remaining_need -= to_consume
        
        if lot.alpha_rao_remaining == 0:
            lot.status = LotStatus.CLOSED
        else:
            lot.status = LotStatus.PARTIAL

        consumed_lots.append(AlphaLotConsumption(
            lot_id=lot.lot_id,
            alpha_consumed=to_consume / 1e9,
            cost_basis_consumed=basis_consumed,
            acquisition_timestamp=lot.timestamp,
        ))

    return consumed_lots, cost_basis

def consume_alpha_lots_for_expense(
    expense_id: str,
    alpha_lots: list[AlphaLot], 
    delegation: TaoStatsDelegation,
    cost_basis_method: CostBasisMethod = CostBasisMethod.HIFO,
) -> Expense:
    
    consumed_lots, cost_basis = _consume_alpha_lots(
        alpha_lots,
        delegation.alpha,
        delegation.timestamp_unix,
        cost_basis_method
    )

    # TODO: Add in slippage to this as well
    realized_gain_loss = delegation.usd - cost_basis

    # Determine gain type (short-term if held < 1 year)
    newest_lot_timestamp = min(c.acquisition_timestamp for c in consumed_lots)
    holding_period_days = (delegation.timestamp_unix - newest_lot_timestamp) / (24 * 60 * 60)
    gain_type = GainType.LONG_TERM if holding_period_days >= 365 else GainType.SHORT_TERM

    expense = Expense(
        expense_id=expense_id,
        timestamp=delegation.timestamp_unix,
        block_number=delegation.block_number,
        transfer_address=delegation.transfer_address.ss58 if delegation.transfer_address else "",
        alpha_disposed=delegation.alpha_float,
        tao_received=0.0,  # No TAO received for ALPHA expenses
        tao_price_usd=0.0,
        usd_proceeds=delegation.usd or 0.0,
        cost_basis=cost_basis,
        realized_gain_loss=realized_gain_loss,
        gain_type=gain_type,
        consumed_lots=consumed_lots,
        created_tao_lot_id="",  # No TAO lot created for direct ALPHA expenses
        network_fee_tao=0.0,
        network_fee_usd=0.0,
        extrinsic_id=delegation.extrinsic_id,
        notes=f"Alpha expense to {delegation.transfer_address.ss58[:8]}... at block {delegation.block_number}"
    )

    return expense

def consume_alpha_lots_for_sale(
    sale_id: str,
    tao_lot_id: str,
    alpha_lots: list[AlphaLot], 
    delegation: TaoStatsDelegation,
    associated_transfers: list[TaoStatsTransfer],
    cost_basis_method: CostBasisMethod = CostBasisMethod.HIFO
) -> tuple[AlphaSale, TaoLot]:
    
    tao_price = delegation.usd / (delegation.amount / 1e9)  
    slippage_usd = delegation.slippage * tao_price
    network_fee = sum([t.fee_rao + t.amount_rao for t in associated_transfers])
    fee_usd = (network_fee / 1e9)* tao_price
    
    consumed_lots, cost_basis = _consume_alpha_lots(
        alpha_lots,
        delegation.alpha,
        delegation.timestamp_unix,
        cost_basis_method
    )
    
    # Calculate realized gain/loss
    # usd_proceeds is already net of network fees (based on tao_amount which has fees deducted)
    realized_gain_loss = delegation.usd - cost_basis
    lot_age = delegation.timestamp_unix - max([
        lot.acquisition_timestamp for lot in consumed_lots
    ])
    gain_type = GainType.LONG_TERM if lot_age / 86400 >= 365 else GainType.SHORT_TERM,
    
    alpha_sale = AlphaSale(
        sale_id=sale_id,
        timestamp=delegation.timestamp_unix,
        block_number=delegation.block_number,
        alpha_disposed=delegation.alpha_float,
        tao_received=(delegation.amount - network_fee) / 1e9,
        tao_price_usd=tao_price,
        usd_proceeds=delegation.usd,
        cost_basis=cost_basis,
        realized_gain_loss=realized_gain_loss,
        gain_type=gain_type,
        consumed_lots=consumed_lots,
        created_tao_lot_id=tao_lot_id,
        tao_slippage=delegation.slippage,
        slippage_usd=slippage_usd,
        network_fee_tao=network_fee / 1e9,
        network_fee_usd=fee_usd,
        extrinsic_id=delegation.extrinsic_id,
    )

    # TAO lot is created with NET amount (gross - network fees)
    # This matches the tracker's _create_alpha_sales logic
    tao_received_rao = delegation.amount - network_fee
    
    tao_lot = TaoLot(
        lot_id=tao_lot_id,
        timestamp=delegation.timestamp_unix,
        block_number=delegation.block_number,
        rao=tao_received_rao,
        rao_remaining=tao_received_rao,
        usd_basis=alpha_sale.usd_proceeds,  # Use proceeds as basis (matches tracker)
        usd_per_tao=alpha_sale.tao_price_usd,
        source_sale_id=alpha_sale.sale_id,
        extrinsic_id=alpha_sale.extrinsic_id,
        status=LotStatus.OPEN,
        notes="TAO lot created from sale",
    )

    return alpha_sale, tao_lot

def consume_tao_lots(
    xfer_id: str, 
    tao_lots: list[TaoLot], 
    taostats_transfer: TaoStatsTransfer,
    tao_price: float
) -> TaoTransfer:

    consumed_lots: list[TaoLotConsumption] = []

    available_lots: list[TaoLot] = [
        lot for lot in tao_lots 
        if lot.timestamp <= taostats_transfer.timestamp_unix 
        and lot.rao_remaining > 0
    ]

    # Total outflow includes both transfer amount and fee (both reduce wallet balance)
    total_outflow_rao = taostats_transfer.amount_rao + taostats_transfer.fee_rao
    remaining_to_consume = total_outflow_rao
    total_cost_basis = 0.0
    for lot in available_lots:
        if remaining_to_consume == 0:
            break
        
        consume_amount = min(remaining_to_consume, lot.rao_remaining)
        
        # Calculate pro-rata cost basis
        cost_basis_consumed = (consume_amount / lot.rao) * lot.usd_basis
        
        consumed_lots.append(TaoLotConsumption(
            lot_id=lot.lot_id,
            tao_consumed=consume_amount / 1e9,
            cost_basis_consumed=cost_basis_consumed,
            acquisition_timestamp=lot.timestamp,
        ))
        
        total_cost_basis += cost_basis_consumed
        lot.rao_remaining -= consume_amount
        remaining_to_consume -= consume_amount
    
    newest_lot_timestamp = min(c.acquisition_timestamp for c in consumed_lots)
    holding_period_days = (taostats_transfer.timestamp_unix - newest_lot_timestamp) / (24 * 60 * 60)
    gain_type = GainType.LONG_TERM if holding_period_days >= 365 else GainType.SHORT_TERM

    # Split cost basis proportionally between transfer and fee
    fee_cost_basis = (total_cost_basis * (taostats_transfer.fee_rao / total_outflow_rao)) if total_outflow_rao > 0 else 0.0

    transfer = TaoTransfer(
        transfer_id=xfer_id,
        timestamp=taostats_transfer.timestamp_unix,
        block_number=taostats_transfer.block_number,
        tao_amount=taostats_transfer.amount_tao,
        tao_price_usd=tao_price,
        usd_proceeds=taostats_transfer.amount_tao * tao_price,
        cost_basis=total_cost_basis,
        realized_gain_loss=taostats_transfer.amount_tao * tao_price - total_cost_basis,
        gain_type=gain_type,
        consumed_tao_lots=consumed_lots,
        transaction_hash=taostats_transfer.transaction_hash,
        extrinsic_id=taostats_transfer.extrinsic_id,
        total_outflow_tao=total_outflow_rao / 1e9,
        fee_tao=taostats_transfer.fee_rao / 1e9,
        fee_cost_basis_usd=fee_cost_basis,
    )

    return transfer