"""Executable P&L arithmetic.

This module decides whether a position is GREEN. GREEN is never derived from
last trade, mark price or candle colour: it means the *entire* remaining
position can be closed right now against resting liquidity (bids for a long,
asks for a short) and the resulting realized P&L, after fees and a safety
buffer, exceeds the configured minimum.

All arithmetic is exact integer math in exchange units (see ``precision.py``).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .precision import MBPS_DENOM, ceil_div, fee_q, price_minus_mbps, price_plus_mbps

LONG = 1
SHORT = -1


def walk_levels(
    levels: Sequence[tuple[int, int]], size: int, limit_price: int, is_buy: bool
) -> tuple[int, int, int, int]:
    """Consume ``levels`` (best first) up to ``size`` without crossing ``limit_price``.

    Returns ``(filled, value_q, worst_price, available)`` where ``available`` is all
    liquidity inside the limit over the supplied levels (may exceed ``size``).
    """
    filled = 0
    value_q = 0
    worst = 0
    available = 0
    for price, level_size in levels:
        if (price > limit_price) if is_buy else (price < limit_price):
            break
        available += level_size
        remaining = size - filled
        if remaining > 0:
            take = level_size if level_size < remaining else remaining
            filled += take
            value_q += take * price
            worst = price
    return filled, value_q, worst, available


@dataclass(frozen=True, slots=True)
class EntryEstimate:
    """Expected result of an aggressive entry of ``size`` against the book."""

    size: int
    fillable: int
    full: bool
    value_q: int
    best_price: int
    limit_price: int
    worst_price: int
    slippage_mbps: int  # VWAP versus best price

    @property
    def vwap(self) -> float:
        return self.value_q / self.fillable if self.fillable else 0.0


def estimate_entry(is_buy: bool, size: int, levels: Sequence[tuple[int, int]], max_slippage_mbps: int) -> EntryEstimate:
    """VWAP and price impact of buying/selling ``size`` now, capped at the slippage limit.

    ``levels`` are the asks (for a buy) or bids (for a sell), best first.
    """
    if not levels or size <= 0:
        return EntryEstimate(size, 0, False, 0, 0, 0, 0, 0)
    best = levels[0][0]
    limit = price_plus_mbps(best, max_slippage_mbps) if is_buy else price_minus_mbps(best, max_slippage_mbps)
    filled, value_q, worst, _ = walk_levels(levels, size, limit, is_buy)
    if filled <= 0:
        return EntryEstimate(size, 0, False, 0, best, limit, 0, 0)
    reference_q = best * filled
    impact_q = value_q - reference_q if is_buy else reference_q - value_q
    slippage = impact_q * MBPS_DENOM // reference_q
    return EntryEstimate(size, filled, filled >= size, value_q, best, limit, worst, slippage)


@dataclass(frozen=True, slots=True)
class CloseEstimate:
    """Expected result of closing ``size`` of a position against the book right now."""

    side: int
    size: int
    fillable: int  # how much of ``size`` can be closed inside the price limit
    available_liquidity: int  # all exit-side liquidity inside the limit (supplied levels)
    full: bool  # the entire position is closeable
    close_value_q: int
    best_price: int
    limit_price: int
    worst_price: int
    gross_pnl_q: int
    exit_fee_q: int
    entry_fee_q: int
    slippage_q: int  # price impact versus the best exit price (already inside gross_pnl_q)
    buffer_q: int
    net_pnl_q: int
    profitable: bool

    @property
    def close_vwap(self) -> float:
        return self.close_value_q / self.fillable if self.fillable else 0.0

    @property
    def fees_q(self) -> int:
        return self.exit_fee_q + self.entry_fee_q


def estimate_close(
    side: int,
    size: int,
    entry_cost_q: int,
    entry_fee_q: int,
    levels: Sequence[tuple[int, int]],
    *,
    taker_fee_tick: int,
    buffer_mbps: int,
    max_slippage_mbps: int,
    min_profit_q: int,
) -> CloseEstimate:
    """Estimate the realized P&L of closing ``size`` immediately.

    Args:
        side: ``LONG`` (+1) or ``SHORT`` (-1).
        size: remaining position size to close, size units.
        entry_cost_q: cost basis of exactly that size (sum of fill price * size).
        entry_fee_q: entry fees already paid for that size.
        levels: exit-side book levels, best first (bids for a long, asks for a short).
        taker_fee_tick: account taker fee, 1 tick = 1e-6 of notional.
        buffer_mbps: safety buffer charged against the exit notional.
        max_slippage_mbps: how far past the best exit price the close may reach.
        min_profit_q: minimum net profit for GREEN, quote units.

    ``net = gross - exit_fee - entry_fee - buffer`` where ``gross`` uses the real
    depth-walked VWAP, so price impact is already included. ``profitable`` is True
    only when the whole size is closeable and ``net > min_profit_q``.
    """
    if not levels or size <= 0:
        return CloseEstimate(side, size, 0, 0, False, 0, 0, 0, 0, 0, 0, entry_fee_q, 0, 0, 0, False)
    is_buy = side == SHORT  # closing a short means buying from the asks
    best = levels[0][0]
    limit = price_plus_mbps(best, max_slippage_mbps) if is_buy else price_minus_mbps(best, max_slippage_mbps)
    filled, value_q, worst, available = walk_levels(levels, size, limit, is_buy)
    full = filled >= size
    if filled <= 0:
        return CloseEstimate(side, size, 0, available, False, 0, best, limit, 0, 0, 0, entry_fee_q, 0, 0, 0, False)
    # Cost basis of the closeable part (identical to entry_cost_q when the close is full).
    cost_q = entry_cost_q if full else entry_cost_q * filled // size
    paid_fee_q = entry_fee_q if full else ceil_div(entry_fee_q * filled, size)
    gross = value_q - cost_q if side == LONG else cost_q - value_q
    exit_fee = fee_q(value_q, taker_fee_tick)
    buffer = ceil_div(value_q * buffer_mbps, MBPS_DENOM) if buffer_mbps > 0 else 0
    reference_q = best * filled
    slippage = reference_q - value_q if side == LONG else value_q - reference_q
    net = gross - exit_fee - paid_fee_q - buffer
    return CloseEstimate(
        side=side,
        size=size,
        fillable=filled,
        available_liquidity=available,
        full=full,
        close_value_q=value_q,
        best_price=best,
        limit_price=limit,
        worst_price=worst,
        gross_pnl_q=gross,
        exit_fee_q=exit_fee,
        entry_fee_q=paid_fee_q,
        slippage_q=slippage,
        buffer_q=buffer,
        net_pnl_q=net,
        profitable=full and net > min_profit_q,
    )


def breakeven_exit_price(
    side: int, size: int, entry_cost_q: int, entry_fee_q: int, *, taker_fee_tick: int, min_profit_q: int
) -> int:
    """Worst uniform exit price at which closing ``size`` still nets more than ``min_profit_q``.

    For a long: smallest price p with ``p*size*(1 - fee) - cost - entry_fee > min_profit``.
    For a short: largest price p with ``cost - p*size*(1 + fee) - entry_fee > min_profit``.
    Returns 0 for a short if no positive price qualifies.
    """
    fee_denom = 1_000_000
    # The extra quote unit absorbs the round-up applied to the exit fee.
    if side == LONG:
        need = entry_cost_q + entry_fee_q + min_profit_q + 1
        return need * fee_denom // (size * (fee_denom - taker_fee_tick)) + 1
    room = entry_cost_q - entry_fee_q - min_profit_q - 1
    if room <= 0:
        return 0
    price = ceil_div(room * fee_denom, size * (fee_denom + taker_fee_tick)) - 1
    return max(price, 0)


def realized_pnl(
    side: int, entry_cost_q: int, exit_value_q: int, entry_fee_q: int, exit_fee_q: int
) -> tuple[int, int, int]:
    """Realized ``(gross, fees, net)`` of a closed trade, quote units."""
    gross = exit_value_q - entry_cost_q if side == LONG else entry_cost_q - exit_value_q
    fees = entry_fee_q + exit_fee_q
    return gross, fees, gross - fees


def adverse_move_mbps(side: int, size: int, entry_cost_q: int, exit_price: int) -> int:
    """How far the best executable exit price sits against the average entry, in milli-bps.

    Positive means the position is losing; negative means it is ahead.
    """
    if size <= 0 or entry_cost_q <= 0:
        return 0
    exit_q = exit_price * size
    diff = entry_cost_q - exit_q if side == LONG else exit_q - entry_cost_q
    return diff * MBPS_DENOM // entry_cost_q
