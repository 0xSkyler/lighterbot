"""Pre-trade gates, sizing and hard protections. Pure functions over integers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .config import Config
from .errors import ConfigError
from .orderbook import spread_mbps
from .pnl import LONG, EntryEstimate, adverse_move_mbps, estimate_entry
from .precision import IMF_DENOM, MBPS_DENOM, MarketMeta, ceil_div, fee_q

NS_PER_MS = 1_000_000


def validate_leverage(leverage: int, meta: MarketMeta) -> int:
    """Return the initial margin fraction for ``leverage`` or raise if the market forbids it.

    The configured leverage is never silently replaced by another value.
    """
    if leverage < 1:
        raise ConfigError(f"LEVERAGE={leverage} is invalid")
    imf = IMF_DENOM // leverage
    if imf < meta.min_imf:
        raise ConfigError(
            f"LEVERAGE={leverage} exceeds the maximum {meta.max_leverage.normalize()}x "
            f"currently allowed for {meta.symbol} on Lighter"
        )
    return imf


@dataclass(frozen=True, slots=True)
class Limits:
    """Risk limits pre-converted to hot-path integer units."""

    notional_q: int
    imf: int
    min_profit_q: int
    min_profit_mbps: int
    max_loss_q: int  # 0 = not set
    max_adverse_mbps: int  # 0 = not set
    max_hold_ns: int
    stale_ns: int
    max_spread_mbps: int
    max_volatility_mbps: int
    entry_slip_mbps: int
    normal_slip_mbps: int
    emergency_slip_mbps: int
    buffer_mbps: int
    maker_improve_ticks: int = 0

    @classmethod
    def from_config(cls, cfg: Config, meta: MarketMeta) -> Limits:
        return cls(
            notional_q=meta.usd_to_q(cfg.notional_usd),
            imf=validate_leverage(cfg.leverage, meta),
            min_profit_q=meta.usd_to_q(cfg.min_profit_usd),
            min_profit_mbps=cfg.min_profit_mbps,
            max_loss_q=meta.usd_to_q(cfg.max_loss_usd),
            max_adverse_mbps=cfg.max_adverse_move_mbps,
            max_hold_ns=cfg.max_hold_ms * NS_PER_MS,
            stale_ns=cfg.market_data_stale_ms * NS_PER_MS,
            max_spread_mbps=cfg.max_spread_mbps,
            max_volatility_mbps=cfg.max_volatility_mbps,
            entry_slip_mbps=cfg.max_entry_slippage_mbps,
            normal_slip_mbps=cfg.max_normal_exit_slippage_mbps,
            emergency_slip_mbps=cfg.max_emergency_exit_slippage_mbps,
            buffer_mbps=cfg.safety_buffer_mbps,
            maker_improve_ticks=cfg.maker_improve_ticks,
        )

    def min_profit_for(self, cost_q: int) -> int:
        """Minimum net profit for GREEN: the larger of the USD and the bps threshold."""
        relative = cost_q * self.min_profit_mbps // MBPS_DENOM
        return relative if relative > self.min_profit_q else self.min_profit_q


@dataclass(frozen=True, slots=True)
class EntryPlan:
    side: int
    size: int
    limit_price: int
    estimate: EntryEstimate
    required_margin_q: int


def plan_entry(
    side: int,
    bids: Sequence[tuple[int, int]],
    asks: Sequence[tuple[int, int]],
    *,
    limits: Limits,
    meta: MarketMeta,
    available_balance_q: int | None,
    volatility_mbps: int,
    fee_tick: int,
) -> tuple[EntryPlan | None, str]:
    """Size an entry and run every market-quality gate.

    Returns ``(plan, "")`` or ``(None, reason)``. An entry is skipped, never
    forced, when execution quality is poor.
    """
    if not bids or not asks:
        return None, "NO_BOOK"
    if spread_mbps(bids[0][0], asks[0][0]) > limits.max_spread_mbps:
        return None, "SPREAD"
    if volatility_mbps > limits.max_volatility_mbps:
        return None, "VOLATILITY"
    is_buy = side == LONG
    levels = asks if is_buy else bids
    price = levels[0][0]
    size = meta.size_for_notional(limits.notional_q, price)
    if size < meta.min_size_at(price):
        return None, "SIZE_BELOW_MIN"
    estimate = estimate_entry(is_buy, size, levels, limits.entry_slip_mbps)
    if not estimate.full:
        return None, "DEPTH"
    if available_balance_q is None:
        return None, "BALANCE_UNKNOWN"
    margin_q = ceil_div(estimate.value_q * limits.imf, IMF_DENOM) + fee_q(estimate.value_q, fee_tick)
    if available_balance_q * 100 < margin_q * 105:
        return None, "BALANCE"
    return EntryPlan(side, size, estimate.limit_price, estimate, margin_q), ""


def plan_maker_entry(
    side: int,
    bids: Sequence[tuple[int, int]],
    asks: Sequence[tuple[int, int]],
    *,
    limits: Limits,
    meta: MarketMeta,
    available_balance_q: int | None,
    volatility_mbps: int,
    fee_tick: int,
) -> tuple[EntryPlan | None, str]:
    """Size a resting (post-only) entry on our own side of the book.

    A long rests at the best bid, a short at the best ask, improved by up to
    ``MAKER_IMPROVE_TICKS`` when the spread has room, and always at least one tick away
    from the opposite side so the order can never cross. There is no depth walk:
    nothing is taken from the book.
    """
    if not bids or not asks:
        return None, "NO_BOOK"
    bid = bids[0][0]
    ask = asks[0][0]
    if ask <= bid:
        return None, "NO_BOOK"
    if spread_mbps(bid, ask) > limits.max_spread_mbps:
        return None, "SPREAD"
    if volatility_mbps > limits.max_volatility_mbps:
        return None, "VOLATILITY"
    improve = limits.maker_improve_ticks
    price = min(bid + improve, ask - 1) if side == LONG else max(ask - improve, bid + 1)
    size = meta.size_for_notional(limits.notional_q, price)
    if size < meta.min_size_at(price):
        return None, "SIZE_BELOW_MIN"
    if available_balance_q is None:
        return None, "BALANCE_UNKNOWN"
    value_q = price * size
    margin_q = ceil_div(value_q * limits.imf, IMF_DENOM) + fee_q(value_q, fee_tick)
    if available_balance_q * 100 < margin_q * 105:
        return None, "BALANCE"
    estimate = EntryEstimate(size, size, True, value_q, price, price, price, 0)
    return EntryPlan(side, size, price, estimate, margin_q), ""


def hard_loss_reason(
    side: int, size: int, cost_q: int, best_exit_price: int, net_pnl_q: int | None, limits: Limits
) -> str | None:
    """Catastrophic-protection check. Returns the breach name or None.

    ``net_pnl_q`` is the executable net P&L of closing now (None if unknown).
    """
    if limits.max_adverse_mbps and best_exit_price > 0:
        if adverse_move_mbps(side, size, cost_q, best_exit_price) >= limits.max_adverse_mbps:
            return "MAX_ADVERSE_MOVE"
    if limits.max_loss_q and net_pnl_q is not None and net_pnl_q <= -limits.max_loss_q:
        return "MAX_LOSS"
    return None


def hold_expired(now_ns: int, opened_ns: int, limits: Limits) -> bool:
    return now_ns - opened_ns >= limits.max_hold_ns


def is_stale(now_ns: int, last_event_ns: int, limits: Limits) -> bool:
    """True when no market-data event arrived within MARKET_DATA_STALE_MS."""
    return last_event_ns == 0 or now_ns - last_event_ns > limits.stale_ns
