"""Turns a finished position into its journal record.

Everything here is computed from confirmed fills. Nothing is derived from mark
price or from an estimate, so a trade is only ever reported with the P&L it
actually realized.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .metrics import utc_date
from .persistence import utc_iso
from .pnl import LONG, realized_pnl
from .position import Position, TradeContext
from .precision import MarketMeta

NS_PER_MS = 1_000_000


@dataclass(frozen=True, slots=True)
class ClosedTrade:
    gross_usd: float
    fees_usd: float
    net_usd: float
    hold_ms: float
    avg_entry: float  # real price
    avg_exit: float  # real price
    exit_slippage_bps: float | None  # realized exit versus the best price at decision time
    record: dict[str, Any]  # row for the trade journal


def _ms(start_ns: int, end_ns: int) -> float | None:
    return round((end_ns - start_ns) / NS_PER_MS, 3) if start_ns and end_ns and end_ns >= start_ns else None


def _iso(ctx: TradeContext, mono_ns: int) -> str | None:
    return utc_iso(ctx.wall_ns(mono_ns)) if mono_ns else None


def close_trade(
    pos: Position, ctx: TradeContext, meta: MarketMeta, leverage: int, now_ns: int, exit_attempts: int
) -> ClosedTrade:
    """Summarise a position whose remaining size has reached zero."""
    gross_q, fees_q, net_q = realized_pnl(
        pos.side, pos.closed_cost_q, pos.exit_value_q, pos.entry_fee_q, pos.exit_fee_q
    )
    gross, fees, net = meta.q_to_usd(gross_q), meta.q_to_usd(fees_q), meta.q_to_usd(net_q)
    hold_ms = (now_ns - (ctx.first_fill_ns or pos.opened_ns)) / NS_PER_MS
    avg_entry = pos.entry_value_q / pos.entry_size / meta.price_scale if pos.entry_size else 0.0
    avg_exit = pos.exit_value_q / pos.exit_size / meta.price_scale if pos.exit_size else 0.0
    slippage: float | None = None
    if ctx.exit_ref_price and avg_exit:
        reference = ctx.exit_ref_price / meta.price_scale
        slippage = (reference - avg_exit) / reference * 10_000.0 * pos.side
    digits = meta.price_decimals + 2
    record: dict[str, Any] = {
        "trade_id": ctx.trade_id,
        "side": "LONG" if pos.side == LONG else "SHORT",
        "leverage": leverage,
        "size": meta.fmt_size(pos.entry_size),
        "signal_score": round(ctx.signal_score, 4),
        "signal_components": ctx.signal_components,
        "entry_decision_at": _iso(ctx, ctx.decision_ns),
        "entry_sent_at": _iso(ctx, ctx.entry_sent_ns),
        "entry_fill_at": _iso(ctx, ctx.full_fill_ns),
        "avg_entry": round(avg_entry, digits),
        "exit_decision_at": _iso(ctx, ctx.exit_decision_ns),
        "exit_sent_at": _iso(ctx, ctx.exit_sent_ns),
        "exit_fill_at": _iso(ctx, now_ns),
        "avg_exit": round(avg_exit, digits),
        "holding_ms": round(hold_ms, 1),
        "gross_pnl_usd": round(gross, 6),
        "fees_usd": round(fees, 6),
        "estimated_slippage_usd": round(meta.q_to_usd(ctx.estimated_slippage_q), 6),
        "realized_pnl_usd": round(net, 6),
        "mfe_usd": round(meta.q_to_usd(pos.mfe_q), 6),
        "mae_usd": round(meta.q_to_usd(pos.mae_q), 6),
        "exit_reason": ctx.exit_reason or "UNKNOWN",
        "adopted": int(pos.adopted),
        "latency": {
            "signal_to_send_ms": _ms(ctx.signal_ns, ctx.entry_sent_ns),
            "send_to_ack_ms": _ms(ctx.entry_sent_ns, ctx.entry_ack_ns),
            "send_to_fill_ms": _ms(ctx.entry_sent_ns, ctx.full_fill_ns),
            "fill_to_profit_ms": _ms(ctx.full_fill_ns, ctx.profit_detected_ns),
            "profit_to_exit_send_ms": _ms(ctx.profit_detected_ns, ctx.exit_sent_ns),
            "exit_send_to_flat_ms": _ms(ctx.exit_sent_ns, now_ns),
            "exit_attempts": exit_attempts,
        },
        "trade_date": utc_date(),
    }
    return ClosedTrade(gross, fees, net, hold_ms, avg_entry, avg_exit, slippage, record)
