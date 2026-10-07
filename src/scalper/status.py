"""Plain-text rendering of the status snapshot for ``lighter-scalper status``.

The running service writes ``data/status.json`` once a second from its writer
thread; this module only formats that dictionary. No curses, no browser.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .orderbook import spread_mbps
from .persistence import utc_iso
from .pnl import LONG, estimate_close

if TYPE_CHECKING:
    from .strategy import Trader

NS_PER_MS = 1_000_000


def recovery_snapshot(trader: Trader, last_event: str = "") -> dict[str, Any]:
    """Small recovery snapshot persisted on every transition and heartbeat."""
    pos = trader.position
    return {
        "state": trader.sm.state.value,
        "heartbeat": utc_iso(),
        "last_event": last_event,
        "last_client_order_index": trader.ids.last,
        "trade_id": trader.ctx.trade_id if trader.ctx is not None else None,
        "entry_order": trader.entry_order.client_order_index if trader.entry_order else None,
        "exit_order": trader.exit_order.client_order_index if trader.exit_order else None,
        "position": None
        if pos is None
        else {
            "side": pos.side,
            "size": pos.size,
            "cost_q": pos.cost_q,
            "entry_size": pos.entry_size,
            "exit_size": pos.exit_size,
            "adopted": pos.adopted,
        },
    }


def trader_view(trader: Trader, now_ns: int) -> dict[str, Any]:
    """Human-oriented view of the trader for the status file. Built once a second, off the hot path."""
    meta = trader.meta
    bids, asks = trader.market.top_bids, trader.market.top_asks
    bid = bids[0][0] if bids else 0
    ask = asks[0][0] if asks else 0
    view: dict[str, Any] = {
        "state": trader.sm.state.value,
        "blocks": dict(trader.blocks),
        "bid": meta.price_to_float(bid) if bid else None,
        "ask": meta.price_to_float(ask) if ask else None,
        "spread_bps": round(spread_mbps(bid, ask) / 1000.0, 3) if bid and ask else None,
        "fee_tick": trader.fee_tick,
        "available_usd": round(meta.q_to_usd(trader.available_q), 2) if trader.available_q is not None else None,
        "signal_score": round(trader.last_signal.score, 3),
        "exchange_position": None if trader.exch_pos is None else meta.size_to_float(trader.exch_pos.signed_size),
        "position": None,
    }
    pos = trader.position
    if pos is not None and pos.size > 0:
        limits = trader.limits
        estimate = estimate_close(
            pos.side,
            pos.size,
            pos.cost_q,
            pos.fee_q,
            bids if pos.side == LONG else asks,
            taker_fee_tick=trader.fee_tick,
            buffer_mbps=limits.buffer_mbps,
            max_slippage_mbps=limits.normal_slip_mbps,
            min_profit_q=limits.min_profit_for(pos.cost_q),
        )
        digits = meta.price_decimals + 2
        view["position"] = {
            "side": "LONG" if pos.side == LONG else "SHORT",
            "size": meta.size_to_float(pos.size),
            "entry": round(pos.avg_entry / meta.price_scale, digits),
            "executable_exit": round(estimate.close_vwap / meta.price_scale, digits) if estimate.fillable else None,
            "estimated_pnl_usd": round(meta.q_to_usd(estimate.net_pnl_q), 4) if estimate.full else None,
            "hold_ms": round((now_ns - pos.opened_ns) / NS_PER_MS),
            "adopted": pos.adopted,
        }
    return view


def _fmt(value: Any, suffix: str = "", digits: int | None = None) -> str:
    if value is None:
        return "-"
    if digits is not None and isinstance(value, int | float):
        return f"{value:.{digits}f}{suffix}"
    return f"{value}{suffix}"


def _latency(metrics: dict[str, Any], key: str) -> str:
    stat = (metrics.get("latency") or {}).get(key) or {}
    if stat.get("p50") is None:
        return "-"
    last, p50, p95 = (_fmt(stat.get(key), digits=1) for key in ("last", "p50", "p95"))
    return f"last {last} / p50 {p50} / p95 {p95}"


def render_status(snapshot: dict[str, Any]) -> str:
    """Format one status snapshot as an aligned text block."""
    metrics = snapshot.get("metrics") or {}
    limit = snapshot.get("rate_limit") or {}
    position = snapshot.get("position")
    blocks = snapshot.get("blocks") or {}
    rows: list[tuple[str, str]] = [
        ("STATUS", f"{snapshot.get('status', '?')} {snapshot.get('mode', '')}  (v{snapshot.get('version', '?')})"),
        ("UPDATED", str(snapshot.get("ts", "-"))),
        ("MARKET", str(snapshot.get("market", "-"))),
        ("BTC BID / ASK", f"{_fmt(snapshot.get('bid'))} / {_fmt(snapshot.get('ask'))}"),
        ("SPREAD", _fmt(snapshot.get("spread_bps"), " bps")),
        ("MARKET STREAM", f"{snapshot.get('market_ws', '-')}  (book age {_fmt(snapshot.get('book_age_ms'), ' ms')})"),
        ("ACCOUNT STREAM", str(snapshot.get("account_ws", "-"))),
        ("BOT STATE", str(snapshot.get("state", "-"))),
        ("ENTRY BLOCKS", ", ".join(sorted(blocks)) if blocks else "none"),
        (
            "LEVERAGE",
            f"{snapshot.get('leverage', '-')}x "
            f"({'confirmed' if snapshot.get('leverage_confirmed') else 'UNCONFIRMED'})",
        ),
        ("AVAILABLE", _fmt(snapshot.get("available_usd"), " USD")),
    ]
    if position:
        rows += [
            ("POSITION", f"{position.get('side')} {position.get('size')} BTC"),
            ("ENTRY", _fmt(position.get("entry"))),
            ("EXECUTABLE EXIT", _fmt(position.get("executable_exit"))),
            ("ESTIMATED P&L", _fmt(position.get("estimated_pnl_usd"), " USD")),
            ("HOLDING", _fmt(position.get("hold_ms"), " ms")),
        ]
    else:
        rows.append(("POSITION", "flat"))
    rows += [
        ("EXCHANGE POSITION", _fmt(snapshot.get("exchange_position"), " BTC")),
        ("SIGNAL SCORE", _fmt(snapshot.get("signal_score"))),
        ("TRADES TODAY", str(metrics.get("trades_today", 0))),
        (
            "WINS / LOSSES",
            f"{metrics.get('wins', 0)} / {metrics.get('losses', 0)}  ({_fmt(metrics.get('win_pct'), '%')})",
        ),
        ("REALIZED P&L", _fmt(metrics.get("realized_pnl_usd"), " USD")),
        ("FEES", _fmt(metrics.get("fees_usd"), " USD")),
        ("TRADES / MIN", str(metrics.get("trades_per_minute", 0))),
        ("HOLD (ms)", _latency(metrics, "hold_ms")),
        ("SIGNAL->SEND (ms)", _latency(metrics, "signal_to_send_ms")),
        ("SEND->ACK (ms)", _latency(metrics, "send_to_ack_ms")),
        ("SEND->FILL (ms)", _latency(metrics, "send_to_fill_ms")),
        ("GREEN->EXIT SEND (ms)", _latency(metrics, "profit_to_exit_send_ms")),
        ("EXIT SEND->FLAT (ms)", _latency(metrics, "exit_send_to_flat_ms")),
        ("API READ (ms)", _latency(metrics, "api_read_ms")),
        ("WS PING (ms)", _latency(metrics, "ws_ping_market_ms")),
        (
            "RATE LIMIT",
            f"tier {limit.get('tier', '-')}, headroom {limit.get('request_headroom', '-')}"
            f"/{limit.get('request_capacity', '-')} per min"
            + (f", quota {limit.get('volume_quota')}" if limit.get("volume_quota") is not None else ""),
        ),
        (
            "RECONNECTS",
            f"market {metrics.get('reconnects_market', 0)}, account {metrics.get('reconnects_account', 0)}",
        ),
        ("REJECTIONS / RECOVERIES", f"{metrics.get('order_rejections', 0)} / {metrics.get('recoveries', 0)}"),
    ]
    width = max(len(label) for label, _ in rows)
    lines = [f"{label:<{width}} : {value}" for label, value in rows]
    for name, detail in sorted(blocks.items()):
        lines.append(f"  block {name}: {detail}")
    return "\n".join(lines)
