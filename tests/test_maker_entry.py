"""Maker (post-only) entries: rest on our own side, cancel when the reason is gone.

The real ``Trader`` runs against a fake executor. No order leaves the process.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from scalper.config import load_config
from scalper.errors import ConfigError
from scalper.lighter_client import POST_ONLY_EXPIRY_MS, LighterClient
from scalper.pnl import LONG, SHORT
from scalper.position import ActiveOrder, OrderKind, TradeFill
from scalper.precision import fee_q
from scalper.rate_limits import RateLimiter, limits_for_tier
from scalper.risk import Limits, plan_maker_entry
from scalper.signals import NO_SIGNAL
from scalper.state_machine import State

from .conftest import BASE_ENV, BTC, make_config, px, sz
from .fakes import ASKS, BIDS, LONG_SIGNAL, SHORT_SIGNAL, ambiguous, exchange_position, flat_position, rejected
from .test_strategy import Rig, make_rig, settle

BEST_BID = px(83691.1)
BEST_ASK = px(83693.9)
TICK = 1


def maker_rig(**overrides: Any) -> Rig:
    return make_rig(make_config(ENTRY_MODE="maker", **overrides))


async def rest_long(rig: Rig) -> ActiveOrder:
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    assert rig.trader.sm.state is State.ENTRY_PENDING
    return rig.order()


# ------------------------------------------------------------------ pricing


def plan(side: int, bids: Any = BIDS, asks: Any = ASKS, **kw: Any) -> tuple[Any, str]:
    params: dict[str, Any] = {
        "limits": Limits.from_config(make_config(ENTRY_MODE="maker"), BTC),
        "meta": BTC,
        "available_balance_q": 100_000_000,
        "volatility_mbps": 0,
        "fee_tick": 0,
    }
    params.update(kw)
    return plan_maker_entry(side, bids, asks, **params)


def test_maker_plan_rests_one_tick_inside_the_spread() -> None:
    long_plan, _ = plan(LONG)
    short_plan, _ = plan(SHORT)
    assert long_plan.limit_price == BEST_BID + TICK and long_plan.side == LONG
    assert short_plan.limit_price == BEST_ASK - TICK and short_plan.side == SHORT
    assert long_plan.size == 250_000_000 // long_plan.limit_price  # 10 USD margin x 25


def test_maker_plan_never_crosses_a_one_tick_spread() -> None:
    bids = [(px(83691.1), sz(0.05))]
    asks = [(px(83691.2), sz(0.05))]
    assert plan(LONG, bids, asks)[0].limit_price == px(83691.1)  # joins the bid
    assert plan(SHORT, bids, asks)[0].limit_price == px(83691.2)  # joins the ask


def test_maker_plan_can_join_the_touch_without_improving() -> None:
    limits = Limits.from_config(make_config(ENTRY_MODE="maker", MAKER_IMPROVE_TICKS=0), BTC)
    assert plan(LONG, limits=limits)[0].limit_price == BEST_BID
    assert plan(SHORT, limits=limits)[0].limit_price == BEST_ASK


def test_maker_plan_gates() -> None:
    wide = [(px(83800.0), sz(0.05))]
    assert plan(LONG, asks=wide) == (None, "SPREAD")
    assert plan(LONG, volatility_mbps=10**9) == (None, "VOLATILITY")
    assert plan(LONG, available_balance_q=None) == (None, "BALANCE_UNKNOWN")
    assert plan(LONG, available_balance_q=1_000_000) == (None, "BALANCE")
    assert plan(LONG, bids=[]) == (None, "NO_BOOK")
    crossed = [(px(83691.1), sz(0.05))]
    assert plan(LONG, bids=crossed, asks=crossed) == (None, "NO_BOOK")
    tiny = Limits.from_config(make_config(ENTRY_MODE="maker", MARGIN_PER_TRADE_USD="0.1"), BTC)
    assert plan(LONG, limits=tiny) == (None, "SIZE_BELOW_MIN")


# ---------------------------------------------------------------- lifecycle


async def test_maker_long_rests_then_fills_and_exits_on_green() -> None:
    rig = maker_rig()
    entry = await rest_long(rig)
    assert entry.kind is OrderKind.ENTRY and entry.post_only and not entry.is_ask
    assert not entry.reduce_only and not entry.market_order
    assert entry.limit_price == BEST_BID + TICK  # below the ask: we do not pay the spread

    # Resting: the signal is still there, but no second order is ever sent.
    rig.tick()
    rig.tick()
    assert len(rig.executor.submitted) == 1 and rig.executor.cancels == []

    rig.fill(entry)
    assert rig.trader.sm.state is State.OPEN_LONG
    assert rig.executor.cancels == []
    rig.trader.on_position(exchange_position(entry.size, entry.limit_price))
    rig.signals.signal = NO_SIGNAL

    rig.market.set([(px(83695.0), sz(0.05))], [(px(83696.0), sz(0.05))])
    rig.tick()
    assert rig.trader.sm.state is State.EXIT_PENDING
    exit_order = rig.order()
    assert exit_order.reduce_only and exit_order.is_ask and not exit_order.post_only
    await settle()
    rig.fill(exit_order, price=px(83695.0))
    assert rig.trader.sm.state is State.FLAT
    trade = rig.journal.trades[0]
    assert trade["exit_reason"] == "PROFIT"
    assert trade["realized_pnl_usd"] == pytest.approx(entry.size * (px(83695.0) - entry.limit_price) / 1_000_000)


async def test_maker_short_rests_below_the_ask() -> None:
    rig = maker_rig()
    rig.signals.signal = SHORT_SIGNAL
    rig.tick()
    await settle()
    entry = rig.order()
    assert entry.is_ask and entry.post_only and entry.limit_price == BEST_ASK - TICK
    rig.fill(entry)
    assert rig.trader.sm.state is State.OPEN_SHORT


async def test_unfilled_entry_is_cancelled_after_the_rest_time() -> None:
    rig = maker_rig(MAKER_REST_MS=100)
    entry = await rest_long(rig)
    await asyncio.sleep(0.2)
    assert rig.executor.cancels == [entry.client_order_index]
    # Not flat yet: the order can still fill until the exchange confirms the cancel.
    assert rig.trader.sm.state is State.ENTRY_PENDING
    rig.tick()
    assert len(rig.executor.submitted) == 1 and len(rig.executor.cancels) == 1

    rig.fill(entry, filled=0, status="canceled")
    assert rig.trader.sm.state is State.FLAT
    assert rig.trader.position is None and rig.journal.trades == []
    assert rig.trader.metrics.entries_cancelled == 1 and rig.trader.metrics.entries_missed == 1

    rig.tick()  # the signal is still valid: a fresh order may rest now
    assert rig.trader.sm.state is State.ENTRY_PENDING and len(rig.executor.submitted) == 2


async def test_fill_that_beats_the_cancel_is_managed_as_a_position() -> None:
    rig = maker_rig(MAKER_REST_MS=100)
    entry = await rest_long(rig)
    await asyncio.sleep(0.2)
    assert rig.executor.cancels == [entry.client_order_index]
    rig.fill(entry)  # filled before the cancel took effect
    assert rig.trader.sm.state is State.OPEN_LONG
    position = rig.trader.position
    assert position is not None and position.size == entry.size
    assert rig.recovery.recoveries == []


async def test_partial_fill_cancels_the_rest_before_any_exit() -> None:
    rig = maker_rig()
    entry = await rest_long(rig)
    part = entry.size // 3
    rig.fill(entry, filled=part, status="open")
    await settle()
    assert rig.trader.sm.state is State.PARTIALLY_FILLED
    assert rig.executor.cancels == [entry.client_order_index]

    # Even a green book does not send a reduce-only exit while the entry can still fill.
    rig.market.set([(px(83710.0), sz(0.05))], [(px(83711.0), sz(0.05))])
    rig.tick()
    assert len(rig.executor.submitted) == 1 and len(rig.executor.cancels) == 1

    rig.fill(entry, filled=part, status="canceled")
    exit_order = rig.order()
    assert rig.trader.sm.state is State.EXIT_PENDING  # green was waiting: exit at once
    assert exit_order.reduce_only and exit_order.size == part


async def test_full_fill_seen_on_the_trade_feed_first_needs_no_cancel() -> None:
    rig = maker_rig()
    entry = await rest_long(rig)
    rig.trader.on_trade_fill(TradeFill(1, entry.client_order_index, 1, entry.size, entry.limit_price, False, 0))
    await settle()
    assert rig.trader.sm.state is State.OPEN_LONG and rig.executor.cancels == []


async def test_post_only_rejection_returns_to_flat() -> None:
    rig = maker_rig()
    entry = await rest_long(rig)
    rig.fill(entry, filled=0, status="canceled-post-only")
    assert rig.trader.sm.state is State.FLAT and rig.executor.cancels == []


async def test_rejected_send_returns_to_flat() -> None:
    rig = maker_rig()
    rig.executor.results.append(rejected())
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    assert rig.trader.sm.state is State.FLAT and rig.trader.entry_order is None


async def test_ambiguous_send_goes_to_recovery_and_is_never_resent() -> None:
    rig = maker_rig()
    rig.executor.results.append(ambiguous())
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    assert rig.recovery.recoveries == ["ENTRY_SEND_AMBIGUOUS"] and len(rig.executor.submitted) == 1


# ------------------------------------------------------------ cancel reasons


async def test_entry_is_cancelled_when_the_price_runs_away() -> None:
    rig = maker_rig()
    entry = await rest_long(rig)
    rig.market.set([(BEST_BID + 3, sz(0.05))], [(BEST_ASK + 3, sz(0.05))])  # a few ticks: noise
    rig.tick()
    assert rig.executor.cancels == []
    rig.market.set([(px(83700.0), sz(0.05))], [(px(83701.0), sz(0.05))])  # > 0.5 bps above our bid
    rig.tick()
    await settle()
    assert rig.executor.cancels == [entry.client_order_index]


async def test_drift_check_can_be_disabled() -> None:
    rig = maker_rig(MAKER_MAX_DRIFT_BPS=0)
    await rest_long(rig)
    rig.market.set([(px(83700.0), sz(0.05))], [(px(83701.0), sz(0.05))])
    rig.tick()
    assert rig.executor.cancels == []


async def test_entry_is_cancelled_when_the_signal_reverses() -> None:
    rig = maker_rig()
    entry = await rest_long(rig)
    rig.signals.signal = NO_SIGNAL  # fading is not a reversal
    rig.tick()
    assert rig.executor.cancels == []
    rig.signals.signal = SHORT_SIGNAL
    rig.tick()
    await settle()
    assert rig.executor.cancels == [entry.client_order_index]


async def test_entry_is_cancelled_when_entries_get_blocked() -> None:
    rig = maker_rig()
    entry = await rest_long(rig)
    rig.trader.blocks["PAUSED"] = "operator"
    rig.tick()
    await settle()
    assert rig.executor.cancels == [entry.client_order_index]


async def test_entry_is_cancelled_when_market_data_goes_stale() -> None:
    rig = maker_rig()
    entry = await rest_long(rig)
    rig.trader.on_market_stale()
    await settle()
    assert rig.executor.cancels == [entry.client_order_index] and rig.recovery.recoveries == []


async def test_cancel_decided_before_the_ack_is_sent_after_it() -> None:
    rig = maker_rig()
    rig.executor.gate = asyncio.Event()
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    entry = rig.order()
    rig.trader.on_market_stale()
    await settle()
    assert rig.executor.cancels == [] and entry.cancel_requested  # nothing to cancel yet
    rig.executor.gate.set()
    await settle()
    assert rig.executor.cancels == [entry.client_order_index]


async def test_unconfirmed_cancel_ends_in_recovery() -> None:
    rig = maker_rig(MAKER_REST_MS=100, ORDER_RESOLVE_TIMEOUT_MS=500)
    rig.executor.cancel_results.append(rejected())
    await rest_long(rig)
    await asyncio.sleep(0.8)
    assert len(rig.executor.cancels) == 1  # the cancel itself is never repeated blindly
    assert rig.recovery.recoveries == ["ORDER_UNRESOLVED"]


# -------------------------------------------------------------- environment


async def test_stale_order_counter_does_not_block_maker_entries() -> None:
    rig = maker_rig()
    rig.trader.exch_pos = flat_position(open_orders=1)  # our own previous order, not yet refreshed
    await rest_long(rig)
    rig2 = maker_rig()
    rig2.trader.exch_pos = exchange_position(10, BEST_BID)  # a real position always blocks
    rig2.signals.signal = LONG_SIGNAL
    rig2.tick()
    assert rig2.trader.sm.state is State.FLAT


async def test_maker_entry_budgets_three_transactions() -> None:
    limiter = RateLimiter(limits_for_tier("standard"), 20)
    while limiter.can_enter(3):
        limiter.note_tx()
    assert not limiter.can_enter(3)
    assert limiter.can_enter(2)  # one transaction of headroom separates the two modes


async def test_maker_fee_rate_is_used_for_the_entry() -> None:
    rig = maker_rig()
    rig.trader.fee_tick = 500
    rig.trader.maker_fee_tick = 40
    entry = await rest_long(rig)
    rig.fill(entry)
    position = rig.trader.position
    assert position is not None
    assert position.fee_q == fee_q(entry.size * entry.limit_price, 40)  # not the taker rate
    assert position.fee_q < fee_q(entry.size * entry.limit_price, 500)


def test_entry_mode_defaults_to_maker_and_is_validated() -> None:
    env = {k: v for k, v in BASE_ENV.items() if k != "ENTRY_MODE"}
    cfg = load_config(env=env)
    assert cfg.entry_mode == "maker" and cfg.maker_rest_ms == 1500
    assert cfg.maker_improve_ticks == 1 and cfg.maker_max_drift_mbps == 500
    with pytest.raises(ConfigError):
        load_config(env={**env, "ENTRY_MODE": "market"})
    with pytest.raises(ConfigError):
        load_config(env={**env, "MAKER_REST_MS": "5"})


# ------------------------------------------------------------------ signing


class _Signer:
    ORDER_TYPE_LIMIT = 0
    ORDER_TYPE_MARKET = 1
    ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
    ORDER_TIME_IN_FORCE_POST_ONLY = 2
    NIL_TRIGGER_PRICE = 0
    DEFAULT_IOC_EXPIRY = 0

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def sign_create_order(self, **kw: Any) -> tuple[int, str, str, None]:
        self.calls.append(kw)
        return 14, "{}", "hash", None


def _order(post_only: bool) -> ActiveOrder:
    return ActiveOrder(77, OrderKind.ENTRY, False, 298, BEST_BID, False, False, "SIGNAL", 0, post_only=post_only)


def test_post_only_orders_are_signed_as_resting_limit_orders(monkeypatch: pytest.MonkeyPatch) -> None:
    signer = _Signer()
    client = LighterClient.__new__(LighterClient)
    client._signer = signer  # type: ignore[assignment]
    client._cfg = SimpleNamespace(api_key_index=4)  # type: ignore[assignment]
    monkeypatch.setattr("scalper.lighter_client.time.time", lambda: 1_700_000_000.0)

    client.sign_order(_order(post_only=True), 1, 5)
    client.sign_order(_order(post_only=False), 1, 6)
    resting, ioc = signer.calls
    assert resting["time_in_force"] == 2 and resting["order_type"] == 0 and resting["reduce_only"] == 0
    assert resting["order_expiry"] == 1_700_000_000_000 + POST_ONLY_EXPIRY_MS
    assert POST_ONLY_EXPIRY_MS >= 5 * 60 * 1000  # Lighter rejects shorter expiries
    assert ioc["time_in_force"] == 0 and ioc["order_expiry"] == 0
