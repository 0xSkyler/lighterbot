"""The trading core end to end against fakes: entry, fills, GREEN exit, protections.

These tests drive the real ``Trader`` with a fake executor. No order leaves the process.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from scalper import strategy
from scalper.config import Config
from scalper.errors import ErrorClass
from scalper.metrics import Metrics
from scalper.orderbook import OrderBook
from scalper.position import ActiveOrder, ClientOrderIds, OrderKind, OrderUpdate, TradeFill
from scalper.rate_limits import RateLimiter, limits_for_tier
from scalper.risk import Limits
from scalper.signals import NO_SIGNAL
from scalper.state_machine import State
from scalper.strategy import Trader

from .conftest import BTC, make_config, px, sz
from .fakes import (
    ASKS,
    BIDS,
    LONG_SIGNAL,
    SHORT_SIGNAL,
    FakeExecutor,
    FakeJournal,
    FakeMarket,
    FakeRecovery,
    FakeSignals,
    ambiguous,
    exchange_position,
    flat_position,
    rejected,
)

ENTRY_ASK = px(83693.9)
ENTRY_BID = px(83691.1)


@dataclass
class Rig:
    trader: Trader
    market: FakeMarket
    signals: FakeSignals
    executor: FakeExecutor
    journal: FakeJournal
    recovery: FakeRecovery
    limiter: RateLimiter
    clock: list[int]

    def tick(self, advance_ms: int = 0) -> None:
        self.clock[0] += advance_ms * 1_000_000
        self.trader.on_market(self.clock[0])

    def order(self, index: int = -1) -> ActiveOrder:
        return self.executor.submitted[index]

    def fill(
        self, order: ActiveOrder, filled: int | None = None, price: int | None = None, status: str = "filled"
    ) -> None:
        size = order.size if filled is None else filled
        fill_price = price if price is not None else order.limit_price
        self.trader.on_order_update(
            OrderUpdate(
                order.client_order_index,
                9,
                1,
                order.is_ask,
                status,
                size,
                size * fill_price,
                order.size - size,
                order.reduce_only,
            )
        )

    def exchange_flat(self) -> None:
        self.trader.on_position(flat_position())


def make_rig(cfg: Config | None = None, fee_tick: int = 0) -> Rig:
    cfg = cfg or make_config()
    book = OrderBook()
    market = FakeMarket(book)
    market.set(BIDS, ASKS)
    signals = FakeSignals()
    executor = FakeExecutor()
    journal = FakeJournal()
    recovery = FakeRecovery()
    limiter = RateLimiter(limits_for_tier("premium"), cfg.max_entries_per_minute)
    clock = [1_000_000_000_000]
    trader = Trader(
        cfg=cfg,
        meta=BTC,
        limits=Limits.from_config(cfg, BTC),
        book=book,
        market=market,
        signals=signals,  # type: ignore[arg-type]
        executor=executor,  # type: ignore[arg-type]
        limiter=limiter,
        metrics=Metrics(),
        journal=journal,  # type: ignore[arg-type]
        ids=ClientOrderIds(),
        fee_tick=fee_tick,
        clock=lambda: clock[0],
    )
    trader.recovery = recovery
    # Equivalent of a completed startup reconciliation.
    trader.sm.transition(State.SYNCING, "test")
    trader.sm.transition(State.FLAT, "test")
    trader.blocks.clear()
    trader.exch_pos = flat_position()
    trader.available_q = 100_000_000
    return Rig(trader, market, signals, executor, journal, recovery, limiter, clock)


async def settle() -> None:
    """Let spawned submission tasks run."""
    for _ in range(5):
        await asyncio.sleep(0)


async def open_long(rig: Rig) -> ActiveOrder:
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    entry = rig.order()
    rig.fill(entry, price=ENTRY_ASK)
    rig.trader.on_position(exchange_position(entry.size, ENTRY_ASK))
    rig.signals.signal = NO_SIGNAL
    assert rig.trader.sm.state is State.OPEN_LONG
    return entry


# ------------------------------------------------------------ full lifecycle


async def test_long_round_trip_exits_on_first_executable_green() -> None:
    rig = make_rig()
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    assert rig.trader.sm.state is State.ENTRY_PENDING
    await settle()
    entry = rig.order()
    assert entry.kind is OrderKind.ENTRY and not entry.is_ask and not entry.reduce_only
    assert not entry.market_order  # LIMIT + IOC with a hard price cap
    assert entry.size == 250_000_000 // ENTRY_ASK
    assert ENTRY_ASK <= entry.limit_price <= px(83702.3)  # at most 1 bps through the ask

    rig.fill(entry, price=ENTRY_ASK)
    assert rig.trader.sm.state is State.OPEN_LONG
    position = rig.trader.position
    assert position is not None and position.size == entry.size and position.cost_q == entry.size * ENTRY_ASK
    rig.trader.on_position(exchange_position(entry.size, ENTRY_ASK))

    # Not green yet: the executable bid is still below the entry price.
    rig.tick()
    assert rig.trader.sm.state is State.OPEN_LONG and len(rig.executor.submitted) == 1

    # The bid rises above entry: the whole position can be sold for > 0.01 USD net -> exit NOW.
    rig.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    rig.tick()
    assert rig.trader.sm.state is State.EXIT_PENDING
    exit_order = rig.order()
    assert exit_order.kind is OrderKind.EXIT and exit_order.is_ask and exit_order.reduce_only
    assert exit_order.size == entry.size and exit_order.reason == "PROFIT" and not exit_order.emergency
    assert exit_order.limit_price == px(83699.0) * (10_000_000 - 2000) // 10_000_000  # 2 bps below the bid
    await settle()

    rig.fill(exit_order, price=px(83699.0))
    assert rig.trader.sm.state is State.FLAT
    assert rig.trader.position is None
    trade = rig.journal.trades[0]
    assert trade["side"] == "LONG" and trade["exit_reason"] == "PROFIT"
    assert trade["realized_pnl_usd"] == pytest.approx(entry.size * (px(83699.0) - ENTRY_ASK) / 1_000_000)
    assert trade["realized_pnl_usd"] > 0.01
    assert rig.trader.metrics.daily.wins == 1 and rig.trader.metrics.daily.losses == 0
    assert trade["latency"]["signal_to_send_ms"] is not None


async def test_short_round_trip_is_symmetric() -> None:
    rig = make_rig()
    rig.signals.signal = SHORT_SIGNAL
    rig.tick()
    await settle()
    entry = rig.order()
    assert entry.is_ask and not entry.reduce_only
    assert entry.limit_price <= ENTRY_BID
    rig.fill(entry, price=ENTRY_BID)
    assert rig.trader.sm.state is State.OPEN_SHORT
    rig.signals.signal = NO_SIGNAL

    rig.market.set([(px(83684.0), sz(0.05))], [(px(83685.0), sz(0.05))])  # ask fell below our short entry
    rig.tick()
    exit_order = rig.order()
    assert not exit_order.is_ask and exit_order.reduce_only and exit_order.size == entry.size
    assert exit_order.limit_price >= px(83685.0)
    await settle()
    rig.fill(exit_order, price=px(83685.0))
    assert rig.trader.sm.state is State.FLAT
    assert rig.journal.trades[0]["side"] == "SHORT" and rig.journal.trades[0]["realized_pnl_usd"] > 0


async def test_green_requires_fees_to_be_covered() -> None:
    rig = make_rig(fee_tick=200)  # 2 bps taker fee each way
    entry = await open_long(rig)
    # +0.5 USD/BTC gross is nowhere near 4 bps of fees.
    rig.market.set([(px(83694.4), sz(0.05))], [(px(83695.0), sz(0.05))])
    rig.tick()
    assert rig.trader.sm.state is State.OPEN_LONG
    # ~5 bps above entry clears fees and the 0.01 USD minimum.
    rig.market.set([(px(83740.0), sz(0.05))], [(px(83741.0), sz(0.05))])
    rig.tick()
    assert rig.trader.sm.state is State.EXIT_PENDING and rig.order().size == entry.size


async def test_green_requires_depth_for_the_entire_position() -> None:
    rig = make_rig()
    entry = await open_long(rig)
    # A great bid, but only for a sliver of the position; the rest of the book is below entry.
    rig.market.set([(px(83720.0), sz(0.0001)), (px(83680.0), sz(1.0))], [(px(83721.0), sz(0.05))])
    rig.tick()
    assert rig.trader.sm.state is State.OPEN_LONG
    assert len(rig.executor.submitted) == 1 and entry.size > sz(0.0001)


# ------------------------------------------------- duplicate order prevention


async def test_many_identical_signals_produce_one_entry() -> None:
    rig = make_rig()
    rig.signals.signal = LONG_SIGNAL
    for _ in range(50):
        rig.tick(1)
    await settle()
    for _ in range(50):
        rig.tick(1)
    assert len(rig.executor.submitted) == 1
    assert rig.trader.sm.state is State.ENTRY_PENDING


async def test_many_green_events_produce_one_exit() -> None:
    rig = make_rig()
    await open_long(rig)
    rig.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    for _ in range(50):
        rig.tick(1)
    await settle()
    for _ in range(50):
        rig.tick(1)
    assert len(rig.executor.submitted) == 2  # one entry, one exit


async def test_no_signal_is_computed_while_a_position_is_open() -> None:
    rig = make_rig()
    await open_long(rig)
    before = rig.signals.computed
    for _ in range(20):
        rig.tick(1)
    assert rig.signals.computed == before


async def test_slow_acknowledgement_never_causes_a_second_entry() -> None:
    rig = make_rig()
    rig.executor.gate = asyncio.Event()  # the acknowledgement is delayed
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    for _ in range(20):
        rig.tick(5)
    assert len(rig.executor.submitted) == 1
    rig.executor.gate.set()
    await settle()
    assert len(rig.executor.submitted) == 1


# ----------------------------------------------------------- entry outcomes


@pytest.mark.parametrize(
    "prepare",
    [
        lambda rig: rig.trader.blocks.__setitem__("MARKET_DATA_STALE", "x"),
        lambda rig: setattr(rig.trader, "exch_pos", None),
        lambda rig: setattr(rig.trader, "exch_pos", exchange_position(100, ENTRY_ASK)),
        lambda rig: setattr(rig.trader, "exch_pos", flat_position(open_orders=1)),
        lambda rig: setattr(rig.market, "age_ms", 5000.0),
        lambda rig: setattr(rig.trader, "available_q", None),
        lambda rig: rig.book_invalid(),
        lambda rig: rig.limiter.penalize(30),
        lambda rig: setattr(rig.signals, "volatility_mbps", 50_000),
        lambda rig: rig.market.set(BIDS, [(px(83800.0), sz(0.05))]),
    ],
    ids=[
        "blocked",
        "exchange-unknown",
        "exchange-position",
        "resting-order",
        "stale-data",
        "balance-unknown",
        "book-invalid",
        "rate-limit",
        "volatility",
        "wide-spread",
    ],
)
async def test_entry_is_refused_when_any_precondition_fails(prepare: Any) -> None:
    rig = make_rig()
    rig.book_invalid = rig.trader.book.invalidate  # type: ignore[attr-defined]
    prepare(rig)
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    assert rig.executor.submitted == []
    assert rig.trader.sm.state is State.FLAT


async def test_unfilled_ioc_entry_returns_to_flat() -> None:
    rig = make_rig()
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    rig.fill(rig.order(), filled=0, status="canceled-too-much-slippage")
    assert rig.trader.sm.state is State.FLAT and rig.trader.position is None
    assert rig.trader.metrics.entries_missed == 1 and rig.journal.trades == []


async def test_rejected_entry_returns_to_flat_without_retry() -> None:
    rig = make_rig()
    rig.executor.results.append(rejected())
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    assert rig.trader.sm.state is State.FLAT
    assert len(rig.executor.submitted) == 1 and rig.recovery.recoveries == []


async def test_ambiguous_entry_is_reconciled_never_resent() -> None:
    rig = make_rig()
    rig.executor.results.append(ambiguous())
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    for _ in range(10):
        rig.tick(10)
    assert len(rig.executor.submitted) == 1
    assert rig.recovery.recoveries == ["ENTRY_SEND_AMBIGUOUS"]
    assert rig.trader.sm.state is State.ENTRY_PENDING  # no new entry until reconciliation resolves it


async def test_auth_rejection_halts_and_requests_process_exit() -> None:
    rig = make_rig()
    rig.executor.results.append(rejected(ErrorClass.AUTH_ERROR, 21120, "invalid signature"))
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    assert rig.trader.sm.state is State.HALTED and rig.trader.fatal_reason is not None
    rig.tick()
    assert len(rig.executor.submitted) == 1


# -------------------------------------------------------------- partial fills


async def test_partial_entry_fill_is_managed_at_its_actual_size() -> None:
    rig = make_rig()
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    entry = rig.order()
    rig.fill(entry, filled=120, price=ENTRY_ASK, status="canceled-too-much-slippage")
    assert rig.trader.sm.state is State.OPEN_LONG
    assert rig.trader.position is not None and rig.trader.position.size == 120
    rig.signals.signal = NO_SIGNAL
    # +5.1 USD/BTC on 0.0012 BTC is only 0.006 USD: below the 0.01 USD minimum, so no exit yet.
    rig.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    rig.tick()
    assert rig.trader.sm.state is State.OPEN_LONG
    rig.market.set([(px(83710.0), sz(0.05))], [(px(83711.0), sz(0.05))])
    rig.tick()
    assert rig.trader.sm.state is State.EXIT_PENDING
    assert rig.order().size == 120 and rig.order().reduce_only  # never the originally requested 298


async def test_non_terminal_partial_fill_activates_the_exit_engine() -> None:
    rig = make_rig()
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    entry = rig.order()
    rig.fill(entry, filled=100, price=ENTRY_ASK, status="open")
    assert rig.trader.sm.state is State.PARTIALLY_FILLED
    rig.market.set([(px(83610.0), sz(0.05))], [(px(83611.0), sz(0.05))])  # 10 bps against us
    rig.tick()
    assert rig.trader.sm.state is State.EXIT_PENDING
    assert rig.order().size == 100 and rig.order().reduce_only and rig.order().emergency


async def test_partial_exit_resends_only_the_remaining_quantity() -> None:
    rig = make_rig()
    entry = await open_long(rig)
    rig.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    rig.tick()
    await settle()
    first_exit = rig.order()
    rig.fill(first_exit, filled=100, price=px(83699.0), status="canceled-too-much-slippage")
    assert rig.trader.position is not None and rig.trader.position.size == entry.size - 100
    assert rig.trader.sm.state is State.EXIT_PENDING
    retry = rig.order()
    assert retry is not first_exit
    assert retry.size == entry.size - 100 and retry.reduce_only and retry.is_ask
    assert retry.client_order_index != first_exit.client_order_index
    await settle()
    rig.fill(retry, price=px(83699.0))
    assert rig.trader.sm.state is State.FLAT
    assert rig.journal.trades[0]["latency"]["exit_attempts"] == 2
    assert rig.trader.metrics.partial_fills == 1


async def test_missed_exit_escalates_to_emergency_slippage() -> None:
    rig = make_rig()
    await open_long(rig)
    rig.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    rig.tick()
    await settle()
    rig.fill(rig.order(), filled=0, status="canceled")
    assert not rig.order().emergency  # second attempt, still inside normal slippage
    await settle()
    rig.fill(rig.order(), filled=0, status="canceled")
    third = rig.order()
    assert third.emergency and third.limit_price == px(83699.0) * (10_000_000 - 30_000) // 10_000_000
    assert rig.trader.sm.state is State.EXIT_PENDING


async def test_exit_attempts_exhausted_hands_over_to_recovery() -> None:
    rig = make_rig(make_config(MAX_EXIT_ATTEMPTS="3"))
    await open_long(rig)
    rig.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    rig.tick()
    for _ in range(3):
        await settle()
        rig.fill(rig.order(), filled=0, status="canceled")
    assert rig.recovery.recoveries == ["EXIT_ATTEMPTS_EXHAUSTED"]


# ---------------------------------------------------------- hard protections


async def test_hard_adverse_move_flattens_with_emergency_slippage() -> None:
    rig = make_rig()
    entry = await open_long(rig)
    rig.market.set([(px(83610.0), sz(0.05))], [(px(83611.0), sz(0.05))])  # ~10 bps below entry
    rig.tick()
    order = rig.order()
    assert rig.trader.sm.state is State.EXIT_PENDING
    assert order.reason == "MAX_ADVERSE_MOVE" and order.emergency and order.reduce_only
    assert order.size == entry.size and order.market_order
    assert order.limit_price == px(83610.0) * (10_000_000 - 30_000) // 10_000_000
    await settle()
    rig.fill(order, price=px(83610.0))
    assert rig.trader.sm.state is State.FLAT
    assert rig.journal.trades[0]["realized_pnl_usd"] < 0 and rig.trader.metrics.daily.losses == 1


async def test_max_loss_usd_flattens() -> None:
    rig = make_rig(make_config(MAX_ADVERSE_MOVE_BPS="0", MAX_LOSS_USD="0.02"))
    await open_long(rig)
    rig.market.set([(px(83687.0), sz(0.05))], [(px(83688.0), sz(0.05))])  # about -0.0206 USD on 0.00298 BTC
    rig.tick()
    assert rig.order().reason == "MAX_LOSS" and rig.order().emergency


async def test_max_hold_time_flattens_on_market_event() -> None:
    rig = make_rig()
    await open_long(rig)
    rig.tick(4999)
    assert rig.trader.sm.state is State.OPEN_LONG
    rig.tick(2)
    assert rig.trader.sm.state is State.EXIT_PENDING
    assert rig.order().reason == "MAX_HOLD" and rig.order().reduce_only


async def test_max_hold_timer_fires_without_market_events(monkeypatch: pytest.MonkeyPatch) -> None:
    rig = make_rig(make_config(MAX_HOLD_MS="100"))
    await open_long(rig)
    rig.clock[0] += 150 * 1_000_000  # the injected clock moves; no market event arrives
    await asyncio.sleep(0.2)  # the real timer (armed for 100 ms) fires on the loop
    assert rig.trader.sm.state is State.EXIT_PENDING and rig.order().reason == "MAX_HOLD"


async def test_max_hold_with_stale_data_goes_to_recovery() -> None:
    rig = make_rig(make_config(MAX_HOLD_MS="100"))
    await open_long(rig)
    rig.market.age_ms = 9999.0
    rig.clock[0] += 150 * 1_000_000
    await asyncio.sleep(0.2)
    assert rig.recovery.recoveries == ["MAX_HOLD_NO_MARKET_DATA"]
    assert len(rig.executor.submitted) == 1  # no exit priced from stale data


async def test_stale_market_data_with_exposure_triggers_recovery() -> None:
    rig = make_rig()
    rig.trader.on_market_stale()
    assert rig.recovery.recoveries == []  # flat: entries are simply blocked by the health monitor
    await open_long(rig)
    rig.trader.on_market_stale()
    assert rig.recovery.recoveries == ["MARKET_DATA_STALE"]


async def test_exit_is_never_blocked_by_the_rate_limiter() -> None:
    rig = make_rig()
    await open_long(rig)
    rig.limiter.penalize(60)
    assert not rig.limiter.can_enter()
    rig.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    rig.tick()
    assert rig.trader.sm.state is State.EXIT_PENDING and len(rig.executor.submitted) == 2


async def test_strategy_exception_cannot_disable_loss_protection() -> None:
    rig = make_rig(make_config(EXIT_ON_SIGNAL_REVERSAL="true"))
    await open_long(rig)

    def broken(now_ns: int) -> Any:
        raise RuntimeError("signal engine failure")

    rig.signals.compute = broken  # type: ignore[method-assign]
    rig.market.set([(px(83610.0), sz(0.05))], [(px(83611.0), sz(0.05))])
    rig.tick()  # the hard-loss check runs before any signal code
    assert rig.order().reason == "MAX_ADVERSE_MOVE"


# -------------------------------------------------------------- exit outcomes


async def test_ambiguous_exit_goes_to_recovery_not_blind_retry() -> None:
    rig = make_rig()
    await open_long(rig)
    rig.executor.results.append(ambiguous())
    rig.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    rig.tick()
    await settle()
    assert rig.recovery.recoveries == ["EXIT_SEND_AMBIGUOUS"]
    assert len(rig.executor.submitted) == 2


async def test_reduce_only_rejection_means_our_view_is_wrong() -> None:
    rig = make_rig()
    await open_long(rig)
    rig.executor.results.append(rejected(ErrorClass.ORDER_REJECTED, 21732, "reduce only increases position"))
    rig.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    rig.tick()
    await settle()
    assert rig.recovery.recoveries == ["EXIT_REDUCE_ONLY_REJECTED"]


async def test_rejected_exit_is_retried_with_a_new_order() -> None:
    rig = make_rig()
    entry = await open_long(rig)
    rig.executor.results.append(rejected(ErrorClass.NONCE_ERROR, 21104, "invalid nonce"))
    rig.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    rig.tick()
    await settle()
    assert len(rig.executor.submitted) == 3
    assert rig.order().size == entry.size and rig.order().reduce_only
    assert rig.order().client_order_index != rig.order(-2).client_order_index


async def test_unresolved_order_times_out_into_recovery() -> None:
    rig = make_rig(make_config(ORDER_RESOLVE_TIMEOUT_MS="500"))
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    await asyncio.sleep(0.6)  # accepted, but no fill/cancel update ever arrives
    assert rig.recovery.recoveries == ["ORDER_UNRESOLVED"]
    assert len(rig.executor.submitted) == 1


async def test_profit_only_mode_caps_the_exit_at_break_even() -> None:
    rig = make_rig(make_config(PROFIT_EXIT_MODE="profit_only"))
    entry = await open_long(rig)
    rig.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    rig.tick()
    order = rig.order()
    assert not order.market_order and order.reduce_only  # LIMIT + IOC
    assert order.limit_price > ENTRY_ASK  # can only fill at a price that is still green
    assert (order.limit_price - ENTRY_ASK) * entry.size > 10_000
    await settle()
    # The price ran away during the exchange delay: nothing filled and the book is no longer green.
    rig.market.set([(px(83692.0), sz(0.05))], [(px(83693.0), sz(0.05))])
    rig.fill(order, filled=0, status="canceled")
    assert rig.trader.sm.state is State.OPEN_LONG  # back to monitoring, protections still armed
    assert rig.order() is order  # no chase order was sent at a loss
    rig.market.set([(px(83610.0), sz(0.05))], [(px(83611.0), sz(0.05))])
    rig.tick()
    assert rig.order().reason == "MAX_ADVERSE_MOVE" and rig.order().market_order


# -------------------------------------------------- exchange state as authority


async def test_fill_reports_from_both_feeds_are_not_double_counted() -> None:
    rig = make_rig()
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    entry = rig.order()
    half = entry.size // 2
    rig.trader.on_trade_fill(TradeFill(1, entry.client_order_index, 1, half, ENTRY_ASK, True, 0))
    assert rig.trader.sm.state is State.PARTIALLY_FILLED
    rig.trader.on_trade_fill(TradeFill(2, entry.client_order_index, 1, entry.size - half, ENTRY_ASK, True, 0))
    assert rig.trader.sm.state is State.OPEN_LONG  # a completely filled IOC is finished
    rig.fill(entry, price=ENTRY_ASK)  # the order update arrives afterwards
    rig.trader.on_trade_fill(TradeFill(2, entry.client_order_index, 1, entry.size - half, ENTRY_ASK, True, 0))
    assert rig.trader.position is not None and rig.trader.position.size == entry.size
    assert rig.recovery.resyncs == []


async def test_observed_fee_above_assumption_is_adopted_immediately() -> None:
    rig = make_rig()
    rig.trader.on_trade_fill(TradeFill(1, 999, 1, 10, ENTRY_ASK, True, 250))
    assert rig.trader.fee_tick == 250
    rig.trader.on_trade_fill(TradeFill(2, 999, 1, 10, ENTRY_ASK, True, 100))
    assert rig.trader.fee_tick == 250  # never lowered automatically


async def test_position_mismatch_triggers_reconciliation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(strategy, "MISMATCH_GRACE_S", 0.05)
    rig = make_rig()
    rig.trader.on_position(exchange_position(300, ENTRY_ASK))  # exchange shows a position we do not know
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    assert rig.executor.submitted == []  # entries stop immediately
    await asyncio.sleep(0.1)
    assert rig.recovery.resyncs == ["STATE_MISMATCH"]


async def test_transient_position_lag_does_not_trigger_reconciliation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(strategy, "MISMATCH_GRACE_S", 0.05)
    rig = make_rig()
    entry = await open_long(rig)
    rig.trader.on_position(flat_position())  # a late, older position message
    rig.trader.on_position(exchange_position(entry.size, ENTRY_ASK))  # corrected within the grace period
    await asyncio.sleep(0.1)
    assert rig.recovery.resyncs == []


async def test_fill_for_an_abandoned_order_requests_reconciliation() -> None:
    rig = make_rig()
    rig.executor.results.append(rejected())
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    await settle()
    abandoned = rig.order()
    assert rig.trader.sm.state is State.FLAT
    rig.signals.signal = NO_SIGNAL
    rig.fill(abandoned, price=ENTRY_ASK)  # the exchange filled it after all
    assert rig.trader.position is None  # local state is not guessed
    assert rig.recovery.resyncs == ["ORPHAN_FILL"]


async def test_account_stream_loss_blocks_entries_and_escalates_with_exposure() -> None:
    rig = make_rig(make_config(ACCOUNT_STREAM_GRACE_MS="50"))
    rig.trader.on_account_stream_down()
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    assert rig.executor.submitted == [] and rig.recovery.recoveries == []
    rig.trader.on_account_stream_up()
    assert rig.recovery.resyncs == ["ACCOUNT_STREAM_UP"]  # never assumes FLAT after a reconnect
    rig.trader.exch_pos = flat_position()
    await open_long(rig)
    rig.trader.on_account_stream_down()
    await asyncio.sleep(0.1)
    assert rig.recovery.recoveries == ["ACCOUNT_STREAM_DOWN"]


async def test_foreign_resting_order_blocks_entries() -> None:
    rig = make_rig()
    rig.trader.on_order_update(OrderUpdate(12345, 1, 1, True, "open", 0, 0, 100, False))
    assert "FOREIGN_ORDERS" in rig.trader.blocks
    rig.signals.signal = LONG_SIGNAL
    rig.tick()
    assert rig.executor.submitted == []


async def test_immediate_reentry_after_confirmed_flat() -> None:
    rig = make_rig()
    await open_long(rig)
    rig.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    rig.tick()
    await settle()
    rig.fill(rig.order(), price=px(83699.0))
    assert rig.trader.sm.state is State.FLAT
    rig.signals.signal = LONG_SIGNAL
    rig.tick(1)
    assert len(rig.executor.submitted) == 2  # exchange has not confirmed zero exposure yet
    rig.exchange_flat()
    rig.tick(1)  # no cooldown: the next qualifying event trades again
    assert len(rig.executor.submitted) == 3 and rig.trader.sm.state is State.ENTRY_PENDING


async def test_state_snapshot_is_persisted_on_every_transition() -> None:
    rig = make_rig()
    await open_long(rig)
    states = [snapshot["state"] for snapshot in rig.journal.states]
    assert states[-2:] == ["ENTRY_PENDING", "OPEN_LONG"]
    assert rig.journal.states[-1]["position"]["size"] > 0
    assert rig.journal.states[-1]["last_client_order_index"] > 0
