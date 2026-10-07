"""Startup reconciliation, reconnect resync and crash recovery.

Real ``Trader`` + ``Executor`` + ``Reconciler`` against the simulated exchange in ``fakes.py``.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import pytest

from scalper import execution, reconciliation
from scalper.config import Config
from scalper.errors import ApiError, ErrorClass
from scalper.execution import Executor
from scalper.metrics import Metrics
from scalper.orderbook import OrderBook
from scalper.position import ClientOrderIds, OrderUpdate
from scalper.rate_limits import RateLimiter, limits_for_tier
from scalper.reconciliation import Action, Reconciler, decide, leverage_matches
from scalper.risk import Limits
from scalper.signals import NO_SIGNAL
from scalper.state_machine import State
from scalper.strategy import Trader

from .conftest import BTC, make_config, px, sz
from .fakes import (
    ASKS,
    BIDS,
    LONG_SIGNAL,
    FakeClient,
    FakeJournal,
    FakeMarket,
    FakeSignals,
    ambiguous,
    exchange_position,
    flat_position,
    no_sleep,
)

ENTRY_ASK = px(83693.9)


@pytest.fixture(autouse=True)
def instant_waits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(execution, "_sleep", no_sleep)
    monkeypatch.setattr(reconciliation, "_sleep", no_sleep)


# ------------------------------------------------------------ pure decision


def bot_order(ids: ClientOrderIds) -> OrderUpdate:
    return OrderUpdate(ids.next(), 1, 1, True, "open", 0, 0, 100, False)


FOREIGN_ORDER = OrderUpdate(555, 2, 1, False, "open", 0, 0, 100, False)


def test_decide_flat_exchange_resumes() -> None:
    decision = decide(flat_position(), [], local_signed_size=None, existing_position_action="manage")
    assert decision.action is Action.RESUME_FLAT and decision.bot_orders == () and decision.foreign_orders == 0


@pytest.mark.parametrize(
    ("action", "expected"),
    [("manage", Action.ADOPT_POSITION), ("flatten", Action.FLATTEN), ("halt", Action.HALT)],
)
def test_decide_existing_position_follows_configuration(action: str, expected: Action) -> None:
    for local in (None, 0):
        decision = decide(
            exchange_position(-300, ENTRY_ASK), [], local_signed_size=local, existing_position_action=action
        )
        assert decision.action is expected and decision.reason == "existing_position"


def test_decide_matching_local_position_continues() -> None:
    decision = decide(exchange_position(300, ENTRY_ASK), [], local_signed_size=300, existing_position_action="halt")
    assert decision.action is Action.ADOPT_POSITION and decision.reason == "matches_local"


def test_decide_size_or_side_mismatch_flattens_regardless_of_configuration() -> None:
    for exchange_size in (150, -300, 301):
        decision = decide(
            exchange_position(exchange_size, ENTRY_ASK), [], local_signed_size=300, existing_position_action="halt"
        )
        assert decision.action is Action.FLATTEN and decision.reason == "size_mismatch"


def test_decide_separates_bot_orders_from_foreign_orders() -> None:
    ids = ClientOrderIds()
    stale = bot_order(ids)
    decision = decide(
        flat_position(), [stale, FOREIGN_ORDER], local_signed_size=None, existing_position_action="manage"
    )
    assert decision.bot_orders == (stale.client_order_index,) and decision.foreign_orders == 1


def test_leverage_match_requires_exact_fraction_and_mode() -> None:
    assert leverage_matches(flat_position(400, 0), 400, 0)
    assert not leverage_matches(flat_position(500, 0), 400, 0)
    assert not leverage_matches(flat_position(400, 1), 400, 0)
    assert not leverage_matches(flat_position(None, None), 400, 0)


# --------------------------------------------------------------- full system


@dataclass
class System:
    trader: Trader
    reconciler: Reconciler
    client: FakeClient
    journal: FakeJournal
    market: FakeMarket
    signals: FakeSignals
    ids: ClientOrderIds

    def order_kinds(self) -> list[str]:
        return [entry["kind"] for entry in self.client.signed]

    def streams_up(self) -> None:
        """What the market stream and health monitor do once data flows."""
        self.trader.blocks.pop("ACCOUNT_STREAM_DOWN", None)
        self.trader.blocks.pop("MARKET_DATA_STALE", None)


def make_system(cfg: Config | None = None) -> System:
    cfg = cfg or make_config()
    client = FakeClient(BTC)
    ids = ClientOrderIds()
    metrics = Metrics()
    executor = Executor(cfg, BTC, client, metrics, ids)  # type: ignore[arg-type]
    book = OrderBook()
    market = FakeMarket(book)
    market.set(BIDS, ASKS)
    signals = FakeSignals()
    journal = FakeJournal()
    trader = Trader(
        cfg=cfg,
        meta=BTC,
        limits=Limits.from_config(cfg, BTC),
        book=book,
        market=market,
        signals=signals,  # type: ignore[arg-type]
        executor=executor,
        limiter=RateLimiter(limits_for_tier("premium"), cfg.max_entries_per_minute),
        metrics=metrics,
        journal=journal,  # type: ignore[arg-type]
        ids=ids,
        fee_tick=0,
    )
    reconciler = Reconciler(trader, client, executor, cfg)  # type: ignore[arg-type]
    trader.recovery = reconciler
    return System(trader, reconciler, client, journal, market, signals, ids)


async def settle() -> None:
    for _ in range(10):
        await asyncio.sleep(0)


async def started(cfg: Config | None = None) -> System:
    system = make_system(cfg)
    await system.reconciler.startup()
    system.streams_up()
    assert system.trader.sm.state is State.FLAT and not system.trader.blocks
    return system


async def open_long(system: System) -> int:
    """Trade into a long through the real executor; the simulated exchange fills it."""
    system.signals.signal = LONG_SIGNAL
    system.trader.on_market(time.monotonic_ns())
    await settle()
    system.signals.signal = NO_SIGNAL
    order = system.trader.entry_order
    assert order is not None
    system.trader.on_order_update(system.client.finished_orders[order.client_order_index])
    assert system.trader.sm.state is State.OPEN_LONG
    return order.size


# ------------------------------------------------------ startup reconciliation


async def test_nothing_trades_before_startup_reconciliation() -> None:
    system = make_system()
    system.signals.signal = LONG_SIGNAL
    system.trader.on_market(time.monotonic_ns())
    await settle()
    assert system.trader.sm.state is State.STARTING and system.client.sent == []


async def test_startup_when_flat_confirms_leverage_without_sending_anything() -> None:
    system = make_system()
    system.client.position = flat_position(imf=400, margin_mode=0)
    await system.reconciler.startup()
    assert system.trader.sm.state is State.FLAT
    assert system.reconciler.leverage_confirmed
    assert "SYNCING" not in system.trader.blocks and "LEVERAGE_UNCONFIRMED" not in system.trader.blocks
    assert system.client.sent == []
    assert system.trader.exch_pos is not None and system.trader.exch_pos.signed_size == 0
    assert system.trader.available_q == 100_000_000


async def test_startup_sets_and_reads_back_the_configured_leverage() -> None:
    system = make_system()
    system.client.position = flat_position(imf=500, margin_mode=0)  # exchange is at 20x, config wants 25x
    await system.reconciler.startup()
    assert system.order_kinds() == ["leverage"]
    assert system.client.signed[0]["imf"] == 400 and system.client.signed[0]["margin_mode"] == 0
    assert system.reconciler.leverage_confirmed and system.trader.sm.state is State.FLAT


async def test_startup_sets_leverage_when_the_account_has_no_btc_record() -> None:
    system = make_system()
    system.client.position = flat_position(imf=None, margin_mode=None)
    system.client.has_market_entry = False
    await system.reconciler.startup()
    assert system.order_kinds() == ["leverage"] and system.reconciler.leverage_confirmed


async def test_unconfirmed_leverage_blocks_entries() -> None:
    system = make_system()
    system.client.position = flat_position(imf=500, margin_mode=0)
    system.client.on_leverage = None  # the exchange accepts the transaction but never reflects it
    await system.reconciler.startup()
    system.streams_up()
    assert system.trader.sm.state is State.FLAT
    assert not system.reconciler.leverage_confirmed and "LEVERAGE_UNCONFIRMED" in system.trader.blocks
    system.signals.signal = LONG_SIGNAL
    system.trader.on_market(time.monotonic_ns())
    await settle()
    assert system.order_kinds() == ["leverage"]  # no order was ever sent


async def test_startup_adopts_and_manages_an_existing_position() -> None:
    system = make_system()
    system.client.position = exchange_position(300, ENTRY_ASK)
    await system.reconciler.startup()
    system.streams_up()
    position = system.trader.position
    assert system.trader.sm.state is State.OPEN_LONG
    assert position is not None and position.adopted and position.size == 300
    assert position.cost_q == 300 * ENTRY_ASK
    assert system.client.sent == []  # no second position is ever opened on top of it
    # The exit engine now owns it: the first executable green closes it, reduce-only.
    system.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    system.trader.on_market(time.monotonic_ns())
    await settle()
    exit_order = system.client.signed[-1]["order"]
    assert exit_order.reduce_only and exit_order.is_ask and exit_order.size == 300
    system.trader.on_order_update(system.client.finished_orders[exit_order.client_order_index])
    assert system.trader.sm.state is State.FLAT
    assert system.journal.trades[0]["adopted"] == 1 and system.journal.trades[0]["exit_reason"] == "PROFIT"


async def test_startup_can_flatten_an_existing_position() -> None:
    system = make_system(make_config(EXISTING_POSITION_ACTION="flatten"))
    system.client.position = exchange_position(-300, px(83691.1))
    await system.reconciler.startup()
    assert system.trader.sm.state is State.FLAT
    assert system.client.position.signed_size == 0
    close = system.client.signed[0]["order"]
    assert close.reduce_only and not close.is_ask and close.size == 300


async def test_startup_can_halt_on_an_existing_position() -> None:
    system = make_system(make_config(EXISTING_POSITION_ACTION="halt"))
    system.client.position = exchange_position(300, ENTRY_ASK)
    await system.reconciler.startup()
    assert system.trader.sm.state is State.HALTED
    assert system.client.sent == [] and system.client.position.signed_size == 300
    assert system.trader.fatal_reason is None  # the service stays up, idle and visible


async def test_startup_cancels_stale_bot_orders_and_flags_foreign_ones() -> None:
    system = make_system()
    stale = bot_order(system.ids)
    system.client.active_orders = [stale, FOREIGN_ORDER]
    await system.reconciler.startup()
    assert ("cancel", stale.client_order_index) in [(s["kind"], s.get("order_ref")) for s in system.client.signed]
    assert system.client.active_orders == [FOREIGN_ORDER]
    assert "FOREIGN_ORDERS" in system.trader.blocks  # entries stay blocked; nothing foreign is touched


async def test_foreign_orders_are_cancelled_only_when_configured() -> None:
    system = make_system(make_config(CANCEL_FOREIGN_ORDERS="true"))
    system.client.active_orders = [FOREIGN_ORDER]
    await system.reconciler.startup()
    assert "cancel_all" in system.order_kinds() and system.client.active_orders == []
    assert "FOREIGN_ORDERS" not in system.trader.blocks


async def test_failed_startup_read_keeps_entries_blocked_and_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(reconciliation, "SYNC_RETRY_S", 0.02)
    system = make_system()
    system.client.account_errors.append(ApiError("timeout", error_class=ErrorClass.NETWORK_ERROR, ambiguous=True))
    await system.reconciler.startup()
    assert system.trader.sm.state is State.SYNCING  # never assumes FLAT without an authoritative answer
    await asyncio.sleep(0.1)
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.FLAT


async def test_authentication_failure_at_startup_is_fatal() -> None:
    system = make_system()
    system.client.account_errors.append(ApiError("invalid auth", error_class=ErrorClass.AUTH_ERROR, http_status=401))
    await system.reconciler.startup()
    assert system.trader.sm.state is State.HALTED and system.trader.fatal_reason is not None


# ---------------------------------------------------------- runtime resyncs


async def test_reconnect_resync_blocks_entries_until_the_exchange_answers() -> None:
    system = await started()
    system.client.account_gate = asyncio.Event()
    system.trader.on_account_stream_up()  # stream came back: do NOT assume FLAT
    await settle()
    assert system.trader.sm.state is State.SYNCING
    system.signals.signal = LONG_SIGNAL
    system.trader.on_market(time.monotonic_ns())
    assert system.client.sent == []
    system.client.account_gate.set()
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.FLAT


async def test_periodic_check_runs_in_the_background_while_flat() -> None:
    system = await started()
    system.client.account_gate = asyncio.Event()
    system.reconciler.request_resync("PERIODIC")
    await settle()
    assert system.trader.sm.state is State.FLAT  # trading is not paused for a routine check
    system.client.account_gate.set()
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.FLAT


async def test_stale_reconciliation_result_is_discarded() -> None:
    system = await started()
    system.client.account_gate = asyncio.Event()
    system.reconciler.request_resync("PERIODIC")
    await settle()
    size = await open_long(system)  # trading moved on while the read was in flight
    system.client.account_gate.set()
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.OPEN_LONG
    assert system.trader.position is not None and system.trader.position.size == size
    assert not system.trader.position.adopted


async def test_unexpected_exchange_position_while_flat_is_adopted_under_manage() -> None:
    system = await started()
    system.client.position = exchange_position(300, ENTRY_ASK)  # e.g. a late fill of an abandoned entry
    system.reconciler.request_resync("STATE_MISMATCH")
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.OPEN_LONG
    assert system.trader.position is not None and system.trader.position.adopted


async def test_matching_position_after_reconnect_continues_undisturbed() -> None:
    system = await started()
    size = await open_long(system)
    sent_before = len(system.client.sent)
    system.trader.on_account_stream_up()
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.OPEN_LONG
    assert system.trader.position is not None and system.trader.position.size == size
    assert len(system.client.sent) == sent_before


async def test_local_position_but_exchange_flat_is_abandoned_not_invented() -> None:
    system = await started()
    await open_long(system)
    system.client.position = flat_position()  # closed elsewhere (manual close, liquidation)
    system.reconciler.request_resync("STATE_MISMATCH")
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.FLAT and system.trader.position is None
    assert "TRADE_UNRESOLVED" in system.journal.event_kinds()
    assert system.journal.trades == []  # no P&L is fabricated for a trade we cannot explain


async def test_position_size_disagreement_flattens_from_exchange_truth() -> None:
    system = await started()
    await open_long(system)
    system.client.position = exchange_position(150, ENTRY_ASK)
    system.reconciler.request_resync("STATE_MISMATCH")
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.FLAT
    assert system.client.position.signed_size == 0
    close = system.client.signed[-1]["order"]
    assert close.reduce_only and close.size == 150  # sized from the exchange, not from local state


# ----------------------------------------------------------------- recovery


async def test_recovery_flattens_and_books_the_trade_from_confirmed_fills() -> None:
    system = await started()
    size = await open_long(system)
    system.reconciler.start_recovery("MARKET_DATA_STALE")
    assert system.trader.sm.state is State.RECOVERY
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.FLAT and system.trader.position is None
    assert system.client.position.signed_size == 0
    trade = system.journal.trades[0]
    assert trade["exit_reason"] == "MARKET_DATA_STALE"
    # Bought at the ask, flattened at the bid: the loss is the spread, taken from the fill record.
    assert trade["realized_pnl_usd"] == pytest.approx(size * (px(83691.1) - ENTRY_ASK) / 1_000_000)
    assert system.journal.event_kinds().count("RECOVERY_START") == 1
    assert "RECOVERY_DONE" in system.journal.event_kinds()


async def test_recovery_requests_are_single_flight() -> None:
    system = await started()
    await open_long(system)
    for reason in ("A", "B", "C"):
        system.reconciler.start_recovery(reason)
    await system.reconciler.wait_idle()
    closes = [s for s in system.client.signed if s["kind"] == "order" and s["order"].reduce_only]
    assert len(closes) == 1 and system.journal.event_kinds().count("RECOVERY_START") == 1


async def test_ambiguous_entry_that_never_arrived_recovers_to_flat() -> None:
    system = await started()
    system.client.results.append(ambiguous())  # the send times out; the order never reaches the exchange
    system.signals.signal = LONG_SIGNAL
    system.trader.on_market(time.monotonic_ns())
    system.signals.signal = NO_SIGNAL
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.FLAT and system.trader.position is None
    assert [s["kind"] for s in system.client.signed] == ["order"]  # the entry was never re-sent
    assert system.trader.entry_order is None


async def test_ambiguous_entry_that_did_fill_is_flattened_by_recovery() -> None:
    system = await started()
    system.signals.signal = LONG_SIGNAL
    system.trader.on_market(time.monotonic_ns())
    system.signals.signal = NO_SIGNAL
    await settle()
    assert system.client.position.signed_size > 0  # it is on the exchange, but we never saw a fill
    system.reconciler.start_recovery("ORDER_UNRESOLVED")
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.FLAT
    assert system.client.position.signed_size == 0
    close = system.client.signed[-1]["order"]
    assert close.reduce_only
    # Both legs were read back from the exchange, so the trade is booked from real fills.
    trade = system.journal.trades[0]
    assert trade["exit_reason"] == "ORDER_UNRESOLVED"
    assert trade["realized_pnl_usd"] == pytest.approx(close.size * (px(83691.1) - ENTRY_ASK) / 1_000_000)
    assert "TRADE_UNRESOLVED" not in system.journal.event_kinds()


async def test_close_fill_arriving_before_the_entry_is_known_is_still_booked() -> None:
    system = await started()
    system.signals.signal = LONG_SIGNAL
    system.trader.on_market(time.monotonic_ns())
    system.signals.signal = NO_SIGNAL
    await settle()
    assert system.trader.position is None  # the entry filled on the exchange; we never heard
    # From now on the account stream works: the flatten order's fill arrives immediately,
    # before recovery has learned about the entry fill.
    system.client.stream = system.trader.on_order_update
    system.reconciler.start_recovery("ORDER_UNRESOLVED")
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.FLAT and system.trader.position is None
    assert len(system.journal.trades) == 1
    assert system.journal.trades[0]["realized_pnl_usd"] < 0  # paid the spread, nothing invented
    assert "TRADE_UNRESOLVED" not in system.journal.event_kinds()


async def test_exit_filled_during_a_stream_outage_is_booked_from_the_exchange_record() -> None:
    system = await started()
    size = await open_long(system)
    system.client.bbo = (px(83699.0), px(83700.0))
    system.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    system.trader.on_market(time.monotonic_ns())  # GREEN -> exit sent and filled on the exchange
    await settle()
    assert system.trader.sm.state is State.EXIT_PENDING  # ...but its fill never reached us
    assert system.client.position.signed_size == 0
    system.reconciler.start_recovery("ORDER_UNRESOLVED")
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.FLAT
    closes = [s for s in system.client.signed if s["kind"] == "order" and s["order"].reduce_only]
    assert len(closes) == 1  # recovery found the account flat and sent nothing more
    trade = system.journal.trades[0]
    assert trade["exit_reason"] == "PROFIT>ORDER_UNRESOLVED"
    assert trade["realized_pnl_usd"] == pytest.approx(size * (px(83699.0) - ENTRY_ASK) / 1_000_000)
    assert system.trader.metrics.daily.wins == 1


async def test_recovery_keeps_trying_until_the_exchange_is_flat() -> None:
    system = await started()
    await open_long(system)
    # More failed closes than one flatten run allows: the recovery loop must start another run.
    system.client.fill_fraction.extend([0.0] * (execution.FLATTEN_MAX_ATTEMPTS + 2))
    system.reconciler.start_recovery("TEST")
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.FLAT and system.client.position.signed_size == 0


async def test_recovery_halts_when_credentials_are_rejected() -> None:
    system = await started()
    await open_long(system)
    system.client.account_errors.append(ApiError("invalid auth", error_class=ErrorClass.AUTH_ERROR, http_status=401))
    system.reconciler.start_recovery("TEST")
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.HALTED and system.trader.fatal_reason is not None


# ---------------------------------------------------------- operator flatten


async def test_operator_flatten_closes_an_open_position_and_books_it() -> None:
    system = await started()
    size = await open_long(system)
    system.reconciler.manual_flatten()
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.FLAT and system.client.position.signed_size == 0
    close = system.client.signed[-1]["order"]
    assert close.reduce_only and close.is_ask and close.size == size
    assert system.journal.trades[0]["exit_reason"] == "MANUAL_FLATTEN"


async def test_operator_flatten_while_flat_checks_the_exchange_and_sends_nothing() -> None:
    system = await started()
    reads = system.client.account_reads
    sent = len(system.client.sent)
    system.reconciler.manual_flatten()
    await system.reconciler.wait_idle()
    assert system.trader.sm.state is State.FLAT
    assert system.client.account_reads > reads  # flat was confirmed by the exchange, not assumed
    assert len(system.client.sent) == sent


async def test_operator_flatten_works_even_when_trading_is_halted() -> None:
    system = make_system(make_config(EXISTING_POSITION_ACTION="halt"))
    system.client.position = exchange_position(300, ENTRY_ASK)
    await system.reconciler.startup()
    assert system.trader.sm.state is State.HALTED and system.client.sent == []
    system.reconciler.manual_flatten()  # the operator asking for flat always wins
    await system.reconciler.wait_idle()
    assert system.client.position.signed_size == 0
    close = system.client.signed[-1]["order"]
    assert close.reduce_only and close.size == 300
    assert system.trader.sm.state is State.HALTED  # still halted: flattening does not resume trading
    assert "MANUAL_FLATTEN" in system.journal.event_kinds()


# ------------------------------------------------- resting (maker) entries


async def maker_system() -> System:
    system = await started(make_config(ENTRY_MODE="maker", MAKER_REST_MS=100))
    system.client.stream = system.trader.on_order_update
    return system


async def rest_entry(system: System) -> int:
    system.signals.signal = LONG_SIGNAL
    system.trader.on_market(time.monotonic_ns())
    await settle()
    order = system.trader.entry_order
    assert order is not None and order.post_only and system.trader.sm.state is State.ENTRY_PENDING
    assert system.client.position.open_orders == 1 and system.client.position.signed_size == 0
    return order.client_order_index


async def test_resting_entry_is_cancelled_on_the_exchange_after_the_rest_time() -> None:
    system = await maker_system()
    coi = await rest_entry(system)
    system.signals.signal = NO_SIGNAL
    await asyncio.sleep(0.25)
    await settle()
    assert system.order_kinds()[-2:] == ["order", "cancel"]
    assert system.client.signed[-1]["order_ref"] == coi
    assert system.client.active_orders == [] and system.client.position.signed_size == 0
    assert system.trader.sm.state is State.FLAT and system.journal.trades == []


async def test_resting_entry_filled_by_a_counterparty_is_exited_reduce_only() -> None:
    system = await maker_system()
    coi = await rest_entry(system)
    system.signals.signal = NO_SIGNAL
    fill = system.client.fill_resting(coi)
    assert system.trader.sm.state is State.OPEN_LONG
    system.trader.on_position(system.client.position)
    system.client.bbo = (px(83699.0), px(83700.0))
    system.market.set([(px(83699.0), sz(0.05))], [(px(83700.0), sz(0.05))])
    system.trader.on_market(time.monotonic_ns())  # GREEN
    await settle()
    assert system.trader.sm.state is State.FLAT and system.client.position.signed_size == 0
    assert "cancel" not in system.order_kinds()[-2:]
    trade = system.journal.trades[0]
    assert trade["exit_reason"] == "PROFIT" and trade["realized_pnl_usd"] > 0
    assert trade["avg_entry"] == pytest.approx(fill.filled_quote_q / fill.filled / 10)


async def test_recovery_with_a_resting_entry_cancels_it_and_returns_flat() -> None:
    system = await maker_system()
    await rest_entry(system)
    system.signals.signal = NO_SIGNAL
    system.reconciler.start_recovery("ACCOUNT_STREAM_DOWN")
    await system.reconciler.wait_idle()
    assert system.client.active_orders == [] and system.client.position.signed_size == 0
    assert system.trader.sm.state is State.FLAT and system.trader.entry_order is None
    assert system.journal.trades == []  # nothing filled, nothing invented


async def test_resting_order_left_by_a_previous_run_is_cancelled_at_startup() -> None:
    system = make_system(make_config(ENTRY_MODE="maker"))
    stale = bot_order(system.ids)
    system.client.active_orders = [stale]
    system.client.position = flat_position(open_orders=1)
    await system.reconciler.startup()
    assert system.client.active_orders == []
    assert (
        system.client.signed[0]["kind"] == "cancel" and system.client.signed[0]["order_ref"] == stale.client_order_index
    )
