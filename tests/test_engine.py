"""The whole service wired together, running against the simulated exchange.

Real Engine, Trader, Executor, Reconciler, HealthMonitor, Journal, SignalEngine and OrderBook.
Only the two network edges are replaced: the WebSocket ``run`` loops are swapped for in-process
feeders and the REST client is ``FakeClient``. No socket is opened and no order leaves the process.
"""

from __future__ import annotations

import asyncio
import signal
import sqlite3
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from scalper import execution, reconciliation
from scalper.account_stream import AccountStream
from scalper.control import Command, is_paused, set_paused, submit_command
from scalper.engine import Engine, Session
from scalper.lighter_client import AccountTier
from scalper.market_data import MarketDataStream
from scalper.metrics import Metrics, utc_date
from scalper.persistence import Journal
from scalper.position import OrderUpdate
from scalper.rate_limits import RateLimiter, limits_for_tier
from scalper.state_machine import State

from .conftest import BTC, make_config
from .fakes import FakeClient, no_sleep
from .test_strategy import make_rig


def level(price: str, size: str) -> dict[str, str]:
    return {"price": price, "size": size}


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class ServiceClient(FakeClient):
    """FakeClient plus the housekeeping calls the engine and health monitor make."""

    def __init__(self) -> None:
        super().__init__(BTC)
        self.last_request_monotonic = time.monotonic()
        self.closed = False

    def auth_token(self, *, force: bool = False) -> str:
        return "token"

    def token_age_remaining(self) -> float:
        return 6 * 3600.0

    async def ping(self) -> float:
        return 1.0

    async def fetch_market_meta(self, symbol: str) -> Any:
        return BTC

    async def close(self) -> None:
        self.closed = True


async def market_feed(self: MarketDataStream) -> None:
    """Stands in for the public WebSocket: one snapshot, then a heartbeat of deltas."""
    self.connected = True
    self.connected_at = time.monotonic()
    snapshot = {
        "order_book": {
            # Heavier asks than bids: a persistent downward-pressure signal.
            "asks": [level("83693.9", "0.50000"), level("83694.2", "0.40000")],
            "bids": [level("83691.1", "0.05000"), level("83691.0", "0.05000")],
            "nonce": 100,
            "begin_nonce": 0,
        },
        "timestamp": now_ms(),
    }
    self._handle("subscribed/order_book", snapshot, time.monotonic_ns())
    nonce = 100
    while True:
        await asyncio.sleep(0.02)
        delta = {
            "order_book": {"asks": [], "bids": [], "begin_nonce": nonce, "nonce": nonce + 1},
            "timestamp": now_ms(),
        }
        nonce += 1
        self._handle("update/order_book", delta, time.monotonic_ns())


def account_feed_for(client: ServiceClient) -> Any:
    async def account_feed(self: AccountStream) -> None:
        """Stands in for the authenticated WebSocket: pushes fills and positions as they happen."""
        events = self._events

        def push(update: OrderUpdate) -> None:
            events.on_order_update(update)
            events.on_position(client.position)

        client.stream = push
        self.synced = True
        events.on_account_stream_up()
        events.on_position(client.position)
        events.on_balance(BTC.usd_to_q(client.balance))
        await asyncio.Event().wait()

    return account_feed


@pytest.fixture
def restore_signals() -> Iterator[None]:
    saved = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
    yield
    for signum, handler in saved.items():
        signal.signal(signum, handler)


async def test_service_trades_and_shuts_down_flat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, restore_signals: None
) -> None:
    monkeypatch.setattr(execution, "_sleep", no_sleep)
    monkeypatch.setattr(reconciliation, "_sleep", no_sleep)
    client = ServiceClient()
    monkeypatch.setattr(MarketDataStream, "run", market_feed)
    monkeypatch.setattr(AccountStream, "run", account_feed_for(client))

    cfg = make_config(
        DATA_DIR=tmp_path,
        LOG_DIR=tmp_path / "logs",
        ENTRY_SCORE_THRESHOLD="0.05",
        SIGNAL_WARMUP_MS="1000",
        MAX_HOLD_MS="250",
        MAX_ENTRIES_PER_MINUTE="2",
    )
    metrics = Metrics()
    limiter = RateLimiter(limits_for_tier("standard"), cfg.max_entries_per_minute)
    session = Session(cfg, client, BTC, AccountTier("standard", 0, 0), limiter, metrics)  # type: ignore[arg-type]
    journal = Journal(tmp_path)
    journal.start()
    engine = Engine(cfg)

    serving = asyncio.create_task(engine._serve(session, journal))
    await asyncio.sleep(3.0)
    snapshot = Journal.read_status(tmp_path)
    engine.request_stop()
    trader = await asyncio.wait_for(serving, 30)
    for task in engine._tasks.values():
        task.cancel()
    await asyncio.gather(*engine._tasks.values(), return_exceptions=True)
    journal.stop()

    # Startup reconciliation ran first and confirmed leverage; then the service traded live.
    orders = [entry["order"] for entry in client.signed if entry["kind"] == "order"]
    entries = [o for o in orders if not o.reduce_only]
    closes = [o for o in orders if o.reduce_only]
    assert 1 <= len(entries) <= 2  # MAX_ENTRIES_PER_MINUTE caps it; nothing forces a trade count
    assert all(o.is_ask for o in entries)  # the simulated book only ever signals SHORT
    assert all(not o.is_ask for o in closes) and len(closes) >= len(entries)
    assert [tx.nonce for tx in client.sent] == list(range(100, 100 + len(client.sent)))  # strictly sequential

    # It ended flat, on the exchange and locally, and booked only confirmed trades.
    assert client.position.signed_size == 0
    assert trader.sm.state is State.FLAT and trader.position is None
    rows = journal.load_day_totals(utc_date())
    assert len(rows) == len(entries)
    assert all(net < 0 for _, _, net, _ in rows)  # sold the bid, bought back the ask: the spread
    db = sqlite3.connect(tmp_path / "scalper.db")
    reasons = [row[0] for row in db.execute("select exit_reason from trades")]
    saved_state = db.execute("select value from kv where key='state'").fetchone()[0]
    db.close()
    assert all(reason.startswith("MAX_HOLD") or "SHUTDOWN" in reason for reason in reasons)
    assert '"state":"FLAT"' in saved_state

    # The health monitor published a coherent status snapshot while running.
    assert snapshot is not None
    assert snapshot["status"] == "LIVE" and snapshot["market_ws"] == "connected"
    assert snapshot["account_ws"] == "connected" and snapshot["leverage_confirmed"] is True
    assert snapshot["bid"] == 83691.1 and snapshot["ask"] == 83693.9
    assert "MARKET_DATA_STALE" not in snapshot["blocks"] and "SYNCING" not in snapshot["blocks"]
    assert snapshot["rate_limit"]["tier"] == "standard"
    assert (tmp_path / "trades.csv").exists()


async def test_service_refuses_to_trade_when_the_exchange_already_holds_a_position_and_halt_is_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, restore_signals: None
) -> None:
    monkeypatch.setattr(execution, "_sleep", no_sleep)
    monkeypatch.setattr(reconciliation, "_sleep", no_sleep)
    client = ServiceClient()
    client.position = client.position.__class__(300, 836939, 0, 0, 400, 0, 0)  # an existing long
    monkeypatch.setattr(MarketDataStream, "run", market_feed)
    monkeypatch.setattr(AccountStream, "run", account_feed_for(client))
    cfg = make_config(
        DATA_DIR=tmp_path,
        LOG_DIR=tmp_path / "logs",
        ENTRY_SCORE_THRESHOLD="0.05",
        SIGNAL_WARMUP_MS="1000",
        EXISTING_POSITION_ACTION="halt",
    )
    session = Session(
        cfg,
        client,
        BTC,
        AccountTier("standard", 0, 0),
        RateLimiter(limits_for_tier("standard"), 5),
        Metrics(),  # type: ignore[arg-type]
    )
    journal = Journal(tmp_path)
    journal.start()
    engine = Engine(cfg)
    serving = asyncio.create_task(engine._serve(session, journal))
    await asyncio.sleep(2.0)
    engine.request_stop()
    trader = await asyncio.wait_for(serving, 30)
    for task in engine._tasks.values():
        task.cancel()
    await asyncio.gather(*engine._tasks.values(), return_exceptions=True)
    journal.stop()
    assert trader.sm.state is State.HALTED
    assert client.sent == []  # nothing was sent: no entry, no close, no leverage change
    assert client.position.signed_size == 300


async def wait_until(predicate: Any, limit_s: float = 8.0) -> bool:
    deadline = time.monotonic() + limit_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.05)
    return False


async def test_operator_commands_reach_the_running_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, restore_signals: None
) -> None:
    monkeypatch.setattr(execution, "_sleep", no_sleep)
    monkeypatch.setattr(reconciliation, "_sleep", no_sleep)
    monkeypatch.delenv("INVOCATION_ID", raising=False)  # not under systemd: "stop" is honoured
    client = ServiceClient()
    monkeypatch.setattr(MarketDataStream, "run", market_feed)
    monkeypatch.setattr(AccountStream, "run", account_feed_for(client))
    submit_command(tmp_path, "pause")  # queued before this start: must be discarded, never run
    cfg = make_config(
        DATA_DIR=tmp_path,
        LOG_DIR=tmp_path / "logs",
        ENTRY_SCORE_THRESHOLD="0.05",
        SIGNAL_WARMUP_MS="1000",
        MAX_HOLD_MS="60000",  # the position stays open until the operator acts
        MAX_ENTRIES_PER_MINUTE="5",
    )
    session = Session(
        cfg,
        client,
        BTC,
        AccountTier("standard", 0, 0),
        RateLimiter(limits_for_tier("standard"), 5),
        Metrics(),  # type: ignore[arg-type]
    )
    journal = Journal(tmp_path)
    journal.start()
    engine = Engine(cfg)
    serving = asyncio.create_task(engine._serve(session, journal))
    try:
        assert await wait_until(lambda: client.position.signed_size != 0), "the service never opened a position"
        assert not is_paused(tmp_path)  # the stale command was purged at startup
        entries = lambda: [e for e in client.signed if e["kind"] == "order" and not e["order"].reduce_only]  # noqa: E731
        assert len(entries()) == 1

        # Flatten: closes the position and leaves entries paused.
        submit_command(tmp_path, "flatten")
        assert await wait_until(lambda: client.position.signed_size == 0), "flatten did not close the position"
        assert await wait_until(lambda: (Journal.read_status(tmp_path) or {}).get("state") == "FLAT")
        assert is_paused(tmp_path)
        await asyncio.sleep(1.5)
        assert len(entries()) == 1, "a paused bot must not enter"
        status = Journal.read_status(tmp_path) or {}
        assert status["control"]["paused"] is True and "PAUSED" in status["blocks"]
        assert status["control"]["last_command"]["command"] == "flatten"

        # Resume: it trades again.
        submit_command(tmp_path, "resume")
        assert await wait_until(lambda: len(entries()) == 2), "the bot did not resume entering"
        assert not is_paused(tmp_path)

        # Stop: a graceful shutdown that flattens first, without any signal being sent.
        submit_command(tmp_path, "stop")
        trader = await asyncio.wait_for(serving, 30)
    finally:
        serving.cancel()
        for task in engine._tasks.values():
            task.cancel()
        await asyncio.gather(serving, *engine._tasks.values(), return_exceptions=True)
        journal.stop()
    assert trader.sm.state is State.FLAT and client.position.signed_size == 0
    closes = [e["order"] for e in client.signed if e["kind"] == "order" and e["order"].reduce_only]
    assert len(closes) == 2 and all(order.reduce_only for order in closes)


async def test_command_handling_and_the_systemd_guard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rig = make_rig(make_config(DATA_DIR=tmp_path))
    engine = Engine(rig.trader.cfg)
    reconciler: Any = rig.recovery

    def send(name: str) -> dict[str, str]:
        engine._on_command(Command(f"id-{name}", name, time.time()), rig.trader, reconciler)
        assert rig.trader.last_command is not None
        return rig.trader.last_command

    assert send("pause")["result"] == "ok" and "PAUSED" in rig.trader.blocks
    assert send("resume")["result"] == "ok" and "PAUSED" not in rig.trader.blocks
    send("flatten")
    assert "PAUSED" in rig.trader.blocks and rig.recovery.recoveries == ["MANUAL_FLATTEN"]

    monkeypatch.setenv("INVOCATION_ID", "systemd-unit-invocation")
    assert send("stop")["result"].startswith("ignored") and not engine._stop.is_set()
    monkeypatch.delenv("INVOCATION_ID")
    assert send("stop")["result"] == "ok" and engine._stop.is_set()
    assert [kind for kind, _ in rig.journal.events].count("CONTROL") == 5


async def test_a_paused_bot_starts_paused_and_trades_only_after_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, restore_signals: None
) -> None:
    monkeypatch.setattr(execution, "_sleep", no_sleep)
    monkeypatch.setattr(reconciliation, "_sleep", no_sleep)
    monkeypatch.delenv("INVOCATION_ID", raising=False)
    client = ServiceClient()
    monkeypatch.setattr(MarketDataStream, "run", market_feed)
    monkeypatch.setattr(AccountStream, "run", account_feed_for(client))
    set_paused(tmp_path, True)  # paused from the panel while the bot was stopped
    cfg = make_config(
        DATA_DIR=tmp_path,
        LOG_DIR=tmp_path / "logs",
        ENTRY_SCORE_THRESHOLD="0.05",
        SIGNAL_WARMUP_MS="1000",
        MAX_HOLD_MS="60000",
        MAX_ENTRIES_PER_MINUTE="5",
    )
    session = Session(
        cfg,
        client,
        BTC,
        AccountTier("standard", 0, 0),
        RateLimiter(limits_for_tier("standard"), 5),
        Metrics(),  # type: ignore[arg-type]
    )
    journal = Journal(tmp_path)
    journal.start()
    engine = Engine(cfg)
    serving = asyncio.create_task(engine._serve(session, journal))
    try:
        assert await wait_until(lambda: (Journal.read_status(tmp_path) or {}).get("state") == "FLAT")
        await asyncio.sleep(2.5)  # well past the signal warm-up: it would have entered by now
        orders = [e for e in client.signed if e["kind"] == "order"]
        assert orders == [], "a bot that was paused before it started must not trade"
        assert "PAUSED" in (Journal.read_status(tmp_path) or {})["blocks"]
        submit_command(tmp_path, "resume")
        assert await wait_until(lambda: client.position.signed_size != 0), "it did not trade after resume"
        engine.request_stop()
        await asyncio.wait_for(serving, 30)
    finally:
        serving.cancel()
        for task in engine._tasks.values():
            task.cancel()
        await asyncio.gather(serving, *engine._tasks.values(), return_exceptions=True)
        journal.stop()
    assert client.position.signed_size == 0
