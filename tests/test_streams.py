"""Stream message handling with real-shaped Lighter messages. No sockets are opened."""

from __future__ import annotations

import time
from typing import Any

from scalper.account_stream import AccountStream
from scalper.market_data import Backoff, MarketDataStream
from scalper.metrics import Metrics
from scalper.orderbook import OrderBook
from scalper.position import ExchangePosition, OrderUpdate, TradeFill
from scalper.signals import SignalEngine

from .conftest import BTC, make_config

ACCOUNT = 12345  # LIGHTER_ACCOUNT_INDEX in the test configuration


def level(price: str, size: str) -> dict[str, str]:
    return {"price": price, "size": size}


SNAPSHOT = {
    "channel": "order_book:1",
    "order_book": {
        "asks": [level("83693.9", "0.00238"), level("83694.2", "0.04000")],
        "bids": [level("83691.1", "0.00020"), level("83691.0", "0.00500")],
        "nonce": 100,
        "begin_nonce": 0,
    },
    "timestamp": 1791365635786,
    "type": "subscribed/order_book",
}


def delta(begin: int, nonce: int, bids: list[dict[str, str]], asks: list[dict[str, str]]) -> dict[str, Any]:
    return {
        "channel": "order_book:1",
        "order_book": {"asks": asks, "bids": bids, "nonce": nonce, "begin_nonce": begin},
        "timestamp": 1791365635829,
        "type": "update/order_book",
    }


def ticker(nonce: int, bid: tuple[str, str], ask: tuple[str, str]) -> dict[str, Any]:
    return {
        "channel": "ticker:1",
        "nonce": nonce,
        "ticker": {"s": "BTC", "a": level(*ask), "b": level(*bid)},
        "timestamp": 1791365635798,
        "type": "update/ticker",
    }


class MarketRig:
    def __init__(self) -> None:
        cfg = make_config()
        self.book = OrderBook()
        self.signals = SignalEngine(cfg.weights, 0.6, 10, 2.0, 1000)
        self.wakes = 0
        self.stream = MarketDataStream(cfg, BTC, self.book, self.signals, Metrics(), self._wake)

    def _wake(self) -> None:
        self.wakes += 1

    def feed(self, message: dict[str, Any]) -> bool:
        return self.stream._handle(message["type"], message, time.monotonic_ns())


# ---------------------------------------------------------------- market data


def test_snapshot_then_delta_maintains_the_book() -> None:
    rig = MarketRig()
    assert rig.stream.staleness_ms(time.monotonic_ns()) == float("inf")  # nothing received yet
    assert rig.feed(SNAPSHOT)
    assert rig.book.valid and rig.book.nonce == 100
    assert rig.stream.top_bids[0] == (836911, 20) and rig.stream.top_asks[0] == (836939, 238)
    assert rig.feed(
        delta(100, 120, [level("83691.1", "0.00000"), level("83690.5", "0.01000")], [level("83693.9", "0.01911")])
    )
    assert rig.stream.top_bids == [(836910, 500), (836905, 1000)]
    assert rig.stream.top_asks[0] == (836939, 1911)
    assert rig.wakes == 2
    assert rig.stream.staleness_ms(time.monotonic_ns()) < 1000


def test_sequence_gap_requests_a_resync() -> None:
    rig = MarketRig()
    rig.feed(SNAPSHOT)
    assert not rig.feed(delta(105, 120, [], []))  # begin_nonce does not continue from 100
    assert not rig.book.valid


def test_ticker_overlay_is_applied_only_when_newer() -> None:
    rig = MarketRig()
    rig.feed(SNAPSHOT)
    wakes = rig.wakes
    assert rig.feed(ticker(90, ("83690.0", "0.00100"), ("83695.0", "0.00100")))  # older than the book
    assert rig.stream.top_bids[0] == (836911, 20) and rig.wakes == wakes
    assert rig.feed(ticker(110, ("83691.0", "0.00400"), ("83694.2", "0.03000")))
    assert rig.stream.top_bids[0] == (836910, 400)  # 83691.1 is gone according to the ticker
    assert rig.stream.top_asks[0] == (836942, 3000)
    assert rig.wakes == wakes + 1
    # The next book delta supersedes the overlay.
    rig.feed(delta(100, 130, [level("83691.1", "0.00000"), level("83691.0", "0.00450")], [level("83693.9", "0.00000")]))
    assert rig.stream.top_bids[0] == (836910, 450) and rig.stream.top_asks[0] == (836942, 4000)


def test_trades_feed_the_flow_window_with_the_aggressor_side() -> None:
    rig = MarketRig()
    rig.feed(SNAPSHOT)
    message = {
        "channel": "trade:1",
        "type": "update/trade",
        "trades": [
            {"size": "0.00250", "price": "83691.1", "is_maker_ask": True},  # aggressor bought
            {"size": "0.00100", "price": "83691.0", "is_maker_ask": False},  # aggressor sold
        ],
        "liquidation_trades": [{"size": "0.00050", "price": "83691.0", "is_maker_ask": False}],
    }
    assert rig.feed(message)
    buys, sells, count = rig.signals._flow(time.monotonic_ns(), 1000)
    assert (buys, sells, count) == (250, 150, 3)


def test_trade_history_in_the_subscription_reply_is_ignored() -> None:
    rig = MarketRig()
    rig.feed(SNAPSHOT)
    history = {"type": "subscribed/trade", "trades": [{"size": "5.00000", "price": "80000.0", "is_maker_ask": True}]}
    assert rig.stream._handle("subscribed/trade", history, time.monotonic_ns())
    assert rig.signals._flow(time.monotonic_ns(), 10_000) == (0, 0, 0)


def test_feed_lag_is_measured_against_the_best_seen_offset() -> None:
    rig = MarketRig()
    now_ms = time.time_ns() // 1_000_000
    rig.stream._update_lag(now_ms - 40, time.monotonic_ns())  # baseline: 40 ms clock offset + latency
    assert rig.stream.feed_lag_ms == 0
    rig.stream._update_lag(now_ms - 340, time.monotonic_ns())  # we are processing 300 ms late
    assert 290 <= rig.stream.feed_lag_ms <= 320


def test_disconnect_invalidates_the_book_and_resets_signals() -> None:
    rig = MarketRig()
    rig.feed(SNAPSHOT)
    rig.stream._on_disconnect()
    assert not rig.book.valid and rig.stream.top_bids == [] and not rig.stream.connected
    assert not rig.signals.ready(time.monotonic_ns() + 10**12)


def test_backoff_is_exponential_and_bounded() -> None:
    backoff = Backoff(0.25, 10.0)
    delays = [backoff.next() for _ in range(12)]
    assert all(0 < d <= 10.0 for d in delays)
    assert delays[0] <= 0.25 and delays[-1] >= 5.0
    backoff.reset()
    assert backoff.next() <= 0.25


# ------------------------------------------------------------- account stream


class Recorder:
    def __init__(self) -> None:
        self.orders: list[OrderUpdate] = []
        self.fills: list[TradeFill] = []
        self.positions: list[ExchangePosition] = []
        self.balances: list[int] = []

    def on_order_update(self, update: OrderUpdate) -> None:
        self.orders.append(update)

    def on_trade_fill(self, fill: TradeFill) -> None:
        self.fills.append(fill)

    def on_position(self, position: ExchangePosition) -> None:
        self.positions.append(position)

    def on_balance(self, available_q: int) -> None:
        self.balances.append(available_q)

    def on_account_stream_up(self) -> None: ...

    def on_account_stream_down(self) -> None: ...


def account_rig() -> tuple[AccountStream, Recorder]:
    recorder = Recorder()
    return AccountStream(make_config(), BTC, lambda: "token", Metrics(), recorder), recorder


def run(stream: AccountStream, message: dict[str, Any]) -> None:
    for event in stream._parse(message["type"], message):
        event()


ORDER = {
    "order_index": 844421350816976,
    "client_order_index": 199098906046313,
    "market_index": 1,
    "initial_base_amount": "0.00298",
    "remaining_base_amount": "0.00000",
    "filled_base_amount": "0.00298",
    "filled_quote_amount": "249.407822",
    "is_ask": False,
    "reduce_only": False,
    "status": "filled",
}


def test_order_updates_are_parsed_and_filtered_by_market() -> None:
    stream, recorder = account_rig()
    message = {
        "type": "update/account_orders",
        "channel": "account_orders:1",
        "orders": {"1": [ORDER], "0": [{**ORDER, "market_index": 0, "client_order_index": 5}]},
    }
    run(stream, message)
    assert len(recorder.orders) == 1
    update = recorder.orders[0]
    assert update.client_order_index == 199098906046313 and update.status == "filled"
    assert update.filled == 298 and update.filled_quote_q == 249_407_822


def test_subscription_acknowledgements_are_tracked() -> None:
    stream, recorder = account_rig()
    run(stream, {"type": "subscribed/account_orders", "orders": {}})
    run(stream, {"type": "subscribed/account_all_positions", "positions": {}})
    assert stream._acked == {"account_orders", "account_all_positions"}
    # A position snapshot without our market means: flat.
    assert len(recorder.positions) == 1 and recorder.positions[0].signed_size == 0


def test_position_updates_for_other_markets_are_ignored() -> None:
    stream, recorder = account_rig()
    run(
        stream,
        {"type": "update/account_all_positions", "positions": {"0": {"market_id": 0, "sign": 1, "position": "1.0"}}},
    )
    assert recorder.positions == []
    run(
        stream,
        {
            "type": "update/account_all_positions",
            "positions": {
                "1": {
                    "market_id": 1,
                    "sign": -1,
                    "position": "0.00298",
                    "avg_entry_price": "83691.1",
                    "initial_margin_fraction": "4.00",
                    "margin_mode": 0,
                    "open_order_count": 0,
                }
            },
        },
    )
    assert recorder.positions[0].signed_size == -298 and recorder.positions[0].imf == 400


def test_own_fills_are_extracted_from_account_trades() -> None:
    stream, recorder = account_rig()
    trade = {
        "trade_id": 7,
        "market_id": 1,
        "size": "0.00298",
        "price": "83693.9",
        "is_maker_ask": True,
        "ask_account_id": 999,
        "bid_account_id": ACCOUNT,
        "bid_client_id": 199098906046313,
        "taker_fee": 280,
    }
    run(stream, {"type": "update/account_all_trades", "trades": {"1": [trade], "0": [{**trade, "market_id": 0}]}})
    assert len(recorder.fills) == 1
    fill = recorder.fills[0]
    assert fill.client_order_index == 199098906046313 and fill.is_taker and fill.fee_tick == 280
    # The historical trades in a subscription reply are not fills of this session.
    run(stream, {"type": "subscribed/account_all_trades", "trades": {"1": [trade]}})
    assert len(recorder.fills) == 1


def test_balance_updates() -> None:
    stream, recorder = account_rig()
    run(stream, {"type": "update/user_stats", "stats": {"available_balance": "127.737206", "collateral": "129.1"}})
    assert recorder.balances == [127_737_206]
    run(stream, {"type": "update/user_stats", "stats": {}})
    assert len(recorder.balances) == 1


def test_unknown_and_error_messages_produce_no_events() -> None:
    stream, recorder = account_rig()
    for message in ({"type": "error", "error": {"code": 20013}}, {"type": "update/height", "height": 1}, {"type": ""}):
        assert stream._parse(str(message["type"]), message) == []
    assert recorder.orders == [] and recorder.positions == []
