"""Public market-data stream for the BTC perpetual.

One persistent WebSocket carrying exactly three channels:

* ``order_book/{market}`` - snapshot + ~50 ms deltas (the authority for depth)
* ``ticker/{market}``     - best bid/offer, faster than the book (optional overlay)
* ``trade/{market}``      - public trades with aggressor side

Each message is applied to the in-memory book synchronously as it is read and
then the strategy is *woken* (not called). When several messages are already
buffered they are all applied before the strategy runs once, so decisions are
always made on the latest state rather than on a backlog.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import Callable
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from .config import Config
from .errors import ErrorClass
from .metrics import Metrics
from .orderbook import OrderBook
from .precision import MarketMeta, to_scaled
from .signals import SignalEngine
from .wire import parse_levels

log = logging.getLogger("scalper.market")

TOP_LEVELS = 50  # levels per side exposed to the signal engine and the depth walks
PING_INTERVAL_S = 5.0
OFFSET_RESET_NS = 120 * 1_000_000_000
_PONG = '{"type":"pong"}'
_WS_ERRORS = (OSError, WebSocketException, asyncio.TimeoutError)


class Backoff:
    """Exponential backoff with jitter and an upper bound."""

    def __init__(self, base: float = 0.25, cap: float = 10.0) -> None:
        self._base = base
        self._cap = cap
        self._attempt = 0

    def next(self) -> float:
        delay = min(self._cap, self._base * float(2**self._attempt))
        self._attempt = min(self._attempt + 1, 16)
        return delay * (0.5 + random.random() * 0.5)

    def reset(self) -> None:
        self._attempt = 0


class MarketDataStream:
    """Maintains the local book, feeds the signal engine and wakes the strategy."""

    def __init__(
        self,
        cfg: Config,
        meta: MarketMeta,
        book: OrderBook,
        signals: SignalEngine,
        metrics: Metrics,
        wake: Callable[[], None],
    ) -> None:
        self._cfg = cfg
        self._meta = meta
        self._book = book
        self._signals = signals
        self._metrics = metrics
        self._wake = wake
        self._ws: ClientConnection | None = None
        self._stop = False
        self.connected = False
        self.top_bids: list[tuple[int, int]] = []
        self.top_asks: list[tuple[int, int]] = []
        self.last_book_ns = 0  # monotonic receipt time of the last order_book message
        self.event_ns = 0  # monotonic receipt time of the last message that changed the view
        self.feed_lag_ms = 0
        self._min_offset_ms: int | None = None
        self._min_offset_at_ns = 0
        self.connected_at = 0.0  # monotonic time of the current connection

    # ---------------------------------------------------------------- public

    def staleness_ms(self, now_ns: int) -> float:
        """Age of market data: time since the last order-book message plus estimated feed lag.

        Keeps counting from the last message across a disconnect, so a brief reconnect is
        judged against MARKET_DATA_STALE_MS like any other gap. (Whether the book is usable
        at all is a separate check: ``OrderBook.valid``.)
        """
        if not self.last_book_ns:
            return float("inf")
        return (now_ns - self.last_book_ns) / 1_000_000 + self.feed_lag_ms

    def ping_ms(self) -> float | None:
        ws = self._ws
        if ws is None or not self.connected or ws.latency <= 0:
            return None
        return ws.latency * 1000.0

    def request_reconnect(self) -> None:
        """Drop the socket; the run loop reconnects and resubscribes."""
        ws = self._ws
        if ws is not None and ws.transport is not None:
            ws.transport.abort()

    def stop(self) -> None:
        self._stop = True
        self.request_reconnect()

    async def run(self) -> None:
        """Connect, subscribe and process messages forever with bounded exponential backoff."""
        backoff = Backoff()
        while not self._stop:
            try:
                async with connect(
                    self._cfg.ws_url,
                    ping_interval=PING_INTERVAL_S,
                    ping_timeout=PING_INTERVAL_S,
                    open_timeout=10,
                    close_timeout=2,
                    max_size=16 * 1024 * 1024,
                    max_queue=512,
                    compression=None,
                ) as ws:
                    self._ws = ws
                    self.connected = True
                    self.connected_at = time.monotonic()
                    await self._session(ws, backoff)
            except asyncio.CancelledError:
                raise
            except _WS_ERRORS as exc:
                log.warning("MARKET_WS_DISCONNECTED error=%s", type(exc).__name__)
            finally:
                self._on_disconnect()
            if self._stop:
                return
            self._metrics.count_reconnect("market")
            await asyncio.sleep(backoff.next())

    # -------------------------------------------------------------- internal

    def _on_disconnect(self) -> None:
        self.connected = False
        self._ws = None
        self._book.invalidate()
        self._signals.reset()
        self.top_bids = []
        self.top_asks = []
        self._wake()

    async def _subscribe(self, ws: ClientConnection) -> None:
        market = self._meta.market_id
        channels = [f"order_book/{market}", f"trade/{market}"]
        if self._cfg.use_ticker_stream:
            channels.append(f"ticker/{market}")
        for channel in channels:
            await ws.send(json.dumps({"type": "subscribe", "channel": channel}))

    async def _session(self, ws: ClientConnection, backoff: Backoff) -> None:
        async for raw in ws:
            recv_ns = time.monotonic_ns()
            try:
                message = json.loads(raw)
                kind = message.get("type")
                if kind == "connected":
                    await self._subscribe(ws)
                    continue
                if kind == "ping":
                    await ws.send(_PONG)
                    continue
                healthy = self._handle(kind, message, recv_ns)
            except (KeyError, TypeError, ValueError, AttributeError) as exc:
                self._metrics.count_error(ErrorClass.MARKET_DATA_ERROR)
                log.error("MARKET_DATA_ERROR error=%s", type(exc).__name__)
                healthy = False
            if not healthy:
                # Sequence gap, crossed book or unparseable message: resync from a fresh snapshot.
                log.warning("MARKET_RESYNC reason=book_integrity")
                return
            if self._book.valid:
                backoff.reset()

    def _handle(self, kind: str, message: dict[str, Any], recv_ns: int) -> bool:
        """Apply one message. Returns False when the book must be resynchronised."""
        if kind == "update/order_book":
            book = message["order_book"]
            meta = self._meta
            ok = self._book.apply_delta(
                parse_levels(book["bids"], meta),
                parse_levels(book["asks"], meta),
                int(book["begin_nonce"]),
                int(book["nonce"]),
            )
            if not ok:
                return False
            self.last_book_ns = recv_ns
            self._publish(message, recv_ns)
        elif kind == "update/ticker" or kind == "subscribed/ticker":
            ticker = message["ticker"]
            pd = self._meta.price_decimals
            sd = self._meta.size_decimals
            bid, ask = ticker["b"], ticker["a"]
            applied = self._book.apply_bbo(
                to_scaled(bid["price"], pd),
                to_scaled(bid["size"], sd),
                to_scaled(ask["price"], pd),
                to_scaled(ask["size"], sd),
                int(message["nonce"]),
            )
            if applied:
                self._publish(message, recv_ns)
        elif kind == "update/trade":
            self._on_trades(message, recv_ns)
        elif kind == "subscribed/order_book":
            book = message["order_book"]
            meta = self._meta
            self._book.apply_snapshot(
                parse_levels(book["bids"], meta), parse_levels(book["asks"], meta), int(book["nonce"])
            )
            if not self._book.valid:
                return False
            self._signals.reset()
            self.last_book_ns = recv_ns
            self._publish(message, recv_ns)
            bids, asks = self._book.level_counts()
            log.info("MARKET_BOOK_SYNCED bids=%d asks=%d nonce=%d", bids, asks, self._book.nonce)
        elif kind == "error":
            log.warning("MARKET_WS_ERROR message=%s", str(message)[:300])
        return True

    def _publish(self, message: dict[str, Any], recv_ns: int) -> None:
        """Refresh the shared top-of-book view, update signals and wake the strategy."""
        book = self._book
        self.top_bids = book.top_bids(TOP_LEVELS)
        self.top_asks = book.top_asks(TOP_LEVELS)
        self._signals.on_book(recv_ns, self.top_bids, self.top_asks)
        self.event_ns = recv_ns
        stamp = message.get("timestamp")
        if stamp:
            self._update_lag(int(stamp), recv_ns)
        self._wake()

    def _on_trades(self, message: dict[str, Any], recv_ns: int) -> None:
        size_decimals = self._meta.size_decimals
        on_trade = self._signals.on_trade
        seen = False
        for key in ("trades", "liquidation_trades"):
            for trade in message.get(key) or ():
                # is_maker_ask=True: the resting order was an ask, so the aggressor bought.
                on_trade(recv_ns, to_scaled(trade["size"], size_decimals), bool(trade["is_maker_ask"]))
                seen = True
        if seen:
            self.event_ns = recv_ns
            self._wake()

    def _update_lag(self, exchange_ms: int, recv_ns: int) -> None:
        """Estimate how far behind the exchange clock our processing runs.

        The smallest (local - exchange) offset seen recently approximates clock
        offset plus network latency; anything above it is backlog or delay.
        """
        offset = time.time_ns() // 1_000_000 - exchange_ms
        best = self._min_offset_ms
        if best is None or offset < best or recv_ns - self._min_offset_at_ns > OFFSET_RESET_NS:
            self._min_offset_ms = offset
            self._min_offset_at_ns = recv_ns
            self.feed_lag_ms = 0
        else:
            self.feed_lag_ms = offset - best
