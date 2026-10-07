"""Authenticated account stream: order updates, fills, position and balance.

A second persistent WebSocket (separate from market data so a burst of book
updates can never delay a fill) subscribed with an auth token to:

* ``account_orders/{market}/{account}``  - order lifecycle and cumulative fills
* ``account_all_trades/{account}``       - individual fills with fee ticks
* ``account_all_positions/{account}``    - authoritative position
* ``user_stats/{account}``               - available balance

An accepted order is never treated as filled: fills are only ever taken from
these messages (or from REST during recovery).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from functools import partial
from typing import Any, Protocol

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from .config import Config
from .errors import ApiError, ErrorClass
from .market_data import Backoff
from .metrics import Metrics
from .position import ExchangePosition, OrderUpdate, TradeFill
from .precision import MarketMeta
from .wire import find_market_position, iter_objects, parse_order, parse_position, parse_trade

log = logging.getLogger("scalper.account")

PING_INTERVAL_S = 5.0
SUBSCRIBE_TIMEOUT_S = 8.0
_PONG = '{"type":"pong"}'
_WS_ERRORS = (OSError, WebSocketException, asyncio.TimeoutError)
_REQUIRED = frozenset({"account_orders", "account_all_positions"})


class AccountEvents(Protocol):
    """Callbacks invoked synchronously on the event loop for each parsed account event."""

    def on_order_update(self, update: OrderUpdate) -> None: ...

    def on_trade_fill(self, fill: TradeFill) -> None: ...

    def on_position(self, position: ExchangePosition) -> None: ...

    def on_balance(self, available_q: int) -> None: ...

    def on_account_stream_up(self) -> None: ...

    def on_account_stream_down(self) -> None: ...


class AccountStream:
    """Maintains the authenticated subscriptions and dispatches parsed events."""

    def __init__(
        self,
        cfg: Config,
        meta: MarketMeta,
        token_provider: Callable[[], str],
        metrics: Metrics,
        events: AccountEvents,
    ) -> None:
        self._cfg = cfg
        self._meta = meta
        self._token_provider = token_provider
        self._metrics = metrics
        self._events = events
        self._ws: ClientConnection | None = None
        self._stop = False
        self._acked: set[str] = set()
        self._subscribe_deadline = 0.0
        self.synced = False  # required subscriptions acknowledged on the current connection
        self.down_since_ns = time.monotonic_ns()
        self.connected_at = 0.0

    # ---------------------------------------------------------------- public

    def ping_ms(self) -> float | None:
        ws = self._ws
        if ws is None or not self.synced or ws.latency <= 0:
            return None
        return ws.latency * 1000.0

    def request_reconnect(self) -> None:
        """Drop the socket (e.g. to renew the auth token); the run loop resubscribes."""
        ws = self._ws
        if ws is not None and ws.transport is not None:
            ws.transport.abort()

    def stop(self) -> None:
        self._stop = True
        self.request_reconnect()

    async def run(self) -> None:
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
                    self._acked = set()
                    await self._session(ws, backoff)
            except asyncio.CancelledError:
                raise
            except _WS_ERRORS as exc:
                log.warning("ACCOUNT_WS_DISCONNECTED error=%s", type(exc).__name__)
            except ApiError as exc:
                self._metrics.count_error(exc.error_class)
                log.error("ACCOUNT_WS_AUTH_ERROR error=%s", exc)
            finally:
                self._on_disconnect()
            if self._stop:
                return
            self._metrics.count_reconnect("account")
            await asyncio.sleep(backoff.next())

    # -------------------------------------------------------------- internal

    def _on_disconnect(self) -> None:
        self._ws = None
        if self.synced:
            self.synced = False
            self.down_since_ns = time.monotonic_ns()
            self._events.on_account_stream_down()

    async def _subscribe(self, ws: ClientConnection) -> None:
        token = self._token_provider()
        account = self._cfg.account_index
        market = self._meta.market_id
        for channel in (
            f"account_orders/{market}/{account}",
            f"account_all_positions/{account}",
            f"account_all_trades/{account}",
            f"user_stats/{account}",
        ):
            await ws.send(json.dumps({"type": "subscribe", "channel": channel, "auth": token}))
        self._subscribe_deadline = time.monotonic() + SUBSCRIBE_TIMEOUT_S

    async def _session(self, ws: ClientConnection, backoff: Backoff) -> None:
        while True:
            if self.synced:
                raw = await ws.recv()
            else:
                # Until the required channels are acknowledged, do not wait forever.
                timeout = max(0.1, self._subscribe_deadline - time.monotonic()) if self._subscribe_deadline else 10.0
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout)
                except TimeoutError:
                    log.error("ACCOUNT_WS_SUBSCRIBE_TIMEOUT acked=%s", sorted(self._acked))
                    return
            try:
                message = json.loads(raw)
                kind = str(message.get("type") or "")
                if kind == "connected":
                    await self._subscribe(ws)
                    continue
                if kind == "ping":
                    await ws.send(_PONG)
                    continue
                events = self._parse(kind, message)
            except (KeyError, TypeError, ValueError, AttributeError, InvalidOperation) as exc:
                # Only parsing is guarded. A message we cannot read may have carried a fill, so
                # the order-resolve timeout and reconciliation remain the backstop.
                self._metrics.count_error(ErrorClass.EXCHANGE_ERROR)
                log.error("ACCOUNT_MESSAGE_ERROR error=%s type=%s", type(exc).__name__, raw[:120])
                continue
            for event in events:
                event()  # trading logic runs outside the guard: its errors are never swallowed
            if not self.synced and self._acked >= _REQUIRED:
                self.synced = True
                self.connected_at = time.monotonic()
                backoff.reset()
                log.info("ACCOUNT_WS_SYNCED channels=%s", ",".join(sorted(self._acked)))
                self._events.on_account_stream_up()

    def _parse(self, kind: str, message: dict[str, Any]) -> list[Callable[[], None]]:
        """Turn one message into ready-to-run callbacks. Pure parsing: no trading logic here."""
        prefix, _, channel = kind.partition("/")
        if prefix not in ("subscribed", "update"):
            if kind == "error" or "error" in message:
                log.warning("ACCOUNT_WS_ERROR message=%s", str(message)[:300])
            return []
        snapshot = prefix == "subscribed"
        if snapshot:
            self._acked.add(channel)
        meta = self._meta
        market_id = meta.market_id
        events: list[Callable[[], None]] = []
        if channel == "account_orders":
            for obj in iter_objects(message.get("orders") or []):
                update = parse_order(obj, meta)
                if update.market_id == market_id:
                    events.append(partial(self._events.on_order_update, update))
        elif channel == "account_all_trades":
            if not snapshot:  # the subscription reply only carries history
                for obj in iter_objects(message.get("trades") or []):
                    fill = parse_trade(obj, self._cfg.account_index, meta)
                    if fill is not None and fill.market_id == market_id:
                        events.append(partial(self._events.on_trade_fill, fill))
        elif channel == "account_all_positions":
            raw = find_market_position(message.get("positions") or {}, market_id)
            # A snapshot without our market means there is no BTC position at all; an update
            # without it simply does not concern us.
            if raw is not None or snapshot:
                events.append(partial(self._events.on_position, parse_position(raw, meta, time.monotonic_ns())))
        elif channel == "user_stats":
            value = (message.get("stats") or {}).get("available_balance")
            if value not in (None, ""):
                events.append(partial(self._events.on_balance, meta.usd_to_q(Decimal(str(value)))))
        return events
