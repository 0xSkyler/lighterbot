"""In-memory stand-ins for the exchange. Nothing here opens a socket or signs anything."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from scalper.errors import ApiError, ErrorClass
from scalper.lighter_client import AccountSnapshot, SendResult, SignedTx
from scalper.orderbook import OrderBook
from scalper.position import ActiveOrder, ExchangePosition, OrderUpdate
from scalper.precision import MarketMeta
from scalper.signals import NO_SIGNAL, Signal

from .conftest import px, sz

BIDS = [(px(83691.1), sz(0.05)), (px(83690.0), sz(0.10))]
ASKS = [(px(83693.9), sz(0.05)), (px(83695.0), sz(0.10))]

LONG_SIGNAL = Signal(1, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9)
SHORT_SIGNAL = Signal(-1, -0.9, -0.9, -0.9, -0.9, -0.9, -0.9, -0.9, -0.9)


def accepted(tx_hash: str = "hash") -> SendResult:
    now = time.monotonic_ns()
    return SendResult(True, False, None, 200, "", tx_hash, now, now)


def rejected(
    error_class: ErrorClass = ErrorClass.ORDER_REJECTED, code: int = 21739, message: str = "rejected"
) -> SendResult:
    now = time.monotonic_ns()
    return SendResult(False, False, error_class, code, message, "hash", now, now)


def ambiguous() -> SendResult:
    now = time.monotonic_ns()
    return SendResult(False, True, ErrorClass.NETWORK_ERROR, None, "TimeoutError", "hash", now, now)


def flat_position(imf: int | None = 400, margin_mode: int | None = 0, open_orders: int = 0) -> ExchangePosition:
    return ExchangePosition(0, 0, open_orders, 0, imf, margin_mode, 0)


def exchange_position(signed_size: int, avg_entry: int, imf: int = 400) -> ExchangePosition:
    return ExchangePosition(signed_size, avg_entry, 0, 0, imf, 0, 0)


class FakeMarket:
    """Top-of-book view normally supplied by ``MarketDataStream``."""

    def __init__(self, book: OrderBook) -> None:
        self.book = book
        self.top_bids: list[tuple[int, int]] = []
        self.top_asks: list[tuple[int, int]] = []
        self.event_ns = 0
        self.age_ms = 0.0
        self._nonce = 0

    def set(self, bids: list[tuple[int, int]], asks: list[tuple[int, int]]) -> None:
        self._nonce += 1
        self.book.apply_snapshot(bids, asks, self._nonce)
        self.top_bids = self.book.top_bids(50)
        self.top_asks = self.book.top_asks(50)
        self.event_ns = time.monotonic_ns()

    def staleness_ms(self, now_ns: int) -> float:
        return self.age_ms


class FakeSignals:
    def __init__(self) -> None:
        self.signal = NO_SIGNAL
        self.volatility_mbps = 0
        self.computed = 0

    def compute(self, now_ns: int) -> Signal:
        self.computed += 1
        return self.signal

    def range_mbps(self, now_ns: int, window_ms: int = 1000) -> int:
        return self.volatility_mbps


class FakeExecutor:
    """Records submitted orders and answers with scripted results (default: accepted)."""

    def __init__(self) -> None:
        self.submitted: list[ActiveOrder] = []
        self.results: deque[SendResult] = deque()
        self.gate: asyncio.Event | None = None  # when set, submissions wait until released

    async def submit(self, order: ActiveOrder) -> SendResult:
        self.submitted.append(order)
        if self.gate is not None:
            await self.gate.wait()
        result = self.results.popleft() if self.results else accepted()
        order.sent_ns = result.sent_ns
        order.ack_ns = result.ack_ns
        order.acked = result.accepted
        return result


class FakeJournal:
    def __init__(self) -> None:
        self.trades: list[dict[str, Any]] = []
        self.events: list[tuple[str, dict[str, Any]]] = []
        self.states: list[dict[str, Any]] = []

    def record_trade(self, trade: dict[str, Any]) -> None:
        self.trades.append(trade)

    def record_event(self, kind: str, detail: dict[str, Any]) -> None:
        self.events.append((kind, detail))

    def save_state(self, snapshot: dict[str, Any]) -> None:
        self.states.append(snapshot)

    def event_kinds(self) -> list[str]:
        return [kind for kind, _ in self.events]


class FakeRecovery:
    def __init__(self) -> None:
        self.recoveries: list[str] = []
        self.resyncs: list[str] = []
        self.busy = False

    def start_recovery(self, reason: str) -> None:
        self.recoveries.append(reason)

    def request_resync(self, reason: str) -> None:
        self.resyncs.append(reason)


class FakeClient:
    """A tiny exchange simulator behind the ``LighterClient`` interface used by the executor."""

    def __init__(self, meta: MarketMeta) -> None:
        self.meta = meta
        self.position = flat_position()
        self.has_market_entry = True
        self.balance = Decimal("100")
        self.active_orders: list[OrderUpdate] = []
        self.finished_orders: dict[int, OrderUpdate] = {}
        self.bbo = (px(83691.1), px(83693.9))
        self.nonce = 100
        self.nonce_fetches = 0
        self.signed: list[dict[str, Any]] = []
        self._by_hash: dict[str, dict[str, Any]] = {}
        self.sent: list[SignedTx] = []
        self.results: deque[SendResult] = deque()
        self.account_errors: deque[ApiError] = deque()
        self.fill_fraction: deque[float] = deque()  # per accepted reduce-only order; default 1.0
        self.account_reads = 0
        self.account_gate: asyncio.Event | None = None  # when set, account reads wait until released
        self.sign_error: ApiError | None = None
        self.stream: Callable[[OrderUpdate], None] | None = None  # simulated account stream
        self.on_leverage: Callable[[int, int], None] | None = self._apply_leverage

    # ------------------------------------------------------------ reads

    async def fetch_next_nonce(self) -> int:
        self.nonce_fetches += 1
        return self.nonce

    async def fetch_account(self, meta: MarketMeta) -> AccountSnapshot:
        self.account_reads += 1
        if self.account_gate is not None:
            await self.account_gate.wait()
        if self.account_errors:
            raise self.account_errors.popleft()
        return AccountSnapshot(self.balance, self.balance, self.position, self.has_market_entry)

    async def fetch_active_orders(self, meta: MarketMeta) -> list[OrderUpdate]:
        return list(self.active_orders)

    async def fetch_recent_orders(self, meta: MarketMeta) -> dict[int, OrderUpdate]:
        return dict(self.finished_orders)

    async def fetch_rest_bbo(self, meta: MarketMeta) -> tuple[int, int]:
        return self.bbo

    # ---------------------------------------------------------- signing

    def _sign(self, kind: str, nonce: int, **fields: Any) -> SignedTx:
        entry = {"kind": kind, "nonce": nonce, **fields}
        self.signed.append(entry)
        tx_hash = f"hash-{len(self.signed)}"
        self._by_hash[tx_hash] = entry
        return SignedTx(0, "{}", tx_hash, nonce, time.monotonic_ns())

    def sign_order(self, order: ActiveOrder, market_id: int, nonce: int) -> SignedTx:
        if self.sign_error is not None:
            raise self.sign_error
        return self._sign("order", nonce, order=order, market_id=market_id)

    def sign_cancel(self, market_id: int, order_index: int, nonce: int) -> SignedTx:
        return self._sign("cancel", nonce, order_ref=order_index)

    def sign_cancel_all(self, market_id: int, nonce: int) -> SignedTx:
        return self._sign("cancel_all", nonce, market_id=market_id)

    def sign_update_leverage(self, market_id: int, imf: int, margin_mode: int, nonce: int) -> SignedTx:
        return self._sign("leverage", nonce, imf=imf, margin_mode=margin_mode)

    # ---------------------------------------------------------- sending

    def _apply_leverage(self, imf: int, margin_mode: int) -> None:
        p = self.position
        self.position = ExchangePosition(p.signed_size, p.avg_entry_price, p.open_orders, 0, imf, margin_mode, 0)
        self.has_market_entry = True

    async def send_signed(self, signed: SignedTx) -> SendResult:
        self.sent.append(signed)
        entry = self._by_hash[signed.tx_hash]
        result = self.results.popleft() if self.results else accepted(signed.tx_hash)
        if not result.accepted:
            return result
        self.nonce = signed.nonce + 1
        kind = entry["kind"]
        if kind == "order":
            self._execute(entry["order"])
        elif kind == "cancel":
            self.active_orders = [o for o in self.active_orders if o.client_order_index != entry["order_ref"]]
        elif kind == "cancel_all":
            self.active_orders = []
        elif kind == "leverage" and self.on_leverage is not None:
            self.on_leverage(entry["imf"], entry["margin_mode"])
        return result

    def _execute(self, order: ActiveOrder) -> None:
        """Fill an accepted order against the simulated position (reduce-only never reverses)."""
        p = self.position
        delta = -order.size if order.is_ask else order.size
        price = self.bbo[0] if order.is_ask else self.bbo[1]
        if order.reduce_only:
            if p.signed_size == 0 or (p.signed_size > 0) != order.is_ask:
                return  # nothing to reduce in that direction
            fraction = self.fill_fraction.popleft() if self.fill_fraction else 1.0
            closable = min(order.size, abs(p.signed_size))
            filled = int(closable * fraction)
            delta = -filled if order.is_ask else filled
        else:
            filled = order.size
        if filled <= 0:
            return
        new_size = p.signed_size + delta
        avg = p.avg_entry_price if p.signed_size else price
        self.position = ExchangePosition(new_size, avg if new_size else 0, 0, 0, p.imf, p.margin_mode, 0)
        update = OrderUpdate(
            order.client_order_index,
            1,
            self.meta.market_id,
            order.is_ask,
            "filled" if filled == order.size else "canceled",
            filled,
            filled * price,
            order.size - filled,
            order.reduce_only,
        )
        self.finished_orders[order.client_order_index] = update
        if self.stream is not None:
            self.stream(update)


async def no_sleep(_: float) -> None:
    """Replacement for timing waits: yield once so other tasks can run, but do not wait."""
    await asyncio.sleep(0)
