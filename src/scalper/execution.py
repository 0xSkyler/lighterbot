"""Order submission, nonce management and the central emergency flatten.

Rules enforced here:

* All signed transactions go through one lock, so nonces are issued strictly
  in order and two coroutines can never sign with the same nonce.
* A signed transaction is never re-sent. After an ambiguous outcome the nonce
  is re-read from the exchange before anything else is signed, and callers
  reconcile against authoritative state instead of retrying.
* Every closing order is ``reduce_only``; a duplicate or late close can shrink
  the position but can never reverse it.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from .config import Config
from .errors import ApiError, ErrorClass
from .lighter_client import LighterClient, SendResult, SignedTx
from .metrics import Metrics
from .position import ActiveOrder, ClientOrderIds, ExchangePosition, OrderKind, is_bot_order
from .precision import MarketMeta, price_minus_mbps, price_plus_mbps

log = logging.getLogger("scalper.execution")

_sleep = asyncio.sleep  # indirection so tests can run the timing-dependent paths instantly

FLATTEN_MAX_ATTEMPTS = 6
FLATTEN_SETTLE_S = 0.7  # longer than Lighter's taker speed bump (300 ms standard / 140 ms premium)


class NonceTracker:
    """Local nonce for the single API key, re-read from the exchange whenever it is in doubt."""

    def __init__(self, fetch: Callable[[], Awaitable[int]]) -> None:
        self._fetch = fetch
        self._next: int | None = None

    async def take(self) -> int:
        if self._next is None:
            self._next = int(await self._fetch())
        return self._next

    def settle(self, result: SendResult) -> None:
        """Advance, keep or invalidate the nonce according to the send outcome."""
        if result.accepted:
            if self._next is not None:
                self._next += 1
        elif result.ambiguous or result.error_class is ErrorClass.NONCE_ERROR:
            self._next = None  # unknown whether it was consumed: re-read before the next signature
        # A definitive API rejection does not consume the nonce (Lighter docs), so it is reused.

    def invalidate(self) -> None:
        self._next = None


@dataclass(slots=True)
class FlattenResult:
    flat: bool
    attempts: int
    position_before: int  # signed size seen on the first authoritative read
    orders: list[ActiveOrder] = field(default_factory=list)
    last_error: str = ""
    fatal: bool = False
    final_position: ExchangePosition | None = None  # last authoritative read


def _not_sent(error: ApiError, tx_hash: str = "") -> SendResult:
    now = time.monotonic_ns()
    return SendResult(False, False, error.error_class, error.code, str(error), tx_hash, now, now)


class Executor:
    """Signs and submits transactions; owns the nonce and the flatten procedure."""

    def __init__(
        self,
        cfg: Config,
        meta: MarketMeta,
        client: LighterClient,
        metrics: Metrics,
        ids: ClientOrderIds,
    ) -> None:
        self._cfg = cfg
        self._meta = meta
        self._client = client
        self._metrics = metrics
        self._ids = ids
        self._lock = asyncio.Lock()
        self._nonce = NonceTracker(client.fetch_next_nonce)
        self.last_tx_ns = 0  # monotonic time of the last submission (execution heartbeat)

    async def prime(self) -> None:
        """Load the nonce ahead of the first order so the first entry pays no extra round trip."""
        async with self._lock:
            await self._nonce.take()

    async def _send(self, sign: Callable[[int], SignedTx]) -> tuple[SendResult, SignedTx | None]:
        """Take a nonce, sign and submit exactly once, under the transaction lock."""
        async with self._lock:
            try:
                nonce = await self._nonce.take()
                signed = sign(nonce)
            except ApiError as exc:
                # Nothing left this process: the nonce could not be read or signing failed.
                self._nonce.invalidate()
                return _not_sent(exc), None
            self.last_tx_ns = time.monotonic_ns()
            result = await self._client.send_signed(signed)
            self._nonce.settle(result)
            return result, signed

    async def submit(self, order: ActiveOrder) -> SendResult:
        """Submit one IOC order. The caller owns the state machine; nothing is retried here."""
        result, signed = await self._send(lambda nonce: self._client.sign_order(order, self._meta.market_id, nonce))
        if signed is not None:
            order.tx_hash = signed.tx_hash
            order.signed_ns = signed.signed_ns
        order.sent_ns = result.sent_ns
        order.ack_ns = result.ack_ns
        order.acked = result.accepted
        if not result.accepted:
            log.warning(
                "ORDER_SEND_FAILED kind=%s coi=%d ambiguous=%s class=%s code=%s message=%s",
                order.kind.value,
                order.client_order_index,
                result.ambiguous,
                result.error_class.value if result.error_class else "",
                result.code,
                result.message,
            )
        return result

    async def cancel_order(self, order_ref: int) -> SendResult:
        """Cancel one order by client order index or exchange order index."""
        result, _ = await self._send(lambda nonce: self._client.sign_cancel(self._meta.market_id, order_ref, nonce))
        return result

    async def cancel_all_in_market(self) -> SendResult:
        result, _ = await self._send(lambda nonce: self._client.sign_cancel_all(self._meta.market_id, nonce))
        return result

    async def update_leverage(self, imf: int, margin_mode: int) -> SendResult:
        result, _ = await self._send(
            lambda nonce: self._client.sign_update_leverage(self._meta.market_id, imf, margin_mode, nonce)
        )
        return result

    async def cancel_resting_orders(self, *, cancel_all: bool) -> int:
        """Cancel resting orders in the BTC market. Returns how many foreign orders were left alone.

        Bot orders (identified by the client-order-index tag) are always cancelled.
        Orders the bot did not create are only cancelled when ``cancel_all`` is set.
        """
        orders = await self._client.fetch_active_orders(self._meta)
        if not orders:
            return 0
        foreign = [o for o in orders if not is_bot_order(o.client_order_index)]
        if cancel_all:
            result = await self.cancel_all_in_market()
            log.warning("CANCEL_ALL market=%s orders=%d accepted=%s", self._meta.symbol, len(orders), result.accepted)
            return 0
        for order in orders:
            if is_bot_order(order.client_order_index):
                result = await self.cancel_order(order.client_order_index)
                log.warning("CANCEL_STALE_BOT_ORDER coi=%d accepted=%s", order.client_order_index, result.accepted)
        return len(foreign)

    async def flatten_position(
        self,
        reason: str,
        *,
        get_bbo: Callable[[], tuple[int, int] | None],
        on_order: Callable[[ActiveOrder], None] | None = None,
        cancel_all: bool = False,
        max_attempts: int = FLATTEN_MAX_ATTEMPTS,
    ) -> FlattenResult:
        """Bring the BTC position to zero using authoritative state. The single flatten path.

        Each attempt: read the account from the exchange, cancel conflicting orders,
        and if a position exists send a reduce-only MARKET+IOC order for exactly that
        size inside the emergency slippage limit. The next attempt re-reads the
        account, so partial fills, late fills and duplicate submissions all converge
        on zero without any risk of reversing the position.

        ``get_bbo`` returns a fresh ``(bid, ask)`` from the live book or None, in
        which case the REST book is used.
        """
        cfg = self._cfg
        meta = self._meta
        result = FlattenResult(flat=False, attempts=0, position_before=0)
        log.warning("FLATTEN_START reason=%s", reason)
        for attempt in range(1, max_attempts + 1):
            result.attempts = attempt
            if attempt > 1:
                await _sleep(min(0.25 * attempt, 2.0))
            try:
                snapshot = await self._client.fetch_account(meta)
                position = snapshot.position
                result.final_position = position
                if attempt == 1:
                    result.position_before = position.signed_size
                if position.open_orders or position.pending_orders or (cancel_all and attempt == 1):
                    await self.cancel_resting_orders(cancel_all=cancel_all or cfg.cancel_foreign_orders)
                if position.signed_size == 0:
                    result.flat = True
                    log.warning("FLATTEN_DONE reason=%s attempts=%d", reason, attempt)
                    return result
                bbo = get_bbo() or await self._client.fetch_rest_bbo(meta)
            except ApiError as exc:
                result.last_error = str(exc)
                self._metrics.count_error(exc.error_class)
                log.error("FLATTEN_READ_FAILED attempt=%d class=%s error=%s", attempt, exc.error_class.value, exc)
                if exc.error_class is ErrorClass.AUTH_ERROR:
                    result.fatal = True
                    return result
                continue

            is_long = position.signed_size > 0
            size = abs(position.signed_size)
            slip = cfg.max_emergency_exit_slippage_mbps
            limit = price_minus_mbps(bbo[0], slip) if is_long else price_plus_mbps(bbo[1], slip)
            order = ActiveOrder(
                client_order_index=self._ids.next(),
                kind=OrderKind.EXIT,
                is_ask=is_long,
                size=size,
                limit_price=limit,
                reduce_only=True,
                market_order=True,
                reason=reason,
                created_ns=time.monotonic_ns(),
                emergency=True,
            )
            if on_order is not None:
                on_order(order)
            result.orders.append(order)
            log.warning(
                "FLATTEN_ORDER attempt=%d side=%s qty=%s limit=%s reduce_only=true coi=%d",
                attempt,
                "SELL" if is_long else "BUY",
                meta.fmt_size(size),
                meta.fmt_price(limit),
                order.client_order_index,
            )
            sent = await self.submit(order)
            if sent.rejected:
                result.last_error = sent.message
                if sent.error_class is ErrorClass.AUTH_ERROR:
                    result.fatal = True
                    return result
                continue
            # Accepted or ambiguous: give the sequencer time, then loop to re-read the truth.
            await _sleep(FLATTEN_SETTLE_S)

        # Out of attempts: report what the exchange says now.
        try:
            final = await self._client.fetch_account(meta)
            result.final_position = final.position
            result.flat = final.position.signed_size == 0
        except ApiError as exc:
            result.last_error = str(exc)
        log.error("FLATTEN_INCOMPLETE reason=%s attempts=%d flat=%s", reason, result.attempts, result.flat)
        return result
