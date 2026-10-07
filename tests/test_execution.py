"""Nonce handling, no-blind-retry semantics and the central flatten procedure."""

from __future__ import annotations

import asyncio
import time

import pytest

from scalper import execution
from scalper.config import Config
from scalper.errors import ApiError, ErrorClass
from scalper.execution import Executor
from scalper.metrics import Metrics
from scalper.position import ActiveOrder, ClientOrderIds, OrderKind, OrderUpdate

from .conftest import BTC, make_config, px
from .fakes import FakeClient, ambiguous, exchange_position, flat_position, no_sleep, rejected


@pytest.fixture(autouse=True)
def instant_waits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(execution, "_sleep", no_sleep)


def make_executor(cfg: Config | None = None) -> tuple[Executor, FakeClient, ClientOrderIds]:
    client = FakeClient(BTC)
    ids = ClientOrderIds()
    return Executor(cfg or make_config(), BTC, client, Metrics(), ids), client, ids  # type: ignore[arg-type]


def entry_order(ids: ClientOrderIds, size: int = 300) -> ActiveOrder:
    return ActiveOrder(
        client_order_index=ids.next(),
        kind=OrderKind.ENTRY,
        is_ask=False,
        size=size,
        limit_price=px(83700.0),
        reduce_only=False,
        market_order=False,
        reason="test",
        created_ns=time.monotonic_ns(),
    )


def nonces(client: FakeClient) -> list[int]:
    return [tx.nonce for tx in client.sent]


# --------------------------------------------------------------------- nonces


async def test_nonce_is_fetched_once_then_incremented() -> None:
    executor, client, ids = make_executor()
    await executor.prime()
    for _ in range(3):
        result = await executor.submit(entry_order(ids))
        assert result.accepted
    assert nonces(client) == [100, 101, 102]
    assert client.nonce_fetches == 1


async def test_definitive_rejection_does_not_consume_the_nonce() -> None:
    executor, client, ids = make_executor()
    client.results.append(rejected())
    first = await executor.submit(entry_order(ids))
    second = await executor.submit(entry_order(ids))
    assert first.rejected and second.accepted
    assert nonces(client) == [100, 100]
    assert client.nonce_fetches == 1


async def test_ambiguous_send_forces_a_nonce_reread_and_is_not_retried() -> None:
    executor, client, ids = make_executor()
    client.results.append(ambiguous())
    order = entry_order(ids)
    result = await executor.submit(order)
    assert result.ambiguous and not order.acked
    assert len(client.sent) == 1  # submit() itself never re-sends
    client.nonce = 101  # the exchange did in fact consume nonce 100
    await executor.submit(entry_order(ids))
    assert nonces(client) == [100, 101]
    assert client.nonce_fetches == 2


async def test_invalid_nonce_error_forces_a_reread() -> None:
    executor, client, ids = make_executor()
    client.results.append(rejected(ErrorClass.NONCE_ERROR, 21104, "invalid nonce"))
    await executor.submit(entry_order(ids))
    client.nonce = 107
    await executor.submit(entry_order(ids))
    assert nonces(client) == [100, 107]


async def test_concurrent_submissions_never_share_a_nonce() -> None:
    executor, client, ids = make_executor()
    results = await asyncio.gather(*(executor.submit(entry_order(ids)) for _ in range(12)))
    assert all(r.accepted for r in results)
    assert nonces(client) == list(range(100, 112))
    assert len({tx.tx_hash for tx in client.sent}) == 12


async def test_signing_failure_sends_nothing() -> None:
    executor, client, ids = make_executor()
    client.sign_error = ApiError("signing failed", error_class=ErrorClass.ORDER_REJECTED)
    result = await executor.submit(entry_order(ids))
    assert result.rejected and not result.ambiguous
    assert client.sent == []


async def test_order_records_latency_timestamps() -> None:
    executor, _, ids = make_executor()
    order = entry_order(ids)
    await executor.submit(order)
    assert order.acked and order.tx_hash
    assert 0 < order.signed_ns <= order.sent_ns <= order.ack_ns


# -------------------------------------------------------------------- flatten


async def test_flatten_when_already_flat_sends_nothing() -> None:
    executor, client, _ = make_executor()
    result = await executor.flatten_position("TEST", get_bbo=lambda: None)
    assert result.flat and result.attempts == 1 and result.orders == []
    assert client.sent == []


async def test_flatten_long_sends_one_reduce_only_sell_for_the_exact_size() -> None:
    executor, client, _ = make_executor()
    client.position = exchange_position(300, px(83693.9))
    result = await executor.flatten_position("TEST", get_bbo=lambda: None)
    assert result.flat and result.position_before == 300 and len(result.orders) == 1
    order = result.orders[0]
    assert order.is_ask and order.reduce_only and order.market_order and order.emergency
    assert order.size == 300
    assert order.limit_price == px(83691.1) * (10_000_000 - 30_000) // 10_000_000  # 30 bps below the REST bid
    assert client.position.signed_size == 0


async def test_flatten_short_buys_back_inside_emergency_slippage() -> None:
    executor, client, _ = make_executor()
    client.position = exchange_position(-250, px(83691.1))
    result = await executor.flatten_position("TEST", get_bbo=lambda: None)
    order = result.orders[0]
    assert result.flat and not order.is_ask and order.reduce_only and order.size == 250
    assert order.limit_price == -(-px(83693.9) * (10_000_000 + 30_000) // 10_000_000)


async def test_flatten_prefers_the_live_book_when_fresh() -> None:
    executor, client, _ = make_executor()
    client.position = exchange_position(300, px(83693.9))
    live = (px(83500.0), px(83501.0))
    result = await executor.flatten_position("TEST", get_bbo=lambda: live)
    assert result.orders[0].limit_price == px(83500.0) * (10_000_000 - 30_000) // 10_000_000


async def test_flatten_after_partial_fill_uses_the_remaining_quantity() -> None:
    executor, client, _ = make_executor()
    client.position = exchange_position(300, px(83693.9))
    client.fill_fraction.append(0.4)  # the first close only fills 120
    result = await executor.flatten_position("TEST", get_bbo=lambda: None)
    assert result.flat
    assert [o.size for o in result.orders] == [300, 180]  # the retry is for exactly what is left
    assert all(o.reduce_only for o in result.orders)
    assert len({o.client_order_index for o in result.orders}) == 2
    assert client.position.signed_size == 0


async def test_flatten_survives_rejection_ambiguity_and_read_errors() -> None:
    executor, client, _ = make_executor()
    client.position = exchange_position(300, px(83693.9))
    client.results.extend([rejected(), ambiguous()])
    client.account_errors.append(ApiError("timeout", error_class=ErrorClass.NETWORK_ERROR, ambiguous=True))
    result = await executor.flatten_position("TEST", get_bbo=lambda: None)
    assert result.flat and client.position.signed_size == 0
    # Every attempt re-read the exchange first; nothing was sized from stale local state.
    assert client.account_reads >= len(result.orders) + 1


async def test_flatten_can_never_reverse_the_position() -> None:
    executor, client, _ = make_executor()
    client.position = exchange_position(300, px(83693.9))
    result = await executor.flatten_position("TEST", get_bbo=lambda: None)
    # A stale duplicate of the same close arriving later is reduce-only: it has nothing to act on.
    client._execute(result.orders[0])
    assert client.position.signed_size == 0


async def test_flatten_reports_failure_instead_of_pretending() -> None:
    executor, client, _ = make_executor()
    client.position = exchange_position(300, px(83693.9))
    client.fill_fraction.extend([0.0] * 10)  # nothing ever fills
    result = await executor.flatten_position("TEST", get_bbo=lambda: None, max_attempts=3)
    assert not result.flat and result.attempts == 3 and len(result.orders) == 3
    assert client.position.signed_size == 300


async def test_flatten_stops_on_authentication_failure() -> None:
    executor, client, _ = make_executor()
    client.position = exchange_position(300, px(83693.9))
    client.results.append(rejected(ErrorClass.AUTH_ERROR, 21120, "invalid signature"))
    result = await executor.flatten_position("TEST", get_bbo=lambda: None)
    assert not result.flat and result.fatal and len(result.orders) == 1


async def test_manual_flatten_cancels_every_resting_order_first() -> None:
    executor, client, ids = make_executor()
    client.position = flat_position(open_orders=2)
    client.active_orders = [
        OrderUpdate(ids.next(), 1, 1, True, "open", 0, 0, 100, False),
        OrderUpdate(555, 2, 1, False, "open", 0, 0, 100, False),
    ]
    result = await executor.flatten_position("MANUAL_FLATTEN", get_bbo=lambda: None, cancel_all=True)
    assert result.flat
    assert [s["kind"] for s in client.signed] == ["cancel_all"]
    assert client.active_orders == []


async def test_stale_bot_orders_are_cancelled_but_foreign_orders_are_left_alone() -> None:
    executor, client, ids = make_executor()
    bot_order = ids.next()
    client.active_orders = [
        OrderUpdate(bot_order, 1, 1, True, "open", 0, 0, 100, False),
        OrderUpdate(555, 2, 1, False, "open", 0, 0, 100, False),
    ]
    foreign_left = await executor.cancel_resting_orders(cancel_all=False)
    assert foreign_left == 1
    assert [(s["kind"], s["order_ref"]) for s in client.signed] == [("cancel", bot_order)]
    assert [o.client_order_index for o in client.active_orders] == [555]
