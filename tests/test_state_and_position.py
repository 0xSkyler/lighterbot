"""State machine transitions, fill merging, partial fills and reduce-only quantities."""

from __future__ import annotations

import pytest

from scalper.position import (
    LONG,
    SHORT,
    ActiveOrder,
    ClientOrderIds,
    OrderKind,
    OrderUpdate,
    Position,
    TradeFill,
    is_bot_order,
    is_terminal_status,
)
from scalper.state_machine import InvalidTransition, State, StateMachine


def advance(sm: StateMachine, *states: State) -> None:
    for state in states:
        sm.transition(state, "test")


# ------------------------------------------------------------- state machine


def test_happy_path_long() -> None:
    seen: list[tuple[str, str, str]] = []
    sm = StateMachine(lambda old, new, reason: seen.append((old.value, new.value, reason)))
    advance(sm, State.SYNCING, State.FLAT, State.ENTRY_PENDING, State.OPEN_LONG, State.EXIT_PENDING, State.FLAT)
    assert sm.is_flat
    assert sm.seq == 6
    assert seen[2] == ("FLAT", "ENTRY_PENDING", "test")


def test_partial_fill_and_partial_exit_path() -> None:
    sm = StateMachine()
    advance(
        sm,
        State.SYNCING,
        State.FLAT,
        State.ENTRY_PENDING,
        State.PARTIALLY_FILLED,
        State.OPEN_SHORT,
        State.EXIT_PENDING,
        State.PARTIAL_EXIT,
        State.EXIT_PENDING,
        State.FLAT,
    )
    assert sm.is_flat


@pytest.mark.parametrize(
    "state",
    [
        State.STARTING,
        State.SYNCING,
        State.ENTRY_PENDING,
        State.PARTIALLY_FILLED,
        State.OPEN_LONG,
        State.OPEN_SHORT,
        State.EXIT_PENDING,
        State.PARTIAL_EXIT,
        State.RECOVERY,
        State.HALTED,
    ],
)
def test_entry_is_only_possible_from_flat(state: State) -> None:
    sm = StateMachine()
    sm.state = state
    assert not sm.can(State.ENTRY_PENDING)
    with pytest.raises(InvalidTransition):
        sm.transition(State.ENTRY_PENDING, "SIGNAL")
    assert sm.state is state  # a refused transition changes nothing


def test_open_cannot_jump_to_flat_without_an_exit() -> None:
    sm = StateMachine()
    sm.state = State.OPEN_LONG
    assert not sm.try_transition(State.FLAT, "x")
    assert sm.try_transition(State.EXIT_PENDING, "PROFIT")


def test_halted_is_terminal() -> None:
    sm = StateMachine()
    sm.state = State.HALTED
    for state in State:
        assert not sm.try_transition(state, "x")


def test_every_live_state_can_reach_recovery() -> None:
    for state in State:
        if state in (State.STARTING, State.RECOVERY, State.HALTED):
            continue
        sm = StateMachine()
        sm.state = state
        assert sm.can(State.RECOVERY), state


# ------------------------------------------------------------------ order ids


def test_client_order_ids_are_unique_tagged_and_monotonic() -> None:
    now = [1_800_000_000.0]
    ids = ClientOrderIds(clock=lambda: now[0])
    first = ids.next()
    second = ids.next()  # same millisecond
    now[0] -= 60  # wall clock steps backwards
    third = ids.next()
    assert first < second < third
    assert all(is_bot_order(i) for i in (first, second, third))
    assert third < 2**48
    assert not is_bot_order(123456)
    # A restart resumes above the persisted value even if the clock is behind.
    resumed = ClientOrderIds(last=ids.last, clock=lambda: 1_700_000_000.0)
    assert resumed.next() > third


# ----------------------------------------------------------------- order fills


def make_order(size: int = 300, kind: OrderKind = OrderKind.ENTRY, is_ask: bool = False) -> ActiveOrder:
    return ActiveOrder(
        client_order_index=1,
        kind=kind,
        is_ask=is_ask,
        size=size,
        limit_price=836939,
        reduce_only=kind is OrderKind.EXIT,
        market_order=kind is OrderKind.EXIT,
        reason="test",
        created_ns=0,
    )


def update(filled: int, quote: int, status: str) -> OrderUpdate:
    return OrderUpdate(1, 77, 1, False, status, filled, quote, 300 - filled, False)


def fill(trade_id: int, size: int, price: int, fee_tick: int = 0) -> TradeFill:
    return TradeFill(trade_id, 1, 1, size, price, True, fee_tick)


def test_terminal_statuses() -> None:
    assert is_terminal_status("filled")
    assert is_terminal_status("canceled")
    assert is_terminal_status("canceled-too-much-slippage")
    assert not is_terminal_status("open")
    assert not is_terminal_status("in-progress")
    assert not is_terminal_status("pending")


def test_accepted_order_is_not_a_fill() -> None:
    order = make_order()
    assert order.apply_order_update(update(0, 0, "in-progress"), 5) == (0, 0)
    assert order.filled == 0 and not order.terminal


def test_order_update_is_idempotent() -> None:
    order = make_order()
    assert order.apply_order_update(update(300, 251_081_700, "filled"), 10) == (300, 251_081_700)
    assert order.apply_order_update(update(300, 251_081_700, "filled"), 11) == (0, 0)
    assert order.terminal and order.first_fill_ns == 10 and order.done_ns == 10
    assert order.avg_price == pytest.approx(836939.0)


def test_trades_then_order_update_never_double_count() -> None:
    order = make_order()
    assert order.apply_trade(fill(1, 100, 836900), 1) == (100, 100 * 836900)
    assert order.apply_trade(fill(1, 100, 836900), 2) == (0, 0)  # duplicate trade id
    assert order.apply_trade(fill(2, 200, 836950), 3) == (200, 200 * 836950)
    total_q = 100 * 836900 + 200 * 836950
    assert order.apply_order_update(update(300, total_q, "filled"), 4) == (0, 0)
    assert order.filled == 300 and order.filled_quote_q == total_q


def test_order_update_then_trades_never_double_count() -> None:
    order = make_order()
    total_q = 100 * 836900 + 200 * 836950
    assert order.apply_order_update(update(300, total_q, "filled"), 1) == (300, total_q)
    assert order.apply_trade(fill(1, 100, 836900), 2) == (0, 0)
    assert order.apply_trade(fill(2, 200, 836950), 3) == (0, 0)
    assert order.filled == 300 and order.filled_quote_q == total_q


def test_partial_ioc_fill() -> None:
    order = make_order()
    assert order.apply_order_update(update(120, 120 * 836939, "canceled-too-much-slippage"), 1) == (120, 120 * 836939)
    assert order.terminal and order.filled == 120


def test_stale_lower_cumulative_update_is_ignored() -> None:
    order = make_order()
    order.apply_order_update(update(200, 200 * 836939, "open"), 1)
    assert order.apply_order_update(update(100, 100 * 836939, "open"), 2) == (0, 0)
    assert order.filled == 200


# -------------------------------------------------------------------- position


def test_position_average_entry_from_fills() -> None:
    pos = Position(side=LONG, opened_ns=0)
    pos.add_entry(100, 100 * 836900, 10)
    pos.add_entry(200, 200 * 836950, 20)
    assert pos.size == 300
    assert pos.cost_q == 100 * 836900 + 200 * 836950
    assert pos.avg_entry == pytest.approx((100 * 836900 + 200 * 836950) / 300)
    assert pos.fee_q == 30 and pos.signed_size == 300


def test_partial_exit_leaves_the_actual_remaining_quantity() -> None:
    pos = Position(side=SHORT, opened_ns=0)
    pos.add_entry(300, 300 * 837000, 30)
    pos.add_exit(100, 100 * 836900, 5)
    assert pos.size == 200  # the next reduce-only order must be for 200, not 300
    assert pos.cost_q == 200 * 837000
    assert pos.closed_cost_q == 100 * 837000
    assert pos.fee_q == 20
    assert pos.signed_size == -200
    pos.add_exit(200, 200 * 836900, 10)
    assert pos.size == 0 and pos.cost_q == 0 and pos.fee_q == 0
    assert pos.closed_cost_q == 300 * 837000
    assert pos.exit_value_q == 300 * 836900 and pos.exit_fee_q == 15


def test_exit_fill_can_never_reverse_the_position() -> None:
    pos = Position(side=LONG, opened_ns=0)
    pos.add_entry(300, 300 * 836900, 0)
    pos.add_exit(500, 500 * 836900, 0)  # an over-sized fill report
    assert pos.size == 0
    assert pos.closed_cost_q == 300 * 836900


def test_excursions() -> None:
    pos = Position(side=LONG, opened_ns=0)
    for net in (-50, 20, -80, 10):
        pos.observe(net)
    assert pos.mfe_q == 20 and pos.mae_q == -80
