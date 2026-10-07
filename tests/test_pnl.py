"""VWAP, executable-close estimation (GREEN), break-even price and realized P&L."""

from __future__ import annotations

from scalper.pnl import (
    LONG,
    SHORT,
    adverse_move_mbps,
    breakeven_exit_price,
    estimate_close,
    estimate_entry,
    realized_pnl,
    walk_levels,
)

from .conftest import px, sz

# Bids best-first: 0.001 @ 83700.0, 0.002 @ 83699.0, 0.010 @ 83690.0
BIDS = [(px(83700.0), sz(0.001)), (px(83699.0), sz(0.002)), (px(83690.0), sz(0.010))]
ASKS = [(px(83701.0), sz(0.001)), (px(83702.0), sz(0.002)), (px(83712.0), sz(0.010))]
SIZE = sz(0.003)


def close(side: int, cost_q: int, levels: list[tuple[int, int]], **kw: int) -> object:
    params = {"taker_fee_tick": 0, "buffer_mbps": 0, "max_slippage_mbps": 2000, "min_profit_q": 10_000}
    params.update(kw)
    return estimate_close(side, SIZE, cost_q, 0, levels, **params)


def test_walk_levels_vwap_sell() -> None:
    filled, value, worst, available = walk_levels(BIDS, SIZE, limit_price=0, is_buy=False)
    assert filled == SIZE
    assert value == sz(0.001) * px(83700.0) + sz(0.002) * px(83699.0)
    assert worst == px(83699.0)
    assert available == sz(0.013)


def test_walk_levels_respects_limit_price() -> None:
    # A buy capped at 83701.0 only reaches the first ask.
    filled, value, worst, available = walk_levels(ASKS, SIZE, limit_price=px(83701.0), is_buy=True)
    assert filled == sz(0.001)
    assert value == sz(0.001) * px(83701.0)
    assert worst == px(83701.0)
    assert available == sz(0.001)


def test_estimate_entry_vwap_and_slippage() -> None:
    est = estimate_entry(True, SIZE, ASKS, max_slippage_mbps=1000)  # 1 bps cap
    assert est.full
    assert est.best_price == px(83701.0)
    assert est.value_q == sz(0.001) * px(83701.0) + sz(0.002) * px(83702.0)
    assert est.vwap == est.value_q / SIZE
    # VWAP is 0.667 USD above the best ask of 83701 -> ~0.0796 bps
    assert 70 <= est.slippage_mbps <= 90
    assert est.limit_price >= px(83709.3)


def test_estimate_entry_insufficient_depth_inside_cap() -> None:
    est = estimate_entry(True, sz(0.010), ASKS, max_slippage_mbps=500)  # 0.5 bps: 83712 is out of reach
    assert not est.full
    assert est.fillable == sz(0.003)


def test_estimate_entry_empty_book() -> None:
    est = estimate_entry(False, SIZE, [], 1000)
    assert not est.full
    assert est.fillable == 0


def test_long_is_green_only_on_executable_bid_vwap() -> None:
    entry_price = px(83698.0)
    cost = SIZE * entry_price
    est = close(LONG, cost, BIDS)
    expected_value = sz(0.001) * px(83700.0) + sz(0.002) * px(83699.0)
    assert est.full
    assert est.close_value_q == expected_value
    assert est.gross_pnl_q == expected_value - cost  # +0.004 USD
    assert est.gross_pnl_q == 4000
    assert est.net_pnl_q == 4000
    assert not est.profitable  # below the 0.01 USD minimum
    assert close(LONG, cost, BIDS, min_profit_q=3999).profitable
    # Top-of-book alone would look better than the real depth-walked exit.
    assert est.slippage_q == SIZE * px(83700.0) - expected_value
    assert est.close_vwap < px(83700.0)


def test_short_is_green_only_on_executable_ask_vwap() -> None:
    entry_price = px(83710.0)
    cost = SIZE * entry_price
    est = close(SHORT, cost, ASKS)
    expected_value = sz(0.001) * px(83701.0) + sz(0.002) * px(83702.0)
    assert est.full
    assert est.gross_pnl_q == cost - expected_value
    assert est.gross_pnl_q > 0
    assert est.profitable
    assert est.best_price == px(83701.0)


def test_last_price_or_mid_never_makes_a_trade_green() -> None:
    # Long from 83700.5: the mid (83700.5) equals entry but the executable bid VWAP is below it.
    cost = SIZE * px(83700.5)
    est = close(LONG, cost, BIDS, min_profit_q=0)
    assert est.net_pnl_q < 0
    assert not est.profitable


def test_fees_and_buffer_are_subtracted() -> None:
    cost = SIZE * px(83690.0)
    free = close(LONG, cost, BIDS)
    paid = estimate_close(
        LONG, SIZE, cost, 5_000, BIDS, taker_fee_tick=200, buffer_mbps=1000, max_slippage_mbps=2000, min_profit_q=0
    )
    assert paid.gross_pnl_q == free.gross_pnl_q
    assert paid.exit_fee_q == -(-paid.close_value_q * 200 // 1_000_000)
    assert paid.buffer_q == -(-paid.close_value_q * 1000 // 10_000_000)
    assert paid.entry_fee_q == 5_000
    assert paid.fees_q == paid.exit_fee_q + 5_000
    assert paid.net_pnl_q == paid.gross_pnl_q - paid.exit_fee_q - 5_000 - paid.buffer_q
    assert paid.net_pnl_q < free.net_pnl_q


def test_not_green_when_entire_position_cannot_be_closed() -> None:
    # Only 0.003 BTC is available inside the band; a 0.004 position cannot fully exit.
    size = sz(0.004)
    est = estimate_close(
        LONG, size, size * px(83000.0), 0, BIDS, taker_fee_tick=0, buffer_mbps=0, max_slippage_mbps=500, min_profit_q=0
    )
    assert not est.full
    assert est.fillable == sz(0.003)
    assert est.available_liquidity == sz(0.003)
    assert not est.profitable  # hugely positive on paper, but not executable in full


def test_estimate_close_empty_book() -> None:
    est = close(LONG, SIZE * px(83000.0), [])
    assert not est.full and not est.profitable and est.fillable == 0


def test_breakeven_price_long_is_tight() -> None:
    cost = SIZE * px(83698.0)
    for fee_tick, entry_fee in ((0, 0), (200, 50_000)):
        floor = breakeven_exit_price(LONG, SIZE, cost, entry_fee, taker_fee_tick=fee_tick, min_profit_q=10_000)

        def net_at(price: int, fee_tick: int = fee_tick, entry_fee: int = entry_fee) -> int:
            return estimate_close(
                LONG,
                SIZE,
                cost,
                entry_fee,
                [(price, SIZE)],
                taker_fee_tick=fee_tick,
                buffer_mbps=0,
                max_slippage_mbps=0,
                min_profit_q=10_000,
            ).net_pnl_q

        assert net_at(floor) > 10_000
        assert net_at(floor - 2) <= 10_000


def test_breakeven_price_short_is_tight() -> None:
    cost = SIZE * px(83710.0)
    for fee_tick, entry_fee in ((0, 0), (200, 50_000)):
        cap = breakeven_exit_price(SHORT, SIZE, cost, entry_fee, taker_fee_tick=fee_tick, min_profit_q=10_000)

        def net_at(price: int, fee_tick: int = fee_tick, entry_fee: int = entry_fee) -> int:
            return estimate_close(
                SHORT,
                SIZE,
                cost,
                entry_fee,
                [(price, SIZE)],
                taker_fee_tick=fee_tick,
                buffer_mbps=0,
                max_slippage_mbps=0,
                min_profit_q=10_000,
            ).net_pnl_q

        assert cap > 0
        assert net_at(cap) > 10_000
        assert net_at(cap + 2) <= 10_000


def test_realized_pnl_long_and_short() -> None:
    entry = SIZE * px(83700.0)
    exit_value = SIZE * px(83710.0)
    assert realized_pnl(LONG, entry, exit_value, 100, 200) == (30_000, 300, 29_700)
    assert realized_pnl(SHORT, entry, exit_value, 100, 200) == (-30_000, 300, -30_300)


def test_adverse_move() -> None:
    cost = SIZE * px(83700.0)
    assert adverse_move_mbps(LONG, SIZE, cost, px(83700.0)) == 0
    # 83.7 USD below entry = 10 bps against a long
    assert adverse_move_mbps(LONG, SIZE, cost, px(83616.3)) == 10_000
    assert adverse_move_mbps(SHORT, SIZE, cost, px(83783.7)) == 10_000
    assert adverse_move_mbps(LONG, SIZE, cost, px(83783.7)) < 0  # in profit
