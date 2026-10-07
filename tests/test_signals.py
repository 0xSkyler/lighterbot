"""Signal components: imbalance, flow, momentum, BBO momentum, symmetry and warm-up."""

from __future__ import annotations

import pytest

from scalper.signals import NS_PER_MS, Series, SignalEngine, SignalWeights

T0 = 1_000_000_000_000  # arbitrary monotonic origin (ns)


def only(**weights: float) -> SignalWeights:
    base = {
        "book_imbalance": 0.0,
        "trade_flow": 0.0,
        "micro_momentum": 0.0,
        "bbo_momentum": 0.0,
        "microprice": 0.0,
        "volume_accel": 0.0,
        "depth_change": 0.0,
    }
    base.update(weights)
    return SignalWeights(**base)


def engine(weights: SignalWeights, threshold: float = 0.5) -> SignalEngine:
    return SignalEngine(weights, threshold, depth_levels=2, momentum_scale_bps=2.0, warmup_ms=1000)


def ms(value: int) -> int:
    return T0 + value * NS_PER_MS


def test_series_lookback_and_retention() -> None:
    series = Series(retain_ms=1000)
    assert series.value_at(ms(0)) is None
    series.append(ms(0), 1.0)
    series.append(ms(500), 2.0)
    series.append(ms(900), 3.0)
    assert series.value_at(ms(499)) == 1.0
    assert series.value_at(ms(500)) == 2.0
    assert series.value_at(ms(5000)) == 3.0
    assert series.range_since(ms(450)) == (1.0, 3.0)  # the value in force at t=450 is 1.0
    assert series.range_since(ms(600)) == (2.0, 3.0)
    series.append(ms(3000), 4.0)  # everything but one anchor sample is now out of retention
    assert len(series) == 2
    assert series.value_at(ms(2500)) == 3.0


def test_no_signal_before_warmup() -> None:
    eng = engine(only(book_imbalance=1.0))
    eng.on_book(ms(0), [(1000, 90), (999, 90)], [(1001, 10), (1002, 10)])
    assert not eng.ready(ms(500))
    assert eng.compute(ms(500)).side == 0
    assert eng.ready(ms(1000))
    assert eng.compute(ms(1000)).side == 1


def test_book_imbalance_and_symmetry() -> None:
    long_eng = engine(only(book_imbalance=1.0))
    long_eng.on_book(ms(0), [(1000, 90), (999, 90)], [(1001, 10), (1002, 10)])
    long_signal = long_eng.compute(ms(1500))
    assert long_signal.book_imbalance == pytest.approx(0.8)
    assert long_signal.side == 1

    short_eng = engine(only(book_imbalance=1.0))
    short_eng.on_book(ms(0), [(1000, 10), (999, 10)], [(1001, 90), (1002, 90)])
    short_signal = short_eng.compute(ms(1500))
    assert short_signal.book_imbalance == pytest.approx(-0.8)
    assert short_signal.side == -1
    assert short_signal.score == pytest.approx(-long_signal.score)


def test_book_imbalance_uses_only_configured_depth() -> None:
    eng = engine(only(book_imbalance=1.0))
    # The third level is outside depth_levels=2 and must not count.
    eng.on_book(ms(0), [(1000, 10), (999, 10), (998, 10_000)], [(1001, 10), (1002, 10), (1003, 1)])
    assert eng.compute(ms(1500)).book_imbalance == pytest.approx(0.0)


def test_trade_flow_imbalance() -> None:
    eng = engine(only(trade_flow=1.0))
    eng.on_book(ms(0), [(1000, 10)], [(1001, 10)])
    for offset in (1100, 1200, 1300):
        eng.on_trade(ms(offset), 30, taker_is_buyer=True)
    eng.on_trade(ms(1350), 10, taker_is_buyer=False)
    signal = eng.compute(ms(1400))
    assert signal.trade_flow == pytest.approx((90 - 10) / 100)
    assert signal.side == 1
    # Trades older than the 1 s flow window no longer count.
    assert eng.compute(ms(2400)).trade_flow == 0.0


def test_trade_flow_needs_a_minimum_number_of_trades() -> None:
    eng = engine(only(trade_flow=1.0))
    eng.on_book(ms(0), [(1000, 10)], [(1001, 10)])
    eng.on_trade(ms(1300), 500, taker_is_buyer=True)
    assert eng.compute(ms(1400)).trade_flow == 0.0


def test_micro_momentum_direction_and_clamp() -> None:
    up = engine(only(micro_momentum=1.0))
    up.on_book(ms(0), [(100_000, 10)], [(100_010, 10)])
    up.on_book(ms(1450), [(100_100, 10)], [(100_110, 10)])  # +10 bps within the last 100 ms
    signal = up.compute(ms(1500))
    assert signal.micro_momentum == 1.0  # clamped
    assert signal.side == 1

    down = engine(only(micro_momentum=1.0))
    down.on_book(ms(0), [(100_000, 10)], [(100_010, 10)])
    down.on_book(ms(1450), [(99_900, 10)], [(99_910, 10)])
    assert down.compute(ms(1500)).micro_momentum == -1.0
    assert down.compute(ms(1500)).side == -1


def test_bbo_momentum_counts_best_price_moves() -> None:
    eng = engine(only(bbo_momentum=1.0))
    eng.on_book(ms(0), [(1000, 10)], [(1002, 10)])
    eng.on_book(ms(1100), [(1001, 10)], [(1002, 10)])  # bid up
    eng.on_book(ms(1200), [(1001, 10)], [(1003, 10)])  # ask up
    eng.on_book(ms(1300), [(1000, 10)], [(1003, 10)])  # bid down
    signal = eng.compute(ms(1400))
    assert signal.bbo_momentum == pytest.approx((2 - 1) / 3)


def test_microprice_is_top_of_book_size_imbalance() -> None:
    eng = engine(only(microprice=1.0))
    eng.on_book(ms(0), [(1000, 30)], [(1001, 10)])
    assert eng.compute(ms(1500)).microprice == pytest.approx(0.5)


def test_depth_change_detects_liquidity_pulled_from_asks() -> None:
    eng = engine(only(depth_change=1.0))
    eng.on_book(ms(0), [(1000, 50), (999, 50)], [(1001, 50), (1002, 50)])
    eng.on_book(ms(1300), [(1000, 50), (999, 50)], [(1001, 10), (1002, 10)])  # asks pulled
    signal = eng.compute(ms(1400))
    assert signal.depth_change == pytest.approx(0.8)
    assert signal.side == 1


def test_volume_acceleration_is_directional() -> None:
    eng = SignalEngine(only(volume_accel=1.0), 0.5, depth_levels=2, momentum_scale_bps=2.0, warmup_ms=1000)
    eng.on_book(ms(0), [(1000, 10)], [(1001, 10)])
    eng.on_trade(ms(1000), 10, taker_is_buyer=True)  # sparse baseline
    for offset in (5900, 5950, 5990):
        eng.on_trade(ms(offset), 100, taker_is_buyer=False)  # sudden burst of selling
    signal = eng.compute(ms(6000))
    assert signal.volume_accel == -1.0
    assert signal.side == -1


def test_weighted_combination_and_threshold() -> None:
    eng = SignalEngine(
        only(book_imbalance=1.0, microprice=1.0), threshold=0.7, depth_levels=1, momentum_scale_bps=2.0, warmup_ms=1000
    )
    eng.on_book(ms(0), [(1000, 80)], [(1001, 20)])  # both components = +0.6
    signal = eng.compute(ms(1500))
    assert signal.score == pytest.approx(0.6)
    assert signal.side == 0  # below the 0.7 threshold: no trade is forced


def test_volatility_range_and_reset() -> None:
    eng = engine(only(book_imbalance=1.0))
    eng.on_book(ms(0), [(100_000, 10)], [(100_010, 10)])
    eng.on_book(ms(1200), [(100_200, 10)], [(100_210, 10)])
    assert eng.range_mbps(ms(1300)) == pytest.approx(20_000, rel=0.01)  # ~20 bps range in 1 s
    eng.reset()
    assert not eng.ready(ms(5000))
    assert eng.range_mbps(ms(5000)) == 0
