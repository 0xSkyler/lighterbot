"""Microstructure signal engine.

Deterministic arithmetic over rolling in-memory windows (100 ms .. 10 s). These
are rolling windows over individual order-book/trade events, not candles. Each
component is normalised to [-1, +1] (positive = upward pressure) and combined
into one weighted score:

    score = sum(weight_i * component_i) / sum(weight_i)

LONG when ``score >= threshold``, SHORT when ``score <= -threshold``. The logic
is symmetric; there is no directional bias and no learning at runtime.
"""

from __future__ import annotations

from array import array
from bisect import bisect_right
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass

NS_PER_MS = 1_000_000

MOMENTUM_WINDOWS_MS = (100, 250, 500, 1000)
FLOW_WINDOW_MS = 1000
BBO_WINDOW_MS = 1000
DEPTH_CHANGE_WINDOW_MS = 500
ACCEL_SHORT_MS = 250
ACCEL_LONG_MS = 5000
VOLATILITY_WINDOW_MS = 1000
RETAIN_MS = 10_000
MIN_FLOW_TRADES = 3
MIN_BBO_TICKS = 2
ACCEL_FULL_SCALE = 3.0  # short-window volume rate 4x the 5 s baseline => component 1.0
MAX_TRADES_KEPT = 8192


class Series:
    """Append-only (time, value) series with bounded retention and O(log n) lookback."""

    __slots__ = ("_retain_ns", "_start", "t", "v")

    def __init__(self, retain_ms: int) -> None:
        self.t = array("q")
        self.v = array("d")
        self._start = 0
        self._retain_ns = retain_ms * NS_PER_MS

    def append(self, t_ns: int, value: float) -> None:
        self.t.append(t_ns)
        self.v.append(value)
        cutoff = t_ns - self._retain_ns
        t = self.t
        start = self._start
        # Keep one sample at or before the cutoff so lookbacks at the window edge resolve.
        while start + 1 < len(t) and t[start + 1] <= cutoff:
            start += 1
        self._start = start
        if start > 4096:
            del self.t[:start]
            del self.v[:start]
            self._start = 0

    def value_at(self, t_ns: int) -> float | None:
        """Last value recorded at or before ``t_ns``; None if history does not reach back."""
        i = bisect_right(self.t, t_ns, self._start) - 1
        if i < self._start:
            return None
        return self.v[i]

    def last(self) -> float | None:
        return self.v[-1] if len(self.v) > self._start else None

    def range_since(self, t_ns: int) -> tuple[float, float] | None:
        """(min, max) of values in force since ``t_ns``."""
        lo_i = max(bisect_right(self.t, t_ns, self._start) - 1, self._start)
        if lo_i >= len(self.v):
            return None
        window = self.v[lo_i:]
        return min(window), max(window)

    def first_time(self) -> int | None:
        return self.t[self._start] if len(self.t) > self._start else None

    def clear(self) -> None:
        del self.t[:]
        del self.v[:]
        self._start = 0

    def __len__(self) -> int:
        return len(self.t) - self._start


@dataclass(frozen=True, slots=True)
class SignalWeights:
    book_imbalance: float
    trade_flow: float
    micro_momentum: float
    bbo_momentum: float
    microprice: float
    volume_accel: float
    depth_change: float

    def total(self) -> float:
        return (
            self.book_imbalance
            + self.trade_flow
            + self.micro_momentum
            + self.bbo_momentum
            + self.microprice
            + self.volume_accel
            + self.depth_change
        )


@dataclass(frozen=True, slots=True)
class Signal:
    """One evaluation of the entry signal. ``side``: +1 long, -1 short, 0 none."""

    side: int
    score: float
    book_imbalance: float
    trade_flow: float
    micro_momentum: float
    bbo_momentum: float
    microprice: float
    volume_accel: float
    depth_change: float

    def components(self) -> dict[str, float]:
        return {
            "book_imbalance": round(self.book_imbalance, 4),
            "trade_flow": round(self.trade_flow, 4),
            "micro_momentum": round(self.micro_momentum, 4),
            "bbo_momentum": round(self.bbo_momentum, 4),
            "microprice": round(self.microprice, 4),
            "volume_accel": round(self.volume_accel, 4),
            "depth_change": round(self.depth_change, 4),
        }


NO_SIGNAL = Signal(0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)


def _clamp(x: float) -> float:
    return 1.0 if x > 1.0 else (-1.0 if x < -1.0 else x)


class SignalEngine:
    """Rolling microstructure state and the weighted entry score."""

    def __init__(
        self,
        weights: SignalWeights,
        threshold: float,
        depth_levels: int,
        momentum_scale_bps: float,
        warmup_ms: int = 5000,
    ) -> None:
        self._w = weights
        self._w_total = weights.total()
        self._threshold = threshold
        self._levels = depth_levels
        self._momentum_scale = momentum_scale_bps
        self._warmup_ns = warmup_ms * NS_PER_MS
        self._mid = Series(RETAIN_MS)  # bid + ask (twice the mid), exchange price units
        self._bid_depth = Series(RETAIN_MS)
        self._ask_depth = Series(RETAIN_MS)
        self._bbo_ticks: deque[tuple[int, int]] = deque(maxlen=4096)  # (t_ns, +1 / -1)
        self._trades: deque[tuple[int, int]] = deque(maxlen=MAX_TRADES_KEPT)  # (t_ns, signed size)
        self._last_bid = 0
        self._last_ask = 0
        self._bid_sz = 0
        self._ask_sz = 0
        self._bid_depth_now = 0
        self._ask_depth_now = 0
        self._since_ns = 0

    # ----------------------------------------------------------------- updates

    def reset(self) -> None:
        """Drop all rolling state (after a market-data resync)."""
        self._mid.clear()
        self._bid_depth.clear()
        self._ask_depth.clear()
        self._bbo_ticks.clear()
        self._trades.clear()
        self._last_bid = 0
        self._last_ask = 0
        self._since_ns = 0

    def on_book(self, now_ns: int, bids: Sequence[tuple[int, int]], asks: Sequence[tuple[int, int]]) -> None:
        """Record the current top of book (best-first level lists)."""
        if not bids or not asks:
            return
        bid, bid_sz = bids[0]
        ask, ask_sz = asks[0]
        if self._since_ns == 0:
            self._since_ns = now_ns
        if self._last_bid:
            if bid > self._last_bid:
                self._bbo_ticks.append((now_ns, 1))
            elif bid < self._last_bid:
                self._bbo_ticks.append((now_ns, -1))
            if ask > self._last_ask:
                self._bbo_ticks.append((now_ns, 1))
            elif ask < self._last_ask:
                self._bbo_ticks.append((now_ns, -1))
        if bid != self._last_bid or ask != self._last_ask:
            self._mid.append(now_ns, float(bid + ask))
        self._last_bid = bid
        self._last_ask = ask
        self._bid_sz = bid_sz
        self._ask_sz = ask_sz
        n = self._levels
        bid_depth = 0
        for _, size in bids[:n]:
            bid_depth += size
        ask_depth = 0
        for _, size in asks[:n]:
            ask_depth += size
        if bid_depth != self._bid_depth_now:
            self._bid_depth.append(now_ns, float(bid_depth))
            self._bid_depth_now = bid_depth
        if ask_depth != self._ask_depth_now:
            self._ask_depth.append(now_ns, float(ask_depth))
            self._ask_depth_now = ask_depth

    def on_trade(self, now_ns: int, size: int, taker_is_buyer: bool) -> None:
        """Record one public trade with its aggressor side."""
        self._trades.append((now_ns, size if taker_is_buyer else -size))

    # ------------------------------------------------------------------- reads

    def ready(self, now_ns: int) -> bool:
        """True once enough history exists for every window used by the score."""
        return self._since_ns != 0 and now_ns - self._since_ns >= self._warmup_ns

    def range_mbps(self, now_ns: int, window_ms: int = VOLATILITY_WINDOW_MS) -> int:
        """Mid-price high-low range over the window, in milli-bps (short-term volatility)."""
        result = self._mid.range_since(now_ns - window_ms * NS_PER_MS)
        if result is None:
            return 0
        low, high = result
        if low <= 0:
            return 0
        return int((high - low) / low * 10_000_000)

    def _flow(self, now_ns: int, window_ms: int) -> tuple[int, int, int]:
        """(buy volume, sell volume, trade count) inside the window."""
        cutoff = now_ns - window_ms * NS_PER_MS
        buys = 0
        sells = 0
        count = 0
        for t_ns, signed in reversed(self._trades):
            if t_ns < cutoff:
                break
            count += 1
            if signed > 0:
                buys += signed
            else:
                sells -= signed
        return buys, sells, count

    def compute(self, now_ns: int) -> Signal:
        """Evaluate the weighted score on the current rolling state."""
        if not self.ready(now_ns) or self._w_total <= 0:
            return NO_SIGNAL
        w = self._w

        # 1. Order-book imbalance over the top N levels.
        depth = self._bid_depth_now + self._ask_depth_now
        book_imb = (self._bid_depth_now - self._ask_depth_now) / depth if depth else 0.0

        # 2. Aggressive trade-flow imbalance (1 s).
        buys, sells, count = self._flow(now_ns, FLOW_WINDOW_MS)
        flow = (buys - sells) / (buys + sells) if count >= MIN_FLOW_TRADES and buys + sells else 0.0

        # 3. Micro momentum: mean mid return over 100/250/500/1000 ms, in bps.
        mid_now = float(self._last_bid + self._last_ask)
        total_bps = 0.0
        samples = 0
        for window in MOMENTUM_WINDOWS_MS:
            then = self._mid.value_at(now_ns - window * NS_PER_MS)
            if then:
                total_bps += (mid_now - then) / then * 10_000.0
                samples += 1
        momentum = _clamp(total_bps / samples / self._momentum_scale) if samples else 0.0

        # 4. BBO momentum: net direction of best bid/ask moves (1 s).
        cutoff = now_ns - BBO_WINDOW_MS * NS_PER_MS
        ups = 0
        downs = 0
        for t_ns, direction in reversed(self._bbo_ticks):
            if t_ns < cutoff:
                break
            if direction > 0:
                ups += 1
            else:
                downs += 1
        bbo = (ups - downs) / (ups + downs) if ups + downs >= MIN_BBO_TICKS else 0.0

        # 5. Microprice versus mid, in half-spreads. Equals top-of-book size imbalance.
        top = self._bid_sz + self._ask_sz
        micro = (self._bid_sz - self._ask_sz) / top if top else 0.0

        # 6. Directional volume acceleration: 250 ms rate versus the 5 s baseline.
        s_buys, s_sells, _ = self._flow(now_ns, ACCEL_SHORT_MS)
        l_buys, l_sells, _ = self._flow(now_ns, ACCEL_LONG_MS)
        accel = 0.0
        long_volume = l_buys + l_sells
        if long_volume and s_buys != s_sells:
            ratio = ((s_buys + s_sells) / ACCEL_SHORT_MS) / (long_volume / ACCEL_LONG_MS)
            magnitude = (ratio - 1.0) / ACCEL_FULL_SCALE
            if magnitude > 0.0:
                accel = min(magnitude, 1.0) * (1.0 if s_buys > s_sells else -1.0)

        # 7. Depth change (500 ms): bids added / asks pulled is upward pressure.
        depth_change = 0.0
        then_ns = now_ns - DEPTH_CHANGE_WINDOW_MS * NS_PER_MS
        bid_then = self._bid_depth.value_at(then_ns)
        ask_then = self._ask_depth.value_at(then_ns)
        if bid_then and ask_then:
            bid_delta = (self._bid_depth_now - bid_then) / bid_then
            ask_delta = (self._ask_depth_now - ask_then) / ask_then
            depth_change = _clamp(bid_delta - ask_delta)

        score = (
            w.book_imbalance * book_imb
            + w.trade_flow * flow
            + w.micro_momentum * momentum
            + w.bbo_momentum * bbo
            + w.microprice * micro
            + w.volume_accel * accel
            + w.depth_change * depth_change
        ) / self._w_total
        side = 1 if score >= self._threshold else (-1 if score <= -self._threshold else 0)
        return Signal(side, score, book_imb, flow, momentum, bbo, micro, accel, depth_change)
