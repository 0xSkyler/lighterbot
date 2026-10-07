"""Local BTC order book.

Maintained from Lighter's ``order_book/{market}`` channel: one full snapshot on
subscribe, then deltas (absolute size per price, size 0 = level removed) batched
roughly every 50 ms. Continuity is checked by matching each delta's
``begin_nonce`` against the previous message's ``nonce``.

The faster ``ticker/{market}`` channel (best bid/offer only) is applied as a
non-destructive *overlay*: while its nonce is newer than the book's, reads see
the ticker's BBO and ignore book levels that the ticker proves are gone. The
underlying book is never mutated by ticker data, so a later delta can never
leave it inconsistent.

All prices/sizes are exchange-native integers (see ``precision.py``).
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right, insort
from collections.abc import Iterable

Level = tuple[int, int]  # (price, size)


class OrderBook:
    """Price-level book with sorted price arrays and O(log n) updates."""

    __slots__ = (
        "_ask_px",
        "_ask_sz",
        "_bbo_ask_px",
        "_bbo_ask_sz",
        "_bbo_bid_px",
        "_bbo_bid_sz",
        "_bbo_nonce",
        "_bid_px",
        "_bid_sz",
        "_max_levels",
        "nonce",
        "valid",
        "version",
    )

    def __init__(self, max_levels_per_side: int = 5000) -> None:
        self._bid_px: list[int] = []  # ascending; best bid is the last element
        self._bid_sz: dict[int, int] = {}
        self._ask_px: list[int] = []  # ascending; best ask is the first element
        self._ask_sz: dict[int, int] = {}
        self.nonce = 0
        self.valid = False
        self.version = 0  # bumped on every visible change
        self._bbo_nonce = 0
        self._bbo_bid_px = 0
        self._bbo_bid_sz = 0
        self._bbo_ask_px = 0
        self._bbo_ask_sz = 0
        self._max_levels = max_levels_per_side

    # ------------------------------------------------------------------ writes

    def invalidate(self) -> None:
        """Mark the book unusable until the next snapshot (disconnect, gap, crossed book)."""
        self.valid = False
        self._bbo_nonce = 0
        self.version += 1

    def apply_snapshot(self, bids: Iterable[Level], asks: Iterable[Level], nonce: int) -> None:
        """Replace the whole book."""
        self._bid_sz = {p: s for p, s in bids if s > 0}
        self._ask_sz = {p: s for p, s in asks if s > 0}
        self._bid_px = sorted(self._bid_sz)
        self._ask_px = sorted(self._ask_sz)
        self.nonce = nonce
        self._bbo_nonce = 0
        self.valid = bool(self._bid_px) and bool(self._ask_px) and not self._crossed()
        self.version += 1

    def apply_delta(self, bids: Iterable[Level], asks: Iterable[Level], begin_nonce: int, nonce: int) -> bool:
        """Apply one delta. Returns False (and invalidates) on a sequence gap or crossed book."""
        if not self.valid:
            return False
        if begin_nonce != self.nonce:
            self.invalidate()
            return False
        for price, size in bids:
            self._set(self._bid_px, self._bid_sz, price, size)
        for price, size in asks:
            self._set(self._ask_px, self._ask_sz, price, size)
        self.nonce = nonce
        if self._bbo_nonce <= nonce:
            self._bbo_nonce = 0  # the book has caught up with the ticker overlay
        if not self._bid_px or not self._ask_px or self._crossed():
            self.invalidate()
            return False
        self._trim()
        self.version += 1
        return True

    def apply_bbo(self, bid_px: int, bid_sz: int, ask_px: int, ask_sz: int, nonce: int) -> bool:
        """Overlay a ticker BBO. Ignored unless strictly newer than the book and the last overlay."""
        if not self.valid or nonce <= self.nonce or nonce <= self._bbo_nonce:
            return False
        if bid_px <= 0 or ask_px <= 0 or bid_px >= ask_px:
            return False
        self._bbo_nonce = nonce
        self._bbo_bid_px = bid_px
        self._bbo_bid_sz = bid_sz
        self._bbo_ask_px = ask_px
        self._bbo_ask_sz = ask_sz
        self.version += 1
        return True

    @staticmethod
    def _set(px: list[int], sz: dict[int, int], price: int, size: int) -> None:
        if size > 0:
            if price not in sz:
                insort(px, price)
            sz[price] = size
        elif price in sz:
            del sz[price]
            del px[bisect_left(px, price)]

    def _crossed(self) -> bool:
        return bool(self._bid_px and self._ask_px and self._bid_px[-1] >= self._ask_px[0])

    def _trim(self) -> None:
        """Bound memory: drop the levels furthest from the touch beyond the cap."""
        excess = len(self._bid_px) - self._max_levels
        if excess > 0:
            for price in self._bid_px[:excess]:
                del self._bid_sz[price]
            del self._bid_px[:excess]
        excess = len(self._ask_px) - self._max_levels
        if excess > 0:
            for price in self._ask_px[-excess:]:
                del self._ask_sz[price]
            del self._ask_px[-excess:]

    # ------------------------------------------------------------------- reads

    def top_bids(self, n: int) -> list[Level]:
        """Up to ``n`` best bids, best (highest) first."""
        px = self._bid_px
        sz = self._bid_sz
        out: list[Level] = []
        hi = len(px)
        if self._bbo_nonce:
            best = self._bbo_bid_px
            if self._bbo_bid_sz > 0:
                out.append((best, self._bbo_bid_sz))
            hi = bisect_left(px, best)  # book bids strictly below the ticker's best bid
        i = hi - 1
        while i >= 0 and len(out) < n:
            price = px[i]
            out.append((price, sz[price]))
            i -= 1
        return out

    def top_asks(self, n: int) -> list[Level]:
        """Up to ``n`` best asks, best (lowest) first."""
        px = self._ask_px
        sz = self._ask_sz
        out: list[Level] = []
        lo = 0
        if self._bbo_nonce:
            best = self._bbo_ask_px
            if self._bbo_ask_sz > 0:
                out.append((best, self._bbo_ask_sz))
            lo = bisect_right(px, best)  # book asks strictly above the ticker's best ask
        end = len(px)
        i = lo
        while i < end and len(out) < n:
            price = px[i]
            out.append((price, sz[price]))
            i += 1
        return out

    def best_bid(self) -> int:
        """Best bid price, 0 if unavailable."""
        if self._bbo_nonce:
            return self._bbo_bid_px
        return self._bid_px[-1] if self._bid_px else 0

    def best_ask(self) -> int:
        """Best ask price, 0 if unavailable."""
        if self._bbo_nonce:
            return self._bbo_ask_px
        return self._ask_px[0] if self._ask_px else 0

    def level_counts(self) -> tuple[int, int]:
        return len(self._bid_px), len(self._ask_px)


def spread_mbps(bid: int, ask: int) -> int:
    """Quoted spread relative to mid, in milli-bps. Returns a huge value for an unusable quote."""
    if bid <= 0 or ask <= bid:
        return 10**12
    # (ask - bid) / ((ask + bid) / 2) * 10_000_000
    return (ask - bid) * 20_000_000 // (ask + bid)
