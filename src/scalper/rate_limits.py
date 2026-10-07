"""Rate-limit accounting with capacity reserved for exits.

Lighter limits are per account tier (queried at startup via ``accountLimits``):

* standard:        60 requests per rolling minute, sendTx included
* plus / premium:  24,000 weighted requests per minute, at least 4,000 sendTx
                   per minute, plus a volume quota reported on every sendTx reply

This limiter tracks usage locally in a rolling window. Entries are refused
while the remaining headroom is not enough for the entry, its exit and a fixed
reserve (retries, cancel, emergency flatten, reconciliation). Exits are never
blocked here: EXIT > ENTRY.
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

RESERVED_TX = 6  # exit, two retries, cancel, emergency flatten, spare
RESERVED_READS = 4  # account, active orders, order lookup, nonce
VOLUME_QUOTA_RESERVE = 20
PENALTY_SECONDS = 15.0


@dataclass(frozen=True, slots=True)
class TierLimits:
    name: str
    request_capacity: int  # weighted units per minute
    tx_capacity: int | None  # separate sendTx cap per minute, None if it shares request_capacity
    tx_weight: int
    read_weight: int
    has_volume_quota: bool


def limits_for_tier(tier: str, override_per_minute: int | None = None) -> TierLimits:
    """Documented limits for an account tier. Unknown tiers get the strictest (standard) limits."""
    name = (tier or "").lower()
    if "prem" in name or "plus" in name or "build" in name:
        label = "premium" if "prem" in name else ("plus" if "plus" in name else "builder")
        capacity = 240_000 if label == "builder" else 24_000
        limits = TierLimits(label, capacity, 4000, 6, 300, True)
    else:
        limits = TierLimits("standard", 60, None, 1, 1, False)
    if override_per_minute is not None:
        limits = TierLimits(
            limits.name,
            override_per_minute,
            limits.tx_capacity,
            limits.tx_weight,
            limits.read_weight,
            limits.has_volume_quota,
        )
    return limits


class RollingBudget:
    """Weighted usage inside a rolling time window."""

    __slots__ = ("_events", "_used", "_window", "capacity")

    def __init__(self, capacity: int, window_s: float = 60.0) -> None:
        self.capacity = capacity
        self._window = window_s
        self._events: deque[tuple[float, int]] = deque()
        self._used = 0

    def used(self, now: float) -> int:
        cutoff = now - self._window
        events = self._events
        while events and events[0][0] <= cutoff:
            self._used -= events.popleft()[1]
        return self._used

    def add(self, cost: int, now: float) -> None:
        self._events.append((now, cost))
        self._used += cost

    def headroom(self, now: float) -> int:
        return self.capacity - self.used(now)


class RateLimiter:
    """Tracks request/transaction usage and answers "may a new entry be sent?"."""

    def __init__(
        self,
        tier: TierLimits,
        max_entries_per_minute: int,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.tier = tier
        self._clock = clock
        self._requests = RollingBudget(tier.request_capacity)
        self._txs = RollingBudget(tier.tx_capacity) if tier.tx_capacity is not None else None
        self._entries = RollingBudget(max_entries_per_minute)
        self._penalty_until = 0.0
        self.volume_quota: int | None = None

    def set_tier(self, tier: TierLimits) -> None:
        """Adopt the limits of the account tier reported by the exchange."""
        self.tier = tier
        self._requests.capacity = tier.request_capacity
        if tier.tx_capacity is None:
            self._txs = None
        elif self._txs is None:
            self._txs = RollingBudget(tier.tx_capacity)
        else:
            self._txs.capacity = tier.tx_capacity

    # ----------------------------------------------------------------- record

    def note_tx(self) -> None:
        """A signed transaction was sent (order, cancel, leverage update)."""
        now = self._clock()
        self._requests.add(self.tier.tx_weight, now)
        if self._txs is not None:
            self._txs.add(1, now)

    def note_read(self) -> None:
        """A REST read was sent."""
        self._requests.add(self.tier.read_weight, self._clock())

    def note_entry(self) -> None:
        self._entries.add(1, self._clock())

    def note_volume_quota(self, remaining: int | None) -> None:
        if remaining is not None and self.tier.has_volume_quota:
            self.volume_quota = remaining

    def penalize(self, seconds: float = PENALTY_SECONDS) -> None:
        """The exchange answered with a rate-limit error: stop sending entries for a while."""
        self._penalty_until = max(self._penalty_until, self._clock() + seconds)

    # ------------------------------------------------------------------ query

    def _reserve_cost(self) -> int:
        return RESERVED_TX * self.tier.tx_weight + RESERVED_READS * self.tier.read_weight

    def _has_headroom(self, tx_count: int) -> bool:
        now = self._clock()
        if now < self._penalty_until:
            return False
        if self._requests.headroom(now) < tx_count * self.tier.tx_weight + self._reserve_cost():
            return False
        if self._txs is not None and self._txs.headroom(now) < tx_count + RESERVED_TX:
            return False
        if self.tier.has_volume_quota and self.volume_quota is not None:
            if self.volume_quota < tx_count + VOLUME_QUOTA_RESERVE:
                return False
        return True

    def can_enter(self, tx_count: int = 2) -> bool:
        """True if an entry *and* its exit fit while leaving the exit reserve untouched.

        ``tx_count`` is 3 for a resting entry, which may also need a cancel.
        """
        if self._entries.headroom(self._clock()) < 1:
            return False
        return self._has_headroom(tx_count)

    def can_send_optional(self) -> bool:
        """True if one opportunistic transaction fits without eating the exit reserve."""
        return self._has_headroom(1)

    def in_penalty(self) -> bool:
        return self._clock() < self._penalty_until

    def snapshot(self) -> dict[str, int | str | None]:
        now = self._clock()
        return {
            "tier": self.tier.name,
            "request_headroom": self._requests.headroom(now),
            "request_capacity": self._requests.capacity,
            "tx_headroom": self._txs.headroom(now) if self._txs is not None else None,
            "entries_last_minute": self._entries.used(now),
            "volume_quota": self.volume_quota,
            "penalty_s": max(0, round(self._penalty_until - now)),
        }
