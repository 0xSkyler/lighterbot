"""Explicit finite state machine for the single-position lifecycle.

All transitions happen synchronously on the asyncio event loop thread with no
``await`` between the check and the assignment, which makes them atomic with
respect to every other coroutine. A new entry is only possible from ``FLAT``.
"""

from __future__ import annotations

import enum
import time
from collections import deque
from collections.abc import Callable


class State(enum.Enum):
    STARTING = "STARTING"
    SYNCING = "SYNCING"
    FLAT = "FLAT"
    ENTRY_PENDING = "ENTRY_PENDING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    OPEN_LONG = "OPEN_LONG"
    OPEN_SHORT = "OPEN_SHORT"
    EXIT_PENDING = "EXIT_PENDING"
    PARTIAL_EXIT = "PARTIAL_EXIT"
    RECOVERY = "RECOVERY"
    HALTED = "HALTED"


S = State

_ALLOWED: dict[State, frozenset[State]] = {
    S.STARTING: frozenset({S.SYNCING, S.HALTED}),
    S.SYNCING: frozenset({S.FLAT, S.OPEN_LONG, S.OPEN_SHORT, S.RECOVERY, S.HALTED}),
    S.FLAT: frozenset({S.ENTRY_PENDING, S.SYNCING, S.RECOVERY, S.HALTED}),
    S.ENTRY_PENDING: frozenset({S.PARTIALLY_FILLED, S.OPEN_LONG, S.OPEN_SHORT, S.FLAT, S.RECOVERY, S.HALTED}),
    S.PARTIALLY_FILLED: frozenset({S.OPEN_LONG, S.OPEN_SHORT, S.EXIT_PENDING, S.RECOVERY, S.HALTED}),
    S.OPEN_LONG: frozenset({S.EXIT_PENDING, S.RECOVERY, S.HALTED}),
    S.OPEN_SHORT: frozenset({S.EXIT_PENDING, S.RECOVERY, S.HALTED}),
    S.EXIT_PENDING: frozenset({S.PARTIAL_EXIT, S.FLAT, S.OPEN_LONG, S.OPEN_SHORT, S.RECOVERY, S.HALTED}),
    S.PARTIAL_EXIT: frozenset({S.EXIT_PENDING, S.FLAT, S.OPEN_LONG, S.OPEN_SHORT, S.RECOVERY, S.HALTED}),
    S.RECOVERY: frozenset({S.FLAT, S.OPEN_LONG, S.OPEN_SHORT, S.SYNCING, S.HALTED}),
    S.HALTED: frozenset(),
}

# States in which the bot holds (or may hold) exposure that the exit engine must manage.
EXPOSED_STATES = frozenset({S.PARTIALLY_FILLED, S.OPEN_LONG, S.OPEN_SHORT})
# States in which an order of ours is unresolved.
PENDING_STATES = frozenset({S.ENTRY_PENDING, S.PARTIALLY_FILLED, S.EXIT_PENDING, S.PARTIAL_EXIT})


class InvalidTransition(Exception):
    """Raised when code attempts a transition the lifecycle does not allow."""


class StateMachine:
    """Holds the current :class:`State` and enforces the transition table."""

    __slots__ = ("_listener", "history", "seq", "since_ns", "state")

    def __init__(self, listener: Callable[[State, State, str], None] | None = None) -> None:
        self.state = State.STARTING
        self.since_ns = time.monotonic_ns()
        self.seq = 0  # incremented on every transition; lets async code detect that state moved on
        self.history: deque[tuple[int, str, str, str]] = deque(maxlen=64)
        self._listener = listener

    def can(self, new: State) -> bool:
        return new in _ALLOWED[self.state]

    def transition(self, new: State, reason: str) -> None:
        """Move to ``new`` or raise :class:`InvalidTransition`. No awaits: atomic on the loop."""
        old = self.state
        if new not in _ALLOWED[old]:
            raise InvalidTransition(f"{old.value} -> {new.value} ({reason})")
        self.state = new
        self.seq += 1
        self.since_ns = time.monotonic_ns()
        self.history.append((self.since_ns, old.value, new.value, reason))
        if self._listener is not None:
            self._listener(old, new, reason)

    def try_transition(self, new: State, reason: str) -> bool:
        """Transition if allowed; return whether it happened."""
        if new not in _ALLOWED[self.state]:
            return False
        self.transition(new, reason)
        return True

    @property
    def is_flat(self) -> bool:
        return self.state is State.FLAT

    @property
    def is_exposed(self) -> bool:
        return self.state in EXPOSED_STATES
