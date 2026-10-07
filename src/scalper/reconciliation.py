"""Reconciliation of local state against the exchange.

The exchange is the authority. On every start, after every account-stream
reconnect, periodically while flat, and whenever local and exchange state
disagree, the bot reads the account and active orders over REST and decides
what to do with :func:`decide`, a pure function that is unit tested.

Local state is never trusted to conclude "flat": only an authoritative read can.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .errors import ApiError, ErrorClass
from .market_data import Backoff
from .position import ExchangePosition, OrderUpdate, is_bot_order
from .state_machine import State

if TYPE_CHECKING:
    from .config import Config
    from .execution import Executor, FlattenResult
    from .lighter_client import AccountSnapshot, LighterClient
    from .strategy import Trader

log = logging.getLogger("scalper.reconcile")

_sleep = asyncio.sleep  # indirection so tests can run the timing-dependent paths instantly

ORDER_SETTLE_S = 1.0  # an in-flight order may wait this long in the sequencer speed-bump queue
LEVERAGE_CONFIRM_POLLS = 4
LEVERAGE_CONFIRM_INTERVAL_S = 0.7
SYNC_RETRY_S = 3.0


class Action(enum.Enum):
    RESUME_FLAT = "RESUME_FLAT"  # no position: normal operation
    ADOPT_POSITION = "ADOPT_POSITION"  # manage the exchange position with the exit engine
    FLATTEN = "FLATTEN"  # close the exchange position now
    HALT = "HALT"  # leave everything untouched and stop trading


@dataclass(frozen=True, slots=True)
class Decision:
    action: Action
    reason: str
    bot_orders: tuple[int, ...]  # client order indices of stale bot orders to cancel
    foreign_orders: int  # resting orders this bot did not create


def decide(
    position: ExchangePosition,
    active_orders: list[OrderUpdate],
    *,
    local_signed_size: int | None,
    existing_position_action: str,
) -> Decision:
    """Choose how to proceed given authoritative exchange state.

    Args:
        position: the BTC position reported by the exchange.
        active_orders: resting/pending BTC orders of the account.
        local_signed_size: the position this process believes it holds, or None
            when it has no knowledge (fresh start).
        existing_position_action: ``manage`` | ``flatten`` | ``halt`` - what to do
            with a position this process did not open itself.
    """
    bot_orders = tuple(o.client_order_index for o in active_orders if is_bot_order(o.client_order_index))
    foreign = sum(1 for o in active_orders if not is_bot_order(o.client_order_index))
    exchange = position.signed_size

    if exchange == 0:
        return Decision(Action.RESUME_FLAT, "exchange_flat", bot_orders, foreign)

    if local_signed_size is not None and local_signed_size != 0:
        if local_signed_size == exchange:
            return Decision(Action.ADOPT_POSITION, "matches_local", bot_orders, foreign)
        # We hold a position but the exchange disagrees about it: do not guess, get flat.
        return Decision(Action.FLATTEN, "size_mismatch", bot_orders, foreign)

    # A position exists that this process did not open (restart, late fill, manual trade).
    if existing_position_action == "halt":
        return Decision(Action.HALT, "existing_position", bot_orders, foreign)
    if existing_position_action == "flatten":
        return Decision(Action.FLATTEN, "existing_position", bot_orders, foreign)
    return Decision(Action.ADOPT_POSITION, "existing_position", bot_orders, foreign)


def leverage_matches(position: ExchangePosition, imf: int, margin_mode: int) -> bool:
    """True when the exchange reports exactly the configured leverage and margin mode."""
    return position.imf == imf and position.margin_mode == margin_mode


class Reconciler:
    """Runs reconciliation and recovery. At most one of the two is active at any time.

    * ``request_resync``: read the exchange, then resume / adopt / flatten / halt.
    * ``start_recovery``: state is uncertain or cannot be managed -> flatten via
      :meth:`Executor.flatten_position` and only then return to FLAT.
    """

    def __init__(self, trader: Trader, client: LighterClient, executor: Executor, cfg: Config) -> None:
        self._trader = trader
        self._client = client
        self._executor = executor
        self._cfg = cfg
        self._task: asyncio.Task[None] | None = None
        self._recovering = False
        self._queued_recovery: str | None = None
        self._queued_resync: str | None = None
        self._retry_handle: asyncio.TimerHandle | None = None
        self.leverage_confirmed = False
        self.last_sync_monotonic = 0.0

    # ---------------------------------------------------------------- control

    @property
    def busy(self) -> bool:
        return self._task is not None and not self._task.done()

    def start_recovery(self, reason: str) -> None:
        """Flatten through authoritative state. Safe to call repeatedly."""
        if self._trader.sm.state is State.HALTED:
            return
        if self.busy:
            if not self._recovering and self._queued_recovery is None:
                self._queued_recovery = reason  # runs as soon as the current resync returns
            return
        self._launch_recovery(reason)

    def request_resync(self, reason: str) -> None:
        """Reconcile against the exchange. Coalesced if something is already running."""
        if self._trader.sm.state is State.HALTED:
            return
        if self.busy:
            if not self._recovering:
                self._queued_resync = reason
            return
        self._launch(self._resync(reason))

    async def startup(self) -> None:
        """Mandatory reconciliation before the first trade of a process."""
        self._launch(self._resync("STARTUP"))
        await self.wait_idle()

    async def wait_idle(self) -> None:
        while self.busy:
            assert self._task is not None
            await asyncio.wait({self._task})
            await asyncio.sleep(0)  # let the done-callback start any queued follow-up

    def _launch_recovery(self, reason: str) -> None:
        trader = self._trader
        self._recovering = True
        trader.metrics.recoveries += 1
        trader.sm.try_transition(State.RECOVERY, reason)
        self._launch(self._recover(reason))

    def _launch(self, coro: Any) -> None:
        self._task = asyncio.get_running_loop().create_task(coro)
        self._task.add_done_callback(self._on_done)

    def _on_done(self, task: asyncio.Task[None]) -> None:
        was_recovery = self._recovering
        self._recovering = False
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            log.critical("RECONCILER_CRASH recovery=%s error=%r", was_recovery, error, exc_info=error)
            # Never leave exposure unmanaged because of a bug: go through the flatten path
            # (after a pause if the flatten path itself is what failed).
            if was_recovery:
                self._retry_handle = asyncio.get_running_loop().call_later(5.0, self.start_recovery, "RECONCILER_CRASH")
                return
            self._queued_recovery = self._queued_recovery or "RECONCILER_CRASH"
        if self._queued_recovery is not None:
            reason, self._queued_recovery = self._queued_recovery, None
            self._queued_resync = None
            if self._trader.sm.state is not State.HALTED:
                self._launch_recovery(reason)
        elif self._queued_resync is not None:
            reason, self._queued_resync = self._queued_resync, None
            self.request_resync(reason)

    def _retry_later(self, reason: str) -> None:
        if self._retry_handle is not None:
            self._retry_handle.cancel()
        self._retry_handle = asyncio.get_running_loop().call_later(SYNC_RETRY_S, self.request_resync, reason)

    def stop(self) -> None:
        if self._retry_handle is not None:
            self._retry_handle.cancel()
        if self._task is not None:
            self._task.cancel()

    # ----------------------------------------------------------------- resync

    async def _resync(self, reason: str) -> None:
        trader = self._trader
        cfg = self._cfg
        meta = trader.meta
        state0 = trader.sm.state
        # A periodic check while flat runs in the background; every other reason blocks
        # entries (FLAT -> SYNCING) until the exchange has answered.
        blocking = reason != "PERIODIC" or not self.leverage_confirmed
        if state0 is State.STARTING or (state0 is State.FLAT and blocking):
            trader.sm.transition(State.SYNCING, reason)
        elif state0 not in (State.FLAT, State.SYNCING, State.OPEN_LONG, State.OPEN_SHORT):
            return  # an order is in flight: its own events and timeouts resolve it
        seq = trader.sm.seq
        position0 = trader.position
        try:
            snapshot = await self._client.fetch_account(meta)
            orders = await self._client.fetch_active_orders(meta)
        except ApiError as exc:
            log.error("RECONCILE_FAILED reason=%s class=%s error=%s", reason, exc.error_class.value, exc)
            if exc.error_class is ErrorClass.AUTH_ERROR:
                trader.halt(f"authentication failed during reconciliation: {exc}", exit_process=True)
            else:
                self._retry_later(reason)  # entries stay blocked while the state is SYNCING
            return
        if trader.sm.seq != seq or trader.position is not position0:
            return  # trading moved on while we were reading; this view is already stale

        trader.exch_pos = snapshot.position
        trader.available_q = meta.usd_to_q(snapshot.available_balance)
        self.last_sync_monotonic = time.monotonic()
        decision = decide(
            snapshot.position,
            orders,
            local_signed_size=position0.signed_size if position0 is not None else None,
            existing_position_action=cfg.existing_position_action,
        )
        if reason != "PERIODIC" or decision.action is not Action.RESUME_FLAT or orders:
            log.info(
                "RECONCILE reason=%s exchange_position=%s active_orders=%d bot_orders=%d foreign_orders=%d action=%s",
                reason,
                meta.fmt_size(snapshot.position.signed_size),
                len(orders),
                len(decision.bot_orders),
                decision.foreign_orders,
                decision.action.value,
            )
        if decision.bot_orders or (decision.foreign_orders and cfg.cancel_foreign_orders):
            try:
                await self._executor.cancel_resting_orders(cancel_all=cfg.cancel_foreign_orders)
            except ApiError as exc:
                log.error("CANCEL_FAILED class=%s error=%s", exc.error_class.value, exc)
            if trader.sm.seq != seq or trader.position is not position0:
                return
        if decision.foreign_orders and not cfg.cancel_foreign_orders:
            trader.blocks["FOREIGN_ORDERS"] = f"{decision.foreign_orders} resting BTC order(s) not created by this bot"
        else:
            trader.blocks.pop("FOREIGN_ORDERS", None)

        action = decision.action
        if action is Action.RESUME_FLAT:
            if position0 is not None:
                self._abandon_local_position(reason)
            else:
                await self._resume_flat(snapshot, seq)
        elif action is Action.ADOPT_POSITION:
            if position0 is None:
                trader.blocks.pop("SYNCING", None)
                trader.sm.try_transition(State.SYNCING, reason)  # FLAT -> SYNCING after a background check
                trader.adopt_position(snapshot.position)
        elif action is Action.FLATTEN:
            trader.blocks.pop("SYNCING", None)
            self._queued_recovery = f"RECONCILE_{decision.reason.upper()}"
        else:
            trader.halt("a BTC position already exists and EXISTING_POSITION_ACTION=halt")

    async def _resume_flat(self, snapshot: AccountSnapshot, seq: int) -> None:
        trader = self._trader
        if trader.sm.state is not State.SYNCING:
            return  # the background check confirmed what we already believed
        if not self.leverage_confirmed:
            await self._ensure_leverage(snapshot)
            if trader.sm.seq != seq:
                return
        trader.blocks.pop("SYNCING", None)
        trader.sm.transition(State.FLAT, "SYNCED")

    def _abandon_local_position(self, reason: str) -> None:
        """We believed we held a position but the exchange is flat (closed elsewhere or liquidated)."""
        trader = self._trader
        position = trader.position
        log.error(
            "POSITION_GONE reason=%s local_size=%s", reason, trader.meta.fmt_size(position.size if position else 0)
        )
        trader.metrics.count_error(ErrorClass.STATE_MISMATCH)
        trader.journal.record_event("TRADE_UNRESOLVED", {"reason": reason, "state": trader.snapshot_state(reason)})
        trader.sm.try_transition(State.RECOVERY, "EXCHANGE_FLAT")
        trader.clear_trade()
        trader.sm.try_transition(State.FLAT, "EXCHANGE_FLAT")

    async def _ensure_leverage(self, snapshot: AccountSnapshot) -> None:
        """Set and read back the configured leverage. Entries stay blocked until it is confirmed."""
        trader = self._trader
        cfg = self._cfg
        imf = trader.limits.imf
        mode = cfg.margin_mode
        position = snapshot.position
        if snapshot.has_market_entry and leverage_matches(position, imf, mode):
            self._leverage_ok()
            return
        if position.signed_size or position.open_orders or position.pending_orders:
            trader.blocks["LEVERAGE_UNCONFIRMED"] = "cannot change leverage while a position or order exists"
            return
        log.info(
            "LEVERAGE_UPDATE leverage=%dx imf=%d margin_mode=%s", cfg.leverage, imf, "isolated" if mode else "cross"
        )
        result = await self._executor.update_leverage(imf, mode)
        if not result.accepted:
            trader.blocks["LEVERAGE_UNCONFIRMED"] = f"leverage update not accepted: {result.message}"
            log.error(
                "LEVERAGE_UPDATE_FAILED class=%s message=%s",
                result.error_class.value if result.error_class else "",
                result.message,
            )
            if result.error_class is ErrorClass.AUTH_ERROR:
                trader.halt(f"exchange rejected credentials: {result.message}", exit_process=True)
            return
        for _ in range(LEVERAGE_CONFIRM_POLLS):
            await _sleep(LEVERAGE_CONFIRM_INTERVAL_S)
            try:
                check = await self._client.fetch_account(trader.meta)
            except ApiError as exc:
                log.error("LEVERAGE_CONFIRM_READ_FAILED error=%s", exc)
                continue
            if check.has_market_entry and leverage_matches(check.position, imf, mode):
                self._leverage_ok()
                return
        trader.blocks["LEVERAGE_UNCONFIRMED"] = "exchange did not report the configured leverage"
        log.error("LEVERAGE_UNCONFIRMED leverage=%dx: entries stay blocked", cfg.leverage)

    def _leverage_ok(self) -> None:
        self.leverage_confirmed = True
        self._trader.blocks.pop("LEVERAGE_UNCONFIRMED", None)
        log.info("LEVERAGE_CONFIRMED leverage=%dx imf=%d", self._cfg.leverage, self._trader.limits.imf)

    # --------------------------------------------------------------- recovery

    def _fresh_bbo(self) -> tuple[int, int] | None:
        trader = self._trader
        market = trader.market
        if not trader.book.valid or market.staleness_ms(time.monotonic_ns()) > self._cfg.market_data_stale_ms:
            return None
        if not market.top_bids or not market.top_asks:
            return None
        return market.top_bids[0][0], market.top_asks[0][0]

    async def _recover(self, reason: str) -> None:
        trader = self._trader
        log.error("RECOVERY_START reason=%s", reason)
        trader.journal.record_event("RECOVERY_START", {"reason": reason, "state": trader.snapshot_state(reason)})
        in_flight = [o for o in (trader.entry_order, trader.exit_order) if o is not None and not o.terminal]
        if in_flight:
            await _sleep(ORDER_SETTLE_S)
        backoff = Backoff(1.0, 30.0)
        while True:
            result = await self._executor.flatten_position(
                reason, get_bbo=self._fresh_bbo, on_order=trader.track_recovery_order
            )
            if result.flat:
                break
            if result.fatal:
                trader.halt(f"cannot flatten: {result.last_error}", exit_process=True)
                return
            # Exposure may still exist: keep trying for as long as the process lives.
            log.critical("RECOVERY_NOT_FLAT reason=%s last_error=%s", reason, result.last_error)
            await _sleep(backoff.next())
        await self._settle(result, reason)

    async def _settle(self, result: FlattenResult, reason: str) -> None:
        """The exchange confirmed zero exposure: book what we know and return to FLAT."""
        trader = self._trader
        # Orders whose outcome we never saw on the stream (entry first, then closes): read their
        # final fills back so the trade is booked from what really executed.
        unresolved = [o for o in (trader.entry_order, trader.exit_order) if o is not None]
        unresolved += [o for o in result.orders if o not in unresolved]
        finished: dict[int, OrderUpdate] = {}
        if any(o.filled < o.size for o in unresolved):
            try:
                finished = await self._client.fetch_recent_orders(trader.meta)
            except ApiError as exc:
                log.error("RECOVERY_FILL_LOOKUP_FAILED class=%s error=%s", exc.error_class.value, exc)
        for order in unresolved:  # entry first, so closing fills always find their position
            trader.apply_recovered_order(order, finished.get(order.client_order_index))
        now_ns = time.monotonic_ns()
        for pending in (trader.entry_order, trader.exit_order):
            if pending is not None and not pending.terminal:
                pending.mark_terminal("unresolved", now_ns)
        trader.entry_order = None
        trader.exit_order = None
        position = trader.position
        if position is not None and position.size == 0 and position.exit_size > 0:
            trader.finalize_trade(time.monotonic_ns(), reason)
        else:
            if position is not None:
                # Flat on the exchange, but our fills do not explain it. Do not invent a P&L.
                log.error("TRADE_UNRESOLVED reason=%s remaining_local=%s", reason, trader.meta.fmt_size(position.size))
                trader.journal.record_event(
                    "TRADE_UNRESOLVED", {"reason": reason, "state": trader.snapshot_state(reason)}
                )
            trader.clear_trade()
            trader.sm.try_transition(State.FLAT, "RECOVERED_FLAT")
        trader.recovery_orders.clear()
        if result.final_position is not None:
            trader.exch_pos = result.final_position
        log.warning("RECOVERY_DONE reason=%s attempts=%d", reason, result.attempts)
        trader.journal.record_event("RECOVERY_DONE", {"reason": reason, "attempts": result.attempts})
