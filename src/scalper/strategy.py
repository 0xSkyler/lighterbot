"""The trading core: entry engine, fill state machine and the immediate-profit exit engine.

Event flow (everything below runs synchronously on the event loop, so each
state check + transition is atomic):

    market event  -> on_market()      FLAT: evaluate entry      OPEN: evaluate exit
    account event -> on_order_update() / on_trade_fill() / on_position()

Priorities while exposure exists: hard-loss protection, then GREEN -> exit,
then the hold timeout. No entry signal is computed while a position is open.
Problems that leave state uncertain are never guessed at here; they are handed
to ``reconciliation.Reconciler`` (authoritative read, then flatten).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Coroutine
from functools import partial
from typing import Any, Protocol

from .config import Config
from .errors import ErrorClass, is_reduce_only_rejection
from .execution import Executor
from .lighter_client import SendResult
from .metrics import Metrics
from .orderbook import OrderBook
from .persistence import Journal, utc_iso
from .pnl import LONG, SHORT, CloseEstimate, breakeven_exit_price, estimate_close
from .position import (
    ActiveOrder,
    ClientOrderIds,
    ExchangePosition,
    OrderKind,
    OrderUpdate,
    Position,
    TradeContext,
    TradeFill,
    is_bot_order,
    is_terminal_status,
)
from .precision import MarketMeta, fee_q, price_minus_mbps, price_plus_mbps
from .rate_limits import RateLimiter
from .risk import EntryPlan, Limits, hard_loss_reason, hold_expired, plan_entry
from .signals import NO_SIGNAL, Signal, SignalEngine
from .state_machine import EXPOSED_STATES, State, StateMachine
from .trade_record import close_trade

log = logging.getLogger("scalper.strategy")

NS_PER_MS = 1_000_000
MISMATCH_GRACE_S = 0.5
RATE_LIMIT_RETRY_S = 0.5
REJECT_STREAK_LIMIT = 5
REJECT_STREAK_PAUSE_S = 30.0
MAX_TRACKED_ORDERS = 64


class MarketView(Protocol):
    """What the trader needs from the market-data stream."""

    top_bids: list[tuple[int, int]]
    top_asks: list[tuple[int, int]]
    event_ns: int

    def staleness_ms(self, now_ns: int) -> float: ...


class RecoveryHooks(Protocol):
    """Implemented by ``reconciliation.Reconciler``."""

    def start_recovery(self, reason: str) -> None: ...

    def request_resync(self, reason: str) -> None: ...

    @property
    def busy(self) -> bool: ...


class Trader:
    """Single-position BTC scalper state machine."""

    def __init__(
        self,
        *,
        cfg: Config,
        meta: MarketMeta,
        limits: Limits,
        book: OrderBook,
        market: MarketView,
        signals: SignalEngine,
        executor: Executor,
        limiter: RateLimiter,
        metrics: Metrics,
        journal: Journal,
        ids: ClientOrderIds,
        fee_tick: int,
        clock: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        self.cfg = cfg
        self.meta = meta
        self.limits = limits
        self.book = book
        self.market = market
        self.signals = signals
        self.executor = executor
        self.limiter = limiter
        self.metrics = metrics
        self.journal = journal
        self.ids = ids
        self.fee_tick = fee_tick
        self._clock = clock
        self.sm = StateMachine(self._on_transition)
        self.recovery: RecoveryHooks | None = None

        self.position: Position | None = None
        self.ctx: TradeContext | None = None
        self.entry_order: ActiveOrder | None = None
        self.exit_order: ActiveOrder | None = None
        self.recovery_orders: set[int] = set()  # client order indices of flatten orders in flight
        self._orders: dict[int, ActiveOrder] = {}

        # Entries are blocked while this dict is non-empty: {reason: detail}.
        self.blocks: dict[str, str] = {
            "SYNCING": "startup reconciliation pending",
            "LEVERAGE_UNCONFIRMED": "leverage not yet verified on the exchange",
            "ACCOUNT_STREAM_DOWN": "authenticated stream not connected",
            "MARKET_DATA_STALE": "no fresh market data yet",
        }
        self.exch_pos: ExchangePosition | None = None
        self.available_q: int | None = None
        self.last_signal: Signal = NO_SIGNAL
        self.shutting_down = False
        self.fatal_reason: str | None = None  # set when the process must exit and stay down

        self._exit_reason = ""
        self._exit_emergency = False
        self._exit_attempts = 0
        self._reject_streak = 0
        self._timers: dict[str, asyncio.TimerHandle] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._wake = asyncio.Event()
        self._price_fmt = f"%.{meta.price_decimals}f"
        self._size_fmt = f"%.{meta.size_decimals}f"

    # ====================================================================== loop

    def notify(self) -> None:
        """Called by the market stream after each applied message. Never blocks."""
        self._wake.set()

    async def run(self) -> None:
        """Strategy task: one evaluation per wake-up, always on the latest book."""
        wake = self._wake
        clock = self._clock
        while True:
            await wake.wait()
            wake.clear()
            self.on_market(clock())

    def on_market(self, now_ns: int) -> None:
        state = self.sm.state
        if state in EXPOSED_STATES:
            self._evaluate_exit(now_ns)  # exits always come before any entry logic
        elif state is State.FLAT:
            self._evaluate_entry(now_ns)

    # =================================================================== entries

    def _evaluate_entry(self, now_ns: int) -> None:
        if self.blocks:
            return
        exchange = self.exch_pos
        if exchange is None or exchange.signed_size or exchange.open_orders or exchange.pending_orders:
            return  # the exchange has not confirmed "flat, no resting orders"
        market = self.market
        bids = market.top_bids
        asks = market.top_asks
        if not bids or not asks or not self.book.valid:
            return
        if market.staleness_ms(now_ns) > self.cfg.market_data_stale_ms:
            return
        signal = self.signals.compute(now_ns)
        self.last_signal = signal
        if signal.side == 0:
            return
        if not self.limiter.can_enter():
            self.metrics.count_skip("RATE_LIMIT_RESERVE")
            return
        plan, reason = plan_entry(
            signal.side,
            bids,
            asks,
            limits=self.limits,
            meta=self.meta,
            available_balance_q=self.available_q,
            volatility_mbps=self.signals.range_mbps(now_ns),
            taker_fee_tick=self.fee_tick,
        )
        if plan is None:
            self.metrics.count_skip(reason)
            return
        self._begin_entry(signal, plan, now_ns)

    def _begin_entry(self, signal: Signal, plan: EntryPlan, now_ns: int) -> None:
        # FLAT -> ENTRY_PENDING happens before anything is sent; every further signal is
        # ignored until this order's lifecycle resolves (duplicate-signal protection).
        self.sm.transition(State.ENTRY_PENDING, "SIGNAL")
        order = ActiveOrder(
            client_order_index=self.ids.next(),
            kind=OrderKind.ENTRY,
            is_ask=plan.side == SHORT,
            size=plan.size,
            limit_price=plan.limit_price,
            reduce_only=False,
            market_order=False,  # LIMIT + IOC: never fills beyond the slippage cap
            reason="SIGNAL",
            created_ns=now_ns,
        )
        ctx = TradeContext(
            side=plan.side,
            signal_score=signal.score,
            signal_components=signal.components(),
            signal_ns=self.market.event_ns or now_ns,
            decision_ns=now_ns,
            entry_ref_price=plan.estimate.best_price,
        )
        self.ctx = ctx
        self.entry_order = order
        self._track(order)
        self.limiter.note_entry()
        self.metrics.entries_sent += 1
        self._spawn(self._submit_entry(order, ctx))
        scale = self.meta.price_scale
        log.info(
            "SIGNAL %s score=%.3f imb=%.2f flow=%.2f mom=%.2f bbo=%.2f micro=%.2f accel=%.2f depth=%.2f",
            "LONG" if plan.side == LONG else "SHORT",
            signal.score,
            signal.book_imbalance,
            signal.trade_flow,
            signal.micro_momentum,
            signal.bbo_momentum,
            signal.microprice,
            signal.volume_accel,
            signal.depth_change,
        )
        log.info(
            "ENTRY_SENT side=%s qty="
            + self._size_fmt
            + " limit="
            + self._price_fmt
            + " best="
            + self._price_fmt
            + " coi=%d",
            "SELL" if order.is_ask else "BUY",
            order.size / self.meta.size_scale,
            order.limit_price / scale,
            plan.estimate.best_price / scale,
            order.client_order_index,
        )

    async def _submit_entry(self, order: ActiveOrder, ctx: TradeContext) -> None:
        result = await self.executor.submit(order)
        ctx.entry_signed_ns = order.signed_ns
        ctx.entry_sent_ns = order.sent_ns
        ctx.entry_ack_ns = order.ack_ns
        self.metrics.add_ns("signal_to_send_ms", ctx.signal_ns, order.sent_ns)
        self.metrics.add_ns("send_to_ack_ms", order.sent_ns, order.ack_ns)
        if order.sent_ns and order.ack_ns:
            self.metrics.note_execution_latency((order.ack_ns - order.sent_ns) / NS_PER_MS)
        if order is not self.entry_order or order.terminal:
            return  # already resolved by the account stream, or recovery took over
        if result.accepted:
            self._reject_streak = 0
            self._arm_resolve(order)
            return
        self._apply_error_policy(result)
        if result.ambiguous:
            # Unknown whether the order exists. Never re-send: reconcile against the exchange.
            self._start_recovery("ENTRY_SEND_AMBIGUOUS")
            return
        # Definitive rejection: the order does not exist.
        order.mark_terminal("rejected", self._clock())
        self.entry_order = None
        self.ctx = None
        if self.sm.state is State.ENTRY_PENDING:
            self.sm.transition(State.FLAT, f"ENTRY_REJECTED:{result.error_class.value if result.error_class else ''}")

    # ===================================================================== exits

    def _evaluate_exit(self, now_ns: int) -> None:
        """Run on every market event while exposure exists. GREEN triggers an immediate exit."""
        pos = self.position
        if pos is None or pos.size <= 0:
            return
        levels = self.market.top_bids if pos.side == LONG else self.market.top_asks
        if not levels or not self.book.valid:
            return  # the health monitor escalates stale data to recovery
        limits = self.limits
        estimate = estimate_close(
            pos.side,
            pos.size,
            pos.cost_q,
            pos.fee_q,
            levels,
            taker_fee_tick=self.fee_tick,
            buffer_mbps=limits.buffer_mbps,
            max_slippage_mbps=limits.normal_slip_mbps,
            min_profit_q=limits.min_profit_for(pos.cost_q),
        )
        loss_view: CloseEstimate | None = estimate if estimate.full else None
        if estimate.full:
            pos.observe(estimate.net_pnl_q)
        elif limits.max_loss_q:
            # Thin book inside the normal band: value the position across the emergency band.
            wide = estimate_close(
                pos.side,
                pos.size,
                pos.cost_q,
                pos.fee_q,
                levels,
                taker_fee_tick=self.fee_tick,
                buffer_mbps=0,
                max_slippage_mbps=limits.emergency_slip_mbps,
                min_profit_q=0,
            )
            loss_view = wide if wide.full else None

        # 1. Hard adverse-move / maximum-loss protection.
        breach = hard_loss_reason(
            pos.side, pos.size, pos.cost_q, levels[0][0], loss_view.net_pnl_q if loss_view else None, limits
        )
        if breach is not None:
            self._begin_exit(now_ns, breach, True, estimate)
            return
        # 2. GREEN: the whole position can be closed for positive net realized P&L. Exit now.
        if estimate.profitable:
            if self.cfg.profit_exit_mode == "profit_only" and not self.limiter.can_send_optional():
                return
            ctx = self.ctx
            if ctx is not None and not ctx.profit_detected_ns:
                ctx.profit_detected_ns = now_ns
            self._begin_exit(now_ns, "PROFIT", False, estimate)
            return
        # 3. Maximum holding time.
        if hold_expired(now_ns, pos.opened_ns, limits):
            self._begin_exit(now_ns, "MAX_HOLD", False, estimate)
            return
        # 4. Optional: the entry signal has flipped against the position.
        if self.cfg.exit_on_signal_reversal:
            signal = self.signals.compute(now_ns)
            self.last_signal = signal
            if signal.side == -pos.side:
                self._begin_exit(now_ns, "SIGNAL_REVERSAL", False, estimate)

    def _begin_exit(self, now_ns: int, reason: str, emergency: bool, estimate: CloseEstimate) -> None:
        # The state leaves OPEN before the order is created, so two market events can
        # never both start an exit.
        self.sm.transition(State.EXIT_PENDING, reason)
        self._cancel("hold")
        self._exit_reason = reason
        self._exit_emergency = emergency
        self._exit_attempts = 0
        ctx = self.ctx
        if ctx is not None:
            ctx.exit_reason = reason
            ctx.exit_decision_ns = now_ns
            ctx.exit_ref_price = estimate.best_price
            ctx.expected_net_q = estimate.net_pnl_q
            ctx.estimated_slippage_q = estimate.slippage_q
        self._send_exit(now_ns)  # submit first; logging below must not delay the order
        if reason == "PROFIT":
            log.info(
                "GREEN expected_net_pnl=%.4f gross=%.4f fees=%.4f vwap=" + self._price_fmt,
                self.meta.q_to_usd(estimate.net_pnl_q),
                self.meta.q_to_usd(estimate.gross_pnl_q),
                self.meta.q_to_usd(estimate.fees_q),
                estimate.close_vwap / self.meta.price_scale,
            )
        else:
            log.warning("EXIT_TRIGGER reason=%s est_net_pnl=%.4f", reason, self.meta.q_to_usd(estimate.net_pnl_q))

    def _send_exit(self, now_ns: int) -> None:
        """Create and submit a reduce-only IOC for exactly the remaining position."""
        pos = self.position
        if pos is None or pos.size <= 0:
            return
        market = self.market
        levels = market.top_bids if pos.side == LONG else market.top_asks
        if not levels or not self.book.valid or market.staleness_ms(now_ns) > self.cfg.market_data_stale_ms:
            self._start_recovery("EXIT_NO_MARKET_DATA")
            return
        cfg = self.cfg
        limits = self.limits
        emergency = self._exit_emergency or self._exit_attempts >= cfg.exit_escalate_after
        slip = limits.emergency_slip_mbps if emergency else limits.normal_slip_mbps
        best = levels[0][0]
        limit = price_minus_mbps(best, slip) if pos.side == LONG else price_plus_mbps(best, slip)
        market_order = True
        if self._exit_reason == "PROFIT" and cfg.profit_exit_mode == "profit_only" and not emergency:
            # Never let a detected green fill at a loss: cap the order at the break-even price.
            floor = breakeven_exit_price(
                pos.side,
                pos.size,
                pos.cost_q,
                pos.fee_q,
                taker_fee_tick=self.fee_tick,
                min_profit_q=limits.min_profit_for(pos.cost_q),
            )
            if floor > 0:
                limit = max(limit, floor) if pos.side == LONG else min(limit, floor)
                market_order = False
        order = ActiveOrder(
            client_order_index=self.ids.next(),
            kind=OrderKind.EXIT,
            is_ask=pos.side == LONG,
            size=pos.size,  # the actual remaining size, never the original entry size
            limit_price=limit,
            reduce_only=True,
            market_order=market_order,
            reason=self._exit_reason,
            created_ns=now_ns,
            emergency=emergency,
        )
        self.exit_order = order
        self._track(order)
        self.sm.try_transition(State.EXIT_PENDING, "EXIT_RETRY")  # from PARTIAL_EXIT on re-sends
        self._spawn(self._submit_exit(order, self.ctx))
        log.info(
            "EXIT_SENT reduce_only=true side=%s qty="
            + self._size_fmt
            + " limit="
            + self._price_fmt
            + " reason=%s emergency=%s attempt=%d coi=%d",
            "SELL" if order.is_ask else "BUY",
            order.size / self.meta.size_scale,
            limit / self.meta.price_scale,
            self._exit_reason,
            emergency,
            self._exit_attempts + 1,
            order.client_order_index,
        )

    async def _submit_exit(self, order: ActiveOrder, ctx: TradeContext | None) -> None:
        result = await self.executor.submit(order)
        if ctx is not None and not ctx.exit_sent_ns:
            ctx.exit_signed_ns = order.signed_ns
            ctx.exit_sent_ns = order.sent_ns
            ctx.exit_ack_ns = order.ack_ns
            self.metrics.add_ns("profit_to_exit_send_ms", ctx.profit_detected_ns, order.sent_ns)
        self.metrics.add_ns("send_to_ack_ms", order.sent_ns, order.ack_ns)
        if order is not self.exit_order or order.terminal:
            return
        if result.accepted:
            self._arm_resolve(order)
            return
        self._apply_error_policy(result)
        if result.ambiguous:
            self._start_recovery("EXIT_SEND_AMBIGUOUS")
            return
        now_ns = self._clock()
        order.mark_terminal("rejected", now_ns)
        self.exit_order = None
        if is_reduce_only_rejection(result.code, result.message):
            # The exchange says there is nothing to reduce: our view of the position is wrong.
            self._start_recovery("EXIT_REDUCE_ONLY_REJECTED")
        elif result.error_class is ErrorClass.RATE_LIMIT_ERROR:
            self.sm.try_transition(State.PARTIAL_EXIT, "EXIT_RATE_LIMITED")
            self._exit_attempts += 1
            self._arm("exit_retry", RATE_LIMIT_RETRY_S, self._retry_exit)
        else:
            self._on_exit_incomplete(order, now_ns)

    def _retry_exit(self) -> None:
        self._timers.pop("exit_retry", None)
        if self.sm.state in (State.PARTIAL_EXIT, State.EXIT_PENDING) and self.exit_order is None:
            if self._exit_attempts >= self.cfg.max_exit_attempts:
                self._start_recovery("EXIT_ATTEMPTS_EXHAUSTED")
            else:
                self._send_exit(self._clock())

    def _on_exit_incomplete(self, order: ActiveOrder, now_ns: int) -> None:
        """An exit order finished but exposure remains (partial fill, miss or rejection)."""
        pos = self.position
        if pos is None or self.sm.state in (State.RECOVERY, State.HALTED):
            return  # recovery owns the position while it runs
        self._exit_attempts += 1
        if order.filled > 0:
            self.metrics.partial_fills += 1
            self.metrics.count_error(ErrorClass.PARTIAL_FILL)
        log.warning(
            "EXIT_INCOMPLETE status=%s filled=" + self._size_fmt + " remaining=" + self._size_fmt + " attempts=%d",
            order.status,
            order.filled / self.meta.size_scale,
            pos.size / self.meta.size_scale,
            self._exit_attempts,
        )
        if (
            self._exit_reason == "PROFIT"
            and self.cfg.profit_exit_mode == "profit_only"
            and not self._exit_emergency
            and not self.shutting_down
        ):
            # The green opportunity was missed; keep the position under full protection
            # and wait for the next executable green.
            self.sm.transition(State.OPEN_LONG if pos.side == LONG else State.OPEN_SHORT, "EXIT_MISSED")
            self._arm_hold_timer(now_ns)
            self._evaluate_exit(now_ns)
            return
        self.sm.try_transition(State.PARTIAL_EXIT, "EXIT_INCOMPLETE")
        if self._exit_attempts >= self.cfg.max_exit_attempts:
            self._start_recovery("EXIT_ATTEMPTS_EXHAUSTED")
            return
        self._send_exit(now_ns)

    # ============================================================ account events

    def on_order_update(self, update: OrderUpdate) -> None:
        order = self._orders.get(update.client_order_index)
        if order is None:
            self._on_unknown_order(update)
            return
        now_ns = self._clock()
        order.apply_order_update(update, now_ns)
        self._apply_fill(order, now_ns)
        if order.terminal:
            self._on_order_terminal(order, now_ns)

    def on_trade_fill(self, fill: TradeFill) -> None:
        if fill.is_taker and fill.fee_tick > self.fee_tick:
            log.warning("FEE_RATE_HIGHER_THAN_EXPECTED observed_tick=%d assumed_tick=%d", fill.fee_tick, self.fee_tick)
            self.fee_tick = fill.fee_tick
        order = self._orders.get(fill.client_order_index)
        if order is None:
            return
        now_ns = self._clock()
        order.apply_trade(fill, now_ns)
        self._apply_fill(order, now_ns)
        if not order.terminal and order.filled >= order.size:
            order.mark_terminal("filled", now_ns)  # an IOC that is completely filled is finished
            self._on_order_terminal(order, now_ns)

    def on_position(self, position: ExchangePosition) -> None:
        self.exch_pos = position
        local = self.position.signed_size if self.position is not None else 0
        if position.signed_size == local:
            self._cancel("mismatch")
        elif self.sm.state in (State.FLAT, State.OPEN_LONG, State.OPEN_SHORT) and "mismatch" not in self._timers:
            # Order and position messages are not ordered against each other: allow a short grace.
            self._arm("mismatch", MISMATCH_GRACE_S, self._on_mismatch_timer)

    def on_balance(self, available_q: int) -> None:
        self.available_q = available_q

    def on_account_stream_up(self) -> None:
        self.blocks.pop("ACCOUNT_STREAM_DOWN", None)
        self._cancel("account_grace")
        # After any reconnect the exchange is asked for the truth before trading resumes.
        if self.sm.state is not State.STARTING:
            self._request_resync("ACCOUNT_STREAM_UP")

    def on_account_stream_down(self) -> None:
        self.blocks["ACCOUNT_STREAM_DOWN"] = "authenticated stream disconnected"
        self.exch_pos = None
        if self.sm.state not in (State.FLAT, State.STARTING, State.SYNCING, State.HALTED):
            self._arm("account_grace", self.cfg.account_stream_grace_ms / 1000.0, self._on_account_grace)

    def _on_unknown_order(self, update: OrderUpdate) -> None:
        if is_terminal_status(update.status):
            return
        if is_bot_order(update.client_order_index):
            self._request_resync("STALE_BOT_ORDER")  # left over from a previous run: cancel it
        elif "FOREIGN_ORDERS" not in self.blocks:
            self.blocks["FOREIGN_ORDERS"] = "a resting BTC order not created by this bot exists"
            log.warning("FOREIGN_ORDER_DETECTED order_index=%d status=%s", update.order_index, update.status)

    def _apply_fill(self, order: ActiveOrder, now_ns: int) -> None:
        """Book the not-yet-applied part of an order's confirmed fill into the position.

        Works from ``order.filled - order.applied`` rather than from message deltas, so it is
        idempotent and independent of the order in which fills become known.
        """
        d_base = order.filled - order.applied
        d_quote = order.filled_quote_q - order.applied_quote_q
        if d_base == 0 and d_quote == 0:
            return
        live = order is self.entry_order or order is self.exit_order
        if not live and order.client_order_index not in self.recovery_orders:
            if d_base > 0:
                # A fill for an order we already gave up on: let reconciliation establish the truth.
                log.error("ORPHAN_FILL coi=%d qty=%d", order.client_order_index, d_base)
                self.metrics.count_error(ErrorClass.STATE_MISMATCH)
                self._request_resync("ORPHAN_FILL")
            return
        fee = fee_q(d_quote, self.fee_tick) if d_quote > 0 else 0
        if order.kind is OrderKind.ENTRY:
            if self.position is None:
                self.position = Position(side=SHORT if order.is_ask else LONG, opened_ns=now_ns)
                if self.ctx is not None:
                    self.ctx.first_fill_ns = now_ns
            self.position.add_entry(d_base, d_quote, fee)
            order.applied = order.filled
            order.applied_quote_q = order.filled_quote_q
            if not order.terminal:
                self.sm.try_transition(State.PARTIALLY_FILLED, "ENTRY_PARTIAL")
        else:
            pos = self.position
            if pos is None:
                return  # stays unapplied until the entry side is known (see apply_recovered_order)
            pos.add_exit(d_base, d_quote, fee)
            order.applied = order.filled
            order.applied_quote_q = order.filled_quote_q
            if pos.size > 0 and not order.terminal and order is self.exit_order:
                self.sm.try_transition(State.PARTIAL_EXIT, "EXIT_PARTIAL")

    def _on_order_terminal(self, order: ActiveOrder, now_ns: int) -> None:
        if order is self.entry_order:
            self._cancel("resolve_entry")
            self.entry_order = None
            self._on_entry_done(order, now_ns)
        elif order is self.exit_order:
            self._cancel("resolve_exit")
            self.exit_order = None
            pos = self.position
            if pos is None:
                return
            if pos.size == 0:
                self.finalize_trade(now_ns)
            else:
                self._on_exit_incomplete(order, now_ns)

    def _on_entry_done(self, order: ActiveOrder, now_ns: int) -> None:
        pos = self.position
        ctx = self.ctx
        meta = self.meta
        if pos is None or pos.size <= 0:
            self.ctx = None
            self.metrics.entries_missed += 1
            log.info("ENTRY_UNFILLED status=%s coi=%d", order.status, order.client_order_index)
            if self.sm.state is State.ENTRY_PENDING:
                self.sm.transition(State.FLAT, f"ENTRY_UNFILLED:{order.status}")
            return
        partial = order.filled < order.size
        if partial:
            self.metrics.partial_fills += 1
            self.metrics.count_error(ErrorClass.PARTIAL_FILL)
        if ctx is not None:
            ctx.full_fill_ns = now_ns
            self.metrics.add_ns("send_to_fill_ms", ctx.entry_sent_ns or ctx.decision_ns, now_ns)
            if ctx.entry_ref_price:
                slip = (order.avg_price - ctx.entry_ref_price) / ctx.entry_ref_price * 10_000.0 * pos.side
                self.metrics.add("entry_slippage_bps", slip)
        log.info(
            "ENTRY_FILL avg="
            + self._price_fmt
            + " qty="
            + self._size_fmt
            + " requested="
            + self._size_fmt
            + " partial=%s status=%s",
            order.avg_price / meta.price_scale,
            order.filled / meta.size_scale,
            order.size / meta.size_scale,
            partial,
            order.status,
        )
        if self.sm.state in (State.ENTRY_PENDING, State.PARTIALLY_FILLED):
            self.sm.transition(
                State.OPEN_LONG if pos.side == LONG else State.OPEN_SHORT,
                "ENTRY_PARTIAL_FILL" if partial else "ENTRY_FILLED",
            )
            self._arm_hold_timer(now_ns)
            self._evaluate_exit(now_ns)  # the exit engine is live from the first confirmed fill

    # ========================================================== trade completion

    def adopt_position(self, exchange: ExchangePosition) -> None:
        """Take over a position found on the exchange and manage it with the exit engine."""
        now_ns = self._clock()
        side = LONG if exchange.signed_size > 0 else SHORT
        size = abs(exchange.signed_size)
        cost_q = exchange.avg_entry_price * size
        pos = Position(side=side, opened_ns=now_ns, adopted=True)
        pos.add_entry(size, cost_q, fee_q(cost_q, self.fee_tick))
        self.position = pos
        self.ctx = TradeContext(side=side, decision_ns=now_ns, first_fill_ns=now_ns, full_fill_ns=now_ns)
        self.sm.transition(State.OPEN_LONG if side == LONG else State.OPEN_SHORT, "ADOPTED")
        log.warning(
            "POSITION_ADOPTED side=%s qty=%s avg_entry=%s",
            "LONG" if side == LONG else "SHORT",
            self.meta.fmt_size(size),
            self.meta.fmt_price(exchange.avg_entry_price),
        )
        self._arm_hold_timer(now_ns)
        self._evaluate_exit(now_ns)

    def finalize_trade(self, now_ns: int, exit_reason: str | None = None) -> None:
        """Book a completed trade from confirmed fills only, then return to FLAT."""
        pos = self.position
        if pos is None:
            return
        ctx = self.ctx or TradeContext(side=pos.side)
        ctx.flat_ns = now_ns
        if exit_reason:
            ctx.exit_reason = f"{ctx.exit_reason}>{exit_reason}" if ctx.exit_reason else exit_reason
        closed = close_trade(pos, ctx, self.meta, self.cfg.leverage, now_ns, self._exit_attempts + 1)
        metrics = self.metrics
        metrics.on_trade_closed(closed.gross_usd, closed.fees_usd, closed.net_usd, closed.hold_ms)
        metrics.add_ns("fill_to_profit_ms", ctx.full_fill_ns, ctx.profit_detected_ns)
        metrics.add_ns("exit_send_to_flat_ms", ctx.exit_sent_ns, now_ns)
        if closed.exit_slippage_bps is not None:
            metrics.add("exit_slippage_bps", closed.exit_slippage_bps)
        self.journal.record_trade(closed.record)
        log.info(
            "FLAT realized_pnl=%.4f gross=%.4f fees=%.4f hold_ms=%.0f exit_reason=%s avg_entry="
            + self._price_fmt
            + " avg_exit="
            + self._price_fmt,
            closed.net_usd,
            closed.gross_usd,
            closed.fees_usd,
            closed.hold_ms,
            ctx.exit_reason or "UNKNOWN",
            closed.avg_entry,
            closed.avg_exit,
        )
        self.clear_trade()
        if self.sm.state is not State.HALTED:
            self.sm.transition(State.FLAT, "EXIT_FILLED")
        # No cooldown: the very next market event may open the next trade.

    def clear_trade(self) -> None:
        """Forget the current trade's in-memory state (position, orders, timers)."""
        self.position = None
        self.ctx = None
        self.entry_order = None
        self.exit_order = None
        self._exit_reason = ""
        self._exit_emergency = False
        self._exit_attempts = 0
        for name in ("hold", "resolve_entry", "resolve_exit", "exit_retry", "account_grace"):
            self._cancel(name)

    # ================================================================== policies

    def _apply_error_policy(self, result: SendResult) -> None:
        """Explicit handling per error class for a failed order submission."""
        error_class = result.error_class
        if error_class is ErrorClass.AUTH_ERROR:
            self.halt(f"exchange rejected credentials: {result.message}", exit_process=True)
        elif error_class is ErrorClass.RATE_LIMIT_ERROR:
            pass  # the limiter is already penalised by the client; entries pause automatically
        elif error_class is ErrorClass.ORDER_REJECTED:
            self._reject_streak += 1
            if self._reject_streak >= REJECT_STREAK_LIMIT:
                log.error("ENTRY_PAUSED reason=reject_streak count=%d", self._reject_streak)
                self.limiter.penalize(REJECT_STREAK_PAUSE_S)
                self._reject_streak = 0
        # NONCE_ERROR: the executor re-reads the nonce before the next signature.
        # NETWORK_ERROR / EXCHANGE_ERROR: ambiguous, handled by the caller through recovery.

    def halt(self, reason: str, *, exit_process: bool = False) -> None:
        """Stop trading for good. ``exit_process`` also ends the service (fatal credentials/config)."""
        self.blocks["HALTED"] = reason
        if exit_process:
            self.fatal_reason = reason
        log.critical("HALTED reason=%s exit_process=%s", reason, exit_process)
        self.journal.record_event("HALTED", {"reason": reason, "state": self.sm.state.value})
        self.sm.try_transition(State.HALTED, reason)

    # ==================================================================== timers

    def _arm(self, name: str, delay_s: float, callback: Callable[[], None]) -> None:
        self._cancel(name)
        self._timers[name] = asyncio.get_running_loop().call_later(max(delay_s, 0.0), callback)

    def _cancel(self, name: str) -> None:
        handle = self._timers.pop(name, None)
        if handle is not None:
            handle.cancel()

    def _arm_hold_timer(self, now_ns: int) -> None:
        pos = self.position
        if pos is None:
            return
        remaining_ns = pos.opened_ns + self.limits.max_hold_ns - now_ns
        self._arm("hold", remaining_ns / 1e9, self._on_hold_timer)

    def _on_hold_timer(self) -> None:
        """Fires even when no market event arrives, so a stuck scalp can never be held silently."""
        self._timers.pop("hold", None)
        if self.sm.state not in EXPOSED_STATES:
            return
        now_ns = self._clock()
        if not self.book.valid or self.market.staleness_ms(now_ns) > self.cfg.market_data_stale_ms:
            self._start_recovery("MAX_HOLD_NO_MARKET_DATA")
        else:
            self._evaluate_exit(now_ns)

    def _arm_resolve(self, order: ActiveOrder) -> None:
        name = "resolve_entry" if order.kind is OrderKind.ENTRY else "resolve_exit"
        self._arm(name, self.cfg.order_resolve_timeout_ms / 1000.0, partial(self._on_resolve_timeout, name, order))

    def _on_resolve_timeout(self, name: str, order: ActiveOrder) -> None:
        """An accepted order produced no terminal update in time: stop guessing."""
        self._timers.pop(name, None)
        if (order is self.entry_order or order is self.exit_order) and not order.terminal:
            log.error("ORDER_UNRESOLVED kind=%s coi=%d", order.kind.value, order.client_order_index)
            self._start_recovery("ORDER_UNRESOLVED")

    def _on_mismatch_timer(self) -> None:
        self._timers.pop("mismatch", None)
        exchange = self.exch_pos
        if exchange is None or self.sm.state not in (State.FLAT, State.OPEN_LONG, State.OPEN_SHORT):
            return
        local = self.position.signed_size if self.position is not None else 0
        if exchange.signed_size != local:
            self.metrics.count_error(ErrorClass.STATE_MISMATCH)
            log.error("STATE_MISMATCH local=%d exchange=%d", local, exchange.signed_size)
            self._request_resync("STATE_MISMATCH")

    def _on_account_grace(self) -> None:
        self._timers.pop("account_grace", None)
        if "ACCOUNT_STREAM_DOWN" in self.blocks and self.sm.state not in (
            State.FLAT,
            State.STARTING,
            State.SYNCING,
            State.HALTED,
        ):
            self._start_recovery("ACCOUNT_STREAM_DOWN")

    # ================================================================== plumbing

    def _start_recovery(self, reason: str) -> None:
        if self.recovery is not None:
            self.recovery.start_recovery(reason)

    def _request_resync(self, reason: str) -> None:
        if self.recovery is not None:
            self.recovery.request_resync(reason)

    def on_market_stale(self) -> None:
        """Health monitor: market data went stale. With exposure this is an emergency."""
        if self.sm.state in EXPOSED_STATES or self.sm.state in (State.EXIT_PENDING, State.PARTIAL_EXIT):
            self._start_recovery("MARKET_DATA_STALE")

    def _track(self, order: ActiveOrder) -> None:
        orders = self._orders
        orders[order.client_order_index] = order
        if len(orders) > MAX_TRACKED_ORDERS:
            for key in list(orders)[: len(orders) - MAX_TRACKED_ORDERS]:
                if orders[key].terminal:
                    del orders[key]

    def apply_recovered_order(self, order: ActiveOrder, update: OrderUpdate | None) -> None:
        """Recovery: book an order's fills, optionally merging its final state read over REST.

        Call for the entry order first, then for the closing orders.
        """
        now_ns = self._clock()
        self.recovery_orders.add(order.client_order_index)
        if update is not None:
            order.apply_order_update(update, now_ns)
        self._apply_fill(order, now_ns)

    def track_recovery_order(self, order: ActiveOrder) -> None:
        """Register a flatten order so its fills are applied to the position."""
        self.recovery_orders.add(order.client_order_index)
        self._track(order)

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        # Eager start: the coroutine runs right now up to its first real suspension, so the
        # order is signed and written to the socket before control returns to the event loop.
        task = asyncio.Task(coro, loop=asyncio.get_running_loop(), eager_start=True)
        self._tasks.add(task)
        task.add_done_callback(self._on_task_done)
        return task

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            # A submission coroutine failed in an unforeseen way: assume nothing, reconcile.
            log.critical("INTERNAL_ERROR task=%s error=%r", task.get_name(), error, exc_info=error)
            self._start_recovery("INTERNAL_ERROR")

    def _on_transition(self, old: State, new: State, reason: str) -> None:
        log.info("STATE %s -> %s reason=%s", old.value, new.value, reason)
        self.journal.save_state(self.snapshot_state(reason))

    def snapshot_state(self, last_event: str = "") -> dict[str, Any]:
        """Small recovery snapshot persisted on every transition and heartbeat."""
        pos = self.position
        return {
            "state": self.sm.state.value,
            "heartbeat": utc_iso(),
            "last_event": last_event,
            "last_client_order_index": self.ids.last,
            "trade_id": self.ctx.trade_id if self.ctx is not None else None,
            "entry_order": self.entry_order.client_order_index if self.entry_order else None,
            "exit_order": self.exit_order.client_order_index if self.exit_order else None,
            "position": None
            if pos is None
            else {
                "side": pos.side,
                "size": pos.size,
                "cost_q": pos.cost_q,
                "entry_size": pos.entry_size,
                "exit_size": pos.exit_size,
                "adopted": pos.adopted,
            },
        }
