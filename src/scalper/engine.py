"""Service bootstrap, task supervision and graceful shutdown.

Startup order (nothing trades until every step has succeeded):

    config -> signer/API key check -> BTC market metadata -> account tier/fees
    -> market stream + account stream -> startup reconciliation (position, orders,
    leverage) -> FLAT (or managing an adopted position) -> LIVE

Tasks: market_data, account_stream, strategy, health. If any of them ends
unexpectedly the service flattens (per configuration) and exits so systemd can
restart it into a fresh reconciliation.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from . import __version__
from .account_stream import AccountStream
from .config import Config
from .control import Command, ControlInbox, is_paused, purge_commands
from .errors import EXIT_FATAL, EXIT_OK, ConfigError, ErrorClass, FatalError, ScalperError
from .execution import Executor
from .health import HealthMonitor, sd_notify
from .lighter_client import AccountTier, LighterClient
from .market_data import MarketDataStream
from .metrics import Metrics, utc_date
from .orderbook import OrderBook
from .persistence import Journal, utc_iso
from .position import ClientOrderIds
from .precision import MarketMeta
from .rate_limits import RateLimiter, limits_for_tier
from .reconciliation import Reconciler
from .risk import Limits
from .signals import SignalEngine
from .state_machine import State
from .strategy import Trader

log = logging.getLogger("scalper.engine")

STREAM_START_TIMEOUT_S = 30.0
SYNC_START_TIMEOUT_S = 60.0
SHUTDOWN_FLATTEN_TIMEOUT_S = 90.0
TICKS_PER_BPS = 100  # 1 bps = 1e-4 = 100 fee ticks of 1e-6


class StartupError(ScalperError):
    """Startup could not complete for a transient reason (network, exchange). Safe to retry."""


@dataclass(slots=True)
class Session:
    """A verified connection to Lighter shared by ``run``, ``flatten``, ``status`` and ``check``."""

    cfg: Config
    client: LighterClient
    meta: MarketMeta
    tier: AccountTier
    limiter: RateLimiter
    metrics: Metrics

    @property
    def taker_fee_tick(self) -> int:
        """Fee tick used for GREEN: the exchange-reported tier fee, never below a configured override."""
        tick = self.tier.taker_fee_tick
        override = self.cfg.taker_fee_bps_override
        if override is not None:
            tick = max(tick, int(override * TICKS_PER_BPS))
        return tick


async def open_session(cfg: Config, metrics: Metrics | None = None) -> Session:
    """Connect, verify the API key, locate the BTC market and read the account tier."""
    metrics = metrics or Metrics()
    limiter = RateLimiter(limits_for_tier("standard", cfg.rate_limit_per_minute_override), cfg.max_entries_per_minute)
    client = LighterClient(cfg, limiter, metrics)
    try:
        await client.connect()
        await client.verify_api_key()
        meta = await client.fetch_market_meta(cfg.market_symbol)
        tier = await client.fetch_account_tier()
    except BaseException:
        await client.close()
        raise
    limiter.set_tier(limits_for_tier(tier.name, cfg.rate_limit_per_minute_override))
    return Session(cfg, client, meta, tier, limiter, metrics)


def describe(cfg: Config, session: Session, limits: Limits) -> list[str]:
    """Static part of the startup banner."""
    meta = session.meta
    return [
        f"LIGHTER BTC SCALPER v{__version__}",
        "MODE: LIVE MAINNET",
        f"ACCOUNT: index {cfg.account_index} (api key index {cfg.api_key_index}, tier {session.tier.name})",
        f"MARKET: {meta.symbol} perpetual (market id {meta.market_id})",
        f"LEVERAGE: {cfg.leverage}x {'isolated' if cfg.margin_mode else 'cross'} "
        f"(market maximum {meta.max_leverage.normalize()}x)",
        f"POSITION SIZE: {cfg.notional_usd:.2f} USD notional ({cfg.position_mode})",
        f"MINIMUM PROFIT: {cfg.min_profit_usd} USD / {Decimal(cfg.min_profit_mbps) / 1000} bps",
        f"MAX LOSS: {cfg.max_loss_usd} USD / {Decimal(cfg.max_adverse_move_mbps) / 1000} bps adverse move",
        f"MAX HOLD: {cfg.max_hold_ms} ms",
        f"TAKER FEE: {Decimal(session.taker_fee_tick) / TICKS_PER_BPS} bps",
        f"BTC MARKET: verified (price decimals {meta.price_decimals}, size decimals {meta.size_decimals}, "
        f"min size {meta.fmt_size(meta.min_base)})",
    ]


class Engine:
    """Owns the long-running service."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self._stop = asyncio.Event()
        self._exit_code = EXIT_OK
        self._tasks: dict[str, asyncio.Task[None]] = {}

    def request_stop(self, code: int = EXIT_OK) -> None:
        self._exit_code = max(self._exit_code, code)
        self._stop.set()

    async def run(self) -> int:
        cfg = self.cfg
        journal = Journal(cfg.data_dir, write_csv=cfg.trade_csv)
        journal.start()
        metrics = Metrics()
        metrics.restore_day(journal.load_day_totals(utc_date()))
        session: Session | None = None
        trader: Trader | None = None
        try:
            session = await open_session(cfg, metrics)
            trader = await self._serve(session, journal)
        except (ConfigError, FatalError) as exc:
            log.critical("FATAL class=%s error=%s", exc.error_class.value, exc)
            journal.record_event("FATAL", {"class": exc.error_class.value, "error": str(exc)})
            self._exit_code = EXIT_FATAL
        except ScalperError as exc:
            log.error("STARTUP_FAILED class=%s error=%s", exc.error_class.value, exc)
            fatal = exc.error_class in (ErrorClass.AUTH_ERROR, ErrorClass.CONFIG_ERROR)
            self._exit_code = EXIT_FATAL if fatal else 1
        finally:
            sd_notify("STOPPING=1")
            for task in self._tasks.values():
                task.cancel()
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
            if session is not None:
                await session.client.close()
            journal.write_daily_summary(metrics.daily.date, metrics.daily.to_summary())
            if trader is not None:
                journal.save_state(trader.snapshot_state("SHUTDOWN"))
            journal.stop()
        log.info("STOPPED exit_code=%d", self._exit_code)
        return self._exit_code

    # ------------------------------------------------------------------ serve

    async def _serve(self, session: Session, journal: Journal) -> Trader:
        cfg = self.cfg
        meta = session.meta
        client = session.client
        metrics = session.metrics
        if meta.status != "active":
            raise FatalError(f"{meta.symbol} market is not tradeable (status={meta.status})", ErrorClass.CONFIG_ERROR)
        limits = Limits.from_config(cfg, meta)  # validates the leverage against the market
        _, ask = await client.fetch_rest_bbo(meta)
        if meta.size_for_notional(limits.notional_q, ask) < meta.min_size_at(ask):
            raise ConfigError("position size is below the exchange minimum order size for BTC")
        if cfg.api_key_index < 4:
            log.warning("API_KEY_INDEX_RESERVED index=%d (0-3 are used by Lighter's own apps)", cfg.api_key_index)

        persisted = journal.load_state()
        ids = ClientOrderIds(last=int(persisted.get("last_client_order_index") or 0))
        book = OrderBook()
        signals = SignalEngine(
            cfg.weights, cfg.entry_score_threshold, cfg.book_depth_levels, cfg.momentum_scale_bps, cfg.signal_warmup_ms
        )
        executor = Executor(cfg, meta, client, metrics, ids)
        holder: list[Trader] = []
        market = MarketDataStream(cfg, meta, book, signals, metrics, wake=lambda: holder[0].notify())
        trader = Trader(
            cfg=cfg,
            meta=meta,
            limits=limits,
            book=book,
            market=market,
            signals=signals,
            executor=executor,
            limiter=session.limiter,
            metrics=metrics,
            journal=journal,
            ids=ids,
            fee_tick=session.taker_fee_tick,
            maker_fee_tick=session.tier.maker_fee_tick,
        )
        holder.append(trader)
        reconciler = Reconciler(trader, client, executor, cfg)
        trader.recovery = reconciler
        # Operator control (control panel / CLI). Stale commands from before this start are
        # discarded; a pause that was in force stays in force.
        purge_commands(cfg.data_dir)
        if is_paused(cfg.data_dir):
            trader.blocks["PAUSED"] = "entries paused by the operator"
            log.warning("ENTRIES PAUSED: the pause set before this start is still in force")
        account = AccountStream(cfg, meta, lambda: client.auth_token(force=True), metrics, trader)
        health = HealthMonitor(
            cfg=cfg,
            meta=meta,
            trader=trader,
            reconciler=reconciler,
            market=market,
            account=account,
            client=client,
            limiter=session.limiter,
            metrics=metrics,
            journal=journal,
            request_stop=self.request_stop,
        )
        for line in describe(cfg, session, limits):
            log.info(line)
        log.info("API: connected")
        if persisted:
            log.info(
                "PREVIOUS_RUN state=%s heartbeat=%s position=%s",
                persisted.get("state"),
                persisted.get("heartbeat"),
                persisted.get("position"),
            )
        self._install_signal_handlers()

        loop = asyncio.get_running_loop()
        self._tasks = {
            "market_data": loop.create_task(market.run(), name="market_data"),
            "account_stream": loop.create_task(account.run(), name="account_stream"),
            "strategy": loop.create_task(trader.run(), name="strategy"),
            "health": loop.create_task(health.run(), name="health"),
        }
        await executor.prime()
        await self._wait_for(lambda: book.valid, STREAM_START_TIMEOUT_S, "the market data stream")
        log.info("MARKET STREAM: connected")
        await self._wait_for(lambda: account.synced, STREAM_START_TIMEOUT_S, "the account stream")
        log.info("ACCOUNT STREAM: connected")

        # Every start assumes a position may already exist: reconcile before anything else.
        await reconciler.startup()
        await self._wait_for(
            lambda: trader.sm.state not in (State.STARTING, State.SYNCING) or trader.fatal_reason is not None,
            SYNC_START_TIMEOUT_S,
            "startup reconciliation",
        )
        if trader.fatal_reason is not None:
            raise FatalError(trader.fatal_reason, ErrorClass.AUTH_ERROR)
        position = trader.position
        log.info(
            "POSITION: %s",
            "flat"
            if position is None
            else f"open {'LONG' if position.side > 0 else 'SHORT'} {meta.fmt_size(position.size)} (adopted)",
        )
        if trader.blocks:
            log.warning("ENTRIES BLOCKED: %s", "; ".join(f"{k} ({v})" for k, v in sorted(trader.blocks.items())))
        log.info("LIVE EXECUTION: %s", "HALTED" if trader.sm.state is State.HALTED else "ENABLED")
        sd_notify("READY=1")

        # The pause flag is the source of truth. It may have been changed from the panel while this
        # process was starting, so apply whatever it says now, before commands are accepted.
        if is_paused(cfg.data_dir):
            trader.blocks.setdefault("PAUSED", "entries paused by the operator")
        else:
            trader.blocks.pop("PAUSED", None)
        inbox = ControlInbox(cfg.data_dir, loop, lambda command: self._on_command(command, trader, reconciler))
        inbox.start()
        try:
            await self._supervise()
        finally:
            inbox.stop()
        await self._shutdown(trader, reconciler, market, account)
        return trader

    def _on_command(self, command: Command, trader: Trader, reconciler: Reconciler) -> None:
        """Apply one operator command. Runs on the event loop; never blocks."""
        name = command.name
        result = "ok"
        if name == "pause":
            trader.blocks["PAUSED"] = "entries paused by the operator"
        elif name == "resume":
            trader.blocks.pop("PAUSED", None)
        elif name == "flatten":
            # Flat means flat: stop opening positions first, then close what is there.
            trader.blocks["PAUSED"] = "entries paused by the operator (flatten)"
            reconciler.manual_flatten()
        elif name == "stop":
            if os.environ.get("INVOCATION_ID"):
                # Under systemd an exit would simply be restarted: stopping is systemctl's job.
                result = "ignored: managed by systemd, use systemctl stop"
            else:
                self.request_stop(EXIT_OK)
        trader.last_command = {"id": command.id, "command": name, "at": utc_iso(), "result": result}
        log.warning("CONTROL command=%s id=%s result=%s state=%s", name, command.id, result, trader.sm.state.value)
        trader.journal.record_event("CONTROL", {"command": name, "id": command.id, "result": result})

    async def _wait_for(self, ready: Callable[[], bool], limit_s: float, what: str) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + limit_s
        while not ready():
            if self._stop.is_set():
                raise StartupError("stop requested during startup", ErrorClass.NETWORK_ERROR)
            for name, task in self._tasks.items():
                if task.done():
                    raise StartupError(f"task {name} ended during startup", ErrorClass.EXCHANGE_ERROR)
            if loop.time() > deadline:
                raise StartupError(f"timed out waiting for {what}", ErrorClass.NETWORK_ERROR)
            await asyncio.sleep(0.05)

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, self.request_stop, EXIT_OK)
            except NotImplementedError:  # platforms without loop signal support
                signal.signal(signum, lambda *_: loop.call_soon_threadsafe(self.request_stop, EXIT_OK))

    async def _supervise(self) -> None:
        """Run until a stop is requested or a core task dies."""
        stop_wait = asyncio.get_running_loop().create_task(self._stop.wait())
        try:
            done, _ = await asyncio.wait({stop_wait, *self._tasks.values()}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            stop_wait.cancel()
        for name, task in self._tasks.items():
            if task in done:
                error = None if task.cancelled() else task.exception()
                log.critical("TASK_ENDED name=%s error=%r", name, error, exc_info=error)
                self._exit_code = max(self._exit_code, 1)

    async def _shutdown(
        self, trader: Trader, reconciler: Reconciler, market: MarketDataStream, account: AccountStream
    ) -> None:
        """SIGTERM/SIGINT: stop entering, resolve what is in flight, flatten if configured."""
        cfg = self.cfg
        loop = asyncio.get_running_loop()
        trader.shutting_down = True
        trader.blocks["SHUTDOWN"] = "service stopping"
        log.info("SHUTDOWN_START state=%s", trader.sm.state.value)
        # An entry in flight is reconciled first, never abandoned.
        deadline = loop.time() + cfg.tx_timeout_s + cfg.order_resolve_timeout_ms / 1000.0 + 1.0
        while trader.sm.state is State.ENTRY_PENDING and loop.time() < deadline:
            await asyncio.sleep(0.05)
        idle_states = (State.FLAT, State.HALTED, State.STARTING, State.SYNCING)
        if trader.fatal_reason is None:
            if trader.sm.state not in idle_states:
                if cfg.shutdown_position_action == "flatten":
                    reconciler.start_recovery("SHUTDOWN")
                else:
                    log.warning(
                        "SHUTDOWN_POSITION_KEPT state=%s (SHUTDOWN_POSITION_ACTION=keep)", trader.sm.state.value
                    )
            if reconciler.busy:  # a flatten or reconciliation in progress is always allowed to finish
                try:
                    await asyncio.wait_for(reconciler.wait_idle(), SHUTDOWN_FLATTEN_TIMEOUT_S)
                except TimeoutError:
                    log.critical("SHUTDOWN_FLATTEN_TIMEOUT state=%s", trader.sm.state.value)
        position = trader.position
        if position is not None and position.size > 0:
            log.critical(
                "SHUTDOWN_WITH_OPEN_POSITION side=%s size=%s",
                "LONG" if position.side > 0 else "SHORT",
                trader.meta.fmt_size(position.size),
            )
        reconciler.stop()
        market.stop()
        account.stop()
        log.info("SHUTDOWN_DONE state=%s", trader.sm.state.value)
