"""Watchdog and housekeeping. Nothing here is on the trading path.

A 250 ms tick that:

* measures event-loop lag and blocks entries after a stall,
* detects stale market data (blocks entries; with exposure it triggers recovery),
* publishes the status snapshot, the recovery heartbeat and the systemd watchdog ping,
* keeps the HTTPS connection warm and measures API / WebSocket latency,
* schedules the periodic authoritative reconciliation and auth-token renewal,
* rolls the daily summary at the UTC date change.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
from collections.abc import Callable
from typing import Any

from . import __version__
from .account_stream import AccountStream
from .config import Config
from .errors import ApiError, FatalError
from .lighter_client import LighterClient
from .market_data import MarketDataStream
from .metrics import Metrics
from .persistence import Journal, utc_iso
from .precision import MarketMeta
from .rate_limits import RateLimiter
from .reconciliation import Reconciler
from .state_machine import State
from .status import trader_view
from .strategy import Trader

log = logging.getLogger("scalper.health")

TICK_S = 0.25
LOOP_STALL_MS = 500.0
LOOP_STALL_HOLD_S = 2.0
KEEP_WARM_S = 20.0
STATUS_LOG_S = 60.0
META_REFRESH_S = 600.0
TOKEN_RENEW_MARGIN_S = 1800.0
FORCED_RECONNECT_GAP_S = 5.0


def sd_notify(message: str) -> None:
    """Send a notification to systemd (READY=1, WATCHDOG=1, STOPPING=1). No-op elsewhere."""
    address = os.environ.get("NOTIFY_SOCKET")
    if not address or not hasattr(socket, "AF_UNIX"):
        return
    if address.startswith("@"):
        address = "\0" + address[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.connect(address)
            sock.sendall(message.encode())
    except OSError:
        pass


class HealthMonitor:
    """Periodic supervision of streams, event loop and housekeeping tasks."""

    def __init__(
        self,
        *,
        cfg: Config,
        meta: MarketMeta,
        trader: Trader,
        reconciler: Reconciler,
        market: MarketDataStream,
        account: AccountStream,
        client: LighterClient,
        limiter: RateLimiter,
        metrics: Metrics,
        journal: Journal,
        request_stop: Callable[[int], None],
    ) -> None:
        self._cfg = cfg
        self._meta = meta
        self._trader = trader
        self._reconciler = reconciler
        self._market = market
        self._account = account
        self._client = client
        self._limiter = limiter
        self._metrics = metrics
        self._journal = journal
        self._request_stop = request_stop
        self._stall_until = 0.0
        self._market_was_stale = True
        self._last_forced_reconnect = 0.0
        self._last_second = 0.0
        self._last_heartbeat = 0.0
        self._last_status_log = 0.0
        self._last_meta_refresh = time.monotonic()
        self._background: set[asyncio.Task[None]] = set()
        self._ping_running = False

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        next_tick = loop.time() + TICK_S
        while True:
            await asyncio.sleep(max(0.0, next_tick - loop.time()))
            now = loop.time()
            lag_ms = (now - next_tick) * 1000.0
            next_tick = now + TICK_S
            now_ns = time.monotonic_ns()
            self._check_loop(now, lag_ms)
            self._check_market(now, now_ns)
            if now - self._last_second >= 1.0:
                self._last_second = now
                self._every_second(now, now_ns)

    # ----------------------------------------------------------------- checks

    def _check_loop(self, now: float, lag_ms: float) -> None:
        blocks = self._trader.blocks
        if lag_ms > LOOP_STALL_MS:
            log.warning("EVENT_LOOP_STALL lag_ms=%.0f", lag_ms)
            blocks["LOOP_STALL"] = f"event loop stalled {lag_ms:.0f} ms"
            self._stall_until = now + LOOP_STALL_HOLD_S
        elif "LOOP_STALL" in blocks and now >= self._stall_until:
            del blocks["LOOP_STALL"]

    def _check_market(self, now: float, now_ns: int) -> None:
        trader = self._trader
        age_ms = self._market.staleness_ms(now_ns)
        threshold = self._cfg.market_data_stale_ms
        if age_ms > threshold:
            if not self._market_was_stale:
                log.warning("MARKET_DATA_STALE age_ms=%.0f lag_ms=%d", age_ms, self._market.feed_lag_ms)
                self._market_was_stale = True
            trader.blocks["MARKET_DATA_STALE"] = "market data older than MARKET_DATA_STALE_MS"
            trader.on_market_stale()  # with exposure: recovery -> authoritative state -> flatten
            # A connection that has been up for a while but delivers nothing is replaced. A fresh
            # connection is given time to deliver its first snapshot.
            if (
                self._market.connected
                and age_ms > 3 * threshold
                and time.monotonic() - self._market.connected_at > FORCED_RECONNECT_GAP_S
                and now - self._last_forced_reconnect > FORCED_RECONNECT_GAP_S
            ):
                self._last_forced_reconnect = now
                log.warning("MARKET_WS_FORCE_RECONNECT age_ms=%.0f", age_ms)
                self._market.request_reconnect()
        elif self._market_was_stale:
            self._market_was_stale = False
            trader.blocks.pop("MARKET_DATA_STALE", None)
            log.info("MARKET_DATA_FRESH")

    # ------------------------------------------------------------ once a second

    def _every_second(self, now: float, now_ns: int) -> None:
        trader = self._trader
        if trader.fatal_reason is not None:
            self._request_stop(78)
        sd_notify("WATCHDOG=1")
        for key, value in (
            ("ws_ping_market_ms", self._market.ping_ms()),
            ("ws_ping_account_ms", self._account.ping_ms()),
        ):
            if value is not None:
                self._metrics.add(key, value)
        self._journal.write_status(self.build_status(now_ns))
        if now - self._last_heartbeat >= 5.0:
            self._last_heartbeat = now
            self._journal.save_state(trader.snapshot_state("HEARTBEAT"))
        finished = self._metrics.roll_day()
        if finished is not None:
            self._journal.write_daily_summary(finished.date, finished.to_summary())
            log.info(
                "DAILY_SUMMARY date=%s trades=%d net_pnl=%.4f", finished.date, finished.trades, finished.net_pnl_usd
            )
        self._schedule_background(now)
        if now - self._last_status_log >= STATUS_LOG_S:
            self._last_status_log = now
            self._log_status()

    def _schedule_background(self, now: float) -> None:
        trader = self._trader
        flat = trader.sm.state is State.FLAT
        reconciler = self._reconciler
        if (
            flat
            and self._account.synced
            and not reconciler.busy
            and time.monotonic() - reconciler.last_sync_monotonic >= self._cfg.reconcile_interval_s
        ):
            reconciler.request_resync("PERIODIC")
        if flat and self._account.synced and self._client.token_age_remaining() < TOKEN_RENEW_MARGIN_S:
            log.info("AUTH_TOKEN_RENEW")
            self._account.request_reconnect()  # reconnects with a fresh token, then resyncs
        if not self._ping_running and time.monotonic() - self._client.last_request_monotonic > KEEP_WARM_S:
            self._spawn(self._keep_warm())
        if flat and time.monotonic() - self._last_meta_refresh > META_REFRESH_S:
            self._last_meta_refresh = time.monotonic()
            self._spawn(self._refresh_meta())

    def _spawn(self, coro: Any) -> None:
        task = asyncio.get_running_loop().create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _keep_warm(self) -> None:
        """Keep one TLS connection open so the next order does not pay for a handshake."""
        self._ping_running = True
        try:
            await self._client.ping()
        except ApiError as exc:
            log.warning("API_PING_FAILED class=%s", exc.error_class.value)
        finally:
            self._ping_running = False

    async def _refresh_meta(self) -> None:
        """Re-validate BTC market metadata; a precision change requires a restart."""
        try:
            fresh = await self._client.fetch_market_meta(self._cfg.market_symbol)
        except (ApiError, FatalError, KeyError, ValueError) as exc:
            log.error("MARKET_META_REFRESH_FAILED error=%s", exc)
            return
        trader = self._trader
        old = self._meta
        if (fresh.market_id, fresh.price_decimals, fresh.size_decimals) != (
            old.market_id,
            old.price_decimals,
            old.size_decimals,
        ):
            trader.halt("BTC market metadata changed on the exchange; restart required", exit_process=True)
        elif fresh.status != "active" or fresh.min_imf > trader.limits.imf:
            trader.blocks["MARKET_NOT_TRADEABLE"] = f"status={fresh.status} min_imf={fresh.min_imf}"
            log.error("MARKET_NOT_TRADEABLE status=%s min_imf=%d", fresh.status, fresh.min_imf)
        else:
            trader.blocks.pop("MARKET_NOT_TRADEABLE", None)

    # ----------------------------------------------------------------- status

    def build_status(self, now_ns: int) -> dict[str, Any]:
        trader = self._trader
        age_ms = self._market.staleness_ms(now_ns)
        status: dict[str, Any] = {
            "ts": utc_iso(),
            "status": "LIVE",
            "mode": "MAINNET",
            "version": __version__,
            "pid": os.getpid(),
            "market": f"{self._meta.symbol} perpetual",
            "leverage": self._cfg.leverage,
            "leverage_confirmed": self._reconciler.leverage_confirmed,
            "market_ws": "connected" if self._market.connected else "disconnected",
            "account_ws": "connected" if self._account.synced else "disconnected",
            "book_age_ms": None if age_ms == float("inf") else round(age_ms, 1),
            "feed_lag_ms": self._market.feed_lag_ms,
            "rate_limit": self._limiter.snapshot(),
            "metrics": self._metrics.snapshot(),
        }
        status.update(trader_view(trader, now_ns))
        return status

    def _log_status(self) -> None:
        trader = self._trader
        day = self._metrics.daily
        limit = self._limiter.snapshot()
        log.info(
            "STATUS state=%s trades=%d wins=%d losses=%d realized_pnl=%.4f blocks=%s req_headroom=%s",
            trader.sm.state.value,
            day.trades,
            day.wins,
            day.losses,
            day.net_pnl_usd,
            ",".join(sorted(trader.blocks)) or "-",
            limit["request_headroom"],
        )
