"""Rolling performance and latency statistics.

Everything is bounded (fixed-size deques) and updated with O(1) appends on the
trading path. Percentiles are only computed when a status snapshot is built.
Wins and losses are counted exclusively from confirmed exit fills.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import asdict, dataclass, field

from .errors import ErrorClass

NS_PER_MS = 1_000_000


class RollingStat:
    """Last N samples of one measurement."""

    __slots__ = ("_values",)

    def __init__(self, maxlen: int = 512) -> None:
        self._values: deque[float] = deque(maxlen=maxlen)

    def add(self, value: float) -> None:
        self._values.append(value)

    def percentile(self, q: float) -> float | None:
        if not self._values:
            return None
        ordered = sorted(self._values)
        index = min(len(ordered) - 1, max(0, round(q * (len(ordered) - 1))))
        return ordered[index]

    def mean(self) -> float | None:
        return sum(self._values) / len(self._values) if self._values else None

    def last(self) -> float | None:
        return self._values[-1] if self._values else None

    def summary(self) -> dict[str, float | None]:
        return {
            "last": _r(self.last()),
            "p50": _r(self.percentile(0.5)),
            "p95": _r(self.percentile(0.95)),
            "mean": _r(self.mean()),
        }


def _r(value: float | None) -> float | None:
    return None if value is None else round(value, 3)


@dataclass(slots=True)
class DailyStats:
    """Aggregates for one UTC day. Persisted as the daily summary."""

    date: str
    trades: int = 0
    wins: int = 0
    losses: int = 0
    gross_pnl_usd: float = 0.0
    fees_usd: float = 0.0
    net_pnl_usd: float = 0.0
    largest_win_usd: float = 0.0
    largest_loss_usd: float = 0.0
    hold_ms_total: float = 0.0
    exec_latency_ms_total: float = 0.0
    exec_latency_samples: int = 0
    reconnects: int = 0
    errors: int = 0

    def to_summary(self) -> dict[str, float | int | str | None]:
        data: dict[str, float | int | str | None] = dict(asdict(self))
        data["avg_hold_ms"] = round(self.hold_ms_total / self.trades, 1) if self.trades else None
        data["avg_execution_latency_ms"] = (
            round(self.exec_latency_ms_total / self.exec_latency_samples, 2) if self.exec_latency_samples else None
        )
        data["win_pct"] = round(100.0 * self.wins / self.trades, 2) if self.trades else None
        for key in ("gross_pnl_usd", "fees_usd", "net_pnl_usd", "largest_win_usd", "largest_loss_usd"):
            data[key] = round(float(data[key] or 0.0), 6)
        del data["hold_ms_total"], data["exec_latency_ms_total"], data["exec_latency_samples"]
        return data


def utc_date(epoch_s: float | None = None) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(epoch_s))


LATENCY_KEYS = (
    "signal_to_send_ms",
    "send_to_ack_ms",
    "send_to_fill_ms",
    "fill_to_profit_ms",
    "profit_to_exit_send_ms",
    "exit_send_to_flat_ms",
    "hold_ms",
    "api_read_ms",
    "ws_ping_market_ms",
    "ws_ping_account_ms",
    "entry_slippage_bps",
    "exit_slippage_bps",
)


@dataclass(slots=True)
class Metrics:
    """All rolling statistics of the running service."""

    stats: dict[str, RollingStat] = field(default_factory=lambda: {k: RollingStat() for k in LATENCY_KEYS})
    daily: DailyStats = field(default_factory=lambda: DailyStats(utc_date()))
    wins_usd: RollingStat = field(default_factory=RollingStat)
    losses_usd: RollingStat = field(default_factory=RollingStat)
    trade_times: deque[float] = field(default_factory=lambda: deque(maxlen=2048))
    entries_sent: int = 0
    entries_missed: int = 0
    entries_cancelled: int = 0
    partial_fills: int = 0
    order_rejections: int = 0
    reconnects_market: int = 0
    reconnects_account: int = 0
    recoveries: int = 0
    errors: dict[str, int] = field(default_factory=dict)
    skips: dict[str, int] = field(default_factory=dict)

    def add(self, key: str, value: float) -> None:
        self.stats[key].add(value)

    def add_ns(self, key: str, start_ns: int, end_ns: int) -> None:
        """Record a latency in ms from two monotonic timestamps (ignored if either is missing)."""
        if start_ns and end_ns and end_ns >= start_ns:
            self.stats[key].add((end_ns - start_ns) / NS_PER_MS)

    def count_error(self, error_class: ErrorClass) -> None:
        self.errors[error_class.value] = self.errors.get(error_class.value, 0) + 1
        self.daily.errors += 1
        if error_class is ErrorClass.ORDER_REJECTED:
            self.order_rejections += 1

    def count_skip(self, reason: str) -> None:
        self.skips[reason] = self.skips.get(reason, 0) + 1

    def count_reconnect(self, stream: str) -> None:
        if stream == "market":
            self.reconnects_market += 1
        else:
            self.reconnects_account += 1
        self.daily.reconnects += 1

    def roll_day(self) -> DailyStats | None:
        """If the UTC date changed, return the finished day's stats and start a new day."""
        today = utc_date()
        if today == self.daily.date:
            return None
        finished = self.daily
        self.daily = DailyStats(today)
        return finished

    def on_trade_closed(self, gross_usd: float, fees_usd: float, net_usd: float, hold_ms: float) -> None:
        """Record one completed trade. Call only after the exit fill is confirmed."""
        day = self.daily
        day.trades += 1
        day.gross_pnl_usd += gross_usd
        day.fees_usd += fees_usd
        day.net_pnl_usd += net_usd
        day.hold_ms_total += hold_ms
        if net_usd > 0:
            day.wins += 1
            self.wins_usd.add(net_usd)
            day.largest_win_usd = max(day.largest_win_usd, net_usd)
        else:
            day.losses += 1
            self.losses_usd.add(net_usd)
            day.largest_loss_usd = min(day.largest_loss_usd, net_usd)
        self.stats["hold_ms"].add(hold_ms)
        self.trade_times.append(time.monotonic())

    def restore_day(self, rows: list[tuple[float, float, float, float]]) -> None:
        """Rebuild today's totals from the journal after a restart: ``(gross, fees, net, hold_ms)``."""
        day = self.daily
        for gross_usd, fees_usd, net_usd, hold_ms in rows:
            day.trades += 1
            day.gross_pnl_usd += gross_usd
            day.fees_usd += fees_usd
            day.net_pnl_usd += net_usd
            day.hold_ms_total += hold_ms
            if net_usd > 0:
                day.wins += 1
                day.largest_win_usd = max(day.largest_win_usd, net_usd)
            else:
                day.losses += 1
                day.largest_loss_usd = min(day.largest_loss_usd, net_usd)

    def note_execution_latency(self, ms: float) -> None:
        self.daily.exec_latency_ms_total += ms
        self.daily.exec_latency_samples += 1

    def trades_per_minute(self) -> int:
        cutoff = time.monotonic() - 60.0
        return sum(1 for t in self.trade_times if t >= cutoff)

    def snapshot(self) -> dict[str, object]:
        day = self.daily
        return {
            "trades_today": day.trades,
            "wins": day.wins,
            "losses": day.losses,
            "win_pct": round(100.0 * day.wins / day.trades, 2) if day.trades else None,
            "realized_pnl_usd": round(day.net_pnl_usd, 6),
            "gross_pnl_usd": round(day.gross_pnl_usd, 6),
            "fees_usd": round(day.fees_usd, 6),
            "avg_win_usd": _r(self.wins_usd.mean()),
            "avg_loss_usd": _r(self.losses_usd.mean()),
            "trades_per_minute": self.trades_per_minute(),
            "entries_sent": self.entries_sent,
            "entries_missed": self.entries_missed,
            "entries_cancelled": self.entries_cancelled,
            "partial_fills": self.partial_fills,
            "order_rejections": self.order_rejections,
            "reconnects_market": self.reconnects_market,
            "reconnects_account": self.reconnects_account,
            "recoveries": self.recoveries,
            "errors": dict(self.errors),
            "skips": dict(self.skips),
            "latency": {key: stat.summary() for key, stat in self.stats.items()},
        }
