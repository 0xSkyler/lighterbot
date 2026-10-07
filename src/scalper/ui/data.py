"""Read-only access to what the bot records: status snapshot, trade journal, events, log.

Everything here is blocking file / SQLite I/O and is called through
``asyncio.to_thread`` by the server. Nothing in this module writes.
"""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any

from ..persistence import DB_FILE, STATUS_FILE

LOG_FILE = "scalper.log"
MAX_CURVE_POINTS = 600
_TAIL_BYTES = 256 * 1024


def read_status(data_dir: Path) -> tuple[dict[str, Any] | None, float | None]:
    """Latest status snapshot and its age in seconds."""
    path = data_dir / STATUS_FILE
    try:
        age = max(0.0, time.time() - path.stat().st_mtime)
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None, None
    return (loaded, age) if isinstance(loaded, dict) else (None, None)


def _connect(data_dir: Path) -> sqlite3.Connection | None:
    path = data_dir / DB_FILE
    if not path.is_file():
        return None
    try:
        connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=2.0)
    except sqlite3.Error:
        return None
    connection.row_factory = sqlite3.Row
    return connection


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _trade(row: sqlite3.Row) -> dict[str, Any]:
    try:
        latency = json.loads(row["latency"]) if row["latency"] else {}
    except ValueError:
        latency = {}
    return {
        "trade_id": row["trade_id"],
        "closed_at": row["exit_fill_at"],
        "side": row["side"],
        "size": row["size"],
        "avg_entry": _number(row["avg_entry"]),
        "avg_exit": _number(row["avg_exit"]),
        "holding_ms": _number(row["holding_ms"]),
        "gross_pnl_usd": _number(row["gross_pnl_usd"]),
        "fees_usd": _number(row["fees_usd"]),
        "realized_pnl_usd": _number(row["realized_pnl_usd"]),
        "exit_reason": row["exit_reason"],
        "signal_score": _number(row["signal_score"]),
        "adopted": row["adopted"] in ("1", 1),
        "send_to_fill_ms": latency.get("send_to_fill_ms") if isinstance(latency, dict) else None,
    }


def read_trades(data_dir: Path, date: str | None, limit: int) -> list[dict[str, Any]]:
    """Most recent completed trades, newest first. ``date`` is a UTC date or None for all."""
    connection = _connect(data_dir)
    if connection is None:
        return []
    with closing(connection):
        try:
            if date:
                rows = connection.execute(
                    "SELECT * FROM trades WHERE trade_date = ? ORDER BY exit_fill_at DESC LIMIT ?", (date, limit)
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM trades ORDER BY exit_fill_at DESC LIMIT ?", (limit,)
                ).fetchall()
        except sqlite3.Error:
            return []
    return [_trade(row) for row in rows]


def read_pnl_curve(data_dir: Path, date: str) -> list[dict[str, Any]]:
    """Cumulative realized P&L over one UTC day, one point per trade (thinned if very long)."""
    connection = _connect(data_dir)
    if connection is None:
        return []
    with closing(connection):
        try:
            rows = connection.execute(
                "SELECT exit_fill_at, realized_pnl_usd FROM trades WHERE trade_date = ? ORDER BY exit_fill_at", (date,)
            ).fetchall()
        except sqlite3.Error:
            return []
    points: list[dict[str, Any]] = []
    total = 0.0
    for index, row in enumerate(rows, 1):
        net = _number(row["realized_pnl_usd"]) or 0.0
        total += net
        points.append({"t": row["exit_fill_at"], "n": index, "net": round(net, 6), "cum": round(total, 6)})
    if len(points) > MAX_CURVE_POINTS:
        step = len(points) / MAX_CURVE_POINTS
        kept = [points[int(i * step)] for i in range(MAX_CURVE_POINTS - 1)]
        kept.append(points[-1])  # the last point carries the day's total
        points = kept
    return points


def read_daily(data_dir: Path, days: int = 14) -> list[dict[str, Any]]:
    """Per-day totals computed from the trade journal, newest day first."""
    connection = _connect(data_dir)
    if connection is None:
        return []
    with closing(connection):
        try:
            rows = connection.execute(
                """
                SELECT trade_date AS date,
                       COUNT(*) AS trades,
                       SUM(CASE WHEN CAST(realized_pnl_usd AS REAL) > 0 THEN 1 ELSE 0 END) AS wins,
                       SUM(CAST(gross_pnl_usd AS REAL)) AS gross,
                       SUM(CAST(fees_usd AS REAL)) AS fees,
                       SUM(CAST(realized_pnl_usd AS REAL)) AS net,
                       AVG(CAST(holding_ms AS REAL)) AS avg_hold_ms,
                       MAX(CAST(realized_pnl_usd AS REAL)) AS best,
                       MIN(CAST(realized_pnl_usd AS REAL)) AS worst
                FROM trades GROUP BY trade_date ORDER BY trade_date DESC LIMIT ?
                """,
                (days,),
            ).fetchall()
        except sqlite3.Error:
            return []
    result = []
    for row in rows:
        trades = int(row["trades"] or 0)
        wins = int(row["wins"] or 0)
        result.append(
            {
                "date": row["date"],
                "trades": trades,
                "wins": wins,
                "losses": trades - wins,
                "win_pct": round(100.0 * wins / trades, 1) if trades else None,
                "gross_pnl_usd": round(row["gross"] or 0.0, 6),
                "fees_usd": round(row["fees"] or 0.0, 6),
                "net_pnl_usd": round(row["net"] or 0.0, 6),
                "avg_hold_ms": round(row["avg_hold_ms"] or 0.0, 1),
                "best_usd": round(row["best"] or 0.0, 6),
                "worst_usd": round(row["worst"] or 0.0, 6),
            }
        )
    return result


def read_events(data_dir: Path, limit: int) -> list[dict[str, Any]]:
    """Recent incident / control events, newest first."""
    connection = _connect(data_dir)
    if connection is None:
        return []
    with closing(connection):
        try:
            rows = connection.execute(
                "SELECT ts, kind, detail FROM events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        except sqlite3.Error:
            return []
    events = []
    for row in rows:
        try:
            detail = json.loads(row["detail"])
        except ValueError:
            detail = {}
        summary = ""
        if isinstance(detail, dict):
            summary = str(detail.get("reason") or detail.get("command") or detail.get("error") or "")
        events.append({"ts": row["ts"], "kind": row["kind"], "summary": summary[:200]})
    return events


def tail_log(log_dir: Path, lines: int) -> list[str]:
    """Last ``lines`` lines of the bot's log file."""
    path = log_dir / LOG_FILE
    try:
        with open(path, "rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            handle.seek(max(0, size - _TAIL_BYTES))
            data = handle.read()
    except OSError:
        return []
    text = data.decode("utf-8", "replace").splitlines()
    if size > _TAIL_BYTES and text:
        text = text[1:]  # the first line is probably cut in the middle
    return text[-lines:]
