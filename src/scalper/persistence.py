"""Crash-recovery state and the trade journal.

Trading state lives in memory. This module only records it: every public
method enqueues a small item and returns immediately; a dedicated writer thread
owns the SQLite connection and all file I/O, so the event loop is never behind
a disk write or a database transaction.

After a crash the exchange is the authority (see ``reconciliation.py``); the
persisted snapshot supplies context such as the last client order index.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import queue
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger("scalper.persistence")

DB_FILE = "scalper.db"
STATUS_FILE = "status.json"
CSV_FILE = "trades.csv"
EVENT_RETENTION_DAYS = 14

TRADE_COLUMNS = (
    "trade_id",
    "side",
    "leverage",
    "size",
    "signal_score",
    "signal_components",
    "entry_decision_at",
    "entry_sent_at",
    "entry_fill_at",
    "avg_entry",
    "exit_decision_at",
    "exit_sent_at",
    "exit_fill_at",
    "avg_exit",
    "holding_ms",
    "gross_pnl_usd",
    "fees_usd",
    "estimated_slippage_usd",
    "realized_pnl_usd",
    "mfe_usd",
    "mae_usd",
    "exit_reason",
    "adopted",
    "latency",
    "trade_date",
)

_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS trades (
    {", ".join(f"{c} {'TEXT PRIMARY KEY' if c == 'trade_id' else 'TEXT'}" for c in TRADE_COLUMNS)}
);
CREATE INDEX IF NOT EXISTS trades_by_date ON trades (trade_date);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, kind TEXT NOT NULL, detail TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS daily_summary (
    date TEXT PRIMARY KEY, summary TEXT NOT NULL, updated_at TEXT NOT NULL
);
"""


def utc_iso(epoch_ns: int | None = None) -> str:
    """UTC ISO-8601 timestamp with millisecond precision."""
    ns = time.time_ns() if epoch_ns is None else epoch_ns
    seconds, remainder = divmod(ns, 1_000_000_000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds)) + f".{remainder // 1_000_000:03d}Z"


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=5.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(_SCHEMA)
    return conn


class Journal:
    """Asynchronous, append-mostly persistence backed by SQLite and a few small files."""

    def __init__(self, data_dir: Path, *, write_csv: bool = True) -> None:
        self._dir = data_dir
        self._write_csv = write_csv
        self._queue: queue.SimpleQueue[tuple[str, Any] | None] = queue.SimpleQueue()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------ lifecycle

    def start(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        (self._dir / "daily").mkdir(exist_ok=True)
        _connect(self._dir / DB_FILE).close()  # fail fast if the database is unusable
        self._thread = threading.Thread(target=self._run, name="journal-writer", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Flush everything queued so far and stop the writer."""
        if self._thread is None:
            return
        self._queue.put(None)
        self._thread.join(timeout)
        self._thread = None

    # ------------------------------------------------- non-blocking writers

    def save_state(self, snapshot: dict[str, Any]) -> None:
        """Persist the latest recovery snapshot (state, position, order ids, heartbeat)."""
        self._queue.put(("state", snapshot))

    def record_trade(self, trade: dict[str, Any]) -> None:
        self._queue.put(("trade", trade))

    def record_event(self, kind: str, detail: dict[str, Any]) -> None:
        self._queue.put(("event", (utc_iso(), kind, detail)))

    def write_status(self, status: dict[str, Any]) -> None:
        self._queue.put(("status", status))

    def write_daily_summary(self, date: str, summary: dict[str, Any]) -> None:
        self._queue.put(("daily", (date, summary)))

    # ------------------------------------------------- blocking readers (startup / CLI)

    def load_state(self) -> dict[str, Any]:
        """Last persisted recovery snapshot, or {} if none. Startup only."""
        path = self._dir / DB_FILE
        if not path.is_file():
            return {}
        conn = _connect(path)
        try:
            row = conn.execute("SELECT value FROM kv WHERE key='state'").fetchone()
        finally:
            conn.close()
        if not row:
            return {}
        try:
            loaded = json.loads(row[0])
        except ValueError:
            return {}
        return loaded if isinstance(loaded, dict) else {}

    def load_day_totals(self, date: str) -> list[tuple[float, float, float, float]]:
        """``(gross, fees, net, holding_ms)`` for every journaled trade of a UTC date. Startup only."""
        path = self._dir / DB_FILE
        if not path.is_file():
            return []
        conn = _connect(path)
        try:
            rows = conn.execute(
                "SELECT gross_pnl_usd, fees_usd, realized_pnl_usd, holding_ms FROM trades WHERE trade_date=?",
                (date,),
            ).fetchall()
        finally:
            conn.close()
        totals = []
        for gross, fees, net, hold in rows:
            try:
                totals.append((float(gross), float(fees), float(net), float(hold)))
            except (TypeError, ValueError):
                continue
        return totals

    @staticmethod
    def read_status(data_dir: Path) -> dict[str, Any] | None:
        """Latest status snapshot written by the running service, if any."""
        try:
            loaded = json.loads((data_dir / STATUS_FILE).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return loaded if isinstance(loaded, dict) else None

    # --------------------------------------------------------- writer thread

    def _run(self) -> None:
        conn = _connect(self._dir / DB_FILE)
        last_prune = 0.0
        try:
            while True:
                item = self._queue.get()
                batch = [item]
                while len(batch) < 256:
                    try:
                        batch.append(self._queue.get_nowait())
                    except queue.Empty:
                        break
                stop = False
                for entry in batch:
                    if entry is None:
                        stop = True
                        continue
                    try:
                        self._apply(conn, entry[0], entry[1])
                    except (sqlite3.Error, OSError, ValueError, TypeError) as exc:
                        log.error("PERSIST_ERROR kind=%s error=%s", entry[0], exc)
                try:
                    conn.commit()
                except sqlite3.Error as exc:
                    log.error("PERSIST_ERROR kind=commit error=%s", exc)
                if time.monotonic() - last_prune > 3600:
                    last_prune = time.monotonic()
                    self._prune(conn)
                if stop:
                    return
        finally:
            conn.close()

    def _apply(self, conn: sqlite3.Connection, kind: str, payload: Any) -> None:
        if kind == "state":
            conn.execute(
                "INSERT INTO kv (key, value, updated_at) VALUES ('state', ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (json.dumps(payload, separators=(",", ":")), utc_iso()),
            )
        elif kind == "trade":
            row = [_cell(payload.get(column)) for column in TRADE_COLUMNS]
            conn.execute(
                f"INSERT OR REPLACE INTO trades ({', '.join(TRADE_COLUMNS)}) "
                f"VALUES ({', '.join('?' * len(TRADE_COLUMNS))})",
                row,
            )
            if self._write_csv:
                self._append_csv(row)
        elif kind == "event":
            ts, name, detail = payload
            conn.execute(
                "INSERT INTO events (ts, kind, detail) VALUES (?, ?, ?)",
                (ts, name, json.dumps(detail, separators=(",", ":"), default=str)),
            )
        elif kind == "status":
            _atomic_write(self._dir / STATUS_FILE, json.dumps(payload, separators=(",", ":"), default=str))
        elif kind == "daily":
            date, summary = payload
            text = json.dumps(summary, indent=2, default=str)
            conn.execute(
                "INSERT INTO daily_summary (date, summary, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(date) DO UPDATE SET summary=excluded.summary, updated_at=excluded.updated_at",
                (date, text, utc_iso()),
            )
            _atomic_write(self._dir / "daily" / f"{date}.json", text)

    def _append_csv(self, row: list[str | None]) -> None:
        path = self._dir / CSV_FILE
        is_new = not path.exists()
        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            if is_new:
                writer.writerow(TRADE_COLUMNS)
            writer.writerow(row)

    @staticmethod
    def _prune(conn: sqlite3.Connection) -> None:
        """Bound the events table; trades and daily summaries are kept."""
        cutoff = utc_iso(time.time_ns() - EVENT_RETENTION_DAYS * 86_400 * 1_000_000_000)
        try:
            conn.execute("DELETE FROM events WHERE ts < ?", (cutoff,))
            conn.commit()
        except sqlite3.Error as exc:
            log.error("PERSIST_ERROR kind=prune error=%s", exc)


def _cell(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict | list):
        return json.dumps(value, separators=(",", ":"))
    return str(value)


def _atomic_write(path: Path, text: str) -> None:
    """Write via a temp file + rename so readers never see a partial file."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)
