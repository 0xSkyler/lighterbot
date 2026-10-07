"""Structured, non-blocking logging.

Trading code only enqueues records (``QueueHandler``); a listener thread does
the formatting, secret redaction and disk/console I/O, so a slow disk can never
delay an exit. Files rotate by size so logs cannot fill the VPS disk.

Line format: ``2026-10-07T10:15:01.001Z EVENT key=value ...`` (level shown for WARNING+).
"""

from __future__ import annotations

import logging
import logging.handlers
import queue
import sys
import time
from collections.abc import Sequence
from pathlib import Path

LOG_FILE = "scalper.log"
MAX_BYTES = 20 * 1024 * 1024
BACKUP_COUNT = 5

log = logging.getLogger("scalper")


class _UtcFormatter(logging.Formatter):
    """UTC millisecond timestamps plus redaction of configured secrets."""

    def __init__(self, secrets: Sequence[str]) -> None:
        super().__init__()
        self._secrets = [s for s in secrets if s and len(s) >= 8]

    def format(self, record: logging.LogRecord) -> str:
        stamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
        message = record.getMessage()
        if record.exc_info:
            message = f"{message} | {self.formatException(record.exc_info)}"
        if record.levelno >= logging.WARNING:
            line = f"{stamp}.{int(record.msecs):03d}Z {record.levelname} {message}"
        else:
            line = f"{stamp}.{int(record.msecs):03d}Z {message}"
        for secret in self._secrets:
            if secret in line:
                line = line.replace(secret, "***REDACTED***")
        return line


class _RawQueueHandler(logging.handlers.QueueHandler):
    """Enqueue the record untouched; all formatting happens on the listener thread."""

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return record


def setup_logging(
    log_dir: Path | None, level: str, secrets: Sequence[str], *, console: bool = True
) -> logging.handlers.QueueListener:
    """Configure the ``scalper`` logger. Returns the listener; call ``stop()`` on shutdown."""
    formatter = _UtcFormatter(secrets)
    handlers: list[logging.Handler] = []
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_dir / LOG_FILE, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    if console:
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setFormatter(formatter)
        handlers.append(stream_handler)

    records: queue.SimpleQueue[logging.LogRecord] = queue.SimpleQueue()
    listener = logging.handlers.QueueListener(records, *handlers, respect_handler_level=False)
    root = logging.getLogger("scalper")
    root.handlers.clear()
    root.addHandler(_RawQueueHandler(records))
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.propagate = False
    # The SDK logs full transaction payloads at DEBUG; keep it quiet.
    logging.getLogger().setLevel(logging.WARNING)
    listener.start()
    return listener


def sanitize(text: object, secrets: Sequence[str] = ()) -> str:
    """Single-line, length-bounded text safe to put in a log line or journal row."""
    line = " ".join(str(text).split())
    for secret in secrets:
        if secret and secret in line:
            line = line.replace(secret, "***REDACTED***")
    return line[:500]
