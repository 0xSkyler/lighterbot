"""Operator control channel between the control panel (or CLI) and the running bot.

The panel never reaches into the trading process. It drops a small command file
into ``DATA_DIR/control/``; a dedicated thread in the bot picks it up and hands
it to the event loop. There is no listening socket in the trading process and no
file I/O on the event loop.

Commands only move in the safe direction or are explicit operator actions:

* ``pause``   - stop opening new positions (an open position is still managed)
* ``resume``  - allow entries again
* ``flatten`` - pause, then close any BTC exposure now
* ``stop``    - graceful shutdown (used when the bot is not run by systemd)

The paused state is a flag file, so it survives restarts: a bot that was paused
comes back paused.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger("scalper.control")

CONTROL_DIR = "control"
PAUSE_FLAG = "paused"
COMMANDS = ("pause", "resume", "flatten", "stop")
MAX_COMMAND_AGE_S = 15.0  # a command that was not picked up promptly is dropped, never run late
POLL_S = 0.2
_PREFIX = "cmd-"


@dataclass(frozen=True, slots=True)
class Command:
    id: str
    name: str
    created: float  # epoch seconds


def control_dir(data_dir: Path) -> Path:
    return data_dir / CONTROL_DIR


def is_paused(data_dir: Path) -> bool:
    """True if entries are paused (persisted across restarts)."""
    return (control_dir(data_dir) / PAUSE_FLAG).exists()


def set_paused(data_dir: Path, paused: bool) -> None:
    directory = control_dir(data_dir)
    flag = directory / PAUSE_FLAG
    if paused:
        directory.mkdir(parents=True, exist_ok=True)
        flag.write_text(str(int(time.time())), encoding="utf-8")
    else:
        flag.unlink(missing_ok=True)


def submit_command(data_dir: Path, name: str) -> str:
    """Queue a command for the running bot. Returns the command id."""
    if name not in COMMANDS:
        raise ValueError(f"unknown command: {name}")
    directory = control_dir(data_dir)
    directory.mkdir(parents=True, exist_ok=True)
    command_id = f"{int(time.time() * 1000)}-{secrets.token_hex(4)}"
    payload = json.dumps({"id": command_id, "command": name, "created": time.time()})
    tmp = directory / f".{command_id}.tmp"
    tmp.write_text(payload, encoding="utf-8")
    os.replace(tmp, directory / f"{_PREFIX}{command_id}.json")  # atomic: never read half-written
    return command_id


def purge_commands(data_dir: Path) -> int:
    """Delete queued commands. Called at startup so nothing stale runs against a fresh process."""
    directory = control_dir(data_dir)
    if not directory.is_dir():
        return 0
    removed = 0
    for path in directory.glob(f"{_PREFIX}*.json"):
        path.unlink(missing_ok=True)
        removed += 1
    return removed


def _read_command(path: Path) -> Command | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        command = Command(str(data["id"]), str(data["command"]), float(data["created"]))
    except (OSError, ValueError, KeyError, TypeError):
        return None
    return command if command.name in COMMANDS else None


class ControlInbox:
    """Background thread that delivers queued commands to the event loop."""

    def __init__(
        self,
        data_dir: Path,
        loop: asyncio.AbstractEventLoop,
        handler: Callable[[Command], None],
        poll_s: float = POLL_S,
    ) -> None:
        self._data_dir = data_dir
        self._dir = control_dir(data_dir)
        self._loop = loop
        self._handler = handler
        self._poll_s = poll_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        self._thread = threading.Thread(target=self._run, name="control-inbox", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(2.0)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.wait(self._poll_s):
            try:
                paths = sorted(self._dir.glob(f"{_PREFIX}*.json"))
            except OSError:
                continue
            for path in paths:
                command = _read_command(path)
                path.unlink(missing_ok=True)
                if command is None:
                    log.warning("CONTROL_COMMAND_INVALID file=%s", path.name)
                    continue
                age = time.time() - command.created
                if age > MAX_COMMAND_AGE_S:
                    log.warning("CONTROL_COMMAND_EXPIRED command=%s age_s=%.0f", command.name, age)
                    continue
                self._persist(command)
                try:
                    self._loop.call_soon_threadsafe(self._handler, command)
                except RuntimeError:  # the event loop is closed: the process is going down
                    return

    def _persist(self, command: Command) -> None:
        """Record the paused state on disk here, on this thread, before the bot acts on it."""
        try:
            if command.name in ("pause", "flatten"):
                set_paused(self._data_dir, True)
            elif command.name == "resume":
                set_paused(self._data_dir, False)
        except OSError as exc:
            log.error("CONTROL_PAUSE_FLAG_ERROR error=%s", exc)
