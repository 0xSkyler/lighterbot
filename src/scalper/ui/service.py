"""Starting and stopping the trading service from the control panel.

Two ways the bot can be run, detected automatically:

* **systemd** (server install): ``systemctl start|stop|restart lighter-scalper``.
  The panel's user is allowed to manage exactly that one unit by a polkit rule.
* **process** (no systemd, e.g. a desktop PC): the panel launches ``lighter-scalper run``
  itself as a detached process and stops it gracefully through the operator
  command channel, so a stop still flattens before exiting.

Stopping is always graceful. The panel never kills the trading process.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from ..control import submit_command

UNIT = "lighter-scalper.service"
STATUS_FRESH_S = 5.0
STARTUP_GRACE_S = 240.0
_SYSTEMCTL_TIMEOUT_S = 15.0


@dataclass(frozen=True, slots=True)
class ServiceState:
    mode: str  # systemd | process
    state: str  # running | starting | stopping | stopped | failed
    detail: str
    pid: int | None = None

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ActionResult:
    ok: bool
    message: str


class ServiceManager(Protocol):
    mode: str

    async def status(self) -> ServiceState: ...

    async def start(self) -> ActionResult: ...

    async def stop(self) -> ActionResult: ...

    async def restart(self) -> ActionResult: ...


def bot_command(env_file: Path, *args: str) -> list[str]:
    """Command line that runs the bot CLI against the same environment file the panel edits."""
    command = [sys.executable, "-m", "scalper.main"]
    if env_file.is_file():
        command += ["--env-file", str(env_file)]
    return [*command, *args]


def bot_environment(data_dir: Path, log_dir: Path) -> dict[str, str]:
    """Process environment for the bot CLI: the panel's directories, so both look at the same files."""
    env = dict(os.environ)
    env["DATA_DIR"] = str(data_dir)
    env["LOG_DIR"] = str(log_dir)
    return env


def status_age_s(data_dir: Path) -> float | None:
    """Seconds since the bot last wrote its status snapshot (None if it never has)."""
    try:
        return max(0.0, time.time() - (data_dir / "status.json").stat().st_mtime)
    except OSError:
        return None


# --------------------------------------------------------------------- systemd


async def _run(*args: str) -> tuple[int, str]:
    try:
        process = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        output, _ = await asyncio.wait_for(process.communicate(), _SYSTEMCTL_TIMEOUT_S)
    except (TimeoutError, OSError) as exc:
        return 1, f"{type(exc).__name__}: {exc}"
    return process.returncode or 0, output.decode("utf-8", "replace").strip()


class SystemdManager:
    """Controls the installed ``lighter-scalper.service`` unit."""

    mode = "systemd"

    def __init__(self, unit: str = UNIT) -> None:
        self._unit = unit

    async def status(self) -> ServiceState:
        code, output = await _run(
            "systemctl", "show", self._unit, "--no-pager",
            "-p", "LoadState", "-p", "ActiveState", "-p", "SubState", "-p", "MainPID",
            "-p", "ExecMainStatus", "-p", "Result",
        )  # fmt: skip
        if code != 0:
            return ServiceState(self.mode, "stopped", f"systemctl failed: {output[:200]}")
        props = dict(line.split("=", 1) for line in output.splitlines() if "=" in line)
        return self._interpret(props)

    def _interpret(self, props: dict[str, str]) -> ServiceState:
        active = props.get("ActiveState", "")
        sub = props.get("SubState", "")
        pid = int(props.get("MainPID") or 0) or None
        exit_status = props.get("ExecMainStatus", "0")
        if props.get("LoadState") == "not-found":
            return ServiceState(self.mode, "stopped", "the lighter-scalper service is not installed")
        if active == "active":
            return ServiceState(self.mode, "running", "running under systemd", pid)
        if active == "activating":
            if sub == "auto-restart":
                return ServiceState(self.mode, "starting", "restarting after a failure (systemd is retrying)", pid)
            return ServiceState(self.mode, "starting", "starting: connecting and reconciling with the exchange", pid)
        if active == "deactivating":
            return ServiceState(self.mode, "stopping", "stopping: resolving orders and flattening if configured", pid)
        if active == "failed":
            if exit_status == "78":
                return ServiceState(
                    self.mode, "failed", "stopped: configuration or credentials were rejected. Check Settings and Logs."
                )
            return ServiceState(self.mode, "failed", f"failed ({props.get('Result', 'unknown')}). See Logs.")
        return ServiceState(self.mode, "stopped", "stopped")

    async def _action(self, verb: str, done: str) -> ActionResult:
        # --no-block: starting waits for reconciliation and stopping may flatten first.
        code, output = await _run("systemctl", "--no-ask-password", "--no-block", verb, self._unit)
        if code == 0:
            return ActionResult(True, done)
        hint = ""
        if (
            "authentication" in output.lower()
            or "access denied" in output.lower()
            or "not authorized" in output.lower()
        ):
            hint = (
                " The panel is not allowed to control the service: re-run deploy/install.sh to install the polkit rule."
            )
        return ActionResult(False, f"systemctl {verb} failed: {output[:300]}{hint}")

    async def start(self) -> ActionResult:
        return await self._action("start", "Start requested. The bot reconciles with the exchange before trading.")

    async def stop(self) -> ActionResult:
        return await self._action("stop", "Stop requested. The bot resolves open orders and flattens if configured.")

    async def restart(self) -> ActionResult:
        return await self._action("restart", "Restart requested.")


# --------------------------------------------------------------------- process


class ProcessManager:
    """Runs the bot as a detached child process when there is no systemd unit."""

    mode = "process"

    def __init__(self, data_dir: Path, log_dir: Path, env_file: Path, command: list[str] | None = None) -> None:
        self._data_dir = data_dir
        self._log_dir = log_dir
        self._env_file = env_file
        self._command = command
        self._process: subprocess.Popen[bytes] | None = None
        self._started = 0.0
        self._stop_requested = 0.0

    def _alive(self) -> bool:
        age = status_age_s(self._data_dir)
        return age is not None and age < STATUS_FRESH_S

    async def status(self) -> ServiceState:
        process = self._process
        exit_code = process.poll() if process is not None else None
        if self._alive():
            if self._stop_requested and time.monotonic() - self._stop_requested < STARTUP_GRACE_S:
                return ServiceState(self.mode, "stopping", "stopping: resolving orders and flattening if configured")
            return ServiceState(self.mode, "running", "running", process.pid if process is not None else None)
        self._stop_requested = 0.0
        if process is not None and exit_code is None and time.monotonic() - self._started < STARTUP_GRACE_S:
            return ServiceState(self.mode, "starting", "starting: connecting and reconciling", process.pid)
        if process is not None and exit_code not in (None, 0):
            if exit_code == 78:
                return ServiceState(
                    self.mode, "failed", "stopped: configuration or credentials were rejected. Check Settings and Logs."
                )
            return ServiceState(self.mode, "failed", f"exited with code {exit_code}. See Logs.")
        return ServiceState(self.mode, "stopped", "stopped")

    async def start(self) -> ActionResult:
        if self._alive() or (self._process is not None and self._process.poll() is None):
            return ActionResult(False, "The bot is already running.")
        try:
            self._process = await asyncio.to_thread(self._spawn)
        except OSError as exc:
            return ActionResult(False, f"could not start the bot: {exc}")
        self._started = time.monotonic()
        self._stop_requested = 0.0
        return ActionResult(True, "Start requested. The bot reconciles with the exchange before trading.")

    def _spawn(self) -> subprocess.Popen[bytes]:
        self._log_dir.mkdir(parents=True, exist_ok=True)
        console = open(self._log_dir / "bot-console.log", "ab")
        kwargs: dict[str, Any] = {}
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
        else:
            kwargs["start_new_session"] = True  # the bot must outlive a restart of the panel
        try:
            return subprocess.Popen(
                self._command or bot_command(self._env_file, "run"),
                stdin=subprocess.DEVNULL,
                stdout=console,
                stderr=subprocess.STDOUT,
                env=bot_environment(self._data_dir, self._log_dir),
                **kwargs,
            )
        finally:
            console.close()

    async def stop(self) -> ActionResult:
        if not self._alive():
            return ActionResult(False, "The bot is not running.")
        await asyncio.to_thread(submit_command, self._data_dir, "stop")
        self._stop_requested = time.monotonic()
        return ActionResult(True, "Stop requested. The bot resolves open orders and flattens if configured.")

    async def restart(self) -> ActionResult:
        if self._alive():
            stopped = await self.stop()
            if not stopped.ok:
                return stopped
            deadline = time.monotonic() + STARTUP_GRACE_S
            while self._alive() and time.monotonic() < deadline:
                await asyncio.sleep(0.5)
            if self._alive():
                return ActionResult(False, "The bot did not stop in time; it was not restarted.")
        return await self.start()


def detect_manager(mode: str, data_dir: Path, log_dir: Path, env_file: Path) -> ServiceManager:
    """Pick the service manager: ``systemd`` when the unit is installed, else ``process``."""
    if mode == "process":
        return ProcessManager(data_dir, log_dir, env_file)
    if mode == "systemd":
        return SystemdManager()
    systemctl = shutil.which("systemctl")
    if systemctl:
        try:
            found = (
                subprocess.run([systemctl, "cat", UNIT], capture_output=True, timeout=10, check=False).returncode == 0
            )
        except (OSError, subprocess.SubprocessError):
            found = False
        if found:
            return SystemdManager()
    return ProcessManager(data_dir, log_dir, env_file)
