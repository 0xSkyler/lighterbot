"""Operator command channel: delivery, expiry, persistence of the pause flag."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import pytest

from scalper import control
from scalper.control import Command, ControlInbox, control_dir, is_paused, purge_commands, set_paused, submit_command


async def wait_for(predicate: object, limit_s: float = 2.0) -> None:
    deadline = time.monotonic() + limit_s
    while not predicate() and time.monotonic() < deadline:  # type: ignore[operator]
        await asyncio.sleep(0.02)


def start_inbox(tmp_path: Path) -> tuple[ControlInbox, list[Command]]:
    received: list[Command] = []
    inbox = ControlInbox(tmp_path, asyncio.get_running_loop(), received.append, poll_s=0.02)
    inbox.start()
    return inbox, received


def test_unknown_command_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        submit_command(tmp_path, "double_position")
    assert list(control_dir(tmp_path).glob("*")) == [] or not control_dir(tmp_path).exists()


def test_pause_flag_round_trip(tmp_path: Path) -> None:
    assert not is_paused(tmp_path)
    set_paused(tmp_path, True)
    assert is_paused(tmp_path)
    set_paused(tmp_path, True)  # idempotent
    set_paused(tmp_path, False)
    set_paused(tmp_path, False)
    assert not is_paused(tmp_path)


async def test_commands_are_delivered_in_order_on_the_event_loop(tmp_path: Path) -> None:
    inbox, received = start_inbox(tmp_path)
    try:
        first = submit_command(tmp_path, "pause")
        await asyncio.sleep(0.005)
        second = submit_command(tmp_path, "resume")
        await wait_for(lambda: len(received) == 2)
    finally:
        inbox.stop()
    assert [(c.id, c.name) for c in received] == [(first, "pause"), (second, "resume")]
    assert list(control_dir(tmp_path).glob("cmd-*.json")) == []  # consumed exactly once


async def test_pause_state_is_persisted_before_the_bot_acts(tmp_path: Path) -> None:
    inbox, received = start_inbox(tmp_path)
    try:
        submit_command(tmp_path, "pause")
        await wait_for(lambda: len(received) == 1)
        assert is_paused(tmp_path)
        submit_command(tmp_path, "resume")
        await wait_for(lambda: len(received) == 2)
        assert not is_paused(tmp_path)
        submit_command(tmp_path, "flatten")  # flatten implies pause
        await wait_for(lambda: len(received) == 3)
        assert is_paused(tmp_path)
    finally:
        inbox.stop()


async def test_stale_command_is_dropped_not_run_late(tmp_path: Path) -> None:
    directory = control_dir(tmp_path)
    directory.mkdir(parents=True)
    old = {"id": "old", "command": "resume", "created": time.time() - control.MAX_COMMAND_AGE_S - 5}
    (directory / "cmd-old.json").write_text(json.dumps(old), encoding="utf-8")
    set_paused(tmp_path, True)
    inbox, received = start_inbox(tmp_path)
    try:
        await wait_for(lambda: not list(directory.glob("cmd-*.json")))
        await asyncio.sleep(0.1)
    finally:
        inbox.stop()
    assert received == []
    assert is_paused(tmp_path)  # an expired "resume" must not re-enable entries


async def test_malformed_command_files_are_ignored(tmp_path: Path) -> None:
    directory = control_dir(tmp_path)
    directory.mkdir(parents=True)
    (directory / "cmd-bad.json").write_text("{not json", encoding="utf-8")
    (directory / "cmd-evil.json").write_text(
        json.dumps({"id": "x", "command": "set_leverage", "created": time.time()}), encoding="utf-8"
    )
    inbox, received = start_inbox(tmp_path)
    try:
        good = submit_command(tmp_path, "pause")
        await wait_for(lambda: len(received) == 1)
    finally:
        inbox.stop()
    assert [c.id for c in received] == [good]
    assert list(directory.glob("cmd-*.json")) == []


def test_purge_removes_queued_commands_but_keeps_the_pause_flag(tmp_path: Path) -> None:
    submit_command(tmp_path, "resume")
    submit_command(tmp_path, "flatten")
    set_paused(tmp_path, True)
    assert purge_commands(tmp_path) == 2
    assert list(control_dir(tmp_path).glob("cmd-*.json")) == []
    assert is_paused(tmp_path)
    assert purge_commands(tmp_path / "missing") == 0
