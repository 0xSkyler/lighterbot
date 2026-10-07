"""Control panel HTTP API: authentication, request guards, control actions, settings, data.

The service manager and the flatten runner are fakes: nothing here starts a bot or talks to Lighter.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from aiohttp import ClientResponse
from aiohttp.test_utils import TestClient, TestServer

from scalper.control import control_dir, is_paused
from scalper.metrics import utc_date
from scalper.persistence import TRADE_COLUMNS, Journal, utc_iso
from scalper.ui.server import COOKIE, UiConfig, build_app, config_from_env
from scalper.ui.service import ActionResult, ServiceState

from .conftest import BASE_ENV

PASSWORD = "panel-test-password"
SECRET = BASE_ENV["LIGHTER_API_PRIVATE_KEY"]
ENV_HEADER = "# Lighter BTC scalper - test environment\n# second comment line\n"


class FakeService:
    mode = "process"

    def __init__(self) -> None:
        self.state = "stopped"
        self.calls: list[str] = []

    async def status(self) -> ServiceState:
        return ServiceState(self.mode, self.state, f"{self.state} (fake)")

    async def _act(self, name: str) -> ActionResult:
        self.calls.append(name)
        return ActionResult(True, f"{name} requested")

    async def start(self) -> ActionResult:
        return await self._act("start")

    async def stop(self) -> ActionResult:
        return await self._act("stop")

    async def restart(self) -> ActionResult:
        return await self._act("restart")


@dataclass
class Rig:
    client: TestClient[Any, Any]
    cfg: UiConfig
    service: FakeService
    flattens: list[int] = field(default_factory=list)
    csrf: str = ""

    async def post(self, path: str, body: dict[str, Any] | None = None, **kwargs: Any) -> ClientResponse:
        headers = {"X-CSRF-Token": self.csrf, **kwargs.pop("headers", {})}
        return await self.client.post(path, json=body or {}, headers=headers, **kwargs)

    async def login(self) -> None:
        response = await self.client.post("/api/setup", json={"password": PASSWORD})
        assert response.status == 200
        self.csrf = (await response.json())["csrf"]

    def commands(self) -> list[str]:
        return sorted(
            json.loads(p.read_text(encoding="utf-8"))["command"]
            for p in control_dir(self.cfg.data_dir).glob("cmd-*.json")
        )

    def write_status(self, **extra: Any) -> None:
        status = {"ts": utc_iso(), "state": "FLAT", "blocks": {}, "metrics": {"trades_today": 3}, **extra}
        (self.cfg.data_dir / "status.json").write_text(json.dumps(status), encoding="utf-8")


def write_env(path: Path, **overrides: str) -> None:
    values = {**BASE_ENV, **overrides}
    path.write_text(ENV_HEADER + "".join(f"{key}={value}\n" for key, value in values.items()), encoding="utf-8")


@pytest.fixture
async def rig(tmp_path: Path) -> AsyncIterator[Rig]:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    env_file = tmp_path / "bot.env"
    write_env(env_file)
    cfg = UiConfig("127.0.0.1", 8787, data_dir, tmp_path / "logs", env_file, "process")
    service = FakeService()
    flattens: list[int] = []

    async def flatten_runner() -> ActionResult:
        flattens.append(1)
        return ActionResult(True, "The exchange confirms the account is flat.")

    client = TestClient(TestServer(build_app(cfg, service, flatten_runner)))
    await client.start_server()
    try:
        yield Rig(client, cfg, service, flattens)
    finally:
        await client.close()


# ----------------------------------------------------------------------- auth


async def test_first_run_requires_setup_and_blocks_the_api(rig: Rig) -> None:
    session = await (await rig.client.get("/api/session")).json()
    assert session == {**session, "authenticated": False, "setup_required": True, "csrf": None}
    for path in ("/api/overview", "/api/settings", "/api/trades", "/api/logs"):
        assert (await rig.client.get(path)).status == 401
    assert (await rig.client.post("/api/bot/flatten", json={})).status == 401
    assert (await rig.client.post("/api/service/start", json={})).status == 401


async def test_setup_enforces_a_minimum_password_and_runs_only_once(rig: Rig) -> None:
    assert (await rig.client.post("/api/setup", json={"password": "short"})).status == 400
    await rig.login()
    assert (await rig.client.get("/api/overview")).status == 200
    assert (await rig.client.post("/api/setup", json={"password": "another-password"})).status == 409
    stored = (rig.cfg.data_dir / "ui_auth.json").read_text(encoding="utf-8")
    assert PASSWORD not in stored  # only a salted hash is kept


async def test_login_logout_and_session_cookie(rig: Rig) -> None:
    await rig.login()
    cookie = rig.client.session.cookie_jar.filter_cookies(rig.client.make_url("/"))[COOKIE]
    assert cookie.value
    assert (await rig.post("/api/logout")).status == 200
    assert (await rig.client.get("/api/overview")).status == 401
    assert (await rig.client.post("/api/login", json={"password": "wrong-password"})).status == 401
    response = await rig.client.post("/api/login", json={"password": PASSWORD})
    assert response.status == 200
    assert (await rig.client.get("/api/overview")).status == 200


async def test_repeated_wrong_passwords_are_throttled(rig: Rig) -> None:
    await rig.login()
    await rig.post("/api/logout")
    for _ in range(5):
        assert (await rig.client.post("/api/login", json={"password": "guess-guess"})).status == 401
    locked = await rig.client.post("/api/login", json={"password": PASSWORD})
    assert locked.status == 429  # even the right password must wait out the lock


async def test_password_change_logs_every_session_out(rig: Rig) -> None:
    await rig.login()
    assert (await rig.post("/api/password", {"current": "nope-nope-nope", "new": "brand-new-password"})).status == 403
    assert (await rig.post("/api/password", {"current": PASSWORD, "new": "short"})).status == 400
    assert (await rig.post("/api/password", {"current": PASSWORD, "new": "brand-new-password"})).status == 200
    assert (await rig.client.get("/api/overview")).status == 401
    assert (await rig.client.post("/api/login", json={"password": PASSWORD})).status == 401
    assert (await rig.client.post("/api/login", json={"password": "brand-new-password"})).status == 200


# --------------------------------------------------------------------- guards


async def test_state_changing_calls_need_the_csrf_token(rig: Rig) -> None:
    await rig.login()
    assert (await rig.client.post("/api/bot/pause", json={})).status == 403
    assert (await rig.client.post("/api/bot/pause", json={}, headers={"X-CSRF-Token": "wrong"})).status == 403
    assert not is_paused(rig.cfg.data_dir)
    assert (await rig.post("/api/bot/pause")).status == 200


async def test_foreign_host_header_is_refused(rig: Rig) -> None:
    await rig.login()
    response = await rig.client.get("/api/overview", headers={"Host": "attacker.example:8787"})
    assert response.status == 403  # DNS rebinding: a foreign name resolving to 127.0.0.1
    assert (await rig.client.get("/", headers={"Host": "attacker.example"})).status == 403
    assert (await rig.client.get("/api/overview", headers={"Host": "localhost:8787"})).status == 200


async def test_cross_site_requests_are_refused(rig: Rig) -> None:
    await rig.login()
    assert (await rig.post("/api/bot/flatten", headers={"Origin": "http://attacker.example"})).status == 403
    assert (await rig.post("/api/bot/flatten", headers={"Sec-Fetch-Site": "cross-site"})).status == 403
    assert rig.flattens == [] and rig.commands() == []


async def test_security_headers_and_static_files(rig: Rig) -> None:
    index = await rig.client.get("/")
    assert index.status == 200 and "text/html" in index.headers["Content-Type"]
    policy = index.headers["Content-Security-Policy"]
    assert "default-src 'self'" in policy and "unsafe-inline" not in policy and "frame-ancestors 'none'" in policy
    assert index.headers["X-Content-Type-Options"] == "nosniff"
    for name in ("app.js", "app.css", "icon.svg"):
        assert (await rig.client.get(f"/static/{name}")).status == 200
    for name in ("server.py", "..%2Fserver.py", "missing.js", "..%2F..%2Fconfig.py"):
        assert (await rig.client.get(f"/static/{name}")).status == 404


def test_remote_binding_must_be_explicit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "UI_HOST",
        "UI_PORT",
        "UI_ALLOW_REMOTE",
        "UI_ALLOWED_HOSTS",
        "DATA_DIR",
        "LOG_DIR",
        "SCALPER_ENV_FILE",
    ):
        monkeypatch.delenv(name, raising=False)
    default = config_from_env(env_file=tmp_path / "missing.env")
    assert default.host == "127.0.0.1" and default.port == 8787 and default.is_loopback
    with pytest.raises(SystemExit):
        config_from_env(host="0.0.0.0", env_file=tmp_path / "missing.env")
    monkeypatch.setenv("UI_ALLOW_REMOTE", "true")
    monkeypatch.setenv("UI_ALLOWED_HOSTS", "panel.example, 10.0.0.5")
    remote = config_from_env(host="0.0.0.0", port=9000, env_file=tmp_path / "missing.env")
    assert remote.port == 9000 and remote.allowed_hosts == frozenset({"panel.example", "10.0.0.5"})


# ------------------------------------------------------------------- overview


async def test_overview_distinguishes_live_from_stale_status(rig: Rig) -> None:
    await rig.login()
    empty = await (await rig.client.get("/api/overview")).json()
    assert empty["bot_live"] is False and empty["status"] is None and empty["service"]["state"] == "stopped"
    assert empty["config"] == {"ready": True, "blockers": []}

    rig.write_status()
    rig.service.state = "running"
    live = await (await rig.client.get("/api/overview")).json()
    assert live["bot_live"] is True and live["status"]["state"] == "FLAT"

    stale = time.time() - 120
    os.utime(rig.cfg.data_dir / "status.json", (stale, stale))
    old = await (await rig.client.get("/api/overview")).json()
    assert old["bot_live"] is False and old["status"] is None  # never present an old snapshot as current
    assert old["last_status"]["metrics"] == {"trades_today": 3}


async def test_overview_reports_what_blocks_starting(rig: Rig) -> None:
    write_env(rig.cfg.env_file, LIVE_TRADING="", LIGHTER_API_PRIVATE_KEY="")
    await rig.login()
    overview = await (await rig.client.get("/api/overview")).json()
    assert overview["config"]["ready"] is False
    text = " ".join(overview["config"]["blockers"])
    assert "LIVE_TRADING" in text and "LIGHTER_API_PRIVATE_KEY" in text


# -------------------------------------------------------------------- control


async def test_start_is_refused_until_the_configuration_is_complete(rig: Rig) -> None:
    write_env(rig.cfg.env_file, I_UNDERSTAND_THIS_USES_REAL_FUNDS="")
    await rig.login()
    response = await rig.post("/api/service/start")
    assert response.status == 409 and "I_UNDERSTAND" in " ".join((await response.json())["blockers"])
    assert rig.service.calls == []
    write_env(rig.cfg.env_file)
    os.utime(rig.cfg.env_file, (time.time() + 5, time.time() + 5))
    assert (await rig.post("/api/service/start")).status == 200
    assert rig.service.calls == ["start"]


async def test_stop_and_restart_go_to_the_service_manager(rig: Rig) -> None:
    await rig.login()
    assert (await rig.post("/api/service/stop")).status == 200
    assert (await rig.post("/api/service/restart")).status == 200
    assert (await rig.post("/api/service/destroy")).status == 404
    assert rig.service.calls == ["stop", "restart"]


async def test_pause_and_resume_while_running_queue_commands(rig: Rig) -> None:
    await rig.login()
    rig.service.state = "running"
    assert (await rig.post("/api/bot/pause")).status == 200
    assert is_paused(rig.cfg.data_dir) and rig.commands() == ["pause"]
    assert (await rig.post("/api/bot/resume")).status == 200
    assert not is_paused(rig.cfg.data_dir) and rig.commands() == ["pause", "resume"]


async def test_pause_while_stopped_only_sets_the_flag(rig: Rig) -> None:
    await rig.login()
    assert (await rig.post("/api/bot/pause")).status == 200
    assert is_paused(rig.cfg.data_dir)  # the bot will come up paused
    assert rig.commands() == []  # no command is left behind to fire at the next start


async def test_flatten_while_running_pauses_and_queues_the_command(rig: Rig) -> None:
    await rig.login()
    rig.service.state = "running"
    response = await rig.post("/api/bot/flatten")
    assert response.status == 200
    assert rig.commands() == ["flatten"] and is_paused(rig.cfg.data_dir) and rig.flattens == []


async def test_flatten_while_stopped_runs_the_flatten_procedure_directly(rig: Rig) -> None:
    await rig.login()
    response = await rig.post("/api/bot/flatten")
    payload = await response.json()
    assert response.status == 200 and payload["ok"] and "flat" in payload["message"]
    assert rig.flattens == [1] and rig.commands() == [] and is_paused(rig.cfg.data_dir)


async def test_flatten_while_stopping_does_not_start_a_second_flatten(rig: Rig) -> None:
    await rig.login()
    rig.service.state = "stopping"
    assert (await rig.post("/api/bot/flatten")).status == 200
    assert rig.flattens == [] and rig.commands() == []


async def test_unknown_bot_action_is_not_found(rig: Rig) -> None:
    await rig.login()
    assert (await rig.post("/api/bot/double")).status == 404


# ------------------------------------------------------------------- settings


async def test_the_private_key_never_leaves_the_server(rig: Rig) -> None:
    await rig.login()
    response = await rig.client.get("/api/settings")
    text = await response.text()
    assert SECRET not in text
    payload = json.loads(text)
    assert payload["values"]["LIGHTER_API_PRIVATE_KEY"] is True  # only "a key is stored"
    assert payload["values"]["LEVERAGE"] == "25" and payload["values"]["LIVE_TRADING"] is True
    assert payload["check"]["ready"] is True
    for path in ("/api/overview", "/api/logs", "/api/events"):
        assert SECRET not in await (await rig.client.get(path)).text()


async def test_saving_updates_values_in_place_and_keeps_the_rest(rig: Rig) -> None:
    await rig.login()
    rig.service.state = "running"
    response = await rig.post("/api/settings", {"values": {"LEVERAGE": "20", "MAX_HOLD_MS": "3000"}})
    payload = await response.json()
    assert response.status == 200 and payload["saved"] == ["LEVERAGE", "MAX_HOLD_MS"]
    assert payload["restart_needed"] is True and payload["values"]["LEVERAGE"] == "20"
    assert SECRET not in json.dumps(payload)
    text = rig.cfg.env_file.read_text(encoding="utf-8")
    assert text.startswith(ENV_HEADER)  # comments and order survive
    assert "LEVERAGE=20\n" in text and "MAX_HOLD_MS=3000\n" in text and "LEVERAGE=25" not in text
    assert f"LIGHTER_API_PRIVATE_KEY={SECRET}\n" in text  # untouched


async def test_an_empty_secret_field_keeps_the_stored_key(rig: Rig) -> None:
    await rig.login()
    assert (await rig.post("/api/settings", {"values": {"LIGHTER_API_PRIVATE_KEY": ""}})).status == 200
    assert f"LIGHTER_API_PRIVATE_KEY={SECRET}\n" in rig.cfg.env_file.read_text(encoding="utf-8")
    new_key = "cd" * 40
    assert (await rig.post("/api/settings", {"values": {"LIGHTER_API_PRIVATE_KEY": new_key}})).status == 200
    assert f"LIGHTER_API_PRIVATE_KEY={new_key}\n" in rig.cfg.env_file.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "values",
    [
        {"LEVERAGE": "abc"},
        {"LEVERAGE": "0"},
        {"MAX_NORMAL_EXIT_SLIPPAGE_BPS": "50"},  # above the emergency band of 30
        {"MIN_PROFIT_USD": "0", "MIN_PROFIT_BPS": "0"},
        {"MAX_ADVERSE_MOVE_BPS": "0", "MAX_LOSS_USD": "0"},
        {"PROFIT_EXIT_MODE": "trailing"},
        {"LIGHTER_API_PRIVATE_KEY": "not-a-key"},
        {"LEVERAGE": "25\nLIVE_TRADING=true"},
        {"LEVERAGE": "25 # comment"},
        {"LIGHTER_BASE_URL": "https://example.com"},
        {"PATH": "/tmp"},
    ],
)
async def test_invalid_settings_are_rejected_and_nothing_is_written(rig: Rig, values: dict[str, str]) -> None:
    await rig.login()
    before = rig.cfg.env_file.read_bytes()
    response = await rig.post("/api/settings", {"values": values})
    assert response.status == 400 and (await response.json())["errors"]
    assert rig.cfg.env_file.read_bytes() == before


async def test_a_gate_can_be_switched_off_to_disarm_the_bot(rig: Rig) -> None:
    await rig.login()
    response = await rig.post("/api/settings", {"values": {"LIVE_TRADING": False}})
    payload = await response.json()
    assert response.status == 200 and payload["check"]["ready"] is False
    assert "LIVE_TRADING=\n" in rig.cfg.env_file.read_text(encoding="utf-8")
    assert (await rig.post("/api/service/start")).status == 409  # and it really cannot be started now
    assert (await rig.post("/api/settings", {"values": {"LIVE_TRADING": True}})).status == 200
    assert "LIVE_TRADING=true\n" in rig.cfg.env_file.read_text(encoding="utf-8")


async def test_settings_can_be_built_up_from_an_empty_file(rig: Rig) -> None:
    rig.cfg.env_file.unlink()
    await rig.login()
    first = await (await rig.client.get("/api/settings")).json()
    assert first["check"]["ready"] is False and first["values"]["LIGHTER_API_PRIVATE_KEY"] is False
    partial = {
        key: BASE_ENV[key]
        for key in (
            "LEVERAGE",
            "MARGIN_PER_TRADE_USD",
            "MIN_PROFIT_USD",
            "MAX_HOLD_MS",
            "MAX_ENTRY_SLIPPAGE_BPS",
            "MAX_NORMAL_EXIT_SLIPPAGE_BPS",
            "MAX_EMERGENCY_EXIT_SLIPPAGE_BPS",
            "MAX_ADVERSE_MOVE_BPS",
            "MAX_LOSS_USD",
            "MAX_SPREAD_BPS",
            "MARKET_DATA_STALE_MS",
            "ENTRY_SCORE_THRESHOLD",
        )
    }
    saved = await rig.post("/api/settings", {"values": partial})
    assert saved.status == 200  # credentials and gates may come later
    assert (await saved.json())["check"]["ready"] is False
    rest = {
        "LIGHTER_ACCOUNT_INDEX": "7",
        "LIGHTER_API_KEY_INDEX": "4",
        "LIGHTER_API_PRIVATE_KEY": SECRET,
        "LIVE_TRADING": True,
        "I_UNDERSTAND_THIS_USES_REAL_FUNDS": True,
    }
    done = await (await rig.post("/api/settings", {"values": rest})).json()
    assert done["check"]["ready"] is True


# ----------------------------------------------------------------------- data


def record_trades(data_dir: Path, nets: list[float]) -> None:
    journal = Journal(data_dir)
    journal.start()
    base = time.time_ns()
    for index, net in enumerate(nets):
        row = dict.fromkeys(TRADE_COLUMNS)
        row.update(
            trade_id=f"t{index}",
            side="LONG" if index % 2 == 0 else "SHORT",
            size="0.00298",
            avg_entry=83700.0,
            avg_exit=83701.0,
            exit_fill_at=utc_iso(base + index * 1_000_000_000),
            holding_ms=1500.0,
            gross_pnl_usd=net,
            fees_usd=0.0,
            realized_pnl_usd=net,
            exit_reason="PROFIT" if net > 0 else "MAX_HOLD",
            adopted=0,
            latency={"send_to_fill_ms": 320.0},
            trade_date=utc_date(),
        )
        journal.record_trade(row)
    journal.record_event("RECOVERY_START", {"reason": "MARKET_DATA_STALE"})
    journal.stop()


async def test_trades_curve_daily_and_events_come_from_the_journal(rig: Rig) -> None:
    record_trades(rig.cfg.data_dir, [0.02, -0.01, 0.03])
    await rig.login()
    trades = (await (await rig.client.get("/api/trades")).json())["trades"]
    assert [t["trade_id"] for t in trades] == ["t2", "t1", "t0"]  # newest first
    assert trades[0]["realized_pnl_usd"] == 0.03 and trades[0]["send_to_fill_ms"] == 320.0
    points = (await (await rig.client.get("/api/pnl")).json())["points"]
    assert [p["cum"] for p in points] == [0.02, 0.01, 0.04]
    day = (await (await rig.client.get("/api/daily")).json())["days"][0]
    assert (day["trades"], day["wins"], day["losses"]) == (3, 2, 1) and day["net_pnl_usd"] == pytest.approx(0.04)
    assert day["best_usd"] == 0.03 and day["worst_usd"] == -0.01 and day["win_pct"] == 66.7
    assert (await (await rig.client.get("/api/trades?date=1999-01-01")).json())["trades"] == []
    events = (await (await rig.client.get("/api/events")).json())["events"]
    assert events[0]["kind"] == "RECOVERY_START" and events[0]["summary"] == "MARKET_DATA_STALE"


async def test_data_endpoints_are_empty_before_the_bot_has_ever_run(rig: Rig) -> None:
    await rig.login()
    assert (await (await rig.client.get("/api/trades")).json())["trades"] == []
    assert (await (await rig.client.get("/api/pnl")).json())["points"] == []
    assert (await (await rig.client.get("/api/daily")).json())["days"] == []
    assert (await (await rig.client.get("/api/logs")).json())["lines"] == []


async def test_log_tail(rig: Rig) -> None:
    rig.cfg.log_dir.mkdir()
    lines = [f"2026-10-07T10:15:{i % 60:02d}.000Z LINE {i}" for i in range(400)]
    (rig.cfg.log_dir / "scalper.log").write_text("\n".join(lines) + "\n", encoding="utf-8")
    await rig.login()
    tail = (await (await rig.client.get("/api/logs?lines=50")).json())["lines"]
    assert len(tail) == 50 and tail[-1].endswith("LINE 399") and tail[0].endswith("LINE 350")
