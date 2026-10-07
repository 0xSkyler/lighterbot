"""Control panel building blocks: env-file editing, settings schema, service managers, static assets."""

from __future__ import annotations

import json
import os
import re
import stat
import sys
import time
from pathlib import Path

import pytest

from scalper.control import control_dir
from scalper.ui.auth import LoginThrottle, PasswordStore, Sessions
from scalper.ui.envfile import read_env, update_env_file
from scalper.ui.schema import FIELDS, GROUPS, SECRET_KEYS, check_settings, normalise, public_values, schema_json
from scalper.ui.service import ProcessManager, SystemdManager, bot_command, bot_environment

from .conftest import BASE_ENV

ROOT = Path(__file__).resolve().parent.parent
STATIC = ROOT / "src" / "scalper" / "ui" / "static"


# ------------------------------------------------------------------- env file


def test_update_replaces_in_place_and_preserves_layout(tmp_path: Path) -> None:
    path = tmp_path / "bot.env"
    path.write_text(
        "# header\nLEVERAGE=25\n\n# sizing\nexport MAX_HOLD_MS=5000\n#LEVERAGE=99\nOTHER=keep\n", encoding="utf-8"
    )
    update_env_file(path, {"LEVERAGE": "10", "MAX_HOLD_MS": "3000", "NEW_KEY": "1"})
    assert path.read_text(encoding="utf-8") == (
        "# header\nLEVERAGE=10\n\n# sizing\nMAX_HOLD_MS=3000\n#LEVERAGE=99\nOTHER=keep\n\n"
        "# Added by the control panel\nNEW_KEY=1\n"
    )
    assert read_env(path)["LEVERAGE"] == "10"


def test_update_collapses_duplicate_definitions(tmp_path: Path) -> None:
    path = tmp_path / "bot.env"
    path.write_text("LEVERAGE=25\nLEVERAGE=50\n", encoding="utf-8")
    update_env_file(path, {"LEVERAGE": "10"})
    assert path.read_text(encoding="utf-8") == "LEVERAGE=10\n"  # a later duplicate can no longer win


@pytest.mark.parametrize("value", ["1 2", "1#x", "a\nb", '"1"', "$(id)", "a;b", "a\\b"])
def test_update_refuses_values_that_could_change_meaning(tmp_path: Path, value: str) -> None:
    path = tmp_path / "bot.env"
    path.write_text("LEVERAGE=25\n", encoding="utf-8")
    with pytest.raises(ValueError):
        update_env_file(path, {"LEVERAGE": value})
    with pytest.raises(ValueError):
        update_env_file(path, {"lower case": "1"})
    assert path.read_text(encoding="utf-8") == "LEVERAGE=25\n"


def test_update_creates_a_private_file_and_keeps_existing_permissions(tmp_path: Path) -> None:
    path = tmp_path / "new" / "bot.env"
    update_env_file(path, {"LEVERAGE": "10"})
    assert read_env(path) == {"LEVERAGE": "10"}
    assert not list(path.parent.glob("*.tmp"))
    if os.name != "nt":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        os.chmod(path, 0o640)
        update_env_file(path, {"LEVERAGE": "20"})
        assert stat.S_IMODE(path.stat().st_mode) == 0o640


# --------------------------------------------------------------------- schema


def test_every_setting_in_the_panel_is_a_real_configuration_key() -> None:
    config_source = (ROOT / "src" / "scalper" / "config.py").read_text(encoding="utf-8")
    for key in FIELDS:
        assert f'"{key}"' in config_source, key
    assert len(FIELDS) == sum(len(group.fields) for group in GROUPS)  # no key appears twice
    assert set(SECRET_KEYS) <= set(FIELDS)
    json.dumps(schema_json())  # serialisable for the front end


def test_check_separates_wrong_values_from_missing_prerequisites() -> None:
    complete = check_settings(BASE_ENV)
    assert complete.ready and complete.value_errors == [] and complete.start_blockers == []

    disarmed = check_settings({**BASE_ENV, "LIVE_TRADING": "", "LIGHTER_API_PRIVATE_KEY": ""})
    assert not disarmed.ready and disarmed.value_errors == []
    assert len(disarmed.start_blockers) == 2

    wrong = check_settings({**BASE_ENV, "LEVERAGE": "abc"})
    assert not wrong.ready and any("LEVERAGE" in error for error in wrong.value_errors)

    bad_key = check_settings({**BASE_ENV, "LIGHTER_API_PRIVATE_KEY": "zz"})
    assert any("LIGHTER_API_PRIVATE_KEY" in error for error in bad_key.value_errors)


def test_public_values_never_contain_the_secret() -> None:
    values = public_values({**BASE_ENV, "EXIT_ON_SIGNAL_REVERSAL": "true"})
    assert values["LIGHTER_API_PRIVATE_KEY"] is True
    assert BASE_ENV["LIGHTER_API_PRIVATE_KEY"] not in json.dumps(values)
    assert values["LIVE_TRADING"] is True and values["I_UNDERSTAND_THIS_USES_REAL_FUNDS"] is True
    assert values["EXIT_ON_SIGNAL_REVERSAL"] is True
    assert values["USE_TICKER_STREAM"] is True  # absent in the file: shows the default that is in force
    assert values["CANCEL_FOREIGN_ORDERS"] is False
    assert public_values({})["LIGHTER_API_PRIVATE_KEY"] is False


def test_normalise_gates_booleans_and_text() -> None:
    assert normalise(FIELDS["LIVE_TRADING"], True) == "true"
    assert normalise(FIELDS["LIVE_TRADING"], False) == ""
    assert normalise(FIELDS["I_UNDERSTAND_THIS_USES_REAL_FUNDS"], True) == "YES"
    assert normalise(FIELDS["CANCEL_FOREIGN_ORDERS"], True) == "true"
    assert normalise(FIELDS["CANCEL_FOREIGN_ORDERS"], False) == "false"
    assert normalise(FIELDS["LEVERAGE"], " 25 ") == "25"
    assert normalise(FIELDS["TAKER_FEE_BPS"], None) == ""


# ----------------------------------------------------------------------- auth


def test_password_store_and_sessions(tmp_path: Path) -> None:
    store = PasswordStore(tmp_path)
    assert not store.is_set() and not store.verify("anything")
    with pytest.raises(ValueError):
        store.set("short")
    store.set("correct horse battery")
    assert store.is_set() and store.verify("correct horse battery") and not store.verify("correct horse")
    sessions = Sessions(ttl_s=0.05)
    session = sessions.create()
    assert sessions.get(session.token) is session and sessions.get("other") is None and sessions.get(None) is None
    time.sleep(0.08)
    assert sessions.get(session.token) is None  # expired


def test_login_throttle_locks_and_recovers() -> None:
    throttle = LoginThrottle(max_failures=3, lockout_s=0.1)
    for _ in range(2):
        throttle.failed()
    assert throttle.locked_for() == 0
    throttle.failed()
    assert throttle.locked_for() > 0
    time.sleep(0.12)
    assert throttle.locked_for() == 0
    throttle.succeeded()
    assert throttle.locked_for() == 0


# -------------------------------------------------------------------- service


@pytest.mark.parametrize(
    ("props", "state", "fragment"),
    [
        (
            {"LoadState": "loaded", "ActiveState": "active", "SubState": "running", "MainPID": "42"},
            "running",
            "running",
        ),
        ({"LoadState": "loaded", "ActiveState": "activating", "SubState": "start"}, "starting", "reconciling"),
        ({"LoadState": "loaded", "ActiveState": "activating", "SubState": "auto-restart"}, "starting", "retrying"),
        ({"LoadState": "loaded", "ActiveState": "deactivating", "SubState": "stop-sigterm"}, "stopping", "flatten"),
        ({"LoadState": "loaded", "ActiveState": "inactive", "SubState": "dead"}, "stopped", "stopped"),
        ({"LoadState": "loaded", "ActiveState": "failed", "ExecMainStatus": "78"}, "failed", "configuration"),
        (
            {"LoadState": "loaded", "ActiveState": "failed", "ExecMainStatus": "1", "Result": "exit-code"},
            "failed",
            "exit-code",
        ),
        ({"LoadState": "not-found", "ActiveState": "inactive"}, "stopped", "not installed"),
    ],
)
def test_systemd_states_are_mapped(props: dict[str, str], state: str, fragment: str) -> None:
    result = SystemdManager()._interpret(props)
    assert result.state == state and fragment in result.detail and result.mode == "systemd"
    assert result.pid == (42 if props.get("MainPID") == "42" else None)


def test_bot_command_uses_the_panels_env_file_and_directories(tmp_path: Path) -> None:
    env_file = tmp_path / "bot.env"
    assert bot_command(env_file, "run") == [sys.executable, "-m", "scalper.main", "run"]
    env_file.write_text("LEVERAGE=25\n", encoding="utf-8")
    assert bot_command(env_file, "flatten") == [
        sys.executable,
        "-m",
        "scalper.main",
        "--env-file",
        str(env_file),
        "flatten",
    ]
    environment = bot_environment(tmp_path / "data", tmp_path / "logs")
    assert environment["DATA_DIR"] == str(tmp_path / "data") and environment["LOG_DIR"] == str(tmp_path / "logs")


async def test_process_manager_follows_the_status_heartbeat(tmp_path: Path) -> None:
    manager = ProcessManager(tmp_path, tmp_path / "logs", tmp_path / "bot.env")
    assert (await manager.status()).state == "stopped"
    assert not (await manager.stop()).ok  # nothing to stop

    (tmp_path / "status.json").write_text("{}", encoding="utf-8")
    assert (await manager.status()).state == "running"
    assert not (await manager.start()).ok  # never a second instance

    stopped = await manager.stop()
    assert stopped.ok  # graceful: a stop command, never a kill
    command = json.loads(next(control_dir(tmp_path).glob("cmd-*.json")).read_text(encoding="utf-8"))
    assert command["command"] == "stop"
    assert (await manager.status()).state == "stopping"

    old = time.time() - 60
    os.utime(tmp_path / "status.json", (old, old))
    assert (await manager.status()).state == "stopped"


async def test_process_manager_reports_startup_and_failure(tmp_path: Path) -> None:
    quick_exit = [sys.executable, "-c", "import sys; sys.exit(78)"]
    manager = ProcessManager(tmp_path, tmp_path / "logs", tmp_path / "bot.env", command=quick_exit)
    assert (await manager.start()).ok
    assert manager._process is not None
    manager._process.wait(30)
    failed = await manager.status()
    assert failed.state == "failed" and "configuration or credentials" in failed.detail
    assert (tmp_path / "logs" / "bot-console.log").exists()

    sleeper = [sys.executable, "-c", "import time; time.sleep(30)"]
    manager = ProcessManager(tmp_path, tmp_path / "logs", tmp_path / "bot.env", command=sleeper)
    assert (await manager.start()).ok
    try:
        assert (await manager.status()).state == "starting"  # alive, but no status heartbeat yet
    finally:
        assert manager._process is not None
        manager._process.terminate()  # test cleanup of the dummy process only
        manager._process.wait(30)


# --------------------------------------------------------------------- static


def test_static_assets_are_self_contained_and_csp_clean() -> None:
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    assert re.findall(r'(?:src|href)="([^"]+)"', html) == [
        "/static/icon.svg",
        "/static/app.css",
        "/static/app.js",
    ]  # nothing is loaded from another origin
    assert "<style" not in html and " style=" not in html  # the CSP forbids inline styles
    assert re.search(r"<script(?![^>]*\bsrc=)", html) is None  # and inline scripts
    assert not re.search(r"\son[a-z]+=", html)  # and inline event handlers
    for name in ("app.js", "app.css"):
        assert "http://" not in (STATIC / name).read_text(encoding="utf-8").replace("http://www.w3.org/2000/svg", "")


def test_every_element_the_script_uses_exists_in_the_page() -> None:
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    script = (STATIC / "app.js").read_text(encoding="utf-8")
    page_ids = set(re.findall(r'\bid="([^"]+)"', html))
    used = set(re.findall(r"\$\('([A-Za-z0-9_-]+)'\)", script))
    assert used - page_ids == set()
    assert len(page_ids) == len(re.findall(r'\bid="([^"]+)"', html))  # ids are unique
    for tab in ("overview", "trades", "settings", "logs"):
        assert f'id="tab-{tab}"' in html and f'data-tab="{tab}"' in html


def test_the_script_never_builds_markup_from_data() -> None:
    script = (STATIC / "app.js").read_text(encoding="utf-8")
    for forbidden in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("):
        assert forbidden not in script, forbidden


def test_static_files_are_packaged() -> None:
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert '"scalper.ui" = ["static/*"]' in pyproject
    assert {path.name for path in STATIC.iterdir()} == {"index.html", "app.css", "app.js", "icon.svg"}
