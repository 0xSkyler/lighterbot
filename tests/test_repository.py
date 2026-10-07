"""Repository hygiene: the shipped example config, secrets, and excluded features."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from scalper.config import load_config, parse_env_file
from scalper.errors import ConfigError

ROOT = Path(__file__).resolve().parent.parent
SOURCES = sorted((ROOT / "src" / "scalper").glob("*.py"))


def test_env_example_refuses_to_start_as_shipped() -> None:
    with pytest.raises(ConfigError) as error:
        load_config(env={}, env_file=ROOT / ".env.example")
    text = str(error.value)
    assert "LIVE_TRADING" in text and "I_UNDERSTAND_THIS_USES_REAL_FUNDS" in text
    assert "LIGHTER_API_PRIVATE_KEY" in text and "LIGHTER_ACCOUNT_INDEX" in text


def test_env_example_is_valid_once_credentials_and_gates_are_filled() -> None:
    env = {
        "LIVE_TRADING": "true",
        "I_UNDERSTAND_THIS_USES_REAL_FUNDS": "YES",
        "LIGHTER_ACCOUNT_INDEX": "1",
        "LIGHTER_API_KEY_INDEX": "4",
        "LIGHTER_API_PRIVATE_KEY": "cd" * 40,
    }
    config = load_config(env=env, env_file=ROOT / ".env.example")
    assert config.leverage == 25 and config.notional_usd == 250
    assert config.max_hold_ms == 5000 and config.existing_position_action == "manage"


def test_env_example_contains_no_credentials() -> None:
    values = parse_env_file(ROOT / ".env.example")
    for key in ("LIGHTER_API_PRIVATE_KEY", "LIGHTER_ACCOUNT_INDEX", "LIGHTER_API_KEY_INDEX", "LIVE_TRADING"):
        assert values[key] == ""


def test_gitignore_excludes_secrets_and_runtime_state() -> None:
    ignored = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    for pattern in (".env", "*.env", "data/*", "logs/*", "*.db"):
        assert pattern in ignored
    assert "!.env.example" in ignored


def test_no_secret_looking_literals_in_the_source_tree() -> None:
    long_hex = re.compile(r"\b(?:0x)?[0-9a-fA-F]{64,}\b")
    for path in [*SOURCES, *(ROOT / "deploy").iterdir(), ROOT / "README.md", ROOT / ".env.example"]:
        assert not long_hex.search(path.read_text(encoding="utf-8")), path.name


def test_only_mainnet_endpoints_and_no_excluded_features() -> None:
    text = "\n".join(path.read_text(encoding="utf-8") for path in SOURCES).lower()
    assert "testnet.zklighter" not in text  # no testnet workflow
    for forbidden in ("import pandas", "import numpy", "openai", "anthropic", "telegram", "selenium", "playwright"):
        assert forbidden not in text, forbidden
    for word in ("paper_trading", "paper_mode", "shadow_mode", "dry_run", "backtest"):
        assert word not in text, word


def test_no_placeholders_in_execution_code() -> None:
    for path in SOURCES:
        text = path.read_text(encoding="utf-8")
        assert "TODO" not in text and "FIXME" not in text and "NotImplementedError()" not in text, path.name


def test_modules_stay_focused() -> None:
    for path in SOURCES:
        lines = len(path.read_text(encoding="utf-8").splitlines())
        assert lines < 1000, f"{path.name} has {lines} lines"


def test_deployment_files_exist() -> None:
    for relative in (
        "deploy/install.sh",
        "deploy/update.sh",
        "deploy/lighter-scalper.service",
        "README.md",
        "LICENSE",
        "pyproject.toml",
        "requirements.txt",
        ".env.example",
    ):
        assert (ROOT / relative).is_file(), relative
    unit = (ROOT / "deploy" / "lighter-scalper.service").read_text(encoding="utf-8")
    assert "Restart=always" in unit and "RestartPreventExitStatus=78" in unit
    assert "After=network-online.target" in unit and "EnvironmentFile=" in unit
