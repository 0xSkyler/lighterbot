"""Shared fixtures. No test in this suite talks to the network or submits an order."""

from __future__ import annotations

from typing import Any

import pytest

from scalper.config import Config, load_config
from scalper.precision import MarketMeta
from scalper.risk import Limits

# Mirrors Lighter mainnet BTC perp metadata (orderBookDetails, market_id 1).
BTC = MarketMeta(
    symbol="BTC",
    market_id=1,
    price_decimals=1,
    size_decimals=5,
    min_base=7,  # 0.00007 BTC
    min_quote_q=10_000_000,  # 10 USD
    min_imf=200,  # 50x
    default_imf=500,
    maintenance_imf=120,
    status="active",
)

BASE_ENV: dict[str, str] = {
    "LIVE_TRADING": "true",
    "I_UNDERSTAND_THIS_USES_REAL_FUNDS": "YES",
    "LIGHTER_ACCOUNT_INDEX": "12345",
    "LIGHTER_API_KEY_INDEX": "4",
    "LIGHTER_API_PRIVATE_KEY": "ab" * 40,
    "LEVERAGE": "25",
    "POSITION_MODE": "fixed_margin",
    "MARGIN_PER_TRADE_USD": "10",
    "MIN_PROFIT_USD": "0.01",
    "MAX_ENTRY_SLIPPAGE_BPS": "1",
    "MAX_NORMAL_EXIT_SLIPPAGE_BPS": "2",
    "MAX_EMERGENCY_EXIT_SLIPPAGE_BPS": "30",
    "MAX_ADVERSE_MOVE_BPS": "10",
    "MAX_LOSS_USD": "0.5",
    "MAX_HOLD_MS": "5000",
    "MAX_SPREAD_BPS": "1.0",
    "MARKET_DATA_STALE_MS": "1000",
    "ENTRY_SCORE_THRESHOLD": "0.6",
    "ENTRY_MODE": "taker",  # most tests exercise the IOC path; maker tests override this
}


def make_config(**overrides: Any) -> Config:
    env = dict(BASE_ENV)
    env.update({key: str(value) for key, value in overrides.items()})
    return load_config(env=env)


@pytest.fixture
def meta() -> MarketMeta:
    return BTC


@pytest.fixture
def cfg() -> Config:
    return make_config()


@pytest.fixture
def limits(cfg: Config, meta: MarketMeta) -> Limits:
    return Limits.from_config(cfg, meta)


def px(price: float) -> int:
    """USD price -> BTC price units (1 decimal)."""
    return round(price * 10)


def sz(size: float) -> int:
    """BTC size -> size units (5 decimals)."""
    return round(size * 100_000)
