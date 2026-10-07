"""Entry gates, hard protections, rate-limit reserve, stale data and configuration validation."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from scalper.config import load_config, parse_env_file
from scalper.errors import ConfigError, ErrorClass, classify_api_error, is_reduce_only_rejection
from scalper.pnl import LONG, SHORT
from scalper.precision import MarketMeta
from scalper.rate_limits import RESERVED_READS, RESERVED_TX, RateLimiter, limits_for_tier
from scalper.risk import Limits, hard_loss_reason, hold_expired, is_stale, plan_entry, validate_leverage

from .conftest import BASE_ENV, BTC, make_config, px, sz

BIDS = [(px(83691.1), sz(0.05)), (px(83690.0), sz(0.10))]
ASKS = [(px(83693.9), sz(0.05)), (px(83695.0), sz(0.10))]
BALANCE = 100_000_000  # 100 USD


def plan(side: int = LONG, **kw: object) -> tuple[object, str]:
    params: dict[str, object] = {
        "limits": Limits.from_config(make_config(), BTC),
        "meta": BTC,
        "available_balance_q": BALANCE,
        "volatility_mbps": 0,
        "taker_fee_tick": 0,
    }
    bids = kw.pop("bids", BIDS)
    asks = kw.pop("asks", ASKS)
    params.update(kw)
    return plan_entry(side, bids, asks, **params)  # type: ignore[arg-type]


# ----------------------------------------------------------------- leverage


def test_leverage_is_validated_against_the_market() -> None:
    assert validate_leverage(25, BTC) == 400
    assert validate_leverage(50, BTC) == 200
    assert validate_leverage(30, BTC) == 333  # same rounding as the official SDK
    with pytest.raises(ConfigError):
        validate_leverage(51, BTC)  # above the 50x the market currently allows
    with pytest.raises(ConfigError):
        validate_leverage(0, BTC)


def test_limits_from_config(limits: Limits) -> None:
    assert limits.notional_q == 250_000_000  # 10 USD margin * 25x
    assert limits.imf == 400
    assert limits.min_profit_q == 10_000
    assert limits.max_loss_q == 500_000
    assert limits.max_adverse_mbps == 10_000
    assert limits.max_hold_ns == 5_000_000_000
    assert limits.min_profit_for(250_000_000) == 10_000


def test_min_profit_uses_the_larger_of_usd_and_bps() -> None:
    limits = Limits.from_config(make_config(MIN_PROFIT_USD="0.01", MIN_PROFIT_BPS="1"), BTC)
    assert limits.min_profit_for(250_000_000) == 25_000  # 1 bps of 250 USD beats 0.01 USD


def test_fixed_notional_mode() -> None:
    cfg = make_config(POSITION_MODE="fixed_notional", NOTIONAL_PER_TRADE_USD="400")
    assert Limits.from_config(cfg, BTC).notional_q == 400_000_000


# -------------------------------------------------------------- entry gates


def test_entry_plan_long_and_short() -> None:
    long_plan, reason = plan(LONG)
    assert reason == "" and long_plan is not None
    assert long_plan.size == 250_000_000 // px(83693.9)  # type: ignore[attr-defined]
    assert long_plan.limit_price >= px(83693.9)  # type: ignore[attr-defined]
    short_plan, reason = plan(SHORT)
    assert reason == "" and short_plan is not None
    assert short_plan.limit_price <= px(83691.1)  # type: ignore[attr-defined]
    assert short_plan.estimate.best_price == px(83691.1)  # type: ignore[attr-defined]


def test_entry_skipped_when_spread_is_too_wide() -> None:
    assert plan(asks=[(px(83720.0), sz(0.05))]) == (None, "SPREAD")


def test_entry_skipped_on_pathological_volatility() -> None:
    assert plan(volatility_mbps=20_000) == (None, "VOLATILITY")


def test_entry_skipped_when_depth_would_exceed_slippage() -> None:
    thin = [(px(83693.9), sz(0.0001)), (px(83750.0), sz(1.0))]
    assert plan(asks=thin) == (None, "DEPTH")


def test_entry_skipped_when_balance_unknown_or_insufficient() -> None:
    assert plan(available_balance_q=None) == (None, "BALANCE_UNKNOWN")
    assert plan(available_balance_q=5_000_000) == (None, "BALANCE")  # needs ~10 USD margin


def test_entry_skipped_when_size_below_exchange_minimum() -> None:
    cfg = make_config(MARGIN_PER_TRADE_USD="0.2")  # 5 USD notional, below Lighter's 10 USD minimum
    assert plan(limits=Limits.from_config(cfg, BTC)) == (None, "SIZE_BELOW_MIN")


def test_entry_skipped_without_a_book() -> None:
    assert plan(bids=[]) == (None, "NO_BOOK")


# ---------------------------------------------------------- hard protection


def test_hard_loss_on_adverse_move(limits: Limits) -> None:
    size = sz(0.003)
    cost = size * px(83700.0)
    assert hard_loss_reason(LONG, size, cost, px(83699.0), None, limits) is None
    assert hard_loss_reason(LONG, size, cost, px(83616.3), None, limits) == "MAX_ADVERSE_MOVE"
    assert hard_loss_reason(SHORT, size, cost, px(83783.7), None, limits) == "MAX_ADVERSE_MOVE"
    assert hard_loss_reason(SHORT, size, cost, px(83600.0), None, limits) is None  # short in profit


def test_hard_loss_on_usd_loss() -> None:
    limits = Limits.from_config(make_config(MAX_ADVERSE_MOVE_BPS="0", MAX_LOSS_USD="0.05"), BTC)
    size = sz(0.003)
    cost = size * px(83700.0)
    assert hard_loss_reason(LONG, size, cost, px(83690.0), -49_999, limits) is None
    assert hard_loss_reason(LONG, size, cost, px(83690.0), -50_000, limits) == "MAX_LOSS"
    assert hard_loss_reason(LONG, size, cost, px(83690.0), None, limits) is None  # unknown estimate


def test_hold_timeout_and_stale_detection(limits: Limits) -> None:
    assert not hold_expired(4_999_999_999, 0, limits)
    assert hold_expired(5_000_000_000, 0, limits)
    assert is_stale(10**12, 0, limits)  # never received anything
    assert not is_stale(2_000_000_000, 1_000_000_000, limits)  # exactly 1000 ms old
    assert is_stale(2_000_000_001, 1_000_000_000, limits)


# ------------------------------------------------------------- rate limiting


def make_limiter(tier: str = "standard", max_entries: int = 100) -> tuple[RateLimiter, list[float]]:
    now = [1000.0]
    return RateLimiter(limits_for_tier(tier), max_entries, clock=lambda: now[0]), now


def test_tier_mapping_is_conservative() -> None:
    assert limits_for_tier("standard").request_capacity == 60
    assert limits_for_tier("something-new").name == "standard"
    premium = limits_for_tier("Premium")
    assert premium.request_capacity == 24_000 and premium.tx_capacity == 4000 and premium.has_volume_quota
    assert limits_for_tier("standard", 120).request_capacity == 120


def test_entry_never_consumes_the_exit_reserve() -> None:
    limiter, now = make_limiter()
    reserve = RESERVED_TX + RESERVED_READS
    sent = 0
    while limiter.can_enter():
        limiter.note_entry()
        limiter.note_tx()  # entry
        limiter.note_tx()  # its exit
        sent += 2
    # Entries stop while there is still room for exits, retries, cancel and reconciliation.
    assert 60 - sent >= reserve
    assert 60 - sent < reserve + 2
    assert limiter.snapshot()["request_headroom"] == 60 - sent
    # Exits are never refused by the limiter: it only records them.
    for _ in range(reserve):
        limiter.note_tx()
    assert limiter.snapshot()["request_headroom"] == 60 - sent - reserve
    now[0] += 61  # the rolling window moves on
    assert limiter.can_enter()


def test_max_entries_per_minute_cap() -> None:
    limiter, now = make_limiter("premium", max_entries=3)
    for _ in range(3):
        assert limiter.can_enter()
        limiter.note_entry()
    assert not limiter.can_enter()
    now[0] += 61
    assert limiter.can_enter()


def test_penalty_after_rate_limit_error() -> None:
    limiter, now = make_limiter()
    limiter.penalize(15)
    assert not limiter.can_enter() and limiter.in_penalty()
    now[0] += 16
    assert limiter.can_enter()


def test_volume_quota_only_applies_to_quota_tiers() -> None:
    standard, _ = make_limiter("standard")
    standard.note_volume_quota(0)
    assert standard.can_enter()
    premium, _ = make_limiter("premium")
    premium.note_volume_quota(5)
    assert not premium.can_enter()
    premium.note_volume_quota(5000)
    assert premium.can_enter()


def test_optional_transactions_respect_the_reserve() -> None:
    limiter, _ = make_limiter()
    for _ in range(60 - RESERVED_TX - RESERVED_READS - 1):
        limiter.note_tx()
    assert limiter.can_send_optional()
    limiter.note_tx()
    assert not limiter.can_send_optional()


# --------------------------------------------------------------- error classes


def test_api_error_classification() -> None:
    assert classify_api_error(400, 21104, "invalid nonce") is ErrorClass.NONCE_ERROR
    assert classify_api_error(429, None, "") is ErrorClass.RATE_LIMIT_ERROR
    assert classify_api_error(405, None, "") is ErrorClass.RATE_LIMIT_ERROR
    assert classify_api_error(400, 23000, "Too Many Requests!") is ErrorClass.RATE_LIMIT_ERROR
    assert classify_api_error(401, 20013, "invalid auth: invalid auth string") is ErrorClass.AUTH_ERROR
    assert classify_api_error(400, 21120, "invalid signature") is ErrorClass.AUTH_ERROR
    assert classify_api_error(400, 21739, "not enough margin to create the order") is ErrorClass.ORDER_REJECTED
    assert classify_api_error(503, None, "") is ErrorClass.EXCHANGE_ERROR
    assert is_reduce_only_rejection(21732, "reduce only increases position")
    assert not is_reduce_only_rejection(21739, "not enough margin to create the order")


# -------------------------------------------------------------- configuration


def test_valid_configuration(cfg: object) -> None:
    config = make_config()
    assert config.leverage == 25
    assert config.notional_usd == Decimal(250)
    assert config.initial_margin_fraction == 400
    assert config.max_normal_exit_slippage_mbps == 2000
    assert config.profit_exit_mode == "flat_first"
    assert config.api_private_key not in repr(config)  # secrets never appear in reprs/logs


@pytest.mark.parametrize(
    ("override", "fragment"),
    [
        ({"LIVE_TRADING": ""}, "LIVE_TRADING"),
        ({"LIVE_TRADING": "false"}, "LIVE_TRADING"),
        ({"I_UNDERSTAND_THIS_USES_REAL_FUNDS": "yes"}, "I_UNDERSTAND_THIS_USES_REAL_FUNDS"),
        ({"LIGHTER_API_PRIVATE_KEY": ""}, "LIGHTER_API_PRIVATE_KEY"),
        ({"LIGHTER_API_PRIVATE_KEY": "not-hex"}, "LIGHTER_API_PRIVATE_KEY"),
        ({"LIGHTER_ACCOUNT_INDEX": "-1"}, "LIGHTER_ACCOUNT_INDEX"),
        ({"LIGHTER_ACCOUNT_INDEX": "abc"}, "LIGHTER_ACCOUNT_INDEX"),
        ({"LIGHTER_API_KEY_INDEX": "255"}, "LIGHTER_API_KEY_INDEX"),
        ({"LEVERAGE": "0"}, "LEVERAGE"),
        ({"LEVERAGE": ""}, "LEVERAGE"),
        ({"MARGIN_PER_TRADE_USD": "0"}, "MARGIN_PER_TRADE_USD"),
        ({"MIN_PROFIT_USD": "0"}, "minimum profit"),
        ({"MIN_PROFIT_USD": "-1"}, "MIN_PROFIT_USD"),
        ({"MAX_ADVERSE_MOVE_BPS": "0", "MAX_LOSS_USD": "0"}, "loss protection"),
        ({"MAX_NORMAL_EXIT_SLIPPAGE_BPS": "50", "MAX_EMERGENCY_EXIT_SLIPPAGE_BPS": "10"}, "MAX_EMERGENCY"),
        ({"MAX_ENTRY_SLIPPAGE_BPS": "-1"}, "MAX_ENTRY_SLIPPAGE_BPS"),
        ({"MAX_HOLD_MS": "0"}, "MAX_HOLD_MS"),
        ({"ENTRY_SCORE_THRESHOLD": "1.5"}, "ENTRY_SCORE_THRESHOLD"),
        ({"LIGHTER_BASE_URL": "https://testnet.zklighter.elliot.ai"}, "mainnet"),
        ({"LIGHTER_WS_URL": "wss://example.com/stream"}, "mainnet"),
        ({"MARKET": "ETH"}, "BTC"),
        ({"POSITION_MODE": "all_in"}, "POSITION_MODE"),
        ({"PROFIT_EXIT_MODE": "trailing"}, "PROFIT_EXIT_MODE"),
    ],
)
def test_invalid_configuration_fails_closed(override: dict[str, str], fragment: str) -> None:
    env = dict(BASE_ENV)
    env.update(override)
    with pytest.raises(ConfigError) as error:
        load_config(env=env)
    assert fragment in str(error.value)
    assert BASE_ENV["LIGHTER_API_PRIVATE_KEY"] not in str(error.value)


def test_all_problems_are_reported_together() -> None:
    with pytest.raises(ConfigError) as error:
        load_config(env={})
    text = str(error.value)
    for key in ("LIVE_TRADING", "LIGHTER_ACCOUNT_INDEX", "LIGHTER_API_PRIVATE_KEY", "LEVERAGE", "MAX_HOLD_MS"):
        assert key in text


def test_env_file_parsing_and_environment_precedence(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    lines = [f"{key}={value}" for key, value in BASE_ENV.items() if key != "LEVERAGE"]
    lines += ["# comment", "", "export LEVERAGE='10'", 'LOG_LEVEL="debug"', "MAX_HOLD_MS=3000 # inline comment"]
    env_file.write_text("\n".join(lines), encoding="utf-8")
    parsed = parse_env_file(env_file)
    assert parsed["LEVERAGE"] == "10" and parsed["LOG_LEVEL"] == "debug" and parsed["MAX_HOLD_MS"] == "3000"
    config = load_config(env={"LEVERAGE": "20"}, env_file=env_file)
    assert config.leverage == 20  # the process environment wins over the file
    assert config.log_level == "DEBUG"
    assert config.max_hold_ms == 3000


def test_meta_fixture_matches_expected_shape(meta: MarketMeta) -> None:
    assert meta.symbol == "BTC" and meta.market_id == 1
