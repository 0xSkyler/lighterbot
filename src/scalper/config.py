"""Configuration loading and validation.

Values come from the process environment, optionally seeded from a ``.env``
file (the environment wins). Validation fails closed: every problem is
collected and raised as one :class:`ConfigError` and the service refuses to
start. There is no paper, shadow or testnet mode.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .errors import ConfigError
from .precision import bps_to_mbps
from .signals import SignalWeights

MAINNET_API_URL = "https://mainnet.zklighter.elliot.ai"
MAINNET_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"

MARGIN_CROSS = 0
MARGIN_ISOLATED = 1

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off", ""}


@dataclass(frozen=True, slots=True)
class Config:
    """Validated runtime configuration. Limits are stored in hot-path integer units."""

    # --- exchange / credentials
    base_url: str
    ws_url: str
    account_index: int
    api_key_index: int
    api_private_key: str = field(repr=False)
    market_symbol: str = "BTC"

    # --- sizing
    leverage: int = 0
    margin_mode: int = MARGIN_CROSS
    position_mode: str = "fixed_margin"
    margin_per_trade_usd: Decimal = Decimal(0)
    notional_per_trade_usd: Decimal = Decimal(0)

    # --- profit / exits
    min_profit_usd: Decimal = Decimal(0)
    min_profit_mbps: int = 0
    safety_buffer_mbps: int = 0
    profit_exit_mode: str = "flat_first"  # or "profit_only"
    exit_on_signal_reversal: bool = False
    max_normal_exit_slippage_mbps: int = 0
    max_emergency_exit_slippage_mbps: int = 0
    max_exit_attempts: int = 6
    exit_escalate_after: int = 2

    # --- protection
    max_adverse_move_mbps: int = 0
    max_loss_usd: Decimal = Decimal(0)
    max_hold_ms: int = 0
    market_data_stale_ms: int = 0
    order_resolve_timeout_ms: int = 3000
    account_stream_grace_ms: int = 1500

    # --- entries
    entry_mode: str = "maker"  # maker: resting post-only order | taker: LIMIT+IOC across the spread
    maker_rest_ms: int = 1500
    maker_improve_ticks: int = 1
    maker_max_drift_mbps: int = 500
    max_entry_slippage_mbps: int = 0
    max_spread_mbps: int = 0
    max_volatility_mbps: int = 15_000
    entry_score_threshold: float = 0.0
    weights: SignalWeights = field(default_factory=lambda: SignalWeights(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0))
    book_depth_levels: int = 10
    momentum_scale_bps: float = 2.0
    signal_warmup_ms: int = 5000
    max_entries_per_minute: int = 20
    use_ticker_stream: bool = True

    # --- account / limits
    taker_fee_bps_override: Decimal | None = None
    rate_limit_per_minute_override: int | None = None
    existing_position_action: str = "manage"  # manage | flatten | halt
    shutdown_position_action: str = "flatten"  # flatten | keep
    cancel_foreign_orders: bool = False
    reconcile_interval_s: int = 30
    auth_token_ttl_s: int = 7 * 3600
    tx_timeout_s: float = 3.0
    rest_timeout_s: float = 5.0

    # --- runtime
    data_dir: Path = Path("data")
    log_dir: Path = Path("logs")
    log_level: str = "INFO"
    log_to_file: bool = True
    trade_csv: bool = True

    @property
    def notional_usd(self) -> Decimal:
        """Target position notional in USD."""
        if self.position_mode == "fixed_notional":
            return self.notional_per_trade_usd
        return self.margin_per_trade_usd * self.leverage

    @property
    def initial_margin_fraction(self) -> int:
        """Exchange leverage parameter, 1/10_000 (same rounding as the official SDK)."""
        return 10_000 // self.leverage


def parse_env_file(path: Path) -> dict[str, str]:
    """Parse a simple KEY=VALUE file. Comments and blank lines are ignored."""
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, sep, value = line.partition("=")
        if not sep:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()
        values[key.strip()] = value
    return values


class _Reader:
    """Typed accessors over the raw environment that collect errors instead of raising."""

    def __init__(self, env: Mapping[str, str]) -> None:
        self.env = env
        self.errors: list[str] = []

    def raw(self, key: str) -> str:
        return (self.env.get(key) or "").strip()

    def text(self, key: str, default: str | None = None, choices: tuple[str, ...] = ()) -> str:
        value = self.raw(key)
        if not value:
            if default is None:
                self.errors.append(f"{key} is required")
                return ""
            value = default
        if choices and value.lower() not in choices:
            self.errors.append(f"{key} must be one of {', '.join(choices)} (got {value!r})")
            return choices[0]
        return value.lower() if choices else value

    def integer(self, key: str, default: int | None = None, lo: int | None = None, hi: int | None = None) -> int:
        value = self.raw(key)
        if not value:
            if default is None:
                self.errors.append(f"{key} is required")
                return 0
            return default
        try:
            number = int(value)
        except ValueError:
            self.errors.append(f"{key} must be an integer (got {value!r})")
            return 0
        if (lo is not None and number < lo) or (hi is not None and number > hi):
            self.errors.append(f"{key}={number} is outside the allowed range [{lo}, {hi}]")
        return number

    def decimal(
        self, key: str, default: Decimal | None = None, lo: Decimal | None = None, hi: Decimal | None = None
    ) -> Decimal:
        value = self.raw(key)
        if not value:
            if default is None:
                self.errors.append(f"{key} is required")
                return Decimal(0)
            return default
        try:
            number = Decimal(value)
        except InvalidOperation:
            self.errors.append(f"{key} must be a number (got {value!r})")
            return Decimal(0)
        if not number.is_finite():
            self.errors.append(f"{key} must be finite")
            return Decimal(0)
        if (lo is not None and number < lo) or (hi is not None and number > hi):
            self.errors.append(f"{key}={number} is outside the allowed range [{lo}, {hi}]")
        return number

    def optional_decimal(self, key: str, lo: Decimal, hi: Decimal) -> Decimal | None:
        return self.decimal(key, lo=lo, hi=hi) if self.raw(key) else None

    def optional_integer(self, key: str, lo: int, hi: int) -> int | None:
        return self.integer(key, lo=lo, hi=hi) if self.raw(key) else None

    def boolean(self, key: str, default: bool) -> bool:
        value = self.raw(key).lower()
        if not value:
            return default
        if value in _TRUE:
            return True
        if value in _FALSE:
            return False
        self.errors.append(f"{key} must be true or false (got {value!r})")
        return default


def _check_safety_gates(r: _Reader) -> None:
    """The service is live-only; these gates only stop an incomplete install from trading."""
    if r.raw("LIVE_TRADING").lower() != "true":
        r.errors.append("LIVE_TRADING must be set to true (this application only trades live)")
    if r.raw("I_UNDERSTAND_THIS_USES_REAL_FUNDS") != "YES":
        r.errors.append("I_UNDERSTAND_THIS_USES_REAL_FUNDS must be set to YES")


def _read_private_key(r: _Reader) -> str:
    key = r.raw("LIGHTER_API_PRIVATE_KEY")
    if not key:
        r.errors.append("LIGHTER_API_PRIVATE_KEY is required")
        return ""
    body = key[2:] if key.lower().startswith("0x") else key
    try:
        int(body, 16)
        valid = 64 <= len(body) <= 128
    except ValueError:
        valid = False
    if not valid:
        # Never echo any part of the key.
        r.errors.append("LIGHTER_API_PRIVATE_KEY is not a valid hex API private key")
    return key


def load_config(env: Mapping[str, str] | None = None, env_file: Path | None = None) -> Config:
    """Build a validated :class:`Config`.

    ``env`` defaults to ``os.environ``. If ``env_file`` exists its values are used
    for keys the environment does not define.
    """
    merged: dict[str, str] = {}
    if env_file is not None and env_file.is_file():
        merged.update(parse_env_file(env_file))
    merged.update(os.environ if env is None else env)
    r = _Reader(merged)

    _check_safety_gates(r)

    base_url = r.text("LIGHTER_BASE_URL", MAINNET_API_URL).rstrip("/")
    ws_url = r.text("LIGHTER_WS_URL", MAINNET_WS_URL).rstrip("/")
    if base_url != MAINNET_API_URL or ws_url != MAINNET_WS_URL:
        r.errors.append(f"unexpected endpoint: only Lighter mainnet is supported ({MAINNET_API_URL}, {MAINNET_WS_URL})")
    market = r.text("MARKET", "BTC").upper()
    if market != "BTC":
        r.errors.append("MARKET must be BTC (this application trades the BTC perpetual only)")

    account_index = r.integer("LIGHTER_ACCOUNT_INDEX", lo=0)
    api_key_index = r.integer("LIGHTER_API_KEY_INDEX", lo=0, hi=254)
    private_key = _read_private_key(r)

    leverage = r.integer("LEVERAGE", lo=1, hi=100)
    margin_mode = r.text("MARGIN_MODE", "cross", ("cross", "isolated"))
    position_mode = r.text("POSITION_MODE", "fixed_margin", ("fixed_margin", "fixed_notional"))
    margin_usd = r.decimal("MARGIN_PER_TRADE_USD", Decimal(0), lo=Decimal(0))
    notional_usd = r.decimal("NOTIONAL_PER_TRADE_USD", Decimal(0), lo=Decimal(0))
    if position_mode == "fixed_margin" and margin_usd <= 0:
        r.errors.append("MARGIN_PER_TRADE_USD must be > 0 when POSITION_MODE=fixed_margin")
    if position_mode == "fixed_notional" and notional_usd <= 0:
        r.errors.append("NOTIONAL_PER_TRADE_USD must be > 0 when POSITION_MODE=fixed_notional")

    min_profit_usd = r.decimal("MIN_PROFIT_USD", Decimal(0), lo=Decimal(0))
    min_profit_bps = r.decimal("MIN_PROFIT_BPS", Decimal(0), lo=Decimal(0), hi=Decimal(1000))
    if min_profit_usd <= 0 and min_profit_bps <= 0:
        r.errors.append("minimum profit invalid: set MIN_PROFIT_USD and/or MIN_PROFIT_BPS above 0")

    entry_slip = r.decimal("MAX_ENTRY_SLIPPAGE_BPS", lo=Decimal(0), hi=Decimal(100))
    normal_slip = r.decimal("MAX_NORMAL_EXIT_SLIPPAGE_BPS", lo=Decimal("0.001"), hi=Decimal(500))
    emergency_slip = r.decimal("MAX_EMERGENCY_EXIT_SLIPPAGE_BPS", lo=Decimal("0.001"), hi=Decimal(1000))
    if emergency_slip < normal_slip:
        r.errors.append("MAX_EMERGENCY_EXIT_SLIPPAGE_BPS must be >= MAX_NORMAL_EXIT_SLIPPAGE_BPS")

    adverse_bps = r.decimal("MAX_ADVERSE_MOVE_BPS", Decimal(0), lo=Decimal(0), hi=Decimal(5000))
    max_loss_usd = r.decimal("MAX_LOSS_USD", Decimal(0), lo=Decimal(0))
    if adverse_bps <= 0 and max_loss_usd <= 0:
        r.errors.append("loss protection invalid: set MAX_ADVERSE_MOVE_BPS and/or MAX_LOSS_USD above 0")

    weights = SignalWeights(
        book_imbalance=float(r.decimal("BOOK_IMBALANCE_WEIGHT", Decimal("1.0"), lo=Decimal(0))),
        trade_flow=float(r.decimal("TRADE_FLOW_WEIGHT", Decimal("1.5"), lo=Decimal(0))),
        micro_momentum=float(r.decimal("MICRO_MOMENTUM_WEIGHT", Decimal("1.5"), lo=Decimal(0))),
        bbo_momentum=float(r.decimal("BBO_MOMENTUM_WEIGHT", Decimal("1.0"), lo=Decimal(0))),
        microprice=float(r.decimal("MICROPRICE_WEIGHT", Decimal("1.0"), lo=Decimal(0))),
        volume_accel=float(r.decimal("VOLUME_ACCEL_WEIGHT", Decimal("0.5"), lo=Decimal(0))),
        depth_change=float(r.decimal("DEPTH_CHANGE_WEIGHT", Decimal("0.5"), lo=Decimal(0))),
    )
    if weights.total() <= 0:
        r.errors.append("at least one signal weight must be > 0")

    config = Config(
        base_url=base_url,
        ws_url=ws_url,
        account_index=account_index,
        api_key_index=api_key_index,
        api_private_key=private_key,
        market_symbol=market,
        leverage=leverage or 1,
        margin_mode=MARGIN_ISOLATED if margin_mode == "isolated" else MARGIN_CROSS,
        position_mode=position_mode,
        margin_per_trade_usd=margin_usd,
        notional_per_trade_usd=notional_usd,
        min_profit_usd=min_profit_usd,
        min_profit_mbps=bps_to_mbps(min_profit_bps),
        safety_buffer_mbps=bps_to_mbps(r.decimal("SAFETY_BUFFER_BPS", Decimal(0), lo=Decimal(0), hi=Decimal(100))),
        profit_exit_mode=r.text("PROFIT_EXIT_MODE", "flat_first", ("flat_first", "profit_only")),
        exit_on_signal_reversal=r.boolean("EXIT_ON_SIGNAL_REVERSAL", False),
        max_normal_exit_slippage_mbps=bps_to_mbps(normal_slip),
        max_emergency_exit_slippage_mbps=bps_to_mbps(emergency_slip),
        max_exit_attempts=r.integer("MAX_EXIT_ATTEMPTS", 6, lo=1, hi=50),
        exit_escalate_after=r.integer("EXIT_ESCALATE_AFTER", 2, lo=1, hi=50),
        max_adverse_move_mbps=bps_to_mbps(adverse_bps),
        max_loss_usd=max_loss_usd,
        max_hold_ms=r.integer("MAX_HOLD_MS", lo=100, hi=600_000),
        market_data_stale_ms=r.integer("MARKET_DATA_STALE_MS", lo=100, hi=30_000),
        order_resolve_timeout_ms=r.integer("ORDER_RESOLVE_TIMEOUT_MS", 3000, lo=500, hi=60_000),
        account_stream_grace_ms=r.integer("ACCOUNT_STREAM_GRACE_MS", 1500, lo=0, hi=60_000),
        entry_mode=r.text("ENTRY_MODE", "maker", ("maker", "taker")),
        maker_rest_ms=r.integer("MAKER_REST_MS", 1500, lo=100, hi=60_000),
        maker_improve_ticks=r.integer("MAKER_IMPROVE_TICKS", 1, lo=0, hi=100),
        maker_max_drift_mbps=bps_to_mbps(
            r.decimal("MAKER_MAX_DRIFT_BPS", Decimal("0.5"), lo=Decimal(0), hi=Decimal(100))
        ),
        max_entry_slippage_mbps=bps_to_mbps(entry_slip),
        max_spread_mbps=bps_to_mbps(r.decimal("MAX_SPREAD_BPS", lo=Decimal("0.001"), hi=Decimal(1000))),
        max_volatility_mbps=bps_to_mbps(r.decimal("MAX_VOLATILITY_BPS", Decimal(15), lo=Decimal("0.001"))),
        entry_score_threshold=float(r.decimal("ENTRY_SCORE_THRESHOLD", lo=Decimal("0.01"), hi=Decimal(1))),
        weights=weights,
        book_depth_levels=r.integer("BOOK_DEPTH_LEVELS", 10, lo=1, hi=50),
        momentum_scale_bps=float(r.decimal("MOMENTUM_SCALE_BPS", Decimal(2), lo=Decimal("0.01"))),
        signal_warmup_ms=r.integer("SIGNAL_WARMUP_MS", 5000, lo=1000, hi=120_000),
        max_entries_per_minute=r.integer("MAX_ENTRIES_PER_MINUTE", 20, lo=1, hi=600),
        use_ticker_stream=r.boolean("USE_TICKER_STREAM", True),
        taker_fee_bps_override=r.optional_decimal("TAKER_FEE_BPS", Decimal(0), Decimal(100)),
        rate_limit_per_minute_override=r.optional_integer("RATE_LIMIT_PER_MINUTE", 10, 1_000_000),
        existing_position_action=r.text("EXISTING_POSITION_ACTION", "manage", ("manage", "flatten", "halt")),
        shutdown_position_action=r.text("SHUTDOWN_POSITION_ACTION", "flatten", ("flatten", "keep")),
        cancel_foreign_orders=r.boolean("CANCEL_FOREIGN_ORDERS", False),
        reconcile_interval_s=r.integer("RECONCILE_INTERVAL_S", 30, lo=5, hi=3600),
        auth_token_ttl_s=r.integer("AUTH_TOKEN_TTL_S", 7 * 3600, lo=600, hi=8 * 3600),
        tx_timeout_s=float(r.decimal("TX_TIMEOUT_S", Decimal(3), lo=Decimal("0.5"), hi=Decimal(30))),
        rest_timeout_s=float(r.decimal("REST_TIMEOUT_S", Decimal(5), lo=Decimal("0.5"), hi=Decimal(60))),
        data_dir=Path(r.text("DATA_DIR", "data")),
        log_dir=Path(r.text("LOG_DIR", "logs")),
        log_level=r.text("LOG_LEVEL", "INFO", ("debug", "info", "warning", "error")).upper(),
        log_to_file=r.boolean("LOG_TO_FILE", True),
        trade_csv=r.boolean("TRADE_CSV", True),
    )
    if r.errors:
        raise ConfigError("invalid configuration:\n  - " + "\n  - ".join(r.errors), r.errors)
    return config
