"""The settings the control panel can show and edit, and how they are validated.

Validation is delegated to ``config.load_config`` so the panel and the bot can
never disagree about what a valid configuration is.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from ..config import load_config
from ..errors import ConfigError

SECRET_KEYS = frozenset({"LIGHTER_API_PRIVATE_KEY"})


@dataclass(frozen=True, slots=True)
class Field:
    key: str
    label: str
    kind: str  # int | number | choice | bool | secret | gate
    help: str = ""
    unit: str = ""
    placeholder: str = ""
    choices: tuple[str, ...] = ()
    gate_value: str = ""  # for kind == "gate": the exact value that arms the switch
    default: str = ""  # value in force when the setting is absent (booleans)


@dataclass(frozen=True, slots=True)
class Group:
    id: str
    title: str
    description: str
    fields: tuple[Field, ...]
    advanced: bool = False


GROUPS: tuple[Group, ...] = (
    Group(
        "account",
        "Lighter account",
        "The account the bot trades. Use a dedicated sub-account: the bot treats any BTC position on it as its own.",
        (
            Field("LIGHTER_ACCOUNT_INDEX", "Account index", "int", "Index of the (sub-)account on Lighter."),
            Field(
                "LIGHTER_API_KEY_INDEX",
                "API key index",
                "int",
                "4 to 254. 0 to 3 belong to Lighter's own apps.",
                placeholder="4",
            ),
            Field(
                "LIGHTER_API_PRIVATE_KEY",
                "API key private key",
                "secret",
                "The API key's private key (hex). Never your wallet key or seed phrase. "
                "It is stored in the environment file and never shown again.",
            ),
        ),
    ),
    Group(
        "gates",
        "Live trading switches",
        "Both must be on before the bot will start. Turning one off keeps the bot from starting.",
        (
            Field(
                "LIVE_TRADING",
                "Live trading enabled",
                "gate",
                "This bot only trades live on Lighter mainnet.",
                gate_value="true",
            ),
            Field(
                "I_UNDERSTAND_THIS_USES_REAL_FUNDS",
                "I understand this uses real funds",
                "gate",
                "Leveraged positions are opened with the real balance of the account above.",
                gate_value="YES",
            ),
        ),
    ),
    Group(
        "size",
        "Position size",
        "One position at a time. The account balance is never used automatically.",
        (
            Field(
                "LEVERAGE",
                "Leverage",
                "int",
                "Checked against what Lighter allows and confirmed before trading.",
                unit="x",
                placeholder="25",
            ),
            Field("MARGIN_MODE", "Margin mode", "choice", choices=("cross", "isolated")),
            Field(
                "POSITION_MODE",
                "Sizing mode",
                "choice",
                "fixed_margin: notional = margin x leverage. fixed_notional: notional as entered.",
                choices=("fixed_margin", "fixed_notional"),
            ),
            Field(
                "MARGIN_PER_TRADE_USD",
                "Margin per trade",
                "number",
                "Used when sizing mode is fixed_margin.",
                unit="USD",
                placeholder="10",
            ),
            Field(
                "NOTIONAL_PER_TRADE_USD",
                "Notional per trade",
                "number",
                "Used when sizing mode is fixed_notional.",
                unit="USD",
            ),
        ),
    ),
    Group(
        "profit",
        "Profit exit",
        "A trade is green when the whole position can be closed now for more than this, after fees.",
        (
            Field("MIN_PROFIT_USD", "Minimum profit", "number", unit="USD", placeholder="0.01"),
            Field(
                "MIN_PROFIT_BPS",
                "Minimum profit (relative)",
                "number",
                "The larger of the two minimums applies.",
                unit="bps",
                placeholder="0",
            ),
            Field(
                "SAFETY_BUFFER_BPS",
                "Safety buffer",
                "number",
                "Extra cushion taken off the estimated exit value.",
                unit="bps",
                placeholder="0",
            ),
            Field(
                "PROFIT_EXIT_MODE",
                "When green is detected",
                "choice",
                "flat_first: close within the normal slippage band and keep closing. "
                "profit_only: the exit can only fill at a still-profitable price; a miss goes back to watching.",
                choices=("flat_first", "profit_only"),
            ),
            Field("EXIT_ON_SIGNAL_REVERSAL", "Also exit when the signal reverses", "bool"),
        ),
    ),
    Group(
        "slippage",
        "Slippage limits",
        "How far past the best price an order may reach.",
        (
            Field("MAX_ENTRY_SLIPPAGE_BPS", "Entry", "number", unit="bps", placeholder="1"),
            Field("MAX_NORMAL_EXIT_SLIPPAGE_BPS", "Normal exit", "number", unit="bps", placeholder="2"),
            Field("MAX_EMERGENCY_EXIT_SLIPPAGE_BPS", "Emergency exit", "number", unit="bps", placeholder="30"),
        ),
    ),
    Group(
        "protection",
        "Protection",
        "Catastrophic limits, not profit targets. At least one loss limit must be set.",
        (
            Field("MAX_ADVERSE_MOVE_BPS", "Maximum adverse move", "number", unit="bps", placeholder="10"),
            Field("MAX_LOSS_USD", "Maximum loss per trade", "number", unit="USD", placeholder="0.50"),
            Field(
                "MAX_HOLD_MS",
                "Maximum holding time",
                "int",
                "A position that is neither green nor stopped is closed after this long.",
                unit="ms",
                placeholder="5000",
            ),
            Field("MARKET_DATA_STALE_MS", "Market data counts as stale after", "int", unit="ms", placeholder="1000"),
        ),
    ),
    Group(
        "signal",
        "Entry signal",
        "Score from -1 to +1 built from order-book and trade-flow components.",
        (
            Field("ENTRY_SCORE_THRESHOLD", "Score needed to enter", "number", "Between 0 and 1.", placeholder="0.60"),
            Field("MAX_SPREAD_BPS", "Maximum spread", "number", unit="bps", placeholder="1.0"),
            Field("MAX_VOLATILITY_BPS", "Maximum 1 s price range", "number", unit="bps", placeholder="15"),
            Field(
                "MAX_ENTRIES_PER_MINUTE",
                "Entry cap per minute",
                "int",
                "An upper bound only; the market decides the real frequency.",
                placeholder="20",
            ),
            Field("BOOK_IMBALANCE_WEIGHT", "Weight: book imbalance", "number", placeholder="1.0"),
            Field("TRADE_FLOW_WEIGHT", "Weight: trade flow", "number", placeholder="1.5"),
            Field("MICRO_MOMENTUM_WEIGHT", "Weight: micro momentum", "number", placeholder="1.5"),
            Field("BBO_MOMENTUM_WEIGHT", "Weight: best bid/ask momentum", "number", placeholder="1.0"),
            Field("MICROPRICE_WEIGHT", "Weight: microprice", "number", placeholder="1.0"),
            Field("VOLUME_ACCEL_WEIGHT", "Weight: volume acceleration", "number", placeholder="0.5"),
            Field("DEPTH_CHANGE_WEIGHT", "Weight: depth change", "number", placeholder="0.5"),
            Field("BOOK_DEPTH_LEVELS", "Book levels used", "int", placeholder="10"),
            Field("MOMENTUM_SCALE_BPS", "Momentum full scale", "number", unit="bps", placeholder="2"),
        ),
    ),
    Group(
        "recovery",
        "Restart and recovery",
        "What happens to a position at startup and at shutdown.",
        (
            Field(
                "EXISTING_POSITION_ACTION",
                "A BTC position already exists",
                "choice",
                "manage: adopt it and apply the normal exit rules. flatten: close it. "
                "halt: touch nothing, do not trade.",
                choices=("manage", "flatten", "halt"),
            ),
            Field(
                "SHUTDOWN_POSITION_ACTION",
                "When the bot is stopped",
                "choice",
                "flatten: close the position first. keep: leave it open.",
                choices=("flatten", "keep"),
            ),
            Field(
                "CANCEL_FOREIGN_ORDERS",
                "Cancel BTC orders the bot did not create",
                "bool",
                "Otherwise such orders block new entries.",
            ),
        ),
    ),
    Group(
        "advanced",
        "Advanced",
        "Leave empty to use the built-in defaults or what the exchange reports.",
        (
            Field("TAKER_FEE_BPS", "Taker fee override", "number", unit="bps"),
            Field("RATE_LIMIT_PER_MINUTE", "Request budget override", "int", unit="per min"),
            Field("MAX_EXIT_ATTEMPTS", "Exit attempts before recovery", "int", placeholder="6"),
            Field("EXIT_ESCALATE_AFTER", "Exit attempts before the emergency band", "int", placeholder="2"),
            Field("ORDER_RESOLVE_TIMEOUT_MS", "Order result timeout", "int", unit="ms", placeholder="3000"),
            Field(
                "ACCOUNT_STREAM_GRACE_MS",
                "Account stream outage tolerated with a position",
                "int",
                unit="ms",
                placeholder="1500",
            ),
            Field("RECONCILE_INTERVAL_S", "Exchange check interval while flat", "int", unit="s", placeholder="30"),
            Field("USE_TICKER_STREAM", "Use the fast best-bid/ask stream", "bool", default="true"),
            Field("LOG_LEVEL", "Log level", "choice", choices=("INFO", "DEBUG", "WARNING", "ERROR")),
        ),
        advanced=True,
    ),
)

FIELDS: dict[str, Field] = {field.key: field for group in GROUPS for field in group.fields}


def schema_json() -> list[dict[str, Any]]:
    """Schema in a JSON-friendly form for the front end."""
    return [asdict(group) for group in GROUPS]


@dataclass(frozen=True, slots=True)
class SettingsCheck:
    value_errors: list[str]  # a value that was entered is wrong: saving is refused
    start_blockers: list[str]  # everything that currently prevents the bot from starting

    @property
    def ready(self) -> bool:
        return not self.start_blockers


def _problems(env: Mapping[str, str]) -> list[str]:
    try:
        load_config(env=env)
    except ConfigError as exc:
        return list(exc.problems)
    return []


def check_settings(env: Mapping[str, str]) -> SettingsCheck:
    """Validate a candidate environment.

    Missing credentials and switched-off gates do not make the *values* wrong (the operator
    may be saving in steps, or deliberately disarming), so they are judged separately.
    """
    blockers = _problems(env)
    probe = dict(env)
    probe["LIVE_TRADING"] = "true"
    probe["I_UNDERSTAND_THIS_USES_REAL_FUNDS"] = "YES"
    if not probe.get("LIGHTER_API_PRIVATE_KEY", "").strip():
        probe["LIGHTER_API_PRIVATE_KEY"] = "0" * 80
    if not probe.get("LIGHTER_ACCOUNT_INDEX", "").strip():
        probe["LIGHTER_ACCOUNT_INDEX"] = "0"
    if not probe.get("LIGHTER_API_KEY_INDEX", "").strip():
        probe["LIGHTER_API_KEY_INDEX"] = "4"
    return SettingsCheck(_problems(probe), blockers)


def normalise(field: Field, raw: Any) -> str:
    """Convert a value submitted by the front end into its environment-file text."""
    if field.kind == "gate":
        return field.gate_value if raw in (True, "true", "on", field.gate_value) else ""
    if field.kind == "bool":
        return "true" if raw in (True, "true", "on") else "false"
    return str(raw if raw is not None else "").strip()


def public_values(env: Mapping[str, str]) -> dict[str, Any]:
    """Values safe to send to the browser: secrets are reduced to set / not set."""
    values: dict[str, Any] = {}
    for key, field in FIELDS.items():
        raw = env.get(key, "").strip()
        if field.kind == "secret":
            values[key] = bool(raw)
        elif field.kind == "gate":
            values[key] = raw == field.gate_value or (key == "LIVE_TRADING" and raw.lower() == "true")
        elif field.kind == "bool":
            values[key] = (raw or field.default).lower() in ("1", "true", "yes", "on")
        else:
            values[key] = raw
    return values
