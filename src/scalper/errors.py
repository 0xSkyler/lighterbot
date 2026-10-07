"""Error taxonomy.

Every failure on a trading path is mapped to exactly one :class:`ErrorClass`
and each class has explicit handling in ``strategy.py`` / ``execution.py``.
"""

from __future__ import annotations

import enum

# sysexits.h EX_CONFIG. The systemd unit lists it in RestartPreventExitStatus so
# a bad credential or config never becomes a restart loop hammering the API.
EXIT_OK = 0
EXIT_FATAL = 78


class ErrorClass(enum.Enum):
    MARKET_DATA_ERROR = "MARKET_DATA_ERROR"
    AUTH_ERROR = "AUTH_ERROR"
    RATE_LIMIT_ERROR = "RATE_LIMIT_ERROR"
    ORDER_REJECTED = "ORDER_REJECTED"
    NONCE_ERROR = "NONCE_ERROR"
    PARTIAL_FILL = "PARTIAL_FILL"
    NETWORK_ERROR = "NETWORK_ERROR"
    STATE_MISMATCH = "STATE_MISMATCH"
    EXCHANGE_ERROR = "EXCHANGE_ERROR"
    CONFIG_ERROR = "CONFIG_ERROR"


class ScalperError(Exception):
    """Base class carrying an :class:`ErrorClass`."""

    error_class: ErrorClass = ErrorClass.EXCHANGE_ERROR

    def __init__(self, message: str, error_class: ErrorClass | None = None) -> None:
        super().__init__(message)
        if error_class is not None:
            self.error_class = error_class


class ConfigError(ScalperError):
    """Invalid or incomplete configuration. Always fatal; the service stays down."""

    error_class = ErrorClass.CONFIG_ERROR

    def __init__(self, message: str, problems: list[str] | None = None) -> None:
        super().__init__(message)
        self.problems: list[str] = list(problems) if problems else [message]


class FatalError(ScalperError):
    """Unrecoverable condition (bad credentials, market missing). Service stays down."""


class ApiError(ScalperError):
    """A REST call returned a non-success answer or could not be completed.

    ``ambiguous`` is True when we cannot know whether the server acted on the
    request (timeout, connection reset, 5xx). Signed transactions with an
    ambiguous outcome are never blindly retried.
    """

    def __init__(
        self,
        message: str,
        *,
        error_class: ErrorClass,
        http_status: int | None = None,
        code: int | None = None,
        ambiguous: bool = False,
    ) -> None:
        super().__init__(message, error_class)
        self.http_status = http_status
        self.code = code
        self.ambiguous = ambiguous


# Lighter error codes, from the official "Data structures, constants and errors" page.
_NONCE_CODES = frozenset({21104, 21105})
_RATE_LIMIT_CODES = frozenset(range(23000, 23007))
_AUTH_CODES = frozenset({20013, 21120})
_REDUCE_ONLY_CODES = frozenset({21732, 21738, 21740})
TX_NOT_FOUND_CODE = 21500


def classify_api_error(http_status: int | None, code: int | None, message: str) -> ErrorClass:
    """Map an HTTP status / Lighter error code / message to an :class:`ErrorClass`."""
    text = (message or "").lower()
    if code in _NONCE_CODES or "nonce" in text:
        return ErrorClass.NONCE_ERROR
    # Lighter answers rate limiting with HTTP 429 or HTTP 405.
    if code in _RATE_LIMIT_CODES or http_status in (429, 405) or "too many" in text:
        return ErrorClass.RATE_LIMIT_ERROR
    if code in _AUTH_CODES or http_status in (401, 403) or "invalid auth" in text or "signature" in text:
        return ErrorClass.AUTH_ERROR
    if http_status is not None and http_status >= 500:
        return ErrorClass.EXCHANGE_ERROR
    if code in (29500, 29501):
        return ErrorClass.EXCHANGE_ERROR
    if http_status == 400 or (code is not None and 21000 <= code < 22000):
        return ErrorClass.ORDER_REJECTED
    return ErrorClass.EXCHANGE_ERROR


def is_reduce_only_rejection(code: int | None, message: str) -> bool:
    """True when the exchange refused a reduce-only order (position already gone or flipped)."""
    return code in _REDUCE_ONLY_CODES or "reduce only" in (message or "").lower()
