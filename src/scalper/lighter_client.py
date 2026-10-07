"""Thin wrapper around the official Lighter Python SDK (``lighter-sdk``).

Everything that talks to the exchange over REST goes through this class:

* signing uses ``lighter.SignerClient.sign_*`` (the SDK's native signer),
* requests use the SDK's generated API classes (``OrderApi``, ``AccountApi``,
  ``TransactionApi``, ``RootApi``) through their ``*_without_preload_content``
  variants, so replies are parsed here as plain JSON: no model validation on
  the hot path and no crash if Lighter adds a field,
* one persistent keep-alive HTTP session is shared by all calls.

Nothing here retries a signed transaction. :class:`SendResult` tells the caller
whether the transaction was accepted, definitively rejected, or *ambiguous*.
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import aiohttp

from .config import MAINNET_API_URL, MAINNET_WS_URL, Config
from .errors import TX_NOT_FOUND_CODE, ApiError, ErrorClass, FatalError, classify_api_error
from .logging_setup import sanitize
from .metrics import Metrics
from .position import ActiveOrder, ExchangePosition, OrderUpdate
from .precision import MarketMeta, to_scaled
from .rate_limits import RateLimiter
from .wire import find_market_position, iter_objects, parse_order, parse_position

log = logging.getLogger("scalper.client")

READ_RETRIES = 2
KEEPALIVE_SECONDS = 90.0
_NETWORK_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError, OSError)


@dataclass(frozen=True, slots=True)
class SignedTx:
    tx_type: int
    tx_info: str
    tx_hash: str
    nonce: int
    signed_ns: int


@dataclass(frozen=True, slots=True)
class SendResult:
    """Outcome of submitting one signed transaction."""

    accepted: bool
    ambiguous: bool  # True: unknown whether the exchange received/accepted it
    error_class: ErrorClass | None
    code: int | None
    message: str
    tx_hash: str
    sent_ns: int
    ack_ns: int
    volume_quota_remaining: int | None = None

    @property
    def rejected(self) -> bool:
        return not self.accepted and not self.ambiguous


@dataclass(frozen=True, slots=True)
class AccountTier:
    name: str
    taker_fee_tick: int
    maker_fee_tick: int


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    """Authoritative account state for one market, from ``GET /api/v1/account``."""

    available_balance: Decimal
    collateral: Decimal
    position: ExchangePosition
    has_market_entry: bool  # False: the account has no leverage/margin record for this market yet


def _parse_json(raw: bytes) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        loaded = json.loads(raw)
    except ValueError:
        return {"message": raw[:200].decode("utf-8", "replace")}
    return loaded if isinstance(loaded, dict) else {"data": loaded}


def _code(data: dict[str, Any]) -> int | None:
    code = data.get("code")
    return code if isinstance(code, int) else None


class LighterClient:
    """REST + signing access to Lighter mainnet for one account and one API key."""

    def __init__(self, cfg: Config, limiter: RateLimiter, metrics: Metrics) -> None:
        self._cfg = cfg
        self._limiter = limiter
        self._metrics = metrics
        self._signer: Any = None
        self._order_api: Any = None
        self._account_api: Any = None
        self._tx_api: Any = None
        self._root_api: Any = None
        self._session: aiohttp.ClientSession | None = None
        self._old_session_close: asyncio.Task[None] | None = None
        self._token = ""
        self._token_expires_at = 0.0
        self.last_request_monotonic = 0.0

    # ------------------------------------------------------------ lifecycle

    async def connect(self) -> None:
        """Create the SDK clients. Must run inside the event loop. No orders are sent."""
        import lighter
        from lighter.endpoint_profiles import MAINNET
        from lighter.nonce_manager import NonceManagerType

        cfg = self._cfg
        if MAINNET.api_url != MAINNET_API_URL or MAINNET.ws_url != MAINNET_WS_URL:
            raise FatalError(
                "the installed lighter-sdk reports different mainnet endpoints than this build expects; "
                "refusing to start",
                ErrorClass.CONFIG_ERROR,
            )
        try:
            self._signer = lighter.SignerClient(
                url=cfg.base_url,
                account_index=cfg.account_index,
                api_private_keys={cfg.api_key_index: cfg.api_private_key},
                nonce_management_type=NonceManagerType.NONE,  # nonces are managed in execution.py
                chain_id=MAINNET.chain_id,
            )
        except Exception as exc:  # signer library load / key import failure
            raise FatalError(
                f"could not initialise the Lighter signer: {sanitize(exc, [cfg.api_private_key])}",
                ErrorClass.AUTH_ERROR,
            ) from None
        api_client = self._signer.api_client
        self._install_session(api_client)
        self._order_api = lighter.OrderApi(api_client)
        self._account_api = lighter.AccountApi(api_client)
        self._tx_api = lighter.TransactionApi(api_client)
        self._root_api = lighter.RootApi(api_client)

    def _install_session(self, api_client: Any) -> None:
        """Swap in an HTTP session with a long keep-alive so orders reuse a warm TLS connection."""
        rest_client = getattr(api_client, "rest_client", None)
        old = getattr(rest_client, "pool_manager", None)
        if rest_client is None or not isinstance(old, aiohttp.ClientSession):
            log.warning("CLIENT_SESSION_DEFAULT reason=unexpected_sdk_layout")
            return
        connector = aiohttp.TCPConnector(
            limit=8, keepalive_timeout=KEEPALIVE_SECONDS, ttl_dns_cache=600, ssl=ssl.create_default_context()
        )
        self._session = aiohttp.ClientSession(connector=connector, trust_env=True)
        rest_client.pool_manager = self._session
        self._old_session_close = asyncio.get_running_loop().create_task(old.close())

    async def close(self) -> None:
        if self._signer is not None:
            try:
                await self._signer.close()
            except _NETWORK_ERRORS:
                pass
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def verify_api_key(self) -> None:
        """Confirm the configured API key is the one registered on Lighter for this account."""
        error = await asyncio.to_thread(self._signer.check_client)
        if error is not None:
            raise FatalError(
                "API key check failed: " + sanitize(error, [self._cfg.api_private_key]), ErrorClass.AUTH_ERROR
            )

    # ----------------------------------------------------------------- auth

    def auth_token(self, *, force: bool = False) -> str:
        """Auth token for REST headers / WS subscriptions (cached; Lighter caps expiry at 8 h)."""
        now = time.time()
        if not force and self._token and now < self._token_expires_at - 600:
            return self._token
        ttl = self._cfg.auth_token_ttl_s
        token, error = self._signer.create_auth_token_with_expiry(ttl, api_key_index=self._cfg.api_key_index)
        if error is not None or not token:
            raise ApiError("could not create auth token", error_class=ErrorClass.AUTH_ERROR)
        self._token = str(token)
        self._token_expires_at = now + ttl
        return self._token

    def token_age_remaining(self) -> float:
        return self._token_expires_at - time.time()

    # ----------------------------------------------------------- REST reads

    async def _read(self, call: Callable[..., Awaitable[Any]], **params: Any) -> dict[str, Any]:
        """Execute a read-only request with a bounded retry on network errors and 5xx."""
        last_error: ApiError | None = None
        for attempt in range(READ_RETRIES + 1):
            if attempt:
                await asyncio.sleep(0.25 * attempt)
            self._limiter.note_read()
            started = time.monotonic()
            self.last_request_monotonic = started
            try:
                response = await call(**params, _request_timeout=self._cfg.rest_timeout_s)
                raw = await response.read()
            except _NETWORK_ERRORS as exc:
                last_error = ApiError(
                    f"network error: {type(exc).__name__}", error_class=ErrorClass.NETWORK_ERROR, ambiguous=True
                )
                continue
            self._metrics.add("api_read_ms", (time.monotonic() - started) * 1000.0)
            data = _parse_json(raw)
            code = _code(data)
            if response.status == 200 and code in (None, 0, 200):
                return data
            error_class = classify_api_error(response.status, code, str(data.get("message", "")))
            last_error = ApiError(
                f"HTTP {response.status} code={code} {sanitize(data.get('message', ''))}",
                error_class=error_class,
                http_status=response.status,
                code=code,
            )
            if error_class is ErrorClass.RATE_LIMIT_ERROR:
                self._limiter.penalize()
                break
            if response.status < 500:
                break
        assert last_error is not None
        self._metrics.count_error(last_error.error_class)
        raise last_error

    async def ping(self) -> float:
        """Cheap request that keeps the HTTPS connection warm. Returns latency in ms."""
        started = time.monotonic()
        await self._read(self._root_api.status_without_preload_content)
        return (time.monotonic() - started) * 1000.0

    async def fetch_market_meta(self, symbol: str) -> MarketMeta:
        """Locate the BTC perpetual by symbol and load its precision / margin constraints."""
        books = await self._read(self._order_api.order_books_without_preload_content)
        matches = [
            book
            for book in books.get("order_books", [])
            if str(book.get("symbol", "")).upper() == symbol.upper() and book.get("market_type") == "perp"
        ]
        if len(matches) != 1:
            raise FatalError(
                f"expected exactly one {symbol} perpetual market on Lighter, found {len(matches)}",
                ErrorClass.CONFIG_ERROR,
            )
        market_id = int(matches[0]["market_id"])
        details = await self._read(self._order_api.order_book_details_without_preload_content, market_id=market_id)
        detail = next(
            (d for d in details.get("order_book_details", []) if int(d.get("market_id", -1)) == market_id), None
        )
        if detail is None or str(detail.get("symbol", "")).upper() != symbol.upper():
            raise FatalError(f"{symbol} market details unavailable", ErrorClass.CONFIG_ERROR)
        return self._build_meta(detail)

    @staticmethod
    def _build_meta(detail: dict[str, Any]) -> MarketMeta:
        price_decimals = int(detail["price_decimals"])
        size_decimals = int(detail["size_decimals"])
        for key, expected in (("supported_price_decimals", price_decimals), ("supported_size_decimals", size_decimals)):
            if key in detail and int(detail[key]) != expected:
                raise FatalError(f"inconsistent market precision ({key})", ErrorClass.CONFIG_ERROR)
        config = detail.get("market_config") or {}
        status = str(detail.get("status", ""))
        if detail.get("is_frozen") or config.get("force_reduce_only"):
            status = "restricted"
        meta = MarketMeta(
            symbol=str(detail["symbol"]).upper(),
            market_id=int(detail["market_id"]),
            price_decimals=price_decimals,
            size_decimals=size_decimals,
            min_base=to_scaled(str(detail["min_base_amount"]), size_decimals),
            min_quote_q=int(Decimal(str(detail["min_quote_amount"])) * 10 ** (price_decimals + size_decimals)),
            min_imf=int(detail["min_initial_margin_fraction"]),
            default_imf=int(detail["default_initial_margin_fraction"]),
            maintenance_imf=int(detail["maintenance_margin_fraction"]),
            status=status,
        )
        if meta.min_base <= 0 or meta.min_imf <= 0 or not 0 <= price_decimals <= 12 or not 0 <= size_decimals <= 12:
            raise FatalError("market metadata failed validation", ErrorClass.CONFIG_ERROR)
        return meta

    async def fetch_account_tier(self) -> AccountTier:
        """Account tier and current fee ticks (``accountLimits``)."""
        data = await self._read(
            self._account_api.account_limits_without_preload_content,
            account_index=self._cfg.account_index,
            authorization=self.auth_token(),
        )
        return AccountTier(
            name=str(data.get("user_tier_name") or data.get("user_tier") or "standard"),
            taker_fee_tick=int(data.get("current_taker_fee_tick") or 0),
            maker_fee_tick=int(data.get("current_maker_fee_tick") or 0),
        )

    async def fetch_account(self, meta: MarketMeta) -> AccountSnapshot:
        """Authoritative balance and BTC position."""
        data = await self._read(
            self._account_api.account_without_preload_content, by="index", value=str(self._cfg.account_index)
        )
        accounts = data.get("accounts") or []
        account = next(
            (a for a in accounts if int(a.get("index", a.get("account_index", -1))) == self._cfg.account_index), None
        )
        if account is None:
            raise ApiError("account not found in reply", error_class=ErrorClass.EXCHANGE_ERROR)
        raw_position = find_market_position(account.get("positions") or [], meta.market_id)
        return AccountSnapshot(
            available_balance=Decimal(str(account.get("available_balance") or "0")),
            collateral=Decimal(str(account.get("collateral") or "0")),
            position=parse_position(raw_position, meta, time.monotonic_ns()),
            has_market_entry=raw_position is not None,
        )

    async def fetch_active_orders(self, meta: MarketMeta) -> list[OrderUpdate]:
        """All resting/pending orders of this account in the BTC market."""
        data = await self._read(
            self._order_api.account_active_orders_without_preload_content,
            authorization=self.auth_token(),
            account_index=self._cfg.account_index,
            market_id=meta.market_id,
        )
        return [parse_order(obj, meta) for obj in iter_objects(data.get("orders") or [])]

    async def fetch_recent_orders(self, meta: MarketMeta) -> dict[int, OrderUpdate]:
        """Recently finished BTC orders keyed by client order index (final fills after recovery)."""
        data = await self._read(
            self._order_api.account_inactive_orders_without_preload_content,
            authorization=self.auth_token(),
            account_index=self._cfg.account_index,
            market_id=meta.market_id,
            limit=50,
        )
        orders = (parse_order(obj, meta) for obj in iter_objects(data.get("orders") or []))
        return {order.client_order_index: order for order in orders}

    async def fetch_next_nonce(self) -> int:
        data = await self._read(
            self._tx_api.next_nonce_without_preload_content,
            account_index=self._cfg.account_index,
            api_key_index=self._cfg.api_key_index,
        )
        return int(data["nonce"])

    async def fetch_tx_status(self, tx_hash: str) -> int | None:
        """Transaction status (0 failed, 1 pending, 2 executed, 3 pending-final); None if unknown."""
        try:
            data = await self._read(self._tx_api.tx_without_preload_content, by="hash", value=tx_hash)
        except ApiError as exc:
            if exc.code == TX_NOT_FOUND_CODE:
                return None
            raise
        status = data.get("status")
        return int(status) if status is not None else None

    async def fetch_rest_bbo(self, meta: MarketMeta) -> tuple[int, int]:
        """Best bid / ask from REST. Used only when the WebSocket book is unavailable."""
        data = await self._read(
            self._order_api.order_book_orders_without_preload_content, market_id=meta.market_id, limit=1
        )
        bids = data.get("bids") or []
        asks = data.get("asks") or []
        if not bids or not asks:
            raise ApiError("empty order book", error_class=ErrorClass.MARKET_DATA_ERROR)
        return meta.price_to_int(str(bids[0]["price"])), meta.price_to_int(str(asks[0]["price"]))

    # ------------------------------------------------------------- signing

    def _signed(self, result: tuple[Any, Any, Any, Any], nonce: int) -> SignedTx:
        tx_type, tx_info, tx_hash, error = result
        if error is not None or not tx_info:
            raise ApiError(
                "signing failed: " + sanitize(error, [self._cfg.api_private_key]),
                error_class=ErrorClass.ORDER_REJECTED,
            )
        return SignedTx(int(tx_type), str(tx_info), str(tx_hash), nonce, time.monotonic_ns())

    def sign_order(self, order: ActiveOrder, market_id: int, nonce: int) -> SignedTx:
        """Sign an IOC order: LIMIT+IOC (strict per-fill cap) or MARKET+IOC (worst-price bound)."""
        signer = self._signer
        return self._signed(
            signer.sign_create_order(
                market_index=market_id,
                client_order_index=order.client_order_index,
                base_amount=order.size,
                price=order.limit_price,
                is_ask=int(order.is_ask),
                order_type=signer.ORDER_TYPE_MARKET if order.market_order else signer.ORDER_TYPE_LIMIT,
                time_in_force=signer.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=int(order.reduce_only),
                trigger_price=signer.NIL_TRIGGER_PRICE,
                order_expiry=signer.DEFAULT_IOC_EXPIRY,
                nonce=nonce,
                api_key_index=self._cfg.api_key_index,
            ),
            nonce,
        )

    def sign_cancel(self, market_id: int, order_index: int, nonce: int) -> SignedTx:
        return self._signed(
            self._signer.sign_cancel_order(
                market_index=market_id, order_index=order_index, nonce=nonce, api_key_index=self._cfg.api_key_index
            ),
            nonce,
        )

    def sign_cancel_all(self, market_id: int, nonce: int) -> SignedTx:
        """Immediate cancel-all scoped to one market."""
        signer = self._signer
        return self._signed(
            signer.sign_cancel_all_orders(
                time_in_force=signer.CANCEL_ALL_TIF_IMMEDIATE,
                timestamp_ms=0,
                cancel_all_market_index=market_id,
                nonce=nonce,
                api_key_index=self._cfg.api_key_index,
            ),
            nonce,
        )

    def sign_update_leverage(self, market_id: int, imf: int, margin_mode: int, nonce: int) -> SignedTx:
        return self._signed(
            self._signer.sign_update_leverage(
                market_index=market_id,
                fraction=imf,
                margin_mode=margin_mode,
                nonce=nonce,
                api_key_index=self._cfg.api_key_index,
            ),
            nonce,
        )

    # ------------------------------------------------------------- sending

    async def send_signed(self, signed: SignedTx) -> SendResult:
        """POST one signed transaction. Never retried here."""
        self._limiter.note_tx()
        sent_ns = time.monotonic_ns()
        self.last_request_monotonic = time.monotonic()
        try:
            response = await self._tx_api.send_tx_without_preload_content(
                tx_type=signed.tx_type, tx_info=signed.tx_info, _request_timeout=self._cfg.tx_timeout_s
            )
            raw = await response.read()
        except _NETWORK_ERRORS as exc:
            self._metrics.count_error(ErrorClass.NETWORK_ERROR)
            return SendResult(
                False,
                True,
                ErrorClass.NETWORK_ERROR,
                None,
                type(exc).__name__,
                signed.tx_hash,
                sent_ns,
                time.monotonic_ns(),
            )
        ack_ns = time.monotonic_ns()
        data = _parse_json(raw)
        code = _code(data)
        quota = data.get("volume_quota_remaining")
        quota = quota if isinstance(quota, int) else None
        if response.status == 200 and code == 200:
            self._limiter.note_volume_quota(quota)
            return SendResult(
                True, False, None, code, "", str(data.get("tx_hash") or signed.tx_hash), sent_ns, ack_ns, quota
            )
        message = sanitize(data.get("message", ""))
        error_class = classify_api_error(response.status, code, message)
        self._metrics.count_error(error_class)
        if error_class is ErrorClass.RATE_LIMIT_ERROR:
            self._limiter.penalize()
        # A 5xx (or a 200 without an explicit success code) leaves the outcome unknown.
        ambiguous = response.status >= 500 or response.status == 200
        return SendResult(False, ambiguous, error_class, code, message, signed.tx_hash, sent_ns, ack_ns, quota)
