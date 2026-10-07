"""Parsing of Lighter REST / WebSocket payloads into internal integer types.

Pure functions with no SDK dependency. They are deliberately tolerant about
container shapes (Lighter keys some payloads by market index, others are plain
lists) and strict about numbers: a value that cannot be parsed raises instead
of being guessed.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from .position import ExchangePosition, OrderUpdate, TradeFill
from .precision import MarketMeta, to_scaled


def iter_objects(container: Any) -> Iterator[Mapping[str, Any]]:
    """Yield JSON objects from a list, a dict of lists, or a dict of objects."""
    if isinstance(container, list):
        for item in container:
            if isinstance(item, Mapping):
                yield item
            elif isinstance(item, list):
                yield from iter_objects(item)
    elif isinstance(container, Mapping):
        for value in container.values():
            if isinstance(value, Mapping):
                yield value
            elif isinstance(value, list):
                yield from iter_objects(value)


def _int(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def quote_to_q(value: Any, meta: MarketMeta) -> int:
    """USD decimal string -> quote units (price_int * size_int scale)."""
    text = str(value if value not in (None, "") else "0")
    decimals = meta.price_decimals + meta.size_decimals
    try:
        return to_scaled(text, decimals)
    except ValueError:
        return int((Decimal(text) * meta.q_scale).to_integral_value(ROUND_HALF_UP))


def parse_order(obj: Mapping[str, Any], meta: MarketMeta) -> OrderUpdate:
    """Order object (REST ``Order`` model or ``account_orders`` stream element)."""
    client_id = obj.get("client_order_index")
    if client_id in (None, ""):
        client_id = obj.get("client_order_id")
    return OrderUpdate(
        client_order_index=_int(client_id),
        order_index=_int(obj.get("order_index")),
        market_id=_int(obj.get("market_index", obj.get("market_id", -1))),
        is_ask=bool(obj.get("is_ask")),
        status=str(obj.get("status") or ""),
        filled=meta.size_to_int(str(obj.get("filled_base_amount") or "0")),
        filled_quote_q=quote_to_q(obj.get("filled_quote_amount"), meta),
        remaining=meta.size_to_int(str(obj.get("remaining_base_amount") or "0")),
        reduce_only=bool(obj.get("reduce_only")),
    )


def parse_trade(obj: Mapping[str, Any], account_index: int, meta: MarketMeta) -> TradeFill | None:
    """Trade object -> our fill, or None if this account is not a party to it."""
    maker_is_ask = bool(obj.get("is_maker_ask"))
    if _int(obj.get("ask_account_id")) == account_index:
        we_are_ask = True
        client_id = obj.get("ask_client_id_str") or obj.get("ask_client_id")
    elif _int(obj.get("bid_account_id")) == account_index:
        we_are_ask = False
        client_id = obj.get("bid_client_id_str") or obj.get("bid_client_id")
    else:
        return None
    is_taker = we_are_ask != maker_is_ask
    fee = obj.get("taker_fee") if is_taker else obj.get("maker_fee")  # omitted when zero
    return TradeFill(
        trade_id=_int(obj.get("trade_id_str") or obj.get("trade_id")),
        client_order_index=_int(client_id),
        market_id=_int(obj.get("market_id", -1)),
        size=meta.size_to_int(str(obj.get("size") or "0")),
        price=meta.price_to_int(str(obj.get("price") or "0")),
        is_taker=is_taker,
        fee_tick=_int(fee),
    )


def parse_imf(value: Any) -> int | None:
    """Position ``initial_margin_fraction`` (percent string such as "5.00") -> 1/10_000 units."""
    if value in (None, ""):
        return None
    return int((Decimal(str(value)) * 100).to_integral_value(ROUND_HALF_UP))


def parse_position(obj: Mapping[str, Any] | None, meta: MarketMeta, now_ns: int) -> ExchangePosition:
    """Position object -> :class:`ExchangePosition`. ``None`` means no entry, i.e. flat."""
    if obj is None:
        return ExchangePosition(0, 0, 0, 0, None, None, now_ns)
    size = meta.size_to_int(str(obj.get("position") or "0"))
    sign = _int(obj.get("sign"))
    if size < 0:
        size, sign = -size, -1
    signed = -size if sign < 0 else size
    avg = Decimal(str(obj.get("avg_entry_price") or "0"))
    avg_int = int((avg * meta.price_scale).to_integral_value(ROUND_HALF_UP))
    margin_mode = obj.get("margin_mode")
    return ExchangePosition(
        signed_size=signed,
        avg_entry_price=avg_int,
        open_orders=_int(obj.get("open_order_count")),
        pending_orders=_int(obj.get("pending_order_count")),
        imf=parse_imf(obj.get("initial_margin_fraction")),
        margin_mode=None if margin_mode is None else _int(margin_mode),
        seen_ns=now_ns,
    )


def find_market_position(positions: Any, market_id: int) -> Mapping[str, Any] | None:
    """Locate the position object of ``market_id`` in a list or a dict keyed by market index."""
    if isinstance(positions, Mapping):
        if "market_id" in positions:  # a single position object
            return positions if _int(positions.get("market_id")) == market_id else None
        direct = positions.get(str(market_id), positions.get(market_id))
        if isinstance(direct, Mapping):
            return direct
    for obj in iter_objects(positions):
        if "market_id" in obj and _int(obj.get("market_id")) == market_id:
            return obj
    return None


def parse_levels(levels: Any, meta: MarketMeta) -> list[tuple[int, int]]:
    """Order-book level list ``[{"price": "...", "size": "..."}]`` -> integer tuples."""
    price_decimals = meta.price_decimals
    size_decimals = meta.size_decimals
    return [(to_scaled(level["price"], price_decimals), to_scaled(level["size"], size_decimals)) for level in levels]
