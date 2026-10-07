"""Price/quantity rounding and integer conversions."""

from __future__ import annotations

from decimal import Decimal

import pytest

from scalper.precision import (
    MBPS_DENOM,
    MarketMeta,
    bps_to_mbps,
    ceil_div,
    fee_q,
    price_minus_mbps,
    price_plus_mbps,
    to_scaled,
)


@pytest.mark.parametrize(
    ("text", "decimals", "expected"),
    [
        ("83693.9", 1, 836939),
        ("0.00238", 5, 238),
        ("10.000000", 6, 10_000_000),
        ("5", 2, 500),
        ("0", 5, 0),
        ("-0.00250", 5, -250),
        ("83693.90", 1, 836939),  # trailing zeros beyond precision are fine
        (".5", 1, 5),
    ],
)
def test_to_scaled_exact(text: str, decimals: int, expected: int) -> None:
    assert to_scaled(text, decimals) == expected


def test_to_scaled_rejects_excess_precision() -> None:
    with pytest.raises(ValueError):
        to_scaled("83693.95", 1)


def test_bps_to_mbps() -> None:
    assert bps_to_mbps(Decimal("1")) == 1000
    assert bps_to_mbps(Decimal("0.5")) == 500
    assert bps_to_mbps(Decimal("0.001")) == 1


def test_price_rounding_is_never_in_our_favour() -> None:
    price = 836939
    # A seller's worst price rounds down, a buyer's worst price rounds up.
    assert price_minus_mbps(price, 2000) == 836771  # 836939 * (1 - 0.0002) = 836771.61
    assert price_plus_mbps(price, 2000) == 837107  # 836939 * (1 + 0.0002) = 837106.39
    assert price_minus_mbps(price, 0) == price
    assert price_plus_mbps(price, 0) == price
    assert price_minus_mbps(price, MBPS_DENOM) == 0


def test_ceil_div() -> None:
    assert ceil_div(10, 5) == 2
    assert ceil_div(11, 5) == 3
    assert ceil_div(0, 5) == 0


def test_fee_rounds_up_and_handles_zero() -> None:
    assert fee_q(251_081_700, 0) == 0
    assert fee_q(0, 200) == 0
    # 2 bps taker fee = 200 ticks: 251081700 * 200 / 1e6 = 50216.34 -> 50217
    assert fee_q(251_081_700, 200) == 50_217


def test_market_meta_scales(meta: MarketMeta) -> None:
    assert meta.price_scale == 10
    assert meta.size_scale == 100_000
    assert meta.q_scale == 1_000_000
    assert meta.max_leverage == Decimal(50)
    assert meta.usd_to_q(Decimal("250")) == 250_000_000
    assert meta.q_to_usd(251_081_700) == pytest.approx(251.0817)
    assert meta.fmt_price(836939) == "83693.9"
    assert meta.fmt_size(300) == "0.00300"


def test_quantity_rounds_down_to_size_step(meta: MarketMeta) -> None:
    # 250 USD at 83693.9 = 0.0029870...; never round up into more notional than configured.
    size = meta.size_for_notional(250_000_000, 836939)
    assert size == 298
    assert size * 836939 <= 250_000_000
    assert (size + 1) * 836939 > 250_000_000
    assert meta.size_for_notional(250_000_000, 0) == 0


def test_minimum_order_size_respects_both_limits(meta: MarketMeta) -> None:
    # 10 USD minimum notional dominates the 0.00007 BTC minimum at this price.
    assert meta.min_size_at(836939) == 12
    assert meta.min_size_at(836939) * 836939 >= meta.min_quote_q
    # At a very high price the base minimum dominates.
    assert meta.min_size_at(10_000_000_0) == 7
