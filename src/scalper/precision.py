"""Exchange-native integer arithmetic.

Lighter transmits prices and sizes as decimal strings and expects integers
scaled by the market's ``price_decimals`` / ``size_decimals`` in signed
transactions. Everything on the hot path stays in those integers:

* ``price``  -> int, real price  * 10**price_decimals
* ``size``   -> int, real size   * 10**size_decimals
* ``quote``  -> int ("q" suffix), price_int * size_int, i.e. USD * 10**(price_decimals + size_decimals)

Slippage/spread limits are integers in milli-basis-points ("mbps"): 1 bps = 1000 mbps,
100% = 10_000_000 mbps. Fees are integers in fee ticks: 1 tick = 1e-6 of notional.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

MBPS_PER_BPS = 1000
MBPS_DENOM = 10_000_000  # 100% expressed in milli-bps

# Fee tick denominator. Verified against live mainnet trades: a liquidation fee of
# "1.0000" (percent) is reported as taker_fee=10000, so one tick is 1e-6 of notional.
FEE_TICK_DENOM = 1_000_000

# Initial margin fraction denominator (leverage 25x -> fraction 400).
IMF_DENOM = 10_000


def to_scaled(value: str, decimals: int) -> int:
    """Convert a decimal string to an integer scaled by ``10**decimals`` exactly.

    Raises ``ValueError`` if the string carries more non-zero precision than
    ``decimals`` allows, rather than rounding silently.
    """
    text = value.strip()
    negative = text.startswith("-")
    if negative or text.startswith("+"):
        text = text[1:]
    whole, _, frac = text.partition(".")
    if len(frac) > decimals:
        if frac[decimals:].strip("0"):
            raise ValueError(f"{value!r} has more than {decimals} decimals")
        frac = frac[:decimals]
    else:
        frac = frac + "0" * (decimals - len(frac))
    result = int((whole or "0") + frac) if (whole or frac) else 0
    return -result if negative else result


def bps_to_mbps(bps: Decimal) -> int:
    """Convert a basis-point Decimal to integer milli-bps (exact to 0.001 bps)."""
    return int((bps * MBPS_PER_BPS).to_integral_value())


def price_minus_mbps(price: int, mbps: int) -> int:
    """``price`` lowered by ``mbps``, rounded down (never better than requested for a seller)."""
    return price * (MBPS_DENOM - mbps) // MBPS_DENOM


def price_plus_mbps(price: int, mbps: int) -> int:
    """``price`` raised by ``mbps``, rounded up (never better than requested for a buyer)."""
    return -((-price * (MBPS_DENOM + mbps)) // MBPS_DENOM)


def ceil_div(numerator: int, denominator: int) -> int:
    """Integer division rounded toward positive infinity."""
    return -((-numerator) // denominator)


def fee_q(value_q: int, fee_tick: int) -> int:
    """Fee on a notional, rounded up so costs are never understated."""
    if fee_tick <= 0 or value_q <= 0:
        return 0
    return ceil_div(value_q * fee_tick, FEE_TICK_DENOM)


@dataclass(frozen=True, slots=True)
class MarketMeta:
    """Immutable BTC market metadata, loaded from ``orderBookDetails`` at startup."""

    symbol: str
    market_id: int
    price_decimals: int
    size_decimals: int
    min_base: int  # minimum order size, size units
    min_quote_q: int  # minimum order notional, quote units
    min_imf: int  # smallest allowed initial margin fraction (1/10_000) => max leverage
    default_imf: int
    maintenance_imf: int
    status: str

    @property
    def price_scale(self) -> int:
        return int(10**self.price_decimals)

    @property
    def size_scale(self) -> int:
        return int(10**self.size_decimals)

    @property
    def q_scale(self) -> int:
        return int(10 ** (self.price_decimals + self.size_decimals))

    @property
    def max_leverage(self) -> Decimal:
        return Decimal(IMF_DENOM) / Decimal(self.min_imf)

    def price_to_int(self, value: str) -> int:
        return to_scaled(value, self.price_decimals)

    def size_to_int(self, value: str) -> int:
        return to_scaled(value, self.size_decimals)

    def usd_to_q(self, usd: Decimal) -> int:
        """USD amount -> quote units, rounded down."""
        return int(usd * self.q_scale)

    def q_to_usd(self, q: int) -> float:
        return q / self.q_scale

    def price_to_float(self, price: int) -> float:
        return price / self.price_scale

    def size_to_float(self, size: int) -> float:
        return size / self.size_scale

    def fmt_price(self, price: int) -> str:
        return f"{price / self.price_scale:.{self.price_decimals}f}"

    def fmt_size(self, size: int) -> str:
        return f"{size / self.size_scale:.{self.size_decimals}f}"

    def fmt_usd(self, q: int) -> str:
        return f"{q / self.q_scale:.4f}"

    def size_for_notional(self, notional_q: int, price: int) -> int:
        """Largest size (size units, rounded down) whose notional at ``price`` fits ``notional_q``."""
        if price <= 0:
            return 0
        return notional_q // price

    def min_size_at(self, price: int) -> int:
        """Smallest valid order size at ``price`` honouring both exchange minimums."""
        if price <= 0:
            return self.min_base
        return max(self.min_base, ceil_div(self.min_quote_q, price))
