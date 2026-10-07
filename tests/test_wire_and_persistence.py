"""Parsing of real-shaped Lighter payloads, and the journal round trip."""

from __future__ import annotations

import json
from pathlib import Path

from scalper.persistence import TRADE_COLUMNS, Journal
from scalper.precision import MarketMeta
from scalper.wire import (
    find_market_position,
    iter_objects,
    parse_imf,
    parse_levels,
    parse_order,
    parse_position,
    parse_trade,
    quote_to_q,
)

ACCOUNT = 316936

# Shape captured from the live mainnet ``trade/1`` channel (public data).
TRADE = {
    "trade_id": 33582975421,
    "trade_id_str": "33582975421",
    "type": "trade",
    "market_id": 1,
    "size": "0.00250",
    "price": "83691.1",
    "usd_amount": "209.227750",
    "ask_id": 562953498587041,
    "bid_id": 844421350816976,
    "ask_client_id": 179971268523011,
    "ask_client_id_str": "179971268523011",
    "bid_client_id": 1791365635551,
    "bid_client_id_str": "1791365635551",
    "ask_account_id": 27927,
    "bid_account_id": ACCOUNT,
    "is_maker_ask": True,
    "timestamp": 1791365635812,
    "maker_fee": 38,
}

ORDER = {
    "order_index": 844421350816976,
    "client_order_index": 1791365635551,
    "market_index": 1,
    "initial_base_amount": "0.00250",
    "remaining_base_amount": "0.00000",
    "filled_base_amount": "0.00250",
    "filled_quote_amount": "209.227750",
    "price": "83700.0",
    "is_ask": False,
    "type": "limit",
    "time_in_force": "immediate-or-cancel",
    "reduce_only": False,
    "status": "filled",
}

# Shape captured from ``GET /api/v1/account`` (public data of an unrelated account).
POSITION = {
    "market_id": 1,
    "symbol": "BTC",
    "initial_margin_fraction": "5.00",
    "open_order_count": 10,
    "pending_order_count": 0,
    "sign": -1,
    "position": "0.32909",
    "avg_entry_price": "83694.2",
    "margin_mode": 0,
}


def test_parse_trade_as_taker_buyer(meta: MarketMeta) -> None:
    fill = parse_trade(TRADE, ACCOUNT, meta)
    assert fill is not None
    assert fill.trade_id == 33582975421
    assert fill.client_order_index == 1791365635551
    assert fill.size == 250 and fill.price == 836911
    assert fill.is_taker  # the maker was the ask, we were the bid
    assert fill.fee_tick == 0  # taker_fee is omitted by Lighter when it is zero


def test_parse_trade_as_maker_and_unrelated(meta: MarketMeta) -> None:
    maker = parse_trade(TRADE, 27927, meta)
    assert maker is not None and not maker.is_taker and maker.fee_tick == 38
    assert maker.client_order_index == 179971268523011
    assert parse_trade(TRADE, 999, meta) is None


def test_parse_trade_with_taker_fee(meta: MarketMeta) -> None:
    fill = parse_trade({**TRADE, "taker_fee": 280}, ACCOUNT, meta)
    assert fill is not None and fill.fee_tick == 280


def test_parse_order(meta: MarketMeta) -> None:
    order = parse_order(ORDER, meta)
    assert order.client_order_index == 1791365635551
    assert order.market_id == 1
    assert order.status == "filled"
    assert order.filled == 250
    assert order.filled_quote_q == 209_227_750  # exactly 0.00250 * 83691.1 in quote units
    assert order.filled_quote_q == 250 * 836911
    assert order.remaining == 0 and not order.is_ask and not order.reduce_only


def test_parse_position_short(meta: MarketMeta) -> None:
    position = parse_position(POSITION, meta, 5)
    assert position.signed_size == -32909
    assert position.avg_entry_price == 836942
    assert position.imf == 500  # "5.00" percent -> 20x
    assert position.margin_mode == 0
    assert position.open_orders == 10 and position.seen_ns == 5


def test_parse_position_absent_means_flat(meta: MarketMeta) -> None:
    position = parse_position(None, meta, 0)
    assert position.signed_size == 0 and position.imf is None


def test_parse_position_tolerates_signed_strings(meta: MarketMeta) -> None:
    position = parse_position({**POSITION, "sign": 1, "position": "-0.00100"}, meta, 0)
    assert position.signed_size == -100


def test_parse_imf() -> None:
    assert parse_imf("4.00") == 400
    assert parse_imf("3.33") == 333
    assert parse_imf("2") == 200
    assert parse_imf(None) is None


def test_find_market_position_in_every_container_shape() -> None:
    other = {**POSITION, "market_id": 0}
    assert find_market_position([other, POSITION], 1) is POSITION
    assert find_market_position({"0": other, "1": POSITION}, 1) is POSITION
    assert find_market_position({"1": [POSITION]}, 1) is POSITION
    assert find_market_position(POSITION, 1) is POSITION
    assert find_market_position({"0": other}, 1) is None
    assert find_market_position([], 1) is None


def test_iter_objects_shapes() -> None:
    assert list(iter_objects([ORDER])) == [ORDER]
    assert list(iter_objects({"1": [ORDER, ORDER]})) == [ORDER, ORDER]
    assert list(iter_objects({"1": ORDER})) == [ORDER]
    assert list(iter_objects([])) == []
    assert list(iter_objects(None)) == []


def test_parse_levels_and_quote(meta: MarketMeta) -> None:
    levels = parse_levels([{"price": "83693.9", "size": "0.00238"}, {"price": "83694.3", "size": "0.00000"}], meta)
    assert levels == [(836939, 238), (836943, 0)]
    assert quote_to_q("209.227750", meta) == 209_227_750
    assert quote_to_q("209.2277505", meta) == 209_227_751  # more precision than expected: rounded, not rejected
    assert quote_to_q(None, meta) == 0


# ---------------------------------------------------------------- persistence


def test_journal_round_trip(tmp_path: Path) -> None:
    journal = Journal(tmp_path, write_csv=True)
    assert journal.load_state() == {}
    journal.start()
    journal.save_state({"state": "OPEN_LONG", "last_client_order_index": 42})
    journal.save_state({"state": "FLAT", "last_client_order_index": 43})
    trade = {column: None for column in TRADE_COLUMNS}
    trade.update(
        trade_id="t1",
        side="LONG",
        gross_pnl_usd=0.05,
        fees_usd=0.01,
        realized_pnl_usd=0.04,
        holding_ms=812.5,
        signal_components={"trade_flow": 0.9},
        trade_date="2026-10-07",
    )
    journal.record_trade(trade)
    journal.record_trade({**trade, "trade_id": "t2", "realized_pnl_usd": -0.02, "gross_pnl_usd": -0.02, "fees_usd": 0})
    journal.record_event("RECOVERY_START", {"reason": "test"})
    journal.write_status({"state": "FLAT", "bid": 83691.1})
    journal.write_daily_summary("2026-10-07", {"trades": 2, "net_pnl_usd": 0.02})
    journal.stop()

    reopened = Journal(tmp_path)
    assert reopened.load_state() == {"state": "FLAT", "last_client_order_index": 43}  # latest snapshot wins
    assert sorted(reopened.load_day_totals("2026-10-07")) == [(-0.02, 0.0, -0.02, 812.5), (0.05, 0.01, 0.04, 812.5)]
    assert reopened.load_day_totals("2026-10-08") == []
    assert Journal.read_status(tmp_path) == {"state": "FLAT", "bid": 83691.1}
    assert json.loads((tmp_path / "daily" / "2026-10-07.json").read_text())["trades"] == 2
    csv_lines = (tmp_path / "trades.csv").read_text().strip().splitlines()
    assert csv_lines[0].split(",")[0] == "trade_id" and len(csv_lines) == 3


def test_read_status_missing_or_corrupt(tmp_path: Path) -> None:
    assert Journal.read_status(tmp_path) is None
    (tmp_path / "status.json").write_text("{not json", encoding="utf-8")
    assert Journal.read_status(tmp_path) is None
