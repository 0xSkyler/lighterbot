"""Order-book maintenance, sequence checking, ticker overlay and spread."""

from __future__ import annotations

from scalper.orderbook import OrderBook, spread_mbps

BIDS = [(1000, 5), (999, 7), (998, 9)]
ASKS = [(1001, 4), (1002, 6), (1003, 8)]


def make_book() -> OrderBook:
    book = OrderBook()
    book.apply_snapshot(BIDS, ASKS, nonce=10)
    return book


def test_snapshot_sorts_and_validates() -> None:
    book = OrderBook()
    assert not book.valid
    book.apply_snapshot(reversed(BIDS), reversed(ASKS), nonce=10)
    assert book.valid
    assert book.best_bid() == 1000
    assert book.best_ask() == 1001
    assert book.top_bids(2) == [(1000, 5), (999, 7)]
    assert book.top_asks(2) == [(1001, 4), (1002, 6)]
    assert book.level_counts() == (3, 3)


def test_snapshot_ignores_zero_sizes_and_rejects_empty_side() -> None:
    book = OrderBook()
    book.apply_snapshot([(1000, 0)], ASKS, nonce=1)
    assert not book.valid


def test_delta_updates_inserts_and_removes_levels() -> None:
    book = make_book()
    assert book.apply_delta([(1000, 0), (997, 3)], [(1001, 10), (1004, 2)], begin_nonce=10, nonce=11)
    assert book.top_bids(10) == [(999, 7), (998, 9), (997, 3)]
    assert book.top_asks(10) == [(1001, 10), (1002, 6), (1003, 8), (1004, 2)]
    assert book.nonce == 11


def test_delta_removing_unknown_level_is_harmless() -> None:
    book = make_book()
    assert book.apply_delta([(500, 0)], [], begin_nonce=10, nonce=11)
    assert book.top_bids(10) == BIDS


def test_sequence_gap_invalidates_book() -> None:
    book = make_book()
    assert not book.apply_delta([(1000, 1)], [], begin_nonce=9, nonce=12)
    assert not book.valid
    # Nothing is applied to an invalid book until a fresh snapshot arrives.
    assert not book.apply_delta([(1000, 1)], [], begin_nonce=12, nonce=13)
    book.apply_snapshot(BIDS, ASKS, nonce=20)
    assert book.valid


def test_crossed_book_is_rejected() -> None:
    book = make_book()
    assert not book.apply_delta([(1001, 5)], [], begin_nonce=10, nonce=11)
    assert not book.valid


def test_version_changes_on_every_visible_update() -> None:
    book = make_book()
    v0 = book.version
    book.apply_delta([(1000, 6)], [], 10, 11)
    assert book.version == v0 + 1


def test_ticker_overlay_hides_levels_proven_gone() -> None:
    book = make_book()
    # Ticker (newer nonce) says the best bid dropped to 999 and the best ask rose to 1002.
    assert book.apply_bbo(999, 2, 1002, 3, nonce=15)
    assert book.best_bid() == 999
    assert book.best_ask() == 1002
    assert book.top_bids(3) == [(999, 2), (998, 9)]
    assert book.top_asks(3) == [(1002, 3), (1003, 8)]


def test_ticker_overlay_adds_better_levels() -> None:
    book = make_book()
    # 1000.5-style improvement expressed in integer ticks: a new bid above the book's best.
    book.apply_snapshot([(1000, 5)], [(1010, 4)], nonce=10)
    assert book.apply_bbo(1004, 1, 1006, 2, nonce=11)
    assert book.top_bids(3) == [(1004, 1), (1000, 5)]
    assert book.top_asks(3) == [(1006, 2), (1010, 4)]


def test_ticker_overlay_is_non_destructive_and_cleared_by_newer_delta() -> None:
    book = make_book()
    book.apply_bbo(999, 2, 1002, 3, nonce=15)
    # A book delta that has caught up (nonce >= overlay) restores the book as the authority.
    assert book.apply_delta([], [], begin_nonce=10, nonce=16)
    assert book.best_bid() == 1000
    assert book.top_bids(3) == BIDS
    assert book.top_asks(3) == ASKS


def test_ticker_overlay_survives_older_delta() -> None:
    book = make_book()
    book.apply_bbo(999, 2, 1002, 3, nonce=15)
    assert book.apply_delta([(998, 1)], [], begin_nonce=10, nonce=12)
    assert book.best_bid() == 999  # overlay (nonce 15) is still newer than the book (nonce 12)
    assert book.top_bids(3) == [(999, 2), (998, 1)]


def test_stale_or_invalid_ticker_is_ignored() -> None:
    book = make_book()
    assert not book.apply_bbo(999, 2, 1002, 3, nonce=10)  # not newer than the book
    assert book.apply_bbo(999, 2, 1002, 3, nonce=12)
    assert not book.apply_bbo(998, 2, 1003, 3, nonce=11)  # older than the current overlay
    assert not book.apply_bbo(1002, 2, 1002, 3, nonce=20)  # crossed quote
    assert book.best_bid() == 999


def test_invalidate_drops_overlay() -> None:
    book = make_book()
    book.apply_bbo(999, 2, 1002, 3, nonce=15)
    book.invalidate()
    assert not book.valid
    book.apply_snapshot(BIDS, ASKS, nonce=30)
    assert book.best_bid() == 1000


def test_memory_is_bounded() -> None:
    book = OrderBook(max_levels_per_side=3)
    book.apply_snapshot([(100 - i, 1) for i in range(3)], [(101 + i, 1) for i in range(3)], nonce=1)
    assert book.apply_delta([(90, 1), (91, 1)], [(110, 1), (111, 1)], 1, 2)
    assert book.level_counts() == (3, 3)
    assert book.top_bids(5) == [(100, 1), (99, 1), (98, 1)]  # the furthest levels were dropped
    assert book.top_asks(5) == [(101, 1), (102, 1), (103, 1)]


def test_spread() -> None:
    # 83691.1 / 83693.9 -> 2.8 USD on a 83692.5 mid = 0.3346 bps
    assert spread_mbps(836911, 836939) == 334
    assert spread_mbps(0, 836939) > 10**9
    assert spread_mbps(836939, 836939) > 10**9
