from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace

import pytest
from test_domain import event, make_book

from btc5m.config import Settings
from btc5m.domain import Market
from btc5m.ledger import Ledger
from btc5m.public import MarketStream
from btc5m.service import Service


class OfflineConnect:
    async def __aenter__(self):
        raise RuntimeError("offline test: unexpected reconnect")

    async def __aexit__(self, *args):
        pass


def test_whole_price_change_is_atomic_and_true_cross_still_rejected():
    book = make_book(bid=".49", ask=".51")
    stream = MarketStream(Settings(), None, {"up": book}, lambda: None, clock=lambda: 1800000002.0)
    message = {
        "event_type": "price_change",
        "timestamp": "1800000002000",
        "price_changes": [
            {"asset_id": "up", "side": "BUY", "price": ".52", "size": "10"},
            {"asset_id": "up", "side": "SELL", "price": ".51", "size": "0"},
            {"asset_id": "up", "side": "SELL", "price": ".54", "size": "10"},
        ],
    }
    stream.apply_price_change(message, set())
    assert book.healthy(1800000002, 5)
    assert book.best_bid == D(".52") and book.best_ask == D(".54")
    before = (dict(book.bids), dict(book.asks))
    with pytest.raises(ValueError, match="crossed"):
        stream.apply_price_change(
            {
                "timestamp": "1800000003000",
                "price_changes": [{"asset_id": "up", "side": "BUY", "price": ".55", "size": "10"}],
            },
            set(),
        )
    assert (book.bids, book.asks) == before
    assert not book.connected


def test_atomicity_across_tokens_and_partial_initial_barrier():
    up, down = make_book(), make_book("down")
    stream = MarketStream(
        Settings(), None, {"up": up, "down": down}, lambda: None, clock=lambda: 1800000002.0
    )
    before = dict(up.bids)
    with pytest.raises(ValueError):
        stream.apply_price_change(
            {
                "timestamp": "1800000002000",
                "price_changes": [
                    {"asset_id": "up", "side": "BUY", "price": ".48", "size": "100"},
                    {"asset_id": "down", "side": "BUY", "price": ".60", "size": "100"},
                ],
            },
            set(),
        )
    assert up.bids == before
    assert not up.connected and not down.connected
    # A fully initialized token can receive deltas while the other initial dump is pending.
    up = make_book()
    up.connected = False
    stream.books = {"up": up, "down": down}
    stream.apply_price_change(
        {
            "timestamp": "1800000002000",
            "price_changes": [{"asset_id": "up", "side": "BUY", "price": ".48", "size": "100"}],
        },
        {"down"},
    )
    assert up.bids[D(".48")] == 100
    assert not up.connected


@pytest.mark.asyncio
async def test_same_window_metadata_does_not_restart_stream_or_anchor(tmp_path):
    service = Service(
        Settings(runtime_dir=tmp_path), None, connect=lambda *a, **k: OfflineConnect()
    )
    market = Market.from_event(event())
    service.market = market
    service.known_market = market
    service.books = {"up": make_book(), "down": make_book("down")}
    service.stream = SimpleNamespace(ready=True, reconnects=3)
    service.strategy.anchors[market.slug] = D(".50")
    service.ever_ready = True
    previous_stream = service.stream
    for tick in [".001", ".01", ".001"]:
        await service.switch(replace(market, tick=D(tick), min_size=D("6")))
    assert service.stream is previous_stream
    assert service.books["up"].tick == D(
        ".01"
    )  # WS/snapshot owns dynamic tick, not cached metadata.
    assert service.market.min_size == 6
    assert service.strategy.anchors[market.slug] == D(".50")
    assert service.ever_ready


@pytest.mark.asyncio
async def test_same_slug_identity_change_fails_closed(tmp_path):
    service = Service(
        Settings(runtime_dir=tmp_path), None, connect=lambda *a, **k: OfflineConnect()
    )
    service.ledger = Ledger(tmp_path / "ledger.sqlite3", service.settings)
    market = Market.from_event(event())
    service.market = service.known_market = market
    with pytest.raises(ValueError, match="identity"):
        await service.switch(replace(market, condition="other"))
    assert service.market is None
    assert service.ledger.summary()["circuit"] == "market_identity_changed"
    assert service.ledger.summary()["fills"] == 0
    service.ledger.close()


def test_real_public_delta_removes_implicitly_consumed_levels():
    import json
    from pathlib import Path

    from btc5m.domain import Book

    fixture = json.loads(
        (Path(__file__).parent / "fixtures/real_ws_price_change_2026-11-07.json").read_text()
    )
    books = {}
    for raw in fixture["snapshots"]:
        book = Book(raw["asset_id"], D(raw["tick_size"]))
        book.snapshot(raw, float(raw["timestamp"]) / 1000)
        books[book.asset_id] = book
    stamp = float(fixture["message"]["timestamp"]) / 1000
    stream = MarketStream(Settings(), None, books, lambda: None, clock=lambda: stamp)
    stream.apply_price_change(fixture["message"], set())
    for change in fixture["message"]["price_changes"]:
        book = books[change["asset_id"]]
        assert book.best_bid == D(change["best_bid"])
        assert book.best_ask == D(change["best_ask"])
        assert book.healthy(stamp, 5)


def test_advertised_top_never_fabricates_missing_liquidity():
    book = make_book()
    stream = MarketStream(Settings(), None, {"up": book}, lambda: None, clock=lambda: 1800000002.0)
    with pytest.raises(ValueError, match="top"):
        stream.apply_price_change(
            {
                "timestamp": "1800000002000",
                "price_changes": [
                    {
                        "asset_id": "up",
                        "side": "BUY",
                        "price": ".48",
                        "size": "100",
                        "best_bid": ".49",
                        "best_ask": ".52",
                    }
                ],
            },
            set(),
        )
    assert not book.connected
    assert D(".52") not in book.asks


@pytest.mark.asyncio
async def test_rest_resync_precedes_subscription_and_ws_owns_ordered_baseline():
    import asyncio
    import json

    from btc5m.domain import Book

    stop = asyncio.Event()
    actions = []
    book = Book("up", D(".01"))

    def snapshot(stamp, bid, ask):
        return {
            "asset_id": "up",
            "event_type": "book",
            "timestamp": stamp,
            "bids": [{"price": bid, "size": "10"}],
            "asks": [{"price": ask, "size": "10"}],
        }

    class API:
        async def snapshot(self, token):
            actions.append("rest")
            return snapshot("1000002", ".54", ".56")

    class WS:
        def __init__(self):
            self.count = 0

        async def send(self, raw):
            actions.append("subscribe")
            assert actions == ["rest", "subscribe"]

        async def recv(self):
            self.count += 1
            if self.count == 1:
                return json.dumps(snapshot("1000001", ".49", ".51"))
            await stop.wait()
            return "PONG"

    class Connect:
        async def __aenter__(self):
            return WS()

        async def __aexit__(self, *args):
            pass

    observed = []
    stream = None

    def update():
        if stream.ready:
            observed.append(book.best_bid)
            stop.set()

    stream = MarketStream(
        Settings(),
        API(),
        {"up": book},
        update,
        clock=lambda: 1000.003,
        connect=lambda *a, **k: Connect(),
    )
    await asyncio.wait_for(stream.run(stop), 1)
    assert observed == [D(".49")]
    assert stream.reconnects == 0
    assert stream.last_gap_seconds is not None


@pytest.mark.asyncio
async def test_reconnect_total_survives_market_switch(tmp_path):
    import json

    service = Service(Settings(runtime_dir=tmp_path), None)
    service.market = Market.from_event(event())
    service.stream = SimpleNamespace(ready=False, reconnects=7)
    await service.switch(None)
    service.health()
    assert json.loads((tmp_path / "health.json").read_text())["reconnects"] == 7
