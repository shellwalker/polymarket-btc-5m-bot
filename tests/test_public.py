import asyncio
import json
from decimal import Decimal

import httpx
import pytest

from btc5m.config import Settings
from btc5m.domain import Book
from btc5m.public import MarketStream, PublicAPI


@pytest.mark.asyncio
async def test_get_retries_and_never_accepts_unapproved_path():
    calls = []

    async def handle(request):
        calls.append(request)
        return httpx.Response(503 if len(calls) < 2 else 200, json=123)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        api = PublicAPI(Settings(retry_initial=0.001), client)
        assert await api.get("clob", "/time") == 123
        with pytest.raises(ValueError):
            await api.get("clob", "/orders")
    assert len(calls) == 2
    assert all(r.method == "GET" for r in calls)


@pytest.mark.asyncio
async def test_ws_rest_barrier_delta_tick_disconnect_reconnect():
    stop = asyncio.Event()
    connections = 0
    frames = []
    books = {"u": Book("u", Decimal(".01"))}
    now = 1000.0
    snapshot = {
        "event_type": "book",
        "asset_id": "u",
        "timestamp": "1000000",
        "bids": [{"price": ".4", "size": "20"}],
        "asks": [{"price": ".5", "size": "20"}],
    }

    class API:
        async def snapshot(self, token):
            frames.append("rest")
            return snapshot

    class WS:
        def __init__(self, count):
            self.count = count
            self.queue = asyncio.Queue()
            self.queue.put_nowait(json.dumps([snapshot]))
            if count == 1:
                self.queue.put_nowait(
                    json.dumps(
                        {
                            "event_type": "price_change",
                            "timestamp": "1000001",
                            "price_changes": [
                                {"asset_id": "u", "side": "SELL", "price": ".5", "size": "9"}
                            ],
                        }
                    )
                )
                self.queue.put_nowait(
                    json.dumps(
                        {
                            "event_type": "tick_size_change",
                            "asset_id": "u",
                            "new_tick_size": ".001",
                            "timestamp": "1000002",
                        }
                    )
                )
                self.queue.put_nowait(None)

        async def send(self, data):
            frames.append(data)
            if data == "PING":
                self.queue.put_nowait("PONG")

        async def recv(self):
            value = await self.queue.get()
            if value is None:
                raise OSError("test disconnect")
            return value

    class Context:
        async def __aenter__(self):
            nonlocal connections
            connections += 1
            return WS(connections)

        async def __aexit__(self, *args):
            return None

    def connect(*args, **kwargs):
        return Context()

    ready = []

    def update():
        ready.append(books["u"].connected)
        if connections == 2 and books["u"].connected:
            stop.set()

    stream = MarketStream(
        Settings(retry_initial=0.001, heartbeat_seconds=0.01),
        API(),
        books,
        update,
        connect=connect,
        clock=lambda: now,
    )
    await asyncio.wait_for(stream.run(stop), 2)
    assert connections == 2
    assert sum("assets_ids" in x for x in frames) == 2
    assert frames.count("rest") == 2
    assert books["u"].tick == Decimal(".001")
    assert False in ready and True in ready
    assert not books["u"].connected


@pytest.mark.asyncio
async def test_pong_timeout_disconnects_even_if_market_frames_arrive():
    stop = asyncio.Event()
    books = {"u": Book("u", Decimal(".01"))}

    class API:
        async def snapshot(self, token):
            return {"asset_id": "u", "timestamp": "1000000", "bids": [], "asks": []}

    class WS:
        async def send(self, value):
            pass

        async def recv(self):
            await asyncio.sleep(0.001)
            return json.dumps({"event_type": "last_trade_price", "asset_id": "u"})

    class Context:
        async def __aenter__(self):
            return WS()

        async def __aexit__(self, *args):
            stop.set()

    stream = MarketStream(
        Settings(heartbeat_seconds=0.01, pong_timeout=0.02),
        API(),
        books,
        lambda: None,
        connect=lambda *a, **k: Context(),
        clock=lambda: 1000.0,
    )
    await asyncio.wait_for(stream.run(stop), 1)
    assert not books["u"].connected
    assert stream.reconnects == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "patch",
    [
        {"minimum_order_size": "NaN"},
        {"minimum_order_size": 0},
        {"minimum_order_size": -1},
        {"minimum_tick_size": ".005"},
        {"accepting_orders": "false"},
        {"closed": None},
        {"active": False},
        {"enable_order_book": False},
    ],
)
async def test_discovery_rejects_unknown_clob_constraints(patch):
    from test_domain import event

    async def handle(request):
        if request.url.host == "gamma-api.polymarket.com":
            slug = request.url.params["slug"]
            raw = event()
            if raw["slug"] != slug:
                return httpx.Response(200, json=[])
            raw["markets"][0]["conditionId"] = "0xabc"
            return httpx.Response(200, json=[raw])
        detail = {
            "condition_id": "0xabc",
            "minimum_order_size": 5,
            "minimum_tick_size": ".01",
            "accepting_orders": True,
            "closed": False,
            "active": True,
            "enable_order_book": True,
        }
        detail.update(patch)
        return httpx.Response(200, json=detail)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        current, _ = await PublicAPI(Settings(), client).discover(1800000001)
    assert current is None


@pytest.mark.asyncio
async def test_tick_coarsening_does_not_restore_invalid_book_readiness():
    stop = asyncio.Event()
    book = Book("u", Decimal(".001"))
    after_change = [False]
    observed = []
    snapshot = {
        "event_type": "book",
        "asset_id": "u",
        "timestamp": "1000000",
        "tick_size": ".001",
        "bids": [{"price": ".499", "size": "20"}],
        "asks": [{"price": ".501", "size": "20"}],
    }

    class API:
        async def snapshot(self, token):
            return snapshot

    class WS:
        def __init__(self):
            self.n = 0

        async def send(self, raw):
            pass

        async def recv(self):
            self.n += 1
            if self.n == 1:
                return json.dumps(snapshot)
            if self.n == 2:
                after_change[0] = True
                return json.dumps(
                    {"event_type": "tick_size_change", "asset_id": "u", "new_tick_size": ".01"}
                )
            stop.set()
            return "PONG"

    class Context:
        async def __aenter__(self):
            return WS()

        async def __aexit__(self, *args):
            stop.set()

    def update():
        if after_change[0]:
            observed.append(book.healthy(1000.0, 5))

    stream = MarketStream(
        Settings(),
        API(),
        {"u": book},
        update,
        connect=lambda *a, **k: Context(),
        clock=lambda: 1000.0,
    )
    await asyncio.wait_for(stream.run(stop), 1)
    assert observed and not any(observed)
    assert stream.reconnects == 1
