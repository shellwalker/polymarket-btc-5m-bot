"""Full offline window replay through REST adapter, websocket and durable ledger."""

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from btc5m.config import Settings
from btc5m.ledger import Ledger
from btc5m.public import PublicAPI
from btc5m.service import Service


@pytest.mark.asyncio
@pytest.mark.parametrize("auto_recovery", [False, True])
async def test_mock_rest_ws_reconnect_settlement_and_restart(tmp_path, auto_recovery):
    from test_domain import event

    now = [1800000001.0]
    stop = asyncio.Event()
    connections = []
    rest_snapshots = []

    def raw_market(start):
        raw = event(start)
        m = raw["markets"][0]
        m["conditionId"] = "0xabc" if start == 1800000000 else "0xdef"
        m["endDate"] = datetime.fromtimestamp(start + 300, UTC).isoformat()
        m["clobTokenIds"] = json.dumps(["u", "d"] if start == 1800000000 else ["u2", "d2"])
        if now[0] >= start + 300:
            m.update(
                closed=True,
                acceptingOrders=False,
                umaResolutionStatus="resolved",
                outcomePrices='["1","0"]',
            )
        return raw

    def snap(token, ask=".46"):
        return {
            "event_type": "book",
            "asset_id": token,
            "timestamp": str(int(now[0] * 1000)),
            "tick_size": ".01",
            "bids": [{"price": str(Decimal(ask) - Decimal(".01")), "size": "30"}],
            "asks": [{"price": ask, "size": "30"}],
        }

    async def handle(request):
        assert request.method == "GET"
        if request.url.path == "/api/geoblock":
            return httpx.Response(200, json={"blocked": True, "country": "US", "region": "CA"})
        if request.url.path == "/events":
            start = int(request.url.params["slug"].rsplit("-", 1)[1])
            return httpx.Response(200, json=[raw_market(start)])
        if request.url.path.startswith("/markets/"):
            return httpx.Response(
                200,
                json={
                    "condition_id": request.url.path.rsplit("/", 1)[1],
                    "active": True,
                    "closed": False,
                    "accepting_orders": True,
                    "enable_order_book": True,
                    "minimum_order_size": 5,
                    "minimum_tick_size": ".01",
                },
            )
        token = request.url.params["token_id"]
        rest_snapshots.append(token)
        return httpx.Response(200, json=snap(token))

    class WS:
        def __init__(self):
            self.n = 0

        async def send(self, raw):
            if raw != "PING":
                self.tokens = json.loads(raw)["assets_ids"]

        async def recv(self):
            await asyncio.sleep(0.01)
            self.n += 1
            now[0] += 0.01
            if self.n == 1:
                return json.dumps([snap(t) for t in self.tokens])
            if self.n == 2:
                return json.dumps([snap(t, ".52") for t in self.tokens])
            if len(connections) == 1:
                raise OSError("recorded disconnect after committed fill")
            if self.tokens == ["u", "d"]:
                now[0] = 1800000301.0
            else:
                stop.set()
            return "PONG"

    class Context:
        async def __aenter__(self):
            connections.append(1)
            return WS()

        async def __aexit__(self, *args):
            pass

    settings = Settings(
        runtime_dir=tmp_path,
        market_auto_recovery=auto_recovery,
        market_recovery_snapshots=2,
        market_recovery_cooldown=0.001,
        discovery_seconds=0.01,
        resolution_seconds=0.01,
        retry_initial=0.001,
        signal_threshold=Decimal(".02"),
        min_momentum=Decimal(".02"),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        api = PublicAPI(settings, client)
        service = Service(settings, api, connect=lambda *a, **k: Context(), clock=lambda: now[0])
        await asyncio.wait_for(service.run(stop), 3)
    first = Ledger.read_summary(tmp_path / "ledger.sqlite3")
    assert first["fills"] == (2 if auto_recovery else 1)
    assert first["settlements"] == 1
    assert first["pending"] == (1 if auto_recovery else 0)
    assert first["circuit"] == ("" if auto_recovery else "stale_or_disconnected_book")
    if auto_recovery:
        assert first["event_counts"]["circuit_reset"] >= 1
    assert len(connections) >= 3
    assert rest_snapshots.count("u") >= 2
    # Restart against same DB with STOP requires no network and preserves all counters.
    (tmp_path / "STOP").touch()

    class NoNetwork:
        async def get(self, *args, **kwargs):
            raise AssertionError("network forbidden")

    await Service(settings, NoNetwork()).run(asyncio.Event())
    second = Ledger.read_summary(tmp_path / "ledger.sqlite3")
    assert second["fills"] == first["fills"] and second["cash"] == first["cash"]
    assert second["settlements"] == first["settlements"] and second["circuit"] == first["circuit"]
