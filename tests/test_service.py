import asyncio
import json
import os
from decimal import Decimal

import pytest

from btc5m.config import Settings
from btc5m.domain import Market
from btc5m.service import RuntimeLock, Service, read_status


def test_exclusive_lock_and_stale_pid_is_not_authority(tmp_path):
    with RuntimeLock(tmp_path):
        assert json.loads((tmp_path / "pid.json").read_text())["pid"] == os.getpid()
        with pytest.raises(RuntimeError, match="already running"):
            with RuntimeLock(tmp_path):
                pass
    (tmp_path / "pid.json").write_text('{"pid":999999}')
    assert read_status(tmp_path)["running"] is False
    with RuntimeLock(tmp_path):
        pass


@pytest.mark.asyncio
async def test_stop_file_startup_no_network(tmp_path):
    (tmp_path / "STOP").touch()

    class API:
        async def get(self, *args, **kwargs):
            raise AssertionError("must not call network")

    service = Service(Settings(runtime_dir=tmp_path), API())
    await service.run(asyncio.Event())
    assert not read_status(tmp_path)["running"]


@pytest.mark.asyncio
async def test_rolling_windows_resolution_restart_no_duplicate(tmp_path):
    from btc5m.ledger import Ledger

    stop = asyncio.Event()
    now = [900.0]
    market = Market(
        "btc-updown-5m-900",
        900,
        1200,
        "0xabc",
        {"Up": "u", "Down": "d"},
        Decimal("5"),
        Decimal(".01"),
    )
    following = Market(
        "btc-updown-5m-1200",
        1200,
        1500,
        "0xdef",
        {"Up": "u2", "Down": "d2"},
        Decimal("5"),
        Decimal(".01"),
    )

    class API:
        async def get(self, *args, **kwargs):
            return {"blocked": True, "country": "US", "region": "CA"}

        async def discover(self, stamp):
            return (market, following) if stamp < 1200 else (following, None)

        async def snapshot(self, token):
            return snap(token, ".45", ".46")

        async def resolution(self, m):
            return {
                "conditionId": m.condition,
                "closed": True,
                "umaResolutionStatus": "resolved",
                "outcomes": ["Up", "Down"],
                "clobTokenIds": list(m.tokens.values()),
                "outcomePrices": ["1", "0"],
            }

    def snap(token, bid, ask):
        return {
            "event_type": "book",
            "asset_id": token,
            "timestamp": str(int(now[0] * 1000)),
            "bids": [{"price": bid, "size": "30"}],
            "asks": [{"price": ask, "size": "30"}],
        }

    class WS:
        def __init__(self):
            self.n = 0

        async def send(self, raw):
            if raw != "PING":
                self.tokens = json.loads(raw)["assets_ids"]

        async def recv(self):
            self.n += 1
            await asyncio.sleep(0.01)
            if self.n == 1:
                return json.dumps([snap(t, ".45", ".46") for t in self.tokens])
            if self.n == 2:
                return json.dumps([snap(t, ".51", ".52") for t in self.tokens])
            now[0] = 1201.0
            if "u2" in self.tokens:
                stop.set()
            return "PONG"

    class Context:
        async def __aenter__(self):
            return WS()

        async def __aexit__(self, *args):
            pass

    settings = Settings(
        runtime_dir=tmp_path,
        discovery_seconds=0.01,
        resolution_seconds=0.01,
        stale_seconds=10.0,
        signal_threshold=Decimal(".02"),
        min_momentum=Decimal(".02"),
    )
    service = Service(settings, API(), clock=lambda: now[0], connect=lambda *a, **k: Context())
    await asyncio.wait_for(service.run(stop), 3)
    ledger = Ledger(tmp_path / "ledger.sqlite3", settings)
    summary = ledger.summary()
    assert summary["fills"] == 2
    assert summary["settlements"] == 1
    assert summary["pending"] == 1
    ledger.close()
    # Restart uses committed ledger and never replays the settled trade.
    stop2 = asyncio.Event()
    stop2.set()
    await Service(settings, API(), clock=lambda: now[0]).run(stop2)
    ledger = Ledger(tmp_path / "ledger.sqlite3", settings)
    assert ledger.summary()["fills"] == 2
    assert ledger.summary()["settlements"] == 1
    ledger.close()
