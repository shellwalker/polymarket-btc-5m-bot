import asyncio
import json
from decimal import Decimal as D
from types import SimpleNamespace

import pytest
from test_domain import event, make_book

from btc5m.config import Settings
from btc5m.domain import Market, MomentumStrategy, Signal, fee
from btc5m.ledger import Ledger, Rejected
from btc5m.service import Service


def ready_service(tmp_path, **config):
    settings = Settings(runtime_dir=tmp_path, **config)
    service = Service(settings, None, clock=lambda: 1800000002.0)
    service.market = Market.from_event(event())
    service.books = {"up": make_book(), "down": make_book("down")}
    service.stream = SimpleNamespace(
        ready=True, reconnects=0, gap_started=None, last_gap_seconds=None
    )
    service.ledger = Ledger(tmp_path / "ledger.sqlite3", settings)
    service.strategy = SimpleNamespace(signal=lambda *a: Signal("Up", D("5"), D(".53"), "decision"))
    epoch = service.continuity.begin(service.stream, set(service.books))
    for book in service.books.values():
        book.continuity_epoch = epoch
        book.continuity_sync_sequence = service.continuity.sync_sequence
    assert service.continuity.complete(epoch, service.stream, service.books, service.clock())
    return service


@pytest.mark.parametrize("book_state", ["ready", "stale", "abnormal"])
def test_stop_requested_callback_has_no_ledger_mutation(tmp_path, book_state):
    service = ready_service(tmp_path)
    service.stop = asyncio.Event()
    service.ever_ready = True
    if book_state == "stale":
        service.books["up"].disconnect()
    elif book_state == "abnormal":
        service.books["up"] = make_book(bid=".40", ask=".51")
    service.stop.set()
    before = service.ledger.db.total_changes
    service.update()
    assert service.ledger.db.total_changes == before
    assert service.ledger.summary()["fills"] == 0
    assert service.ledger.summary()["circuit"] == ""
    service.health()
    assert not json.loads((tmp_path / "health.json").read_text())["trading_ready"]
    service.ledger.close()


@pytest.mark.parametrize("value", ["0", "-1", ".99", "NaN", "Infinity", "true"])
def test_invalid_conservative_fee_multiplier(value):
    with pytest.raises(ValueError):
        Settings.load(environ={"BTC5M_PAPER_FEE_MULTIPLIER": value})


def test_fee_multiplier_load_and_default(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text('paper_fee_multiplier="2"\n')
    assert Settings.load(path, {}).paper_fee_rate == D(".14")
    assert Settings.load(path, {"BTC5M_PAPER_FEE_MULTIPLIER": "3"}).paper_fee_rate == D(".21")
    assert Settings().paper_fee_rate == D(".07")
    assert fee(D("5"), D(".51")) == D(".08747")


def test_fee_multiplier_per_level_cash_and_risk(tmp_path):
    market = Market.from_event(event())
    book = make_book(size="3")
    book.asks[D(".52")] = D("10")
    settings = Settings(paper_fee_multiplier=D("2"))
    ledger = Ledger(tmp_path / "ledger.sqlite3", settings)
    fill = ledger.execute(
        market, book, Signal("Up", D("5"), D(".53"), "test"), 1800000002, clock=lambda: 1800000002
    )
    expected = fee(D("3"), D(".51"), D(".14")) + fee(D("2"), D(".52"), D(".14"))
    assert D(fill["fee"]) == expected
    assert D(ledger.summary()["cash"]) == D("1000") - D("2.57") - expected
    assert D(ledger.summary()["exposure"]) == D("2.57") + expected
    assert D(fill["paper_fee_rate"]) == D(".14")
    ledger.close()
    # Same price/quantity fits original fee risk budget but not doubled assumption.
    for multiplier, rejected in [("1", False), ("2", True)]:
        ledger = Ledger(
            tmp_path / f"risk-{multiplier}.sqlite",
            Settings(paper_fee_multiplier=D(multiplier), max_trade=D("2.65")),
        )
        if rejected:
            with pytest.raises(Rejected, match="trade"):
                ledger.execute(
                    market,
                    make_book(),
                    Signal("Up", D("5"), D(".53"), "test"),
                    1800000002,
                    clock=lambda: 1800000002,
                )
        else:
            assert ledger.execute(
                market,
                make_book(),
                Signal("Up", D("5"), D(".53"), "test"),
                1800000002,
                clock=lambda: 1800000002,
            )
        ledger.close()


def test_strategy_cost_filter_uses_paper_fee_multiplier():
    market = Market.from_event(event())
    books = {"up": make_book(bid=".44", ask=".46"), "down": make_book("down")}
    standard = MomentumStrategy(Settings())
    conservative = MomentumStrategy(Settings(paper_fee_multiplier=D("2")))
    for strategy in [standard, conservative]:
        assert strategy.signal(market, books, 1800000002) is None
    books["up"] = make_book(bid=".48", ask=".50")  # measured +.04 movement
    assert standard.signal(market, books, 1800000002) is not None
    assert conservative.signal(market, books, 1800000002) is None


@pytest.mark.parametrize(
    "config,reason",
    [
        ({"initial_cash": D("1")}, "cash"),
        ({"max_exposure": D("1")}, "exposure"),
        ({"daily_loss_limit": D("1")}, "daily"),
        ({"max_trade": D("1")}, "trade"),
    ],
)
def test_health_reports_risk_blocked_minimum_and_fee_assumption(tmp_path, config, reason):
    service = ready_service(tmp_path, paper_fee_multiplier=D("2"), **config)
    service.health()
    state = json.loads((tmp_path / "health.json").read_text())
    assert not state["trading_ready"]
    assert reason in " ".join(state["risk_blocked_reasons"])
    assert D(state["paper_fee_rate"]) == D(".14")
    service.ledger.close()


def test_repeated_rejected_signals_rate_limited(tmp_path):
    service = ready_service(tmp_path, max_trade=D("1"))
    for _ in range(100):
        service.update()
    assert service.ledger.summary()["event_counts"]["signal"] == 1
    assert service.ledger.summary()["event_counts"]["order_rejected"] == 1
    service.clock = lambda: 1800000033.0
    service.books = {"up": make_book(now=1800000033), "down": make_book("down", now=1800000033)}
    for book in service.books.values():
        book.continuity_epoch = service.continuity.epoch
        book.continuity_sync_sequence = service.continuity.sync_sequence
    service.update()
    assert service.ledger.summary()["event_counts"]["signal"] == 2
    service.ledger.close()


@pytest.mark.asyncio
async def test_fee_assumption_in_start_event_log_and_health(tmp_path, caplog):
    stop = asyncio.Event()

    class API:
        async def get(self, *a):
            return {"blocked": True, "country": "US", "region": "CA"}

        async def discover(self, *a):
            stop.set()
            return None, None

    settings = Settings(runtime_dir=tmp_path, paper_fee_multiplier=D("2"))
    service = Service(settings, API())
    with caplog.at_level("INFO", logger="btc5m.service"):
        await service.run(stop)
    assert service.stop is stop
    ledger = Ledger(tmp_path / "ledger.sqlite3", settings)
    payload = json.loads(
        ledger.db.execute("SELECT payload FROM events WHERE kind='service_started'").fetchone()[0]
    )
    assert D(payload["paper_fee_rate"]) == D(".14")
    assert payload["verified_fee_rate"] == "0.07"
    logs = [r.details for r in caplog.records if r.message == "service_started"]
    assert logs == [payload]
    assert D(json.loads((tmp_path / "health.json").read_text())["paper_fee_multiplier"]) == 2
    ledger.close()


def test_fee_assumption_does_not_relax_market_verification():
    Settings(paper_fee_multiplier=D("2"))
    raw = event()
    raw["markets"][0]["feeSchedule"]["rate"] = 0.14
    with pytest.raises(ValueError, match="fee"):
        Market.from_event(raw)


def test_persistent_circuit_signals_are_rate_limited(tmp_path):
    service = ready_service(tmp_path)
    service.ledger.trip("test circuit", 1800000002)
    for _ in range(100):
        service.update()
    counts = service.ledger.summary()["event_counts"]
    assert counts["signal"] == 1 and counts["order_rejected"] == 1
    assert service.ledger.summary()["fills"] == 0
    service.ledger.close()
