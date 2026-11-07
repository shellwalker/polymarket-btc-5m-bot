from decimal import Decimal as D

import pytest
from test_domain import event, make_book

from btc5m.config import Settings
from btc5m.domain import Market, Signal
from btc5m.ledger import Ledger, Rejected


def test_fill_restart_no_duplicate_and_authoritative_settlement(tmp_path):
    settings = Settings()
    path = tmp_path / "ledger.sqlite"
    market = Market.from_event(event())
    ledger = Ledger(path, settings)
    signal = Signal("Up", D("5"), D("0.53"), "observed momentum")
    fill = ledger.execute(market, make_book(), signal, 1800000002)
    assert fill is not None
    assert D(ledger.summary()["cash"]) == D("1000") - D("2.55") - D("0.08747")
    ledger.close()
    ledger = Ledger(path, settings)
    assert len(ledger.pending()) == 1
    assert ledger.execute(market, make_book(), signal, 1800000002) is None
    assert (
        ledger.settle(
            market,
            {"closed": False, "umaResolutionStatus": "resolved", "outcomePrices": '["1","0"]'},
            1800000301,
        )
        is None
    )
    official = {
        "conditionId": "condition-1",
        "closed": True,
        "umaResolutionStatus": "resolved",
        "outcomes": '["Up","Down"]',
        "clobTokenIds": '["up","down"]',
        "outcomePrices": '["1","0"]',
    }
    assert ledger.settle(market, official, 1800000301)["pnl"] == "2.36253"
    assert ledger.settle(market, official, 1800000302) is None
    assert ledger.summary()["settlements"] == 1
    assert not ledger.pending()


def test_depth_slippage_tick_and_minimum_rejection(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite", Settings())
    market = Market.from_event(event())
    for signal, book in [
        (Signal("Up", D("5"), D("0.53"), "test"), make_book(size="4")),
        (Signal("Up", D("4"), D("0.53"), "test"), make_book()),
        (Signal("Up", D("5"), D("0.515"), "test"), make_book()),
        (Signal("Up", D("5"), D("0.50"), "test"), make_book()),
    ]:
        with pytest.raises(Rejected):
            ledger.execute(market, book, signal, 1800000002)
    assert ledger.summary()["fills"] == 0
    assert ledger.summary()["cash"] == "1000"


def test_exposure_and_circuit_persist(tmp_path):
    path = tmp_path / "ledger.sqlite"
    settings = Settings(max_trade=D("2"))
    ledger = Ledger(path, settings)
    market = Market.from_event(event())
    with pytest.raises(Rejected, match="trade"):
        ledger.execute(market, make_book(), Signal("Up", D("5"), D("0.53"), "test"), 1800000002)
    ledger.trip("stale", 1800000002)
    ledger.close()
    ledger = Ledger(path, settings)
    assert ledger.summary()["circuit"] == "stale"
    with pytest.raises(Rejected, match="circuit"):
        ledger.execute(market, make_book(), Signal("Up", D("5"), D("0.53"), "test"), 1800000002)


def test_multi_level_depth_fill_consumed_once(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite", Settings())
    market = Market.from_event(event())
    book = make_book(size="3")
    book.delta(
        {"asset_id": "up", "price": "0.52", "size": "4", "side": "SELL"},
        "1800000002000",
        1800000002,
    )
    result = ledger.execute(market, book, Signal("Up", D("5"), D("0.53"), "test"), 1800000002)
    assert result["levels"] == 2
    assert D(result["cost"]) == D("2.57")
    assert book.asks == {D("0.52"): D("2")}
    assert ledger.execute(market, book, Signal("Up", D("5"), D("0.53"), "test"), 1800000002) is None
    assert ledger.summary()["fill_levels"] == 2


def test_daily_and_consecutive_loss_limits_survive_restart(tmp_path):
    path = tmp_path / "ledger.sqlite"
    settings = Settings(max_consecutive_losses=1)
    market = Market.from_event(event())
    ledger = Ledger(path, settings)
    ledger.execute(market, make_book(), Signal("Up", D("5"), D("0.53"), "test"), 1800000002)
    official = {
        "conditionId": "condition-1",
        "closed": True,
        "umaResolutionStatus": "resolved",
        "outcomes": '["Up","Down"]',
        "clobTokenIds": '["up","down"]',
        "outcomePrices": '["0","1"]',
    }
    ledger.settle(market, official, 1800000301)
    ledger.close()
    ledger = Ledger(path, settings)
    assert ledger.summary()["consecutive_losses"] == 1
    assert ledger.summary()["circuit"] == "consecutive_losses"
    assert list(ledger.summary()["daily_losses"].values()) == ["2.63747"]
    assert ledger.summary()["cash"] == "997.36253"


@pytest.mark.parametrize(
    "field,value",
    [
        ("umaResolutionStatus", "proposed"),
        ("closed", False),
        ("outcomePrices", '["0.99","0.01"]'),
        ("outcomePrices", '["1","1"]'),
        ("clobTokenIds", '["down","up"]'),
        ("conditionId", "wrong"),
    ],
)
def test_settlement_rejects_unknown_or_provisional_evidence(tmp_path, field, value):
    ledger = Ledger(tmp_path / "ledger.sqlite", Settings())
    market = Market.from_event(event())
    ledger.execute(market, make_book(), Signal("Up", D("5"), D("0.53"), "test"), 1800000002)
    official = {
        "conditionId": "condition-1",
        "closed": True,
        "umaResolutionStatus": "resolved",
        "outcomes": '["Up","Down"]',
        "clobTokenIds": '["up","down"]',
        "outcomePrices": '["1","0"]',
    }
    official[field] = value
    assert ledger.settle(market, official, 1800000301) is None
    assert ledger.summary()["pending"] == 1


def test_total_exposure_rejection_does_not_consume_depth(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite", Settings(max_exposure=D("2.6")))
    market = Market.from_event(event())
    book = make_book()
    with pytest.raises(Rejected, match="exposure"):
        ledger.execute(market, book, Signal("Up", D("5"), D("0.53"), "test"), 1800000002)
    assert book.asks[D("0.51")] == D("10")
    assert ledger.summary()["cash"] == "1000"
    assert ledger.summary()["fills"] == 0


def test_daily_limit_reserves_worst_case_pending_and_new_debit(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite", Settings(daily_loss_limit=D("2.6")))
    market = Market.from_event(event())
    with pytest.raises(Rejected, match="daily"):
        ledger.execute(market, make_book(), Signal("Up", D("5"), D("0.53"), "test"), 1800000002)
    assert ledger.summary()["fills"] == 0


def test_malformed_resolution_schema_is_pending_not_crash(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite", Settings())
    market = Market.from_event(event())
    official = {
        "conditionId": "condition-1",
        "closed": True,
        "umaResolutionStatus": "resolved",
        "outcomes": [["Up"], "Down"],
        "clobTokenIds": ["up", "down"],
        "outcomePrices": ["1", "0"],
    }
    assert ledger.settle(market, official, 1800000301) is None


def test_sqlite_mid_transaction_failure_rolls_back_cash_order_and_depth(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite", Settings())
    ledger.db.execute(
        "CREATE TRIGGER fail_fill BEFORE INSERT ON fills BEGIN SELECT RAISE(ABORT,'injected'); END"
    )
    market = Market.from_event(event())
    book = make_book()
    import sqlite3

    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        ledger.execute(market, book, Signal("Up", D("5"), D("0.53"), "test"), 1800000002)
    assert ledger.summary()["cash"] == "1000"
    assert ledger.summary()["fills"] == 0
    assert book.asks[D("0.51")] == D("10")
    ledger.db.execute("DROP TRIGGER fail_fill")
    assert ledger.execute(market, book, Signal("Up", D("5"), D("0.53"), "test"), 1800000002)


def test_summary_cost_equity_fees_counters_and_audited_reset(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite", Settings())
    market = Market.from_event(event())
    ledger.execute(market, make_book(), Signal("Up", D("5"), D("0.53"), "test"), 1800000002)
    summary = ledger.summary()
    assert summary["total_fees"] == "0.08747"
    assert summary["pending_cost"] == "2.55"
    assert summary["equity_at_cost"] == "999.91253"
    assert summary["equity_worst_case"] == summary["cash"]
    assert summary["event_counts"]["paper_fill"] == 1
    ledger.trip("stale", 1800000003)
    ledger.reset_circuit(now=1800000004)
    assert ledger.summary()["circuit"] == ""
    assert ledger.summary()["event_counts"]["circuit_reset"] == 1


def test_daily_realized_limit_trips_persistent_audited_circuit(tmp_path):
    path = tmp_path / "ledger.sqlite"
    settings = Settings(daily_loss_limit=D("2.63747"))
    ledger = Ledger(path, settings)
    market = Market.from_event(event())
    ledger.execute(market, make_book(), Signal("Up", D("5"), D("0.53"), "test"), 1800000002)
    official = {
        "conditionId": "condition-1",
        "closed": True,
        "umaResolutionStatus": "resolved",
        "outcomes": '["Up","Down"]',
        "clobTokenIds": '["up","down"]',
        "outcomePrices": '["0","1"]',
    }
    ledger.settle(market, official, 1800000301)
    ledger.close()
    ledger = Ledger(path, settings)
    assert ledger.summary()["circuit"] == "daily_loss"
    assert ledger.summary()["event_counts"]["circuit_breaker"] == 1


def test_read_summary_is_read_only_and_consistent_with_open_writer(tmp_path):
    path = tmp_path / "ledger #1.sqlite"
    ledger = Ledger(path, Settings())
    market = Market.from_event(event())
    ledger.execute(market, make_book(), Signal("Up", D("5"), D("0.53"), "test"), 1800000002)
    before = path.stat().st_mtime_ns
    summary = Ledger.read_summary(path)
    assert summary == ledger.summary()
    assert path.stat().st_mtime_ns == before
    assert summary["fills"] == 1
    assert summary["pending"] == 1
    ledger.db.execute("BEGIN IMMEDIATE")
    ledger.db.execute("UPDATE meta SET value='123' WHERE key='cash'")
    assert Ledger.read_summary(path)["cash"] == "997.36253"
    ledger.db.rollback()
    ledger.close()


def test_read_summary_does_not_create_missing_db_or_accept_unknown_schema(tmp_path):
    path = tmp_path / "missing.sqlite"
    with pytest.raises(ValueError, match="read"):
        Ledger.read_summary(path)
    assert not path.exists()
    ledger = Ledger(path, Settings())
    ledger.db.execute("UPDATE meta SET value='future' WHERE key='schema_version'")
    ledger.db.commit()
    ledger.close()
    with pytest.raises(ValueError, match="schema"):
        Ledger.read_summary(path)


def test_has_order_persists_across_restart(tmp_path):
    path = tmp_path / "ledger.sqlite"
    ledger = Ledger(path, Settings())
    market = Market.from_event(event())
    assert not ledger.has_order(market.slug)
    ledger.execute(market, make_book(), Signal("Up", D("5"), D("0.53"), "test"), 1800000002)
    assert ledger.has_order(market.slug)
    ledger.close()
    ledger = Ledger(path, Settings())
    assert ledger.has_order(market.slug)


def test_execution_revalidates_depth_tick_even_if_book_reports_healthy(tmp_path, monkeypatch):
    ledger = Ledger(tmp_path / "ledger.sqlite", Settings())
    market = Market.from_event(event())
    book = make_book()
    book.asks = {D("0.515"): D("10")}
    monkeypatch.setattr(book, "healthy", lambda *_: True)
    with pytest.raises(Rejected, match="tick"):
        ledger.execute(market, book, Signal("Up", D("5"), D("0.53"), "test"), 1800000002)
    assert ledger.summary()["fills"] == 0


def test_newer_snapshot_minimum_is_respected(tmp_path):
    book = make_book()
    book.snapshot(
        {
            "asset_id": "up",
            "timestamp": "1800000002000",
            "min_order_size": "10",
            "bids": [{"price": ".49", "size": "20"}],
            "asks": [{"price": ".51", "size": "20"}],
        },
        1800000002,
    )
    ledger = Ledger(tmp_path / "ledger.sqlite", Settings())
    with pytest.raises(Rejected, match="minimum"):
        ledger.execute(
            Market.from_event(event()), book, Signal("Up", D("5"), D(".53"), "test"), 1800000002
        )
    assert ledger.summary()["fills"] == 0
    ledger.close()
