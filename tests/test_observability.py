"""Offline forward-only audit contracts; all databases and books are test-owned."""

import json
import sqlite3
from decimal import Decimal as D

import pytest
from test_domain import event, make_book

from btc5m.config import Settings
from btc5m.domain import Market, Signal
from btc5m.ledger import Ledger, Rejected


def payloads(ledger, kind):
    return [
        json.loads(r[0])
        for r in ledger.db.execute("SELECT payload FROM events WHERE kind=? ORDER BY id", (kind,))
    ]


def test_correlated_fill_ladder_and_separate_price_moves(tmp_path):
    from btc5m.telemetry import quote_snapshot

    ledger = Ledger(tmp_path / "db", Settings(), run_id="run-a")
    market = Market.from_event(event())
    signal_quote = quote_snapshot(make_book(ask=".50", now=1800000001), 1800000001.5, 5)
    book = make_book(size="3")
    book.asks[D(".52")] = D("5")
    result = ledger.execute(
        market,
        book,
        Signal("Up", D("5"), D(".53"), "test"),
        1800000002,
        audit={
            "signal_id": "signal-a",
            "signal_at": 1800000001.5,
            "signal_quote": signal_quote,
            "order_at": 1800000002,
        },
        clock=lambda: 1800000002,
    )
    assert result["signal_id"] == "signal-a"
    assert result["order_quote"]["best_ask"] == "0.51"
    assert result["order_quote"]["best_bid"] == "0.49"
    assert result["order_quote"]["mid"] == "0.50"
    assert D(result["execution_slippage"]) == D(".004")
    assert D(result["signal_to_order_move"]) == D(".01")
    assert result["fill_ladder"] == [
        {"price": "0.51", "quantity": "3", "fee": "0.05248"},
        {"price": "0.52", "quantity": "2", "fee": "0.03494"},
    ]
    attempt = payloads(ledger, "order_attempt")[0]
    assert attempt["signal_id"] == "signal-a" and attempt["fill_ladder"] is None
    assert payloads(ledger, "paper_fill")[0]["run_id"] == "run-a"
    assert all(r[0] == "run-a" for r in ledger.db.execute("SELECT run_id FROM events"))
    ledger.close()


def test_rejected_attempts_and_persistent_circuit_interval(tmp_path):
    path = tmp_path / "db"
    ledger = Ledger(path, Settings(), run_id="first")
    ledger.trip("daily_loss", 1800000000)
    market = Market.from_event(event())
    signal = Signal("Up", D("5"), D(".53"), "test")
    for _ in range(2):
        with pytest.raises(Rejected, match="circuit"):
            ledger.execute(market, make_book(), signal, 1800000002, clock=lambda: 1800000002)
    ledger.close()
    ledger = Ledger(path, Settings(), run_id="second")
    with pytest.raises(Rejected):
        ledger.execute(market, make_book(), signal, 1800000003, clock=lambda: 1800000003)
    ledger.reset_circuit(1800000010)
    interval = dict(ledger.db.execute("SELECT * FROM circuit_intervals").fetchone())
    assert interval["reason"] == "daily_loss"
    assert interval["started"] == 1800000000 and interval["ended"] == 1800000010
    assert interval["rejected_signals"] == 3
    assert interval["start_run_id"] == "first" and interval["end_run_id"] == "second"
    assert len(payloads(ledger, "order_attempt")) == 3
    assert len(payloads(ledger, "order_result")) == 3
    assert payloads(ledger, "circuit_interval_closed")[0]["duration_seconds"] == 10
    ledger.close()


def test_complete_and_incomplete_marks_drawdown_survive_restart(tmp_path):
    path = tmp_path / "db"
    settings = Settings()
    ledger = Ledger(path, settings, run_id="first")
    ledger.record_equity({}, 1800000001)
    ledger.execute(
        Market.from_event(event()),
        make_book(),
        Signal("Up", D("5"), D(".53"), "test"),
        1800000002,
        clock=lambda: 1800000002,
    )
    book = make_book(bid=".40", ask=".42")
    snapshot = ledger.record_equity({"up": book}, 1800000002)
    assert snapshot["complete"]
    assert D(snapshot["positions_value_bid"]) == D("2")
    assert D(snapshot["positions_value_mid"]) == D("2.05")
    assert D(snapshot["equity_bid"]) == D("999.36253")
    assert D(snapshot["drawdown"]["bid"]["max_drawdown"]) == D(".63747")
    incomplete = ledger.record_equity({}, 1800000003)
    assert not incomplete["complete"]
    assert incomplete["equity_bid"] is None
    assert incomplete["positions"][0]["mark_status"] == "missing"
    stale = ledger.record_equity({"up": book}, 1800000020)
    assert stale["equity_bid"] is None
    assert stale["positions"][0]["mark_status"] == "stale_or_invalid"
    ledger.close()
    ledger = Ledger(path, settings, run_id="second")
    recovered = ledger.record_equity(
        {"up": make_book(bid=".30", ask=".32", now=1800000021)}, 1800000021
    )
    assert D(recovered["drawdown"]["bid"]["max_drawdown"]) == D("1.13747")
    assert ledger.summary()["cash"] == "997.36253"
    ledger.close()


def legacy_db(path, circuit=""):
    # Manufacture a genuine old-layout fixture using baseline SQL, never a runtime DB.
    from btc5m.schema import BASE_SCHEMA

    db = sqlite3.connect(path)
    for statement in BASE_SCHEMA:
        db.execute(statement)
    db.executemany(
        "INSERT INTO meta VALUES (?,?)",
        [
            ("schema_version", "1"),
            ("cash", "12.34"),
            ("initial_cash", "1000"),
            ("circuit", circuit),
            ("consecutive_losses", "2"),
        ],
    )
    db.execute("INSERT INTO events VALUES (1,'legacy','{\"x\":1}',100)")
    db.commit()
    db.close()


def test_legacy_migration_preserves_rows_and_unknown_active_start(tmp_path):
    path = tmp_path / "db"
    legacy_db(path, "abnormal_spread")
    ledger = Ledger(path, Settings(), run_id="new")
    row = ledger.db.execute("SELECT * FROM events WHERE id=1").fetchone()
    assert row["payload"] == '{"x":1}' and row["run_id"] is None
    assert ledger.summary()["cash"] == "12.34"
    interval = ledger.db.execute("SELECT * FROM circuit_intervals").fetchone()
    assert interval["started"] is None and interval["start_run_id"] is None
    ledger.close()
    ledger = Ledger(path, Settings(), run_id="another")
    assert ledger.db.execute("SELECT COUNT(*) FROM circuit_intervals").fetchone()[0] == 1
    ledger.close()


def test_future_schema_rejected_without_changes(tmp_path):
    path = tmp_path / "db"
    legacy_db(path)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE meta SET value='999' WHERE key='schema_version'")
    before = path.read_bytes()
    with pytest.raises(ValueError, match="schema"):
        Ledger(path, Settings())
    assert path.read_bytes() == before
    assert not path.with_name("db-wal").exists()


def test_migration_ddl_failure_rolls_back_everything(tmp_path):
    path = tmp_path / "db"
    legacy_db(path)
    with sqlite3.connect(path) as db:
        db.execute("CREATE VIEW equity_snapshots AS SELECT 1")
    with pytest.raises(sqlite3.Error):
        Ledger(path, Settings())
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0] == "1"
        assert "run_id" not in [r[1] for r in db.execute("PRAGMA table_info(events)")]
        assert db.execute("SELECT payload FROM events").fetchone()[0] == '{"x":1}'


def test_readonly_stats_filters_legacy_and_aware_times(tmp_path):
    from btc5m.stats import read_stats, timestamp

    path = tmp_path / "db"
    legacy_db(path)
    before = path.read_bytes()
    legacy = read_stats(path)
    assert legacy["event_counts"] == {"legacy": 1}
    assert legacy["run_counts"] == {"legacy": 1}
    assert legacy["equity_snapshot_count"] == 0
    assert read_stats(path, run_id="legacy")["event_count"] == 1
    assert read_stats(path, run_id="new")["event_count"] == 0
    assert path.read_bytes() == before
    ledger = Ledger(path, Settings(), run_id="a")
    ledger.event("test", {"run_id": "spoofed"}, 200)
    ledger.record_equity({}, 200)
    ledger.close()
    ledger = Ledger(path, Settings(), run_id="b")
    ledger.event("test", {}, 300)
    ledger.record_equity({}, 300)
    ledger.close()
    before = path.read_bytes()
    stat = read_stats(path, since=150, until=250, run_id="a")
    assert stat["event_counts"] == {"test": 1}
    assert stat["run_counts"] == {"a": 1}
    assert stat["equity_snapshot_count"] == 1
    assert stat["latest_equity"]["run_id"] == "a"
    assert read_stats(path, run_id="legacy")["event_counts"] == {"legacy": 1}
    assert read_stats(path, run_id="absent")["event_count"] == 0
    assert path.read_bytes() == before
    assert timestamp("2026-11-07T10:00:00-08:00") == timestamp("2026-11-07T18:00:00Z")
    assert timestamp("1800000000") == 1800000000
    for bad in ["2026-11-07T10:00:00", "NaN", "inf", "yesterday"]:
        with pytest.raises(ValueError):
            timestamp(bad)
    with pytest.raises(ValueError):
        read_stats(path, since=300, until=200)


def test_stats_drawdown_uses_selected_complete_samples_and_interval_overlap(tmp_path):
    from btc5m.stats import read_stats

    path = tmp_path / "db"
    ledger = Ledger(path, Settings(), run_id="a")
    ledger.record_equity({}, 1)
    ledger.execute(
        Market.from_event(event()),
        make_book(),
        Signal("Up", D("5"), D(".53"), "test"),
        1800000002,
        clock=lambda: 1800000002,
    )
    ledger.record_equity({"up": make_book(bid=".40", ask=".42")}, 1800000002)
    ledger.record_equity({}, 1800000003)
    ledger.trip("daily_loss", 1800000003)
    assert (
        ledger.execute(
            Market.from_event(event()),
            make_book(),
            Signal("Up", D("5"), D(".53"), "test"),
            1800000004,
            clock=lambda: 1800000004,
        )
        is None
    )
    # Default entry cap precedes circuit check, so use another market for a rejection.
    from dataclasses import replace

    other = replace(
        Market.from_event(event()),
        slug="btc-updown-5m-1800000300",
        start=1800000300,
        end=1800000600,
    )
    with pytest.raises(Rejected):
        ledger.execute(
            other,
            make_book(now=1800000302),
            Signal("Up", D("5"), D(".53"), "test"),
            1800000302,
            clock=lambda: 1800000302,
        )
    ledger.reset_circuit(1800000310)
    ledger.close()
    stat = read_stats(path, since=1800000002, until=1800000305, run_id="a")
    assert stat["equity_snapshot_count"] == 2
    assert stat["complete_equity_snapshot_count"] == 1
    assert D(stat["selected_drawdown"]["bid"]["max_drawdown"]) == 0
    assert stat["selected_drawdown"]["bid"]["current_drawdown"] is None
    interval = stat["circuit_intervals"][0]
    assert interval["duration_seconds"] == 307
    assert interval["selected_rejected_signals"] == 1


def test_service_signal_snapshot_immutable_future_order_null(tmp_path):
    from test_review_fixes import ready_service

    service = ready_service(tmp_path)
    now = [1800000002.0]
    service.clock = lambda: now[0]
    record = service.record

    def after_signal(kind, details):
        record(kind, details)
        if kind == "signal":
            now[0] += 0.1
            book = service.books["up"]
            book.asks = {D(".52"): D("20")}
            book.updated_at = book.source_time = 1800000002.0

    service.record = after_signal
    service.update()
    signal = payloads(service.ledger, "signal")[0]
    fill = payloads(service.ledger, "paper_fill")[0]
    assert signal["signal_quote"]["best_ask"] == "0.51"
    assert signal["order_quote"] is None and signal["order_at"] is None
    assert signal["execution_slippage"] is None and signal["fill_ladder"] is None
    assert fill["signal_id"] == signal["signal_id"]
    assert fill["signal_at"] == 1800000002 and fill["order_at"] == 1800000002.1
    assert D(fill["signal_to_order_move"]) == D(".01")
    assert D(fill["execution_slippage"]) == 0
    assert fill["order_quote"]["source_age_seconds"] == pytest.approx(0.1)
    assert fill["order_quote"]["received_age_seconds"] == pytest.approx(0.1)
    assert fill["order_quote"]["source_at"] == 1800000002
    assert fill["order_quote"]["received_at"] == 1800000002
    service.ledger.close()


def test_service_circuit_counts_not_deduplicated_or_throttle_evaluations(tmp_path):
    from test_review_fixes import ready_service

    service = ready_service(tmp_path)
    service.ledger.trip("daily_loss", 1800000001)
    for stamp in (1800000002, 1800000033, 1800000064):
        service.clock = lambda stamp=stamp: stamp
        service.books = {"up": make_book(now=stamp), "down": make_book("down", now=stamp)}
        for book in service.books.values():
            book.continuity_epoch = service.continuity.epoch
            book.continuity_sync_sequence = service.continuity.sync_sequence
        for _ in range(10):
            service.update()
    counts = service.ledger.summary()["event_counts"]
    assert counts["signal"] == 3
    assert counts["order_rejected"] == 1
    assert counts["order_attempt"] == counts["circuit_signal_rejected"] == 3
    row = service.ledger.db.execute("SELECT rejected_signals FROM circuit_intervals").fetchone()
    assert row[0] == 3
    service.ledger.close()


def test_equity_snapshot_timer_and_health_are_observations_only(tmp_path):
    from test_review_fixes import ready_service

    service = ready_service(tmp_path)
    now = [1800000002.0]
    service.clock = lambda: now[0]
    before = service.ledger.summary()
    service.snapshot_equity_if_due()
    now[0] += 14.9
    service.snapshot_equity_if_due()
    now[0] += 0.1
    service.snapshot_equity_if_due()
    assert service.ledger.db.execute("SELECT COUNT(*) FROM equity_snapshots").fetchone()[0] == 2
    assert service.ledger.summary() == before
    service.health()
    state = json.loads((tmp_path / "health.json").read_text())
    assert state["equity_snapshot"]["complete"]
    assert state["equity_snapshot"]["drawdown"]["bid"]["max_drawdown"] == "0"
    assert state["run_id"] == state["equity_snapshot"]["run_id"]
    service.ledger.close()


@pytest.mark.asyncio
async def test_valuation_fetch_wait_never_blocks_execution_or_mutates_books(tmp_path):
    import asyncio

    from test_review_fixes import ready_service

    service = ready_service(tmp_path, max_entries_per_window=2)
    market = service.market
    service.ledger.execute(
        market,
        make_book(),
        Signal("Up", D("5"), D(".53"), "prior"),
        1800000002,
        clock=lambda: 1800000002,
    )
    requested = asyncio.Event()
    release = asyncio.Event()

    class API:
        async def snapshot(self, token):
            assert token == "up"
            requested.set()
            await release.wait()
            return {
                "asset_id": token,
                "timestamp": "1800000002000",
                "tick_size": ".01",
                "bids": [{"price": ".20", "size": "3"}],
                "asks": [{"price": ".22", "size": "3"}],
            }

    service.api = API()
    task = asyncio.create_task(service.refresh_valuation_quotes())
    await asyncio.wait_for(requested.wait(), 1)
    service.update()
    assert service.ledger.summary()["fills"] == 2
    asks_after_trade = dict(service.books["up"].asks)
    release.set()
    await task
    assert service.books["up"].asks == asks_after_trade
    assert service.books["up"].best_bid == D(".49")
    assert service.valuation_books["up"].best_bid == D(".20")
    assert service.valuation_books["up"] is not service.books["up"]
    assert service.ledger.summary()["circuit"] == ""
    service.ledger.close()


@pytest.mark.asyncio
async def test_marks_cover_old_windows_validate_clob_and_never_use_gamma(tmp_path):
    from dataclasses import replace

    from btc5m.service import Service

    now = 1800000302
    settings = Settings(runtime_dir=tmp_path)
    service = Service(settings, None, clock=lambda: now)
    service.ledger = Ledger(tmp_path / "db", settings)
    old = Market.from_event(event())
    new = replace(
        old,
        slug="btc-updown-5m-1800000300",
        start=1800000300,
        end=1800000600,
        tokens={"Up": "u2", "Down": "d2"},
    )
    service.ledger.execute(
        old,
        make_book(),
        Signal("Up", D("5"), D(".53"), "old"),
        1800000002,
        clock=lambda: 1800000002,
    )
    service.ledger.execute(
        new, make_book("u2", now=now), Signal("Up", D("5"), D(".53"), "new"), now, clock=lambda: now
    )
    service.market = new
    service.books = {"u2": make_book("u2", now=now), "d2": make_book("d2", now=now)}
    requested = []

    class API:
        async def snapshot(self, token):
            requested.append(token)
            assert token in {"up", "u2"}
            return {
                "asset_id": token,
                "timestamp": str(now * 1000),
                "tick_size": ".01",
                "bids": [{"price": ".40", "size": "10"}],
                "asks": [{"price": ".42", "size": "10"}],
            }

    service.api = API()
    await service.refresh_valuation_quotes()
    snapshot = service.ledger.record_equity(service.valuation_books, now)
    assert set(requested) == {"up", "u2"}
    assert snapshot["complete"] and D(snapshot["positions_value_bid"]) == 4
    assert {p["slug"] for p in snapshot["positions"]} == {old.slug, new.slug}
    service.ledger.close()


def test_formatter_run_id_covers_network_and_lifecycle_logs():
    import logging

    from btc5m.cli import JsonFormatter
    from btc5m.telemetry import RUN_ID

    token = RUN_ID.set("process-context")
    try:
        for name, message in [
            ("btc5m.public", "ws_reconnect"),
            ("btc5m.cli", "emergency_stop"),
            ("btc5m.public", "http_retry"),
            ("btc5m.service", "service_started"),
        ]:
            record = logging.LogRecord(name, logging.INFO, __file__, 1, message, (), None)
            encoded = json.loads(JsonFormatter().format(record))
            assert encoded["run_id"] == encoded["details"]["run_id"] == "process-context"
    finally:
        RUN_ID.reset(token)


def test_restarted_run_sees_persisted_interval_without_reassigning_start(tmp_path):
    from btc5m.stats import read_stats

    path = tmp_path / "db"
    ledger = Ledger(path, Settings(), run_id="original")
    ledger.trip("consecutive_losses", 1800000000)
    ledger.close()
    ledger = Ledger(path, Settings(), run_id="resumed")
    ledger.close()
    result = read_stats(path, run_id="resumed")
    assert result["event_count"] == 0  # no historical event/run fabrication
    interval = result["circuit_intervals"][0]
    assert interval["started"] == 1800000000 and interval["start_run_id"] == "original"
    assert interval["ended"] is None
    assert interval["selected_rejected_signals"] == 0


def test_stats_interval_overlap_excludes_end_at_range_start(tmp_path):
    from btc5m.stats import read_stats

    ledger = Ledger(tmp_path / "db", Settings(), run_id="a")
    ledger.trip("daily_loss", 100)
    ledger.reset_circuit(200)
    ledger.close()
    stats = read_stats(tmp_path / "db", since=200, until=300)
    assert stats["circuit_intervals"] == []
    assert stats["event_counts"]["circuit_reset"] == 1


def test_v1_migration_preserves_money_orders_fills_settlements_and_event_bytes(tmp_path):
    path = tmp_path / "db"
    legacy_db(path)
    market = Market.from_event(event())
    with sqlite3.connect(path) as db:
        db.execute(
            "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)",
            (market.slug, json.dumps(market.to_dict()), "Up", "5", "2.5", ".1", 101, 1),
        )
        db.execute("INSERT INTO fills VALUES (?,?,?,?,?)", (market.slug, 0, ".5", "5", ".1"))
        db.execute(
            "INSERT INTO settlements VALUES (?,?,?,?,?,?)",
            (market.slug, "Up", "5", "2.4", 401, '{"official":"legacy"}'),
        )
        before = {
            table: db.execute(f"SELECT * FROM {table}").fetchall()
            for table in ("orders", "fills", "settlements", "events", "daily")
        }
        money = db.execute(
            "SELECT key,value FROM meta WHERE key!='schema_version' ORDER BY key"
        ).fetchall()
    ledger = Ledger(path, Settings(), run_id="upgrade")
    ledger.close()
    # Idempotent opening must not recompute cash, loss counters or old fees/PnL.
    ledger = Ledger(path, Settings(initial_cash=D("9999")), run_id="reopen")
    for table in ("orders", "fills", "settlements", "daily"):
        assert [tuple(r) for r in ledger.db.execute(f"SELECT * FROM {table}")] == before[table]
    assert [tuple(r)[:-1] for r in ledger.db.execute("SELECT * FROM events")] == before["events"]
    assert ledger.db.execute("SELECT run_id FROM events").fetchone()[0] is None
    assert [
        tuple(r)
        for r in ledger.db.execute(
            "SELECT key,value FROM meta WHERE key!='schema_version' ORDER BY key"
        )
    ] == money
    assert ledger.has_order(market.slug)
    assert ledger.summary()["cash"] == "12.34"
    ledger.close()


@pytest.mark.asyncio
async def test_invalid_valuation_quotes_are_incomplete_without_new_breakers(tmp_path):
    from test_review_fixes import ready_service

    service = ready_service(tmp_path, max_entries_per_window=2)
    service.ledger.execute(
        service.market,
        make_book(),
        Signal("Up", D("5"), D(".53"), "prior"),
        1800000002,
        clock=lambda: 1800000002,
    )

    class API:
        async def snapshot(self, token):
            return {
                "asset_id": token,
                "timestamp": "1800000002000",
                "tick_size": ".01",
                "bids": [{"price": ".60", "size": "10"}],
                "asks": [{"price": ".50", "size": "10"}],
            }

    service.api = API()
    await service.refresh_valuation_quotes()
    snapshot = service.ledger.record_equity(service.valuation_books, 1800000002)
    assert not snapshot["complete"] and snapshot["equity_mid"] is None
    assert service.ledger.summary()["circuit"] == ""
    assert service.books["up"].connected and service.books["up"].best_bid == D(".49")
    service.update()
    assert service.ledger.summary()["fills"] == 2
    service.ledger.close()
