from decimal import Decimal as D

import pytest
from test_domain import event, make_book

from btc5m.config import Settings
from btc5m.domain import Market, Signal
from btc5m.ledger import Ledger, Rejected


def test_transient_recovery_requires_new_observations_cooldown_and_restart(tmp_path):
    settings = Settings(market_recovery_snapshots=2, market_recovery_cooldown=2)
    path = tmp_path / "ledger.sqlite"
    ledger = Ledger(path, settings)
    ledger.trip("abnormal_spread", 1)
    ledger.observe_market_health(True, ("window", 1), 2)
    ledger.observe_market_health(True, ("window", 1), 5)
    assert ledger.summary()["circuit"] == "abnormal_spread"
    ledger.close()
    ledger = Ledger(path, settings)
    ledger.observe_market_health(True, ("window", 2), 6)
    ledger.observe_market_health(False, None, 7)
    ledger.observe_market_health(True, ("window", 3), 8)
    ledger.observe_market_health(True, ("window", 4), 9)
    assert ledger.summary()["circuit"]
    ledger.observe_market_health(True, ("window", 5), 10)
    assert ledger.summary()["circuit"] == ""
    assert ledger.summary()["event_counts"]["circuit_reset"] == 1
    ledger.close()


@pytest.mark.parametrize("reason", ["daily_loss", "consecutive_losses", "market_identity_changed"])
def test_manual_breaker_supersedes_transient_and_never_auto_resets(tmp_path, reason):
    ledger = Ledger(
        tmp_path / "db", Settings(market_recovery_snapshots=1, market_recovery_cooldown=1)
    )
    ledger.trip("abnormal_spread", 1)
    ledger.trip(reason, 2)
    ledger.observe_market_health(True, ("w", 1), 3)
    ledger.observe_market_health(True, ("w", 2), 6)
    assert ledger.summary()["circuit"] == reason
    ledger.close()


def test_configurable_cap_durable_rejections_do_not_count_and_settlement(tmp_path):
    settings = Settings(max_entries_per_window=2)
    path = tmp_path / "db"
    market = Market.from_event(event())
    signal = Signal("Up", D("5"), D(".53"), "test")
    ledger = Ledger(path, settings)
    with pytest.raises(Rejected):
        ledger.execute(market, make_book(size="1"), signal, 1800000002, clock=lambda: 1800000002)
    assert ledger.execute(market, make_book(), signal, 1800000002, clock=lambda: 1800000002)
    ledger.close()
    ledger = Ledger(path, settings)
    assert ledger.execute(market, make_book(), signal, 1800000002, clock=lambda: 1800000002)
    ledger.close()
    ledger = Ledger(path, settings)
    assert ledger.window_entry_limit_reached(market.slug)
    assert ledger.execute(market, make_book(), signal, 1800000002, clock=lambda: 1800000002) is None
    assert len(ledger.pending()) == 1
    ledger.settle(
        market,
        dict(
            conditionId=market.condition,
            closed=True,
            umaResolutionStatus="resolved",
            outcomes=["Up", "Down"],
            clobTokenIds=["up", "down"],
            outcomePrices=["1", "0"],
        ),
        1800000301,
    )
    assert ledger.summary()["settlements"] == 2
    assert ledger.summary()["pending"] == 0
    assert ledger.execute(market, make_book(), signal, 1800000002, clock=lambda: 1800000002) is None
    ledger.close()


@pytest.mark.parametrize("reason", ["abnormal_spread", "stale_or_disconnected_book"])
@pytest.mark.parametrize("manual", [False, True])
def test_service_recovery_and_repeated_signals_across_restart(tmp_path, reason, manual):
    from types import SimpleNamespace

    from btc5m.service import Service

    settings = Settings(
        runtime_dir=tmp_path, market_recovery_snapshots=2, market_recovery_cooldown=1
    )
    market = Market.from_event(event())
    stamp = [1800000002.0]
    path = tmp_path / "ledger.sqlite"

    def service():
        instance = Service(settings, None, clock=lambda: stamp[0])
        instance.ledger = Ledger(path, settings)
        instance.market = market
        instance.stream = SimpleNamespace(ready=True)
        instance.books = {"up": make_book(), "down": make_book("down")}
        instance.strategy = SimpleNamespace(
            signal=lambda *a: Signal("Up", D("5"), D(".53"), "test")
        )
        epoch = instance.continuity.begin(instance.stream, set(instance.books))
        for book in instance.books.values():
            book.continuity_epoch = epoch
            book.continuity_sync_sequence = instance.continuity.sync_sequence
        assert instance.continuity.complete(
            epoch, instance.stream, instance.books, instance.clock()
        )
        return instance

    instance = service()
    instance.ledger.trip(reason, stamp[0])
    instance.ledger.db.execute("UPDATE meta SET value='2' WHERE key='consecutive_losses'")
    instance.ledger.db.commit()
    instance.update()
    instance.ledger.close()
    instance = service()
    if manual:
        instance.ledger.trip("daily_loss", stamp[0])
    # New process must collect its own evidence. Polling the same books does not count.
    instance.update()
    stamp[0] += 1
    instance.update()
    assert instance.ledger.summary()["fills"] == 0
    for book in instance.books.values():
        book.source_time = book.updated_at = stamp[0]
    # Attempt throttling from blocked signal must not hide a later successful attempt.
    instance.signal_last.clear()
    instance.update()
    assert instance.ledger.summary()["fills"] == (0 if manual else 1)
    assert instance.ledger.summary()["consecutive_losses"] == 2
    instance.ledger.close()
    if not manual:
        instance = service()
        instance.update()
        assert instance.ledger.summary()["fills"] == 1
        assert instance.risk_blocked_reasons(stamp[0]) == ["window already filled"]
        instance.ledger.close()


@pytest.mark.parametrize("field", ["max_trade", "max_exposure", "daily_loss_limit", "initial_cash"])
def test_entry_allowance_does_not_bypass_risk_limits(tmp_path, field):
    settings = Settings(max_entries_per_window=3, **{field: D("2")})
    ledger = Ledger(tmp_path / "db", settings)
    with pytest.raises(Rejected):
        ledger.execute(
            Market.from_event(event()),
            make_book(),
            Signal("Up", D("5"), D(".53"), "risk"),
            1800000002,
            clock=lambda: 1800000002,
        )
    assert not ledger.window_entry_limit_reached(Market.from_event(event()).slug)
    assert ledger.summary()["fills"] == 0
    ledger.close()


def test_legacy_window_backfill_and_disabled_recovery(tmp_path):
    path = tmp_path / "db"
    ledger = Ledger(path, Settings())
    market = Market.from_event(event())
    ledger.execute(
        market,
        make_book(),
        Signal("Up", D("5"), D(".53"), "legacy"),
        1800000002,
        clock=lambda: 1800000002,
    )
    ledger.trip("abnormal_spread", 1800000003)
    ledger.db.execute("DROP TABLE order_windows")
    ledger.db.execute("DROP TABLE equity_snapshots")
    ledger.db.execute("DROP TABLE circuit_runs")
    ledger.db.execute("DROP TABLE circuit_intervals")
    ledger.db.execute("DROP INDEX events_run_time")
    ledger.db.execute("DROP INDEX events_time")
    ledger.db.execute("ALTER TABLE events DROP COLUMN run_id")
    ledger.db.execute("UPDATE meta SET value='1' WHERE key='schema_version'")
    ledger.db.commit()
    before = ledger.summary()
    ledger.close()
    ledger = Ledger(path, Settings(market_auto_recovery=False))
    assert ledger.summary() == before
    assert ledger.window_entry_limit_reached(market.slug)
    for n in range(100):
        ledger.observe_market_health(True, ("w", n), 1800000010 + n)
    assert ledger.summary()["circuit"] == "abnormal_spread"
    ledger.close()


@pytest.mark.parametrize("name", ["max_entries_per_window", "market_recovery_snapshots"])
@pytest.mark.parametrize("value", ["0", "-1", "1.5", "true"])
def test_invalid_entry_recovery_settings(name, value):
    with pytest.raises(ValueError):
        Settings.load(environ={"BTC5M_" + name.upper(): value})


def test_settlement_loss_replaces_transient_before_recovery(tmp_path):
    ledger = Ledger(tmp_path / "db", Settings(max_consecutive_losses=1))
    market = Market.from_event(event())
    ledger.execute(
        market,
        make_book(),
        Signal("Up", D("5"), D(".53"), "loss"),
        1800000002,
        clock=lambda: 1800000002,
    )
    ledger.trip("abnormal_spread", 1800000003)
    ledger.settle(
        market,
        dict(
            conditionId=market.condition,
            closed=True,
            umaResolutionStatus="resolved",
            outcomes=["Up", "Down"],
            clobTokenIds=["up", "down"],
            outcomePrices=["0", "1"],
        ),
        1800000301,
    )
    for n in range(100):
        ledger.observe_market_health(True, ("next-window", n), 1800000302 + n)
    assert ledger.summary()["circuit"] == "consecutive_losses"
    assert ledger.summary()["consecutive_losses"] == 1
    ledger.close()


def test_shared_outcome_cap_and_pending_exposure_after_first_entry(tmp_path):
    path = tmp_path / "db"
    settings = Settings(max_entries_per_window=2, max_exposure=D("4"))
    market = Market.from_event(event())
    ledger = Ledger(path, settings)
    assert ledger.execute(
        market,
        make_book(),
        Signal("Up", D("5"), D(".53"), "first"),
        1800000002,
        clock=lambda: 1800000002,
    )
    with pytest.raises(Rejected, match="exposure"):
        ledger.execute(
            market,
            make_book("down"),
            Signal("Down", D("5"), D(".53"), "second"),
            1800000002,
            clock=lambda: 1800000002,
        )
    assert not ledger.window_entry_limit_reached(market.slug)
    ledger.close()
    # A budget increase allows exactly the unused successful-fill slot, across outcomes.
    ledger = Ledger(path, Settings(max_entries_per_window=2))
    assert ledger.execute(
        market,
        make_book("down"),
        Signal("Down", D("5"), D(".53"), "second"),
        1800000002,
        clock=lambda: 1800000002,
    )
    assert ledger.window_entry_limit_reached(market.slug)
    assert (
        ledger.execute(
            market,
            make_book(),
            Signal("Up", D("5"), D(".53"), "third"),
            1800000002,
            clock=lambda: 1800000002,
        )
        is None
    )
    ledger.close()


def test_multi_entry_settlement_accepts_refreshed_mutable_market_constraints(tmp_path):
    from dataclasses import replace

    ledger = Ledger(tmp_path / "db", Settings(max_entries_per_window=2))
    market = Market.from_event(event())
    signal = Signal("Up", D("6"), D(".53"), "metadata refresh")
    ledger.execute(market, make_book(), signal, 1800000002, clock=lambda: 1800000002)
    refreshed = replace(market, min_size=D("6"), tick=D(".001"))
    ledger.execute(refreshed, make_book(), signal, 1800000002, clock=lambda: 1800000002)
    result = ledger.settle(
        market,
        dict(
            conditionId=market.condition,
            closed=True,
            umaResolutionStatus="resolved",
            outcomes=["Up", "Down"],
            clobTokenIds=["up", "down"],
            outcomePrices=["1", "0"],
        ),
        1800000301,
    )
    assert D(result["payout"]) == 12
    assert ledger.summary()["settlements"] == 2
    assert not ledger.pending()
    ledger.close()
