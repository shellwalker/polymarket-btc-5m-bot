"""Epoch barriers and transaction clocks; no live runtime or network."""

import asyncio
import json
from decimal import Decimal as D
from types import SimpleNamespace

import pytest
from test_domain import event, make_book

from btc5m.config import Settings
from btc5m.domain import Market, Signal
from btc5m.ledger import Ledger, Rejected


class Clocks:
    def __init__(self):
        self.wall = 1800000002.0
        self.mono = 10.0
        self.boot = 10.0

    def sample(self):
        return {
            "wall": self.wall,
            "monotonic": self.mono,
            "boottime": self.boot,
            "process_cpu": 1.0,
        }


def test_ws_first_wall_jump_cannot_refresh_old_depth():
    from btc5m.continuity import ContinuityBarrier, ContinuityChanged
    from btc5m.public import MarketStream

    clocks = Clocks()
    barrier = ContinuityBarrier(Settings(), clocks=clocks.sample)
    books = {"up": make_book(), "down": make_book("down")}
    stream = MarketStream(
        Settings(), None, books, lambda: None, clock=lambda: clocks.wall, continuity=barrier
    )
    epoch = stream.epoch
    clocks.wall += 40
    with pytest.raises(ContinuityChanged):
        stream.apply_price_change(
            {
                "timestamp": str(int(clocks.wall * 1000)),
                "price_changes": [{"asset_id": "up", "side": "BUY", "price": ".48", "size": "20"}],
            },
            set(),
        )
    assert barrier.epoch == epoch + 1 and barrier.blocked
    assert all(not b.connected and not b.asks for b in books.values())


@pytest.mark.parametrize(
    "wall,mono,boot", [(40, 0, 0), (-1, 0, 0), (0, 40, 40), (40, 40, 40), (0, -1, 0), (0, 0, -1)]
)
def test_epoch_changes_for_clock_anomalies_and_never_reuses_old_lease(wall, mono, boot):
    from btc5m.continuity import ContinuityBarrier

    clocks = Clocks()
    barrier = ContinuityBarrier(Settings(), clocks=clocks.sample)
    owner = object()
    epoch = barrier.begin(owner, {"up", "down"})
    books = {"up": make_book(), "down": make_book("down")}
    for book in books.values():
        book.continuity_epoch = epoch
        book.continuity_sync_sequence = barrier.sync_sequence
    assert barrier.complete(epoch, owner, books, clocks.wall)
    clocks.wall += wall
    clocks.mono += mono
    clocks.boot += boot
    assert not barrier.allowed(epoch, owner)
    assert barrier.epoch == epoch + 1 and barrier.blocked
    assert not barrier.complete(epoch, owner, books, clocks.wall)


def test_transaction_rechecks_current_clock_after_begin_wait(tmp_path):
    clocks = Clocks()
    ledger = Ledger(tmp_path / "test.sqlite", Settings())
    book = make_book()
    actual = ledger.db

    class WaitedConnection:
        def execute(self, sql, *args):
            result = actual.execute(sql, *args)
            if sql == "BEGIN IMMEDIATE":
                clocks.wall += 20
            return result

        def __getattr__(self, name):
            return getattr(actual, name)

        def __enter__(self):
            return actual.__enter__()

        def __exit__(self, *args):
            return actual.__exit__(*args)

    ledger.db = WaitedConnection()
    with pytest.raises(Rejected, match="stale|continuity"):
        ledger.execute(
            Market.from_event(event()),
            book,
            Signal("Up", D("5"), D(".53"), "wait"),
            1800000002.0,
            clock=lambda: clocks.wall,
        )
    assert ledger.summary()["cash"] == "1000" and ledger.summary()["fills"] == 0
    assert book.asks == {D(".51"): D("10")}
    ledger.close()


def test_transaction_rechecks_again_near_mutations(tmp_path, monkeypatch):
    clocks = Clocks()
    ledger = Ledger(tmp_path / "test.sqlite", Settings())
    original = ledger.risk_blocked_reasons

    def slow_risk(debit, now):
        reasons = original(debit, now)
        clocks.wall += 300
        return reasons

    monkeypatch.setattr(ledger, "risk_blocked_reasons", slow_risk)
    with pytest.raises(Rejected, match="window|stale"):
        ledger.execute(
            Market.from_event(event()),
            make_book(),
            Signal("Up", D("5"), D(".53"), "slow risk"),
            1800000002.0,
            clock=lambda: clocks.wall,
        )
    assert ledger.summary()["fills"] == 0 and ledger.summary()["cash"] == "1000"
    ledger.close()


def ready_epoch_service(tmp_path, clocks):
    from btc5m.continuity import ContinuityBarrier
    from btc5m.service import Service

    barrier = ContinuityBarrier(Settings(), clocks=clocks.sample)
    service = Service(
        Settings(runtime_dir=tmp_path), None, clock=lambda: clocks.wall, continuity=barrier
    )
    service.market = Market.from_event(event())
    service.books = {"up": make_book(), "down": make_book("down")}
    service.stream = SimpleNamespace(
        ready=True, reconnects=0, gap_started=None, last_gap_seconds=None
    )
    epoch = barrier.begin(service.stream, set(service.books))
    for book in service.books.values():
        book.continuity_epoch = epoch
        book.continuity_sync_sequence = barrier.sync_sequence
    assert barrier.complete(epoch, service.stream, service.books, clocks.wall)
    service.ledger = Ledger(tmp_path / "test.sqlite", service.settings)
    service.strategy = SimpleNamespace(signal=lambda *a: Signal("Up", D("5"), D(".53"), "test"))
    return service


def test_jump_after_signal_rejects_before_fill_and_preserves_audit(tmp_path):
    clocks = Clocks()
    service = ready_epoch_service(tmp_path, clocks)
    record = service.record

    def delayed_audit(kind, details):
        record(kind, details)
        if kind == "signal":
            clocks.wall += 20

    service.record = delayed_audit
    service.update()
    summary = service.ledger.summary()
    assert summary["fills"] == 0 and summary["cash"] == "1000"
    assert service.continuity.blocked and service.continuity.epoch == 1
    attempt = json.loads(
        service.ledger.db.execute(
            "SELECT payload FROM events WHERE kind='order_attempt'"
        ).fetchone()[0]
    )
    rejected = json.loads(
        service.ledger.db.execute(
            "SELECT payload FROM events WHERE kind='order_result'"
        ).fetchone()[0]
    )
    assert attempt["decision_at"] == 1800000002 and attempt["order_at"] == 1800000022
    assert attempt["continuity_epoch"] == 0 and not attempt["order_quote"]["healthy"]
    assert rejected["status"] == "rejected" and "continuity" in rejected["reason"]
    assert not any(b.asks for b in service.books.values())
    service.ledger.close()


def test_jump_in_transaction_wait_trips_shared_epoch_without_nested_commits(tmp_path):
    clocks = Clocks()
    service = ready_epoch_service(tmp_path, clocks)
    actual = service.ledger.db

    class WaitedConnection:
        def execute(self, sql, *args):
            result = actual.execute(sql, *args)
            if sql == "BEGIN IMMEDIATE":
                clocks.wall += 40
            return result

        def __getattr__(self, name):
            return getattr(actual, name)

        def __enter__(self):
            return actual.__enter__()

        def __exit__(self, *args):
            return actual.__exit__(*args)

    service.ledger.db = WaitedConnection()
    service.update()
    assert service.continuity.epoch == 1 and service.continuity.blocked
    assert service.ledger.summary()["fills"] == 0
    assert not actual.in_transaction
    # Barrier event can be persisted later, outside the rolled-back trade transaction.
    service.emit_heartbeat()
    assert service.ledger.summary()["event_counts"]["continuity_barrier"] == 1
    assert service.ledger.summary()["cash"] == "1000"
    service.ledger.close()


def test_default_transaction_clock_is_not_decision_at(tmp_path, monkeypatch):
    ledger = Ledger(tmp_path / "test.sqlite", Settings())
    monkeypatch.setattr("btc5m.ledger.time.time", lambda: 1800000302.0)
    with pytest.raises(Rejected, match="window"):
        ledger.execute(
            Market.from_event(event()),
            make_book(),
            Signal("Up", D("5"), D(".53"), "late"),
            1800000002.0,
        )
    assert ledger.summary()["fills"] == 0
    ledger.close()


def test_ledger_rejects_epoch_mismatched_book_even_if_guard_returns_true(tmp_path):
    ledger = Ledger(tmp_path / "test.sqlite", Settings())
    book = make_book()
    book.continuity_epoch = 1
    with pytest.raises(Rejected, match="epoch"):
        ledger.execute(
            Market.from_event(event()),
            book,
            Signal("Up", D("5"), D(".53"), "epoch"),
            1800000002.0,
            clock=lambda: 1800000002.0,
            audit={"continuity_epoch": 2},
            continuity_check=lambda: True,
        )
    assert ledger.summary()["fills"] == 0
    ledger.close()


@pytest.mark.asyncio
async def test_jump_during_rest_wait_discards_snapshot_and_never_subscribes(tmp_path):
    from btc5m.continuity import ContinuityBarrier
    from btc5m.service import Service

    clocks = Clocks()
    barrier = ContinuityBarrier(Settings(), clocks=clocks.sample)
    waiting, release = asyncio.Event(), asyncio.Event()
    connections = []

    class API:
        async def snapshot(self, token):
            waiting.set()
            await release.wait()
            return {
                "asset_id": token,
                "timestamp": str(int(clocks.wall * 1000)),
                "bids": [{"price": ".49", "size": "10"}],
                "asks": [{"price": ".51", "size": "10"}],
            }

    class Forbidden:
        async def __aenter__(self):
            connections.append(True)
            raise AssertionError("old-epoch snapshots must not open a WS subscription")

        async def __aexit__(self, *args):
            pass

    service = Service(
        Settings(runtime_dir=tmp_path),
        API(),
        clock=lambda: clocks.wall,
        continuity=barrier,
        connect=lambda *a, **k: Forbidden(),
    )
    await service.switch(Market.from_event(event()))
    old = service.stream
    try:
        await asyncio.wait_for(waiting.wait(), 1)
        clocks.wall += 40
        release.set()
        await asyncio.wait_for(service.stream_task, 1)
        assert barrier.epoch == 1 and barrier.blocked and service.recovery_needed
        assert old.retired and not connections
        assert all(not b.connected and not b.asks for b in old.books.values())
    finally:
        release.set()
        await asyncio.gather(service.stream_task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("manual_latch", [False, True])
async def test_ws_first_gap_reopens_only_after_new_dual_dump_and_confirmed_close(
    tmp_path, manual_latch
):
    from btc5m.continuity import ContinuityBarrier
    from btc5m.service import Service

    clocks = Clocks()
    settings = Settings(runtime_dir=tmp_path, discovery_seconds=0.01, cleanup_seconds=0.1)
    barrier = ContinuityBarrier(settings, clocks=clocks.sample)
    connected = []
    closed = []
    active = [False]
    gap_seen = asyncio.Event()
    first_new_book = asyncio.Event()
    allow_second = asyncio.Event()
    ready = asyncio.Event()
    market = Market.from_event(event())

    def snapshot(token, ask=".51"):
        return {
            "event_type": "book",
            "asset_id": token,
            "timestamp": str(int(clocks.wall * 1000)),
            "bids": [{"price": str(D(ask) - D(".01")), "size": "20"}],
            "asks": [{"price": ask, "size": "20"}],
        }

    class API:
        async def snapshot(self, token):
            return snapshot(token, ".61" if gap_seen.is_set() else ".51")

    class WS:
        def __init__(self, number):
            self.number, self.n = number, 0

        async def send(self, raw):
            pass

        async def recv(self):
            await asyncio.sleep(0.001)
            self.n += 1
            if self.number == 1:
                if self.n == 1:
                    return json.dumps([snapshot(t) for t in ["up", "down"]])
                if self.n == 2:
                    clocks.wall += 40
                    gap_seen.set()
                    return json.dumps(
                        {
                            "event_type": "price_change",
                            "timestamp": str(int(clocks.wall * 1000)),
                            "price_changes": [
                                {"asset_id": "up", "side": "BUY", "price": ".48", "size": "20"}
                            ],
                        }
                    )
            elif self.n == 1:
                first_new_book.set()
                return json.dumps(snapshot("up", ".61"))
            elif self.n == 2:
                await allow_second.wait()
                return json.dumps(snapshot("down", ".61"))
            await asyncio.Event().wait()

    class Context:
        async def __aenter__(self):
            assert not active[0]
            active[0] = True
            connected.append(True)
            return WS(len(connected))

        async def __aexit__(self, *args):
            await asyncio.sleep(0.003)
            active[0] = False
            closed.append(True)

    service = Service(
        settings,
        API(),
        clock=lambda: clocks.wall,
        continuity=barrier,
        connect=lambda *a, **k: Context(),
    )
    service.ledger = Ledger(tmp_path / "test.sqlite", settings)
    if manual_latch:
        service.ledger.trip("consecutive_losses", clocks.wall)

    def signal(*args):
        if gap_seen.is_set():
            ready.set()
            return Signal("Up", D("5"), D(".63"), "new epoch")
        return None

    service.strategy = SimpleNamespace(signal=signal)
    await service.switch(market)
    old = service.stream
    callback = old.on_update
    try:
        await asyncio.wait_for(gap_seen.wait(), 1)
        # No diagnostic/main poll: the WS-first callback must already have fenced itself.
        for _ in range(100):
            if old.retired:
                break
            await asyncio.sleep(0.001)
        assert old.retired and barrier.blocked and barrier.epoch == 1
        assert service.ledger.summary()["fills"] == 0
        assert not old.books["up"].asks
        await service.recover_continuity()
        await asyncio.wait_for(first_new_book.wait(), 1)
        await asyncio.sleep(0.005)
        assert len(connected) == 2 and len(closed) == 1
        assert barrier.blocked and not service.stream.ready
        service.update()
        assert service.ledger.summary()["fills"] == 0
        # Old generation cannot cause update even after a new subscription exists.
        actual_update = service.update
        calls = []
        service.update = lambda: calls.append(True)
        callback()
        service.update = actual_update
        assert calls == []
        old.apply_snapshot("up", snapshot("up", ".31"), initial=True)
        old.apply_price_change(
            {
                "timestamp": str(int(clocks.wall * 1000)),
                "price_changes": [{"asset_id": "up", "side": "SELL", "price": ".31", "size": "50"}],
            },
            set(),
        )
        assert not old.books["up"].asks
        allow_second.set()
        await asyncio.wait_for(ready.wait(), 1)
        assert not barrier.blocked and service.stream.ready
        assert all(b.continuity_epoch == 1 for b in service.books.values())
        assert service.ledger.summary()["fills"] == (0 if manual_latch else 1)
        assert service.ledger.summary()["circuit"] == ("consecutive_losses" if manual_latch else "")
        if not manual_latch:
            fill = json.loads(
                service.ledger.db.execute(
                    "SELECT payload FROM events WHERE kind='paper_fill'"
                ).fetchone()[0]
            )
            assert fill["continuity_epoch"] == 1
            assert D(fill["average_price"]) == D(".61")  # old .51 asks never reused
            assert fill["transaction_at"] >= fill["order_at"] >= fill["signal_at"]
            assert fill["commit_check_at"] >= fill["execution_at"]
    finally:
        service.closing = True
        allow_second.set()
        await service.supervisor.cancel([service.stream_task], "test.cleanup")
        await asyncio.gather(service.stream_task, return_exceptions=True)
        service.ledger.close()


def test_new_subscription_same_epoch_cannot_reuse_prior_full_dump():
    from btc5m.continuity import ContinuityBarrier

    clocks = Clocks()
    barrier = ContinuityBarrier(Settings(), clocks=clocks.sample)
    old, new = object(), object()
    epoch = barrier.begin(old, {"up", "down"})
    books = {"up": make_book(), "down": make_book("down")}
    for book in books.values():
        book.continuity_epoch = epoch
        book.continuity_sync_sequence = barrier.sync_sequence
    assert barrier.complete(epoch, old, books, clocks.wall)
    assert barrier.begin(new, {"up", "down"}) == epoch
    assert not barrier.complete(epoch, old, books, clocks.wall)
    assert not barrier.complete(epoch, new, books, clocks.wall)
    books["up"].continuity_sync_sequence = barrier.sync_sequence
    assert not barrier.complete(epoch, new, books, clocks.wall)
    books["down"].continuity_sync_sequence = barrier.sync_sequence
    assert barrier.complete(epoch, new, books, clocks.wall)


def test_post_mutation_gap_rolls_back_fill_cash_and_allowance(tmp_path):
    clocks = Clocks()
    service = ready_epoch_service(tmp_path, clocks)
    actual = service.ledger.db

    class GapConnection:
        def execute(self, sql, *args):
            result = actual.execute(sql, *args)
            if sql.startswith("INSERT INTO fills"):
                clocks.wall += 40
            return result

        def __getattr__(self, name):
            return getattr(actual, name)

        def __enter__(self):
            return actual.__enter__()

        def __exit__(self, *args):
            return actual.__exit__(*args)

    service.ledger.db = GapConnection()
    service.update()
    summary = service.ledger.summary()
    assert summary["fills"] == 0 and summary["cash"] == "1000" and summary["exposure"] == "0"
    assert not service.ledger.has_order(service.market.slug)
    assert "paper_fill" not in summary["event_counts"]
    assert service.continuity.blocked
    service.ledger.close()


@pytest.mark.asyncio
async def test_jump_during_ws_initial_wait_discards_full_dump_from_old_request(tmp_path):
    from btc5m.continuity import ContinuityBarrier
    from btc5m.service import Service

    clocks = Clocks()
    barrier = ContinuityBarrier(Settings(), clocks=clocks.sample)
    waiting, release = asyncio.Event(), asyncio.Event()
    closed = []

    def snapshot(token):
        return {
            "event_type": "book",
            "asset_id": token,
            "timestamp": str(int(clocks.wall * 1000)),
            "bids": [{"price": ".59", "size": "10"}],
            "asks": [{"price": ".61", "size": "10"}],
        }

    class API:
        async def snapshot(self, token):
            return snapshot(token)

    class WS:
        async def send(self, raw):
            pass

        async def recv(self):
            waiting.set()
            await release.wait()
            return json.dumps([snapshot(t) for t in ["up", "down"]])

    class Context:
        async def __aenter__(self):
            return WS()

        async def __aexit__(self, *args):
            closed.append(True)

    service = Service(
        Settings(runtime_dir=tmp_path),
        API(),
        clock=lambda: clocks.wall,
        continuity=barrier,
        connect=lambda *a, **k: Context(),
    )
    await service.switch(Market.from_event(event()))
    try:
        await asyncio.wait_for(waiting.wait(), 1)
        clocks.wall += 40
        release.set()
        await asyncio.wait_for(service.stream_task, 1)
        assert barrier.epoch == 1 and barrier.blocked and closed == [True]
        assert service.stream.retired
        assert all(not b.asks and b.continuity_epoch is None for b in service.books.values())
    finally:
        release.set()
        await asyncio.gather(service.stream_task, return_exceptions=True)


def test_success_audit_preserves_decision_and_samples_execution_clock(tmp_path):
    ledger = Ledger(tmp_path / "test.sqlite", Settings())
    book = make_book()
    ticks = iter([1800000002.0 + i * 0.1 for i in range(8)])

    def clock():
        return next(ticks)

    fill = ledger.execute(
        Market.from_event(event()),
        book,
        Signal("Up", D("5"), D(".53"), "audit"),
        1800000001.0,
        clock=clock,
    )
    assert fill["decision_at"] == 1800000001.0
    assert fill["order_at"] == 1800000002.0
    assert fill["transaction_at"] == 1800000002.1
    assert fill["execution_at"] >= fill["transaction_at"]
    assert fill["commit_check_at"] >= fill["execution_at"]
    assert fill["execution_quote"]["healthy"]
    ledger.close()


def test_gap_during_paper_fill_audit_insert_is_rechecked_before_commit(tmp_path, monkeypatch):
    clocks = Clocks()
    service = ready_epoch_service(tmp_path, clocks)
    original = service.ledger._event

    def slow_event(kind, payload, now):
        original(kind, payload, now)
        if kind == "paper_fill":
            clocks.wall += 400

    monkeypatch.setattr(service.ledger, "_event", slow_event)
    service.update()
    summary = service.ledger.summary()
    assert summary["fills"] == 0 and summary["cash"] == "1000" and summary["exposure"] == "0"
    assert "paper_fill" not in summary["event_counts"]
    assert not service.ledger.has_order(service.market.slug)
    assert service.continuity.epoch == 1 and service.continuity.blocked
    service.ledger.close()


def test_last_guard_callback_wait_cannot_leave_its_old_clock_authoritative(tmp_path, monkeypatch):
    clocks = Clocks()
    ledger = Ledger(tmp_path / "test.sqlite", Settings())
    audit_written, last_risk_done = [False], [False]
    guards = [0]
    original_event, original_risk = ledger._event, ledger.risk_blocked_reasons

    def audit(kind, payload, now):
        original_event(kind, payload, now)
        if kind == "paper_fill":
            audit_written[0] = True

    def risk(debit, now):
        reasons = original_risk(debit, now)
        if audit_written[0]:
            last_risk_done[0] = True
        return reasons

    def guard():
        if last_risk_done[0]:
            guards[0] += 1
            if guards[0] == 2:
                clocks.wall += 400  # e.g. stat/wait in the final predicate
        return True

    monkeypatch.setattr(ledger, "_event", audit)
    monkeypatch.setattr(ledger, "risk_blocked_reasons", risk)
    with pytest.raises(Rejected, match="window|stale"):
        ledger.execute(
            Market.from_event(event()),
            make_book(),
            Signal("Up", D("5"), D(".53"), "guard"),
            1800000002.0,
            clock=lambda: clocks.wall,
            continuity_check=guard,
        )
    assert ledger.summary()["fills"] == 0 and ledger.summary()["cash"] == "1000"
    ledger.close()


def test_health_exposes_closed_epoch_without_clearing_manual_latch(tmp_path):
    clocks = Clocks()
    service = ready_epoch_service(tmp_path, clocks)
    service.ledger.trip("consecutive_losses", clocks.wall)
    clocks.wall += 40
    service.health()
    health = json.loads((tmp_path / "health.json").read_text())
    state = health["reliability"]["continuity"]
    assert state["epoch"] == 1 and state["blocked"] and state["state"] == "gap_detected"
    assert health["reliability"]["continuity_recovery_needed"]
    assert not health["trading_ready"]
    assert health["summary"]["circuit"] == "consecutive_losses"
    service.ledger.close()
