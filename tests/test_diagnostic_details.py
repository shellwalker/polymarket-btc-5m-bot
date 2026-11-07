"""Bounded failure explanations and non-atomic clock diagnostics, entirely offline."""

import json
from decimal import Decimal as D

import pytest
from test_domain import event, make_book

from btc5m.config import Settings
from btc5m.continuity import ContinuityBarrier
from btc5m.domain import Market


def resync_fixture():
    def clocks():
        return {"wall": 1800000002.0, "monotonic": 10.0, "boottime": 10.0, "process_cpu": 1.0}

    barrier = ContinuityBarrier(Settings(), clocks=clocks)
    owner = object()
    epoch = barrier.begin(owner, {"up", "down"})
    books = {"up": make_book(), "down": make_book("down")}
    for book in books.values():
        book.continuity_epoch = epoch
        book.continuity_sync_sequence = barrier.sync_sequence
    return barrier, owner, epoch, books


@pytest.mark.parametrize(
    "failure,code",
    [
        ("epoch", "epoch_mismatch"),
        ("lease", "lease_mismatch"),
        ("missing", "tokens_mismatch"),
        ("empty", "no_books"),
        ("book_epoch", "book_epoch_mismatch"),
        ("sequence", "book_sync_sequence_mismatch"),
        ("disconnected", "disconnected"),
        ("tick", "invalid_depth"),
        ("bids", "empty_bids"),
        ("asks", "empty_asks"),
        ("crossed", "crossed_or_locked"),
        ("received_future", "received_time_future"),
        ("received_stale", "received_stale"),
        ("source_future", "source_time_future"),
        ("source_stale", "source_stale"),
    ],
)
@pytest.mark.parametrize("token", ["up", "down"])
def test_resync_reports_all_failed_predicates_without_opening_gate(failure, code, token):
    barrier, owner, epoch, books = resync_fixture()
    if failure == "epoch":
        epoch += 1
    elif failure == "lease":
        owner = object()
    elif failure == "missing":
        books.pop("down")
    elif failure == "empty":
        books.clear()
    elif failure == "book_epoch":
        books[token].continuity_epoch = -1
    elif failure == "sequence":
        books[token].continuity_sync_sequence = -1
    elif failure == "disconnected":
        books[token].connected = False
    elif failure == "tick":
        books[token].tick = D(".03")
    elif failure == "bids":
        books[token].bids.clear()
    elif failure == "asks":
        books[token].asks.clear()
    elif failure == "crossed":
        books[token].bids = {D(".51"): D("5")}
    elif failure == "received_future":
        books[token].updated_at = 1800000003.0
    elif failure == "received_stale":
        books[token].updated_at = 1799999990.0
    elif failure == "source_future":
        books[token].source_time = 1800000005.0
    elif failure == "source_stale":
        books[token].source_time = 1799999990.0
    assert not barrier.complete(epoch, owner, books, 1800000002.0)
    report = barrier.last_resync_failure
    assert code in report["reason_codes"]
    assert barrier.blocked
    assert report["expected_tokens"] == ["down", "up"]
    assert report["epochs"]["supplied"] == epoch
    assert report["epochs"]["current"] == barrier.epoch
    assert len(report["tokens"]) <= 2
    assert "object at" not in json.dumps(report)

    if failure not in {"epoch", "lease", "missing", "empty"}:
        details = {detail["token"]: detail for detail in report["tokens"]}
        assert code in details[token]["reason_codes"]
        other = "down" if token == "up" else "up"
        assert not details[other]["reason_codes"]


def test_resync_reports_multiple_predicates_and_metrics_simultaneously():
    barrier, owner, epoch, books = resync_fixture()
    books["up"].connected = False
    books["up"].bids.clear()
    books["up"].source_time -= 10
    books["down"].continuity_sync_sequence = -1
    assert not barrier.complete(epoch, owner, books, 1800000002.0)
    report = barrier.last_resync_failure
    assert {
        "disconnected",
        "empty_bids",
        "missing_best_bid",
        "source_stale",
        "book_sync_sequence_mismatch",
    } <= set(report["reason_codes"])
    up = next(t for t in report["tokens"] if t["token"] == "up")
    assert up["bid_levels"] == 0 and up["ask_levels"] == 1
    assert up["best_bid"] is None and up["best_ask"] == "0.51"
    assert up["source_age_seconds"] == 11 and up["received_age_seconds"] == 1
    assert up["epoch"] == epoch and up["sync_sequence"] == 1


def test_clock_sample_brackets_interruption_and_preserves_raw_first_reads():
    from btc5m.reliability import clock_sample

    def provider(values):
        iterator = iter(values)
        return lambda: next(iterator)

    sample = clock_sample(
        wall=provider([1000.0, 1400.0]),
        monotonic=provider([10.0, 410.0]),
        boottime=provider([410.0, 410.0]),
        cpu=provider([1.0, 1.0]),
    )
    assert sample["wall"] == sample["sample_wall"] == 1000.0
    assert sample["monotonic"] == 10.0 and sample["boottime"] == 410.0
    assert sample["sample_completed_at"] == 1400.0
    assert not sample["sample_consistent"]
    assert sample["sampling_spans"]["wall"] == 400.0
    assert sample["sampling_spans"]["monotonic"] == 400.0
    assert sample["sampling_spans"]["boottime"] == 0


def test_mixed_sample_epochs_remain_fail_closed_without_deduplication():
    state = {"wall": 1000.0, "monotonic": 10.0, "boottime": 10.0, "process_cpu": 1.0}
    barrier = ContinuityBarrier(Settings(), clocks=lambda: dict(state))
    state.update(
        wall=1000.0,
        monotonic=10.0,
        boottime=410.0,
        sample_wall=1000.0,
        sample_completed_at=1400.0,
        sample_consistent=False,
    )
    barrier.poll()
    assert barrier.epoch == 1 and barrier.blocked
    assert barrier.last_gap["sample_wall"] == 1000.0
    assert barrier.last_gap["detected_at"] == 1400.0
    state.update(wall=1400.0, monotonic=410.0, sample_wall=1400.0, sample_consistent=True)
    barrier.poll()
    assert barrier.epoch == 2 and barrier.blocked
    assert len(barrier.events) == 2


def test_btime_change_has_old_new_details_without_process_restart_claim():
    from btc5m.reliability import Diagnostics

    state = {"wall": 1000.0, "monotonic": 10.0, "boottime": 12.0, "process_cpu": 1.0}
    identity = {"pid": 7, "sid": 7, "start_ticks": 123, "boot_id": "same", "btime": 988}
    diag = Diagnostics(Settings(), clocks=lambda: dict(state), identity=lambda: dict(identity))
    diag.heartbeat("run")
    state.update(wall=1015.0, monotonic=25.0, boottime=27.0)
    identity["btime"] += 3
    beat, gap = diag.heartbeat("run")
    assert gap["changed_fields"]["btime"] == {"old": 988, "new": 991}
    assert gap["btime_delta"] == 3 and not gap["process_identity_changed"]
    assert beat["identity"]["uptime"]["seconds"] == 27
    assert beat["identity"]["uptime"]["source"] == "sampled CLOCK_BOOTTIME"
    assert gap["cause"] == "unknown" and "restart" not in gap
    state.update(wall=1030.0, monotonic=40.0, boottime=42.0)
    _, gap = diag.heartbeat("run")
    assert gap is None  # naturally advancing uptime must not look like identity change


def test_bounded_failure_log_before_invalidation_and_success_silent(caplog):
    from btc5m.public import MarketStream
    from btc5m.resync_diagnostics import MAX_LOG_BYTES, bounded

    barrier, owner, epoch, books = resync_fixture()
    books["up"].asks.clear()
    assert not barrier.complete(epoch, owner, books, 1800000002.0)
    stream = MarketStream(
        Settings(),
        None,
        books,
        lambda: None,
        continuity=barrier,
        clock=lambda: 1800000002.0,
        market=Market.from_event(event()),
    )
    stream.report_resync_failure()
    report = next(r.details for r in caplog.records if r.message == "ws_resync_incomplete")
    assert books["up"].bids  # still available when diagnostics are emitted
    assert report["market"]["remaining_seconds"] == 298
    assert "empty_asks" in report["reason_codes"]
    assert (
        len(json.dumps(report, ensure_ascii=False, separators=(",", ":")).encode()) <= MAX_LOG_BYTES
    )
    oversized = bounded({**report, "optional": "界" * 100000})
    assert (
        len(json.dumps(oversized, ensure_ascii=False, separators=(",", ":")).encode())
        <= MAX_LOG_BYTES
    )
    assert oversized["reason_codes"] == report["reason_codes"]
    caplog.clear()
    books["up"] = make_book()
    books["up"].continuity_epoch = epoch
    books["up"].continuity_sync_sequence = barrier.sync_sequence
    assert barrier.complete(epoch, owner, books, 1800000002.0)
    assert not caplog.records


def test_abnormal_created_uses_detection_not_sample_and_preserves_legacy(tmp_path):
    from btc5m.ledger import Ledger
    from btc5m.reliability import Diagnostics
    from btc5m.service import Service

    state = {"wall": 1000.0, "monotonic": 10.0, "boottime": 10.0, "process_cpu": 1.0}
    settings = Settings(runtime_dir=tmp_path)
    diag = Diagnostics(
        settings, clocks=lambda: dict(state), identity=lambda: {}, wall_clock=lambda: 1401.0
    )
    service = Service(settings, None, diagnostics=diag, run_id="new")
    service.continuity = ContinuityBarrier(settings, clocks=lambda: dict(state))
    service.continuity.wall_clock = lambda: 1401.0
    ledger = service.ledger = Ledger(tmp_path / "fixture.sqlite", settings, run_id="new")
    ledger.event("legacy", {"wall": 999.0}, 999.0)
    ledger.trip("consecutive_losses", 999.0)
    original = [tuple(row) for row in ledger.db.execute("SELECT * FROM events")]
    service.emit_heartbeat()
    state.update(boottime=410.0, sample_consistent=False, sample_completed_at=1400.0)
    service.emit_heartbeat()
    rows = ledger.db.execute(
        "SELECT created,payload FROM events WHERE kind IN ('continuity_gap','continuity_barrier')"
    ).fetchall()
    assert rows
    for created, payload in rows:
        data = json.loads(payload)
        assert created == data["detected_at"] == 1401.0
        assert data["sample_wall"] == 1000.0
        assert "emitted_at" in data
    assert [tuple(row) for row in ledger.db.execute("SELECT * FROM events LIMIT 2")] == original
    assert ledger.summary()["circuit"] == "consecutive_losses"
    ledger.close()


@pytest.mark.asyncio
async def test_actual_dual_dump_failure_reports_once_before_clear(caplog):
    import asyncio

    from btc5m.public import MarketStream

    barrier, _, _, books = resync_fixture()
    stop = asyncio.Event()
    snapshots = []

    class API:
        async def snapshot(self, token):
            return {
                "asset_id": token,
                "timestamp": "1800000002000",
                "bids": [{"price": ".49", "size": "10"}],
                "asks": [{"price": ".51", "size": "10"}],
            }

    class WS:
        async def send(self, raw):
            pass

        async def recv(self):
            token = ["up", "down"][len(snapshots)]
            snapshots.append(token)
            data = await API().snapshot(token)
            if token == "up":
                data["asks"] = []
            return json.dumps({"event_type": "book", **data})

    class Context:
        async def __aenter__(self):
            return WS()

        async def __aexit__(self, *args):
            stop.set()

    stream = MarketStream(
        Settings(),
        API(),
        books,
        lambda: None,
        continuity=barrier,
        clock=lambda: 1800000002.0,
        connect=lambda *a, **k: Context(),
    )
    await asyncio.wait_for(stream.run(stop), 1.0)
    reports = [r.details for r in caplog.records if r.message == "ws_resync_incomplete"]
    assert len(reports) == 1
    assert reports[0]["missing_tokens"] == []
    assert "empty_asks" in reports[0]["reason_codes"]
    assert next(t for t in reports[0]["tokens"] if t["token"] == "up")["bid_levels"] == 1
    assert barrier.blocked and not stream.ready
    assert all(not book.connected for book in books.values())
    assert (
        next(r.details["error"] for r in caplog.records if r.message == "ws_reconnect")
        == "current-epoch dual-token resync incomplete"
    )


@pytest.mark.parametrize(
    "field,code",
    [("updated_at", "received_age_nonfinite"), ("source_time", "source_age_nonfinite")],
)
@pytest.mark.parametrize("token", ["up", "down"])
def test_nonfinite_ages_cannot_open_gate(field, code, token):
    barrier, owner, epoch, books = resync_fixture()
    setattr(books[token], field, float("nan"))
    assert not barrier.complete(epoch, owner, books, 1800000002.0)
    assert code in barrier.last_resync_failure["reason_codes"]
    assert barrier.blocked

    details = {detail["token"]: detail for detail in barrier.last_resync_failure["tokens"]}
    assert code in details[token]["reason_codes"]
    other = "down" if token == "up" else "up"
    assert not details[other]["reason_codes"]


def test_legacy_clock_provider_and_coherent_bracket():
    from btc5m.reliability import clock_sample

    sample = clock_sample(
        wall=lambda: 1000.0, monotonic=lambda: 10.0, boottime=lambda: 10.0, cpu=lambda: 1.0
    )
    assert sample["sample_consistent"] and not sample["sample_reason_codes"]
    barrier, owner, epoch, books = resync_fixture()
    assert barrier.complete(epoch, owner, books, 1800000002.0)
    assert not barrier.blocked


@pytest.mark.parametrize("length,truncated", [(78, False), (129, True)])
def test_diagnostic_identifier_limit_separate_from_text_and_byte_budget(length, truncated):
    from btc5m.resync_diagnostics import MAX_LOG_BYTES, bounded, failure_details

    ids = ["1" * length, "2" * length]
    books = {token: make_book(token) for token in ids}
    report = failure_details(set(ids), books, 1800000002.0, 5.0, 0, 0, 1, True, True, True)
    assert report["expected_tokens"] == report["actual_tokens"] == [t[:128] for t in ids]
    assert report["token_text_truncated"] is truncated
    for token, detail in zip(ids, report["tokens"], strict=True):
        assert detail["token"] == detail["asset_id"] == token[:128]
        assert detail["token_truncated"] is truncated
        assert detail["asset_id_truncated"] is truncated
    for payload in [report, {**report, "optional": "界" * 100000}]:
        result = bounded(payload)
        assert result["expected_tokens"] == [t[:128] for t in ids]
        assert result["tokens"][0]["asset_id"] == ids[0][:128]
        assert (
            len(json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode())
            <= MAX_LOG_BYTES
        )
