"""Reliability diagnostics use only controlled clocks, transports and temporary ledgers."""

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest

from btc5m.config import Settings


def test_clock_gaps_identity_and_new_run_do_not_invent_history():
    from btc5m.reliability import Diagnostics

    values = [1000.0, 10.0, 10.0, 1.0]

    def clocks():
        return dict(zip(("wall", "monotonic", "boottime", "process_cpu"), values, strict=True))

    def identity():
        return {"pid": 1, "sid": 1, "start_ticks": 123, "btime": 990, "boot_id": "boot"}

    diag = Diagnostics(Settings(), clocks=clocks, identity=identity)
    first, gap = diag.heartbeat("a")
    assert gap is None and first["identity"]["start_ticks"] == 123
    diag.progress("discovery", "request")
    values[:] = [2500.0, 1510.0, 1510.0, 2.0]
    beat, gap = diag.heartbeat("a")
    assert gap["deltas"]["wall"] == 1500 and gap["deltas"]["monotonic"] == 1500
    assert gap["recovered_at"] == 2500
    assert gap["skipped_windows"] == [
        "btc-updown-5m-1200",
        "btc-updown-5m-1500",
        "btc-updown-5m-1800",
        "btc-updown-5m-2100",
    ]
    assert beat["workers"]["discovery"]["sequence"] == 1
    values[:] = [4000.0, 1511.0, 1511.0, 2.1]
    _, gap = diag.heartbeat("a")
    assert gap["wall_minus_monotonic_delta"] == 1499
    assert gap["cause"] == "unknown"
    assert diag.heartbeat("b")[1] is None


@pytest.mark.asyncio
async def test_http_complete_slow_body_and_retry_total_deadlines(caplog):
    from btc5m.public import PublicAPI
    from btc5m.reliability import OperationDeadline

    closed = []

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            while True:
                await asyncio.sleep(0.005)
                yield b" "

        async def aclose(self):
            closed.append(True)

    async def handle(request):
        return httpx.Response(200, stream=Body())

    settings = Settings(
        http_response_seconds=0.025,
        http_total_seconds=0.055,
        cleanup_seconds=0.02,
        retry_initial=0.001,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        api = PublicAPI(settings, client)
        start = asyncio.get_running_loop().time()
        with pytest.raises(OperationDeadline):
            await api.get("clob", "/time")
        assert asyncio.get_running_loop().time() - start < 0.25
    assert closed
    calls = []

    async def retry(request):
        calls.append(True)
        return httpx.Response(503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(retry)) as client:
        api = PublicAPI(Settings(http_total_seconds=0.025, retry_initial=0.1), client)
        with pytest.raises(OperationDeadline):
            await api.get("clob", "/time")
    assert len(calls) == 1
    assert any(getattr(r, "details", {}).get("operation") == "http.total" for r in caplog.records)


@pytest.mark.asyncio
async def test_noncooperative_deadline_reports_inability_without_wait_for_hang(caplog):
    from btc5m.reliability import CleanupIncomplete, Supervisor

    release = asyncio.Event()

    async def resistant():
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                pass

    supervisor = Supervisor(0.01)
    start = asyncio.get_running_loop().time()
    try:
        with pytest.raises(CleanupIncomplete):
            await supervisor.run(resistant(), 0.01, "test")
        assert asyncio.get_running_loop().time() - start < 0.2
        assert supervisor.unsafe and supervisor.pending
        deadline = next(r.details for r in caplog.records if r.message == "operation_deadline")
        cleanup = next(r.details for r in caplog.records if r.message == "cleanup_incomplete")
        assert deadline["cleanup_elapsed_seconds"] >= 0.01
        assert cleanup["cleanup_elapsed_seconds"] >= 0.01
        assert deadline["elapsed_seconds"] >= deadline["operation_elapsed_seconds"]
        assert deadline["outcome"] == "cleanup_incomplete"
    finally:
        release.set()
        await asyncio.gather(*supervisor.pending, return_exceptions=True)


def test_slow_diagnostics_are_throttled_and_cannot_recurse():
    from btc5m.reliability import SlowOperations

    now = [0.0]
    events = []
    slow = None

    def sink(payload):
        events.append(payload)
        with slow.measure("log.emit"):
            now[0] += 1

    slow = SlowOperations(0.1, 60, clock=lambda: now[0], sink=sink)
    for _ in range(3):
        with slow.measure("db.commit"):
            now[0] += 0.2
    assert len(events) == 1
    assert slow.stats["db.commit"]["count"] == 3
    assert "log.emit" not in slow.stats


@pytest.mark.asyncio
async def test_resistant_old_stream_quarantines_and_fences_late_callbacks(tmp_path):
    from decimal import Decimal as D

    from test_domain import event, make_book

    from btc5m.domain import Market, Signal
    from btc5m.ledger import Ledger
    from btc5m.service import Service

    release = asyncio.Event()
    received = asyncio.Event()
    late_callbacks = []
    settings = Settings(runtime_dir=tmp_path, cleanup_seconds=0.01)
    service = Service(settings, None, clock=lambda: 1800000002.0)
    service.ledger = Ledger(tmp_path / "test.sqlite", settings)
    service.ledger.trip("consecutive_losses", 1800000001.0)
    market = Market.from_event(event())
    connects = []

    class API:
        async def snapshot(self, token):
            return {
                "asset_id": token,
                "timestamp": "1800000002000",
                "bids": [{"price": ".49", "size": "10"}],
                "asks": [{"price": ".51", "size": "10"}],
            }

    class WS:
        async def send(self, data):
            pass

        async def recv(self):
            received.set()
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    pass
            return json.dumps([await API().snapshot(token) for token in market.tokens.values()])

    class Context:
        async def __aenter__(self):
            connects.append(True)
            return WS()

        async def __aexit__(self, *args):
            pass

    service.api = API()
    service.connect = lambda *args, **kwargs: Context()
    await service.switch(market)
    old = service.stream
    old_callback = old.on_update
    service.strategy = SimpleNamespace(signal=lambda *args: Signal("Up", D("5"), D(".53"), "late"))
    await asyncio.wait_for(received.wait(), 1)
    original = service.stream_task
    start = asyncio.get_running_loop().time()
    try:
        await service.switch(None)
        assert asyncio.get_running_loop().time() - start < 0.2
        assert service.cleanup_failed and old.retired
        await service.switch(market)
        assert len(connects) == 1
        actual_update = service.update
        service.update = lambda: late_callbacks.append(True)
        old_callback()
        assert not late_callbacks
        service.update = actual_update
        service.books = {"up": make_book(), "down": make_book("down")}
        service.market = market
        release.set()
        await asyncio.gather(original, return_exceptions=True)
        await asyncio.gather(*list(service.supervisor.pending), return_exceptions=True)
        service.update()
        assert service.ledger.summary()["fills"] == 0
        assert service.ledger.summary()["circuit"] == "consecutive_losses"
        assert "signal" not in service.ledger.summary()["event_counts"]
    finally:
        release.set()
        await asyncio.gather(original, return_exceptions=True)
        service.ledger.close()


@pytest.mark.asyncio
async def test_slow_chunked_wire_response_is_bounded(monkeypatch):
    import httpcore

    from btc5m import public
    from btc5m.reliability import OperationDeadline

    # Real HTTP/1.1 chunk decoding in httpcore/h11, with controlled network bytes.
    # No TCP listener, DNS or external network is required in the restricted sandbox.
    closed = []

    class Wire(httpcore.AsyncNetworkStream):
        def __init__(self):
            self.header = True

        async def read(self, max_bytes, timeout=None):
            if self.header:
                self.header = False
                return b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
            await asyncio.sleep(0.005)
            return b"1\r\n \r\n"

        async def write(self, buffer, timeout=None):
            pass

        async def aclose(self):
            closed.append(True)

        def get_extra_info(self, info):
            return None

    class Backend(httpcore.AsyncNetworkBackend):
        async def connect_tcp(
            self, host, port, timeout=None, local_address=None, socket_options=None
        ):
            return Wire()

    monkeypatch.setitem(public.BASES, "clob", "http://controlled.invalid")
    transport = httpx.AsyncHTTPTransport()
    transport._pool = httpcore.AsyncConnectionPool(network_backend=Backend())
    async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
        api = public.PublicAPI(
            Settings(
                http_response_seconds=0.03,
                http_total_seconds=0.05,
                cleanup_seconds=0.02,
                retry_initial=0.001,
            ),
            client,
        )
        start = asyncio.get_running_loop().time()
        with pytest.raises(OperationDeadline):
            await api.get("clob", "/time")
        assert asyncio.get_running_loop().time() - start < 0.3
    assert closed


@pytest.mark.parametrize(
    "next_clocks,wall_diff,boot_diff",
    [
        ({"wall": 1001.0, "monotonic": 11.0, "boottime": 100.0, "process_cpu": 1.1}, 0, 89),
        ({"wall": 900.0, "monotonic": 11.0, "boottime": 11.0, "process_cpu": 1.1}, -101, 0),
    ],
)
def test_boottime_and_backward_wall_anomalies_remain_neutral(next_clocks, wall_diff, boot_diff):
    from btc5m.reliability import Diagnostics

    clocks = {"wall": 1000.0, "monotonic": 10.0, "boottime": 10.0, "process_cpu": 1.0}
    diag = Diagnostics(Settings(), clocks=lambda: dict(clocks), identity=lambda: {})
    diag.heartbeat("a")
    clocks.update(next_clocks)
    _, gap = diag.heartbeat("a")
    assert gap["wall_minus_monotonic_delta"] == wall_diff
    assert gap["boottime_minus_monotonic_delta"] == boot_diff
    assert gap["cause"] == "unknown"


def test_identity_change_and_proc_start_ticks_are_raw(monkeypatch):
    from pathlib import Path

    from btc5m.reliability import Diagnostics, raw_identity

    stat = "42 (worker (paper)) " + " ".join(["S"] + [str(i) for i in range(4, 23)])
    texts = {
        "/proc/self/stat": stat,
        "/proc/stat": "cpu 1 2 3\nbtime 99\n",
        "/proc/sys/kernel/random/boot_id": "boot-a\n",
    }
    monkeypatch.setattr(Path, "read_text", lambda p: texts[str(p)])
    identity = raw_identity()
    assert identity["start_ticks"] == 22 and identity["btime"] == 99
    assert identity["boot_id"] == "boot-a"
    clocks = {"wall": 1000.0, "monotonic": 10.0, "boottime": 10.0, "process_cpu": 1.0}
    diag = Diagnostics(Settings(), clocks=lambda: dict(clocks), identity=raw_identity)
    diag.heartbeat("a")
    texts["/proc/sys/kernel/random/boot_id"] = "boot-b"
    clocks.update(wall=1015.0, monotonic=25.0, boottime=25.0)
    _, gap = diag.heartbeat("a")
    assert gap["identity_changed"] and gap["cause"] == "unknown"


@pytest.mark.asyncio
@pytest.mark.parametrize("blocked_stage", ["rest", "subscribe", "ping"])
async def test_ws_stage_budgets_do_not_overlap_connections(blocked_stage):
    from decimal import Decimal as D

    from btc5m.domain import Book
    from btc5m.public import MarketStream

    stop = asyncio.Event()
    sends = []
    connections = []
    closed = []
    settings = Settings(
        cleanup_seconds=0.02,
        ws_sync_seconds=0.01,
        ws_send_seconds=0.01,
        heartbeat_seconds=0.01,
        pong_timeout=0.05,
        retry_initial=0.001,
    )

    class API:
        async def snapshot(self, token):
            if blocked_stage == "rest":
                try:
                    await asyncio.sleep(1)
                finally:
                    stop.set()
            return {
                "asset_id": token,
                "timestamp": "1000000",
                "bids": [{"price": ".49", "size": "10"}],
                "asks": [{"price": ".51", "size": "10"}],
            }

    class WS:
        def __init__(self):
            self.first = True

        async def send(self, raw):
            sends.append(raw)
            if (blocked_stage == "subscribe" and raw != "PING") or (
                blocked_stage == "ping" and raw == "PING"
            ):
                await asyncio.sleep(1)

        async def recv(self):
            if self.first:
                self.first = False
                return json.dumps({"event_type": "book", **await API().snapshot("u")})
            await asyncio.sleep(0.002)
            return "PONG"

    class Context:
        async def __aenter__(self):
            assert not connections or closed
            connections.append(True)
            return WS()

        async def __aexit__(self, *args):
            closed.append(True)
            stop.set()

    stream = MarketStream(
        settings,
        API(),
        {"u": Book("u", D(".01"))},
        lambda: None,
        clock=lambda: 1000.0,
        connect=lambda *a, **k: Context(),
    )
    await asyncio.wait_for(stream.run(stop), 0.3)
    assert not stream.ready
    assert len(connections) == (0 if blocked_stage == "rest" else 1)
    if blocked_stage == "ping":
        assert sends.count("PING") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("health_error", [False, True])
async def test_shutdown_keeps_lock_during_resistant_cleanup_even_if_cancelled_twice(
    tmp_path, health_error
):
    from test_domain import event

    from btc5m.domain import Market
    from btc5m.ledger import Ledger
    from btc5m.service import RuntimeLock, Service, read_status

    release, receiving, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()
    market = Market.from_event(event())

    class API:
        async def get(self, *args):
            return {}

        async def discover(self, now):
            return market, None

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
            receiving.set()
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    pass
            return "PONG"

    class Context:
        async def __aenter__(self):
            return WS()

        async def __aexit__(self, *args):
            pass

    settings = Settings(runtime_dir=tmp_path, cleanup_seconds=0.01)
    service = Service(
        settings, API(), clock=lambda: 1800000002.0, connect=lambda *a, **k: Context()
    )
    task = asyncio.create_task(service.run(stop))
    try:
        await asyncio.wait_for(receiving.wait(), 1)
        service.ledger.trip("consecutive_losses", 1800000002.0)
        stop.set()
        for _ in range(100):
            if service.cleanup_failed:
                break
            await asyncio.sleep(0.002)
        assert service.cleanup_failed
        if health_error:

            def failed_health(**kwargs):
                raise OSError("injected health write failure")

            service.health = failed_health
        for _ in range(2):
            task.cancel()
            await asyncio.sleep(0.005)
        assert read_status(tmp_path)["running"]
        with pytest.raises(RuntimeError, match="already running"):
            with RuntimeLock(tmp_path):
                pass
    finally:
        release.set()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 1)
    assert not read_status(tmp_path)["running"]
    ledger = Ledger(tmp_path / "ledger.sqlite3", settings)
    assert ledger.summary()["circuit"] == "consecutive_losses"
    ledger.close()


@pytest.mark.asyncio
async def test_diagnostic_heartbeat_continues_while_startup_request_waits(tmp_path):
    from btc5m.service import Service

    stop, requested = asyncio.Event(), asyncio.Event()

    class API:
        async def get(self, *args):
            requested.set()
            await asyncio.Event().wait()

    service = Service(
        Settings(runtime_dir=tmp_path, diagnostic_heartbeat_seconds=0.01), API(), clock=lambda: 42.0
    )
    task = asyncio.create_task(service.run(stop))
    try:
        await asyncio.wait_for(requested.wait(), 1)
        await asyncio.sleep(0.05)
        rows = service.ledger.db.execute(
            "SELECT created,payload FROM events WHERE kind='heartbeat'"
        ).fetchall()
        assert len(rows) >= 2
        assert all(r["created"] != 42 for r in rows)
        assert json.loads(rows[-1]["payload"])["workers"]["diagnostics"]["sequence"] >= 2
    finally:
        stop.set()
        await asyncio.wait_for(task, 1)


def test_measured_log_handler_recursion_and_throttle(tmp_path, monkeypatch):
    import logging
    from logging.handlers import RotatingFileHandler

    from btc5m.cli import MeasuredFileHandler
    from btc5m.reliability import SlowOperations

    now, records = [0.0], []

    def emit(handler, record):
        now[0] += 1
        records.append(record.getMessage())

    monkeypatch.setattr(RotatingFileHandler, "emit", emit)
    handler = MeasuredFileHandler(tmp_path / "test.log", Settings())
    logger = logging.Logger("test", level=logging.INFO)
    logger.addHandler(handler)
    handler.slow = SlowOperations(
        0.1, 60, clock=lambda: now[0], sink=lambda payload: logger.warning("slow_operation")
    )
    try:
        logger.info("first")
        logger.info("second")
        assert records == ["first", "slow_operation", "second"]
        assert handler.slow.stats["log.emit"]["count"] == 2
    finally:
        handler.close()


def test_stop_guard_still_precedes_cleanup_telemetry_mutations(tmp_path):
    from test_review_fixes import ready_service

    service = ready_service(tmp_path)
    service.supervisor.unsafe = True
    service.stop.set()
    before = service.ledger.db.total_changes
    service.update()
    assert service.ledger.db.total_changes == before
    service.ledger.close()


def test_persistent_gap_events_leave_money_and_loss_latch_unchanged(tmp_path):
    from btc5m.ledger import Ledger
    from btc5m.reliability import Diagnostics
    from btc5m.service import Service

    values = {"wall": 1000.0, "monotonic": 10.0, "boottime": 10.0, "process_cpu": 1.0}
    settings = Settings(runtime_dir=tmp_path)
    diagnostics = Diagnostics(settings, clocks=lambda: dict(values), identity=lambda: {})
    service = Service(settings, None, clock=lambda: 42.0, diagnostics=diagnostics, run_id="a")
    service.ledger = Ledger(tmp_path / "test.sqlite", settings, run_id="a")
    service.ledger.trip("consecutive_losses", 999)
    service.emit_heartbeat()
    values.update(wall=2500.0, monotonic=1510.0, boottime=1510.0, process_cpu=2.0)
    service.emit_heartbeat()
    summary = service.ledger.summary()
    assert summary["cash"] == "1000" and summary["fills"] == 0
    assert summary["circuit"] == "consecutive_losses"
    assert summary["event_counts"]["continuity_gap"] == 1
    assert (
        service.ledger.db.execute(
            "SELECT created FROM events WHERE kind='continuity_gap'"
        ).fetchone()[0]
        == 2500
    )
    service.ledger.close()
    resumed = Service(
        settings,
        None,
        clock=lambda: 42.0,
        run_id="b",
        diagnostics=Diagnostics(settings, clocks=lambda: dict(values), identity=lambda: {}),
    )
    resumed.ledger = Ledger(tmp_path / "test.sqlite", settings, run_id="b")
    resumed.emit_heartbeat()
    assert resumed.ledger.summary()["event_counts"]["continuity_gap"] == 1
    assert resumed.ledger.summary()["circuit"] == "consecutive_losses"
    resumed.ledger.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["pong_only", "one_book"])
async def test_ws_initial_snapshot_deadline_cannot_be_extended_by_receive_activity(mode, caplog):
    from decimal import Decimal as D

    from btc5m.domain import Book
    from btc5m.public import MarketStream

    stop = asyncio.Event()
    closed = []
    activity = []
    connections = []
    callbacks = []
    settings = Settings(ws_sync_seconds=0.025, cleanup_seconds=0.02, retry_initial=0.001)

    class API:
        async def snapshot(self, token):
            return {
                "asset_id": token,
                "timestamp": "1000000",
                "bids": [{"price": ".49", "size": "10"}],
                "asks": [{"price": ".51", "size": "10"}],
            }

    class WS:
        async def send(self, raw):
            pass

        async def recv(self):
            await asyncio.sleep(0.001)
            activity.append(True)
            if mode == "one_book" and len(activity) == 1:
                return json.dumps({"event_type": "book", **await API().snapshot("u")})
            return "PONG"

    class Context:
        async def __aenter__(self):
            connections.append(True)
            return WS()

        async def __aexit__(self, *args):
            closed.append(True)
            stop.set()

    stream = MarketStream(
        settings,
        API(),
        {t: Book(t, D(".01")) for t in ["u", "d"]},
        lambda: callbacks.append(stream.ready),
        clock=lambda: 1000.0,
        connect=lambda *a, **k: Context(),
    )
    try:
        await asyncio.wait_for(stream.run(stop), 0.15)
    except TimeoutError:
        pytest.fail("PONG/partial initial dump kept synchronization alive beyond its budget")
    assert closed == [True] and connections == [True]
    assert len(activity) > 1 and not stream.ready
    assert not any(callbacks)
    assert all(not book.connected for book in stream.books.values())
    deadlines = [
        r.details
        for r in caplog.records
        if r.message == "operation_deadline" and r.details["operation"] == "ws.initial_sync"
    ]
    assert len(deadlines) == 1
    assert deadlines[0]["operation_elapsed_seconds"] >= 0.025
    assert deadlines[0]["cleanup_elapsed_seconds"] >= 0
    assert deadlines[0]["elapsed_seconds"] >= deadlines[0]["operation_elapsed_seconds"]


@pytest.mark.asyncio
async def test_operation_duration_stats_separate_cleanup_and_success_is_quiet(caplog):
    caplog.set_level("INFO", logger="btc5m")
    from btc5m.reliability import OperationDeadline, Supervisor

    supervisor = Supervisor(0.05)

    async def success():
        await asyncio.sleep(0.002)
        return 7

    for _ in range(3):
        assert await supervisor.run(success(), 0.05, "http.complete_response") == 7
    stats = supervisor.operation_durations["http.complete_response"]
    assert stats["count"] == 3 and stats["last_seconds"] > 0
    assert stats["max_seconds"] >= stats["last_seconds"]
    assert not caplog.records

    async def slow_cleanup():
        try:
            await asyncio.sleep(1)
        finally:
            await asyncio.sleep(0.01)

    with pytest.raises(OperationDeadline):
        await supervisor.run(slow_cleanup(), 0.01, "ws.subscribe")
    record = next(r.details for r in caplog.records if r.message == "operation_deadline")
    assert record["operation_elapsed_seconds"] >= 0.01
    assert record["cleanup_elapsed_seconds"] >= 0.01
    assert record["elapsed_seconds"] >= 0.02
    assert supervisor.cleanup_durations["ws.subscribe"]["last_seconds"] >= 0.01
    assert supervisor.operation_durations["ws.subscribe"]["last_outcome"] == "timeout"


@pytest.mark.asyncio
async def test_initial_snapshot_timeout_reconnects_only_after_context_cleanup():
    from decimal import Decimal as D

    from btc5m.domain import Book
    from btc5m.public import MarketStream

    stop = asyncio.Event()
    opened, closed = [], []
    active = [False]

    class API:
        async def snapshot(self, token):
            return {
                "asset_id": token,
                "timestamp": "1000000",
                "bids": [{"price": ".49", "size": "10"}],
                "asks": [{"price": ".51", "size": "10"}],
            }

    class WS:
        def __init__(self, number):
            self.number = number

        async def send(self, raw):
            pass

        async def recv(self):
            await asyncio.sleep(0.001)
            if self.number == 1:
                return "PONG"
            return json.dumps(
                [{"event_type": "book", **await API().snapshot(t)} for t in ["u", "d"]]
            )

    class Context:
        async def __aenter__(self):
            assert not active[0]
            active[0] = True
            opened.append(True)
            return WS(len(opened))

        async def __aexit__(self, *args):
            await asyncio.sleep(0.002)
            active[0] = False
            closed.append(True)

    def update():
        if stream.ready:
            assert len(opened) == 2 and len(closed) == 1
            stop.set()

    stream = MarketStream(
        Settings(ws_sync_seconds=0.02, cleanup_seconds=0.02, retry_initial=0.001),
        API(),
        {t: Book(t, D(".01")) for t in ["u", "d"]},
        update,
        clock=lambda: 1000.0,
        connect=lambda *a, **k: Context(),
    )
    await asyncio.wait_for(stream.run(stop), 0.2)
    assert len(opened) == len(closed) == 2
    assert stream.supervisor.operation_durations["ws.initial_sync"]["count"] == 2
    assert stream.supervisor.operation_durations["ws.initial_sync"]["last_outcome"] == "success"


@pytest.mark.asyncio
async def test_initial_sync_timeout_resistant_receive_never_reconnects():
    from decimal import Decimal as D

    from btc5m.domain import Book
    from btc5m.public import MarketStream
    from btc5m.reliability import CleanupIncomplete

    stop, release = asyncio.Event(), asyncio.Event()
    connections = []

    class API:
        async def snapshot(self, token):
            return {
                "asset_id": token,
                "timestamp": "1000000",
                "bids": [{"price": ".49", "size": "10"}],
                "asks": [{"price": ".51", "size": "10"}],
            }

    class WS:
        async def send(self, raw):
            pass

        async def recv(self):
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    pass
            return json.dumps({"event_type": "book", **await API().snapshot("u")})

    class Context:
        async def __aenter__(self):
            connections.append(True)
            return WS()

        async def __aexit__(self, *args):
            pass

    stream = MarketStream(
        Settings(ws_sync_seconds=0.01, cleanup_seconds=0.01),
        API(),
        {"u": Book("u", D(".01"))},
        lambda: None,
        clock=lambda: 1000.0,
        connect=lambda *a, **k: Context(),
    )
    try:
        with pytest.raises(CleanupIncomplete):
            await asyncio.wait_for(stream.run(stop), 0.2)
        assert len(connections) == 1
        assert stream.retired and stream.supervisor.unsafe
        assert not stream.ready and not stream.books["u"].connected
    finally:
        release.set()
        await asyncio.gather(*list(stream.supervisor.pending), return_exceptions=True)


@pytest.mark.asyncio
async def test_http_success_duration_health_stats_without_success_log_spam(tmp_path, caplog):
    caplog.set_level("INFO", logger="btc5m")
    from btc5m.public import PublicAPI
    from btc5m.service import Service

    async def handle(request):
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        api = PublicAPI(Settings(runtime_dir=tmp_path), client)
        for _ in range(3):
            assert await api.get("clob", "/time") == {"ok": True}
        service = Service(api.settings, api)
        service.health()
        reliability = json.loads((tmp_path / "health.json").read_text())["reliability"]
    for label in ["http.total", "http.complete_response"]:
        stats = reliability["operation_durations"][label]
        assert stats["count"] == 3 and stats["last_outcome"] == "success"
        assert stats["max_seconds"] >= stats["last_seconds"] > 0
    assert reliability["operation_durations"]["http.total"]["last_cleanup_seconds"] is None
    assert not caplog.records


@pytest.mark.asyncio
async def test_completed_initial_sync_does_not_expire_live_stream_or_log_per_receive(caplog):
    from decimal import Decimal as D

    from btc5m.domain import Book
    from btc5m.public import MarketStream

    caplog.set_level("INFO", logger="btc5m")
    stop = asyncio.Event()
    receives = []
    connections = []

    class API:
        async def snapshot(self, token):
            return {
                "asset_id": token,
                "timestamp": "1000000",
                "bids": [{"price": ".49", "size": "10"}],
                "asks": [{"price": ".51", "size": "10"}],
            }

    class WS:
        async def send(self, raw):
            pass

        async def recv(self):
            await asyncio.sleep(0.001)
            receives.append(True)
            if len(receives) == 1:
                return json.dumps(
                    [{"event_type": "book", **await API().snapshot(t)} for t in ["u", "d"]]
                )
            return "PONG"

    class Context:
        async def __aenter__(self):
            connections.append(True)
            return WS()

        async def __aexit__(self, *args):
            pass

    stream = MarketStream(
        Settings(ws_sync_seconds=0.02),
        API(),
        {t: Book(t, D(".01")) for t in ["u", "d"]},
        lambda: None,
        clock=lambda: 1000.0,
        connect=lambda *a, **k: Context(),
    )
    task = asyncio.create_task(stream.run(stop))
    try:
        await asyncio.sleep(0.06)
        assert stream.ready and len(connections) == 1 and len(receives) > 3
        assert stream.supervisor.operation_durations["ws.initial_sync"]["count"] == 1
        logs = [r.message for r in caplog.records if r.name.startswith("btc5m")]
        assert logs == ["ws_recovered"]
    finally:
        stop.set()
        await asyncio.wait_for(task, 0.2)
