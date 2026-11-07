"""Single-writer paper service and local process lifecycle primitives."""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import replace
from pathlib import Path
from typing import IO, Any
from uuid import uuid4

from .config import Settings
from .continuity import ContinuityBarrier
from .domain import ZERO, Book, Market, MomentumStrategy, fee, number
from .ledger import Ledger, Rejected
from .public import MarketStream
from .reliability import Diagnostics, SlowOperations, Supervisor
from .telemetry import RUN_ID, current_run_id, quote_snapshot

LOG = logging.getLogger(__name__)


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, default=str) + "\n")
    temp.replace(path)


class RuntimeLock(AbstractContextManager["RuntimeLock"]):
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.file: IO[str] | None = None

    def __enter__(self) -> RuntimeLock:
        self.directory.mkdir(parents=True, exist_ok=True)
        self.file = (self.directory / "process.lock").open("a+")
        try:
            fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.file.close()
            raise RuntimeError("paper service already running") from exc
        atomic_json(self.directory / "pid.json", {"pid": os.getpid(), "started": time.time()})
        return self

    def __exit__(self, *args: Any) -> None:
        if self.file is not None:
            (self.directory / "pid.json").unlink(missing_ok=True)
            fcntl.flock(self.file, fcntl.LOCK_UN)
            self.file.close()


def read_status(directory: Path) -> dict[str, Any]:
    running = False
    lock = directory / "process.lock"
    if lock.exists():
        with lock.open("r") as file:
            try:
                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(file, fcntl.LOCK_UN)
            except BlockingIOError:
                running = True
    health = directory / "health.json"
    try:
        state = json.loads(health.read_text()) if health.exists() else {}
    except (OSError, ValueError):
        state = {"error": "health unreadable"}
    if running:
        try:
            identity = json.loads((directory / "pid.json").read_text())
            state["pid"] = identity["pid"]
            state["started"] = identity["started"]
        except (OSError, ValueError, KeyError):
            state["pid"] = None
    state["running"] = running
    state["emergency_stop"] = (directory / "STOP").exists()
    if not running:
        state["trading_ready"] = False
    return dict(state)


class Service:
    def __init__(
        self,
        settings: Settings,
        api: Any,
        *,
        clock: Callable[[], float] = time.time,
        connect: Any = None,
        run_id: str | None = None,
        diagnostics: Diagnostics | None = None,
        continuity: ContinuityBarrier | None = None,
    ) -> None:
        self.settings, self.api, self.clock = settings, api, clock
        self.run_id = run_id or current_run_id()
        self.diagnostics = diagnostics or Diagnostics(settings)
        self.slow = SlowOperations(
            settings.slow_operation_seconds, settings.slow_diagnostic_throttle_seconds
        )
        self.supervisor = getattr(api, "supervisor", None) or Supervisor(settings.cleanup_seconds)
        self.generation = 0
        self.cleanup_failed = False
        self.closing = False
        self.switch_lock = asyncio.Lock()
        self.valuation_books: dict[str, Book] = {}
        self.next_equity_at = float("-inf")
        self.connect = connect
        self.market: Market | None = None
        self.known_market: Market | None = None
        self.prior_reconnects = 0
        self.next_market: Market | None = None
        self.books: dict[str, Book] = {}
        self.strategy = MomentumStrategy(settings)
        self.ledger: Ledger | None = None
        self.stream: MarketStream | None = None
        self.stream_task: asyncio.Task[None] | None = None
        self.stop = asyncio.Event()
        self.signal_last: dict[tuple[str, str], float] = {}
        self.stream_stop = asyncio.Event()
        self.ever_ready = False
        self.switching = False
        self.last_rejection = ""
        self.geo: dict[str, Any] = {}
        self.continuity = continuity or ContinuityBarrier(settings, clocks=self.diagnostics.clocks)
        self.continuity.on_break = self.continuity_broken
        self.recovery_needed = False

    def record(self, kind: str, details: dict[str, Any]) -> None:
        details = {**details, "run_id": self.run_id}
        LOG.info(kind, extra={"details": details})
        if self.ledger is not None:
            self.ledger.event(kind, details, self.clock())

    def retire_stream(self) -> None:
        if isinstance(self.stream, MarketStream):
            self.stream.retire()
        for book in self.books.values():
            book.disconnect()

    def fail_closed(self, reason: str) -> None:
        if self.cleanup_failed:
            return
        self.cleanup_failed = True
        self.generation += 1
        self.retire_stream()
        self.market = None
        try:
            self.record(
                "reliability_halted",
                {
                    "reason": reason,
                    "generation": self.generation,
                    "limitation": "No replacement subscriptions; pending cleanup cannot be forced",
                },
            )
        except Exception:
            LOG.exception("reliability_halt_audit_failed")

    def continuity_broken(self) -> None:
        # Called synchronously, potentially inside a ledger transaction. Never write
        # logs/DB or attempt nested async cleanup here: fence first, audit later.
        self.generation += 1
        self.recovery_needed = True
        self.retire_stream()
        self.stream_stop.set()
        self.valuation_books.clear()
        for book in self.books.values():
            book.bids.clear()
            book.asks.clear()
            book.continuity_epoch = None
            book.continuity_sync_sequence = None
        if self.ledger is not None:
            self.ledger.clear_recovery_progress()

    def entry_allowed(self, epoch: int, generation: int, owner: object | None) -> bool:
        allowed = self.continuity.allowed(epoch, owner)
        return bool(
            allowed
            and generation == self.generation
            and not self.cleanup_failed
            and not self.closing
            and not self.supervisor.unsafe
            and not self.stop.is_set()
            and not (self.settings.runtime_dir / "STOP").exists()
            and self.stream is not None
            and self.stream.ready
            and self.market is not None
            and set(self.books) == set(self.market.tokens.values())
            and all(
                b.continuity_epoch == epoch
                and b.continuity_sync_sequence == self.continuity.sync_sequence
                and b.healthy(self.clock(), self.settings.stale_seconds)
                for b in self.books.values()
            )
        )

    async def recover_continuity(self) -> None:
        if not self.recovery_needed or self.cleanup_failed or self.closing:
            return
        target = self.market
        if target is not None and not target.start <= self.clock() < target.end:
            target = None
        await self.switch(target)

    def emit_heartbeat(self) -> None:
        self.continuity.poll()
        events, self.continuity.events = self.continuity.events, []
        for kind, event_details in events:
            event_details = {
                **event_details,
                "run_id": self.run_id,
                "emitted_at": self.diagnostics.wall_clock(),
            }
            LOG.info(kind, extra={"details": event_details})
            if self.ledger is not None:
                self.ledger.event(
                    kind,
                    event_details,
                    event_details.get("detected_at", event_details["recovered_at"]),
                )
        beat, gap = self.diagnostics.heartbeat(self.run_id)
        if beat is None:
            return
        for kind, details in [("heartbeat", beat), ("continuity_gap", gap)]:
            if details is None:
                continue
            details = {**details, "emitted_at": self.diagnostics.wall_clock()}
            LOG.info(kind, extra={"details": details})
            if self.ledger is not None:
                self.ledger.event(kind, details, details.get("detected_at", beat["clocks"]["wall"]))

    async def periodic_diagnostics(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self.diagnostics.progress("diagnostics", "heartbeat")
            self.emit_heartbeat()
            await self.pause(stop, min(1.0, self.settings.diagnostic_heartbeat_seconds))

    def update(self) -> None:
        # The same event is set by SIGTERM/SIGINT; guard before any ledger mutation.
        if self.stop.is_set() or (self.settings.runtime_dir / "STOP").exists():
            return
        if self.supervisor.unsafe:
            self.fail_closed("incomplete_task_cleanup")
        if self.cleanup_failed or self.closing:
            return
        if self.ledger is None or self.market is None or self.switching:
            return
        self.continuity.poll()
        epoch, generation, owner = self.continuity.epoch, self.generation, self.continuity.owner
        if self.continuity.blocked:
            return
        sync_sequence = self.continuity.sync_sequence
        now = self.clock()
        ready = self.stream is not None and self.stream.ready
        healthy = ready and all(
            b.healthy(now, self.settings.stale_seconds) for b in self.books.values()
        )
        if not healthy:
            self.ledger.observe_market_health(False, None, now)
            if self.ever_ready and now < self.market.end:
                self.ledger.trip("stale_or_disconnected_book", now)
            return
        if not self.entry_allowed(epoch, generation, owner):
            return
        self.ever_ready = True
        if any(
            b.best_ask is not None
            and b.best_bid is not None
            and b.best_ask - b.best_bid > self.settings.max_spread
            for b in self.books.values()
        ):
            self.ledger.observe_market_health(False, None, now)
            self.ledger.trip("abnormal_spread", now)
            return
        observation = (
            self.market.slug,
            *((token, book.source_time) for token, book in sorted(self.books.items())),
        )
        self.ledger.observe_market_health(True, observation, now)
        if (self.settings.runtime_dir / "STOP").exists():
            return
        if self.ledger.window_entry_limit_reached(self.market.slug):
            return
        signal = self.strategy.signal(self.market, self.books, now)
        if signal is None:
            return
        decision = (self.market.slug, signal.outcome)
        if now - self.signal_last.get(decision, float("-inf")) < 30:
            return
        self.signal_last[decision] = now
        signal_id = str(uuid4())
        signal_quote = quote_snapshot(
            self.books[self.market.tokens[signal.outcome]], now, self.settings.stale_seconds
        )
        self.record(
            "signal",
            {
                "slug": self.market.slug,
                "outcome": signal.outcome,
                "quantity": str(signal.quantity),
                "limit": str(signal.limit_price),
                "reason": signal.reason,
                "signal_id": signal_id,
                "attempt_id": None,
                "order_id": None,
                "signal_at": now,
                "continuity_epoch": epoch,
                "continuity_sync_sequence": self.continuity.sync_sequence,
                "stream_generation": generation,
                "signal_quote": signal_quote,
                "order_at": None,
                "order_quote": None,
                "fill_ladder": None,
                "execution_slippage": None,
                "signal_to_order_move": None,
            },
        )
        try:
            fill = self.ledger.execute(
                self.market,
                self.books[self.market.tokens[signal.outcome]],
                signal,
                now,
                audit={
                    "signal_id": signal_id,
                    "signal_at": now,
                    "signal_quote": signal_quote,
                    "order_at": self.clock(),
                    "continuity_epoch": epoch,
                    "continuity_sync_sequence": sync_sequence,
                },
                clock=self.clock,
                continuity_check=lambda: self.entry_allowed(epoch, generation, owner),
            )
            if fill is not None:
                LOG.info("paper_fill", extra={"details": fill})
        except Rejected as exc:
            reason = str(exc)
            if self.last_rejection != reason:
                self.record(
                    "order_rejected",
                    {"slug": self.market.slug, "reason": reason, "signal_id": signal_id},
                )
                self.last_rejection = reason

    async def switch(self, market: Market | None) -> None:
        async with self.switch_lock:
            if self.supervisor.unsafe:
                self.fail_closed("incomplete_task_cleanup")
            if self.cleanup_failed or self.closing:
                return
            await self._switch(market)

    async def _switch(self, market: Market | None) -> None:
        if market is not None and self.known_market is not None:
            known = self.known_market
            if market.slug == known.slug:
                if (market.start, market.end, market.condition, market.tokens) != (
                    known.start,
                    known.end,
                    known.condition,
                    known.tokens,
                ):
                    if self.ledger is not None:
                        self.ledger.trip("market_identity_changed", self.clock())
                    await self._switch(None)
                    raise ValueError("same-window market identity changed")
                if self.market is not None and not self.recovery_needed:
                    # Polling metadata can lag WS dynamic ticks. Refresh constraints
                    # in place; only the ordered WS/snapshot feed changes book ticks.
                    self.market = replace(self.market, min_size=market.min_size)
                    return
        if market is not None:
            self.known_market = market
        self.switching = True
        self.generation += 1
        generation = self.generation
        self.retire_stream()
        self.continuity.block("cancelling_subscription")
        try:
            self.stream_stop.set()
            if self.stream_task is not None:
                self.diagnostics.progress("stream", "cancelling")
                # A continuity failure can already be in orderly context exit. Do
                # not interrupt that close and mistake task cancellation for closure.
                if isinstance(self.stream, MarketStream) and self.stream.session_closing:
                    _, pending = await asyncio.wait(
                        [self.stream_task], timeout=self.settings.cleanup_seconds
                    )
                    if pending:
                        self.supervisor.unsafe = True
                if not await self.supervisor.cancel([self.stream_task], "service.stream_cancel"):
                    self.fail_closed("stream_cancel_budget_exceeded")
                    return
            if self.supervisor.unsafe:
                self.fail_closed("incomplete_task_cleanup")
                return
            self.continuity.poll()
            if market is not None and not market.start <= self.clock() < market.end:
                market = None
            generation = self.generation
            self.recovery_needed = False
            if self.stream is not None:
                self.prior_reconnects += self.stream.reconnects
            self.market, self.books, self.stream = market, {}, None
            self.ever_ready = False
            if self.ledger is not None:
                self.ledger.clear_recovery_progress()
            self.last_rejection = ""
            if market is None:
                return
            self.signal_last = {
                key: stamp for key, stamp in self.signal_last.items() if key[0] == market.slug
            }
            self.books = {token: Book(token, market.tick) for token in market.tokens.values()}
            self.stream_stop = asyncio.Event()
            kwargs = {} if self.connect is None else {"connect": self.connect}

            def update_generation() -> None:
                if generation == self.generation and not self.cleanup_failed and not self.closing:
                    self.update()

            self.stream = MarketStream(
                self.settings,
                self.api,
                self.books,
                update_generation,
                clock=self.clock,
                supervisor=self.supervisor,
                progress=lambda phase: self.diagnostics.progress("stream", phase),
                continuity=self.continuity,
                market=market,
                **kwargs,
            )
            self.stream_task = asyncio.create_task(self.stream.run(self.stream_stop))
            self.record("market_switch", {"slug": market.slug, "end": market.end})
        finally:
            self.switching = False

    def risk_blocked_reasons(self, now: float) -> list[str]:
        if self.ledger is None or self.market is None:
            return ["no active market or ledger"]
        if self.ledger.window_entry_limit_reached(self.market.slug):
            return ["window already filled"]
        candidates = []
        for book in self.books.values():
            if not book.healthy(now, self.settings.stale_seconds) or book.best_ask is None:
                continue
            quantity = max(self.market.min_size, book.min_size or self.market.min_size)
            remaining, debit = quantity, ZERO
            for price, available in sorted(book.asks.items()):
                if (
                    price > self.settings.max_entry_price
                    or price - book.best_ask > self.settings.max_slippage
                ):
                    break
                take = min(remaining, available)
                debit += price * take + fee(take, price, self.settings.paper_fee_rate)
                remaining -= take
                if remaining == ZERO:
                    break
            if remaining == ZERO:
                candidates.append(self.ledger.risk_blocked_reasons(debit, now))
        if not candidates:
            return ["minimum trade unavailable within depth/price/slippage limits"]
        if any(not reasons for reasons in candidates):
            return []
        return sorted({reason for reasons in candidates for reason in reasons})

    def health(self, *, stopped: bool = False) -> None:
        self.continuity.poll()
        now = self.clock()
        healthy = (
            not stopped
            and self.market is not None
            and self.stream is not None
            and self.stream.ready
            and all(b.healthy(now, self.settings.stale_seconds) for b in self.books.values())
        )
        summary = self.ledger.summary() if self.ledger is not None else {}
        risk_blocked = self.risk_blocked_reasons(now)
        equity = self.ledger.latest_equity() if self.ledger else None
        self.continuity.poll()
        now = self.clock()
        healthy = healthy and all(
            b.healthy(now, self.settings.stale_seconds) for b in self.books.values()
        )
        with self.slow.measure("health.write"):
            atomic_json(
                self.settings.runtime_dir / "health.json",
                {
                    "updated": now,
                    "run_id": self.run_id,
                    "reliability": {
                        "heartbeat": self.diagnostics.latest,
                        "workers": self.diagnostics.workers,
                        "generation": self.generation,
                        "continuity": self.continuity.status(),
                        "continuity_recovery_needed": self.recovery_needed,
                        "cleanup_failed": self.cleanup_failed,
                        "owned_pending_tasks": len(self.supervisor.pending),
                        "operation_durations": self.supervisor.operation_durations,
                        "cleanup_durations": self.supervisor.cleanup_durations,
                        "slow_operations": self.slow.stats,
                    },
                    "equity_snapshot": equity,
                    "equity_snapshot_age_seconds": now - equity["created"] if equity else None,
                    "equity_snapshot_overdue": bool(
                        equity and now - equity["created"] > self.settings.equity_snapshot_seconds
                    ),
                    "pid": os.getpid(),
                    "mode": "paper",
                    "stopped": stopped,
                    "market": self.market.slug if self.market else None,
                    "next_market": self.next_market.slug if self.next_market else None,
                    "connected": self.stream.ready if self.stream else False,
                    "trading_ready": bool(
                        healthy
                        and not self.continuity.blocked
                        and not self.stop.is_set()
                        and not risk_blocked
                        and not summary.get("circuit")
                        and not (self.settings.runtime_dir / "STOP").exists()
                        and self.market is not None
                        and self.market.start
                        <= now
                        < self.market.end - self.settings.min_time_remaining
                    ),
                    "risk_blocked_reasons": risk_blocked,
                    **self.settings.fee_assumption(),
                    "geoblock": self.geo,
                    "reconnects": self.prior_reconnects
                    + (self.stream.reconnects if self.stream else 0),
                    "gap_started": self.stream.gap_started if self.stream else None,
                    "last_gap_seconds": self.stream.last_gap_seconds if self.stream else None,
                    "summary": summary,
                },
            )

    def snapshot_equity_if_due(self) -> None:
        if self.ledger is None:
            return
        now = self.clock()
        if now < self.next_equity_at:
            return
        try:
            snapshot = self.ledger.record_equity(self.valuation_books, now)
            LOG.info("equity_snapshot", extra={"details": snapshot})
        except Exception:
            # Observability failure must not become a trading gate or a breaker.
            LOG.exception("equity_snapshot_error", extra={"details": {"run_id": self.run_id}})
        self.next_equity_at = now + self.settings.equity_snapshot_seconds

    async def refresh_valuation_quotes(self) -> None:
        """Independent validated CLOB-only books for all pending windows; never traded."""
        if self.ledger is None:
            return
        tokens = {p["token"]: p["tick"] for p in self.ledger.pending_positions()}
        self.valuation_books = {t: b for t, b in self.valuation_books.items() if t in tokens}
        semaphore = asyncio.Semaphore(4)

        async def fetch(token: str, tick: str) -> None:
            async with semaphore:
                try:
                    request_epoch = self.continuity.poll()
                    raw = await self.api.snapshot(token)
                    if request_epoch != self.continuity.poll():
                        return
                    if self.cleanup_failed or self.closing:
                        return
                    book = Book(token, number(tick))
                    # Use the public snapshot's tick when present, otherwise the verified
                    # persisted market tick. No mutation of Service.books or stream state.
                    book.change_tick(raw.get("tick_size", tick))
                    book.snapshot(raw, self.clock())
                    self.valuation_books[token] = book
                except Exception as exc:
                    self.valuation_books.pop(token, None)
                    LOG.warning(
                        "valuation_quote_unavailable",
                        extra={
                            "details": {"run_id": self.run_id, "token": token, "error": str(exc)}
                        },
                    )

        await asyncio.gather(*(fetch(token, tick) for token, tick in tokens.items()))

    async def periodic_valuation_quotes(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            if self.cleanup_failed or self.closing:
                await self.pause(stop, 0.25)
                continue
            self.diagnostics.progress("valuation", "refresh")
            try:
                await self.refresh_valuation_quotes()
                self.diagnostics.progress("valuation", "waiting")
            except Exception:
                LOG.exception("valuation_refresh_error", extra={"details": {"run_id": self.run_id}})
            await self.pause(
                stop,
                min(
                    self.settings.equity_snapshot_seconds / 2,
                    self.settings.valuation_stale_seconds / 2,
                ),
            )

    async def resolve_pending(self) -> None:
        assert self.ledger is not None
        for market in self.ledger.pending():
            if self.clock() < market.end:
                continue
            try:
                official = await self.api.resolution(market)
                if self.cleanup_failed or self.closing:
                    return
                result = self.ledger.settle(market, official, self.clock())
                if result:
                    LOG.info("settlement", extra={"details": result})
                else:
                    self.record("resolution_pending", {"slug": market.slug})
            except Exception as exc:
                self.record("resolution_error", {"slug": market.slug, "error": str(exc)})

    async def periodic_discovery(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            if self.cleanup_failed or self.closing:
                await self.pause(stop, 0.25)
                continue
            try:
                self.diagnostics.progress("discovery", "request")
                request_epoch = self.continuity.poll()
                current, following = await self.api.discover(self.clock())
                if request_epoch != self.continuity.poll():
                    await self.pause(stop, self.settings.discovery_seconds)
                    continue
                if self.cleanup_failed or self.closing:
                    return
                self.diagnostics.progress("discovery", "received")
                self.next_market = following
                # A failed refresh invalidates a previously tradable market too.
                if current is None:
                    if self.market is not None:
                        await self.switch(None)
                elif self.market != current:
                    await self.switch(current)
            except Exception as exc:
                self.record("discovery_error", {"error": str(exc)})
                await self.switch(None)
            await self.pause(stop, self.settings.discovery_seconds)

    async def periodic_resolution(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            if self.cleanup_failed or self.closing:
                await self.pause(stop, 0.25)
                continue
            self.diagnostics.progress("resolution", "request")
            await self.resolve_pending()
            self.diagnostics.progress("resolution", "waiting")
            await self.pause(stop, self.settings.resolution_seconds)

    @staticmethod
    async def pause(stop: asyncio.Event, duration: float) -> None:
        try:
            await asyncio.wait_for(stop.wait(), timeout=duration)
        except TimeoutError:
            pass

    async def wait_stop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            if (self.settings.runtime_dir / "STOP").exists():
                self.record("emergency_stop", {"source": "file_or_command"})
                stop.set()
                return
            await self.pause(stop, 0.1)

    async def run(self, stop: asyncio.Event) -> None:
        token = RUN_ID.set(self.run_id)
        try:
            await self._run(stop)
        finally:
            RUN_ID.reset(token)

    async def _run(self, stop: asyncio.Event) -> None:
        self.stop = stop
        settings = self.settings
        with RuntimeLock(settings.runtime_dir):
            self.ledger = Ledger(
                settings.runtime_dir / "ledger.sqlite3",
                settings,
                run_id=self.run_id,
                slow=self.slow,
            )
            tasks: list[asyncio.Task[None]] = []
            try:
                if (settings.runtime_dir / "STOP").exists() or stop.is_set():
                    self.record("startup_stopped", {})
                    return
                self.diagnostics.progress("main", "startup")
                self.emit_heartbeat()
                tasks.append(asyncio.create_task(self.periodic_diagnostics(stop)))
                geo_task = asyncio.create_task(self.api.get("geo", "/api/geoblock"))
                stop_task = asyncio.create_task(self.wait_stop(stop))
                try:
                    done, _ = await asyncio.wait(
                        {geo_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
                    )
                    if stop_task in done:
                        return
                    try:
                        geo = geo_task.result()
                        self.geo = {key: geo.get(key) for key in ("blocked", "country", "region")}
                        self.record("geoblock", self.geo)
                    except Exception as exc:
                        self.geo = {"blocked": "unknown", "error": str(exc)}
                        self.record("geoblock_unavailable", self.geo)
                finally:
                    if not await self.supervisor.cancel([geo_task, stop_task], "startup.cleanup"):
                        self.fail_closed("startup_cleanup_incomplete")
                tasks.extend(
                    [
                        asyncio.create_task(self.periodic_discovery(stop)),
                        asyncio.create_task(self.periodic_resolution(stop)),
                        asyncio.create_task(self.periodic_valuation_quotes(stop)),
                    ]
                )
                self.record("service_started", {"mode": "paper", **settings.fee_assumption()})
                while not stop.is_set():
                    self.diagnostics.progress("main", "loop")
                    self.emit_heartbeat()
                    if self.supervisor.unsafe:
                        self.fail_closed("incomplete_task_cleanup")
                    await self.recover_continuity()
                    if (settings.runtime_dir / "STOP").exists():
                        self.record("emergency_stop", {"source": "file_or_command"})
                        stop.set()
                        break
                    # Stop at the exact boundary, independently of discovery retries.
                    if self.market is not None and self.clock() >= self.market.end:
                        await self.switch(None)
                    self.update()
                    self.snapshot_equity_if_due()
                    self.health()
                    for task in tasks:
                        if task.done() and not self.cleanup_failed and not stop.is_set():
                            task.result()
                            raise RuntimeError("background worker exited unexpectedly")
                    await self.pause(stop, 0.25)
            finally:
                self.closing = self.switching = True
                self.generation += 1
                self.retire_stream()
                self.stream_stop.set()
                shutdown = asyncio.create_task(self.finish_shutdown(tasks))
                # A second cancellation must not release the runtime lock while old
                # subscriptions can still exist. Cleanup itself owns bounded waits.
                while not shutdown.done():
                    try:
                        await asyncio.shield(shutdown)
                    except asyncio.CancelledError:
                        self.fail_closed("shutdown_cancelled_while_cleanup_pending")
                shutdown.result()

    async def finish_shutdown(self, tasks: list[asyncio.Task[None]]) -> None:
        assert self.ledger is not None
        cleanup = list(tasks)
        if self.stream_task is not None:
            cleanup.append(self.stream_task)
        try:
            await self.supervisor.cancel(cleanup, "service.shutdown")
        except asyncio.CancelledError:
            self.fail_closed("shutdown_cleanup_cancelled")
        # Keep the runtime lock and DB while uncooperative work remains. A
        # timeout is not proof that its subscription/socket/worker is gone.
        if self.supervisor.pending:
            self.fail_closed("shutdown_cleanup_incomplete")
        reported = False
        while self.supervisor.pending:
            try:
                self.emit_heartbeat()
                self.health(stopped=True)
            except Exception:
                if not reported:
                    LOG.exception("quarantine_diagnostics_failed")
                    reported = True
            try:
                await asyncio.wait(list(self.supervisor.pending), timeout=1.0)
            except asyncio.CancelledError:
                self.fail_closed("shutdown_cleanup_cancelled")
        self.market, self.books, self.stream = None, {}, None
        try:
            self.record("service_stopped", {})
            self.health(stopped=True)
        finally:
            self.ledger.close()
            self.ledger = None
