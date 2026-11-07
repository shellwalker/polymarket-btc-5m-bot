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
from .domain import ZERO, Book, Market, MomentumStrategy, fee, number
from .ledger import Ledger, Rejected
from .public import MarketStream
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
    ) -> None:
        self.settings, self.api, self.clock = settings, api, clock
        self.run_id = run_id or current_run_id()
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

    def record(self, kind: str, details: dict[str, Any]) -> None:
        details = {**details, "run_id": self.run_id}
        LOG.info(kind, extra={"details": details})
        if self.ledger is not None:
            self.ledger.event(kind, details, self.clock())

    def update(self) -> None:
        # The same event is set by SIGTERM/SIGINT; guard before any ledger mutation.
        if self.stop.is_set() or (self.settings.runtime_dir / "STOP").exists():
            return
        if self.ledger is None or self.market is None or self.switching:
            return
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
                },
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
                    await self.switch(None)
                    raise ValueError("same-window market identity changed")
                if self.market is not None:
                    # Polling metadata can lag WS dynamic ticks. Refresh constraints
                    # in place; only the ordered WS/snapshot feed changes book ticks.
                    self.market = replace(self.market, min_size=market.min_size)
                    return
        if market is not None:
            self.known_market = market
        self.switching = True
        try:
            self.stream_stop.set()
            if self.stream_task is not None:
                self.stream_task.cancel()
                await asyncio.gather(self.stream_task, return_exceptions=True)
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
            self.stream = MarketStream(
                self.settings, self.api, self.books, self.update, clock=self.clock, **kwargs
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
        atomic_json(
            self.settings.runtime_dir / "health.json",
            {
                "updated": now,
                "run_id": self.run_id,
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
                    raw = await self.api.snapshot(token)
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
            try:
                await self.refresh_valuation_quotes()
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
                result = self.ledger.settle(market, official, self.clock())
                if result:
                    LOG.info("settlement", extra={"details": result})
                else:
                    self.record("resolution_pending", {"slug": market.slug})
            except Exception as exc:
                self.record("resolution_error", {"slug": market.slug, "error": str(exc)})

    async def periodic_discovery(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                current, following = await self.api.discover(self.clock())
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
            await self.resolve_pending()
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
                settings.runtime_dir / "ledger.sqlite3", settings, run_id=self.run_id
            )
            tasks: list[asyncio.Task[None]] = []
            try:
                if (settings.runtime_dir / "STOP").exists() or stop.is_set():
                    self.record("startup_stopped", {})
                    return
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
                    geo_task.cancel()
                    stop_task.cancel()
                    await asyncio.gather(geo_task, stop_task, return_exceptions=True)
                tasks = [
                    asyncio.create_task(self.periodic_discovery(stop)),
                    asyncio.create_task(self.periodic_resolution(stop)),
                    asyncio.create_task(self.periodic_valuation_quotes(stop)),
                ]
                self.record("service_started", {"mode": "paper", **settings.fee_assumption()})
                while not stop.is_set():
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
                        if task.done():
                            task.result()
                            raise RuntimeError("background worker exited unexpectedly")
                    await self.pause(stop, 0.25)
            finally:
                self.switching = True
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await self.switch(None)
                self.record("service_stopped", {})
                self.health(stopped=True)
                self.ledger.close()
                self.ledger = None
