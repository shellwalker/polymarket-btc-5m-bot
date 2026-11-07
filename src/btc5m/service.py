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
from pathlib import Path
from typing import IO, Any

from .config import Settings
from .domain import ZERO, Book, Market, MomentumStrategy, fee
from .ledger import Ledger, Rejected
from .public import MarketStream

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
    ) -> None:
        self.settings, self.api, self.clock = settings, api, clock
        self.connect = connect
        self.market: Market | None = None
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
            self.ledger.trip("abnormal_spread", now)
            return
        if (self.settings.runtime_dir / "STOP").exists():
            return
        if self.ledger.has_order(self.market.slug):
            return
        signal = self.strategy.signal(self.market, self.books, now)
        if signal is None:
            return
        decision = (self.market.slug, signal.outcome)
        if now - self.signal_last.get(decision, float("-inf")) < 30:
            return
        self.signal_last[decision] = now
        self.record(
            "signal",
            {
                "slug": self.market.slug,
                "outcome": signal.outcome,
                "quantity": str(signal.quantity),
                "limit": str(signal.limit_price),
                "reason": signal.reason,
            },
        )
        try:
            fill = self.ledger.execute(
                self.market, self.books[self.market.tokens[signal.outcome]], signal, now
            )
            if fill is not None:
                LOG.info("paper_fill", extra={"details": fill})
        except Rejected as exc:
            reason = str(exc)
            if self.last_rejection != reason:
                self.record("order_rejected", {"slug": self.market.slug, "reason": reason})
                self.last_rejection = reason

    async def switch(self, market: Market | None) -> None:
        self.switching = True
        try:
            self.stream_stop.set()
            if self.stream_task is not None:
                self.stream_task.cancel()
                await asyncio.gather(self.stream_task, return_exceptions=True)
            self.market, self.books, self.stream = market, {}, None
            self.ever_ready = False
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
        if self.ledger.has_order(self.market.slug):
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
        atomic_json(
            self.settings.runtime_dir / "health.json",
            {
                "updated": now,
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
                "reconnects": self.stream.reconnects if self.stream else 0,
                "summary": summary,
            },
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
        self.stop = stop
        settings = self.settings
        with RuntimeLock(settings.runtime_dir):
            self.ledger = Ledger(settings.runtime_dir / "ledger.sqlite3", settings)
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
