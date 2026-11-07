"""Public, unauthenticated GET and market websocket only."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from copy import copy
from dataclasses import replace
from decimal import Decimal
from typing import Any

import httpx
from websockets.asyncio.client import connect as ws_connect

from .config import Settings
from .continuity import ContinuityBarrier, ContinuityChanged
from .domain import Book, Market, number, window_slugs
from .reliability import CleanupIncomplete, OperationDeadline, Supervisor

LOG = logging.getLogger(__name__)
BASES = {
    "gamma": "https://gamma-api.polymarket.com",
    "clob": "https://clob.polymarket.com",
    "geo": "https://polymarket.com",
}
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class PublicAPI:
    def __init__(self, settings: Settings, client: httpx.AsyncClient) -> None:
        self.settings, self.client = settings, client
        self.supervisor = Supervisor(settings.cleanup_seconds)

    def total_expired(self, service: str, path: str, started: float) -> OperationDeadline:
        LOG.warning(
            "operation_deadline",
            extra={
                "details": {
                    "operation": "http.total",
                    "service": service,
                    "path": path,
                    "budget_seconds": self.settings.http_total_seconds,
                    "elapsed_seconds": asyncio.get_running_loop().time() - started,
                    "operation_elapsed_seconds": None,
                    "cleanup_elapsed_seconds": None,
                    "timing_scope": "overall including retries/backoff; cleanup per attempt",
                }
            },
        )
        return OperationDeadline("HTTP overall retry budget exceeded")

    async def get(self, service: str, path: str, **params: str) -> Any:
        started = asyncio.get_running_loop().time()
        outcome = "error"
        try:
            result = await self._get(service, path, **params)
            outcome = "success"
            return result
        except OperationDeadline:
            outcome = "timeout"
            raise
        except CleanupIncomplete:
            outcome = "cleanup_incomplete"
            raise
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        finally:
            self.supervisor.record_operation(
                "http.total",
                asyncio.get_running_loop().time() - started,
                outcome,
                budget_seconds=self.settings.http_total_seconds,
                cleanup_elapsed_seconds=None,
            )

    async def _get(self, service: str, path: str, **params: str) -> Any:
        allowed = (
            (service == "gamma" and path == "/events")
            or (service == "geo" and path == "/api/geoblock")
            or (service == "clob" and path in {"/time", "/book"})
            or (
                service == "clob"
                and path.startswith("/markets/")
                and len(path.split("/")) == 3
                and path.split("/")[-1].isalnum()
            )
        )
        if not allowed:
            raise ValueError("public endpoint not allowlisted")
        started = asyncio.get_running_loop().time()
        deadline = started + self.settings.http_total_seconds
        delay = self.settings.retry_initial
        for attempt in range(4):
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise self.total_expired(service, path, started)
            try:
                response = await self.supervisor.run(
                    self.client.get(
                        BASES[service] + path,
                        params=params,
                        timeout=self.settings.http_timeout,
                        follow_redirects=False,
                    ),
                    min(remaining, self.settings.http_response_seconds),
                    "http.complete_response",
                )
                response.raise_for_status()
                result = response.json()
                if asyncio.get_running_loop().time() >= deadline:
                    raise OperationDeadline("HTTP overall budget exceeded during decode")
                return result
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 429 and exc.response.status_code < 500:
                    raise
                if attempt == 3:
                    raise
            except (httpx.TransportError, OperationDeadline):
                if attempt == 3:
                    if asyncio.get_running_loop().time() >= deadline:
                        raise self.total_expired(service, path, started) from None
                    raise
            LOG.warning(
                "http_retry",
                extra={"details": {"service": service, "path": path, "attempt": attempt + 1}},
            )
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise self.total_expired(service, path, started)
            await asyncio.sleep(min(delay, remaining))
            delay = min(delay * 2, self.settings.retry_max)
        raise AssertionError("unreachable")

    async def event(self, slug: str) -> dict[str, Any]:
        data = await self.get("gamma", "/events", slug=slug)
        if not isinstance(data, list) or len(data) != 1 or data[0].get("slug") != slug:
            raise ValueError(f"window not published or ambiguous: {slug}")
        return dict(data[0])

    async def discover(self, now: float) -> tuple[Market | None, Market | None]:
        async def one(slug: str) -> Market | None:
            try:
                market = Market.from_event(await self.event(slug))
                detail = await self.get("clob", "/markets/" + market.condition)
                if detail.get("condition_id") != market.condition:
                    raise ValueError("CLOB condition mismatch")
                if (
                    detail.get("accepting_orders") is not True
                    or detail.get("closed") is not False
                    or detail.get("active") is not True
                    or detail.get("enable_order_book") is not True
                ):
                    raise ValueError("market not accepting orders")
                # CLOB constraints must pass the same fail-closed validation.
                minimum = number(detail["minimum_order_size"])
                tick = number(detail["minimum_tick_size"])
                if minimum <= 0 or tick not in {
                    Decimal(".1"),
                    Decimal(".01"),
                    Decimal(".001"),
                    Decimal(".0001"),
                }:
                    raise ValueError("unsupported CLOB minimum size or tick")
                return replace(market, min_size=minimum, tick=tick)
            except (ValueError, KeyError, TypeError, httpx.HTTPError) as exc:
                LOG.warning(
                    "discovery_failed", extra={"details": {"slug": slug, "error": str(exc)}}
                )
                return None

        current, following = await asyncio.gather(*(one(s) for s in window_slugs(now)))
        return current, following

    async def snapshot(self, token: str) -> dict[str, Any]:
        data = await self.get("clob", "/book", token_id=token)
        if not isinstance(data, dict) or data.get("asset_id") != token:
            raise ValueError("snapshot identity mismatch")
        return data

    async def resolution(self, market: Market) -> dict[str, Any]:
        event = await self.event(market.slug)
        markets = event.get("markets", [])
        matches = [m for m in markets if m.get("conditionId") == market.condition]
        if len(matches) != 1:
            raise ValueError("resolution identity mismatch")
        return dict(matches[0])


class MarketStream:
    """REST resync before subscription; WS initial dump owns the ordered baseline.

    There is no WS sequence number. Older timestamps never overwrite newer
    snapshots. A gap/disconnect invalidates every book immediately.
    """

    def __init__(
        self,
        settings: Settings,
        api: Any,
        books: dict[str, Book],
        on_update: Callable[[], None],
        *,
        connect: Any = ws_connect,
        clock: Callable[[], float] = time.time,
        supervisor: Supervisor | None = None,
        progress: Callable[[str], None] | None = None,
        continuity: ContinuityBarrier | None = None,
    ) -> None:
        self.settings, self.api, self.books = settings, api, books
        self.on_update, self.connect, self.clock = on_update, connect, clock
        self.supervisor = supervisor or Supervisor(settings.cleanup_seconds)
        self.progress = progress or (lambda phase: None)
        self.continuity = continuity
        self.epoch = continuity.epoch if continuity else 0
        self.lease = object()
        self.sync_started = False
        self.sync_sequence = 0
        self.session_closing = False
        self.retired = False
        self.reconnects = 0
        self.ready = False
        self.gap_started: float | None = None
        self.last_gap_seconds: float | None = None

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        manager = self.connect(
            WS_URL,
            ping_interval=None,
            proxy=None,
            open_timeout=self.settings.http_timeout,
            close_timeout=2,
            max_size=4 * 1024 * 1024,
        )
        ws = await manager.__aenter__()
        self.session_closing = False

        async def close(exc_type: Any, exc: Any, traceback: Any) -> Any:
            self.session_closing = True
            try:
                result = await self.supervisor.run(
                    manager.__aexit__(exc_type, exc, traceback),
                    self.settings.cleanup_seconds,
                    "ws.close",
                )
            except BaseException:
                # Cancellation of close is not evidence the old subscription is gone.
                self.supervisor.unsafe = True
                self.retire()
                raise
            self.session_closing = False
            return result

        try:
            yield ws
        except BaseException as exc:
            if not await close(type(exc), exc, exc.__traceback__):
                raise
        else:
            await close(None, None, None)

    def check_continuity(self) -> None:
        if self.continuity is not None:
            try:
                self.continuity.require(self.epoch)
                if self.sync_started and (
                    self.continuity.owner is not self.lease
                    or self.sync_sequence != self.continuity.sync_sequence
                ):
                    raise ContinuityChanged("subscription lease replaced")
            except ContinuityChanged:
                self.retire()
                for book in self.books.values():
                    book.bids.clear()
                    book.asks.clear()
                    book.continuity_epoch = None
                    book.continuity_sync_sequence = None
                raise
        if self.retired:
            raise ContinuityChanged("retired subscription generation")

    def retire(self) -> None:
        self.retired = True
        self.ready = False
        for book in self.books.values():
            book.disconnect()

    async def bounded(self, operation: Any, seconds: float, label: str) -> Any:
        self.progress(label)
        try:
            return await self.supervisor.run(operation, seconds, label)
        except CleanupIncomplete:
            self.retire()
            raise
        except OperationDeadline:
            self.invalidate()
            raise

    def invalidate(self) -> None:
        self.ready = False
        if self.gap_started is None:
            self.gap_started = self.clock()
        for book in self.books.values():
            book.disconnect()
        if not self.retired:
            self.on_update()

    def apply_snapshot(
        self, token: str, snapshot: dict[str, Any], *, initial: bool = False
    ) -> None:
        if self.retired:
            return
        self.check_continuity()
        book = self.books[token]
        if initial:
            # WS initial dump is its own ordered baseline, not a merge into a
            # potentially newer REST state with already queued old deltas.
            if (
                self.clock() - float(number(snapshot["timestamp"]) / 1000)
                > self.settings.stale_seconds
            ):
                raise ValueError("stale initial websocket snapshot")
            book.source_time = 0
        if number(snapshot["timestamp"]) / 1000 >= book.source_time:
            if "tick_size" in snapshot:
                book.change_tick(snapshot["tick_size"])
        book.snapshot(snapshot, self.clock())
        self.check_continuity()
        if initial:
            book.continuity_epoch = self.epoch
            book.continuity_sync_sequence = self.sync_sequence

    def apply_price_change(self, message: dict[str, Any], pending: set[str]) -> None:
        if self.retired:
            return
        self.check_continuity()
        grouped: dict[str, list[dict[str, Any]]] = {}
        staged: dict[str, Book] = {}
        try:
            for change in message["price_changes"]:
                asset = change["asset_id"]
                if asset in self.books:
                    if asset in pending:
                        raise ValueError("delta before initial WS snapshot")
                    grouped.setdefault(asset, []).append(change)
            for asset, changes in grouped.items():
                candidate = copy(self.books[asset])
                candidate.connected = True  # This token's initial dump is complete.
                candidate.deltas(changes, message["timestamp"], self.clock())
                staged[asset] = candidate
            self.check_continuity()
            for asset, candidate in staged.items():
                book = self.books[asset]
                book.bids, book.asks = candidate.bids, candidate.asks
                book.source_time, book.updated_at = candidate.source_time, candidate.updated_at
        except (ValueError, KeyError, TypeError):
            self.invalidate()
            raise

    async def run(self, stop: asyncio.Event) -> None:
        delay = self.settings.retry_initial
        try:
            while not stop.is_set() and not self.retired and not self.supervisor.unsafe:
                self.invalidate()
                try:
                    self.check_continuity()
                    self.lease = object()
                    if self.continuity is not None:
                        self.continuity.begin(self.lease, set(self.books))
                        self.sync_sequence = self.continuity.sync_sequence
                        self.sync_started = True
                        self.check_continuity()
                        for book in self.books.values():
                            book.continuity_epoch = None
                            book.continuity_sync_sequence = None
                    snapshots = await self.bounded(
                        asyncio.gather(*(self.api.snapshot(t) for t in self.books)),
                        self.settings.ws_sync_seconds,
                        "ws.rest_sync",
                    )
                    self.check_continuity()
                    if stop.is_set():
                        return
                    for initial_token, snapshot in zip(self.books, snapshots, strict=True):
                        self.books[initial_token].source_time = 0
                        self.apply_snapshot(initial_token, snapshot)
                        self.books[initial_token].connected = False
                    async with self.connection() as ws:
                        self.check_continuity()
                        if stop.is_set():
                            return
                        await self.bounded(
                            ws.send(
                                json.dumps(
                                    {
                                        "assets_ids": list(self.books),
                                        "type": "market",
                                        "custom_feature_enabled": True,
                                        "initial_dump": True,
                                    }
                                )
                            ),
                            self.settings.ws_send_seconds,
                            "ws.subscribe",
                        )
                        self.check_continuity()
                        if stop.is_set():
                            return
                        pending = set(self.books)
                        loop = asyncio.get_running_loop()
                        connected_since = loop.time()
                        sync_deadline = connected_since + self.settings.ws_sync_seconds
                        sync_recorded = False
                        sync_outcome = "cancelled"
                        last_pong = loop.time()
                        next_ping = loop.time() + self.settings.heartbeat_seconds
                        receive = asyncio.create_task(ws.recv())
                        try:
                            while not stop.is_set() and not self.retired:
                                self.check_continuity()
                                now = loop.time()
                                if pending and now >= sync_deadline:
                                    raise OperationDeadline(
                                        "initial WS snapshot synchronization deadline"
                                    )
                                if now - last_pong > self.settings.pong_timeout:
                                    raise TimeoutError("application PONG timeout")
                                if now >= next_ping:
                                    await self.bounded(
                                        ws.send("PING"),
                                        min(
                                            self.settings.ws_send_seconds,
                                            max(0.0, sync_deadline - loop.time()),
                                        )
                                        if pending
                                        else self.settings.ws_send_seconds,
                                        "ws.ping",
                                    )
                                    next_ping = now + self.settings.heartbeat_seconds
                                wait_seconds = min(0.25, self.settings.heartbeat_seconds)
                                if pending:
                                    remaining = sync_deadline - loop.time()
                                    if remaining <= 0:
                                        raise OperationDeadline(
                                            "initial WS snapshot synchronization deadline"
                                        )
                                    wait_seconds = min(wait_seconds, remaining)
                                done, _ = await asyncio.wait({receive}, timeout=wait_seconds)
                                self.check_continuity()
                                if not done:
                                    continue
                                if self.retired or stop.is_set():
                                    return
                                self.progress("ws.received")
                                raw = receive.result()
                                receive = asyncio.create_task(ws.recv())
                                if raw == "PONG":
                                    last_pong = loop.time()
                                    continue
                                data = json.loads(raw)
                                messages = data if isinstance(data, list) else [data]
                                for message in messages:
                                    if not isinstance(message, dict):
                                        raise ValueError("unknown websocket envelope")
                                    kind = message.get("event_type")
                                    token = message.get("asset_id")
                                    if kind == "book" and token in self.books:
                                        self.apply_snapshot(
                                            token, message, initial=token in pending
                                        )
                                        pending.discard(token)
                                    elif kind == "price_change":
                                        self.apply_price_change(message, pending)
                                    elif kind == "last_trade_price" and token in self.books:
                                        self.books[token].trade(message, self.clock())
                                    elif kind == "tick_size_change" and token in self.books:
                                        self.books[token].change_tick(message["new_tick_size"])
                                        if not self.books[token].connected:
                                            raise ValueError("tick change requires snapshot resync")
                                    elif kind not in {
                                        "last_trade_price",
                                        "best_bid_ask",
                                        "new_market",
                                        "market_resolved",
                                        "book",
                                        "tick_size_change",
                                    }:
                                        raise ValueError(f"unknown websocket event: {kind}")
                                if not sync_recorded and loop.time() >= sync_deadline:
                                    raise OperationDeadline(
                                        "initial WS snapshot synchronization deadline"
                                    )
                                self.check_continuity()
                                was_ready = self.ready
                                if not pending and not was_ready and self.continuity is not None:
                                    # Do not publish readiness until the complete current-epoch
                                    # WS dump is fresh on both tokens (REST alone never opens).
                                    for book in self.books.values():
                                        book.connected = True
                                    if not self.continuity.complete(
                                        self.epoch, self.lease, self.books, self.clock()
                                    ):
                                        self.invalidate()
                                        raise ValueError(
                                            "current-epoch dual-token resync incomplete"
                                        )
                                self.ready = not pending
                                if self.ready and not was_ready:
                                    self.supervisor.record_operation(
                                        "ws.initial_sync",
                                        loop.time() - connected_since,
                                        "success",
                                        budget_seconds=self.settings.ws_sync_seconds,
                                    )
                                    sync_recorded = True
                                    self.last_gap_seconds = (
                                        self.clock() - self.gap_started
                                        if self.gap_started is not None
                                        else 0
                                    )
                                    LOG.info(
                                        "ws_recovered",
                                        extra={
                                            "details": {
                                                "assets": list(self.books),
                                                "reconnects": self.reconnects,
                                                "gap_seconds": self.last_gap_seconds,
                                            }
                                        },
                                    )
                                    self.gap_started = None
                                for asset, book in self.books.items():
                                    book.connected = self.ready and asset not in pending
                                if self.ready and loop.time() - connected_since >= 30:
                                    delay = self.settings.retry_initial
                                if not self.retired:
                                    self.on_update()
                        except OperationDeadline:
                            sync_outcome = "timeout"
                            raise
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            sync_outcome = "error"
                            raise
                        finally:
                            phase_elapsed = loop.time() - connected_since
                            sync_timed_out = not sync_recorded and loop.time() >= sync_deadline
                            if not sync_recorded:
                                self.invalidate()
                            cleanup_started = loop.time()
                            clean = False
                            try:
                                clean = await self.supervisor.cancel(
                                    [receive], "ws.receive_cleanup"
                                )
                            finally:
                                if not sync_recorded:
                                    self.supervisor.record_operation(
                                        "ws.initial_sync",
                                        loop.time() - connected_since,
                                        (
                                            "cleanup_incomplete"
                                            if not clean
                                            else "timeout"
                                            if sync_timed_out
                                            else sync_outcome
                                        ),
                                        operation_elapsed_seconds=phase_elapsed,
                                        cleanup_elapsed_seconds=loop.time() - cleanup_started,
                                        budget_seconds=self.settings.ws_sync_seconds,
                                        log_deadline=sync_timed_out,
                                    )
                            if not clean:
                                self.retire()
                                raise CleanupIncomplete("WS receive did not cancel")
                except asyncio.CancelledError:
                    raise
                except CleanupIncomplete:
                    self.retire()
                    raise
                except ContinuityChanged:
                    self.retire()
                    return
                except Exception as exc:
                    self.reconnects += 1
                    self.invalidate()
                    LOG.warning(
                        "ws_reconnect",
                        extra={
                            "details": {
                                "error": str(exc),
                                "delay": delay,
                                "reconnects": self.reconnects,
                                "gap_started": self.gap_started,
                                "assets": list(self.books),
                            }
                        },
                    )
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=delay)
                    except TimeoutError:
                        pass
                    delay = min(delay * 2, self.settings.retry_max)
        finally:
            self.invalidate()
