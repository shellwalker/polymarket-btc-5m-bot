"""Public, unauthenticated GET and market websocket only."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from dataclasses import replace
from decimal import Decimal
from typing import Any

import httpx
from websockets.asyncio.client import connect as ws_connect

from .config import Settings
from .domain import Book, Market, number, window_slugs

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

    async def get(self, service: str, path: str, **params: str) -> Any:
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
        delay = self.settings.retry_initial
        for attempt in range(4):
            try:
                response = await self.client.get(
                    BASES[service] + path,
                    params=params,
                    timeout=self.settings.http_timeout,
                    follow_redirects=False,
                )
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 429 and exc.response.status_code < 500:
                    raise
                if attempt == 3:
                    raise
            except httpx.TransportError:
                if attempt == 3:
                    raise
            LOG.warning(
                "http_retry",
                extra={"details": {"service": service, "path": path, "attempt": attempt + 1}},
            )
            await asyncio.sleep(delay)
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
    """Resubscribe, REST resync, then consume initial WS dump before trading.

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
    ) -> None:
        self.settings, self.api, self.books = settings, api, books
        self.on_update, self.connect, self.clock = on_update, connect, clock
        self.reconnects = 0
        self.ready = False

    def invalidate(self) -> None:
        self.ready = False
        for book in self.books.values():
            book.disconnect()
        self.on_update()

    def apply_snapshot(self, token: str, snapshot: dict[str, Any]) -> None:
        book = self.books[token]
        if number(snapshot["timestamp"]) / 1000 >= book.source_time:
            if "tick_size" in snapshot:
                book.change_tick(snapshot["tick_size"])
        book.snapshot(snapshot, self.clock())

    async def run(self, stop: asyncio.Event) -> None:
        delay = self.settings.retry_initial
        try:
            while not stop.is_set():
                self.invalidate()
                try:
                    async with self.connect(
                        WS_URL,
                        ping_interval=None,
                        proxy=None,
                        open_timeout=self.settings.http_timeout,
                        close_timeout=2,
                        max_size=4 * 1024 * 1024,
                    ) as ws:
                        await ws.send(
                            json.dumps(
                                {
                                    "assets_ids": list(self.books),
                                    "type": "market",
                                    "custom_feature_enabled": True,
                                    "initial_dump": True,
                                }
                            )
                        )
                        snapshots = await asyncio.gather(
                            *(self.api.snapshot(t) for t in self.books)
                        )
                        for initial_token, snapshot in zip(self.books, snapshots, strict=True):
                            self.apply_snapshot(initial_token, snapshot)
                            self.books[initial_token].connected = False
                        pending = set(self.books)
                        loop = asyncio.get_running_loop()
                        connected_since = loop.time()
                        last_pong = loop.time()
                        next_ping = loop.time() + self.settings.heartbeat_seconds
                        receive = asyncio.create_task(ws.recv())
                        try:
                            while not stop.is_set():
                                now = loop.time()
                                if now - last_pong > self.settings.pong_timeout:
                                    raise TimeoutError("application PONG timeout")
                                if now >= next_ping:
                                    await ws.send("PING")
                                    next_ping = now + self.settings.heartbeat_seconds
                                done, _ = await asyncio.wait(
                                    {receive}, timeout=min(0.25, self.settings.heartbeat_seconds)
                                )
                                if not done:
                                    continue
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
                                        self.apply_snapshot(token, message)
                                        pending.discard(token)
                                    elif kind == "price_change":
                                        for change in message["price_changes"]:
                                            asset = change["asset_id"]
                                            if asset in self.books:
                                                # Initial dump must precede deltas.
                                                if asset in pending:
                                                    raise ValueError(
                                                        "delta before initial WS snapshot"
                                                    )
                                                self.books[asset].delta(
                                                    change, message["timestamp"], self.clock()
                                                )
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
                                self.ready = not pending
                                for asset, book in self.books.items():
                                    book.connected = self.ready and asset not in pending
                                if self.ready and loop.time() - connected_since >= 30:
                                    delay = self.settings.retry_initial
                                self.on_update()
                        finally:
                            receive.cancel()
                            await asyncio.gather(receive, return_exceptions=True)
                except asyncio.CancelledError:
                    raise
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
