"""Decimal-only market interpretation, order books, fees, and deterministic signals."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Protocol

from btc5m.config import Settings

ZERO = Decimal("0")
ONE = Decimal("1")
FEE_RATE = Decimal("0.07")


def number(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
    except (ValueError, InvalidOperation) as exc:
        raise ValueError("invalid decimal") from exc
    if not result.is_finite():
        raise ValueError("non-finite decimal")
    return result


def array(value: Any) -> list[Any]:
    result = json.loads(value) if isinstance(value, str) else value
    if not isinstance(result, list):
        raise ValueError("expected array")
    return result


def window_slugs(now: float) -> tuple[str, str]:
    start = int(now // 300) * 300
    return f"btc-updown-5m-{start}", f"btc-updown-5m-{start + 300}"


def fee(quantity: Decimal, price: Decimal, rate: Decimal = FEE_RATE) -> Decimal:
    if not rate.is_finite() or rate < ZERO or quantity <= ZERO or not ZERO < price < ONE:
        raise ValueError("invalid fee inputs")
    return (quantity * rate * price * (ONE - price)).quantize(
        Decimal("0.00001"), rounding=ROUND_HALF_UP
    )


@dataclass(frozen=True)
class Market:
    slug: str
    start: int
    end: int
    condition: str
    tokens: dict[str, str]
    min_size: Decimal
    tick: Decimal

    @classmethod
    def from_event(cls, event: dict[str, Any]) -> Market:
        try:
            slug = str(event["slug"])
            match = re.fullmatch(r"btc-updown-5m-(\d+)", slug)
            if match is None or int(match[1]) % 300:
                raise ValueError("unexpected BTC 5m slug")
            start = int(match[1])
            markets = event["markets"]
            if not isinstance(markets, list) or len(markets) != 1:
                raise ValueError("expected one binary market")
            raw = markets[0]
            if raw.get("slug") != slug or raw.get("version") != "v1":
                raise ValueError("unsupported market version or slug")
            if (
                raw.get("active") is not True
                or raw.get("closed") is not False
                or raw.get("acceptingOrders") is not True
            ):
                raise ValueError("market not verified tradeable")
            rules = raw.get("cryptoMarketConfig", {})
            expected_rules = {
                "id": "btc-5m-twap-60",
                "asset": "btc",
                "duration": "5m",
                "twapEnabled": True,
                "twapLookbackSeconds": 60,
            }
            if not isinstance(rules, dict) or any(
                type(rules.get(key)) is not type(value) or rules.get(key) != value
                for key, value in expected_rules.items()
            ):
                raise ValueError("unsupported BTC Chainlink 60s TWAP rules")
            outcomes = array(raw["outcomes"])
            tokens = array(raw["clobTokenIds"])
            if set(outcomes) != {"Up", "Down"} or len(outcomes) != 2:
                raise ValueError("expected Up/Down outcomes")
            if (
                len(tokens) != 2
                or len(set(tokens)) != 2
                or not all(isinstance(token, str) and token for token in tokens)
            ):
                raise ValueError("invalid token identifiers")
            schedule = raw.get("feeSchedule", {})
            if (
                raw.get("feesEnabled") is not True
                or raw.get("feeType") != "crypto_fees_v2"
                or not isinstance(schedule, dict)
                or number(schedule.get("rate")) != FEE_RATE
                or number(schedule.get("exponent")) != ONE
                or schedule.get("takerOnly") is not True
            ):
                raise ValueError("unverified fee schema")
            end_date = datetime.fromisoformat(str(raw["endDate"]).replace("Z", "+00:00"))
            if end_date.tzinfo is None or int(end_date.timestamp()) != start + 300:
                raise ValueError("market endDate does not match window")
            condition = raw["conditionId"]
            if not isinstance(condition, str) or not condition:
                raise ValueError("missing condition")
            min_size, tick = number(raw["orderMinSize"]), number(raw["orderPriceMinTickSize"])
            if min_size <= ZERO or tick not in {
                Decimal("0.1"),
                Decimal("0.01"),
                Decimal("0.001"),
                Decimal("0.0001"),
            }:
                raise ValueError("invalid minimum size or tick")
            return cls(
                slug,
                start,
                start + 300,
                condition,
                dict(zip(outcomes, tokens, strict=True)),
                min_size,
                tick,
            )
        except (KeyError, TypeError, IndexError, InvalidOperation) as exc:
            raise ValueError(f"unsupported market schema: {exc}") from exc

    def to_dict(self) -> dict[str, Any]:
        return {
            "slug": self.slug,
            "start": self.start,
            "end": self.end,
            "condition": self.condition,
            "tokens": self.tokens,
            "min_size": str(self.min_size),
            "tick": str(self.tick),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Market:
        return cls(
            str(raw["slug"]),
            int(raw["start"]),
            int(raw["end"]),
            str(raw["condition"]),
            dict(raw["tokens"]),
            number(raw["min_size"]),
            number(raw["tick"]),
        )


class Book:
    def __init__(self, asset_id: str, tick: Decimal):
        self.asset_id = asset_id
        self.tick = tick
        self.min_size: Decimal | None = None
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.connected = False
        self.updated_at = 0.0
        self.source_time = 0.0
        self.latest_trade_price: Decimal | None = None
        self.latest_trade_time = 0.0

    @property
    def best_bid(self) -> Decimal | None:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return min(self.asks) if self.asks else None

    @property
    def midpoint(self) -> Decimal | None:
        bid, ask = self.best_bid, self.best_ask
        return (bid + ask) / 2 if bid is not None and ask is not None else None

    def disconnect(self) -> None:
        self.connected = False

    def _time(self, timestamp: Any, now: float) -> float:
        source_time = float(number(timestamp) / 1000)
        if source_time > now + 2:
            raise ValueError("out-of-order or future book timestamp")
        return source_time

    def _levels(self, levels: Any) -> dict[Decimal, Decimal]:
        result: dict[Decimal, Decimal] = {}
        for level in array(levels):
            price, size = number(level["price"]), number(level["size"])
            if not ZERO < price < ONE or size < ZERO or price % self.tick:
                raise ValueError("invalid price, size or tick")
            if size:
                if price in result:
                    raise ValueError("duplicate depth level")
                result[price] = size
        return result

    def snapshot(self, data: dict[str, Any], now: float) -> None:
        try:
            if data.get("asset_id") != self.asset_id:
                raise ValueError("book asset mismatch")
            source_time = self._time(data["timestamp"], now)
            if source_time < self.source_time:
                return
            bids, asks = self._levels(data["bids"]), self._levels(data["asks"])
            if bids and asks and max(bids) >= min(asks):
                raise ValueError("crossed order book")
            minimum = self.min_size
            if "min_order_size" in data:
                minimum = number(data["min_order_size"])
                if minimum <= ZERO:
                    raise ValueError("invalid minimum size")
            self.min_size = minimum
            self.bids, self.asks = bids, asks
            self.source_time, self.updated_at, self.connected = source_time, now, True
        except (ValueError, KeyError, TypeError) as exc:
            self.disconnect()
            raise ValueError(f"invalid book snapshot: {exc}") from exc

    def delta(self, change: dict[str, Any], timestamp: Any, now: float) -> None:
        self.deltas([change], timestamp, now)

    def deltas(self, changes: list[dict[str, Any]], timestamp: Any, now: float) -> None:
        """Commit one complete message, including authoritative top-of-book bounds."""
        try:
            if not self.connected:
                raise ValueError("delta before snapshot")
            source_time = self._time(timestamp, now)
            if source_time < self.source_time:
                return
            bids, asks = dict(self.bids), dict(self.asks)
            bounds = None
            for change in changes:
                if change.get("asset_id") != self.asset_id:
                    raise ValueError("asset mismatch")
                if change.get("side") not in {"BUY", "SELL"}:
                    raise ValueError("unknown book side")
                price, size = number(change["price"]), number(change["size"])
                if not ZERO < price < ONE or size < ZERO or price % self.tick:
                    raise ValueError("invalid delta price or size")
                side = bids if change["side"] == "BUY" else asks
                if size:
                    side[price] = size
                else:
                    side.pop(price, None)
                if "best_bid" in change or "best_ask" in change:
                    bid, ask = number(change["best_bid"]), number(change["best_ask"])
                    if not ZERO <= bid < ask <= ONE:
                        raise ValueError("crossed advertised book top")
                    bounds = bid, ask
            if bounds is not None:
                bid, ask = bounds
                # Public deltas can omit zero-size removals after a match. Only remove
                # levels contradicted by official bounds; never synthesize new depth.
                bids = {p: q for p, q in bids.items() if p <= bid}
                asks = {p: q for p, q in asks.items() if p >= ask}
                if (max(bids) if bids else ZERO) != bid or (min(asks) if asks else ONE) != ask:
                    raise ValueError("book top mismatch requires snapshot resync")
            if bids and asks and max(bids) >= min(asks):
                raise ValueError("crossed order book")
            self.bids, self.asks = bids, asks
            self.source_time, self.updated_at = source_time, now
        except (ValueError, KeyError, TypeError) as exc:
            self.disconnect()
            raise ValueError(f"invalid book delta: {exc}") from exc

    def change_tick(self, value: Any) -> None:
        tick = number(value)
        if tick not in {Decimal("0.1"), Decimal("0.01"), Decimal("0.001"), Decimal("0.0001")}:
            self.disconnect()
            raise ValueError("unknown tick size")
        self.tick = tick
        if any(price % tick for price in self.bids.keys() | self.asks.keys()):
            self.disconnect()

    def trade(self, message: dict[str, Any], now: float) -> None:
        """Record an observed trade without making displayed order-book depth fresher."""
        try:
            if message.get("asset_id") != self.asset_id:
                raise ValueError("trade asset mismatch")
            price, size = number(message["price"]), number(message["size"])
            source_time = float(number(message["timestamp"]) / 1000)
            if not ZERO < price < ONE or size <= ZERO or not 0 < source_time <= now + 2:
                raise ValueError("invalid trade price, size or timestamp")
            if source_time < self.latest_trade_time:
                return
            self.latest_trade_price, self.latest_trade_time = price, source_time
        except (ValueError, KeyError, TypeError) as exc:
            raise ValueError(f"invalid trade: {exc}") from exc

    def valid_depth(self) -> bool:
        """Defense in depth against incompatible tick transitions or corrupt local objects."""
        if self.tick not in {Decimal("0.1"), Decimal("0.01"), Decimal("0.001"), Decimal("0.0001")}:
            return False
        return all(
            price.is_finite()
            and size.is_finite()
            and ZERO < price < ONE
            and size > ZERO
            and price % self.tick == ZERO
            for side in (self.bids, self.asks)
            for price, size in side.items()
        )

    def healthy(self, now: float, stale_seconds: float) -> bool:
        return bool(
            self.connected
            and self.valid_depth()
            and self.bids
            and self.asks
            and self.best_bid is not None
            and self.best_ask is not None
            and self.best_bid < self.best_ask
            and 0 <= now - self.updated_at <= stale_seconds
            and -2 <= now - self.source_time <= stale_seconds
        )


@dataclass(frozen=True)
class Signal:
    outcome: str
    quantity: Decimal
    limit_price: Decimal
    reason: str


class Strategy(Protocol):
    def signal(self, market: Market, books: Mapping[str, Book], now: float) -> Signal | None: ...


class MomentumStrategy:
    """Follow a measured change in Up midpoint; no inferred true probability or profit claim."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.anchors: dict[str, Decimal] = {}

    def signal(self, market: Market, books: Mapping[str, Book], now: float) -> Signal | None:
        settings = self.settings
        if not market.start <= now < market.end - settings.min_time_remaining:
            return None
        if any(
            token not in books or not books[token].healthy(now, settings.stale_seconds)
            for token in market.tokens.values()
        ):
            return None
        up = books[market.tokens["Up"]].midpoint
        if up is None:
            return None
        anchor = self.anchors.setdefault(market.slug, up)
        change = up - anchor
        if abs(change) < max(settings.signal_threshold, settings.min_momentum):
            return None
        outcome = "Up" if change > ZERO else "Down"
        book = books[market.tokens[outcome]]
        ask, bid = book.best_ask, book.best_bid
        if ask is None or bid is None or ask - bid > settings.max_spread:
            return None
        # Fee is a real cost filter, not a claimed estimate of positive expected value.
        if abs(change) <= fee(ONE, ask, settings.paper_fee_rate) + settings.max_slippage:
            return None
        limit = min(ask + settings.max_slippage, settings.max_entry_price)
        limit = (limit / book.tick).to_integral_value(rounding=ROUND_DOWN) * book.tick
        if ask > limit:
            return None
        return Signal(outcome, settings.shares, limit, f"midpoint momentum {change}")
