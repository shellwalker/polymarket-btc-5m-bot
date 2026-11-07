"""Transactional paper ledger. Decimal values are persisted as exact text."""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from btc5m.config import Settings
from btc5m.domain import ONE, ZERO, Book, Market, Signal, array, fee, number


class Rejected(ValueError):
    """A paper order was rejected without consuming cash or depth."""


TRANSIENT_BREAKERS = frozenset({"abnormal_spread", "stale_or_disconnected_book"})


class Ledger:
    def __init__(self, path: Path, settings: Settings):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.settings = settings
        self.clear_recovery_progress()
        self.db = sqlite3.connect(path, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS orders(
                slug TEXT PRIMARY KEY, market TEXT NOT NULL, outcome TEXT NOT NULL,
                quantity TEXT NOT NULL, cost TEXT NOT NULL, fee TEXT NOT NULL,
                created REAL NOT NULL, settled INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS order_windows(
                order_id TEXT PRIMARY KEY REFERENCES orders(slug), window_slug TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS order_windows_slug ON order_windows(window_slug);
            CREATE TABLE IF NOT EXISTS fills(
                slug TEXT NOT NULL REFERENCES orders(slug), level INTEGER NOT NULL,
                price TEXT NOT NULL, quantity TEXT NOT NULL, fee TEXT NOT NULL,
                PRIMARY KEY(slug,level));
            CREATE TABLE IF NOT EXISTS settlements(
                slug TEXT PRIMARY KEY REFERENCES orders(slug), winner TEXT NOT NULL,
                payout TEXT NOT NULL,pnl TEXT NOT NULL,created REAL NOT NULL,
                evidence TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS daily(day TEXT PRIMARY KEY,loss TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events(
                id INTEGER PRIMARY KEY,kind TEXT NOT NULL,
                payload TEXT NOT NULL,created REAL NOT NULL);
        """)
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO order_windows SELECT slug,slug FROM orders")
            self.db.execute(
                "INSERT OR IGNORE INTO meta VALUES ('cash',?)", (str(settings.initial_cash),)
            )
            self.db.execute(
                "INSERT OR IGNORE INTO meta VALUES ('initial_cash',?)",
                (str(settings.initial_cash),),
            )
            self.db.execute("INSERT OR IGNORE INTO meta VALUES ('consecutive_losses','0')")
            self.db.execute("INSERT OR IGNORE INTO meta VALUES ('circuit','')")
            self.db.execute("INSERT OR IGNORE INTO meta VALUES ('schema_version','1')")
        if self._meta("schema_version") != "1":
            self.close()
            raise ValueError("unsupported ledger schema version")

    def _meta(self, key: str) -> str:
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row is None:
            raise ValueError(f"missing ledger state: {key}")
        return str(row[0])

    def _put(self, key: str, value: Any) -> None:
        self.db.execute("UPDATE meta SET value=? WHERE key=?", (str(value), key))

    def _event(self, kind: str, payload: dict[str, Any], now: float) -> None:
        self.db.execute(
            "INSERT INTO events(kind,payload,created) VALUES (?,?,?)",
            (kind, json.dumps(payload, sort_keys=True, default=str), now),
        )

    def event(self, kind: str, payload: dict[str, Any], now: float) -> None:
        with self.db:
            self._event(kind, payload, now)

    def trip(self, reason: str, now: float) -> None:
        with self.db:
            previous = self._meta("circuit")
            if not previous or (
                previous in TRANSIENT_BREAKERS and reason not in TRANSIENT_BREAKERS
            ):
                self._put("circuit", reason)
                self.clear_recovery_progress()
                self._event("circuit_breaker", {"reason": reason}, now)

    def clear_recovery_progress(self) -> None:
        self._healthy_since: float | None = None
        self._healthy_count = 0
        self._last_observation: tuple[Any, ...] | None = None

    def observe_market_health(
        self, healthy: bool, observation: tuple[Any, ...] | None, now: float
    ) -> None:
        """Require continuous health and distinct complete book generations after restart.

        Only allowlisted data breakers can clear; loss counters are never changed here.
        Timers and repeated observations cannot advance or complete recovery.
        """
        reason = self._meta("circuit")
        if (
            not healthy
            or not self.settings.market_auto_recovery
            or reason not in TRANSIENT_BREAKERS
        ):
            self.clear_recovery_progress()
            return
        if observation is None or observation == self._last_observation:
            return
        if self._healthy_since is None or now < self._healthy_since:
            self.clear_recovery_progress()
            self._healthy_since = now
        self._last_observation = observation
        self._healthy_count += 1
        if (
            self._healthy_count < self.settings.market_recovery_snapshots
            or now - self._healthy_since < self.settings.market_recovery_cooldown
        ):
            return
        with self.db:
            if self._meta("circuit") != reason:
                return
            self._put("circuit", "")
            self._event(
                "circuit_reset",
                {
                    "previous_reason": reason,
                    "automatic": True,
                    "healthy_snapshots": self._healthy_count,
                    "healthy_seconds": now - self._healthy_since,
                },
                now,
            )
        self.clear_recovery_progress()

    def window_entry_limit_reached(self, slug: str) -> bool:
        count = self.db.execute(
            "SELECT COUNT(*) FROM order_windows WHERE window_slug=?", (slug,)
        ).fetchone()[0]
        return bool(count >= self.settings.max_entries_per_window)

    def reset_circuit(self, now: float | None = None) -> None:
        """Explicit operator action only; never called as a consequence of restart."""
        with self.db:
            self.clear_recovery_progress()
            previous = self._meta("circuit")
            previous_streak = self._meta("consecutive_losses")
            self._put("circuit", "")
            self._put("consecutive_losses", "0")
            self._event(
                "circuit_reset",
                {"previous_reason": previous, "previous_consecutive_losses": int(previous_streak)},
                time.time() if now is None else now,
            )

    def has_order(self, slug: str) -> bool:
        return (
            self.db.execute("SELECT 1 FROM order_windows WHERE window_slug=?", (slug,)).fetchone()
            is not None
        )

    def pending(self) -> list[Market]:
        return [
            Market.from_dict(json.loads(row[0]))
            for row in self.db.execute(
                "SELECT DISTINCT market FROM orders WHERE settled=0 ORDER BY created"
            )
        ]

    def _exposure(self) -> Decimal:
        return sum(
            (
                number(row[0]) + number(row[1])
                for row in self.db.execute("SELECT cost,fee FROM orders WHERE settled=0")
            ),
            ZERO,
        )

    @staticmethod
    def _day(now: float) -> str:
        return datetime.fromtimestamp(now, UTC).date().isoformat()

    def risk_blocked_reasons(self, debit: Decimal, now: float) -> list[str]:
        """Read-only risk preview; execute repeats these checks inside its transaction."""
        settings = self.settings
        reasons = []
        if self._meta("circuit"):
            reasons.append("circuit breaker active: " + self._meta("circuit"))
        if debit > settings.max_trade:
            reasons.append("per-trade cap exceeded including fees")
        exposure = self._exposure()
        if exposure + debit > settings.max_exposure:
            reasons.append("total exposure cap exceeded")
        if debit > number(self._meta("cash")):
            reasons.append("insufficient paper cash")
        daily = self.db.execute("SELECT loss FROM daily WHERE day=?", (self._day(now),)).fetchone()
        if (number(daily[0]) if daily else ZERO) + exposure + debit > settings.daily_loss_limit:
            reasons.append("daily loss limit including worst-case pending exposure")
        if int(self._meta("consecutive_losses")) >= settings.max_consecutive_losses:
            reasons.append("consecutive loss limit reached")
        return reasons

    def execute(
        self, market: Market, book: Book, signal: Signal, now: float
    ) -> dict[str, Any] | None:
        """All-or-none taker fill with a durable per-window successful entry allowance.

        Displayed asks supply each share once per local book. Window mappings count
        committed orders across outcomes, settlement, reconnect, and restart.
        The atomic cash/risk check is performed under BEGIN IMMEDIATE before any mutation.
        """
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if self.window_entry_limit_reached(market.slug):
                self.db.rollback()
                return None
            circuit = self._meta("circuit")
            if circuit:
                raise Rejected(f"circuit breaker active: {circuit}")
            settings = self.settings
            if (
                signal.outcome not in market.tokens
                or market.tokens[signal.outcome] != book.asset_id
            ):
                raise Rejected("signal asset mismatch")
            if not market.start <= now < market.end - settings.min_time_remaining:
                raise Rejected("outside entry window")
            if not book.valid_depth():
                raise Rejected("depth price or size violates tick")
            if not book.healthy(now, settings.stale_seconds):
                raise Rejected("stale or disconnected book")
            quantity, limit = signal.quantity, signal.limit_price
            if (
                not quantity.is_finite()
                or quantity < max(market.min_size, book.min_size or market.min_size)
                or quantity <= ZERO
            ):
                raise Rejected("quantity below minimum")
            if not limit.is_finite() or not ZERO < limit < ONE or limit % book.tick:
                raise Rejected("limit price violates tick")
            ask, bid = book.best_ask, book.best_bid
            if ask is None or bid is None or ask - bid > settings.max_spread:
                raise Rejected("abnormal spread")
            if limit > settings.max_entry_price:
                raise Rejected("entry price limit")
            remaining = quantity
            fills: list[tuple[Decimal, Decimal, Decimal]] = []
            for price, available in sorted(book.asks.items()):
                if price > limit or price - ask > settings.max_slippage:
                    break
                take = min(remaining, available)
                if take > ZERO:
                    fills.append((price, take, fee(take, price, settings.paper_fee_rate)))
                    remaining -= take
                if remaining == ZERO:
                    break
            if remaining:
                raise Rejected("insufficient depth within slippage/limit")
            cost = sum((price * qty for price, qty, _ in fills), ZERO)
            fees = sum((amount for _, _, amount in fills), ZERO)
            debit = cost + fees
            blocked = self.risk_blocked_reasons(debit, now)
            if blocked:
                raise Rejected("; ".join(blocked))
            cash = number(self._meta("cash"))
            entry = (
                self.db.execute(
                    "SELECT COUNT(*) FROM order_windows WHERE window_slug=?", (market.slug,)
                ).fetchone()[0]
                + 1
            )
            order_id = market.slug if entry == 1 else f"{market.slug}#{entry}"
            self.db.execute(
                "INSERT INTO orders(slug,market,outcome,quantity,cost,fee,created) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    order_id,
                    json.dumps(market.to_dict()),
                    signal.outcome,
                    str(quantity),
                    str(cost),
                    str(fees),
                    now,
                ),
            )
            self.db.execute("INSERT INTO order_windows VALUES (?,?)", (order_id, market.slug))
            for index, (price, qty, amount) in enumerate(fills):
                self.db.execute(
                    "INSERT INTO fills VALUES (?,?,?,?,?)",
                    (order_id, index, str(price), str(qty), str(amount)),
                )
            self._put("cash", cash - debit)
            result = {
                "slug": market.slug,
                "order_id": order_id,
                "entry": entry,
                "outcome": signal.outcome,
                "quantity": str(quantity),
                "cost": str(cost),
                "fee": str(fees),
                "average_price": str(cost / quantity),
                "reason": signal.reason,
                "levels": len(fills),
                **settings.fee_assumption(),
            }
            self._event("paper_fill", result, now)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        # No awaits between commit and this local depletion. Crash here is safe because
        # the persisted slug uniqueness check wins on recovery.
        for price, qty, _ in fills:
            book.asks[price] -= qty
            if book.asks[price] == ZERO:
                del book.asks[price]
        return result

    def settle(self, market: Market, official: dict[str, Any], now: float) -> dict[str, Any] | None:
        """Accept a terminal official Gamma market only; never infer from spot or clock."""
        if now < market.end or official.get("conditionId") != market.condition:
            return None
        if official.get("closed") is not True or official.get("umaResolutionStatus") != "resolved":
            return None
        try:
            outcomes = array(official["outcomes"])
            tokens = array(official["clobTokenIds"])
            prices = [number(item) for item in array(official["outcomePrices"])]
        except (ValueError, KeyError, TypeError):
            return None
        if (
            len(outcomes) != 2
            or len(tokens) != 2
            or len(prices) != 2
            or not all(isinstance(item, str) for item in outcomes + tokens)
            or set(outcomes) != {"Up", "Down"}
            or sorted(prices) != [ZERO, ONE]
            or dict(zip(outcomes, tokens, strict=True)) != market.tokens
        ):
            return None
        winner = str(outcomes[prices.index(ONE)])
        self.db.execute("BEGIN IMMEDIATE")
        try:
            rows = self.db.execute(
                "SELECT orders.* FROM orders JOIN order_windows ON orders.slug=order_id "
                "WHERE window_slug=? AND settled=0 ORDER BY created,orders.slug",
                (market.slug,),
            ).fetchall()
            if not rows:
                self.db.rollback()
                return None
            total_payout, total_pnl = ZERO, ZERO
            for row in rows:
                order_id = row["slug"]
                saved = Market.from_dict(json.loads(row["market"]))
                # Tick/minimum can change within a window; settlement depends only
                # on the immutable schedule and outcome/token identity.
                if (saved.slug, saved.start, saved.end, saved.condition, saved.tokens) != (
                    market.slug,
                    market.start,
                    market.end,
                    market.condition,
                    market.tokens,
                ):
                    raise ValueError("settlement market does not match persisted identity")
                payout = number(row["quantity"]) if row["outcome"] == winner else ZERO
                pnl = payout - number(row["cost"]) - number(row["fee"])
                self.db.execute(
                    "INSERT INTO settlements VALUES (?,?,?,?,?,?)",
                    (
                        row["slug"],
                        winner,
                        str(payout),
                        str(pnl),
                        now,
                        json.dumps(official, sort_keys=True),
                    ),
                )
                self.db.execute("UPDATE orders SET settled=1 WHERE slug=?", (row["slug"],))
                self._put("cash", number(self._meta("cash")) + payout)
                streak = int(self._meta("consecutive_losses")) + 1 if pnl < ZERO else 0
                self._put("consecutive_losses", streak)
                day = self._day(now)
                daily_row = self.db.execute("SELECT loss FROM daily WHERE day=?", (day,)).fetchone()
                losses = (number(daily_row[0]) if daily_row else ZERO) + max(-pnl, ZERO)
                self.db.execute(
                    "INSERT INTO daily VALUES (?,?) "
                    "ON CONFLICT(day) DO UPDATE SET loss=excluded.loss",
                    (day, str(losses)),
                )
                reason = ""
                if losses >= self.settings.daily_loss_limit:
                    reason = "daily_loss"
                elif streak >= self.settings.max_consecutive_losses:
                    reason = "consecutive_losses"
                if reason and (
                    not self._meta("circuit") or self._meta("circuit") in TRANSIENT_BREAKERS
                ):
                    self._put("circuit", reason)
                    self._event("circuit_breaker", {"reason": reason}, now)
                result = {
                    "slug": market.slug,
                    "order_id": order_id,
                    "winner": winner,
                    "payout": str(payout),
                    "pnl": str(pnl),
                }
                self._event("settlement", result, now)
                total_payout += payout
                total_pnl += pnl
            self.db.commit()
            return {
                "slug": market.slug,
                "winner": winner,
                "payout": str(total_payout),
                "pnl": str(total_pnl),
            }
        except BaseException:
            self.db.rollback()
            raise

    def summary(self) -> dict[str, Any]:
        return self._summary(self.db)

    @classmethod
    def read_summary(cls, path: Path) -> dict[str, Any]:
        """Read one consistent committed snapshot without creating or initializing a ledger."""
        try:
            with closing(
                sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
            ) as db:
                db.execute("PRAGMA query_only=ON")
                db.execute("BEGIN")
                try:
                    return cls._summary(db)
                finally:
                    db.rollback()
        except sqlite3.Error as exc:
            raise ValueError(f"cannot read ledger: {exc}") from exc

    @staticmethod
    def _summary(db: sqlite3.Connection) -> dict[str, Any]:
        def meta(key: str) -> str:
            row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            if row is None:
                raise ValueError(f"missing ledger state: {key}")
            return str(row[0])

        if meta("schema_version") != "1":
            raise ValueError("unsupported ledger schema version")
        exposure = sum(
            (
                number(row[0]) + number(row[1])
                for row in db.execute("SELECT cost,fee FROM orders WHERE settled=0")
            ),
            ZERO,
        )
        pnl = sum((number(row[0]) for row in db.execute("SELECT pnl FROM settlements")), ZERO)
        pending_cost = sum(
            (number(row[0]) for row in db.execute("SELECT cost FROM orders WHERE settled=0")),
            ZERO,
        )
        total_fees = sum((number(row[0]) for row in db.execute("SELECT fee FROM orders")), ZERO)
        cash = number(meta("cash"))
        return {
            "total_fees": str(total_fees),
            "pending_cost": str(pending_cost),
            "equity_at_cost": str(cash + pending_cost),
            "equity_worst_case": str(cash),
            "event_counts": {
                row[0]: row[1]
                for row in db.execute(
                    "SELECT kind,COUNT(*) FROM events GROUP BY kind ORDER BY kind"
                )
            },
            "cash": meta("cash"),
            "initial_cash": meta("initial_cash"),
            "realized_pnl": str(pnl),
            "exposure": str(exposure),
            "fills": db.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
            "fill_levels": db.execute("SELECT COUNT(*) FROM fills").fetchone()[0],
            "settlements": db.execute("SELECT COUNT(*) FROM settlements").fetchone()[0],
            "pending": db.execute("SELECT COUNT(*) FROM orders WHERE settled=0").fetchone()[0],
            "consecutive_losses": int(meta("consecutive_losses")),
            "circuit": meta("circuit"),
            "daily_losses": {
                row[0]: row[1] for row in db.execute("SELECT day,loss FROM daily ORDER BY day")
            },
        }

    def close(self) -> None:
        self.db.close()
