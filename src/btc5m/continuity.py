"""Synchronous, process-local epoch barrier; no I/O, ledger writes or risk resets."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .config import Settings
from .domain import Book
from .reliability import clock_sample


class ContinuityChanged(RuntimeError):
    """An operation belongs to an invalidated continuity epoch or subscription."""


class ContinuityBarrier:
    def __init__(
        self,
        settings: Settings,
        *,
        clocks: Callable[[], dict[str, Any]] = clock_sample,
        on_break: Callable[[], None] | None = None,
    ):
        self.settings, self.clocks, self.on_break = settings, clocks, on_break
        self.epoch = 0
        self.blocked = True
        self.state = "awaiting_full_resync"
        self.previous = self.anchor = dict(clocks())
        self.owner: object | None = None
        self.required: set[str] = set()
        self.sync_sequence = 0
        self.last_gap: dict[str, Any] | None = None
        self.events: list[tuple[str, dict[str, Any]]] = []

    def poll(self) -> int:
        current = dict(self.clocks())
        deltas = {
            key: current[key] - self.previous[key]
            if current.get(key) is not None and self.previous.get(key) is not None
            else None
            for key in ("wall", "monotonic", "boottime")
        }
        # A no-check interval beyond existing depth freshness cannot certify continuity.
        max_gap = min(self.settings.stale_seconds, self.settings.continuity_gap_seconds)
        offset = (current["wall"] - self.anchor["wall"]) - (
            current["monotonic"] - self.anchor["monotonic"]
        )
        boot_offset = (
            (current["boottime"] - self.anchor["boottime"])
            - (current["monotonic"] - self.anchor["monotonic"])
            if current.get("boottime") is not None and self.anchor.get("boottime") is not None
            else 0.0
        )
        abnormal = (
            any(value is not None and (value < 0 or value > max_gap) for value in deltas.values())
            or abs(offset) > self.settings.clock_tolerance_seconds
            or abs(boot_offset) > self.settings.clock_tolerance_seconds
        )
        old = self.previous
        self.previous = current
        if abnormal:
            self.epoch += 1
            self.block("gap_detected")
            self.anchor = current
            self.last_gap = {
                "epoch": self.epoch,
                "cause": "unknown",
                "previous": old,
                "current": current,
                "deltas": deltas,
                "wall_minus_monotonic_offset": offset,
                "boottime_minus_monotonic_offset": boot_offset,
                "recovered_at": current["wall"],
            }
            self.events.append(("continuity_barrier", dict(self.last_gap)))
            if self.on_break is not None:
                self.on_break()
        return self.epoch

    def block(self, state: str) -> None:
        self.blocked, self.state, self.owner = True, state, None
        self.required = set()

    def require(self, epoch: int) -> None:
        self.poll()
        if epoch != self.epoch:
            raise ContinuityChanged("continuity epoch changed; full resync required")

    def begin(self, owner: object, tokens: set[str]) -> int:
        self.poll()
        self.blocked, self.state = True, "resynchronizing"
        self.owner, self.required = owner, set(tokens)
        self.sync_sequence += 1
        return self.epoch

    def complete(self, epoch: int, owner: object, books: dict[str, Book], now: float) -> bool:
        self.poll()
        if epoch != self.epoch or owner is not self.owner or set(books) != self.required:
            return False
        if not books or not all(
            book.continuity_epoch == epoch
            and book.continuity_sync_sequence == self.sync_sequence
            and book.healthy(now, self.settings.stale_seconds)
            for book in books.values()
        ):
            return False
        self.blocked, self.state = False, "open"
        self.events.append(
            (
                "continuity_resynced",
                {
                    "epoch": epoch,
                    "sync_sequence": self.sync_sequence,
                    "tokens": sorted(books),
                    "recovered_at": self.previous["wall"],
                },
            )
        )
        return True

    def allowed(self, epoch: int, owner: object | None) -> bool:
        self.poll()
        return not self.blocked and epoch == self.epoch and owner is self.owner

    def status(self) -> dict[str, Any]:
        return {
            "epoch": self.epoch,
            "blocked": self.blocked,
            "state": self.state,
            "sync_sequence": self.sync_sequence,
            "required_tokens": sorted(self.required),
            "last_gap": self.last_gap,
        }
