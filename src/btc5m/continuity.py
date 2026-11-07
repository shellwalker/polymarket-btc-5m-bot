"""Synchronous, process-local epoch barrier; no I/O, ledger writes or risk resets."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .config import Settings
from .domain import Book
from .reliability import changed_fields, clock_sample, sample_observed_at
from .resync_diagnostics import describe_books, failure_details


class ContinuityChanged(RuntimeError):
    """An operation belongs to an invalidated continuity epoch or subscription."""


class ContinuityBarrier:
    def __init__(
        self,
        settings: Settings,
        *,
        clocks: Callable[[], dict[str, Any]] | None = None,
        on_break: Callable[[], None] | None = None,
    ):
        self.settings, self.on_break = settings, on_break
        self.clocks = clocks or (lambda: clock_sample(settings.clock_tolerance_seconds))
        self.wall_clock: Callable[[], float] = lambda: sample_observed_at(self.clocks())
        self.last_resync_failure: dict[str, Any] | None = None
        self.epoch = 0
        self.blocked = True
        self.state = "awaiting_full_resync"
        self.previous = self.anchor = dict(self.clocks())
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
            current.get("sample_consistent") is False
            or any(
                value is not None and (value < 0 or value > max_gap) for value in deltas.values()
            )
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
                "sample_wall": current["wall"],
                "detected_at": self.wall_clock(),
                "changed_fields": changed_fields(
                    old,
                    current,
                    ("wall", "monotonic", "boottime", "process_cpu", "sample_consistent"),
                ),
                "btime_delta": None,
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
        before_epoch = self.epoch
        expected_before = set(self.required)
        before = describe_books(
            self.required, books, now, self.settings.stale_seconds, epoch, self.sync_sequence
        )
        self.poll()

        def capture_failure() -> None:
            self.last_resync_failure = failure_details(
                expected_before,
                books,
                now,
                self.settings.stale_seconds,
                epoch,
                self.epoch,
                self.sync_sequence,
                owner is self.owner,
                owner is not None,
                self.owner is not None,
            )
            self.last_resync_failure["detected_at"] = self.wall_clock()
            if before_epoch != self.epoch:
                self.last_resync_failure["pre_poll_tokens"] = before
                self.last_resync_failure["reason_codes"] = sorted(
                    set(self.last_resync_failure["reason_codes"])
                    | {reason for token in before for reason in token["reason_codes"]}
                )
                self.last_resync_failure["changed_during_poll"] = True

        if epoch != self.epoch or owner is not self.owner or set(books) != self.required:
            capture_failure()
            return False
        if not books or not all(
            book.continuity_epoch == epoch
            and book.continuity_sync_sequence == self.sync_sequence
            and book.healthy(now, self.settings.stale_seconds)
            for book in books.values()
        ):
            capture_failure()
            return False
        self.last_resync_failure = None
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
