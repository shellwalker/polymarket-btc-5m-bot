"""Neutral continuity measurements and explicitly cooperative task ownership."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any, TypeVar

from .config import Settings
from .telemetry import current_run_id

LOG = logging.getLogger(__name__)
T = TypeVar("T")
_EMITTING: ContextVar[bool] = ContextVar("reliability_emitting", default=False)


class OperationDeadline(TimeoutError):
    """The operation exceeded its complete-response or overall retry budget."""


class CleanupIncomplete(RuntimeError):
    """Cancellation was requested, but completion could not be established in budget."""


class Supervisor:
    def __init__(self, cleanup_seconds: float):
        self.cleanup_seconds = cleanup_seconds
        self.pending: set[asyncio.Future[Any]] = set()
        self.unsafe = False
        self.operation_durations: dict[str, dict[str, Any]] = {}
        self.cleanup_durations: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _observe(
        table: dict[str, dict[str, Any]], label: str, elapsed: float, outcome: str, **fields: Any
    ) -> None:
        previous = table.get(label, {})
        table[label] = {
            "count": previous.get("count", 0) + 1,
            "last_seconds": elapsed,
            "max_seconds": max(previous.get("max_seconds", 0.0), elapsed),
            "last_outcome": outcome,
            **fields,
        }

    def record_operation(
        self,
        label: str,
        elapsed_seconds: float,
        outcome: str,
        *,
        operation_elapsed_seconds: float | None = None,
        cleanup_elapsed_seconds: float | None = 0.0,
        budget_seconds: float | None = None,
        log_deadline: bool = False,
    ) -> dict[str, Any]:
        operation_elapsed = (
            (elapsed_seconds if operation_elapsed_seconds is None else operation_elapsed_seconds)
            if cleanup_elapsed_seconds is not None
            else None
        )
        self._observe(
            self.operation_durations,
            label,
            elapsed_seconds,
            outcome,
            last_operation_seconds=operation_elapsed,
            last_cleanup_seconds=cleanup_elapsed_seconds,
            timing_scope="operation_and_own_cleanup"
            if cleanup_elapsed_seconds is not None
            else "overall_including_attempt_cleanup",
        )
        details = {
            "run_id": current_run_id(),
            "operation": label,
            "outcome": outcome,
            "elapsed_seconds": elapsed_seconds,
            "operation_elapsed_seconds": operation_elapsed,
            "cleanup_elapsed_seconds": cleanup_elapsed_seconds,
            "budget_seconds": budget_seconds,
            "timing_scope": "operation_and_own_cleanup"
            if cleanup_elapsed_seconds is not None
            else "overall_including_attempt_cleanup",
        }
        if log_deadline:
            LOG.warning("operation_deadline", extra={"details": details})
        return details

    def _finished(self, task: asyncio.Future[Any]) -> None:
        self.pending.discard(task)
        if not task.cancelled():
            task.exception()

    async def cancel(self, tasks: Sequence[asyncio.Future[Any]], label: str) -> bool:
        started = asyncio.get_running_loop().time()
        for task in tasks:
            if task.done() and not task.cancelled():
                task.exception()
        active = [task for task in tasks if not task.done()]
        outcome = "completed"
        pending: set[asyncio.Future[Any]] = set()
        try:
            if not active:
                return True
            for task in active:
                if task not in self.pending:
                    self.pending.add(task)
                    task.add_done_callback(self._finished)
                task.cancel()
            _, pending = await asyncio.wait(active, timeout=self.cleanup_seconds)
            if pending:
                self.unsafe = True
                outcome = "incomplete"
            return not pending
        except asyncio.CancelledError:
            self.unsafe = any(not task.done() for task in active) or self.unsafe
            outcome = "interrupted"
            raise
        finally:
            elapsed = asyncio.get_running_loop().time() - started
            self._observe(self.cleanup_durations, label, elapsed, outcome)
            if outcome == "incomplete":
                LOG.error(
                    "cleanup_incomplete",
                    extra={
                        "details": {
                            "run_id": current_run_id(),
                            "operation": label,
                            "pending_tasks": len(pending),
                            "cleanup_seconds": self.cleanup_seconds,
                            "elapsed_seconds": elapsed,
                            "cleanup_elapsed_seconds": elapsed,
                            "limitation": "Cannot force cancellation; replacement unsafe",
                        }
                    },
                )

    async def run(self, operation: Awaitable[T], seconds: float, label: str) -> T:
        loop = asyncio.get_running_loop()
        started = loop.time()
        task = asyncio.ensure_future(operation)
        self.pending.add(task)
        task.add_done_callback(self._finished)
        outcome = "error"
        before_cleanup: float | None = None
        cleanup_elapsed = 0.0
        timed_out = False
        try:
            if self.unsafe:
                outcome = "quarantined"
                before_cleanup = loop.time() - started
                cleanup_started = loop.time()
                try:
                    await self.cancel([task], label)
                finally:
                    cleanup_elapsed = loop.time() - cleanup_started
                raise CleanupIncomplete("supervisor quarantined: " + label)
            done, _ = await asyncio.wait({task}, timeout=seconds)
            if done:
                result = task.result()
                outcome = "success"
                return result
            timed_out = True
            before_cleanup = loop.time() - started
            cleanup_started = loop.time()
            try:
                clean = await self.cancel([task], label)
            finally:
                cleanup_elapsed = loop.time() - cleanup_started
            if not clean:
                outcome = "cleanup_incomplete"
                raise CleanupIncomplete("cancellation incomplete: " + label)
            outcome = "timeout"
            raise OperationDeadline("total deadline exceeded: " + label)
        except asyncio.CancelledError:
            outcome = "cancelled"
            if before_cleanup is None:
                before_cleanup = loop.time() - started
            cleanup_started = loop.time()
            try:
                await self.cancel([task], label)
            finally:
                cleanup_elapsed += loop.time() - cleanup_started
            raise
        finally:
            self.record_operation(
                label,
                loop.time() - started,
                outcome,
                operation_elapsed_seconds=before_cleanup,
                cleanup_elapsed_seconds=cleanup_elapsed,
                budget_seconds=seconds,
                log_deadline=timed_out,
            )


def clock_sample() -> dict[str, float | None]:
    return {
        "wall": time.time(),
        "monotonic": time.monotonic(),
        "boottime": time.clock_gettime(time.CLOCK_BOOTTIME)
        if hasattr(time, "CLOCK_BOOTTIME")
        else None,
        "process_cpu": time.process_time(),
    }


def raw_identity() -> dict[str, Any]:
    result: dict[str, Any] = {
        "pid": os.getpid(),
        "sid": None,
        "start_ticks": None,
        "btime": None,
        "boot_id": None,
        "ticks_per_second": None,
    }
    errors = []
    for key, read in (
        ("sid", lambda: os.getsid(0)),
        (
            "start_ticks",
            lambda: int(Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()[19]),
        ),
        (
            "btime",
            lambda: next(
                int(line.split()[1])
                for line in Path("/proc/stat").read_text().splitlines()
                if line.startswith("btime ")
            ),
        ),
        ("boot_id", lambda: Path("/proc/sys/kernel/random/boot_id").read_text().strip()),
        ("ticks_per_second", lambda: os.sysconf("SC_CLK_TCK")),
    ):
        try:
            result[key] = read()
        except (OSError, ValueError, IndexError, StopIteration) as exc:
            errors.append({"field": key, "error": type(exc).__name__})
    result["unavailable"] = errors
    return result


class Diagnostics:
    def __init__(
        self,
        settings: Settings,
        *,
        clocks: Callable[[], dict[str, Any]] = clock_sample,
        identity: Callable[[], dict[str, Any]] = raw_identity,
    ):
        self.settings, self.clocks, self.identity = settings, clocks, identity
        self.previous: dict[str, Any] | None = None
        self.workers: dict[str, dict[str, Any]] = {}
        self.latest: dict[str, Any] | None = None

    def progress(self, worker: str, phase: str) -> None:
        previous = self.workers.get(worker, {})
        self.workers[worker] = {
            "sequence": previous.get("sequence", 0) + 1,
            "phase": phase,
            **self.clocks(),
        }

    def heartbeat(self, run_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        clocks = self.clocks()
        old = self.previous or {}
        same = bool(old) and old["run_id"] == run_id
        deltas: dict[str, Any] = {
            k: clocks[k] - old["clocks"][k]
            if same and clocks[k] is not None and old["clocks"][k] is not None
            else None
            for k in clocks
        }
        difference = deltas["wall"] - deltas["monotonic"] if same else None
        boot_difference = (
            deltas["boottime"] - deltas["monotonic"]
            if same and deltas["boottime"] is not None
            else None
        )
        tolerance = self.settings.clock_tolerance_seconds
        abnormal = same and (
            any(
                deltas[k] is not None
                and (deltas[k] > self.settings.continuity_gap_seconds or deltas[k] < -tolerance)
                for k in ("wall", "monotonic", "boottime")
            )
            or (difference is not None and abs(difference) > tolerance)
            or (boot_difference is not None and abs(boot_difference) > tolerance)
        )
        if (
            same
            and not abnormal
            and deltas["monotonic"] < self.settings.diagnostic_heartbeat_seconds
        ):
            return None, None
        identity = self.identity()
        if same and identity != old["identity"]:
            abnormal = True
        beat = {
            "run_id": run_id,
            "clocks": clocks,
            "identity": identity,
            "workers": {name: dict(progress) for name, progress in self.workers.items()},
        }
        gap = None
        if abnormal:
            first = int(old["clocks"]["wall"] // 300) * 300 + 300
            end = int(clocks["wall"] // 300) * 300
            count = max(0, (end - first) // 300)
            gap = {
                "run_id": run_id,
                "cause": "unknown",
                "previous": old,
                "current": beat,
                "deltas": deltas,
                "wall_minus_monotonic_delta": difference,
                "boottime_minus_monotonic_delta": boot_difference,
                "identity_changed": identity != old["identity"],
                "skipped_window_count": count,
                "skipped_windows": [
                    f"btc-updown-5m-{first + i * 300}" for i in range(min(count, 100))
                ],
                "skipped_windows_truncated": count > 100,
                "recovered_at": clocks["wall"],
            }
        if not same:
            # Progress from an earlier run is not evidence of this process's continuity.
            if old:
                self.workers.clear()
                beat["workers"] = {}
        self.previous = self.latest = beat
        return beat, gap


class SlowOperations:
    def __init__(
        self,
        threshold: float,
        throttle: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sink: Callable[[dict[str, Any]], None] | None = None,
    ):
        self.threshold, self.throttle, self.clock = threshold, throttle, clock
        self.sink = sink or (
            lambda payload: LOG.warning("slow_operation", extra={"details": payload})
        )
        self.stats: dict[str, dict[str, Any]] = {}
        self.last_emit: dict[str, float] = {}

    @contextmanager
    def measure(self, operation: str) -> Iterator[None]:
        if _EMITTING.get():
            yield
            return
        start = self.clock()
        try:
            yield
        finally:
            now = self.clock()
            elapsed = now - start
            if elapsed >= self.threshold:
                stat = self.stats.setdefault(operation, {"count": 0, "max_seconds": 0.0})
                stat.update(
                    count=stat["count"] + 1,
                    last_seconds=elapsed,
                    max_seconds=max(stat["max_seconds"], elapsed),
                )
                if now - self.last_emit.get(operation, float("-inf")) >= self.throttle:
                    self.last_emit[operation] = now
                    token = _EMITTING.set(True)
                    try:
                        self.sink(
                            {
                                "run_id": current_run_id(),
                                "operation": operation,
                                "elapsed_seconds": elapsed,
                                **stat,
                            }
                        )
                    except Exception:
                        pass  # Diagnostics must neither recurse nor replace the original error.
                    finally:
                        _EMITTING.reset(token)
