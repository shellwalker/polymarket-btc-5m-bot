"""Read-only, range-scoped observations. No migration or historical reconstruction."""

from __future__ import annotations

import json
import math
import sqlite3
import time
from collections import Counter
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any

from .domain import ZERO, number
from .schema import version
from .telemetry import update_drawdown


def timestamp(value: str) -> float:
    try:
        result = float(value)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ValueError("ISO timestamp requires an explicit timezone")
            result = parsed.timestamp()
        except (ValueError, OverflowError) as exc:
            raise ValueError("time must be epoch seconds or ISO with explicit timezone") from exc
    if not math.isfinite(result):
        raise ValueError("time must be finite")
    return result


def _range_clause(
    since: float | None, until: float | None, column: str = "created"
) -> tuple[list[str], list[Any]]:
    clauses, params = [], []
    if since is not None:
        clauses.append(f"{column}>=?")
        params.append(since)
    if until is not None:
        clauses.append(f"{column}<?")
        params.append(until)
    return clauses, params


def _run_clause(clauses: list[str], params: list[Any], run_id: str | None) -> None:
    if run_id == "legacy":
        clauses.append("run_id IS NULL")
    elif run_id is not None:
        clauses.append("run_id=?")
        params.append(run_id)


def read_stats(
    path: Path,
    *,
    since: float | None = None,
    until: float | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    if any(v is not None and not math.isfinite(v) for v in (since, until)):
        raise ValueError("time must be finite")
    if since is not None and until is not None and since >= until:
        raise ValueError("--since must precede --until")
    try:
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            try:
                schema = version(db)
                if schema is None:
                    raise ValueError("missing ledger schema")
                return _stats(db, schema, since, until, run_id)
            finally:
                db.rollback()
    except sqlite3.Error as exc:
        raise ValueError(f"cannot read statistics: {exc}") from exc


def _stats(
    db: sqlite3.Connection,
    schema: str,
    since: float | None,
    until: float | None,
    run_id: str | None,
) -> dict[str, Any]:
    clauses, params = _range_clause(since, until)
    # A v1 reader projects a NULL without assigning or writing legacy attribution.
    run_column = "run_id" if schema == "2" else "NULL AS run_id"
    _run_clause(clauses, params, run_id)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    events = db.execute(
        f"SELECT id,kind,payload,created,{run_column} FROM events{where} ORDER BY created,id",
        params,
    ).fetchall()
    counts = Counter(str(row["kind"]) for row in events)
    runs = Counter(row["run_id"] or "legacy" for row in events)
    samples = []
    rejection_counts: Counter[int] = Counter()
    for row in events:
        payload = json.loads(row["payload"])
        if row["kind"] == "paper_fill" and payload.get("execution_slippage") is not None:
            samples.append(number(payload["execution_slippage"]))
        if row["kind"] == "circuit_signal_rejected":
            rejection_counts[int(payload["interval_id"])] += 1
    snapshots = []
    intervals = []
    if schema == "2":
        snapshots = [
            json.loads(row[0])
            for row in db.execute(
                f"SELECT payload FROM equity_snapshots{where} ORDER BY created,id", params
            )
        ]
        # Intervals are included by overlap and process participation, not reconstructed
        # from old breaker/reset events. Their full duration/count and selection count
        # remain separate so a partial query never presents an invented interval.
        as_of = time.time()
        for row in db.execute("SELECT * FROM circuit_intervals ORDER BY id"):
            start = row["started"] if row["started"] is not None else row["observed_since"]
            if since is not None and row["ended"] is not None and row["ended"] <= since:
                continue
            if until is not None and start >= until:
                continue
            if run_id is not None:
                participates = (
                    row["start_run_id"] == run_id
                    or row["end_run_id"] == run_id
                    or row["id"] in rejection_counts
                    or db.execute(
                        "SELECT 1 FROM circuit_runs WHERE interval_id=? AND run_id=?",
                        (row["id"], run_id),
                    ).fetchone()
                    is not None
                )
                if run_id == "legacy":
                    participates = row["start_run_id"] is None
                if not participates:
                    continue
            end = row["ended"] if row["ended"] is not None else as_of
            intervals.append(
                {
                    **dict(row),
                    "duration_seconds": end - row["started"]
                    if row["started"] is not None
                    else None,
                    "duration_as_of": as_of if row["ended"] is None else row["ended"],
                    "observed_duration_seconds": max(0, end - row["observed_since"]),
                    "selected_rejected_signals": rejection_counts[row["id"]],
                }
            )
    drawdown: dict[str, Any] = {"bid": {}, "mid": {}}
    complete = 0
    for snapshot in snapshots:
        if snapshot["complete"]:
            complete += 1
        for basis in ("bid", "mid"):
            equity = number(snapshot[f"equity_{basis}"]) if snapshot["complete"] else None
            drawdown[basis] = update_drawdown(drawdown[basis], equity)
    return {
        "schema_version": schema,
        "filters": {"since_inclusive": since, "until_exclusive": until, "run_id": run_id},
        "event_count": len(events),
        "event_counts": dict(sorted(counts.items())),
        "run_counts": dict(sorted(runs.items())),
        "first_event_at": events[0]["created"] if events else None,
        "last_event_at": events[-1]["created"] if events else None,
        "execution_slippage_samples": len(samples),
        "mean_execution_slippage": str(sum(samples, ZERO) / len(samples)) if samples else None,
        "equity_snapshot_count": len(snapshots),
        "complete_equity_snapshot_count": complete,
        "latest_equity": snapshots[-1] if snapshots else None,
        "selected_drawdown": drawdown if complete else None,
        "circuit_intervals": intervals,
        "caveat": "Forward observations only; missing equity samples do not establish drawdown. "
        "Interval totals span their entire lifetime; selected counts honor filters.",
    }
