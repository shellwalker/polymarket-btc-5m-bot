"""Explicit transactional migrations. Readers never call this module's migrate()."""

from __future__ import annotations

import sqlite3
from decimal import Decimal

CURRENT_VERSION = "2"
SUPPORTED_VERSIONS = {"1", CURRENT_VERSION}
BASE_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS orders("
    "slug TEXT PRIMARY KEY, market TEXT NOT NULL, outcome TEXT NOT NULL,"
    "quantity TEXT NOT NULL, cost TEXT NOT NULL, fee TEXT NOT NULL,"
    "created REAL NOT NULL, settled INTEGER NOT NULL DEFAULT 0)",
    "CREATE TABLE IF NOT EXISTS order_windows("
    "order_id TEXT PRIMARY KEY REFERENCES orders(slug), window_slug TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS order_windows_slug ON order_windows(window_slug)",
    "CREATE TABLE IF NOT EXISTS fills("
    "slug TEXT NOT NULL REFERENCES orders(slug), level INTEGER NOT NULL,"
    "price TEXT NOT NULL, quantity TEXT NOT NULL, fee TEXT NOT NULL, PRIMARY KEY(slug,level))",
    "CREATE TABLE IF NOT EXISTS settlements("
    "slug TEXT PRIMARY KEY REFERENCES orders(slug), winner TEXT NOT NULL,"
    "payout TEXT NOT NULL,pnl TEXT NOT NULL,created REAL NOT NULL,evidence TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS daily(day TEXT PRIMARY KEY,loss TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS events("
    "id INTEGER PRIMARY KEY,kind TEXT NOT NULL,payload TEXT NOT NULL,created REAL NOT NULL)",
)
OBSERVABILITY_SCHEMA = (
    "ALTER TABLE events ADD COLUMN run_id TEXT",
    "CREATE INDEX events_run_time ON events(run_id,created)",
    "CREATE INDEX events_time ON events(created)",
    "CREATE TABLE equity_snapshots("
    "id INTEGER PRIMARY KEY,created REAL NOT NULL,run_id TEXT NOT NULL,payload TEXT NOT NULL)",
    "CREATE INDEX equity_run_time ON equity_snapshots(run_id,created)",
    "CREATE TABLE circuit_intervals("
    "id INTEGER PRIMARY KEY,reason TEXT NOT NULL,started REAL,observed_since REAL NOT NULL,"
    "ended REAL,start_run_id TEXT,end_run_id TEXT,rejected_signals INTEGER NOT NULL DEFAULT 0)",
    "CREATE TABLE circuit_runs("
    "interval_id INTEGER NOT NULL REFERENCES circuit_intervals(id),run_id TEXT NOT NULL,"
    "observed_since REAL NOT NULL,PRIMARY KEY(interval_id,run_id))",
    "CREATE UNIQUE INDEX one_active_circuit ON circuit_intervals((1)) WHERE ended IS NULL",
)


def version(db: sqlite3.Connection) -> str | None:
    """Read before any DDL/PRAGMA that could modify an unknown future database."""
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not tables:
        return None
    if "meta" not in tables:
        raise ValueError("missing ledger schema metadata")
    row = db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    if row is None or row[0] not in SUPPORTED_VERSIONS:
        raise ValueError("unsupported ledger schema version")
    return str(row[0])


def migrate(db: sqlite3.Connection, initial_cash: Decimal) -> None:
    version(db)  # Refuse future schemas before even taking a writer lock.
    db.execute("BEGIN IMMEDIATE")
    try:
        current = version(db)
        if current is None:
            for statement in BASE_SCHEMA:
                db.execute(statement)
            db.executemany(
                "INSERT INTO meta VALUES (?,?)",
                (
                    ("cash", str(initial_cash)),
                    ("initial_cash", str(initial_cash)),
                    ("consecutive_losses", "0"),
                    ("circuit", ""),
                    ("schema_version", "1"),
                ),
            )
            current = "1"
        if current == "1":
            # Also support original v1 ledgers predating configurable entry allowance.
            db.execute(BASE_SCHEMA[2])
            db.execute(BASE_SCHEMA[3])
            db.execute("INSERT OR IGNORE INTO order_windows SELECT slug,slug FROM orders")
            for statement in OBSERVABILITY_SCHEMA:
                db.execute(statement)
            db.execute("UPDATE meta SET value=? WHERE key='schema_version'", (CURRENT_VERSION,))
        db.commit()
    except BaseException:
        db.rollback()
        raise
