# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.4.1", "pytz==2025.2"]
# ///
"""Fetch public, free, read-only history for offline BTC 5m replay.

Sources (no credentials, no wallet, no paid data, no proxy/VPN):
- Gamma events by slug ``btc-updown-5m-{start}`` (market identity, fee schema,
  official outcome, ``closedTime``, Chainlink ``priceToBeat``/``finalPrice``).
- Pendulum Flow v3 hourly Polymarket CLOB market-channel archive (CC BY-NC 4.0,
  credit: pendulumflow). Whole-hour Parquet is downloaded, verified against the
  hour manifest sha256, filtered to the BTC 5m conditions, then deleted.
- Coinbase Exchange public BTC-USD 1 minute candles (spot reference only; the
  official settlement benchmark is Chainlink, carried by Gamma eventMetadata).

Raw data stays outside the repository (default /workspace/pm-backtest-data).
Only scripts, manifests and checksums are committed.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

GAMMA = "https://gamma-api.polymarket.com/events?slug=btc-updown-5m-{start}"
ARCHIVE = "https://dl.pendulumflow.com/v3/{day}/{hh}/{day}T{hh}.parquet"
MANIFEST = "https://dl.pendulumflow.com/v3/{day}/{hh}/manifest.json"
COINBASE = (
    "https://api.exchange.coinbase.com/products/BTC-USD/candles"
    "?granularity=60&start={start}&end={end}"
)
PT = ZoneInfo("America/Los_Angeles")
UA = {"User-Agent": "btc5m-offline-backtest/0.1 (read-only research)"}
EVENT_TYPES = ("book", "price_change", "tick_size_change", "last_trade_price", "market_resolved")
PRE_ROLL = 60
POST_ROLL = 10


def pt_now() -> str:
    return datetime.now(PT).isoformat(timespec="seconds")


def get_json(url: str, attempts: int = 6) -> object:
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60) as r:
                return json.load(r)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            if attempt == attempts - 1:
                raise
            time.sleep(2 * (attempt + 1))
    raise AssertionError


def hours(start: datetime, end: datetime) -> list[datetime]:
    out, cur = [], start.replace(minute=0, second=0, microsecond=0)
    while cur < end:
        out.append(cur)
        cur += timedelta(hours=1)
    return out


def fetch_gamma(data: Path, starts: list[int], log: dict) -> dict[int, dict]:
    folder = data / "gamma"
    folder.mkdir(parents=True, exist_ok=True)
    events: dict[int, dict] = {}
    for start in starts:
        path = folder / f"{start}.json"
        if not path.exists():
            payload = get_json(GAMMA.format(start=start))
            path.write_text(json.dumps({"fetched_at_pt": pt_now(), "events": payload}))
            time.sleep(0.03)
        payload = json.loads(path.read_text())
        if payload["events"]:
            events[start] = payload["events"][0]
    log["gamma"] = {
        "url_template": GAMMA,
        "requested_windows": len(starts),
        "found_windows": len(events),
        "missing_windows": sorted(set(starts) - set(events)),
    }
    return events


def fetch_hour(data: Path, hour: datetime, conditions: list[str], keep_raw: bool) -> dict:
    day, hh = hour.strftime("%Y-%m-%d"), hour.strftime("%H")
    extract = data / "extracts" / f"{day}T{hh}.parquet"
    info_path = extract.with_suffix(".json")
    if extract.exists() and info_path.exists():
        return json.loads(info_path.read_text())
    extract.parent.mkdir(parents=True, exist_ok=True)
    raw = data / "raw" / f"{day}T{hh}.parquet"
    raw.parent.mkdir(parents=True, exist_ok=True)
    url = ARCHIVE.format(day=day, hh=hh)
    info: dict = {"hour_utc": f"{day}T{hh}", "url": url, "manifest_url": MANIFEST.format(day=day, hh=hh)}
    try:
        manifest = get_json(info["manifest_url"])
    except urllib.error.HTTPError as exc:
        info.update(status=f"manifest_http_{exc.code}")
        info_path.write_text(json.dumps(info))
        return info
    info["manifest_sha256"] = manifest.get("sha256")
    info["manifest_bytes"] = manifest.get("bytes")
    info["manifest_rows"] = manifest.get("row_count")
    info["download_started_pt"] = pt_now()
    subprocess.run(
        ["curl", "-sS", "--retry", "5", "--retry-delay", "5", "-o", str(raw), url], check=True
    )
    info["download_finished_pt"] = pt_now()
    digest = hashlib.sha256()
    with raw.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 22), b""):
            digest.update(chunk)
    info["sha256"] = digest.hexdigest()
    info["sha256_verified"] = info["sha256"] == manifest.get("sha256")
    if not info["sha256_verified"]:
        info["status"] = "sha256_mismatch"
        raw.unlink(missing_ok=True)
        info_path.write_text(json.dumps(info))
        return info
    import duckdb

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    markets = ",".join(f"unhex('{c[2:].upper()}')" for c in conditions)
    types = ",".join(f"'{t}'" for t in EVENT_TYPES)
    con.execute(
        f"""
        COPY (
          SELECT event_type, '0x' || lower(hex(market)) AS market, hex(asset_id) AS asset_hex,
                 epoch_ms(timestamp) AS ts_ms, epoch_us(timestamp_received) AS recv_us,
                 sequence, CAST(price AS VARCHAR) AS price, CAST(size AS VARCHAR) AS size, side,
                 CAST(best_bid AS VARCHAR) AS best_bid, CAST(best_ask AS VARCHAR) AS best_ask,
                 to_json(bids) AS bids, to_json(asks) AS asks,
                 CAST(old_tick_size AS VARCHAR) AS old_tick, CAST(new_tick_size AS VARCHAR) AS new_tick,
                 fee_rate_bps, winning_outcome
          FROM read_parquet('{raw}')
          WHERE market IN ({markets}) AND event_type IN ({types})
        ) TO '{extract}' (FORMAT parquet, COMPRESSION zstd)
        """
    )
    info["extracted_rows"] = con.execute(f"SELECT count(*) FROM '{extract}'").fetchone()[0]
    info["hour_rows_by_type"] = dict(
        con.execute(f"SELECT event_type, count(*) FROM read_parquet('{raw}') GROUP BY 1").fetchall()
    )
    con.close()
    info["status"] = "ok"
    if not keep_raw:
        raw.unlink(missing_ok=True)
    info_path.write_text(json.dumps(info))
    return info


def token_decimal(asset_hex: str | None) -> str | None:
    return str(int(asset_hex, 16)) if asset_hex else None


def level_list(raw: str | None) -> list[dict[str, str]]:
    return [
        {"price": str(level["price"]), "size": str(level["size"])}
        for level in json.loads(raw or "[]")
    ]


def build_windows(data: Path, events: dict[int, dict], log: dict) -> None:
    import duckdb

    out = data / "windows"
    out.mkdir(parents=True, exist_ok=True)
    extracts = sorted(str(p) for p in (data / "extracts").glob("*.parquet"))
    con = duckdb.connect()
    con.execute(f"CREATE VIEW x AS SELECT * FROM read_parquet({extracts!r})")
    stats = []
    for start, event in sorted(events.items()):
        market = event["markets"][0]
        condition = market["conditionId"].lower()
        tokens = dict(zip(json.loads(market["outcomes"]), json.loads(market["clobTokenIds"])))
        lo, hi = (start - PRE_ROLL) * 1_000_000, (start + 300 + POST_ROLL) * 1_000_000
        rows = con.execute(
            """
            SELECT * FROM x WHERE market=? AND (
              (recv_us > ? AND recv_us <= ?) OR event_type IN ('tick_size_change','market_resolved'))
            ORDER BY recv_us, ts_ms, sequence
            """,
            [condition, lo, hi],
        ).fetchall()
        cols = [d[0] for d in con.description]
        pre = {}
        for outcome, token in tokens.items():
            hexed = f"{int(token):X}"
            row = con.execute(
                "SELECT * FROM x WHERE market=? AND event_type='book' AND asset_hex=? AND recv_us<=? "
                "ORDER BY recv_us DESC, sequence DESC LIMIT 1",
                [condition, hexed, lo],
            ).fetchone()
            pre[outcome] = dict(zip(cols, row)) if row else None
        items = [dict(zip(cols, r)) for r in rows]
        ticks = [r for r in items if r["event_type"] == "tick_size_change"]
        resolved = [r for r in items if r["event_type"] == "market_resolved"]
        initial_tick = ticks[0]["old_tick"] if ticks else market.get("orderPriceMinTickSize")
        path = out / f"{start}.jsonl.gz"
        counts = {k: 0 for k in EVENT_TYPES}
        with gzip.open(path, "wt") as stream:
            meta = {
                "kind": "meta",
                "start": start,
                "end": start + 300,
                "condition": condition,
                "tokens": tokens,
                "initial_tick": str(initial_tick),
                "initial_tick_source": "archive tick_size_change.old_tick" if ticks else "gamma orderPriceMinTickSize (closed market)",
                "gamma_event": event,
                "archive_market_resolved": [
                    {"recv_us": r["recv_us"], "winning_outcome": r["winning_outcome"]} for r in resolved
                ],
            }
            stream.write(json.dumps(meta) + "\n")
            messages = []
            for outcome, row in pre.items():
                if row is not None:
                    messages.append((row["recv_us"], row["ts_ms"], book_message(row)))
            group: list[dict] = []
            key = None

            def flush() -> None:
                if group:
                    first = group[0]
                    messages.append(
                        (
                            first["recv_us"],
                            first["ts_ms"],
                            {
                                "event_type": "price_change",
                                "market": condition,
                                "timestamp": str(first["ts_ms"]),
                                "price_changes": [
                                    {
                                        "asset_id": token_decimal(r["asset_hex"]),
                                        "price": r["price"],
                                        "size": r["size"],
                                        "side": r["side"],
                                        "best_bid": r["best_bid"],
                                        "best_ask": r["best_ask"],
                                    }
                                    for r in group
                                ],
                            },
                        )
                    )
                    group.clear()

            for r in items:
                if not lo < r["recv_us"] <= hi:
                    continue
                kind = r["event_type"]
                if kind == "price_change":
                    k = (r["recv_us"], r["ts_ms"])
                    if k != key:
                        flush()
                        key = k
                    group.append(r)
                    counts[kind] += 1
                    continue
                flush()
                key = None
                counts[kind] += 1
                if kind == "book":
                    messages.append((r["recv_us"], r["ts_ms"], book_message(r)))
                elif kind == "tick_size_change":
                    messages.append(
                        (
                            r["recv_us"],
                            r["ts_ms"],
                            {
                                "event_type": kind,
                                "asset_id": token_decimal(r["asset_hex"]),
                                "market": condition,
                                "old_tick_size": r["old_tick"],
                                "new_tick_size": r["new_tick"],
                                "timestamp": str(r["ts_ms"]),
                            },
                        )
                    )
                elif kind == "last_trade_price":
                    messages.append(
                        (
                            r["recv_us"],
                            r["ts_ms"],
                            {
                                "event_type": kind,
                                "asset_id": token_decimal(r["asset_hex"]),
                                "market": condition,
                                "price": r["price"],
                                "size": r["size"],
                                "side": r["side"],
                                "fee_rate_bps": r["fee_rate_bps"],
                                "timestamp": str(r["ts_ms"]),
                            },
                        )
                    )
            flush()
            messages.sort(key=lambda m: (m[0], m[1]))
            for recv_us, _, message in messages:
                stream.write(json.dumps({"kind": "ws", "recv_us": recv_us, "message": message}) + "\n")
        stats.append(
            {
                "start": start,
                "rows": counts,
                "messages": len(messages),
                "pre_book": {k: (v["recv_us"] if v else None) for k, v in pre.items()},
                "file": path.name,
            }
        )
    log["windows"] = stats


def book_message(row: dict) -> dict:
    return {
        "event_type": "book",
        "asset_id": token_decimal(row["asset_hex"]),
        "market": row["market"],
        "timestamp": str(row["ts_ms"]),
        "bids": level_list(row["bids"]),
        "asks": level_list(row["asks"]),
    }


def fetch_coinbase(data: Path, start: int, end: int, log: dict) -> None:
    path = data / "reference" / f"coinbase_btcusd_1m_{start}_{end}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: dict[int, list] = {}
    cursor = start
    started = pt_now()
    while cursor < end:
        chunk_end = min(cursor + 300 * 60, end)
        iso = lambda t: datetime.fromtimestamp(t, UTC).isoformat()  # noqa: E731
        try:
            payload = get_json(COINBASE.format(start=iso(cursor), end=iso(chunk_end)))
        except urllib.error.HTTPError as exc:
            log["coinbase"] = {"status": f"http_{exc.code}"}
            return
        for item in payload:  # [time, low, high, open, close, volume]
            if start <= item[0] < end:
                rows[item[0]] = item
        cursor = chunk_end
        time.sleep(0.2)
    with path.open("w") as stream:
        stream.write("time,low,high,open,close,volume\n")
        for key in sorted(rows):
            stream.write(",".join(str(v) for v in rows[key]) + "\n")
    expected = (end - start) // 60
    log["coinbase"] = {
        "url_template": COINBASE,
        "file": path.name,
        "fetched_started_pt": started,
        "fetched_finished_pt": pt_now(),
        "minutes_expected": expected,
        "minutes_found": len(rows),
        "missing_fraction": 1 - len(rows) / expected if expected else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="UTC hour, e.g. 2026-10-28T00")
    parser.add_argument("--end", required=True, help="UTC hour exclusive, e.g. 2026-10-29T00")
    parser.add_argument("--data", type=Path, default=Path("/workspace/pm-backtest-data"))
    parser.add_argument("--keep-raw", action="store_true")
    parser.add_argument("--skip-archive", action="store_true")
    args = parser.parse_args()
    start = datetime.strptime(args.start, "%Y-%m-%dT%H").replace(tzinfo=UTC)
    end = datetime.strptime(args.end, "%Y-%m-%dT%H").replace(tzinfo=UTC)
    log: dict = {"run_started_pt": pt_now(), "range_utc": [args.start, args.end]}
    starts = list(range(int(start.timestamp()), int(end.timestamp()), 300))
    # Conditions for adjacent windows are included so cross-hour books are kept.
    pad = list(range(starts[0] - 3600, starts[0], 300)) + list(range(starts[-1] + 300, starts[-1] + 1200, 300))
    events = fetch_gamma(args.data, starts + pad, log)
    target = {s: e for s, e in events.items() if s in set(starts)}
    conditions = sorted({e["markets"][0]["conditionId"] for e in events.values()})
    hour_info = []
    if not args.skip_archive:
        for hour in hours(start - timedelta(hours=1), end + timedelta(hours=1)):
            info = fetch_hour(args.data, hour, conditions, args.keep_raw)
            hour_info.append(info)
            print(json.dumps({k: info.get(k) for k in ("hour_utc", "status", "extracted_rows")}), flush=True)
    log["archive_hours"] = hour_info
    build_windows(args.data, target, log)
    fetch_coinbase(args.data, int(start.timestamp()), int(end.timestamp()), log)
    log["run_finished_pt"] = pt_now()
    name = f"fetch_log_{args.start}_{args.end}.json"
    (args.data / name).write_text(json.dumps(log, indent=1, default=str))
    print(name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
