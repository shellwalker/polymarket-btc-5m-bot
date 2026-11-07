"""Bounded, read-only explanations; these predicates never authorize a resync."""

from __future__ import annotations

import json
import math
from decimal import Decimal
from typing import Any

from .domain import Book

MAX_TOKENS = 2
MAX_LOG_BYTES = 8192
MAX_IDENTIFIER_CHARS = 128
IDENTIFIER_FIELDS = frozenset(
    {"token", "asset_id", "expected_tokens", "actual_tokens", "missing_tokens", "unexpected_tokens"}
)


def text(value: Any, limit: int = 64) -> str | None:
    if value is None:
        return None
    if not isinstance(value, (str, Decimal)):
        return "<non-text>"
    return str(value)[:limit]


def identifier(value: str) -> str:
    """Preserve normal public asset IDs independently of price/text limits."""
    return value[:MAX_IDENTIFIER_CHARS]


def token_details(
    token: str, book: Book | None, now: float, stale: float, epoch: int, sequence: int
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "token": identifier(token),
        "token_truncated": len(token) > MAX_IDENTIFIER_CHARS,
        "reason_codes": [],
    }
    reasons = result["reason_codes"]
    if book is None:
        result.update(
            bid_levels=None,
            ask_levels=None,
            best_bid=None,
            best_ask=None,
            source_age_seconds=None,
            received_age_seconds=None,
            epoch=None,
            sync_sequence=None,
        )
        reasons.append("missing_token")
        return result
    try:
        bid, ask = book.best_bid, book.best_ask
    except (ArithmeticError, ValueError, TypeError):
        bid, ask = None, None
        reasons.append("best_prices_unavailable")
    try:
        valid = book.valid_depth()
    except (ArithmeticError, ValueError, TypeError):
        valid = False
    received_age, source_age = now - book.updated_at, now - book.source_time
    try:
        uncrossed = bid is not None and ask is not None and bid < ask
    except (ArithmeticError, ValueError, TypeError):
        uncrossed = False
    predicates = {
        "book_epoch_mismatch": book.continuity_epoch == epoch,
        "book_sync_sequence_mismatch": book.continuity_sync_sequence == sequence,
        "disconnected": book.connected,
        "invalid_depth": valid,
        "empty_bids": bool(book.bids),
        "empty_asks": bool(book.asks),
        "missing_best_bid": bid is not None,
        "missing_best_ask": ask is not None,
        "crossed_or_locked": uncrossed if bid is not None and ask is not None else True,
        "received_time_future": received_age >= 0,
        "received_stale": received_age <= stale,
        "source_time_future": source_age >= -2,
        "source_stale": source_age <= stale,
        "received_age_nonfinite": math.isfinite(received_age),
        "source_age_nonfinite": math.isfinite(source_age),
    }
    reasons.extend(code for code, passed in predicates.items() if not passed)
    result.update(
        asset_id=identifier(book.asset_id),
        asset_id_truncated=len(book.asset_id) > MAX_IDENTIFIER_CHARS,
        bid_levels=len(book.bids),
        ask_levels=len(book.asks),
        best_bid=text(bid),
        best_ask=text(ask),
        tick=text(book.tick, 24),
        connected=book.connected,
        valid_depth=valid,
        source_at=book.source_time,
        received_at=book.updated_at,
        source_age_seconds=source_age if math.isfinite(source_age) else None,
        received_age_seconds=received_age if math.isfinite(received_age) else None,
        epoch=book.continuity_epoch,
        sync_sequence=book.continuity_sync_sequence,
    )
    return result


def describe_books(
    expected: set[str], books: dict[str, Book], now: float, stale: float, epoch: int, sequence: int
) -> list[dict[str, Any]]:
    selected = sorted(expected or set(books))[:MAX_TOKENS]
    return [
        token_details(token, books.get(token), now, stale, epoch, sequence) for token in selected
    ]


def failure_details(
    expected: set[str],
    books: dict[str, Book],
    now: float,
    stale: float,
    supplied_epoch: int,
    current_epoch: int,
    sequence: int,
    lease_matches: bool,
    supplied_present: bool,
    current_present: bool,
) -> dict[str, Any]:
    actual = set(books)
    tokens = describe_books(expected, books, now, stale, supplied_epoch, sequence)
    reasons = []
    if supplied_epoch != current_epoch:
        reasons.append("epoch_mismatch")
    if not lease_matches:
        reasons.append("lease_mismatch")
    if actual != expected:
        reasons.append("tokens_mismatch")
    if not books:
        reasons.append("no_books")
    reasons.extend(code for token in tokens for code in token["reason_codes"])
    return {
        "sample_wall": now,
        "reason_codes": sorted(set(reasons)),
        "epochs": {"supplied": supplied_epoch, "current": current_epoch},
        "lease": {
            "matches": lease_matches,
            "supplied_present": supplied_present,
            "current_present": current_present,
            "current_sync_sequence": sequence,
        },
        "expected_tokens": [identifier(t) for t in sorted(expected)[:MAX_TOKENS]],
        "actual_tokens": [identifier(t) for t in sorted(actual)[:MAX_TOKENS]],
        "missing_tokens": [identifier(t) for t in sorted(expected - actual)[:MAX_TOKENS]],
        "unexpected_tokens": [identifier(t) for t in sorted(actual - expected)[:MAX_TOKENS]],
        "expected_token_count": len(expected),
        "actual_token_count": len(actual),
        "token_lists_truncated": len(expected) > MAX_TOKENS or len(actual) > MAX_TOKENS,
        "token_text_truncated": any(len(t) > MAX_IDENTIFIER_CHARS for t in expected | actual)
        or any(len(book.asset_id) > MAX_IDENTIFIER_CHARS for book in books.values()),
        "tokens": tokens,
        "uninspected_actual_token_count": len(actual - {t for t in sorted(expected or actual)[:2]}),
    }


def bounded(details: dict[str, Any]) -> dict[str, Any]:
    """Keep reasons/counts while bounding optional strings and pre-poll observations."""
    result = dict(details)

    def encoded(value: Any) -> int:
        return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())

    if encoded(result) <= MAX_LOG_BYTES:
        return result
    result.pop("pre_poll_tokens", None)
    result["diagnostics_truncated"] = True

    def shorten(value: Any, key: str = "") -> Any:
        if key == "reason_codes":
            return value
        if isinstance(value, str):
            return value[:MAX_IDENTIFIER_CHARS] if key in IDENTIFIER_FIELDS else value[:24]
        if isinstance(value, list):
            return [shorten(item, key) for item in value]
        if isinstance(value, dict):
            return {k: shorten(v, k) for k, v in value.items()}
        return value

    result = dict(shorten(result))
    if encoded(result) > MAX_LOG_BYTES:
        # Fixed report keys, two tokens and bounded reasons normally fit well below
        # budget. Never forward an unbounded unexpected diagnostic structure.
        return {
            "reason_codes": result["reason_codes"],
            "diagnostics_truncated": True,
            "diagnostic_error": "report_budget_exceeded",
        }
    return result
