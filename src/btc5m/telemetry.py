"""Pure measurements and process context; no trading decisions or book mutations."""

from __future__ import annotations

import os
from contextvars import ContextVar
from decimal import Decimal
from typing import Any
from uuid import uuid4

from .domain import ZERO, Book, number

_PROCESS_PID = os.getpid()
_PROCESS_ID = str(uuid4())
RUN_ID: ContextVar[str | None] = ContextVar("btc5m_run_id", default=None)


def process_run_id() -> str:
    global _PROCESS_PID, _PROCESS_ID
    # A fork is also a new process, even without exec/importing this module again.
    if _PROCESS_PID != os.getpid():
        _PROCESS_PID, _PROCESS_ID = os.getpid(), str(uuid4())
        RUN_ID.set(None)
    return _PROCESS_ID


def current_run_id() -> str:
    process_id = process_run_id()
    return RUN_ID.get() or process_id


def quote_snapshot(book: Book | None, now: float, stale_seconds: float) -> dict[str, Any]:
    if book is None:
        return {
            "best_bid": None,
            "best_ask": None,
            "mid": None,
            "source_at": None,
            "received_at": None,
            "source_age_seconds": None,
            "received_age_seconds": None,
            "observed_at": now,
            "healthy": False,
            "status": "missing",
            "source": "clob_public_book",
        }
    healthy = book.healthy(now, stale_seconds)
    return {
        "best_bid": str(book.best_bid) if book.best_bid is not None else None,
        "best_ask": str(book.best_ask) if book.best_ask is not None else None,
        "mid": str(book.midpoint) if book.midpoint is not None else None,
        "source_at": book.source_time,
        "received_at": book.updated_at,
        "source_age_seconds": now - book.source_time,
        "received_age_seconds": now - book.updated_at,
        "observed_at": now,
        "healthy": healthy,
        "status": "healthy" if healthy else "stale_or_invalid",
        "source": "clob_public_book",
    }


def update_drawdown(previous: dict[str, Any], equity: Decimal | None) -> dict[str, Any]:
    """Observed complete-snapshot peak/drawdown; missing marks leave history untouched."""
    if equity is None:
        return {**previous, "current_drawdown": None, "current_drawdown_fraction": None}
    peak = max(number(previous["peak"]), equity) if previous.get("peak") is not None else equity
    drawdown = max(peak - equity, ZERO)
    fraction = drawdown / peak if peak > ZERO else ZERO
    return {
        "peak": str(peak),
        "current_drawdown": str(drawdown),
        "current_drawdown_fraction": str(fraction),
        "max_drawdown": str(max(number(previous.get("max_drawdown", "0")), drawdown)),
        "max_drawdown_fraction": str(
            max(number(previous.get("max_drawdown_fraction", "0")), fraction)
        ),
    }


def equity_snapshot(
    cash: Decimal,
    positions: list[dict[str, Any]],
    books: dict[str, Book],
    now: float,
    stale_seconds: float,
    previous_drawdown: dict[str, Any],
) -> dict[str, Any]:
    marked = []
    complete = True
    bid_value, mid_value = ZERO, ZERO
    for position in positions:
        quote = quote_snapshot(books.get(position["token"]), now, stale_seconds)
        usable = quote["healthy"]
        qty = number(position["quantity"])
        bid = qty * number(quote["best_bid"]) if usable else None
        mid = qty * number(quote["mid"]) if usable else None
        marked.append(
            {
                **position,
                "quote": quote,
                "mark_status": quote["status"],
                "value_bid": str(bid) if bid is not None else None,
                "value_mid": str(mid) if mid is not None else None,
            }
        )
        complete = complete and usable
        if bid is not None and mid is not None:
            bid_value += bid
            mid_value += mid
    equity_bid = cash + bid_value if complete else None
    equity_mid = cash + mid_value if complete else None
    return {
        "created": now,
        "cash": str(cash),
        "complete": complete,
        "positions": marked,
        "positions_value_bid": str(bid_value) if complete else None,
        "positions_value_mid": str(mid_value) if complete else None,
        "equity_bid": str(equity_bid) if equity_bid is not None else None,
        "equity_mid": str(equity_mid) if equity_mid is not None else None,
        "drawdown": {
            "bid": update_drawdown(previous_drawdown.get("bid", {}), equity_bid),
            "mid": update_drawdown(previous_drawdown.get("mid", {}), equity_mid),
        },
        "valuation_basis": "gross displayed CLOB best bid/mid; not executable liquidation value",
    }
