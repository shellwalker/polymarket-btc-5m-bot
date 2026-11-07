"""Strict, credential-free configuration for the paper-only executable."""

from __future__ import annotations

import math
import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, fields
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if str(value).lower() in {"true", "1", "yes"}:
        return True
    if str(value).lower() in {"false", "0", "no"}:
        return False
    raise ValueError("boolean must be true/false")


@dataclass(frozen=True)
class Settings:
    runtime_dir: Path = Path("runtime")
    live: bool = False
    initial_cash: Decimal = Decimal("1000")
    max_trade: Decimal = Decimal("10")
    max_exposure: Decimal = Decimal("50")
    daily_loss_limit: Decimal = Decimal("20")
    max_consecutive_losses: int = 3
    stale_seconds: float = 5.0
    max_spread: Decimal = Decimal("0.05")
    max_slippage: Decimal = Decimal("0.02")
    signal_threshold: Decimal = Decimal("0.03")
    min_momentum: Decimal = Decimal("0.03")
    max_entry_price: Decimal = Decimal("0.85")
    shares: Decimal = Decimal("5")
    min_time_remaining: float = 30.0
    discovery_seconds: float = 5.0
    resolution_seconds: float = 15.0
    http_timeout: float = 10.0
    retry_initial: float = 1.0
    retry_max: float = 30.0
    heartbeat_seconds: float = 10.0
    pong_timeout: float = 20.0

    def __post_init__(self) -> None:
        if self.live:
            raise ValueError("live=true is forbidden: this program only supports paper trading")
        for field in fields(self):
            value = getattr(self, field.name)
            if isinstance(value, Decimal):
                if not value.is_finite() or value <= 0:
                    raise ValueError(f"{field.name} must be finite and positive")
            elif isinstance(value, (float, int)) and not isinstance(value, bool):
                if not math.isfinite(value) or value <= 0:
                    raise ValueError(f"{field.name} must be finite and positive")
        if self.max_spread >= 1 or self.max_slippage >= 1 or self.max_entry_price >= 1:
            raise ValueError("price limits must be less than 1")
        if self.min_time_remaining >= 300:
            raise ValueError("min_time_remaining must be less than the 300s window")
        if self.retry_max < self.retry_initial:
            raise ValueError("retry_max must be >= retry_initial")
        if self.pong_timeout <= self.heartbeat_seconds:
            raise ValueError("pong_timeout must exceed heartbeat_seconds")
        if self.heartbeat_seconds > 10:
            raise ValueError("heartbeat_seconds must be <= 10")

    @classmethod
    def load(cls, path: Path | None = None, environ: Mapping[str, str] | None = None) -> Settings:
        values: dict[str, Any] = {}
        if path is not None:
            with path.open("rb") as stream:
                values = tomllib.load(stream)
            if "live" in values and _bool(values["live"]):
                raise ValueError("live=true is forbidden: this program only supports paper trading")
        defaults = cls()
        names = {field.name for field in fields(cls)}
        environment = os.environ if environ is None else environ
        for key, value in environment.items():
            if key == "BTC5M_LIVE" and _bool(value):
                raise ValueError("live=true is forbidden: this program only supports paper trading")
            if key.startswith("BTC5M_"):
                values[key.removeprefix("BTC5M_").lower()] = value
        if values.keys() - names:
            raise ValueError(f"unknown configuration keys: {sorted(values.keys() - names)}")
        converted: dict[str, Any] = {}
        for key, value in values.items():
            template = getattr(defaults, key)
            try:
                if isinstance(template, bool):
                    converted[key] = _bool(value)
                elif isinstance(template, Decimal):
                    converted[key] = Decimal(str(value))
                elif isinstance(template, Path):
                    if not isinstance(value, str):
                        raise ValueError("path must be a string")
                    converted[key] = Path(value)
                elif isinstance(template, int):
                    if isinstance(value, bool) or str(value) != str(int(str(value))):
                        raise ValueError("integer required")
                    converted[key] = int(str(value))
                else:
                    if isinstance(value, bool):
                        raise ValueError("number required")
                    converted[key] = float(value)
            except (ValueError, TypeError, InvalidOperation) as exc:
                raise ValueError(f"invalid {key}: {exc}") from exc
        return cls(**converted)
