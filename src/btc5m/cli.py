"""Credential-free paper CLI. No cron, systemd, or real trading paths."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import subprocess
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import httpx

from .config import Settings
from .ledger import Ledger
from .public import PublicAPI
from .service import RuntimeLock, Service, read_status


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        result = {
            "time": record.created,
            "level": record.levelname,
            "event": record.getMessage(),
            "details": getattr(record, "details", {}),
        }
        if record.exc_info:
            result["exception"] = self.formatException(record.exc_info)
        return json.dumps(result, ensure_ascii=False, default=str)


def configure_logs(directory: Path) -> None:
    logs = directory / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(logs / "paper.jsonl", maxBytes=5_000_000, backupCount=5)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("btc5m")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False


async def run(settings: Settings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        logging.getLogger(__name__).info("emergency_stop", extra={"details": {"source": "signal"}})
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, request_stop)
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            await Service(settings, PublicAPI(settings, client)).run(stop)
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


def emit(value: dict[str, Any]) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="BTC 5m paper-only public-data service")
    parser.add_argument("--config", type=Path, help="strict TOML; BTC5M_* environment overrides")
    parser.add_argument(
        "command", choices=["run", "start", "status", "health", "summary", "stop", "reset-circuit"]
    )
    args = parser.parse_args()
    try:
        settings = Settings.load(args.config)
        directory = settings.runtime_dir.resolve()
        if args.command in {"status", "health"}:
            emit(read_status(directory))
        elif args.command == "summary":
            emit(Ledger.read_summary(directory / "ledger.sqlite3"))
        elif args.command == "stop":
            directory.mkdir(parents=True, exist_ok=True)
            (directory / "STOP").touch()
            emit({"stop_requested": True, "file": str(directory / "STOP")})
        elif args.command == "reset-circuit":
            with RuntimeLock(directory):
                ledger = Ledger(directory / "ledger.sqlite3", settings)
                try:
                    ledger.reset_circuit()
                    emit(ledger.summary())
                finally:
                    ledger.close()
        elif args.command == "run":
            configure_logs(directory)
            asyncio.run(run(settings))
        else:
            if read_status(directory)["running"]:
                raise ValueError("paper service already running")
            if (directory / "STOP").exists():
                raise ValueError("STOP file exists; inspect risk then remove it explicitly")
            directory.mkdir(parents=True, exist_ok=True)
            command = [sys.executable, "-m", "btc5m"]
            if args.config is not None:
                command.extend(["--config", str(args.config.resolve())])
            command.append("run")
            with (directory / "launcher.log").open("a") as output:
                child = subprocess.Popen(
                    command,
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            for _ in range(100):
                status = read_status(directory)
                if status["running"] and status.get("pid") == child.pid:
                    emit({"started": True, "pid": child.pid, "runtime_dir": str(directory)})
                    return
                if child.poll() is not None:
                    raise RuntimeError("service startup failed; inspect launcher.log")
                time.sleep(0.05)
            raise RuntimeError("startup not confirmed; inspect status/launcher.log before retrying")
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
