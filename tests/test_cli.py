import json
import os
import subprocess
import sys

import pytest


def cli(tmp_path, *args, **kwargs):
    env = {k: v for k, v in os.environ.items() if not k.startswith("BTC5M_")}
    env["BTC5M_RUNTIME_DIR"] = str(tmp_path)
    return subprocess.run(
        [sys.executable, "-m", "btc5m", *args],
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
        **kwargs,
    )


def test_cli_refuses_live_before_network_or_runtime_creation(tmp_path):
    config = tmp_path / "bad.toml"
    config.write_text('live=true\nruntime_dir="never-created"\n')
    result = cli(tmp_path, "--config", str(config), "run")
    assert result.returncode != 0
    assert "paper" in result.stderr
    assert not (tmp_path / "ledger.sqlite3").exists()


def test_cli_status_summary_stop_commands(tmp_path):
    result = cli(tmp_path, "status")
    assert result.returncode == 0
    assert not json.loads(result.stdout)["running"]
    assert not (tmp_path / "ledger.sqlite3").exists()
    assert cli(tmp_path, "stop").returncode == 0
    assert (tmp_path / "STOP").exists()
    result = cli(tmp_path, "run")
    assert result.returncode == 0
    result = cli(tmp_path, "summary")
    assert result.returncode == 0
    assert json.loads(result.stdout)["fills"] == 0


def test_stop_file_clean_shutdown_offline_stopped_run(tmp_path):
    # STOP makes the executable's run path bounded and guarantees no network.
    (tmp_path / "STOP").touch()
    result = cli(tmp_path, "run")
    assert result.returncode == 0
    assert not (tmp_path / "pid.json").exists()
    health = json.loads((tmp_path / "health.json").read_text())
    assert health["stopped"] and not health["trading_ready"]
    log = (tmp_path / "logs" / "paper.jsonl").read_text().splitlines()
    assert any(json.loads(line)["event"] == "service_stopped" for line in log)


@pytest.mark.parametrize("source", ["signal", "file"])
def test_emergency_interrupts_startup_network_wait_and_cleans_pid(tmp_path, source):
    import signal
    import time

    env = {k: v for k, v in os.environ.items() if not k.startswith("BTC5M_")}
    env["BTC5M_RUNTIME_DIR"] = str(tmp_path)
    script = """import asyncio,sys
from btc5m.cli import main
from btc5m.public import PublicAPI
async def slow(self,*args,**kwargs):
    await asyncio.sleep(60)
PublicAPI.get=slow
sys.argv=['btc5m','run']
main()
"""
    child = subprocess.Popen(
        [sys.executable, "-c", script],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 3
        while not (tmp_path / "pid.json").exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert (tmp_path / "pid.json").exists()
        if source == "signal":
            child.send_signal(signal.SIGTERM)
        else:
            (tmp_path / "STOP").touch()
        out, err = child.communicate(timeout=3)
        assert child.returncode == 0, (out, err)
        assert not (tmp_path / "pid.json").exists()
        assert json.loads((tmp_path / "health.json").read_text())["stopped"]
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate()


def test_detached_start_status_stop_offline(tmp_path, monkeypatch, capsys):
    from btc5m import cli as module
    from btc5m.service import read_status

    # Test-only sitecustomize is inherited by the detached child; no HTTP escapes.
    hooks = tmp_path / "hooks"
    hooks.mkdir()
    (hooks / "sitecustomize.py").write_text("""import asyncio
from btc5m.public import PublicAPI
async def offline(self,*args,**kwargs):
    await asyncio.sleep(60)
PublicAPI.get=offline
""")
    monkeypatch.setenv("PYTHONPATH", str(hooks))
    monkeypatch.setenv("BTC5M_RUNTIME_DIR", str(tmp_path))
    original = subprocess.Popen
    children = []

    def launch(*args, **kwargs):
        child = original(*args, **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(module.subprocess, "Popen", launch)
    monkeypatch.setattr(sys, "argv", ["btc5m", "start"])
    try:
        module.main()
        assert json.loads(capsys.readouterr().out)["started"]
        assert read_status(tmp_path)["running"]
        with pytest.raises(SystemExit):
            module.main()  # duplicate start refuses before launching a second process
        assert len(children) == 1
        monkeypatch.setattr(sys, "argv", ["btc5m", "stop"])
        module.main()
        assert children[0].wait(timeout=3) == 0
        assert not read_status(tmp_path)["running"]
        assert not (tmp_path / "pid.json").exists()
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.wait()


def test_stats_cli_is_read_only_filters_and_rejects_naive_time(tmp_path):
    from btc5m.config import Settings
    from btc5m.ledger import Ledger

    ledger = Ledger(tmp_path / "ledger.sqlite3", Settings(), run_id="test-run")
    ledger.event("sample", {}, 1800000000)
    ledger.record_equity({}, 1800000000)
    ledger.close()
    path = tmp_path / "ledger.sqlite3"
    before = path.read_bytes()
    result = cli(
        tmp_path, "stats", "--since", "1799999999", "--until", "1800000001", "--run-id", "test-run"
    )
    assert result.returncode == 0, result.stderr
    stat = json.loads(result.stdout)
    assert stat["event_counts"] == {"sample": 1}
    assert stat["equity_snapshot_count"] == 1
    assert path.read_bytes() == before
    result = cli(tmp_path, "stats", "--since", "2026-11-07T10:00:00")
    assert result.returncode == 2 and "timezone" in result.stderr
    assert path.read_bytes() == before
    absent = tmp_path / "missing"
    absent.mkdir()
    assert cli(absent, "stats").returncode == 2
    assert not (absent / "ledger.sqlite3").exists()


def test_run_id_changes_between_processes_and_covers_all_event_rows(tmp_path):
    import sqlite3

    (tmp_path / "STOP").touch()
    assert cli(tmp_path, "run").returncode == 0
    assert cli(tmp_path, "run").returncode == 0
    with sqlite3.connect(tmp_path / "ledger.sqlite3") as db:
        rows = db.execute("SELECT run_id,payload FROM events").fetchall()
        runs = db.execute(
            "SELECT DISTINCT run_id FROM events WHERE kind='service_stopped'"
        ).fetchall()
    assert len(runs) == 2
    assert all(run_id and json.loads(payload)["run_id"] == run_id for run_id, payload in rows)
    logs = [json.loads(line) for line in (tmp_path / "logs/paper.jsonl").read_text().splitlines()]
    assert {r["run_id"] for r in logs} == {r[0] for r in runs}
    assert all(r["details"]["run_id"] == r["run_id"] for r in logs)
