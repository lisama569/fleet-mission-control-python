"""Shared pytest fixtures and logging helpers.

Every test prints the simulated input events (submit / dispatch / timeout /
cancel / offline) and the resulting engine decisions as both log records
(live via ``--log-cli-level=INFO``) and captured stdout (always visible via
``-s`` / in JUnit output). This keeps scenarios reproducible and auditable.
"""
from __future__ import annotations

import logging
import time

import pytest

from fleet.engine import Engine
from fleet.simulator import VirtualClock

log = logging.getLogger("fleet.test")


@pytest.fixture
def clk():
    return VirtualClock(start_ms=0)


@pytest.fixture
def engine(clk):
    eng = Engine(clock=clk)
    yield eng
    # Cancel anything still running, advance virtual time and let parked
    # attempt threads unwind so no worker thread leaks between tests.
    with eng.store.lock:
        task_ids = list(eng.store.tasks)
    for tid in task_ids:
        eng.cancel_task(tid)
    clk.advance(60 * 60 * 1000)
    assert eng.wait_idle(2.0), "attempt threads still running after test"


def record_case(title: str, **fields) -> None:
    parts = " ".join(f"{k}={v!r}" for k, v in fields.items())
    line = f"[CASE] {title} :: {parts}"
    log.info(line)
    print(line, flush=True)


def log_decisions(engine: Engine, title: str) -> list[dict]:
    events = engine.events_since(0)
    log.info("[TRACE] %s — %d engine events", title, len(events))
    print(f"[TRACE] {title} — {len(events)} engine events:", flush=True)
    for ev in events:
        line = (f"  seq={ev['seq']:>3} t={ev['at_ms']:>5} "
                f"{ev['type']:<26} {ev['detail']}")
        log.info(line)
        print(line, flush=True)
    rejects = engine.recent_rejects()
    if rejects:
        print(f"[REJECTS] {title}:", flush=True)
        for r in rejects:
            line = f"  t={r['at_ms']} {r['step_key']} -> {r['reason']} :: {r['detail']}"
            log.info(line)
            print(line, flush=True)
    return events


def park_or_finish(engine: Engine, clk: VirtualClock, waiters: int,
                   tries: int = 200) -> bool:
    """Wait until waiters parked OR no attempt controls remain."""
    for _ in range(tries):
        with engine.store.lock:
            active = len(engine._controls)
        if active == 0:
            return False
        if clk._waiters >= waiters:
            return True
        time.sleep(0.005)
    return clk._waiters >= waiters
