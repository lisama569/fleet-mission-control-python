"""Concurrency, duplicate-submission, timeout and cancellation scenarios."""
from __future__ import annotations

import threading
import time

from fleet.models import StepSpec, TaskSpec

from conftest import log_decisions, park_or_finish, record_case


def step(key, cap="nav", **kw):
    return StepSpec(key=key, capability=cap, **kw)


def test_concurrent_submissions_respect_capacity(engine, clk):
    """Many threads submit at once; a capacity-1 unit serializes dispatch."""
    engine.register_unit("u1", frozenset({"nav"}), 1)
    n = 8
    barrier = threading.Barrier(n)
    errors = []

    def worker(i: int):
        try:
            barrier.wait()
            spec = TaskSpec(steps=(step(f"s{i}", run_ms=10, timeout_ms=5000),))
            engine.submit(f"conc-{i}", spec)
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    record_case("concurrent_submit", threads=n, unit_capacity=1)
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors

    engine.drive_until_idle(20000)
    views = engine.list_tasks()
    assert len(views) == n
    for v in views:
        assert v["state"] == "SUCCEEDED", v["task_id"]
        assert v["steps"][list(v["steps"])[0]]["state"] == "SUCCEEDED"
    # Capacity never exceeded 1 at any dispatch-accept event.
    events = log_decisions(engine, "concurrent_submit")
    for ev in events:
        if ev["type"] == "DISPATCH_ACCEPTED":
            assert ev["detail"]["capacity_used"] <= 1
    assert engine.unit_view("u1")["capacity_used"] == 0


def test_concurrent_duplicate_submissions_create_one_task(engine, clk):
    engine.register_unit("u1", frozenset({"nav"}), 1)
    n = 6
    barrier = threading.Barrier(n)
    created = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        _, is_created = engine.submit(
            "dup-key",
            TaskSpec(steps=(step("only", run_ms=10, timeout_ms=1000),)))
        with lock:
            created.append(is_created)

    record_case("concurrent_duplicate", threads=n, idempotency_key="dup-key")
    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    engine.drive_until_idle(2000)
    assert created.count(True) == 1
    assert created.count(False) == n - 1
    assert len(engine.list_tasks()) == 1
    views = engine.list_tasks()
    assert len(views[0]["steps"]) == 1
    assert len(views[0]["steps"]["only"]["attempts"]) == 1
    log_decisions(engine, "concurrent_duplicate")


def test_concurrent_dispatch_capacity_never_exceeded(engine, clk):
    engine.register_unit("u1", frozenset({"nav"}), 1)
    specs = [(f"d-{i}", TaskSpec(steps=(step(f"x{i}", run_ms=1000, timeout_ms=50000),)))
             for i in range(5)]
    for key, sp in specs:
        engine.submit(key, sp)
    engine.tick()  # first one dispatched
    barrier = threading.Barrier(4)
    rejected = []
    lock = threading.Lock()

    def dispatch_other(key):
        barrier.wait()
        tid = engine.store._idem[key]
        try:
            engine.dispatch_step(tid, "x" + key.split("-")[1], "u1")
        except Exception as exc:
            with lock:
                rejected.append(exc.reason.value)

    others = [k for k, _ in specs[1:]]
    threads = [threading.Thread(target=dispatch_other, args=(k,))
               for k in others]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    record_case("concurrent_dispatch", attempts=4, free_capacity=0)
    assert sorted(rejected) == ["CAPACITY_FULL"] * 4
    assert engine.unit_view("u1")["capacity_used"] == 1
    log_decisions(engine, "concurrent_dispatch")


def test_cancel_while_running_releases_capacity(engine, clk):
    engine.register_unit("u1", frozenset({"nav"}), 1)
    spec = TaskSpec(steps=(step("a", run_ms=100, timeout_ms=1000),))
    task, _ = engine.submit("cancel-run", spec)
    engine.tick()
    assert park_or_finish(engine, clk, 2)
    assert engine.unit_view("u1")["capacity_used"] == 1
    record_case("cancel_while_running", unit_capacity=1)
    engine.cancel_task(task.task_id)
    time.sleep(0.05)
    clk.advance(50)
    engine.wait_idle(2)
    engine.tick()
    view = engine.task_view(task.task_id)
    assert view["state"] == "CANCELLED"
    assert view["steps"]["a"]["state"] == "CANCELLED"
    assert engine.unit_view("u1")["capacity_used"] == 0
    assert engine.unit_view("u1")["running_steps"] == []
    log_decisions(engine, "cancel_while_running")


def test_cancel_before_start(engine, clk):
    engine.register_unit("u1", frozenset({"nav"}), 1)
    spec = TaskSpec(steps=(
        step("a", run_ms=10),
        step("b", run_ms=10, depends_on=("a",)),
    ))
    task, _ = engine.submit("cancel-early", spec)
    record_case("cancel_before_start")
    engine.cancel_task(task.task_id)  # before any tick
    engine.tick()
    view = engine.task_view(task.task_id)
    assert view["state"] == "CANCELLED"
    assert all(s["state"] == "CANCELLED" for s in view["steps"].values())
    assert engine.unit_view("u1")["capacity_used"] == 0
    log_decisions(engine, "cancel_before_start")


def test_preempt_releases_and_reruns_successfully(engine, clk):
    engine.register_unit("u1", frozenset({"nav"}), 1)
    spec = TaskSpec(steps=(step("a", run_ms=100, timeout_ms=1000),))
    task, _ = engine.submit("preempt-run", spec)
    engine.tick()
    assert park_or_finish(engine, clk, 2)
    record_case("preempt_while_running")
    engine.preempt_task(task.task_id)
    time.sleep(0.05)
    clk.advance(50)
    engine.wait_idle(2)
    # The preempted attempt released its capacity before any re-dispatch.
    assert engine.unit_view("u1")["capacity_used"] == 0
    engine.tick()  # scheduler may now re-dispatch the READY step
    # scheduler re-dispatches; drive to success
    engine.drive_until_idle(2000)
    view = engine.task_view(task.task_id)
    outcomes = [a["outcome"] for a in view["steps"]["a"]["attempts"]]
    assert view["state"] == "SUCCEEDED"
    assert outcomes[0] == "PREEMPTED"
    assert outcomes[-1] == "SUCCEEDED"
    log_decisions(engine, "preempt_while_running")


def test_dispatch_order_fifo_same_priority(engine, clk):
    engine.register_unit("u1", frozenset({"nav"}), 1)
    for i in range(3):
        engine.submit(
            f"ord-{i}",
            TaskSpec(steps=(step(f"k{i}", run_ms=10, timeout_ms=10000),)))
    engine.drive_until_idle(20000)
    events = log_decisions(engine, "dispatch_order")
    accepted = [e["detail"]["task_id"]
                for e in events if e["type"] == "DISPATCH_ACCEPTED"]
    # task ids assigned in submission order; dispatch must match.
    ordered = sorted(engine.store.tasks,
                     key=lambda tid: engine.store.tasks[tid].created_at_ms)
    assert accepted == ordered
