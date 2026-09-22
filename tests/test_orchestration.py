"""Core orchestration engine tests.

Deterministic VirtualClock is used; scenarios print input events and the
engine's decisions (state transitions, dispatch accept/reject, capacity
release, retry scheduling) via conftest.log_decisions.
"""
from __future__ import annotations

import time

import pytest

from fleet.engine import DispatchRejected
from fleet.models import (
    RejectReason,
    StepSpec,
    TaskSpec,
)

from conftest import log_decisions, park_or_finish, record_case


def step(key, cap="nav", **kw):
    return StepSpec(key=key, capability=cap, **kw)


# --------------------------------------------------------------------- #
# 1. dependency ordering + capability/capacity dispatch                  #
# --------------------------------------------------------------------- #
def test_dependency_ordering(engine, clk):
    engine.register_unit("u1", frozenset({"nav", "arm"}), 2)
    spec = TaskSpec(steps=(
        step("a", run_ms=10),
        step("b", "arm", run_ms=10, depends_on=("a",)),
        step("c", "arm", run_ms=10, depends_on=("a",)),
    ))
    record_case("dependency_ordering",
                steps=["a", "b", "c"], deps={"b": ["a"], "c": ["a"]})
    task, _ = engine.submit("dep", spec)
    engine.drive_until_idle(2000)

    events = log_decisions(engine, "dependency_ordering")
    view = engine.task_view(task.task_id)
    assert view["state"] == "SUCCEEDED"
    assert [s["state"] for s in view["steps"].values()] == \
        ["SUCCEEDED", "SUCCEEDED", "SUCCEEDED"]
    # b and c must only be dispatched after a STEP_SUCCEEDED for a.
    ready = [e for e in events if e["type"] == "STEP_READY"]
    assert {e["detail"]["step_key"] for e in ready} == {"a", "b", "c"}
    assert engine.unit_view("u1")["capacity_used"] == 0


def test_explicit_dispatch_with_unmet_dependency_rejected(engine, clk):
    engine.register_unit("u1", frozenset({"arm"}), 1)
    spec = TaskSpec(steps=(step("a", "nav"), step("b", "arm",
                                                  depends_on=("a",))))
    task, _ = engine.submit("dep-x", spec)
    record_case("unmet_dependency_dispatch",
                target="b", depends_on=["a"], a_state="PENDING")
    with pytest.raises(DispatchRejected) as ei:
        engine.dispatch_step(task.task_id, "b", "u1")
    assert ei.value.reason is RejectReason.DEPENDENCIES_UNMET
    log_decisions(engine, "unmet_dependency_dispatch")
    assert engine.unit_view("u1")["capacity_used"] == 0


def test_capability_mismatch_rejected(engine, clk):
    engine.register_unit("u-arm", frozenset({"arm"}), 1)
    spec = TaskSpec(steps=(step("a", "nav"),))
    task, _ = engine.submit("cap-x", spec)
    record_case("capability_mismatch",
                step_capability="nav", unit_capabilities=["arm"])
    with pytest.raises(DispatchRejected) as ei:
        engine.dispatch_step(task.task_id, "a", "u-arm")
    assert ei.value.reason is RejectReason.CAPABILITY_MISMATCH
    rejects = engine.recent_rejects()
    assert rejects[-1]["reason"] == "CAPABILITY_MISMATCH"
    log_decisions(engine, "capability_mismatch")


def test_unit_not_found_and_offline_rejected(engine, clk):
    spec = TaskSpec(steps=(step("a", "nav"),))
    task, _ = engine.submit("off-x", spec)
    with pytest.raises(DispatchRejected) as ei:
        engine.dispatch_step(task.task_id, "a", "ghost")
    assert ei.value.reason is RejectReason.UNIT_NOT_FOUND
    engine.register_unit("u1", frozenset({"nav"}), 1)
    engine.set_unit_online("u1", False)
    record_case("unit_offline_dispatch", unit="u1", online=False)
    with pytest.raises(DispatchRejected) as ei:
        engine.dispatch_step(task.task_id, "a", "u1")
    assert ei.value.reason is RejectReason.UNIT_OFFLINE
    log_decisions(engine, "unit_offline_dispatch")


def test_capacity_full_rejected(engine, clk):
    engine.register_unit("u1", frozenset({"nav"}), 1)
    # Occupy the only slot.
    occupy = TaskSpec(steps=(step("hold", run_ms=100, timeout_ms=1000),))
    engine.submit("hold", occupy)
    engine.tick()  # dispatch hold -> RUNNING, capacity 1/1
    spec = TaskSpec(steps=(step("a", "nav"),))
    task, _ = engine.submit("capfull", spec)
    record_case("capacity_full", capacity_total=1, capacity_used=1, need=1)
    with pytest.raises(DispatchRejected) as ei:
        engine.dispatch_step(task.task_id, "a", "u1")
    assert ei.value.reason is RejectReason.CAPACITY_FULL
    # scheduler path records the same distinguishable reason
    engine.tick()
    assert engine.task_view(task.task_id)["steps"]["a"][
        "last_reject_reason"] == "CAPACITY_FULL"
    log_decisions(engine, "capacity_full")
    assert engine.unit_view("u1")["capacity_used"] == 1


# --------------------------------------------------------------------- #
# 2. idempotency                                                          #
# --------------------------------------------------------------------- #
def test_idempotent_submit_no_duplicates(engine, clk):
    engine.register_unit("u1", frozenset({"nav"}), 1)
    spec = TaskSpec(steps=(step("a", run_ms=10),))
    t1, c1 = engine.submit("same-key", spec)
    t2, c2 = engine.submit("same-key", spec)
    t3, c3 = engine.submit("same-key", spec)
    record_case("idempotent_submit", submissions=3, idempotency_key="same-key")
    assert c1 is True and c2 is False and c3 is False
    assert t1.task_id == t2.task_id == t3.task_id
    engine.drive_until_idle(1000)
    view = engine.task_view(t1.task_id)
    assert len(view["steps"]) == 1
    assert len(view["steps"]["a"]["attempts"]) == 1
    assert len(engine.list_tasks()) == 1
    events = log_decisions(engine, "idempotent_submit")
    assert sum(1 for e in events if e["type"] == "TASK_SUBMITTED") == 1
    assert sum(1 for e in events if e["type"] == "TASK_DUPLICATE") == 2


# --------------------------------------------------------------------- #
# 3. retry chain: transient failure, exactly-once effect, exhaustion     #
# --------------------------------------------------------------------- #
def test_flaky_retries_then_succeeds_effect_once(engine, clk):
    engine.register_unit("u1", frozenset({"nav"}), 1)
    spec = TaskSpec(steps=(step(
        "a", behavior="flaky", flaky_fail_times=2, run_ms=10,
        timeout_ms=1000, max_retries=3, fail_message="transient"),))
    task, _ = engine.submit("flaky", spec)
    record_case("flaky_retry", flaky_fail_times=2, max_retries=3)
    engine.drive_until_idle(2000)
    view = engine.task_view(task.task_id)
    st = view["steps"]["a"]
    assert st["state"] == "SUCCEEDED"
    outcomes = [a["outcome"] for a in st["attempts"]]
    assert outcomes == ["FAILED", "FAILED", "SUCCEEDED"]
    assert st["retry_count"] == 2
    # exactly-once side effect
    effect_events = [e for e in engine.events_since(0)
                     if e["type"] == "EFFECT_RECORDED"]
    assert len(effect_events) == 1
    assert effect_events[0]["detail"]["dedup"] is False
    log_decisions(engine, "flaky_retry")


def test_retries_exhausted_enters_terminal(engine, clk):
    engine.register_unit("u1", frozenset({"nav"}), 1)
    spec = TaskSpec(steps=(step(
        "a", behavior="fail", run_ms=10, timeout_ms=1000, max_retries=1,
        fail_message="permanent-ish"),))
    task, _ = engine.submit("exhaust", spec)
    record_case("retries_exhausted", max_retries=1)
    engine.drive_until_idle(1000)
    view = engine.task_view(task.task_id)
    assert view["steps"]["a"]["state"] == "FAILED"
    assert view["steps"]["a"]["failure_reason"] == "RETRIES_EXHAUSTED"
    assert view["state"] == "FAILED"
    assert len(view["steps"]["a"]["attempts"]) == 2
    assert engine.unit_view("u1")["capacity_used"] == 0
    log_decisions(engine, "retries_exhausted")


def test_non_retryable_failure_immediate_terminal(engine, clk):
    engine.register_unit("u1", frozenset({"nav", "arm"}), 2)
    spec = TaskSpec(steps=(
        step("a", behavior="fail_nonretryable", run_ms=10, max_retries=5),
        step("b", "arm", run_ms=10, depends_on=("a",)),
    ))
    task, _ = engine.submit("nonretry", spec)
    record_case("non_retryable", max_retries=5)
    engine.drive_until_idle(1000)
    view = engine.task_view(task.task_id)
    assert view["steps"]["a"]["state"] == "FAILED"
    assert view["steps"]["a"]["failure_reason"] == "NON_RETRYABLE_ERROR"
    assert len(view["steps"]["a"]["attempts"]) == 1  # no retries used
    assert view["steps"]["b"]["state"] == "CANCELLED"
    assert view["state"] == "FAILED"
    log_decisions(engine, "non_retryable")


def test_timeout_retries_exhausted(engine, clk):
    engine.register_unit("u1", frozenset({"nav"}), 1)
    spec = TaskSpec(steps=(step(
        "a", behavior="timeout", run_ms=1, timeout_ms=50, max_retries=1),))
    task, _ = engine.submit("timeout-job", spec)
    record_case("timeout", timeout_ms=50, max_retries=1)
    engine.drive_until_idle(2000)
    view = engine.task_view(task.task_id)
    assert view["steps"]["a"]["state"] == "TIMED_OUT"
    assert view["steps"]["a"]["failure_reason"] == \
        "TIMEOUT_RETRIES_EXHAUSTED"
    outcomes = [a["outcome"] for a in view["steps"]["a"]["attempts"]]
    assert outcomes == ["TIMED_OUT", "TIMED_OUT"]
    assert engine.unit_view("u1")["capacity_used"] == 0
    log_decisions(engine, "timeout")


def test_offline_interrupt_does_not_consume_retry_budget(engine, clk):
    engine.register_unit("u1", frozenset({"nav"}), 1)
    spec = TaskSpec(steps=(step("a", run_ms=100, timeout_ms=1000,
                                max_retries=0),))
    task, _ = engine.submit("offline-job", spec)
    engine.tick()  # dispatch
    parked = park_or_finish(engine, clk, 2)
    assert parked
    engine.set_unit_online("u1", False)
    time.sleep(0.05)
    clk.advance(50)
    engine.wait_idle(2)
    engine.tick()
    view = engine.task_view(task.task_id)
    assert view["steps"]["a"]["state"] == "READY"
    assert view["steps"]["a"]["attempts"][0]["outcome"] == "INTERRUPTED"
    assert view["steps"]["a"]["retry_count"] == 0
    assert engine.unit_view("u1")["capacity_used"] == 0
    record_case("offline_interrupt", max_retries=0, retry_count=0)
    log_decisions(engine, "offline_interrupt")
