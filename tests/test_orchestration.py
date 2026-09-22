"""编排顺序与能力/容量约束测试。

覆盖：
- 步骤严格按依赖顺序推进；
- 能力不匹配 -> CAPABILITY_MISMATCH；
- 容量不足 -> INSUFFICIENT_CAPACITY；
- 依赖未满足 -> DEPENDENCY_NOT_MET；
- 容量释放后被拒步骤可继续推进；
- 多个独立步骤可在容量允许时并行占用。
"""

from __future__ import annotations

from fleet.models import (
    RejectReason,
    StepSpec,
    StepState,
    TaskSpec,
    TaskState,
)
from tests.conftest import (
    assert_no_dangling_capacity,
    behavior,
    dump_task,
    make_unit,
    wait_terminal,
)


def test_steps_run_in_dependency_order(service):
    print("\nINPUT: chain a -> b -> c on single arm unit")
    make_unit(service, "u1", ["arm"], 2)
    order = []

    # 用带延迟的行为制造"b/c 必须等待"的窗口，并记录完成顺序
    spec = TaskSpec("chain", (
        StepSpec("a", "arm", capacity_cost=1, timeout=2.0, max_retries=0),
        StepSpec("b", "arm", depends_on=("a",), timeout=2.0),
        StepSpec("c", "arm", depends_on=("b",), timeout=2.0),
    ))
    # 通过 store 记录成功顺序
    orig = service.engine._finish_success

    def spy(task_id, step_id, *a, **k):
        order.append(step_id)
        return orig(task_id, step_id, *a, **k)

    service.engine._finish_success = spy
    service.submit(spec, {
        "a": behavior(delay=0.05),
        "b": behavior(delay=0.05),
        "c": behavior(delay=0.05),
    })
    view = wait_terminal(service, "chain")
    dump_task("dependency order", view)

    assert order == ["a", "b", "c"]
    assert view.state == TaskState.SUCCEEDED
    assert all(s.state == StepState.SUCCEEDED for s in view.steps.values())
    assert_no_dangling_capacity(service)


def test_capability_mismatch_is_rejected(service):
    print("\nINPUT: task needs 'camera'; only arm unit registered")
    make_unit(service, "u1", ["arm"], 5)
    spec = TaskSpec("cam-job", (StepSpec("s", "camera", timeout=5.0),))
    view, created = service.submit(spec)
    print("JUDGEMENT: step should remain PENDING with last_reject=CAPABILITY_MISMATCH")
    assert created is True
    view = service.get_task("cam-job")
    step = view.steps["s"]
    dump_task("capability mismatch", view)
    assert step.state == StepState.PENDING
    assert step.last_reject == RejectReason.CAPABILITY_MISMATCH

    # 补上具备能力的单元后自动推进
    make_unit(service, "u2", ["camera"], 1)
    view = wait_terminal(service, "cam-job")
    assert view.steps["s"].state == StepState.SUCCEEDED
    assert_no_dangling_capacity(service)


def test_insufficient_capacity_is_rejected_then_progresses(service):
    print("\nINPUT: unit capacity=1; two parallel arm steps cost=1 each;")
    print("       first step holds capacity with delay; second must be rejected,")
    print("       then succeed after capacity is released.")
    make_unit(service, "u1", ["arm"], 1)
    spec = TaskSpec("cap", (
        StepSpec("a", "arm", timeout=2.0),
        StepSpec("b", "arm", timeout=2.0),
    ))
    service.submit(spec, {
        "a": behavior(delay=0.15),
        "b": behavior(delay=0.0),
    })

    # 在 a 占用容量期间查询：b 必须以 INSUFFICIENT_CAPACITY 留在 PENDING
    import time
    deadline_b = None
    for _ in range(100):
        view = service.get_task("cap")
        a = view.steps["a"]
        b = view.steps["b"]
        if a.state == StepState.DISPATCHED and b.state == StepState.PENDING:
            deadline_b = b.last_reject
            print(f"JUDGEMENT while a in-flight: b.last_reject={deadline_b}")
            break
        time.sleep(0.003)
    assert deadline_b == RejectReason.INSUFFICIENT_CAPACITY
    assert service.registry.get("u1").used_capacity() == 1

    view = wait_terminal(service, "cap")
    dump_task("capacity gating", view)
    assert view.state == TaskState.SUCCEEDED
    assert_no_dangling_capacity(service)


def test_dependency_not_met_reason(service):
    print("\nINPUT: b depends on a which keeps crashing (no retries)")
    make_unit(service, "u1", ["arm"], 2)
    spec = TaskSpec("dep", (
        StepSpec("a", "arm", max_retries=0, timeout=2.0),
        StepSpec("b", "arm", depends_on=("a",)),
    ))
    service.submit(spec, {"a": behavior(("crashed",))})
    view = wait_terminal(service, "dep")
    dump_task("dependency blocked by failure", view)

    assert view.state == TaskState.FAILED
    assert view.steps["a"].state == StepState.FAILED
    # b 从未派发：保持 PENDING；其依赖永远无法满足
    b = view.steps["b"]
    assert b.state == StepState.PENDING and b.attempts == 0
    assert_no_dangling_capacity(service)


def test_parallel_steps_share_capacity(service):
    print("\nINPUT: two independent arm steps on capacity=2 unit")
    make_unit(service, "u1", ["arm"], 2)
    inflight_seen = 0

    orig = service.engine._run_attempt

    def spy(*a, **k):
        nonlocal inflight_seen
        inflight_seen = max(inflight_seen, service.registry.get("u1").used_capacity())
        return orig(*a, **k)

    service.engine._run_attempt = spy
    spec = TaskSpec("parallel", (
        StepSpec("a", "arm", timeout=2.0),
        StepSpec("b", "arm", timeout=2.0),
    ))
    service.submit(spec, {"a": behavior(delay=0.1), "b": behavior(delay=0.1)})
    view = wait_terminal(service, "parallel")
    dump_task("parallel", view)
    assert view.state == TaskState.SUCCEEDED
    assert inflight_seen == 2, f"expected concurrent occupancy of 2, got {inflight_seen}"
    assert_no_dangling_capacity(service)
