"""优先级调度与抢占测试。

覆盖：
- 高优先级抢占低优先级在飞步骤：让位步骤回 PENDING、原因 PREEMPTED 可区分、
  不算失败、不消耗重试次数，容量腾出后自动接着跑；
- 同优先级严格按提交先后 FIFO；
- 不同优先级时高优先级先于更早提交的低优先级；
- 被让位步骤重跑后副作用只计一次（迟到僵尸结果被丢弃）；
- 任务进入终态后在飞步骤被级联取消，迟到结果不改变对外视图；
- 混合优先级并发提交下容量不被突破、无悬空占用；
- 旧提交方式（不带 priority）行为兼容。
"""

from __future__ import annotations

import threading
import time

from fleet.models import (
    FailureCode,
    Priority,
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


def _dispatch_spy(service):
    """记录 (task_id, step_id, attempt) 的实际派发顺序。"""
    dispatched: list[tuple[str, str, int]] = []
    orig = service.engine._run_attempt

    def spy(task_id, step_id, unit_id, attempt, *a, **k):
        dispatched.append((task_id, step_id, attempt))
        return orig(task_id, step_id, unit_id, attempt, *a, **k)

    service.engine._run_attempt = spy
    return dispatched


def test_high_priority_preempts_low_priority_inflight(service):
    print("\nINPUT: low-priority task occupies the only capacity (delay 0.4);")
    print("       then a high-priority task is submitted on the same capability")
    make_unit(service, "u1", ["arm"], 1)
    dispatched = _dispatch_spy(service)

    service.submit(
        TaskSpec("low", (StepSpec("s", "arm", timeout=5.0),), priority=Priority.LOW),
        {"s": behavior(delay=0.4)},
    )
    time.sleep(0.05)
    assert service.get_task("low").steps["s"].state == StepState.DISPATCHED

    service.submit(
        TaskSpec("high", (StepSpec("s", "arm", timeout=5.0),), priority=Priority.HIGH),
        {"s": behavior(delay=0.3)},
    )
    # 抢占在 submit 的调度临界区内同步发生：低优先级步骤立即让位
    low_view = service.get_task("low")
    dump_task("low preempted", low_view)
    s = low_view.steps["s"]
    print(f"JUDGEMENT: low step state={s.state.value} reject={s.last_reject} "
          f"preemptions={s.preemptions} retries_used={s.retries_used}")
    assert s.state == StepState.PENDING               # 让位后重新排队，而非终态
    assert s.last_reject == RejectReason.PREEMPTED    # 明确、可区分的原因
    assert s.preemptions == 1
    assert s.failure_code is None                     # 不算执行失败
    assert s.retries_used == 0                        # 不消耗重试次数
    assert service.get_task("high").steps["s"].state == StepState.DISPATCHED

    low_final = wait_terminal(service, "low")
    high_final = service.get_task("high")
    dump_task("low final", low_final)
    dump_task("high final", high_final)
    print(f"JUDGEMENT: dispatch order = {dispatched}")
    # 高优先级先跑完；低优先级让位后自动接着跑并成功
    assert high_final.state == TaskState.SUCCEEDED
    assert low_final.state == TaskState.SUCCEEDED
    assert low_final.steps["s"].attempts == 2         # 让位后重新派发过一次
    assert [d[0] for d in dispatched] == ["low", "high", "low"]
    assert_no_dangling_capacity(service)


def test_same_priority_is_fifo_by_submit_order(service):
    print("\nINPUT: blocker holds capacity; submit m1 then m2 (both medium)")
    make_unit(service, "u1", ["arm"], 1)
    dispatched = _dispatch_spy(service)

    service.submit(
        TaskSpec("blk", (StepSpec("s", "arm", timeout=5.0),)),
        {"s": behavior(delay=0.15)},
    )
    time.sleep(0.03)
    service.submit(TaskSpec("m1", (StepSpec("s", "arm", timeout=5.0),)),
                   {"s": behavior(delay=0.02)})
    service.submit(TaskSpec("m2", (StepSpec("s", "arm", timeout=5.0),)),
                   {"s": behavior(delay=0.02)})

    assert service.wait_idle(5)
    order = [d[0] for d in dispatched]
    print(f"JUDGEMENT: dispatch order = {order}")
    assert order == ["blk", "m1", "m2"]  # 同优先级：先提交先服务
    assert_no_dangling_capacity(service)


def test_higher_priority_beats_earlier_submit(service):
    print("\nINPUT: blocker holds capacity; submit low-t then high-t;")
    print("       high-t must dispatch first once capacity frees")
    make_unit(service, "u1", ["arm"], 1)
    dispatched = _dispatch_spy(service)

    service.submit(
        TaskSpec("blk", (StepSpec("s", "arm", timeout=5.0),),
                 priority=Priority.HIGH),  # 同级不抢占，blk 不会被 high-t 让位
        {"s": behavior(delay=0.15)},
    )
    time.sleep(0.03)
    service.submit(
        TaskSpec("low-t", (StepSpec("s", "arm", timeout=5.0),), priority=Priority.LOW),
        {"s": behavior(delay=0.02)},
    )
    service.submit(
        TaskSpec("high-t", (StepSpec("s", "arm", timeout=5.0),), priority=Priority.HIGH),
        {"s": behavior(delay=0.02)},
    )

    assert service.wait_idle(5)
    order = [d[0] for d in dispatched]
    print(f"JUDGEMENT: dispatch order = {order}")
    assert order == ["blk", "high-t", "low-t"]  # 优先级高于提交先后
    assert_no_dangling_capacity(service)


def test_preempted_step_side_effects_counted_once_and_retry_budget_intact(service):
    print("\nINPUT: low step outcomes=(succeeded, timeout, succeeded), delay 0.4,")
    print("       side_effects=3, max_retries=2; preempted during attempt 0")
    make_unit(service, "u1", ["arm"], 1)

    service.submit(
        TaskSpec(
            "low",
            (StepSpec("s", "arm", timeout=5.0, max_retries=2),),
            priority=Priority.LOW,
        ),
        {"s": behavior(("succeeded", "timeout", "succeeded"),
                       side_effects=3, delay=0.4)},
    )
    time.sleep(0.05)
    service.submit(
        TaskSpec("high", (StepSpec("s", "arm", timeout=5.0),), priority=Priority.HIGH),
        {"s": behavior(delay=0.05)},
    )
    view = wait_terminal(service, "low", timeout=8)
    dump_task("preempted then retried", view)
    s = view.steps["s"]
    print(f"JUDGEMENT: attempts={s.attempts} preemptions={s.preemptions} "
          f"retries_used={s.retries_used} side_effects={s.side_effects}")
    assert s.state == StepState.SUCCEEDED
    assert s.attempts == 3          # 让位 1 次 + 超时 1 次 + 最终成功
    assert s.preemptions == 1
    assert s.retries_used == 1      # 只有超时消耗重试；抢占不消耗
    assert s.side_effects == 3      # 僵尸成功被丢弃，副作用只计一次
    assert service.get_task("high").state == TaskState.SUCCEEDED
    assert_no_dangling_capacity(service)


def test_terminal_task_cancels_inflight_and_late_result_is_ignored(service):
    print("\nINPUT: capacity 2; step 'bad' fails fast (error), step 'slow'")
    print("       in-flight with delay 0.5 and would succeed late")
    make_unit(service, "u1", ["arm"], 2)

    service.submit(
        TaskSpec("term", (
            StepSpec("bad", "arm", timeout=5.0),
            StepSpec("slow", "arm", timeout=5.0),
        )),
        {
            "bad": behavior(("error",), fail_detail="arm fault", delay=0.05),
            "slow": behavior(("succeeded",), side_effects=5, delay=0.5),
        },
    )
    # 等 bad 失败落定、任务进入终态
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if service.get_task("term").state == TaskState.FAILED:
            break
        time.sleep(0.005)
    mid = service.get_task("term")
    dump_task("task terminal while slow in-flight", mid)
    print(f"JUDGEMENT: task={mid.state.value} slow={mid.steps['slow'].state.value}")
    assert mid.state == TaskState.FAILED
    assert mid.steps["bad"].state == StepState.FAILED
    assert mid.steps["bad"].failure_code == FailureCode.NON_RECOVERABLE
    # 终态时刻起不再有任何在飞步骤
    assert mid.steps["slow"].state == StepState.CANCELLED
    assert service.registry.get("u1").used_capacity() == 0

    time.sleep(0.6)  # 等 slow 的迟到成功落地
    late = service.get_task("term")
    dump_task("after late zombie result", late)
    print(f"JUDGEMENT: task={late.state.value} slow={late.steps['slow'].state.value} "
          f"effects={late.steps['slow'].side_effects}")
    # 迟到结果不改变任何对外视图
    assert late.state == TaskState.FAILED
    assert late.steps["slow"].state == StepState.CANCELLED
    assert late.steps["slow"].side_effects == 0
    assert_no_dangling_capacity(service)


def test_concurrent_mixed_priority_never_exceeds_capacity(default_service):
    print("\nINPUT: 18 threads, mixed priorities, cost=1 vs total arm capacity 3")
    violations = []
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            total = 0
            for u in default_service.registry.all_units():
                if "arm" in u.capabilities:
                    total += u.used_capacity()
                if u.used_capacity() > u.capacity:
                    violations.append((u.unit_id, u.used_capacity()))
            if total > 3:
                violations.append(total)

    t = threading.Thread(target=sampler)
    t.start()

    barrier = threading.Barrier(18)
    prio_cycle = [Priority.HIGH, Priority.MEDIUM, Priority.LOW]

    def worker(i):
        spec = TaskSpec(
            f"p-job-{i:02d}",
            (StepSpec("a", "arm", timeout=5.0),),
            priority=prio_cycle[i % 3],
        )
        barrier.wait()
        default_service.submit(spec, {"a": behavior(delay=0.05)})

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(18)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert default_service.wait_idle(10)
    stop.set()
    t.join()
    print(f"JUDGEMENT: capacity violations sampled = {len(violations)}")
    assert not violations, violations[:5]
    for i in range(18):
        view = default_service.get_task(f"p-job-{i:02d}")
        assert view.state == TaskState.SUCCEEDED, (i, view.state)
    assert_no_dangling_capacity(default_service)


def test_default_priority_matches_legacy_behavior(service):
    print("\nINPUT: submit without priority -> treated as medium")
    make_unit(service, "u1", ["arm"], 1)
    view, created = service.submit(
        TaskSpec("legacy", (StepSpec("s", "arm", timeout=5.0),)),
        {"s": behavior(delay=0.02)},
    )
    assert created is True
    print(f"JUDGEMENT: priority={view.priority.value}")
    assert view.priority == Priority.MEDIUM
    final = wait_terminal(service, "legacy")
    assert final.state == TaskState.SUCCEEDED
    assert final.steps["s"].preemptions == 0
    assert_no_dangling_capacity(service)


def test_api_priority_field_is_optional_and_additive():
    """HTTP 层兼容：旧请求体（无 priority）照常工作；响应只增不改。"""
    import warnings

    warnings.filterwarnings("ignore")
    from fastapi.testclient import TestClient

    from fleet.api import create_app
    from fleet.service import FleetService

    svc = FleetService()
    app = create_app(svc)
    with TestClient(app) as c:
        c.post("/units", json={"unit_id": "u1", "capabilities": ["arm"],
                               "capacity": 1})
        # 旧式请求体：完全没有 priority 字段
        legacy = {
            "task_id": "api-legacy",
            "steps": [{"id": "s", "required_capability": "arm", "timeout": 5.0}],
        }
        r = c.post("/tasks", json=legacy)
        print(f"\nJUDGEMENT: legacy submit -> {r.status_code}")
        assert r.status_code == 200
        body = r.json()
        assert body["created"] is True
        assert body["task"]["priority"] == "medium"  # 缺省即普通优先级
        # 旧响应字段一个不少
        step = body["task"]["steps"][0]
        for key in ("id", "state", "required_capability", "depends_on",
                    "capacity_cost", "attempts", "max_retries", "last_reject",
                    "failure_code", "failure_detail", "assigned_unit",
                    "side_effects"):
            assert key in step, key

        # 显式携带优先级
        r2 = c.post("/tasks", json={
            "task_id": "api-high",
            "priority": "high",
            "steps": [{"id": "s", "required_capability": "arm", "timeout": 5.0}],
        })
        assert r2.status_code == 200
        assert r2.json()["task"]["priority"] == "high"

        # 非法优先级值 -> 422，且不留任务
        r3 = c.post("/tasks", json={
            "task_id": "api-bad",
            "priority": "urgent",
            "steps": [{"id": "s", "required_capability": "arm"}],
        })
        print(f"JUDGEMENT: invalid priority -> {r3.status_code}")
        assert r3.status_code == 422
        assert c.get("/tasks/api-bad").status_code == 404

        assert svc.wait_idle(5)
    svc.shutdown()
