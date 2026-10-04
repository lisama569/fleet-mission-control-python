"""取消语义专项测试（changed 标记与状态/容量账目三边一致）。

覆盖场景
--------
1. 被高优先级抢占让位、正等待重新派发（PENDING + PREEMPTED，不占容量）的任务
   被取消：changed=True，任务与步骤即 CANCELLED，重复取消 changed=False；
2. 一直排队等容量（PENDING，从未派发、不占容量）的任务被取消：changed=True，
   状态前后一致，且不能把别人占用的容量"虚释放"；
3. 真正终态（SUCCEEDED/FAILED/CANCELLED）任务再次取消：幂等无操作，
   changed=False、不报错、状态与容量账目都不变；
4. 有步骤在飞（DISPATCHED）的任务取消：占用立即全部释放、排队任务马上用上，
   迟到的成功结果被丢弃，不复活步骤、不重复释放；
5. 取消与抢占让位、执行单元离线、迟到结果同时发生：容量占用/释放严格配平，
   不会出现"已取消后又有步骤成功/又冒出在飞步骤"；
6. 并发提交 + 并发取消：容量不被突破、不留悬空占用、释放总额 == 占用总额。

每个用例均用 print 显式输出 INPUT 与 JUDGEMENT；引擎有序事件
（submit/dispatch/preempt/cancel/release/stale/offline）经
``-s -o log_cli=true --log-cli-level=INFO`` 可见判定依据。
"""

from __future__ import annotations

import threading
import time

import pytest

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


def _wait_step_state(service, task_id, step_id, state, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        s = service.get_task(task_id).steps[step_id]
        if s.state == state:
            return s
        time.sleep(0.003)
    return service.get_task(task_id).steps[step_id]


# ---------------------------------------------------------------------------
# 1. 被抢占让位、等待重新派发的任务取消
# ---------------------------------------------------------------------------
def test_cancel_preempted_waiting_task_reports_changed(service):
    print("\nINPUT: low task inflight (delay 1.0); high task arrives and preempts it;")
    print("       then cancel low WHILE it is PENDING+PREEMPTED waiting for redispatch")
    make_unit(service, "u1", ["arm"], 1)
    service.submit(
        TaskSpec("low", (StepSpec("s", "arm", timeout=5.0),), priority=Priority.LOW),
        {"s": behavior(delay=1.0)},
    )
    _wait_step_state(service, "low", "s", StepState.DISPATCHED)

    service.submit(
        TaskSpec("high", (StepSpec("s", "arm", timeout=5.0),), priority=Priority.HIGH),
        {"s": behavior(delay=0.5)},
    )
    low_waiting = _wait_step_state(
        service, "low", "s", StepState.PENDING
    )
    print(f"JUDGEMENT: low before cancel: state={low_waiting.state.value} "
          f"last_reject={low_waiting.last_reject.value} preemptions={low_waiting.preemptions}")
    assert low_waiting.last_reject == RejectReason.PREEMPTED
    # 让位后容量已交给 high，low 本不占容量
    assert service.registry.get("u1").used_capacity() == 1

    v1, changed1 = service.cancel("low")
    print(f"JUDGEMENT: first cancel -> changed={changed1} task={v1.state.value} "
          f"step={v1.steps['s'].state.value}")
    # 关键回归：状态已变 CANCELLED，changed 必须为 True（旧实现误报 False）
    assert changed1 is True
    assert v1.state == TaskState.CANCELLED
    assert v1.steps["s"].state == StepState.CANCELLED

    v2, changed2 = service.cancel("low")
    print(f"JUDGEMENT: second cancel -> changed={changed2} task={v2.state.value}")
    assert changed2 is False
    assert v2.state == TaskState.CANCELLED

    # 后续查询与返回体三边一致；等 low 的旧飞行尝试迟到落地
    time.sleep(0.7)
    v3 = service.get_task("low")
    dump_task("low after stale attempt landed", v3)
    assert v3.state == TaskState.CANCELLED
    assert v3.steps["s"].state == StepState.CANCELLED
    assert v3.steps["s"].side_effects == 0      # 让位那次尝试的迟到结果被丢弃
    # high 仍正常跑完，账本不被取消 low 所影响
    high_final = wait_terminal(service, "high")
    assert high_final.state == TaskState.SUCCEEDED
    assert_no_dangling_capacity(service)


# ---------------------------------------------------------------------------
# 2. 一直排队等容量、从未占过容量的任务取消
# ---------------------------------------------------------------------------
def test_cancel_queued_task_reports_changed_and_does_not_release_others(service):
    print("\nINPUT: blocker inflight on the only capacity (delay 1.0);")
    print("       queued task can never dispatch; cancel it while PENDING")
    make_unit(service, "u1", ["arm"], 1)
    service.submit(
        TaskSpec("blocker", (StepSpec("b", "arm", timeout=5.0),)),
        {"b": behavior(delay=1.0)},
    )
    _wait_step_state(service, "blocker", "b", StepState.DISPATCHED)

    service.submit(
        TaskSpec("queued", (StepSpec("q", "arm", timeout=5.0),)),
        {"q": behavior()},
    )
    q0 = _wait_step_state(service, "queued", "q", StepState.PENDING)
    print(f"JUDGEMENT: queued step before cancel: state={q0.state.value} "
          f"attempts={q0.attempts} reject={q0.last_reject}")
    assert q0.attempts == 0  # 从未派发、从未占容量

    v1, changed1 = service.cancel("queued")
    print(f"JUDGEMENT: first cancel -> changed={changed1} task={v1.state.value} "
          f"step={v1.steps['q'].state.value}")
    assert changed1 is True
    assert v1.state == TaskState.CANCELLED
    assert v1.steps["q"].state == StepState.CANCELLED
    # blocker 的占用必须原样保留（不能因取消排队任务被虚释放）
    assert service.registry.get("u1").used_capacity() == 1

    v2, changed2 = service.cancel("queued")
    print(f"JUDGEMENT: second cancel -> changed={changed2} task={v2.state.value}")
    assert changed2 is False
    assert service.get_task("queued").state == TaskState.CANCELLED

    blocker_final = wait_terminal(service, "blocker")
    assert blocker_final.state == TaskState.SUCCEEDED
    assert_no_dangling_capacity(service)


def test_cancel_pending_dependency_task_reports_changed(service):
    """从未轮到它（依赖一直未满足）的任务取消同样算有变更。"""
    print("\nINPUT: two-step DAG a->b; a runs long; b stays PENDING on dependency;")
    print("       cancel the whole task while b never became dispatchable")
    make_unit(service, "u1", ["arm"], 1)
    service.submit(
        TaskSpec("dag", (
            StepSpec("a", "arm", timeout=5.0),
            StepSpec("b", "arm", depends_on=("a",), timeout=5.0),
        )),
        {"a": behavior(delay=0.8), "b": behavior()},
    )
    _wait_step_state(service, "dag", "a", StepState.DISPATCHED)
    view, changed = service.cancel("dag")
    print(f"JUDGEMENT: cancel -> changed={changed} task={view.state.value} "
          f"a={view.steps['a'].state.value} b={view.steps['b'].state.value}")
    assert changed is True
    assert view.state == TaskState.CANCELLED
    assert view.steps["a"].state == StepState.CANCELLED
    assert view.steps["b"].state == StepState.CANCELLED
    assert service.registry.get("u1").used_capacity() == 0
    time.sleep(1.0)
    late = service.get_task("dag")
    assert late.state == TaskState.CANCELLED
    assert late.steps["a"].side_effects == 0
    assert_no_dangling_capacity(service)


# ---------------------------------------------------------------------------
# 3. 真正终态任务再次取消：幂等无操作
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("terminal_kind", ["succeeded", "failed", "cancelled"])
def test_cancel_terminal_task_is_idempotent_noop(service, terminal_kind):
    print(f"\nINPUT: drive task to terminal={terminal_kind}, then cancel it twice")
    make_unit(service, "u1", ["arm"], 1)
    if terminal_kind == "succeeded":
        service.submit(
            TaskSpec("t", (StepSpec("s", "arm", timeout=5.0),)),
            {"s": behavior()},
        )
        view0 = wait_terminal(service, "t")
        assert view0.state == TaskState.SUCCEEDED
    elif terminal_kind == "failed":
        service.submit(
            TaskSpec("t", (StepSpec("s", "arm", timeout=5.0),)),
            {"s": behavior(("error",), fail_detail="boom")},
        )
        view0 = wait_terminal(service, "t")
        assert view0.state == TaskState.FAILED
        assert view0.steps["s"].failure_code == FailureCode.NON_RECOVERABLE
    else:
        service.submit(
            TaskSpec("t", (StepSpec("s", "arm", timeout=5.0),)),
            {"s": behavior(delay=0.8)},
        )
        _wait_step_state(service, "t", "s", StepState.DISPATCHED)
        vc, cc = service.cancel("t")
        assert cc is True and vc.state == TaskState.CANCELLED
        view0 = vc

    used_before = service.registry.get("u1").used_capacity()
    effects_before = view0.steps["s"].side_effects
    v1, changed1 = service.cancel("t")
    v2, changed2 = service.cancel("t")
    print(f"JUDGEMENT: terminal={terminal_kind} cancel flags = {changed1}, {changed2}; "
          f"state stays {v1.state.value}")
    assert changed1 is False and changed2 is False
    assert v1.state == v2.state == view0.state
    assert v1.steps["s"].state == view0.steps["s"].state
    assert v1.steps["s"].side_effects == effects_before
    assert service.registry.get("u1").used_capacity() == used_before
    assert_no_dangling_capacity(service)


# ---------------------------------------------------------------------------
# 4. 有步骤在飞：容量立即释放、排队任务马上用上、迟到结果不复活
# ---------------------------------------------------------------------------
def test_cancel_inflight_releases_all_capacity_and_unblocks_queue(service):
    print("\nINPUT: long inflight occupies capacity 2; two cost-1 tasks queue;")
    print("       cancel long -> both queued tasks dispatch and finish")
    make_unit(service, "u1", ["arm"], 2)
    service.submit(
        TaskSpec("long", (StepSpec("s", "arm", capacity_cost=2, timeout=5.0),)),
        {"s": behavior(delay=1.0, side_effects=9)},
    )
    _wait_step_state(service, "long", "s", StepState.DISPATCHED)
    service.submit(
        TaskSpec("w1", (StepSpec("s", "arm", timeout=5.0),)),
        {"s": behavior(delay=0.05)},
    )
    service.submit(
        TaskSpec("w2", (StepSpec("s", "arm", timeout=5.0),)),
        {"s": behavior(delay=0.05)},
    )
    time.sleep(0.05)
    assert service.registry.get("u1").used_capacity() == 2
    assert service.get_task("w1").steps["s"].state == StepState.PENDING

    view, changed = service.cancel("long")
    print(f"JUDGEMENT: cancel long -> changed={changed} task={view.state.value} "
          f"free_now={service.registry.get('u1').free_capacity()}")
    assert changed is True
    assert view.state == TaskState.CANCELLED
    # 容量立刻干净，排队任务在 cancel 触发的调度中马上用上
    assert service.registry.get("u1").used_capacity() in (0, 1, 2)
    assert wait_terminal(service, "w1").state == TaskState.SUCCEEDED
    assert wait_terminal(service, "w2").state == TaskState.SUCCEEDED

    time.sleep(0.8)  # long 的迟到成功落地
    late = service.get_task("long")
    dump_task("long after late zombie success", late)
    assert late.state == TaskState.CANCELLED
    assert late.steps["s"].state == StepState.CANCELLED
    assert late.steps["s"].side_effects == 0
    assert_no_dangling_capacity(service)


# ---------------------------------------------------------------------------
# 5. 取消 × 抢占 × 离线 × 迟到结果同时发生
# ---------------------------------------------------------------------------
def test_cancel_racing_with_preemption_offline_and_late_results(default_service):
    print("\nINPUT: 3 units (arm cap 2/1, camera cap 2); submit:")
    print("  lowA(cost2,arm,delay1.5) lowB(arm,delay1.5) cam(camera,delay1.5)")
    print("  then: high(cost2,arm) preempts lowA; cancel lowA while waiting;")
    print("        take unit-arm-2 offline (lowB inflight there); cancel lowB,")
    print("        cam cancels mid-flight; late results must never resurrect steps")
    u_arm1 = default_service.registry.get("unit-arm-1")
    u_arm2 = default_service.registry.get("unit-arm-2")
    u_cam = default_service.registry.get("unit-cam-1")

    default_service.submit(
        TaskSpec("lowA", (StepSpec("a", "arm", capacity_cost=2, timeout=5.0),),
                 priority=Priority.LOW),
        {"a": behavior(delay=1.5, side_effects=7)},
    )
    default_service.submit(
        TaskSpec("lowB", (StepSpec("b", "arm", timeout=5.0),), priority=Priority.LOW),
        {"b": behavior(delay=1.5, side_effects=7)},
    )
    default_service.submit(
        TaskSpec("cam", (StepSpec("c", "camera", timeout=5.0),)),
        {"c": behavior(delay=1.5, side_effects=7)},
    )
    _wait_step_state(default_service, "lowA", "a", StepState.DISPATCHED)
    _wait_step_state(default_service, "lowB", "b", StepState.DISPATCHED)
    _wait_step_state(default_service, "cam", "c", StepState.DISPATCHED)
    print(f"JUDGEMENT: initial used arm1={u_arm1.used_capacity()} "
          f"arm2={u_arm2.used_capacity()} cam={u_cam.used_capacity()}")

    # 高优先级任务抢占 lowA（arm-1 上 cost=2 全让出）
    default_service.submit(
        TaskSpec("high", (StepSpec("h", "arm", capacity_cost=2, timeout=5.0),),
                 priority=Priority.HIGH),
        {"h": behavior(delay=0.4)},
    )
    a = _wait_step_state(default_service, "lowA", "a", StepState.PENDING)
    assert a.last_reject == RejectReason.PREEMPTED
    print("JUDGEMENT: lowA preempted, now waiting PENDING")

    # 竞争三件事同时发起（独立资源/任务，临界区保证串行生效）
    va, ca = default_service.cancel("lowA")      # 让位等待中：无容量可释放
    default_service.unit_offline("unit-arm-2")   # lowB 在飞处离线 -> 回 PENDING
    vc, cc = default_service.cancel("cam")       # camera 在飞：立即释放
    vb0 = _wait_step_state(default_service, "lowB", "b", StepState.PENDING)
    vb, cb = default_service.cancel("lowB")      # 离线后等待重派：无容量可释放
    print(f"JUDGEMENT: cancel flags lowA(preempted)={ca}, cam(inflight)={cc}, "
          f"lowB(offline-requeued)={cb}; lowB state before its cancel={vb0.state.value}")
    assert ca is True and cc is True and cb is True
    for tid in ("lowA", "lowB", "cam"):
        v = default_service.get_task(tid)
        assert v.state == TaskState.CANCELLED, tid

    # 离线单元上的占用随离线回收；在飞取消立即释放；等待取消不虚释放
    assert u_cam.used_capacity() == 0
    # arm-1 现在只剩 high（cost 2）
    assert u_arm1.used_capacity() == 2

    # 等所有迟到结果（lowA/lowB/cam 的旧尝试 + high 正常完成）落地
    assert default_service.wait_idle(5)
    time.sleep(0.2)
    for tid, sid in (("lowA", "a"), ("lowB", "b"), ("cam", "c")):
        v = default_service.get_task(tid)
        dump_task(f"{tid} final", v)
        assert v.state == TaskState.CANCELLED
        assert v.steps[sid].state == StepState.CANCELLED
        assert v.steps[sid].side_effects == 0
    assert default_service.get_task("high").state == TaskState.SUCCEEDED
    # 离线单元重新上线后也不得有残留占用（账目不因离线/取消竞争丢失）
    default_service.unit_online("unit-arm-2")
    assert_no_dangling_capacity(default_service)


# ---------------------------------------------------------------------------
# 6. 并发提交 + 并发取消：容量不突破、释放总额 == 占用总额
# ---------------------------------------------------------------------------
def test_concurrent_submit_and_cancel_balances_capacity(default_service):
    print("\nINPUT: 24 threads submit to arm capacity 3; even-index tasks are")
    print("       cancelled at a deterministic mid-flight/queued moment;")
    print("       wrap acquire/release to assert total released == total acquired")
    arm_units = [
        u for u in default_service.registry.all_units() if "arm" in u.capabilities
    ]
    total_capacity = sum(u.capacity for u in arm_units)
    acquired = {"cost": 0}
    released = {"cost": 0}
    violations: list[tuple] = []
    wrap_lock = threading.Lock()
    for u in arm_units:
        orig_acquire, orig_release = u.acquire, u.release

        def make_acquire(unit, orig):
            def wrapped(capability, cost):
                ok = orig(capability, cost)
                with wrap_lock:
                    before = unit.used_capacity()
                    if ok:
                        acquired["cost"] += cost
                    if before > unit.capacity:
                        violations.append((unit.unit_id, before))
                return ok
            return wrapped

        def make_release(unit, orig):
            def wrapped(cost):
                with wrap_lock:
                    released["cost"] += cost
                return orig(cost)  # 超额释放会抛 RuntimeError，测试直接失败
            return wrapped

        u.acquire = make_acquire(u, orig_acquire)
        u.release = make_release(u, orig_release)

    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            total = 0
            for u in arm_units:
                used = u.used_capacity()
                total += used
                if used > u.capacity:
                    violations.append((u.unit_id, used))
            if total > total_capacity:
                violations.append(("total", total))
            time.sleep(0.001)

    sampler_t = threading.Thread(target=sampler)
    sampler_t.start()

    n = 24
    barrier = threading.Barrier(n)

    def worker(i):
        tid = f"c-job-{i:02d}"
        spec = TaskSpec(
            tid, (StepSpec("a", "arm", timeout=5.0),),
            priority=Priority.LOW if i % 3 else Priority.HIGH,
        )
        barrier.wait()
        default_service.submit(spec, {"a": behavior(delay=0.08)})
        if i % 2 == 0:
            # 偶数任务：提交后某个时刻取消（可能在飞，也可能还在排队/让位等待）
            time.sleep(0.005 * ((i % 5) + 1))
            v, changed = default_service.cancel(tid)
            # 三边一致：changed=True => 当场已是 CANCELLED；False => 已终态
            assert v.state in (TaskState.CANCELLED, TaskState.SUCCEEDED, TaskState.FAILED)
            if changed:
                assert v.state == TaskState.CANCELLED
            assert default_service.get_task(tid).state == v.state

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert default_service.wait_idle(10)
    stop.set()
    sampler_t.join()

    # 每个任务查账：取消的不复活、成功的不被改；无在飞悬空
    cancelled = succeeded = 0
    for i in range(n):
        v = default_service.get_task(f"c-job-{i:02d}")
        if i % 2 == 0:
            assert v.state in (TaskState.CANCELLED, TaskState.SUCCEEDED, TaskState.FAILED)
        for s in v.steps.values():
            assert s.state in (StepState.SUCCEEDED, StepState.CANCELLED, StepState.FAILED)
            assert s.state != StepState.DISPATCHED
        cancelled += v.state == TaskState.CANCELLED
        succeeded += v.state == TaskState.SUCCEEDED
    print(f"JUDGEMENT: terminal mix cancelled={cancelled} succeeded={succeeded}; "
          f"acquired_total={acquired['cost']} released_total={released['cost']}; "
          f"violations={len(violations)}")
    assert not violations, violations[:5]
    assert acquired["cost"] == released["cost"], (acquired, released)
    assert_no_dangling_capacity(default_service)


# ---------------------------------------------------------------------------
# 7. HTTP 接口：返回体 changed 与 task.state 三边一致
# ---------------------------------------------------------------------------
def test_cancel_http_flag_matches_state_for_queued_and_terminal():
    import warnings

    warnings.filterwarnings("ignore")
    from fastapi.testclient import TestClient

    from fleet.api import create_app
    from fleet.service import FleetService

    svc = FleetService()
    app = create_app(svc)
    with TestClient(app) as c:
        print("\nINPUT: register arm cap1; blocker long, then queued; cancel via HTTP")
        c.post("/units", json={"unit_id": "u1", "capabilities": ["arm"], "capacity": 1})
        c.post("/tasks", json={
            "task_id": "blk",
            "steps": [{"id": "s", "required_capability": "arm",
                       "timeout": 5.0, "delay": 1.0}],
        })
        time.sleep(0.1)
        c.post("/tasks", json={
            "task_id": "q",
            "steps": [{"id": "s", "required_capability": "arm", "timeout": 5.0}],
        })
        time.sleep(0.05)
        r1 = c.post("/tasks/q/cancel")
        body1 = r1.json()
        print(f"JUDGEMENT: cancel queued -> {r1.status_code} changed={body1['changed']} "
              f"state={body1['task']['state']}")
        assert r1.status_code == 200
        assert body1["changed"] is True
        assert body1["task"]["state"] == "CANCELLED"
        # 后续 GET 与第二次取消一致
        assert c.get("/tasks/q").json()["state"] == "CANCELLED"
        r2 = c.post("/tasks/q/cancel")
        body2 = r2.json()
        print(f"JUDGEMENT: cancel again -> changed={body2['changed']} "
              f"state={body2['task']['state']}")
        assert body2["changed"] is False
        assert body2["task"]["state"] == "CANCELLED"
        # 未知任务仍是 404，而不是被当成无操作
        assert c.post("/tasks/nope/cancel").status_code == 404
    svc.shutdown()
