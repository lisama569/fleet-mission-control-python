"""取消语义专项测试。

固定契约（changed 标记 / 返回体任务状态 / 后续查询 三边始终一致）：
- changed=True  当且仅当本次真的把一个**非终态**任务改成了 CANCELLED，
  与它当时是否占着执行容量无关（在飞 / 排队等容量 / 被抢占让位 /
  离线重排队都算变更）；
- changed=False 当且仅当任务此前已在终态（SUCCEEDED/FAILED/CANCELLED），
  幂等无操作：不报错、不改状态、不碰容量；
- 在飞步骤被取消时占用立即全部释放，排队任务马上能用；迟到结果丢弃，
  不重复释放、不复活步骤、不把"任务已取消后又有步骤成功/在飞"暴露给查询。

覆盖：
1. 被抢占让位后等待重新派发的任务，取消 -> changed=True 且前后一致；
2. 一直排队等容量的任务，取消 -> changed=True 且前后一致；
3. 单元离线重排队后的任务取消（同样没占容量，仍算变更）；
4. 已 SUCCEEDED / FAILED / CANCELLED 的任务取消 -> 幂等无操作；
5. 有步骤在飞的任务取消：容量立刻全部释放、排队任务顶上、
   僵尸迟到结果不造成重复释放；
6. 并发提交 + 并发取消：容量不被突破、无悬空占用，每次返回都满足三边一致；
7. HTTP 端到端：响应 changed 与任务状态、后续 GET 一致。
"""

from __future__ import annotations

import random
import threading
import time

from fleet.models import (
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

TERMINAL = {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}


def _wait_step_state(service, task_id, step_id, expected, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = service.get_task(task_id).steps[step_id].state
        if st == expected:
            return
        time.sleep(0.005)
    assert service.get_task(task_id).steps[step_id].state == expected


# ---------------------------------------------------------------------------
# 1. 被抢占让位、等待重新派发的任务取消
# ---------------------------------------------------------------------------
def test_cancel_preempted_waiting_task_reports_changed_and_consistent_view(service):
    print("\nINPUT: low-priority step is dispatched (delay 0.5, u1 cap 1);")
    print("       high-priority step preempts it -> low waits PENDING(PREEMPTED);")
    print("       THEN cancel low while it holds NO capacity")
    make_unit(service, "u1", ["arm"], 1)

    service.submit(
        TaskSpec("low", (StepSpec("s", "arm", timeout=5.0),), priority=Priority.LOW),
        {"s": behavior(delay=0.5)},
    )
    _wait_step_state(service, "low", "s", StepState.DISPATCHED)
    service.submit(
        TaskSpec("high", (StepSpec("s", "arm", timeout=5.0),), priority=Priority.HIGH),
        {"s": behavior(delay=0.3)},
    )

    preempted = service.get_task("low")
    dump_task("low preempted, waiting for redispatch", preempted)
    low_step = preempted.steps["s"]
    print(f"JUDGEMENT: low task={preempted.state.value} step={low_step.state.value} "
          f"reject={low_step.last_reject} (non-terminal, holds no capacity)")
    assert preempted.state == TaskState.PENDING
    assert low_step.state == StepState.PENDING
    assert low_step.last_reject == RejectReason.PREEMPTED
    assert service.registry.get("u1").used_capacity() == 1  # 占容量的是 high

    # 本次取消确实改了一个非终态任务 -> changed 必须为 True
    view, changed = service.cancel("low")
    again = service.get_task("low")
    print(f"JUDGEMENT: cancel -> changed={changed}, response state={view.state.value}, "
          f"follow-up query={again.state.value}, step={view.steps['s'].state.value}")
    assert changed is True
    assert view.state == TaskState.CANCELLED
    assert view.steps["s"].state == StepState.CANCELLED
    assert again.state == TaskState.CANCELLED                    # 三边一致
    assert again.steps["s"].state == StepState.CANCELLED
    assert service.registry.get("u1").used_capacity() == 1       # low 本就没占容量

    # 再次取消：幂等无操作
    view2, changed2 = service.cancel("low")
    print(f"JUDGEMENT: second cancel -> changed={changed2}, state={view2.state.value}")
    assert changed2 is False
    assert view2.state == TaskState.CANCELLED

    # 僵尸结果迟到：不得复活步骤、不得计副作用、不得冒出在飞步骤
    time.sleep(0.55)
    final = service.get_task("low")
    dump_task("low after late zombie result", final)
    print(f"JUDGEMENT: state={final.state.value} step={final.steps['s'].state.value} "
          f"effects={final.steps['s'].side_effects} used="
          f"{service.registry.get('u1').used_capacity()}")
    assert final.state == TaskState.CANCELLED
    assert final.steps["s"].state == StepState.CANCELLED
    assert final.steps["s"].side_effects == 0
    assert service.get_task("high").state == TaskState.SUCCEEDED
    assert_no_dangling_capacity(service)


# ---------------------------------------------------------------------------
# 2. 一直排队等容量的任务取消
# ---------------------------------------------------------------------------
def test_cancel_queued_task_never_dispatched_reports_changed(service):
    print("\nINPUT: blocker occupies u1 (cap 1, delay 1.0); 'queued' waits for")
    print("       capacity and has never been dispatched; THEN cancel queued")
    make_unit(service, "u1", ["arm"], 1)
    service.submit(
        TaskSpec("blocker", (StepSpec("s", "arm", timeout=5.0),)),
        {"s": behavior(delay=1.0)},
    )
    _wait_step_state(service, "blocker", "s", StepState.DISPATCHED)
    service.submit(
        TaskSpec("queued", (StepSpec("s", "arm", timeout=5.0),)),
        {"s": behavior(delay=0.02)},
    )
    q = service.get_task("queued")
    print(f"JUDGEMENT: queued task={q.state.value} step={q.steps['s'].state.value} "
          f"reject={q.steps['s'].last_reject}")
    assert q.steps["s"].state == StepState.PENDING
    assert q.steps["s"].last_reject == RejectReason.INSUFFICIENT_CAPACITY

    view, changed = service.cancel("queued")
    again = service.get_task("queued")
    print(f"JUDGEMENT: cancel -> changed={changed}, response={view.state.value}, "
          f"query={again.state.value}, step={view.steps['s'].state.value}")
    assert changed is True                                         # 关键回归断言
    assert view.state == TaskState.CANCELLED
    assert view.steps["s"].state == StepState.CANCELLED
    assert again.state == TaskState.CANCELLED
    # 排队任务从未占容量：取消前后 blocker 的占用纹丝不动
    assert service.registry.get("u1").used_capacity() == 1

    view2, changed2 = service.cancel("queued")
    assert changed2 is False and view2.state == TaskState.CANCELLED
    print(f"JUDGEMENT: second cancel -> changed={changed2} (idempotent no-op)")

    # blocker 跑完后，排队任务不得"复活"被派发
    time.sleep(1.1)
    final = service.get_task("queued")
    print(f"JUDGEMENT: after blocker finished, queued stays {final.state.value} "
          f"(attempts={final.steps['s'].attempts})")
    assert final.state == TaskState.CANCELLED
    assert final.steps["s"].attempts == 0
    assert_no_dangling_capacity(service)


# ---------------------------------------------------------------------------
# 3. 单元离线重排队后取消（没占容量，但非终态 -> 仍算变更）
# ---------------------------------------------------------------------------
def test_cancel_offline_requeued_task_reports_changed(service):
    print("\nINPUT: step in-flight on u1 (delay 0.5, retries=3); u1 goes offline")
    print("       -> step requeued PENDING on no capacity; THEN cancel it")
    u1 = make_unit(service, "u1", ["arm"], 1)
    service.submit(
        TaskSpec("off", (StepSpec("s", "arm", timeout=5.0, max_retries=3),)),
        {"s": behavior(delay=0.5)},
    )
    _wait_step_state(service, "off", "s", StepState.DISPATCHED)
    service.unit_offline("u1")

    view0 = service.get_task("off")
    print(f"JUDGEMENT: after offline step={view0.steps['s'].state.value} "
          f"used={u1.used_capacity()}")
    assert view0.steps["s"].state == StepState.PENDING
    assert u1.used_capacity() == 0

    view, changed = service.cancel("off")
    print(f"JUDGEMENT: cancel -> changed={changed}, task={view.state.value}, "
          f"step={view.steps['s'].state.value}")
    assert changed is True
    assert view.state == TaskState.CANCELLED
    assert view.steps["s"].state == StepState.CANCELLED
    assert service.get_task("off").state == TaskState.CANCELLED
    view2, changed2 = service.cancel("off")
    assert changed2 is False
    time.sleep(0.6)  # 离线单元上的僵尸线程落地
    assert u1.used_capacity() == 0
    assert_no_dangling_capacity(service)


# ---------------------------------------------------------------------------
# 4. 真正终态的任务取消：幂等无操作
# ---------------------------------------------------------------------------
def test_cancel_terminal_tasks_is_idempotent_noop(service):
    make_unit(service, "u1", ["arm"], 2)

    # 4a. SUCCEEDED
    print("\nINPUT: task 'ok' runs to SUCCEEDED; cancel it twice")
    service.submit(
        TaskSpec("ok", (StepSpec("s", "arm", timeout=2.0),)),
        {"s": behavior(delay=0.02)},
    )
    ok = wait_terminal(service, "ok")
    assert ok.state == TaskState.SUCCEEDED
    v1, c1 = service.cancel("ok")
    v2, c2 = service.cancel("ok")
    print(f"JUDGEMENT: cancel SUCCEEDED -> changed={c1}, {c2}; "
          f"states={v1.state.value}, {v2.state.value}; effects preserved="
          f"{v2.steps['s'].side_effects}")
    assert c1 is False and c2 is False
    assert v1.state == v2.state == TaskState.SUCCEEDED
    assert v2.steps["s"].side_effects == 1   # 终态数据不被触碰

    # 4b. FAILED（不可恢复错误）
    print("INPUT: task 'bad' fails with non-recoverable error; cancel it")
    service.submit(
        TaskSpec("bad", (StepSpec("s", "arm", timeout=2.0),)),
        {"s": behavior(("error",), fail_detail="arm fault", delay=0.02)},
    )
    bad = wait_terminal(service, "bad")
    assert bad.state == TaskState.FAILED
    v3, c3 = service.cancel("bad")
    print(f"JUDGEMENT: cancel FAILED -> changed={c3}, state={v3.state.value}, "
          f"failure_code={v3.steps['s'].failure_code.value}")
    assert c3 is False
    assert v3.state == TaskState.FAILED
    assert v3.steps["s"].failure_code is not None

    # 4c. CANCELLED（首次取消在飞任务为真变更，其后为无操作）
    print("INPUT: task 'gone' cancelled in-flight, then cancelled again")
    service.submit(
        TaskSpec("gone", (StepSpec("s", "arm", timeout=5.0),)),
        {"s": behavior(delay=0.4)},
    )
    _wait_step_state(service, "gone", "s", StepState.DISPATCHED)
    v4, c4 = service.cancel("gone")
    v5, c5 = service.cancel("gone")
    print(f"JUDGEMENT: first cancel changed={c4} state={v4.state.value}; "
          f"second cancel changed={c5} state={v5.state.value}")
    assert c4 is True and c5 is False
    assert v4.state == v5.state == TaskState.CANCELLED
    assert_no_dangling_capacity(service)


# ---------------------------------------------------------------------------
# 5. 在飞步骤取消：容量立刻全部释放、排队任务顶上、僵尸不重复释放
# ---------------------------------------------------------------------------
def test_cancel_inflight_releases_all_capacity_and_queued_task_proceeds(service):
    print("\nINPUT: u1 cap 2; 'big' has two in-flight steps (delay 0.5) using 2/2;")
    print("       'next' queues for arm; cancel big -> both slots free at once")
    u1 = make_unit(service, "u1", ["arm"], 2)
    service.submit(
        TaskSpec("big", (
            StepSpec("s1", "arm", timeout=5.0),
            StepSpec("s2", "arm", timeout=5.0),
        )),
        {"s1": behavior(delay=0.5), "s2": behavior(delay=0.5)},
    )
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        v = service.get_task("big")
        if (v.steps["s1"].state == StepState.DISPATCHED
                and v.steps["s2"].state == StepState.DISPATCHED):
            break
        time.sleep(0.005)
    assert u1.used_capacity() == 2
    service.submit(
        TaskSpec("next", (StepSpec("s", "arm", timeout=2.0),)),
        {"s": behavior(delay=0.02)},
    )

    view, changed = service.cancel("big")
    dump_task("big cancelled while two steps in-flight", view)
    # 取消在同一临界区释放两个占用并触发一次调度：排队的 next 可能已经顶上，
    # 因此此刻 u1 上若有占用，只能属于 next（cost=1），绝不允许是 big 的残留。
    used_after = u1.used_capacity()
    nxt_step = service.get_task("next").steps["s"]
    print(f"JUDGEMENT: changed={changed}, used immediately after cancel="
          f"{used_after} (only 'next' may hold it: next.state={nxt_step.state.value})")
    assert changed is True
    assert view.state == TaskState.CANCELLED
    assert view.steps["s1"].state == StepState.CANCELLED
    assert view.steps["s2"].state == StepState.CANCELLED
    if used_after:
        assert used_after == 1
        assert nxt_step.state == StepState.DISPATCHED
    assert service.get_task("big").state == TaskState.CANCELLED

    nxt = wait_terminal(service, "next")
    print(f"JUDGEMENT: queued task final state={nxt.state.value}")
    assert nxt.state == TaskState.SUCCEEDED          # 释放的容量立刻被用上

    time.sleep(0.6)  # 等两个僵尸成功结果迟到
    final = service.get_task("big")
    print(f"JUDGEMENT: big after zombies: state={final.state.value} "
          f"s1={final.steps['s1'].state.value} s2={final.steps['s2'].state.value} "
          f"used={u1.used_capacity()}")
    assert final.state == TaskState.CANCELLED
    assert final.steps["s1"].state == StepState.CANCELLED
    assert final.steps["s2"].state == StepState.CANCELLED
    assert final.steps["s1"].side_effects == 0
    assert final.steps["s2"].side_effects == 0
    assert u1.used_capacity() == 0                   # 无重复释放、无悬空占用
    assert_no_dangling_capacity(service)


# ---------------------------------------------------------------------------
# 6. 并发提交 + 并发取消：容量不突破、无悬空占用、三边一致
# ---------------------------------------------------------------------------
def test_concurrent_submit_and_cancel_keeps_capacity_and_views_consistent(
    default_service,
):
    print("\nINPUT: 16 submitters (1 arm step each, random short delay) race with")
    print("       8 cancellers hammering cancel on the same task ids; arm cap = 3")
    units = [u for u in default_service.registry.all_units() if "arm" in u.capabilities]
    total_cap = sum(u.capacity for u in units)
    violations: list = []
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            total = 0
            for u in units:
                used = u.used_capacity()
                total += used
                if used > u.capacity:
                    violations.append((u.unit_id, used))
            if total > total_cap:
                violations.append(("total", total))
            time.sleep(0.001)

    sampler_t = threading.Thread(target=sampler)
    sampler_t.start()

    n_tasks = 16
    barrier = threading.Barrier(n_tasks + 8)
    rng = random.Random(20261004)
    # 记录每次取消调用的 (changed, 返回体状态, 随后查询状态)，逐条校验三边一致
    cancel_results: list[tuple[bool, TaskState, TaskState]] = []
    results_lock = threading.Lock()

    def submitter(i):
        barrier.wait()
        spec = TaskSpec(
            f"c-{i:02d}",
            (StepSpec("s", "arm", timeout=5.0),),
        )
        default_service.submit(
            spec, {"s": behavior(delay=0.02 + rng.random() * 0.08)}
        )

    def canceller():
        barrier.wait()
        end = time.monotonic() + 0.6
        while time.monotonic() < end:
            tid = f"c-{rng.randrange(n_tasks):02d}"
            try:
                view, changed = default_service.cancel(tid)
            except Exception:
                # 提交尚未落库 -> 任务还不存在，等价于没有可取消对象
                continue
            follow = default_service.get_task(tid)
            with results_lock:
                cancel_results.append((changed, view.state, follow.state))
            time.sleep(0.002)

    threads = [threading.Thread(target=submitter, args=(i,)) for i in range(n_tasks)]
    threads += [threading.Thread(target=canceller) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert default_service.wait_idle(10)
    stop.set()
    sampler_t.join()

    print(f"JUDGEMENT: capacity violations sampled = {len(violations)}")
    assert not violations, violations[:5]

    # 每个任务终态自洽：CANCELLED 的任务不允许残留 DISPATCHED/PENDING 非终态步骤
    states = {}
    for i in range(n_tasks):
        v = default_service.get_task(f"c-{i:02d}")
        states[v.task_id] = v.state
        assert v.state in TERMINAL, v.state
        if v.state == TaskState.CANCELLED:
            for sid, step in v.steps.items():
                assert step.state in (
                    StepState.CANCELLED, StepState.SUCCEEDED, StepState.FAILED,
                ), (v.task_id, sid, step.state)
    changed_true = sum(1 for c, _, _ in cancel_results if c)
    changed_false = len(cancel_results) - changed_true
    print(f"JUDGEMENT: cancel calls observed={len(cancel_results)} "
          f"changed=True={changed_true} changed=False={changed_false}")
    assert cancel_results, "cancellers should have observed some tasks"

    # 三边一致不变量：changed 与返回体状态、后续查询必须互相印证
    for changed, resp_state, follow_state in cancel_results:
        if changed:
            assert resp_state == TaskState.CANCELLED
            assert follow_state == TaskState.CANCELLED
        else:
            # 无操作只能发生在此前已终态的任务上，且状态不被本次调用改变
            assert resp_state in TERMINAL
            assert follow_state == resp_state
    print(f"JUDGEMENT: terminal state histogram = "
          f"{ {s.value: list(states.values()).count(s) for s in set(states.values())} }")
    assert_no_dangling_capacity(default_service)


# ---------------------------------------------------------------------------
# 7. HTTP 端到端：changed 与状态、后续 GET 一致
# ---------------------------------------------------------------------------
def test_cancel_api_changed_flag_matches_state():
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
        print("\nINPUT(HTTP): blocker holds u1; api-q queues without ever running")
        c.post("/tasks", json={"task_id": "blocker", "steps": [
            {"id": "s", "required_capability": "arm", "timeout": 10,
             "delay": 1.0}]})
        time.sleep(0.1)
        c.post("/tasks", json={"task_id": "api-q", "steps": [
            {"id": "s", "required_capability": "arm", "timeout": 5}]})
        before = c.get("/tasks/api-q").json()
        print(f"JUDGEMENT: queued step state before cancel = "
              f"{before['steps'][0]['state']}")

        r1 = c.post("/tasks/api-q/cancel").json()
        r2 = c.get("/tasks/api-q").json()
        r3 = c.post("/tasks/api-q/cancel").json()
        print(f"JUDGEMENT: 1st cancel changed={r1['changed']} state={r1['task']['state']}; "
              f"follow-up GET state={r2['state']}; "
              f"2nd cancel changed={r3['changed']} state={r3['task']['state']}")
        assert r1["changed"] is True
        assert r1["task"]["state"] == "CANCELLED"
        assert r1["task"]["steps"][0]["state"] == "CANCELLED"
        assert r2["state"] == "CANCELLED"            # 三边一致
        assert r3["changed"] is False               # 幂等无操作
        assert r3["task"]["state"] == "CANCELLED"
    svc.shutdown()
