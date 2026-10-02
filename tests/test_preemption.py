"""优先级抢占调度测试。

覆盖需求：
- 高优先级步骤抢占在飞的低优先级步骤：状态为 PREEMPTED、原因可区分、
  容量立即移交且不超卖；让位不是失败、不消耗重试次数，稍后自动重跑；
- 被让出步骤重跑成功后副作用只计一次（旧派发的僵尸成功被丢弃）；
- 派发顺序：优先级高者优先，同级按提交先后 FCFS；同级互不相让；
- 抢占重排后顺序稳定（HIGH -> NORMAL -> 被让位 LOW 的重跑）；
- 任务进入 FAILED 终态后，在飞兄弟步骤被同步取消，迟到结果不改变视图；
- 混合优先级并发提交下容量永不被突破、终态无悬空占用。

每个用例都打印 INPUT（提交优先级/行为脚本/时序）与 JUDGEMENT（观察到的
状态、抢占原因、派发顺序、容量账本），引擎事件日志含 prio 字段可逐条复核。
"""

from __future__ import annotations

import threading
import time

from fleet.models import (
    PreemptReason,
    Priority,
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


def collect_events(engine) -> list:
    """拦截有序引擎事件（判定依据），返回事件列表（与引擎日志一一对应）。"""
    events: list = []
    orig = engine._emit

    def spy(kind, *a, **k):
        ev = orig(kind, *a, **k)
        events.append(ev)
        return ev

    engine._emit = spy
    return events


def _wait_for(service, task_id, step_id, states, timeout=1.0):
    """轮询等待步骤进入给定状态集合之一，返回最终步骤视图。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        step = service.get_task(task_id).steps[step_id]
        if step.state in states:
            return step
        time.sleep(0.003)
    raise AssertionError(
        f"{task_id}/{step_id} never reached {states}; "
        f"got {service.get_task(task_id).steps[step_id].state}"
    )


# ---------- 1. 高优先级抢占低优先级 ----------

def test_high_priority_preempts_low_priority(service):
    print("\nINPUT: capacity=1; LOW job in-flight (delay 0.3, max_retries=0);")
    print("       then HIGH job arrives and must take the capacity.")
    events = collect_events(service.engine)
    make_unit(service, "u1", ["arm"], 1)

    low_spec = TaskSpec(
        "low-job",
        (StepSpec("s", "arm", timeout=5.0, max_retries=0),),
        priority=Priority.LOW,
    )
    service.submit(low_spec, {"s": behavior(delay=0.3)})
    _wait_for(service, "low-job", "s", {StepState.DISPATCHED})
    low_seq_before = service.get_task("low-job").submit_seq

    high_spec = TaskSpec(
        "high-job",
        (StepSpec("s", "arm", timeout=5.0),),
        priority=Priority.HIGH,
    )
    service.submit(high_spec, {"s": behavior(delay=0.15, side_effects=2)})

    # 让位窗口：LOW 必须被明确标记 PREEMPTED，HIGH 接管容量
    low = _wait_for(service, "low-job", "s", {StepState.PREEMPTED})
    print("JUDGEMENT during contention:")
    print(f"  LOW  state={low.state.value} preempt_count={low.preempt_count} "
          f"reason={low.last_preempt_reason.value} epoch(attempts)={low.attempts}")
    high = service.get_task("high-job").steps["s"]
    print(f"  HIGH state={high.state.value} unit={high.assigned_unit}")
    assert low.state == StepState.PREEMPTED
    assert low.preempt_count == 1
    assert low.last_preempt_reason == PreemptReason.PREEMPT_FOR_HIGHER_PRIORITY
    assert low.attempts == 1  # 让位时仍是第 1 次派发的代次
    assert high.state == StepState.DISPATCHED and high.assigned_unit == "u1"
    assert service.registry.get("u1").used_capacity() == 1  # 容量不超卖

    preempt_ev = [e for e in events if e.kind == "preempt"]
    preempted_ev = [e for e in events if e.kind == "preempted"]
    assert len(preempt_ev) == 1 and preempt_ev[0].task_id == "high-job"
    assert preempted_ev[0].task_id == "low-job"
    assert preempted_ev[0].reason == PreemptReason.PREEMPT_FOR_HIGHER_PRIORITY.value
    print(f"  events: preempt#{preempt_ev[0].seq} -> "
          f"preempted#{preempted_ev[0].seq} ({preempted_ev[0].detail})")

    # 最终：两个任务都成功；LOW max_retries=0 仍能成功 => 让位不消耗重试额度
    assert wait_terminal(service, "high-job").state == TaskState.SUCCEEDED
    view_low = wait_terminal(service, "low-job")
    dump_task("after preempt redispatch", view_low)
    assert view_low.state == TaskState.SUCCEEDED
    s = view_low.steps["s"]
    assert s.state == StepState.SUCCEEDED and s.side_effects == 1
    assert s.attempts == 2  # 发生过两次派发（让位重跑），但都按第 0 次执行结果跑
    # 两次 dispatch 事件的行为脚本序号都是 0：抢占没有推进可恢复失败计数
    low_dispatches = [
        e for e in events if e.kind == "dispatch" and e.task_id == "low-job"
    ]
    assert len(low_dispatches) == 2
    assert all("script_attempt=0" in e.detail for e in low_dispatches), \
        [e.detail for e in low_dispatches]
    assert [e.unit_id for e in low_dispatches] == ["u1", "u1"]
    # submit_seq 在抢占重排后不变 => 排队顺序稳定可复现
    assert service.get_task("low-job").submit_seq == low_seq_before
    assert_no_dangling_capacity(service)


# ---------- 2. 让位重跑后副作用只计一次 ----------

def test_preempted_step_side_effects_counted_once(service):
    print("\nINPUT: LOW success with side_effects=5 delay=0.4; preempted by HIGH;")
    print("       the preempted (zombie) run later 'succeeds' too; count must stay 5.")
    events = collect_events(service.engine)
    make_unit(service, "u1", ["arm"], 1)

    service.submit(
        TaskSpec("low-job", (StepSpec("s", "arm", timeout=5.0, max_retries=3),),
                 priority=Priority.LOW),
        {"s": behavior(("succeeded",), side_effects=5, delay=0.4)},
    )
    _wait_for(service, "low-job", "s", {StepState.DISPATCHED})
    service.submit(
        TaskSpec("high-job", (StepSpec("s", "arm", timeout=5.0),),
                 priority=Priority.HIGH),
        {"s": behavior(("succeeded",), side_effects=1, delay=0.05)},
    )

    view = wait_terminal(service, "low-job", timeout=3)
    time.sleep(0.5)  # 等让位派发的僵尸成功落地
    view = service.get_task("low-job")
    dump_task("side effects after zombie lands", view)
    s = view.steps["s"]
    assert s.state == StepState.SUCCEEDED
    assert s.side_effects == 5  # 只有重跑那次成功计入；僵尸成功被丢弃
    # 引擎事件里应有一次 stale（旧代次成功丢弃）+ 一次真正的 success
    stale = [e for e in events if e.kind == "stale" and e.task_id == "low-job"]
    succ = [e for e in events if e.kind == "success" and e.task_id == "low-job"]
    print(f"JUDGEMENT: stale={len(stale)} success={len(succ)} effects={s.side_effects}")
    assert len(stale) == 1 and len(succ) == 1
    assert_no_dangling_capacity(service)


# ---------- 3. 同级 FCFS，互不相让 ----------

def test_same_priority_fcfs_and_no_preemption(service):
    print("\nINPUT: capacity=1; NORMAL 'first-job' in-flight delay=0.3;")
    print("       NORMAL 'second-job' arrives: must queue PENDING, never preempt.")
    events = collect_events(service.engine)
    make_unit(service, "u1", ["arm"], 1)
    dispatched: list[tuple[str, int]] = []
    orig = service.engine._run_attempt

    def spy(task_id, step_id, unit_id, epoch, script_attempt, *a, **k):
        dispatched.append((task_id, epoch))
        return orig(task_id, step_id, unit_id, epoch, script_attempt, *a, **k)

    service.engine._run_attempt = spy

    service.submit(
        TaskSpec("first-job", (StepSpec("s", "arm", timeout=5.0),)),
        {"s": behavior(delay=0.3)},
    )
    _wait_for(service, "first-job", "s", {StepState.DISPATCHED})
    service.submit(
        TaskSpec("second-job", (StepSpec("s", "arm", timeout=5.0),)),
        {"s": behavior(delay=0.0)},
    )
    time.sleep(0.05)
    second = service.get_task("second-job").steps["s"]
    print(f"JUDGEMENT: second-job state={second.state.value} "
          f"last_reject={second.last_reject and second.last_reject.value}")
    assert second.state == StepState.PENDING
    assert second.preempt_count == 0
    assert second.last_reject is not None  # INSUFFICIENT_CAPACITY 可解释为何后派
    assert not any(e.kind in ("preempt", "preempted") for e in events)

    view1 = wait_terminal(service, "first-job")
    view2 = wait_terminal(service, "second-job")
    dump_task("first", view1)
    dump_task("second", view2)
    print(f"JUDGEMENT: dispatch order = {dispatched}")
    assert [t for t, _ in dispatched] == ["first-job", "second-job"]
    assert view1.state == view2.state == TaskState.SUCCEEDED
    assert_no_dangling_capacity(service)


# ---------- 4. 抢占重排后的全局顺序 HIGH -> NORMAL -> 让位 LOW 重跑 ----------

def test_redispatch_order_after_preemption_is_stable(service):
    print("\nINPUT: capacity=1; LOW in-flight(delay 0.5); HIGH arrives(0.08);")
    print("       NORMAL arrives(0.08). Expected order: LOW#1, HIGH, NORMAL, LOW#2.")
    events = collect_events(service.engine)
    make_unit(service, "u1", ["arm"], 1)
    dispatched: list[tuple[str, int]] = []
    orig = service.engine._run_attempt

    def spy(task_id, step_id, unit_id, epoch, script_attempt, *a, **k):
        dispatched.append((task_id, epoch))
        return orig(task_id, step_id, unit_id, epoch, script_attempt, *a, **k)

    service.engine._run_attempt = spy

    service.submit(
        TaskSpec("low-job", (StepSpec("s", "arm", timeout=5.0),),
                 priority=Priority.LOW),
        {"s": behavior(delay=0.5)},
    )
    _wait_for(service, "low-job", "s", {StepState.DISPATCHED})
    service.submit(
        TaskSpec("high-job", (StepSpec("s", "arm", timeout=5.0),),
                 priority=Priority.HIGH),
        {"s": behavior(delay=0.08)},
    )
    service.submit(
        TaskSpec("normal-job", (StepSpec("s", "arm", timeout=5.0),),
                 priority=Priority.NORMAL),
        {"s": behavior(delay=0.08)},
    )

    assert wait_terminal(service, "high-job")
    assert wait_terminal(service, "normal-job")
    assert wait_terminal(service, "low-job", timeout=3)
    print(f"JUDGEMENT: dispatch (task, epoch) order = {dispatched}")
    # HIGH 抢占 LOW 先跑；NORMAL 高于 LOW 次之；LOW 最后以新代次重跑
    assert dispatched == [
        ("low-job", 1),
        ("high-job", 1),
        ("normal-job", 1),
        ("low-job", 2),
    ]
    seqs = {
        t: service.get_task(t).submit_seq for t in
        ("low-job", "high-job", "normal-job")
    }
    print(f"JUDGEMENT: submit_seq = {seqs} (LOW kept its original slot on requeue)")
    assert seqs == {"low-job": 1, "high-job": 2, "normal-job": 3}
    assert_no_dangling_capacity(service)


# ---------- 5. 失败终态后在飞兄弟步骤被取消，迟到结果不改视图 ----------

def test_failed_task_terminal_sibling_late_result_discarded(service):
    print("\nINPUT: capacity=2; a,b independent; a=error(delay 0) b=success(delay 0.35);")
    print("       task goes FAILED immediately; b is in-flight and must be cancelled,")
    print("       its late success discarded (no side effects, view stays terminal).")
    events = collect_events(service.engine)
    make_unit(service, "u1", ["arm"], 2)
    spec = TaskSpec("term", (
        StepSpec("a", "arm", timeout=5.0),
        StepSpec("b", "arm", timeout=5.0),
    ))
    service.submit(spec, {
        "a": behavior(("error",), fail_detail="arm hardware fault"),
        "b": behavior(("succeeded",), side_effects=9, delay=0.35),
    })

    view = wait_terminal(service, "term")
    dump_task("terminal task", view)
    assert view.state == TaskState.FAILED
    assert view.steps["a"].state == StepState.FAILED
    b = view.steps["b"]
    assert b.state == StepState.CANCELLED  # 在飞兄弟步骤随终态被同步取消
    assert b.side_effects == 0

    cancel_ev = [e for e in events if e.kind == "cancel_sibling"]
    print(f"JUDGEMENT: cancel_sibling events = "
          f"{[(e.step_id, e.reason) for e in cancel_ev]}")
    assert any(e.step_id == "b" for e in cancel_ev)

    # 等 b 的僵尸成功落地：视图不得复活、不得计数、容量不得悬空
    time.sleep(0.45)
    again = service.get_task("term")
    dump_task("after zombie lands", again)
    assert again.state == TaskState.FAILED
    assert again.steps["b"].state == StepState.CANCELLED
    assert again.steps["b"].side_effects == 0
    stale = [e for e in events if e.kind == "stale" and e.step_id == "b"]
    print(f"JUDGEMENT: stale results for b = {len(stale)}")
    assert len(stale) == 1
    assert_no_dangling_capacity(service)


# ---------- 6. 混合优先级并发提交：容量永不突破 ----------

def test_concurrent_mixed_priority_never_oversells(default_service):
    print("\nINPUT: 15 threads, mixed HIGH/NORMAL/LOW priorities against arm capacity 3")
    violations: list = []
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            for u in default_service.registry.all_units():
                if u.used_capacity() > u.capacity:
                    violations.append((u.unit_id, u.used_capacity(), u.capacity))
            total = sum(
                u.used_capacity() for u in default_service.registry.all_units()
            )
            if total < 0:
                violations.append(("negative-total", total))
            time.sleep(0.001)

    t = threading.Thread(target=sampler)
    t.start()

    prios = [Priority.HIGH, Priority.NORMAL, Priority.LOW]
    barrier = threading.Barrier(15)

    def worker(i):
        spec = TaskSpec(f"pjob-{i:02d}", (
            StepSpec("a", "arm", timeout=5.0),
            StepSpec("b", "arm", depends_on=("a",), timeout=5.0),
        ), priority=prios[i % 3])
        barrier.wait()
        default_service.submit(spec, {
            "a": behavior(delay=0.03),
            "b": behavior(delay=0.03),
        })

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(15)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert default_service.wait_idle(8)
    stop.set()
    t.join()
    print(f"JUDGEMENT: capacity violations sampled = {len(violations)}")
    assert not violations, violations[:5]
    for i in range(15):
        view = default_service.get_task(f"pjob-{i:02d}")
        assert view.state == TaskState.SUCCEEDED, view
        assert view.priority == prios[i % 3]
    assert_no_dangling_capacity(default_service)


# ---------- 7. 在让位窗口中取消：changed=True，PREEMPTED 步骤被取消，僵尸结果丢弃 ----------

def test_cancel_preempted_step_during_yield_window(service):
    print("\nINPUT: LOW preempted by HIGH and sitting PREEMPTED; cancel LOW in that")
    print("       window. cancel must be effective (changed=True), zombie discarded.")
    events = collect_events(service.engine)
    make_unit(service, "u1", ["arm"], 1)

    service.submit(
        TaskSpec("low-job", (StepSpec("s", "arm", timeout=5.0),),
                 priority=Priority.LOW),
        {"s": behavior(("succeeded",), side_effects=4, delay=0.4)},
    )
    _wait_for(service, "low-job", "s", {StepState.DISPATCHED})
    service.submit(
        TaskSpec("high-job", (StepSpec("s", "arm", timeout=5.0),),
                 priority=Priority.HIGH),
        {"s": behavior(("succeeded",), side_effects=1, delay=0.3)},
    )
    # 等 LOW 进入让位态
    _wait_for(service, "low-job", "s", {StepState.PREEMPTED})
    print("JUDGEMENT: cancelling low-job while PREEMPTED (HIGH still running)")
    view, changed = service.cancel("low-job")
    assert changed is True
    assert view.state == TaskState.CANCELLED
    assert view.steps["s"].state == StepState.CANCELLED

    # 再次取消幂等
    _, changed2 = service.cancel("low-job")
    assert changed2 is False

    # 等 LOW 僵尸成功 + HIGH 完成落地
    time.sleep(0.5)
    low = service.get_task("low-job")
    dump_task("low after cancel+zombie", low)
    assert low.state == TaskState.CANCELLED
    assert low.steps["s"].state == StepState.CANCELLED
    assert low.steps["s"].side_effects == 0
    high = wait_terminal(service, "high-job")
    assert high.state == TaskState.SUCCEEDED
    assert_no_dangling_capacity(service)


# ---------- 8. cost>1 的步骤：抢占多个低优先级在飞步骤凑足容量 ----------

def test_preemption_accumulates_capacity_from_multiple_victims(service):
    print("\nINPUT: capacity=2; two LOW steps cost1 fill it; HIGH step cost2 arrives")
    print("       and must evict BOTH low steps (freed accumulates to 2).")
    events = collect_events(service.engine)
    make_unit(service, "u1", ["arm"], 2)

    service.submit(
        TaskSpec("low-a", (StepSpec("s", "arm", timeout=5.0),),
                 priority=Priority.LOW),
        {"s": behavior(delay=0.4)},
    )
    service.submit(
        TaskSpec("low-b", (StepSpec("s", "arm", timeout=5.0),),
                 priority=Priority.LOW),
        {"s": behavior(delay=0.4)},
    )
    _wait_for(service, "low-a", "s", {StepState.DISPATCHED})
    _wait_for(service, "low-b", "s", {StepState.DISPATCHED})
    assert service.registry.get("u1").used_capacity() == 2

    service.submit(
        TaskSpec("high-big", (StepSpec("s", "arm", capacity_cost=2, timeout=5.0),),
                 priority=Priority.HIGH),
        {"s": behavior(delay=0.05)},
    )

    la = _wait_for(service, "low-a", "s", {StepState.PREEMPTED})
    lb = _wait_for(service, "low-b", "s", {StepState.PREEMPTED})
    high = service.get_task("high-big").steps["s"]
    print(f"JUDGEMENT: low-a preempt={la.preempt_count} low-b preempt={lb.preempt_count} "
          f"high={high.state.value}@{high.assigned_unit} used="
          f"{service.registry.get('u1').used_capacity()}")
    assert high.state == StepState.DISPATCHED and high.assigned_unit == "u1"
    assert service.registry.get("u1").used_capacity() == 2  # 占用 2，未超卖
    preempted = [e for e in events if e.kind == "preempted"]
    assert {e.task_id for e in preempted} == {"low-a", "low-b"}

    assert wait_terminal(service, "high-big").state == TaskState.SUCCEEDED
    assert wait_terminal(service, "low-a", timeout=3).state == TaskState.SUCCEEDED
    assert wait_terminal(service, "low-b", timeout=3).state == TaskState.SUCCEEDED
    time.sleep(0.5)
    assert_no_dangling_capacity(service)
