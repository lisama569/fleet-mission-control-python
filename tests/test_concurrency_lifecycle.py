"""并发提交、单元离线与取消的容量生命周期测试。

覆盖：
- 多任务并发提交：单元占用永不超过容量（容量约束不被突破）；
- 派发顺序按 task_id FIFO 可复现；
- 执行单元离线：在飞步骤重新排队、占用释放，恢复后成功；
- 取消抢占后容量立即可被其它任务使用，无悬空占用。
"""

from __future__ import annotations

import threading

from fleet.models import StepSpec, StepState, TaskSpec, TaskState
from tests.conftest import (
    assert_no_dangling_capacity,
    behavior,
    dump_task,
    make_unit,
    wait_terminal,
)


def test_concurrent_submits_never_exceed_capacity(default_service):
    print("\nINPUT: 12 threads x tasks with cost=1 against total arm capacity 3")
    violations = []
    orig_acquire = None

    # 直接监控三个 arm 单元的占用峰值：通过周期性采样 used
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            total = sum(
                u.used_capacity()
                for u in default_service.registry.all_units()
                if "arm" in u.capabilities
            )
            if total > 3:
                violations.append(total)
            # 每个单元自身也不得超额
            for u in default_service.registry.all_units():
                if u.used_capacity() > u.capacity:
                    violations.append((u.unit_id, u.used_capacity()))

    t = threading.Thread(target=sampler)
    t.start()

    barrier = threading.Barrier(12)

    def worker(i):
        spec = TaskSpec(f"job-{i:02d}", (
            StepSpec("a", "arm", timeout=2.0),
            StepSpec("b", "arm", depends_on=("a",), timeout=2.0),
        ))
        barrier.wait()
        default_service.submit(spec, {
            "a": behavior(delay=0.05),
            "b": behavior(delay=0.05),
        })

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert default_service.wait_idle(5)
    stop.set(); t.join()
    print(f"JUDGEMENT: capacity violations sampled = {len(violations)}")
    assert not violations, violations[:5]
    for i in range(12):
        view = default_service.get_task(f"job-{i:02d}")
        assert view.state == TaskState.SUCCEEDED, view
    assert_no_dangling_capacity(default_service)


def test_dispatch_order_is_fifo_when_capacity_contended(service):
    print("\nINPUT: submit z then a then m; capacity 1; verify FIFO by task id")
    make_unit(service, "u1", ["arm"], 1)
    dispatched: list[str] = []
    orig = service.engine._run_attempt

    def spy(task_id, step_id, unit_id, *a, **k):
        dispatched.append((task_id, step_id))
        return orig(task_id, step_id, unit_id, *a, **k)

    service.engine._run_attempt = spy
    # 按非排序顺序提交，制造 contention：先占满容量再排队
    for tid, delay in [("z", 0.12), ("a", 0.02), ("m", 0.02)]:
        service.submit(
            TaskSpec(tid, (StepSpec("s", "arm", timeout=2.0),)),
            {"s": behavior(delay=delay)},
        )
    assert service.wait_idle(3)
    order = [t for t, _ in dispatched]
    print(f"JUDGEMENT: dispatch order = {order}")
    # 容量=1 时，调度按 task_id 排序；首个派发后其余等待，释放后按 a,m,z 推进
    assert order[0] == "z"  # 最先提交时容量空闲立即派发
    assert order[1:] == ["a", "m"], order
    assert_no_dangling_capacity(service)


def test_unit_offline_releases_and_requeues(service):
    print("\nINPUT: step running on u1 (delay 0.4); take u1 offline mid-flight")
    u1 = make_unit(service, "u1", ["arm"], 1)
    make_unit(service, "u2", ["arm"], 1)
    spec = TaskSpec("off", (StepSpec("s", "arm", timeout=5.0, max_retries=3),))
    service.submit(spec, {"s": behavior(("succeeded",), delay=0.4)})

    import time
    time.sleep(0.05)
    assert service.get_task("off").steps["s"].state == StepState.DISPATCHED
    service.unit_offline("u1")
    print("JUDGEMENT: u1 offline -> step requeued PENDING, u1 capacity released")
    assert u1.used_capacity() == 0
    assert u1.is_online() is False
    # 重新派发到在线的 u2
    view = wait_terminal(service, "off")
    dump_task("offline requeue", view)
    assert view.steps["s"].state == StepState.SUCCEEDED
    assert view.steps["s"].assigned_unit == "u2"
    assert service.registry.get("u2").used_capacity() == 0
    # u1 始终离线，其容量账本仍为 0（僵尸线程迟到也不占回）
    time.sleep(0.4)
    assert u1.used_capacity() == 0


def test_cancelled_capacity_is_reused_by_other_task(service):
    print("\nINPUT: job-long holds unit; job-wait queued; cancel long -> wait proceeds")
    make_unit(service, "u1", ["arm"], 1)
    service.submit(
        TaskSpec("long", (StepSpec("s", "arm", timeout=5.0),)),
        {"s": behavior(delay=1.0)},
    )
    import time
    time.sleep(0.05)
    service.submit(
        TaskSpec("wait", (StepSpec("s", "arm", timeout=2.0),)),
        {"s": behavior(delay=0.02)},
    )
    # wait 因容量不足被拒
    assert service.get_task("wait").steps["s"].last_reject is not None
    changed = service.cancel("long")[1]
    assert changed is True
    view = wait_terminal(service, "wait")
    dump_task("capacity reused after cancel", view)
    assert view.state == TaskState.SUCCEEDED
    assert service.get_task("long").state == TaskState.CANCELLED
    assert_no_dangling_capacity(service)


def test_offline_during_long_success_no_double_release(service):
    print("\nINPUT: long success (delay 0.5) on u1; u1 goes offline mid-flight;")
    print("       step redispatches to u2 and succeeds; u1's zombie succeeds late.")
    u1 = make_unit(service, "u1", ["arm"], 1)
    u2 = make_unit(service, "u2", ["arm"], 1)
    spec = TaskSpec("zombie", (StepSpec("s", "arm", timeout=5.0, max_retries=3),))
    service.submit(spec, {"s": behavior(("succeeded",), side_effects=1, delay=0.5)})

    import time
    time.sleep(0.05)
    service.unit_offline("u1")
    view = wait_terminal(service, "zombie")
    dump_task("offline during long success", view)
    assert view.steps["s"].state == StepState.SUCCEEDED
    assert view.steps["s"].assigned_unit == "u2"
    assert view.steps["s"].side_effects == 1  # 只有 u2 的成功计入

    # 等 u1 僵尸线程落地：它曾占的容量已在离线时释放，迟到成功不得再释放
    time.sleep(0.6)
    assert u1.used_capacity() == 0
    assert u2.used_capacity() == 0
    assert_no_dangling_capacity(service)


def test_cancel_during_long_success_no_double_release(service):
    print("\nINPUT: long success (delay 0.4); cancel mid-flight; zombie lands late")
    u1 = make_unit(service, "u1", ["arm"], 1)
    spec = TaskSpec("zcancel", (StepSpec("s", "arm", timeout=5.0),))
    service.submit(spec, {"s": behavior(("succeeded",), side_effects=1, delay=0.4)})
    import time
    time.sleep(0.05)
    changed = service.cancel("zcancel")[1]
    assert changed is True
    time.sleep(0.55)  # 等僵尸成功结果返回
    view = service.get_task("zcancel")
    dump_task("cancel during long success", view)
    assert view.state == TaskState.CANCELLED
    assert view.steps["s"].state == StepState.CANCELLED
    assert view.steps["s"].side_effects == 0  # 迟到成功不计数
    assert u1.used_capacity() == 0
