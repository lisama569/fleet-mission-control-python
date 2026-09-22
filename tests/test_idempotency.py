"""幂等提交与状态视图自洽测试。

覆盖：
- 同一 task_id 重复提交不产生重复步骤/不重复执行（created=False，步骤数不变）；
- 并发重复提交（多线程同时提交同一标识）只产生一次创建、一份步骤；
- 取消幂等；
- 步骤/任务状态视图始终自洽（终态一致性、尝试次数与副作用不重复计数）。
"""

from __future__ import annotations

import threading

from fleet.models import (
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


def test_duplicate_submit_does_not_duplicate(service):
    print("\nINPUT: submit same task_id twice (second with extra steps!)")
    make_unit(service, "u1", ["arm"], 5)
    spec1 = TaskSpec("dup", (StepSpec("a", "arm", timeout=2.0),))
    spec2 = TaskSpec("dup", (
        StepSpec("a", "arm"),
        StepSpec("b", "arm"),
        StepSpec("c", "arm"),
    ))
    v1, c1 = service.submit(spec1, {"a": behavior(delay=0.05)})
    v2, c2 = service.submit(spec2)  # 声明不同也必须被忽略
    print(f"JUDGEMENT: created flags = {c1}, {c2}; step count = {len(v2.steps)}")
    assert c1 is True and c2 is False
    assert set(v2.steps) == {"a"}

    view = wait_terminal(service, "dup")
    dump_task("duplicate submit", view)
    assert view.state == TaskState.SUCCEEDED
    assert view.steps["a"].attempts == 1
    assert view.steps["a"].side_effects == 1
    assert_no_dangling_capacity(service)


def test_concurrent_duplicate_submit_creates_once(default_service):
    print("\nINPUT: 20 threads submit the same task_id concurrently")
    spec = TaskSpec("race", (
        StepSpec("x", "arm", timeout=2.0),
        StepSpec("y", "arm", depends_on=("x",), timeout=2.0),
    ))
    results: list[tuple] = []
    barrier = threading.Barrier(20)

    def worker():
        barrier.wait()
        results.append(default_service.submit(spec, {
            "x": behavior(delay=0.02),
            "y": behavior(delay=0.02),
        }))

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    created = [r[1] for r in results]
    print(f"JUDGEMENT: created=True count = {sum(created)} / 20")
    assert sum(created) == 1
    view = wait_terminal(default_service, "race")
    dump_task("concurrent duplicate", view)
    assert len(view.steps) == 2
    assert view.state == TaskState.SUCCEEDED
    # 每个步骤只执行一次：并发重复提交没有造成重复执行
    assert view.steps["x"].attempts == 1 and view.steps["y"].attempts == 1
    assert view.steps["x"].side_effects == 1
    # 只有一个任务
    assert default_service.list_tasks().count("race") == 1


def test_cancel_is_idempotent(service):
    print("\nINPUT: cancel a long-running step, then cancel again")
    make_unit(service, "u1", ["arm"], 1)
    spec = TaskSpec("cancel", (StepSpec("a", "arm", timeout=5.0),))
    service.submit(spec, {"a": behavior(delay=0.3)})
    import time
    time.sleep(0.05)  # 确保已经派发

    v1, changed1 = service.cancel("cancel")
    print(f"JUDGEMENT: first cancel changed={changed1} state={v1.state.value}")
    v2, changed2 = service.cancel("cancel")
    print(f"JUDGEMENT: second cancel changed={changed2} state={v2.state.value}")
    assert changed1 is True and changed2 is False
    assert v1.state == v2.state == TaskState.CANCELLED
    assert v2.steps["a"].state == StepState.CANCELLED

    # 等待飞行线程落地：其迟到结果必须被丢弃，副作用不计数
    time.sleep(0.4)
    view = service.get_task("cancel")
    dump_task("cancelled (stale result discarded)", view)
    assert view.steps["a"].side_effects == 0
    assert view.steps["a"].state == StepState.CANCELLED
    assert_no_dangling_capacity(service)


def test_state_view_self_consistent(service):
    print("\nINPUT: diamond DAG a -> b,c -> d, all succeed")
    make_unit(service, "u1", ["arm"], 4)
    spec = TaskSpec("diamond", (
        StepSpec("a", "arm", timeout=2.0),
        StepSpec("b", "arm", depends_on=("a",), timeout=2.0),
        StepSpec("c", "arm", depends_on=("a",), timeout=2.0),
        StepSpec("d", "arm", depends_on=("b", "c"), timeout=2.0),
    ))
    service.submit(spec, {sid: behavior(delay=0.02) for sid in "abcd"})
    view = wait_terminal(service, "diamond")
    dump_task("diamond", view)
    assert view.state == TaskState.SUCCEEDED
    for s in view.steps.values():
        assert s.state == StepState.SUCCEEDED
        assert s.failure_code is None and s.last_reject is None
        assert s.side_effects == 1 and s.attempts == 1
    assert_no_dangling_capacity(service)
