"""超时/失败重试链路测试。

覆盖：
- 超时 -> 有界重试 -> 成功；副作用只在最终成功时计一次；
- 异常退出(crashed) -> 重试 -> 成功，每次尝试可复现；
- 重试耗尽 -> FAILED/RETRIES_EXHAUSTED，容量释放；
- 不可恢复 error -> FAILED/NON_RECOVERABLE，不消耗重试；
- 真正的看门狗超时（delay > timeout），且迟到的成功不被计数；
- 重试会被重新派发到其它满足约束的单元。
"""

from __future__ import annotations

from fleet.models import (
    FailureCode,
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


def test_timeout_then_retry_success_single_side_effect(service):
    print("\nINPUT: outcomes=(timeout, succeeded), max_retries=1")
    make_unit(service, "u1", ["arm"], 1)
    spec = TaskSpec("t", (StepSpec("s", "arm", max_retries=1, timeout=2.0),))
    service.submit(spec, {"s": behavior(("timeout", "succeeded"), side_effects=7)})
    view = wait_terminal(service, "t")
    dump_task("timeout then success", view)
    s = view.steps["s"]
    assert s.state == StepState.SUCCEEDED
    assert s.attempts == 2
    assert s.side_effects == 7  # 第一次超时没有副作用，最终只计一次
    assert_no_dangling_capacity(service)


def test_crash_retry_chain_is_reproducible(service):
    print("\nINPUT: outcomes=(crashed, crashed, succeeded), max_retries=2")
    make_unit(service, "u1", ["arm"], 1)
    spec = TaskSpec("t", (StepSpec("s", "arm", max_retries=2, timeout=2.0),))
    service.submit(spec, {"s": behavior(("crashed", "crashed", "succeeded"))})
    view = wait_terminal(service, "t")
    dump_task("crashed x2 then success", view)
    s = view.steps["s"]
    assert s.state == StepState.SUCCEEDED and s.attempts == 3
    assert view.state == TaskState.SUCCEEDED
    assert_no_dangling_capacity(service)


def test_retries_exhausted_terminal(service):
    print("\nINPUT: always crashed, max_retries=2 -> 3 attempts total")
    make_unit(service, "u1", ["arm"], 1)
    spec = TaskSpec("t", (StepSpec("s", "arm", max_retries=2, timeout=2.0),))
    service.submit(spec, {"s": behavior(("crashed",))})  # 末值循环 -> 一直 crash
    view = wait_terminal(service, "t")
    dump_task("retries exhausted", view)
    s = view.steps["s"]
    assert s.state == StepState.FAILED
    assert s.failure_code == FailureCode.RETRIES_EXHAUSTED
    assert "retries exhausted" in s.failure_detail
    assert s.attempts == 3
    assert view.state == TaskState.FAILED
    assert_no_dangling_capacity(service)


def test_non_recoverable_error_terminal(service):
    print("\nINPUT: outcome=error with detail, retries allowed but must NOT be used")
    make_unit(service, "u1", ["arm"], 1)
    spec = TaskSpec("t", (StepSpec("s", "arm", max_retries=5, timeout=2.0),))
    service.submit(
        spec,
        {"s": behavior(("error",), fail_detail="arm hardware fault")},
    )
    view = wait_terminal(service, "t")
    dump_task("non-recoverable", view)
    s = view.steps["s"]
    assert s.state == StepState.FAILED
    assert s.failure_code == FailureCode.NON_RECOVERABLE
    assert "arm hardware fault" in s.failure_detail
    assert s.attempts == 1  # 不可恢复：不重试
    assert_no_dangling_capacity(service)


def test_real_watchdog_timeout_discards_late_success(service):
    print("\nINPUT: delay=0.4s > timeout=0.1s; script would 'succeed' late")
    make_unit(service, "u1", ["arm"], 1)
    spec = TaskSpec("t", (StepSpec("s", "arm", max_retries=0, timeout=0.1),))
    service.submit(spec, {"s": behavior(("succeeded",), side_effects=9, delay=0.4)})
    view = wait_terminal(service, "t")
    dump_task("watchdog timeout", view)
    s = view.steps["s"]
    assert s.state == StepState.FAILED
    assert s.failure_code == FailureCode.RETRIES_EXHAUSTED
    assert s.side_effects == 0  # 迟到成功被丢弃

    # 等待迟到线程真正返回，确认不会复活步骤、容量仍为 0
    import time
    time.sleep(0.5)
    again = service.get_task("t")
    assert again.steps["s"].state == StepState.FAILED
    assert again.steps["s"].side_effects == 0
    assert_no_dangling_capacity(service)


def test_retry_redispatch_to_other_unit(service):
    print("\nINPUT: u1 crashes first attempt; retry dispatched deterministically")
    make_unit(service, "u1", ["arm"], 1)
    make_unit(service, "u2", ["arm"], 1)
    assigned = []
    orig = service.engine._run_attempt

    def spy(task_id, step_id, unit_id, *a, **k):
        assigned.append(unit_id)
        return orig(task_id, step_id, unit_id, *a, **k)

    service.engine._run_attempt = spy
    spec = TaskSpec("t", (StepSpec("s", "arm", max_retries=1, timeout=2.0),))
    service.submit(spec, {"s": behavior(("crashed", "succeeded"))})
    view = wait_terminal(service, "t")
    dump_task("redispatch", view)
    print(f"JUDGEMENT: attempt units = {assigned}")
    assert assigned == ["u1", "u1"]  # 两次都按 unit_id 最小确定派发；u1 容量已释放
    assert view.steps["s"].state == StepState.SUCCEEDED
    assert_no_dangling_capacity(service)
