"""pytest 共享夹具与日志配置。

每个用例通过 caplog/print 输出"输入事件"（提交/行为脚本/取消/离线）
与"判定依据"（引擎 EngineEvent：派发/拒绝原因/重试/收口/容量释放），
保证状态与失败原因可区分、可复现。
"""

from __future__ import annotations

import logging
import sys

import pytest

from fleet.executor import StepBehavior, UnitRegistry
from fleet.models import UnitConfig
from fleet.service import FleetService
from fleet.store import InMemoryStore
from fleet.engine import OrchestrationEngine


@pytest.fixture
def service():
    """一个干净的控制面（无默认单元，由用例自行装配）。"""
    store, registry = InMemoryStore(), UnitRegistry()
    engine = OrchestrationEngine(store, registry)
    svc = FleetService(store=store, registry=registry, engine=engine)
    yield svc
    svc.shutdown()


@pytest.fixture
def default_service():
    """带三个默认模拟单元的控制面。"""
    from fleet.api import _build_service

    svc = _build_service()
    yield svc
    svc.shutdown()


def make_unit(service, unit_id, capabilities, capacity):
    return service.register_unit(
        UnitConfig(unit_id, frozenset(capabilities), capacity)
    )


def behavior(outcomes=("succeeded",), *, side_effects=1, delay=0.0,
             fail_detail="simulated non-recoverable error"):
    return StepBehavior(
        outcomes=tuple(outcomes), side_effects=side_effects,
        delay=delay, fail_detail=fail_detail,
    )


def wait_terminal(service, task_id, timeout=3.0):
    assert service.wait_idle(timeout), f"task {task_id} did not reach terminal state"
    return service.get_task(task_id)


def dump_task(title, view) -> None:
    """打印任务状态视图（测试日志的一部分）。"""
    print(
        f"\n[{title}] task={view.task_id} state={view.state.value} "
        f"priority={view.priority.value} submit_seq={view.submit_seq}"
    )
    for sid, s in view.steps.items():
        print(
            f"  step={sid} state={s.state.value} attempt={s.attempts} "
            f"reject={s.last_reject.value if s.last_reject else '-'} "
            f"failure={s.failure_code.value if s.failure_code else '-'} "
            f"unit={s.assigned_unit} effects={s.side_effects} "
            f"preempt_count={s.preempt_count} "
            f"preempt_reason={s.last_preempt_reason.value if s.last_preempt_reason else '-'} "
            f"detail={s.failure_detail or s.preempt_detail!r}"
        )


def assert_no_dangling_capacity(service) -> None:
    for u in service.registry.all_units():
        used = u.used_capacity()
        print(f"  capacity unit={u.unit_id} used={used}/{u.capacity}")
        assert used == 0, f"dangling capacity on {u.unit_id}: {used}"


def pytest_configure(config):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    root = logging.getLogger("fleet.engine")
    root.setLevel(logging.INFO)
    root.addHandler(handler)
