"""进程内模拟执行单元。

设计要点
--------
- ExecutionUnit 是一个"机器人执行单元"的进程内模拟：声明能力集合与容量，
  支持占用（dispatch）/释放（release），容量不足时拒绝占用。
- 执行过程不 sleep、不接外部服务：结果由步骤携带的 StepBehavior 脚本
  按第几次尝试（attempt 从 0 起）决定，使超时/崩溃/成功等场景完全可复现。
- 模拟副作用由行为脚本决定，只有真正执行到成功的那一次才会计数；
  重试链路上同一逻辑步骤的多次尝试，副作用绝不重复计入
  （成功一次即终态，成功之后不会再有尝试）。
- UnitRegistry 负责单元登记/上下线，以及按能力 + 容量筛选候选单元；
  单元离线会被标记 offline，其占用由上层引擎负责回收。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from threading import Lock

from .errors import (
    NonRecoverableExecError,
    RecoverableExecError,
)
from .models import UnitConfig

# 尝试结果常量
OUT_SUCCEEDED = "succeeded"
OUT_TIMEOUT = "timeout"
OUT_CRASHED = "crashed"
OUT_ERROR = "error"


@dataclass(frozen=True)
class StepBehavior:
    """步骤的可复现执行脚本。

    outcomes: 按尝试序号取值的结果序列；序号超出长度时取最后一个。
              例如 ("timeout", "crashed", "succeeded") 表示：
              第 0 次超时、第 1 次异常退出、第 2 次成功。
    side_effects: 成功那一次产生的副作用数量（只有成功才计入）。
    fail_detail: error（不可恢复）时携带的说明。
    """

    outcomes: tuple[str, ...] = ("succeeded",)
    side_effects: int = 1
    fail_detail: str = "simulated non-recoverable error"
    delay: float = 0.0

    def outcome_at(self, attempt: int) -> str:
        idx = min(attempt, len(self.outcomes) - 1)
        return self.outcomes[idx]


class ExecutionUnit:
    """单个进程内执行单元（线程安全）。"""

    def __init__(self, config: UnitConfig) -> None:
        self._config = config
        self._lock = Lock()
        self._used = 0
        self._online = True

    # ---- 静态属性（配置不可变）----
    @property
    def unit_id(self) -> str:
        return self._config.unit_id

    @property
    def capabilities(self) -> frozenset[str]:
        return self._config.capabilities

    @property
    def capacity(self) -> int:
        return self._config.capacity

    # ---- 运行时状态（持锁访问）----
    def is_online(self) -> bool:
        with self._lock:
            return self._online

    def used_capacity(self) -> int:
        with self._lock:
            return self._used

    def free_capacity(self) -> int:
        with self._lock:
            return self._config.capacity - self._used

    def can_accept(self, capability: str, cost: int) -> bool:
        """能力匹配且剩余容量足够（不改变状态）。"""
        if cost <= 0:
            return False
        with self._lock:
            return (
                self._online
                and capability in self._config.capabilities
                and self._used + cost <= self._config.capacity
            )

    def acquire(self, capability: str, cost: int) -> bool:
        """尝试占用容量；能力不匹配/离线/容量不足返回 False（不抛异常）。"""
        if cost <= 0:
            return False
        with self._lock:
            if not self._online:
                return False
            if capability not in self._config.capabilities:
                return False
            if self._used + cost > self._config.capacity:
                return False
            self._used += cost
            return True

    def release(self, cost: int) -> None:
        """释放容量。步骤完成、取消、抢占或单元离线回收时必须调用。"""
        with self._lock:
            if cost > self._used:
                # 内部一致性保护：绝不允许悬空/超额释放
                raise RuntimeError(
                    f"unit {self.unit_id}: release {cost} exceeds used {self._used}"
                )
            self._used -= cost

    def mark_offline(self) -> int:
        """标记离线并返回当前被占用容量（供上层按占用记录逐项回收）。"""
        with self._lock:
            self._online = False
            return self._used

    def mark_online(self) -> None:
        with self._lock:
            self._online = True

    # ---- 执行模拟 ----
    def run(self, step_id: str, attempt: int, behavior: StepBehavior) -> int:
        """同步执行一次尝试，返回本次成功产生的副作用数。

        - succeeded: 返回 side_effects（>=0）
        - timeout/crashed: 抛 RecoverableExecError（可触发有界重试）
        - error: 抛 NonRecoverableExecError（直接终态 FAILED）
        本方法不真正 sleep；timeout 只是结果语义。
        """
        outcome = behavior.outcome_at(attempt)
        if behavior.delay > 0:
            # 模拟执行耗时：取消/抢占/超时在这段窗口内发生
            time.sleep(behavior.delay)
        if outcome == OUT_SUCCEEDED:
            return behavior.side_effects
        if outcome == OUT_TIMEOUT:
            raise RecoverableExecError(
                f"step {step_id} attempt {attempt} timed out on unit {self.unit_id}"
            )
        if outcome == OUT_CRASHED:
            raise RecoverableExecError(
                f"step {step_id} attempt {attempt} crashed on unit {self.unit_id}"
            )
        raise NonRecoverableExecError(
            f"{behavior.fail_detail} (step={step_id}, attempt={attempt}, "
            f"unit={self.unit_id})"
        )


class UnitRegistry:
    """执行单元注册表（线程安全）。"""

    def __init__(self) -> None:
        self._lock = Lock()
        self._units: dict[str, ExecutionUnit] = {}

    def register(self, unit: ExecutionUnit) -> None:
        with self._lock:
            if unit.unit_id in self._units:
                raise ValueError(f"unit {unit.unit_id} already registered")
            self._units[unit.unit_id] = unit

    def get(self, unit_id: str) -> ExecutionUnit:
        with self._lock:
            return self._units[unit_id]

    def all_units(self) -> list[ExecutionUnit]:
        with self._lock:
            return list(self._units.values())

    def mark_offline(self, unit_id: str) -> ExecutionUnit:
        unit = self.get(unit_id)
        unit.mark_offline()
        return unit

    def mark_online(self, unit_id: str) -> None:
        self.get(unit_id).mark_online()

    def candidates(self, capability: str, cost: int) -> list[ExecutionUnit]:
        """返回当前满足能力与容量约束的在线单元，按 unit_id 排序保证派发顺序确定。"""
        with self._lock:
            units = list(self._units.values())
        return sorted(
            (u for u in units if u.can_accept(capability, cost)),
            key=lambda u: u.unit_id,
        )
