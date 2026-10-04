"""核心类型定义：任务/步骤状态机、约束与对外视图。

本模块只包含数据结构与枚举，是整个控制面的"词汇表"：
- StepState / TaskState：步骤与任务的状态机
- RejectReason / FailureCode：派发被拒原因 与 失败终态原因（可区分）
- StepSpec / TaskSpec：任务提交声明
- StepView / TaskView：对外状态查询视图
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class StepState(str, Enum):
    r"""步骤状态机。

    PENDING -> DISPATCHED -> SUCCEEDED
                       \--> FAILED (不可恢复 / 重试耗尽)
    PENDING 在依赖未满足或派发被拒时保持 PENDING（带最近一次拒绝原因）。
    DISPATCHED --(被高优先级抢占)--> PENDING（last_reject=PREEMPTED，
    不算失败、不消耗重试次数，容量释放后等待重新派发）。
    PENDING/DISPATCHED --(任务取消)--> CANCELLED（容量立即释放）。
    """

    PENDING = "PENDING"
    DISPATCHED = "DISPATCHED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TaskState(str, Enum):
    """任务状态机。

    PENDING(有未完成步骤) -> RUNNING(至少一个步骤被派发过)
    RUNNING/PENDING -> SUCCEEDED(全部步骤成功)
    任意非终态 -> CANCELLED(用户取消)
    任意非终态 -> FAILED(某个步骤进入 FAILED)
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class Priority(str, Enum):
    """任务优先级。提交时不携带则按 MEDIUM（普通）处理，与旧行为一致。

    调度顺序：优先级高者先派；同优先级按提交先后（FIFO）。
    高优先级步骤等容量时，可抢占（让位）严格更低优先级的在飞步骤。
    """

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

    @property
    def rank(self) -> int:
        """数值越小优先级越高（用于排序与抢占比较）。"""
        return _PRIORITY_RANK[self]


_PRIORITY_RANK = {
    Priority.HIGH: 0,
    Priority.MEDIUM: 1,
    Priority.LOW: 2,
}


class RejectReason(str, Enum):
    """派发请求被拒绝/步骤回到 PENDING 的可区分原因。"""

    DEPENDENCY_NOT_MET = "DEPENDENCY_NOT_MET"
    CAPABILITY_MISMATCH = "CAPABILITY_MISMATCH"
    INSUFFICIENT_CAPACITY = "INSUFFICIENT_CAPACITY"
    TASK_NOT_ACTIVE = "TASK_NOT_ACTIVE"
    NO_CANDIDATE_UNIT = "NO_CANDIDATE_UNIT"
    # 在飞步骤被更高优先级步骤抢占而让位：不算失败、不消耗重试次数，
    # 容量释放后回到 PENDING 等待重新派发。
    PREEMPTED = "PREEMPTED"
    # 在飞步骤因执行单元离线被回收：不算失败、不消耗重试次数，容量释放后
    # 回到 PENDING，等待派发到其它在线单元。
    UNIT_OFFLINE_REQUEUED = "UNIT_OFFLINE_REQUEUED"


class FailureCode(str, Enum):
    """步骤进入 FAILED 终态的可区分原因。"""

    RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED"
    NON_RECOVERABLE = "NON_RECOVERABLE"


@dataclass(frozen=True)
class StepSpec:
    """单个步骤的声明。

    depends_on: 必须先 SUCCEEDED 的其它步骤 id（DAG，按声明顺序校验）。
    required_capability: 执行单元必须具备的能力标识。
    capacity_cost: 派发时占用执行单元的容量份额（必须为正整数）。
    max_retries: 超时/异常退出后的最大重试次数（不含首次执行）。
    """

    id: str
    required_capability: str
    depends_on: tuple[str, ...] = ()
    capacity_cost: int = 1
    timeout: float = 1.0
    max_retries: int = 0


@dataclass(frozen=True)
class TaskSpec:
    """一次任务提交的声明。

    priority: 任务级优先级（其所有步骤继承）。缺省 MEDIUM，
              与未携带优先级的旧提交行为完全一致。
    """

    task_id: str
    steps: tuple[StepSpec, ...]
    priority: Priority = Priority.MEDIUM


@dataclass
class AttemptRecord:
    """一次执行尝试的结果记录（用于幂等的副作用统计与可复现的判定依据）。"""

    step_id: str
    attempt: int
    unit_id: str
    outcome: str  # "succeeded" | "timeout" | "crashed" | "error"
    side_effect_count: int = 0
    detail: str = ""


@dataclass
class StepView:
    """步骤的对外状态视图。"""

    id: str
    state: StepState
    required_capability: str
    depends_on: tuple[str, ...]
    capacity_cost: int
    attempts: int = 0
    max_retries: int = 0
    retries_used: int = 0
    preemptions: int = 0
    last_reject: RejectReason | None = None
    failure_code: FailureCode | None = None
    failure_detail: str = ""
    assigned_unit: str | None = None
    side_effects: int = 0


@dataclass
class TaskView:
    """任务的对外状态视图。"""

    task_id: str
    state: TaskState
    steps: dict[str, StepView] = field(default_factory=dict)
    rejection: str = ""
    priority: Priority = Priority.MEDIUM


@dataclass
class UnitConfig:
    """执行单元（进程内模拟 worker）的配置。

    capabilities: 该单元具备的能力集合。
    capacity: 总容量；DISPATCHED 状态的步骤按 capacity_cost 占用，
              完成/取消/抢占/离线时必须释放。
    """

    unit_id: str
    capabilities: frozenset[str]
    capacity: int


@dataclass(frozen=True)
class EngineEvent:
    """引擎判定事件（测试日志打印"输入事件 + 判定依据"用，亦可用于排障）。

    seq: 全局单调序号，保证事件顺序可复现。
    kind: submit / reject / dispatch / success / retry / fail /
          cancel / offline / timeout 等。
    task_id / step_id / unit_id: 事件主体。
    reason: 判定依据（拒绝原因、失败码、释放说明等）。
    detail: 人类可读的补充信息。
    """

    seq: int
    kind: str
    task_id: str = ""
    step_id: str = ""
    unit_id: str = ""
    reason: str = ""
    detail: str = ""
