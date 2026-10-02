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
    DISPATCHED --(让位抢占)--> PREEMPTED -> DISPATCHED（容量腾出后自动重跑）
    PENDING/PREEMPTED/DISPATCHED --(任务取消/失败终态连带收口)--> CANCELLED
    """

    PENDING = "PENDING"
    DISPATCHED = "DISPATCHED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    PREEMPTED = "PREEMPTED"


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


class RejectReason(str, Enum):
    """派发请求被拒绝的可区分原因。"""

    DEPENDENCY_NOT_MET = "DEPENDENCY_NOT_MET"
    CAPABILITY_MISMATCH = "CAPABILITY_MISMATCH"
    INSUFFICIENT_CAPACITY = "INSUFFICIENT_CAPACITY"
    TASK_NOT_ACTIVE = "TASK_NOT_ACTIVE"
    NO_CANDIDATE_UNIT = "NO_CANDIDATE_UNIT"


class FailureCode(str, Enum):
    """步骤进入 FAILED 终态的可区分原因。"""

    RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED"
    NON_RECOVERABLE = "NON_RECOVERABLE"


class Priority(str, Enum):
    """任务提交优先级。

    不携带 priority 的旧请求一律按 NORMAL 处理，默认行为与旧版本完全一致。
    rank 仅用于内部排序：高优先级可抢占在飞的低优先级步骤；同级先来先服务。
    """

    HIGH = "HIGH"
    NORMAL = "NORMAL"
    LOW = "LOW"

    @property
    def rank(self) -> int:
        return _PRIORITY_RANK[self]


_PRIORITY_RANK: dict["Priority", int] = {
    Priority.HIGH: 3,
    Priority.NORMAL: 2,
    Priority.LOW: 1,
}


class PreemptReason(str, Enum):
    """步骤被让出（DISPATCHED -> PREEMPTED）的可区分原因。

    让位不是执行失败：不进 FAILED、不消耗重试次数、副作用不受影响，
    后续容量腾出时自动重新派发。
    """

    PREEMPT_FOR_HIGHER_PRIORITY = "PREEMPT_FOR_HIGHER_PRIORITY"


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

    priority 省略时取 NORMAL：旧的提交写法无需改动，行为与旧版本一致。
    """

    task_id: str
    steps: tuple[StepSpec, ...]
    priority: Priority = Priority.NORMAL


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
    last_reject: RejectReason | None = None
    failure_code: FailureCode | None = None
    failure_detail: str = ""
    assigned_unit: str | None = None
    side_effects: int = 0
    # 让位抢占：被让出次数 + 最近一次让位原因（None 表示从未被让出过）
    preempt_count: int = 0
    last_preempt_reason: PreemptReason | None = None
    preempt_detail: str = ""


@dataclass
class TaskView:
    """任务的对外状态视图。"""

    task_id: str
    state: TaskState
    steps: dict[str, StepView] = field(default_factory=dict)
    rejection: str = ""
    priority: Priority = Priority.NORMAL
    # 提交序号（全局限单调递增）：同优先级下据此先来先服务，
    # 抢占重排不改变它，因此排队顺序始终稳定、可解释。
    submit_seq: int = 0


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
    # 事件主体的优先级（preempt/preempted/dispatch 等），便于日志解释派发顺序
    priority: str = ""
