"""线程安全的内存状态存储。

InMemoryStore 是任务/步骤状态的唯一事实来源：
- 一把 RLock 保护全部读写；编排引擎的"检查依赖 + 占用容量 + 状态流转"
  临界区通过这把锁串行化，从而在并发提交下也不破坏容量约束与派发顺序。
- store 不持有执行单元，也不做调度决策；它只负责状态机流转、
  幂等键、尝试记录、容量占用账本（占用/释放必须配对，杜绝悬空占用）。
- 对查询返回深拷贝快照（StepView/TaskView），调用方无需持锁。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .models import (
    AttemptRecord,
    FailureCode,
    RejectReason,
    StepSpec,
    StepState,
    StepView,
    TaskState,
    TaskView,
)


@dataclass
class StepRecord:
    """步骤的内部可变记录。"""

    spec: StepSpec
    state: StepState = StepState.PENDING
    attempts: int = 0
    last_reject: RejectReason | None = None
    failure_code: FailureCode | None = None
    failure_detail: str = ""
    assigned_unit: str | None = None
    side_effects: int = 0
    attempt_log: list[AttemptRecord] = field(default_factory=list)


@dataclass
class TaskRecord:
    """任务的内部可变记录。"""

    task_id: str
    order: list[str]                     # 步骤 id 的声明顺序
    steps: dict[str, StepRecord]
    state: TaskState = TaskState.PENDING
    rejection: str = ""                  # 提交被拒/非法声明的说明（幂等返回一致）


class InMemoryStore:
    """并发安全的内存存储。"""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self._tasks: dict[str, TaskRecord] = {}

    # ---------- 提交 / 查询 ----------
    def has_task(self, task_id: str) -> bool:
        with self.lock:
            return task_id in self._tasks

    def create_task(self, task_id: str, specs: list[StepSpec]) -> TaskRecord:
        """登记新任务（调用方须先做声明校验并确认 task_id 不存在）。"""
        with self.lock:
            record = TaskRecord(
                task_id=task_id,
                order=[s.id for s in specs],
                steps={s.id: StepRecord(spec=s) for s in specs},
            )
            self._tasks[task_id] = record
            return record

    def _record(self, task_id: str) -> TaskRecord:
        rec = self._tasks.get(task_id)
        if rec is None:
            raise KeyError(task_id)
        return rec

    def snapshot_task(self, task_id: str) -> TaskView:
        """返回任务对外视图的深拷贝快照。"""
        with self.lock:
            rec = self._record(task_id)
            return TaskView(
                task_id=rec.task_id,
                state=rec.state,
                rejection=rec.rejection,
                steps={
                    sid: StepView(
                        id=r.spec.id,
                        state=r.state,
                        required_capability=r.spec.required_capability,
                        depends_on=r.spec.depends_on,
                        capacity_cost=r.spec.capacity_cost,
                        attempts=r.attempts,
                        max_retries=r.spec.max_retries,
                        last_reject=r.last_reject,
                        failure_code=r.failure_code,
                        failure_detail=r.failure_detail,
                        assigned_unit=r.assigned_unit,
                        side_effects=r.side_effects,
                    )
                    for sid, r in rec.steps.items()
                },
            )

    def list_tasks(self) -> list[str]:
        with self.lock:
            return list(self._tasks.keys())

    # ---------- 状态判定（须持锁）----------
    @staticmethod
    def _deps_met(rec: TaskRecord, step: StepRecord) -> bool:
        return all(
            rec.steps[d].state == StepState.SUCCEEDED for d in step.spec.depends_on
        )

    @staticmethod
    def _recompute_task_state(rec: TaskRecord) -> None:
        """根据步骤状态推导任务状态（任务非终态时调用）。"""
        states = [s.state for s in rec.steps.values()]
        if any(s == StepState.FAILED for s in states):
            rec.state = TaskState.FAILED
        elif all(s == StepState.SUCCEEDED for s in states):
            rec.state = TaskState.SUCCEEDED
        elif any(
            s in (StepState.DISPATCHED, StepState.SUCCEEDED, StepState.FAILED)
            for s in states
        ):
            rec.state = TaskState.RUNNING
        else:
            rec.state = TaskState.PENDING

    # ---------- 调度读取（引擎在临界区内调用）----------
    def dispatchable(self, task_id: str) -> tuple[StepRecord | None, RejectReason | None]:
        """按声明顺序找到第一个 PENDING 步骤并判定其可派发性。

        DISPATCHED（在飞行中）与终态步骤都会被跳过，因此排在在飞步骤之后、
        且依赖已满足的独立步骤仍可被派发；返回 (step, None) 表示可派发。
        (None, reason) 表示当前无可派发步骤：
          - 首遇 PENDING 步骤依赖未满足 -> DEPENDENCY_NOT_MET
          - 任务已取消/终态或无 PENDING 步骤 -> TASK_NOT_ACTIVE
        """
        with self.lock:
            rec = self._record(task_id)
            if rec.state in (TaskState.CANCELLED, TaskState.FAILED, TaskState.SUCCEEDED):
                return None, RejectReason.TASK_NOT_ACTIVE
            target: StepRecord | None = None
            for sid in rec.order:
                st = rec.steps[sid]
                if st.state == StepState.PENDING:
                    target = st
                    break
            if target is None:
                return None, RejectReason.TASK_NOT_ACTIVE
            if not self._deps_met(rec, target):
                target.last_reject = RejectReason.DEPENDENCY_NOT_MET
                return None, RejectReason.DEPENDENCY_NOT_MET
            target.last_reject = None
            return target, None

    def record_rejection(self, task_id: str, step_id: str, reason: RejectReason) -> None:
        with self.lock:
            self._record(task_id).steps[step_id].last_reject = reason

    # ---------- 派发 / 结果流转（引擎在临界区内调用）----------
    def mark_dispatched(
        self, task_id: str, step_id: str, unit_id: str
    ) -> int:
        """步骤进入 DISPATCHED 并返回当前尝试序号（从 0 开始）。"""
        with self.lock:
            step = self._record(task_id).steps[step_id]
            step.state = StepState.DISPATCHED
            step.assigned_unit = unit_id
            attempt = step.attempts
            step.attempts += 1
            self._recompute_task_state(self._record(task_id))
            return attempt

    def record_attempt(self, task_id: str, record: AttemptRecord) -> None:
        with self.lock:
            self._record(task_id).steps[record.step_id].attempt_log.append(record)

    def mark_succeeded(
        self, task_id: str, step_id: str, side_effects: int, attempt: int
    ) -> bool:
        """成功收口。状态/代次不匹配（被取消、抢占或已超时重派）则丢弃结果。

        成功只会发生一次（之后状态为 SUCCEEDED 终态），因此副作用只计一次。
        attempt 为本次结果对应的执行序号（0 起），用于挡掉超时后的僵尸结果。
        """
        with self.lock:
            rec = self._record(task_id)
            step = rec.steps[step_id]
            if step.state != StepState.DISPATCHED or step.attempts != attempt + 1:
                return False
            step.state = StepState.SUCCEEDED
            step.side_effects += side_effects
            step.failure_code = None
            self._recompute_task_state(rec)
            return True

    def mark_failed(
        self,
        task_id: str,
        step_id: str,
        code: FailureCode,
        detail: str,
        attempt: int,
    ) -> bool:
        """失败收口（重试耗尽 / 不可恢复）。代次不符/已取消的飞行结果丢弃。"""
        with self.lock:
            rec = self._record(task_id)
            step = rec.steps[step_id]
            if step.state != StepState.DISPATCHED or step.attempts != attempt + 1:
                return False
            step.state = StepState.FAILED
            step.failure_code = code
            step.failure_detail = detail
            self._recompute_task_state(rec)
            return True

    def revert_to_pending(
        self, task_id: str, step_id: str, detail: str, attempt: int
    ) -> bool:
        """可恢复失败但仍有重试额度：回到 PENDING 等待重新派发。

        被取消/抢占/已超时重派的飞行结果丢弃（返回 False），不复活。
        """
        with self.lock:
            rec = self._record(task_id)
            step = rec.steps[step_id]
            if step.state != StepState.DISPATCHED or step.attempts != attempt + 1:
                return False
            step.state = StepState.PENDING
            step.assigned_unit = None
            step.failure_detail = detail
            self._recompute_task_state(rec)
            return True

    # ---------- 取消 / 离线 ----------
    def cancel_task(self, task_id: str) -> list[tuple[str, str, int]]:
        """取消任务。返回需要释放容量的 (step_id, unit_id, cost) 列表。

        - 任务已终态：返回空列表（调用方据此把取消当幂等操作处理）。
        - 否则所有未成功/未失败的步骤置 CANCELLED；处于 DISPATCHED 的步骤
          其容量占用由引擎按返回列表逐项释放，飞行结果稍后到达时会被丢弃。
        """
        with self.lock:
            rec = self._record(task_id)
            if rec.state in (
                TaskState.CANCELLED,
                TaskState.SUCCEEDED,
                TaskState.FAILED,
            ):
                return []
            occupied: list[tuple[str, str, int]] = []
            for sid in rec.order:
                step = rec.steps[sid]
                if step.state in (StepState.SUCCEEDED, StepState.FAILED):
                    continue
                if step.state == StepState.DISPATCHED and step.assigned_unit:
                    occupied.append(
                        (sid, step.assigned_unit, step.spec.capacity_cost)
                    )
                step.state = StepState.CANCELLED
                step.assigned_unit = None
            rec.state = TaskState.CANCELLED
            return occupied

    def is_cancelled(self, task_id: str) -> bool:
        with self.lock:
            return self._record(task_id).state == TaskState.CANCELLED

    def active_dispatch_owners(self) -> list[tuple[str, str, str, int]]:
        """返回全部 DISPATCHED 步骤的 (task_id, step_id, unit_id, cost)。

        执行单元离线时，引擎据此回收该单元上所有悬空占用。
        """
        with self.lock:
            out: list[tuple[str, str, str, int]] = []
            for tid, rec in self._tasks.items():
                for sid in rec.order:
                    step = rec.steps[sid]
                    if (
                        step.state == StepState.DISPATCHED
                        and step.assigned_unit is not None
                    ):
                        out.append(
                            (tid, sid, step.assigned_unit, step.spec.capacity_cost)
                        )
            return out

    def requeue_offline(self, unit_id: str) -> list[tuple[str, str, int]]:
        """单元离线：其上在飞行的步骤回到 PENDING 等待重新派发。

        返回 (task_id, step_id, cost) 供引擎释放容量账本。
        已完成的步骤不受影响；重派成功后副作用仍只计一次（成功仅一次）。
        """
        with self.lock:
            released: list[tuple[str, str, int]] = []
            for tid, rec in self._tasks.items():
                for sid in rec.order:
                    step = rec.steps[sid]
                    if (
                        step.state == StepState.DISPATCHED
                        and step.assigned_unit == unit_id
                    ):
                        step.state = StepState.PENDING
                        step.assigned_unit = None
                        step.failure_detail = (
                            f"unit {unit_id} went offline; requeued for redispatch"
                        )
                        released.append((tid, sid, step.spec.capacity_cost))
                        self._recompute_task_state(rec)
            return released
