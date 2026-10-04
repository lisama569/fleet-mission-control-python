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
    Priority,
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
    retries_used: int = 0      # 已消耗的重试次数（抢占/离线让位不消耗）
    preemptions: int = 0       # 被高优先级抢占让位的次数
    dispatch_seq: int = 0      # 全局派发序号（抢占选 victim 时确定顺序用）
    last_reject: RejectReason | None = None
    failure_code: FailureCode | None = None
    failure_detail: str = ""
    assigned_unit: str | None = None
    side_effects: int = 0
    attempt_log: list[AttemptRecord] = field(default_factory=list)


@dataclass(frozen=True)
class CancelOutcome:
    """取消操作的原子结果（store 临界区内得出，事实即最终视图）。

    changed: 本次是否真的把一个非终态任务改成了 CANCELLED。
             False 仅发生在任务此前已处于终态（成功/失败/已取消），
             即幂等无操作；此时 cancelled_steps/released 必为空。
    cancelled_steps: 本次被置为 CANCELLED 的非终态步骤 id
                     （排队中 PENDING、被抢占等待重派 PENDING、在飞 DISPATCHED）。
    released: 本次需要释放容量的 (step_id, unit_id, cost)——
              仅包含取消前确实占着执行容量的 DISPATCHED 步骤。
    prior_state: 取消前的任务状态（事件日志交代真实原因用）。
    """

    changed: bool
    cancelled_steps: tuple[str, ...]
    released: tuple[tuple[str, str, int], ...]
    prior_state: TaskState | None = None

    @property
    def inflight_released(self) -> int:
        """取消前在飞、容量被立即释放的步骤数。"""
        return len(self.released)

    @property
    def waiting_cancelled(self) -> int:
        """取消前并未占容量（排队/让位等待）的被取消步骤数。"""
        return len(self.cancelled_steps) - len(self.released)


@dataclass
class TaskRecord:
    """任务的内部可变记录。"""

    task_id: str
    order: list[str]                     # 步骤 id 的声明顺序
    steps: dict[str, StepRecord]
    state: TaskState = TaskState.PENDING
    rejection: str = ""                  # 提交被拒/非法声明的说明（幂等返回一致）
    priority: Priority = Priority.MEDIUM
    submit_seq: int = 0                  # 全局提交序号（同优先级 FIFO 的依据）


class InMemoryStore:
    """并发安全的内存存储。"""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self._tasks: dict[str, TaskRecord] = {}
        self._submit_seq = 0

    # ---------- 提交 / 查询 ----------
    def has_task(self, task_id: str) -> bool:
        with self.lock:
            return task_id in self._tasks

    def create_task(
        self,
        task_id: str,
        specs: list[StepSpec],
        priority: Priority = Priority.MEDIUM,
    ) -> TaskRecord:
        """登记新任务（调用方须先做声明校验并确认 task_id 不存在）。

        提交序号在此单调分配，是同优先级 FIFO 派发顺序的唯一依据。
        """
        with self.lock:
            self._submit_seq += 1
            record = TaskRecord(
                task_id=task_id,
                order=[s.id for s in specs],
                steps={s.id: StepRecord(spec=s) for s in specs},
                priority=priority,
                submit_seq=self._submit_seq,
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
                priority=rec.priority,
                steps={
                    sid: StepView(
                        id=r.spec.id,
                        state=r.state,
                        required_capability=r.spec.required_capability,
                        depends_on=r.spec.depends_on,
                        capacity_cost=r.spec.capacity_cost,
                        attempts=r.attempts,
                        max_retries=r.spec.max_retries,
                        retries_used=r.retries_used,
                        preemptions=r.preemptions,
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

    def ordered_task_ids(self) -> list[str]:
        """按调度顺序返回任务 id：优先级高者在前，同优先级按提交序号 FIFO。

        提交序号在任务创建时单调分配且不变，因此抢占重排队后顺序仍稳定可复现。
        """
        with self.lock:
            return [
                rec.task_id
                for rec in sorted(
                    self._tasks.values(),
                    key=lambda r: (r.priority.rank, r.submit_seq),
                )
            ]

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
            # PREEMPTED 保持"粘滞"：被让位的步骤在重新派发前一直展示该原因，
            # 不会被随后例行的容量不足拒绝覆盖，保证让位原因可查询、可区分。
            if target.last_reject != RejectReason.PREEMPTED:
                target.last_reject = None
            return target, None

    def record_rejection(self, task_id: str, step_id: str, reason: RejectReason) -> None:
        with self.lock:
            step = self._record(task_id).steps[step_id]
            # 被抢占让位的步骤等待容量重派是预期行为：保留 PREEMPTED 作为
            # 可区分的等待原因，不被 INSUFFICIENT_CAPACITY 覆盖。
            if step.last_reject == RejectReason.PREEMPTED and (
                reason == RejectReason.INSUFFICIENT_CAPACITY
            ):
                return
            step.last_reject = reason

    # ---------- 派发 / 结果流转（引擎在临界区内调用）----------
    def mark_dispatched(
        self, task_id: str, step_id: str, unit_id: str, dispatch_seq: int = 0
    ) -> int:
        """步骤进入 DISPATCHED 并返回当前尝试序号（从 0 开始）。"""
        with self.lock:
            step = self._record(task_id).steps[step_id]
            step.state = StepState.DISPATCHED
            step.assigned_unit = unit_id
            step.dispatch_seq = dispatch_seq
            step.last_reject = None  # 派发成功即清除等待原因（含 PREEMPTED）
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

    # ---------- 取消 / 离线 ----------
    def cancel_task(self, task_id: str) -> CancelOutcome:
        """取消任务，返回原子的取消结果。

        - 任务此前已终态（CANCELLED/SUCCEEDED/FAILED）：changed=False、
          清单为空——幂等无操作，不报错、不产生任何额外变化。
        - 否则 changed=True：所有未成功/未失败的步骤（含排队中 PENDING、
          被抢占等待重派 PENDING、在飞 DISPATCHED）一律置 CANCELLED，
          任务置 CANCELLED。其中取消前处于 DISPATCHED 的步骤才实际占着
          执行容量，计入 released，由引擎逐项且仅释放一次；PENDING 步骤
          本就不占容量，不出现在 released 中（绝不虚释放）。
        飞行尝试稍后到达时会被状态/代次守卫丢弃，不会复活步骤。
        """
        with self.lock:
            rec = self._record(task_id)
            prior_state = rec.state
            if prior_state in (
                TaskState.CANCELLED,
                TaskState.SUCCEEDED,
                TaskState.FAILED,
            ):
                return CancelOutcome(
                    changed=False,
                    cancelled_steps=(),
                    released=(),
                    prior_state=prior_state,
                )
            cancelled: list[str] = []
            occupied: list[tuple[str, str, int]] = []
            for sid in rec.order:
                step = rec.steps[sid]
                if step.state in (StepState.SUCCEEDED, StepState.FAILED):
                    continue
                if step.state == StepState.DISPATCHED and step.assigned_unit:
                    # 取消前确实占着执行容量：立即回收，且只回收这一次。
                    occupied.append(
                        (sid, step.assigned_unit, step.spec.capacity_cost)
                    )
                step.state = StepState.CANCELLED
                step.assigned_unit = None
                cancelled.append(sid)
            rec.state = TaskState.CANCELLED
            return CancelOutcome(
                changed=True,
                cancelled_steps=tuple(cancelled),
                released=tuple(occupied),
                prior_state=prior_state,
            )

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

    # ---------- 优先级抢占（引擎在临界区内调用）----------
    def priority_rank(self, task_id: str) -> int:
        """任务优先级数值（越小越高），供引擎做抢占比较。"""
        with self.lock:
            return self._record(task_id).priority.rank

    def task_priority(self, task_id: str) -> Priority:
        """任务优先级枚举值（事件日志/视图解释用）。"""
        with self.lock:
            return self._record(task_id).priority

    def inflight_on_unit(self, unit_id: str) -> list[tuple[str, str, int, int, int]]:
        """返回某单元上全部在飞步骤的
        (task_id, step_id, cost, priority_rank, dispatch_seq)，供抢占决策。"""
        with self.lock:
            out: list[tuple[str, str, int, int, int]] = []
            for tid, rec in self._tasks.items():
                for sid in rec.order:
                    step = rec.steps[sid]
                    if (
                        step.state == StepState.DISPATCHED
                        and step.assigned_unit == unit_id
                    ):
                        out.append(
                            (
                                tid,
                                sid,
                                step.spec.capacity_cost,
                                rec.priority.rank,
                                step.dispatch_seq,
                            )
                        )
            return out

    def preempt_step(self, task_id: str, step_id: str, detail: str) -> int | None:
        """抢占让位：在飞步骤回到 PENDING，记录可区分原因 PREEMPTED。

        不算执行失败（failure_code 保持 None）、不消耗重试次数
        （retries_used 不变）、副作用不计；飞行中尝试的迟到结果会被
        状态/代次守卫丢弃。返回释放的容量成本；步骤不在 DISPATCHED 返回 None。
        """
        with self.lock:
            rec = self._record(task_id)
            step = rec.steps[step_id]
            if step.state != StepState.DISPATCHED:
                return None
            step.state = StepState.PENDING
            step.assigned_unit = None
            step.last_reject = RejectReason.PREEMPTED
            step.preemptions += 1
            step.failure_detail = detail
            self._recompute_task_state(rec)
            return step.spec.capacity_cost

    # ---------- 终态级联（引擎在临界区内调用）----------
    def cascade_terminal_inflight(self, task_id: str) -> list[tuple[str, str, int]]:
        """任务已处终态（FAILED）时，把仍在飞的步骤置 CANCELLED 并返回占用清单。

        保证"任务终态 => 无在飞步骤"的自洽性：迟到结果会被状态守卫丢弃，
        不会改变视图、不计副作用。PENDING 步骤保持 PENDING（任务终态后
        不会再被派发，状态语义与既有契约一致）。
        返回 (step_id, unit_id, cost) 供引擎释放容量；任务非终态返回空列表。
        """
        with self.lock:
            rec = self._record(task_id)
            if rec.state not in (TaskState.FAILED, TaskState.CANCELLED):
                return []
            occupied: list[tuple[str, str, int]] = []
            for sid in rec.order:
                step = rec.steps[sid]
                if step.state == StepState.DISPATCHED and step.assigned_unit:
                    occupied.append(
                        (sid, step.assigned_unit, step.spec.capacity_cost)
                    )
                    step.state = StepState.CANCELLED
                    step.assigned_unit = None
            return occupied

    def finish_recoverable(
        self,
        task_id: str,
        step_id: str,
        detail: str,
        attempt: int,
        max_retries: int,
    ) -> tuple[bool, bool]:
        """可恢复失败的原子收口：返回 (是否接受, 是否重试)。

        重试额度按 retries_used 计（抢占/离线让位不消耗额度）：
        retries_used < max_retries 则消耗一次额度并回 PENDING；
        否则进入 FAILED(RETRIES_EXHAUSTED)。代次不符/已取消的飞行结果丢弃。
        """
        with self.lock:
            rec = self._record(task_id)
            step = rec.steps[step_id]
            if step.state != StepState.DISPATCHED or step.attempts != attempt + 1:
                return False, False
            if step.retries_used < max_retries:
                step.retries_used += 1
                step.state = StepState.PENDING
                step.assigned_unit = None
                step.failure_detail = (
                    f"{detail}; retry {step.retries_used}/{max_retries}"
                )
                self._recompute_task_state(rec)
                return True, True
            step.state = StepState.FAILED
            step.failure_code = FailureCode.RETRIES_EXHAUSTED
            step.failure_detail = (
                f"{detail}; retries exhausted after {attempt + 1} attempt(s)"
            )
            self._recompute_task_state(rec)
            return True, False
