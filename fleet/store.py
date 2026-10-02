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
    PreemptReason,
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
    # attempts：派发代次（epoch）。每次重新派发 +1（含抢占/离线后的重跑），
    # 结果收口以它做代次守卫，挡掉让位/取消/离线后的僵尸结果。
    attempts: int = 0
    # retry_count：可恢复失败次数（只随 timeout/crashed 增长）。
    # 让位抢占与离线重排都不是执行失败，不增加此计数，即不消耗重试额度；
    # 行为脚本也按它选择结果，保证抢占后重跑等价于"第一次执行"。
    retry_count: int = 0
    last_reject: RejectReason | None = None
    failure_code: FailureCode | None = None
    failure_detail: str = ""
    assigned_unit: str | None = None
    side_effects: int = 0
    attempt_log: list[AttemptRecord] = field(default_factory=list)
    # 抢占可观测信息
    preempt_count: int = 0
    last_preempt_reason: PreemptReason | None = None
    preempt_detail: str = ""
    # 本次在飞派发的全局序号（抢占挑选牺牲品时做确定性的先后裁决）
    dispatch_seq: int = 0


@dataclass
class TaskRecord:
    """任务的内部可变记录。"""

    task_id: str
    order: list[str]                     # 步骤 id 的声明顺序
    steps: dict[str, StepRecord]
    priority: Priority = Priority.NORMAL
    submit_seq: int = 0                  # 全局提交序号：同级 FCFS 的稳定依据
    state: TaskState = TaskState.PENDING
    rejection: str = ""                  # 提交被拒/非法声明的说明（幂等返回一致）


class InMemoryStore:
    """并发安全的内存存储。"""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self._tasks: dict[str, TaskRecord] = {}
        self._submit_counter = 0
        self._dispatch_counter = 0

    # ---------- 提交 / 查询 ----------
    def has_task(self, task_id: str) -> bool:
        with self.lock:
            return task_id in self._tasks

    def create_task(
        self, task_id: str, specs: list[StepSpec], priority: Priority
    ) -> TaskRecord:
        """登记新任务（调用方须先做声明校验并确认 task_id 不存在）。

        分配全局单调的 submit_seq：派发顺序规则为
        "优先级降序、同优先级按 submit_seq 升序（FCFS）"，
        submit_seq 一旦分配不再变化，抢占重排后顺序依然稳定可复现。
        """
        with self.lock:
            self._submit_counter += 1
            record = TaskRecord(
                task_id=task_id,
                order=[s.id for s in specs],
                steps={s.id: StepRecord(spec=s) for s in specs},
                priority=priority,
                submit_seq=self._submit_counter,
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
                submit_seq=rec.submit_seq,
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
                        preempt_count=r.preempt_count,
                        last_preempt_reason=r.last_preempt_reason,
                        preempt_detail=r.preempt_detail,
                    )
                    for sid, r in rec.steps.items()
                },
            )

    def list_tasks_ordered(self) -> list[str]:
        """按派发顺序规则返回任务 id：优先级降序，同级按提交先后（FCFS）。"""
        with self.lock:
            recs = list(self._tasks.values())
            recs.sort(key=lambda r: (-r.priority.rank, r.submit_seq))
            return [r.task_id for r in recs]

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
            s
            in (
                StepState.DISPATCHED,
                StepState.SUCCEEDED,
                StepState.FAILED,
                StepState.PREEMPTED,
            )
            for s in states
        ):
            # PREEMPTED 表示该步骤至少派发过一次，任务已进入运行阶段
            rec.state = TaskState.RUNNING
        else:
            rec.state = TaskState.PENDING

    # ---------- 调度读取（引擎在临界区内调用）----------
    def dispatchable(self, task_id: str) -> tuple[StepRecord | None, RejectReason | None]:
        """按声明顺序找到第一个待派发步骤并判定其可派发性。

        待派发包括 PENDING（从未派发/重试回队）与 PREEMPTED（被让位抢占后
        等待重跑）。DISPATCHED（在飞行中）与终态步骤都会被跳过，因此排在
        在飞步骤之后、且依赖已满足的独立步骤仍可被派发；返回 (step, None)。
        (None, reason) 表示当前无可派发步骤：
          - 首遇待派发步骤依赖未满足 -> DEPENDENCY_NOT_MET
          - 任务已取消/终态或无待派发步骤 -> TASK_NOT_ACTIVE
        """
        with self.lock:
            rec = self._record(task_id)
            if rec.state in (TaskState.CANCELLED, TaskState.FAILED, TaskState.SUCCEEDED):
                return None, RejectReason.TASK_NOT_ACTIVE
            target: StepRecord | None = None
            for sid in rec.order:
                st = rec.steps[sid]
                if st.state in (StepState.PENDING, StepState.PREEMPTED):
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
        """步骤进入 DISPATCHED，派发代次 +1，返回新代次（首次派发为 1）。

        retry_count 不变：让位抢占/离线后的重新派发是"第一次执行"的延续，
        不消耗重试额度。行为脚本的结果序号由 get_retry_count 提供。
        """
        with self.lock:
            self._dispatch_counter += 1
            step = self._record(task_id).steps[step_id]
            step.state = StepState.DISPATCHED
            step.assigned_unit = unit_id
            step.dispatch_seq = self._dispatch_counter
            step.attempts += 1
            self._recompute_task_state(self._record(task_id))
            return step.attempts

    def get_dispatch_identity(
        self, task_id: str, step_id: str
    ) -> tuple[int, int]:
        """返回 (派发代次 attempts, 可恢复失败次数 retry_count)。"""
        with self.lock:
            step = self._record(task_id).steps[step_id]
            return step.attempts, step.retry_count

    def get_priority(self, task_id: str) -> Priority:
        with self.lock:
            return self._record(task_id).priority

    def record_attempt(self, task_id: str, record: AttemptRecord) -> None:
        with self.lock:
            self._record(task_id).steps[record.step_id].attempt_log.append(record)

    def mark_succeeded(
        self, task_id: str, step_id: str, side_effects: int, epoch: int
    ) -> bool:
        """成功收口。状态/代次不匹配（被取消、抢占或已超时重派）则丢弃结果。

        成功只会发生一次（之后状态为 SUCCEEDED 终态），因此副作用只计一次。
        epoch 为本次结果对应的派发代次（首次派发=1），让位/取消/离线后
        旧代次的僵尸结果一律丢弃。
        """
        with self.lock:
            rec = self._record(task_id)
            step = rec.steps[step_id]
            if step.state != StepState.DISPATCHED or step.attempts != epoch:
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
        epoch: int,
    ) -> tuple[bool, list[tuple[str, str, int]]]:
        """失败收口（重试耗尽 / 不可恢复）。

        代次不符/已取消的飞行结果丢弃（返回 (False, [])）。接受失败时，
        同任务其余已经开始但未终态的步骤（DISPATCHED 在飞 / PREEMPTED 让位中）
        一并置 CANCELLED：任务进入 FAILED 终态后，任何迟到结果都不能再让它们
        成功或改变对外视图。从未派发的 PENDING 步骤保持 PENDING（其依赖已不可能
        满足，且终态任务不会再被调度），与取消语义区分。
        返回 (是否接受, 需要释放容量的 (step_id, unit_id, cost) 列表)。
        """
        with self.lock:
            rec = self._record(task_id)
            step = rec.steps[step_id]
            if step.state != StepState.DISPATCHED or step.attempts != epoch:
                return False, []
            step.state = StepState.FAILED
            step.failure_code = code
            step.failure_detail = detail
            occupied: list[tuple[str, str, int]] = []
            for sid in rec.order:
                other = rec.steps[sid]
                if sid == step_id:
                    continue
                if other.state == StepState.DISPATCHED:
                    if other.assigned_unit is not None:
                        occupied.append(
                            (sid, other.assigned_unit, other.spec.capacity_cost)
                        )
                    other.state = StepState.CANCELLED
                    other.assigned_unit = None
                elif other.state == StepState.PREEMPTED:
                    # 让位中、尚未重跑：随任务终态一并取消；其僵尸结果由代次守卫丢弃
                    other.state = StepState.CANCELLED
            rec.state = TaskState.FAILED
            return True, occupied

    def revert_to_pending(
        self, task_id: str, step_id: str, detail: str, epoch: int
    ) -> bool:
        """可恢复失败但仍有重试额度：回到 PENDING 等待重新派发。

        retry_count + 1（消耗一次重试机会，行为脚本下一次结果据此选择）。
        被取消/抢占/已超时重派的飞行结果丢弃（返回 False），不复活。
        """
        with self.lock:
            rec = self._record(task_id)
            step = rec.steps[step_id]
            if step.state != StepState.DISPATCHED or step.attempts != epoch:
                return False
            step.state = StepState.PENDING
            step.assigned_unit = None
            step.failure_detail = detail
            step.retry_count += 1
            self._recompute_task_state(rec)
            return True

    # ---------- 抢占让位 ----------
    def preempt_victims_on(
        self, unit_id: str, strict_below_rank: int
    ) -> list[tuple[str, str, int, int, int]]:
        """列出指定单元上可被让出的在飞步骤（优先级严格低于请求方）。

        返回 (task_id, step_id, cost, priority_rank, dispatch_seq)，
        按"优先级最低优先、同级后派发先让位（dispatch_seq 大的先让）"排序。
        后派发的先让位可尽量保留更早开始、更接近完成的步骤。
        """
        with self.lock:
            out: list[tuple[str, str, int, int, int]] = []
            for tid, rec in self._tasks.items():
                if rec.state in (
                    TaskState.CANCELLED,
                    TaskState.FAILED,
                    TaskState.SUCCEEDED,
                ):
                    continue
                for sid in rec.order:
                    step = rec.steps[sid]
                    if (
                        step.state == StepState.DISPATCHED
                        and step.assigned_unit == unit_id
                        and rec.priority.rank < strict_below_rank
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
            out.sort(key=lambda x: (x[3], -x[4]))
            return out

    def preempt_steps(
        self,
        victims: list[tuple[str, str]],
        reason: PreemptReason,
        detail: str,
    ) -> list[tuple[str, str, str, int]]:
        """把指定在飞步骤置 PREEMPTED，返回需释放容量的四元组列表。

        让位语义：不失败、不取消、retry_count 不增加（不消耗重试额度）；
        派发代次保持不变，其飞行结果稍后到达时按代次守卫丢弃；
        容量由引擎按返回列表逐项释放，后续 tick 自动重新派发。
        """
        with self.lock:
            released: list[tuple[str, str, str, int]] = []
            for tid, sid in victims:
                rec = self._record(tid)
                step = rec.steps[sid]
                if step.state != StepState.DISPATCHED or not step.assigned_unit:
                    continue
                released.append(
                    (tid, sid, step.assigned_unit, step.spec.capacity_cost)
                )
                step.state = StepState.PREEMPTED
                step.preempt_count += 1
                step.last_preempt_reason = reason
                step.preempt_detail = detail
                step.assigned_unit = None
                self._recompute_task_state(rec)
            return released

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

    def is_terminal(self, task_id: str) -> bool:
        with self.lock:
            return self._record(task_id).state in (
                TaskState.CANCELLED,
                TaskState.SUCCEEDED,
                TaskState.FAILED,
            )

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
