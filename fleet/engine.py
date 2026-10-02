"""编排引擎。

职责
----
1. 调度：按 (任务优先级, 提交序号) 排序推进——高优先级先派、同优先级
   FIFO，任务内步骤按声明顺序；每次推进在 store 全局锁的临界区内完成
   "依赖检查 -> 能力/容量筛选 -> (必要时抢占让位) -> 容量占用 -> 状态流转"，
   并发提交下约束与派发顺序都不会被突破。
2. 抢占：高优先级步骤等容量时，让严格更低优先级的在飞步骤让位
   （PREEMPTED，不算失败、不消耗重试额度），容量立即转交；让位/重派
   不改变任务的 (优先级, 提交序号)，顺序稳定可复现。
3. 执行：把一次尝试提交到线程池，在执行单元上同步跑行为脚本；
   以 watchdog 定时器实现超时语义（不依赖真实 sleep 的脚本结果）。
4. 重试：超时/异常退出属可恢复错误，retries_used 未达 max_retries 则
   释放容量、回到 PENDING 重新派发；耗尽 -> FAILED(RETRIES_EXHAUSTED)；
   不可恢复错误 -> FAILED(NON_RECOVERABLE)。
5. 回收：任务取消、任务进入终态（级联停掉在飞步骤）与执行单元离线时，
   所有被占用容量立即释放，飞行中尝试的迟到结果按"状态 + 尝试代次"
   守卫丢弃，杜绝悬空占用与终态后视图变更。
6. 幂等：已完成的副作用只在成功收口时计一次；成功即终态，不会重放。
7. 事件：每次关键判定都记录 EngineEvent，测试中打印输入事件与判定依据。
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from .errors import NonRecoverableExecError, RecoverableExecError
from .executor import (
    OUT_CRASHED,
    OUT_ERROR,
    OUT_SUCCEEDED,
    OUT_TIMEOUT,
    ExecutionUnit,
    StepBehavior,
    UnitRegistry,
)
from .models import (
    AttemptRecord,
    EngineEvent,
    FailureCode,
    RejectReason,
    StepState,
    TaskState,
)
from .store import InMemoryStore

logger = logging.getLogger("fleet.engine")


class OrchestrationEngine:
    def __init__(
        self,
        store: InMemoryStore,
        registry: UnitRegistry,
        max_workers: int = 16,
    ) -> None:
        self.store = store
        self.registry = registry
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="fleet-exec"
        )
        self._tick_guard = threading.Lock()   # 串行化调度，保证派发顺序确定
        self._seq = 0
        self._dispatch_seq = 0                # 全局派发序号（抢占选 victim 用）
        self._behaviors: dict[tuple[str, str], StepBehavior] = {}

    # ---------------- 事件日志 ----------------
    def emit(
        self,
        kind: str,
        task_id: str = "",
        step_id: str = "",
        unit_id: str = "",
        reason: str = "",
        detail: str = "",
    ) -> EngineEvent:
        """记录一条引擎判定事件（公开入口）。"""
        return self._emit(kind, task_id, step_id, unit_id, reason, detail)

    def _emit(
        self,
        kind: str,
        task_id: str = "",
        step_id: str = "",
        unit_id: str = "",
        reason: str = "",
        detail: str = "",
    ) -> EngineEvent:
        with self.store.lock:
            self._seq += 1
            ev = EngineEvent(
                seq=self._seq,
                kind=kind,
                task_id=task_id,
                step_id=step_id,
                unit_id=unit_id,
                reason=reason,
                detail=detail,
            )
        logger.info(
            "event#%s %s task=%s step=%s unit=%s reason=%s detail=%s",
            ev.seq, ev.kind, ev.task_id, ev.step_id, ev.unit_id,
            ev.reason, ev.detail,
        )
        return ev

    def register_behavior(
        self, task_id: str, step_id: str, behavior: StepBehavior
    ) -> None:
        self._behaviors[(task_id, step_id)] = behavior

    # ---------------- 调度 ----------------
    def kick(self) -> None:
        """触发一次调度扫描（提交/结果到达/离线后调用）。

        用非重入锁保证全局同时只有一次扫描。派发顺序确定、可复现：
        任务按 (优先级, 提交序号) 排序——高优先级先派，同优先级 FIFO；
        任务内步骤按声明顺序；同一 tick 内尽量派发所有当前可派发步骤。
        """
        if not self._tick_guard.acquire(blocking=False):
            return
        try:
            # 成功派发后立即重扫：容量允许时多个独立步骤在同一轮内全部派发；
            # 无可派发步骤（全部等待/拒绝/终态）时退出。
            while True:
                any_dispatch = False
                for task_id in self.store.ordered_task_ids():
                    if self._try_dispatch(task_id):
                        any_dispatch = True
                if not any_dispatch:
                    return
        finally:
            self._tick_guard.release()

    def _try_dispatch(self, task_id: str) -> bool:
        """尝试为任务派发一个步骤；成功发起派发返回 True。"""
        with self.store.lock:
            step, dep_reason = self.store.dispatchable(task_id)
            if step is None:
                # 依赖未满足是"有意义的拒绝"，记录事件便于复现判定依据
                if dep_reason == RejectReason.DEPENDENCY_NOT_MET:
                    # 找到当前卡在最前面的 PENDING 步骤用于事件描述
                    view = self.store.snapshot_task(task_id)
                    for sid, sv in view.steps.items():
                        if sv.state == StepState.PENDING:
                            self._emit(
                                "reject", task_id, sid,
                                reason=RejectReason.DEPENDENCY_NOT_MET.value,
                                detail=f"dependencies not satisfied: {sv.depends_on}",
                            )
                            break
                return False

            spec = step.spec
            # 能力/容量筛选（在 store 临界区内调用，保证检查与占用原子）
            units = self.registry.all_units()
            online = [u for u in units if u.is_online()]
            capable = [
                u for u in online if spec.required_capability in u.capabilities
            ]
            if not online:
                reason = RejectReason.NO_CANDIDATE_UNIT
                detail = "no execution unit online"
            elif not capable:
                reason = RejectReason.CAPABILITY_MISMATCH
                detail = (
                    f"no online unit provides capability "
                    f"'{spec.required_capability}'; "
                    f"online={[u.unit_id for u in online]}"
                )
            else:
                fitting = [
                    u for u in capable
                    if u.free_capacity() >= spec.capacity_cost
                ]
                if not fitting:
                    # 容量不足：若本步骤优先级高于某些在飞步骤，尝试抢占让位。
                    # 整个"筛选 victim -> 让位 -> 占用 -> 派发"在同一临界区完成，
                    # 并发提交/取消/离线不会超卖容量，账目也不会对不上。
                    plan = self._plan_preemption(
                        capable, spec.capacity_cost,
                        self.store.priority_rank(task_id),
                    )
                    if plan is not None:
                        unit, victims = plan
                        for vt, vs, vcost in victims:
                            freed = self.store.preempt_step(
                                vt, vs,
                                f"preempted by higher-priority "
                                f"task={task_id} step={spec.id}",
                            )
                            if freed is None:
                                continue  # 已被并发路径让位（防御，正常不会发生）
                            self._release(
                                unit.unit_id, freed,
                                f"preempted task={vt} step={vs} "
                                f"by task={task_id} step={spec.id}",
                            )
                            self._emit(
                                "preempt", vt, vs, unit.unit_id,
                                reason=RejectReason.PREEMPTED.value,
                                detail=(
                                    f"yields to task={task_id} step={spec.id} "
                                    f"(priority={self.store.task_priority(task_id).value})"
                                ),
                            )
                        fitting = [unit]
                if not fitting:
                    reason = RejectReason.INSUFFICIENT_CAPACITY
                    detail = (
                        f"capable units {[u.unit_id for u in capable]} lack "
                        f"capacity {spec.capacity_cost}; "
                        f"free={ {u.unit_id: u.free_capacity() for u in capable} }"
                    )
                else:
                    unit = min(fitting, key=lambda u: u.unit_id)
                    if not unit.acquire(spec.required_capability, spec.capacity_cost):
                        reason = RejectReason.INSUFFICIENT_CAPACITY
                        detail = f"capacity lost on unit {unit.unit_id}"
                        fitting = []  # fall through to reject path
                    else:
                        self._dispatch_seq += 1
                        attempt = self.store.mark_dispatched(
                            task_id, spec.id, unit.unit_id, self._dispatch_seq
                        )
                        behavior = self._behaviors.get(
                            (task_id, spec.id), StepBehavior()
                        )
                        self._emit(
                            "dispatch", task_id, spec.id, unit.unit_id,
                            detail=(
                                f"attempt={attempt} cost={spec.capacity_cost} "
                                f"priority={self.store.task_priority(task_id).value} "
                                f"free_after={unit.free_capacity()}/"
                                f"{unit.capacity}"
                            ),
                        )
                        self._pool.submit(
                            self._run_attempt, task_id, spec.id,
                            unit.unit_id, attempt, spec.timeout,
                            spec.max_retries, spec.capacity_cost, behavior,
                        )
                        return True

            self.store.record_rejection(task_id, spec.id, reason)
            self._emit("reject", task_id, spec.id, reason=reason.value, detail=detail)
            return False

    # ---------------- 抢占规划 ----------------
    def _plan_preemption(
        self, capable: list, cost: int, waiting_rank: int
    ):
        """为等容量的高优先级步骤规划抢占。须在 store.lock 临界区内调用。

        规则（确定、可复现）：
        - 只抢占优先级**严格更低**（rank 数值更大）的在飞步骤；同优先级不让位；
        - 每个单元上按 (优先级从低到高, 派发序号从晚到早) 的顺序选 victim，
          直到腾出足够容量；选不出则该单元不参与；
        - 在可行单元中选 victim 最少、其次 unit_id 最小者。
        返回 (unit, [(task_id, step_id, cost), ...]) 或 None。
        """
        best = None
        for unit in sorted(capable, key=lambda u: u.unit_id):
            free = unit.free_capacity()
            candidates = [
                iv for iv in self.store.inflight_on_unit(unit.unit_id)
                if iv[3] > waiting_rank  # iv[3]=victim 优先级 rank，严格更低才让位
            ]
            # 最低优先级、最晚派发的先让位
            candidates.sort(key=lambda iv: (-iv[3], -iv[4]))
            victims: list[tuple[str, str, int]] = []
            gained = free
            for iv in candidates:
                if gained >= cost:
                    break
                victims.append((iv[0], iv[1], iv[2]))
                gained += iv[2]
            if gained < cost:
                continue
            key = (len(victims), unit.unit_id)
            if best is None or key < best[0]:
                best = (key, unit, victims)
        if best is None:
            return None
        return best[1], best[2]

    # ---------------- 执行与结果收口 ----------------
    def _run_attempt(
        self,
        task_id: str,
        step_id: str,
        unit_id: str,
        attempt: int,
        timeout: float,
        max_retries: int,
        cost: int,
        behavior: StepBehavior,
    ) -> None:
        unit = self.registry.get(unit_id)
        outcome = OUT_CRASHED
        side_effects = 0
        detail = ""
        timed_out = {"v": False}
        done = threading.Event()

        def _watchdog() -> None:
            if not done.wait(timeout=timeout):
                timed_out["v"] = True
                self._emit(
                    "timeout", task_id, step_id, unit_id,
                    detail=f"attempt={attempt} exceeded timeout {timeout}s",
                )
                self._finish_recoverable(
                    task_id, step_id, unit_id, attempt, cost,
                    max_retries, OUT_TIMEOUT,
                    f"timed out after {timeout}s",
                )

        watcher = threading.Thread(
            target=_watchdog, name=f"fleet-watchdog-{step_id}-{attempt}",
            daemon=True,
        )
        watcher.start()
        try:
            side_effects = unit.run(step_id, attempt, behavior)
            outcome = OUT_SUCCEEDED
        except RecoverableExecError as exc:
            outcome = OUT_TIMEOUT if "timed out" in str(exc) else OUT_CRASHED
            detail = str(exc)
        except NonRecoverableExecError as exc:
            outcome = OUT_ERROR
            detail = str(exc)
        except Exception as exc:  # 防御：任何未知异常都按可恢复崩溃处理
            outcome = OUT_CRASHED
            detail = f"unexpected error: {exc!r}"
        finally:
            done.set()
        watcher.join(timeout=1.0)

        # 若看门狗已按超时收口，本线程结果（可能迟到）一律丢弃
        if timed_out["v"]:
            return

        if outcome == OUT_SUCCEEDED:
            self._finish_success(
                task_id, step_id, unit_id, attempt, cost, side_effects
            )
        elif outcome == OUT_ERROR:
            self._finish_non_recoverable(
                task_id, step_id, unit_id, attempt, cost, detail
            )
        else:
            self._finish_recoverable(
                task_id, step_id, unit_id, attempt, cost,
                max_retries, outcome, detail,
            )

    def _release(self, unit_id: str, cost: int, why: str) -> None:
        """释放单元容量（所有收口路径的唯一出口，保证占用/释放配对）。"""
        try:
            self.registry.get(unit_id).release(cost)
        except KeyError:
            # 单元已注销：容量随对象消失，仅记录
            self._emit("release", unit_id=unit_id, reason="unit-missing", detail=why)
            return
        self._emit(
            "release", unit_id=unit_id,
            detail=f"cost={cost} free_after={self.registry.get(unit_id).free_capacity()} ({why})",
        )

    def _finish_success(
        self, task_id, step_id, unit_id, attempt, cost, side_effects
    ) -> None:
        with self.store.lock:
            accepted = self.store.mark_succeeded(
                task_id, step_id, side_effects, attempt
            )
            self.store.record_attempt(
                task_id,
                AttemptRecord(
                    step_id=step_id, attempt=attempt, unit_id=unit_id,
                    outcome=OUT_SUCCEEDED, side_effect_count=side_effects
                    if accepted else 0,
                    detail="" if accepted else "stale result discarded",
                ),
            )
            if not accepted:
                # 抢占（取消/离线/超时重派）已释放过容量并移交了步骤归属，
                # 迟到结果不得再次触碰容量账本，否则会造成超额/悬空释放。
                self._emit(
                    "stale", task_id, step_id, unit_id,
                    reason="STALE_RESULT",
                    detail="late success discarded; capacity already released",
                )
                return
            self._emit(
                "success", task_id, step_id, unit_id,
                detail=f"attempt={attempt} side_effects={side_effects}",
            )
            self._release(unit_id, cost, "step succeeded")
        self.kick()

    def _cascade_terminal(self, task_id: str) -> None:
        """任务进入终态后回收仍在飞步骤的占用（须在 store.lock 临界区内调用）。

        保证"任务终态 => 无在飞步骤"：在飞步骤置 CANCELLED 并释放容量，
        其迟到结果会被状态/代次守卫丢弃，不计副作用、不改变对外视图。
        """
        for sid, uid, cost in self.store.cascade_terminal_inflight(task_id):
            self._release(
                uid, cost,
                f"task terminal; cancelled in-flight task={task_id} step={sid}",
            )
            self._emit(
                "cancel_inflight", task_id, sid, uid,
                reason="TASK_TERMINAL",
                detail="task reached terminal state; in-flight step cancelled",
            )

    def _finish_non_recoverable(
        self, task_id, step_id, unit_id, attempt, cost, detail
    ) -> None:
        with self.store.lock:
            accepted = self.store.mark_failed(
                task_id, step_id, FailureCode.NON_RECOVERABLE, detail, attempt
            )
            self.store.record_attempt(
                task_id,
                AttemptRecord(
                    step_id=step_id, attempt=attempt, unit_id=unit_id,
                    outcome=OUT_ERROR, detail=detail,
                ),
            )
            if accepted:
                self._release(
                    unit_id, cost,
                    f"non-recoverable task={task_id} step={step_id}",
                )
                self._emit(
                    "fail", task_id, step_id, unit_id,
                    reason=FailureCode.NON_RECOVERABLE.value, detail=detail,
                )
                # 任务因此进入 FAILED 终态：级联停掉同任务其它在飞步骤
                self._cascade_terminal(task_id)
            else:
                self._emit(
                    "stale", task_id, step_id, unit_id,
                    reason="STALE_RESULT",
                    detail="late non-recoverable result discarded; capacity already released",
                )
        self.kick()

    def _finish_recoverable(
        self, task_id, step_id, unit_id, attempt, cost,
        max_retries, outcome, detail,
    ) -> None:
        with self.store.lock:
            self.store.record_attempt(
                task_id,
                AttemptRecord(
                    step_id=step_id, attempt=attempt, unit_id=unit_id,
                    outcome=outcome, detail=detail,
                ),
            )
            # 重试额度按 retries_used 计：抢占/离线让位不消耗额度
            accepted, will_retry = self.store.finish_recoverable(
                task_id, step_id, detail, attempt, max_retries
            )
            if accepted:
                self._release(
                    unit_id, cost,
                    f"{outcome} task={task_id} step={step_id}",
                )
            else:
                self._emit(
                    "stale crashed/timeout", task_id, step_id, unit_id,
                    reason="STALE_RESULT",
                    detail="late recoverable result discarded; capacity already released",
                )
            if accepted and will_retry:
                self._emit(
                    "retry", task_id, step_id, unit_id,
                    reason=outcome,
                    detail=f"{detail}; retry scheduled (max_retries={max_retries})",
                )
            elif accepted:
                self._emit(
                    "fail", task_id, step_id, unit_id,
                    reason=FailureCode.RETRIES_EXHAUSTED.value,
                    detail=f"{detail}; exhausted after {attempt + 1} attempt(s)",
                )
                # 任务因此进入 FAILED 终态：级联停掉同任务其它在飞步骤
                self._cascade_terminal(task_id)
        self.kick()

    # ---------------- 取消 / 离线 ----------------
    def cancel(self, task_id: str) -> bool:
        """取消任务并立即释放其全部占用。返回 False 表示任务已在终态。"""
        with self.store.lock:
            occupied = self.store.cancel_task(task_id)
            if not occupied:
                view = self.store.snapshot_task(task_id)
                self._emit(
                    "cancel", task_id,
                    reason=view.state.value,
                    detail="cancel no-op: task already in terminal state",
                )
                return False
            for step_id, unit_id, cost in occupied:
                self._release(unit_id, cost, f"cancelled task={task_id} step={step_id}")
            self._emit(
                "cancel", task_id,
                reason="CANCELLED",
                detail=f"released {len(occupied)} in-flight step(s)",
            )
        # 取消可能释放了容量，其它任务得以推进
        self.kick()
        return True

    def unit_offline(self, unit_id: str) -> None:
        """执行单元离线：标记下线、回收其占用、受影响步骤重新排队。"""
        with self.store.lock:
            unit = self.registry.get(unit_id)
            unit.mark_offline()
            requeued = self.store.requeue_offline(unit_id)
            for task_id, step_id, cost in requeued:
                self._release(
                    unit_id, cost,
                    f"unit offline task={task_id} step={step_id} requeued",
                )
            self._emit(
                "offline", unit_id=unit_id,
                detail=f"{len(requeued)} in-flight step(s) requeued",
            )
        self.kick()

    def unit_online(self, unit_id: str) -> None:
        self.registry.mark_online(unit_id)
        self._emit("online", unit_id=unit_id)
        self.kick()

    def wait_idle(self, timeout: float = 5.0) -> bool:
        """等待所有任务进入终态（测试用）。"""
        import time

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            views = [
                self.store.snapshot_task(t) for t in self.store.list_tasks()
            ]
            if views and all(
                v.state in (TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED)
                for v in views
            ):
                return True
            time.sleep(0.005)
        return False

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)
