"""应用服务层：幂等提交、状态查询、取消、单元管理。

幂等语义
--------
- 以 task_id 作为提交标识；首次提交登记任务、注册行为脚本并触发调度。
- 相同 task_id 的重复提交：不产生重复步骤、不重复注册行为、不重复执行，
  直接返回首次提交的同一状态视图（created=False 供调用方/测试区分）。
- 取消也幂等：已处于终态的任务再次取消返回 changed=False，不报错。
"""

from __future__ import annotations

from .engine import OrchestrationEngine
from .errors import InvalidSpecError, UnknownTaskError
from .executor import ExecutionUnit, StepBehavior, UnitRegistry
from .models import StepSpec, TaskSpec, TaskView, UnitConfig
from .store import InMemoryStore


class FleetService:
    def __init__(
        self,
        store: InMemoryStore | None = None,
        registry: UnitRegistry | None = None,
        engine: OrchestrationEngine | None = None,
    ) -> None:
        self.store = store or InMemoryStore()
        self.registry = registry or UnitRegistry()
        self.engine = engine or OrchestrationEngine(self.store, self.registry)

    # ---------------- 执行单元管理 ----------------
    def register_unit(self, config: UnitConfig) -> ExecutionUnit:
        unit = ExecutionUnit(config)
        self.registry.register(unit)
        # 新单元可能使原本容量不足的派发成立
        self.engine.kick()
        return unit

    def unit_offline(self, unit_id: str) -> None:
        self.engine.unit_offline(unit_id)

    def unit_online(self, unit_id: str) -> None:
        self.engine.unit_online(unit_id)

    # ---------------- 提交 ----------------
    @staticmethod
    def validate(spec: TaskSpec) -> None:
        """校验任务声明：步骤 id 唯一、依赖存在、DAG 无环、参数合法。"""
        if not spec.task_id:
            raise InvalidSpecError("task_id must be non-empty")
        if not spec.steps:
            raise InvalidSpecError("task must contain at least one step")
        ids = [s.id for s in spec.steps]
        if len(set(ids)) != len(ids):
            raise InvalidSpecError(f"duplicate step ids in {ids}")
        idset = set(ids)
        for s in spec.steps:
            if not s.required_capability:
                raise InvalidSpecError(f"step {s.id}: capability required")
            if s.capacity_cost < 1:
                raise InvalidSpecError(f"step {s.id}: capacity_cost must be >= 1")
            if s.timeout <= 0:
                raise InvalidSpecError(f"step {s.id}: timeout must be > 0")
            if s.max_retries < 0:
                raise InvalidSpecError(f"step {s.id}: max_retries must be >= 0")
            for d in s.depends_on:
                if d not in idset:
                    raise InvalidSpecError(
                        f"step {s.id}: depends on unknown step '{d}'"
                    )
                if d == s.id:
                    raise InvalidSpecError(f"step {s.id}: cannot depend on itself")
        # DAG 环检测：Kahn 拓扑排序（依赖必须先完成）
        indeg = {s.id: 0 for s in spec.steps}
        adj: dict[str, list[str]] = {s.id: [] for s in spec.steps}
        for s in spec.steps:
            for d in s.depends_on:
                adj[d].append(s.id)
                indeg[s.id] += 1
        queue = [i for i, n in indeg.items() if n == 0]
        seen = 0
        while queue:
            cur = queue.pop()
            seen += 1
            for nxt in adj[cur]:
                indeg[nxt] -= 1
                if indeg[nxt] == 0:
                    queue.append(nxt)
        if seen != len(spec.steps):
            raise InvalidSpecError("dependency graph contains a cycle")

    def submit(
        self,
        spec: TaskSpec,
        behaviors: dict[str, StepBehavior] | None = None,
    ) -> tuple[TaskView, bool]:
        """幂等提交。返回 (状态视图, 是否首次创建)。"""
        self.validate(spec)
        behaviors = behaviors or {}
        with self.store.lock:
            if self.store.has_task(spec.task_id):
                # 幂等命中：忽略本次携带的任何新行为，不重复执行
                self.engine.emit(
                    "submit_duplicate", spec.task_id,
                    reason="IDEMPOTENT_HIT",
                    detail="duplicate submit ignored; returning existing task",
                )
                return self.store.snapshot_task(spec.task_id), False
            self.store.create_task(spec.task_id, list(spec.steps))
            for step in spec.steps:
                if step.id in behaviors:
                    self.engine.register_behavior(
                        spec.task_id, step.id, behaviors[step.id]
                    )
            self.engine.emit(
                "submit", spec.task_id,
                detail=f"accepted {len(spec.steps)} step(s)",
            )
        self.engine.kick()
        return self.store.snapshot_task(spec.task_id), True

    # ---------------- 查询 / 取消 ----------------
    def get_task(self, task_id: str) -> TaskView:
        if not self.store.has_task(task_id):
            raise UnknownTaskError(task_id)
        return self.store.snapshot_task(task_id)

    def list_tasks(self) -> list[str]:
        return self.store.list_tasks()

    def cancel(self, task_id: str) -> tuple[TaskView, bool]:
        if not self.store.has_task(task_id):
            raise UnknownTaskError(task_id)
        changed = self.engine.cancel(task_id)
        return self.store.snapshot_task(task_id), changed

    def wait_idle(self, timeout: float = 5.0) -> bool:
        return self.engine.wait_idle(timeout)

    def shutdown(self) -> None:
        self.engine.shutdown()
