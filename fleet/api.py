"""FastAPI 路由层。

保持既有接口行为不变：
- GET / 仍返回 {"Hello": "World"}。

新增本地控制面接口（全部只操作进程内状态，无外部依赖）：
- POST /tasks                幂等提交任务（以 task_id 为提交标识）
- GET  /tasks                列出任务 id
- GET  /tasks/{task_id}      查询任务与步骤状态视图
- POST /tasks/{task_id}/cancel  取消任务（幂等）
- POST /units                登记执行单元
- GET  /units                查询单元能力/容量与占用
- POST /units/{unit_id}/offline | /online  模拟单元掉线/恢复
"""

from __future__ import annotations

from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .errors import InvalidSpecError, UnknownTaskError
from .executor import StepBehavior
from .models import Priority, StepSpec, TaskSpec, UnitConfig
from .service import FleetService

# 行为脚本中可识别的结果字面量
_OUTCOMES = ("succeeded", "timeout", "crashed", "error")


class StepIn(BaseModel):
    id: str
    required_capability: str
    depends_on: list[str] = Field(default_factory=list)
    capacity_cost: int = 1
    timeout: float = 1.0
    max_retries: int = 0
    # 可复现的行为脚本（可选，仅本地模拟用）：
    # outcomes 按尝试序号循环取值；side_effects 为成功那次的副作用数。
    outcomes: list[str] = Field(default_factory=lambda: ["succeeded"])
    side_effects: int = 1
    fail_detail: str = "simulated non-recoverable error"
    delay: float = 0.0


class SubmitIn(BaseModel):
    task_id: str
    steps: list[StepIn]
    # 可选：任务优先级（high/medium/low）。不携带时按 medium（普通）处理，
    # 与旧版请求的行为完全一致。
    priority: Literal["high", "medium", "low"] = "medium"


class UnitIn(BaseModel):
    unit_id: str
    capabilities: list[str]
    capacity: int = 1


def _build_service() -> FleetService:
    svc = FleetService()
    # 默认本地模拟执行单元：能力与容量不同，便于演示约束匹配
    svc.register_unit(UnitConfig("unit-arm-1", frozenset({"arm"}), 2))
    svc.register_unit(UnitConfig("unit-arm-2", frozenset({"arm"}), 1))
    svc.register_unit(UnitConfig("unit-cam-1", frozenset({"camera"}), 2))
    return svc


def create_app(service: FleetService | None = None) -> FastAPI:
    app = FastAPI(
        title="Fleet Mission Control (local simulated)",
        version="0.1.0",
    )
    app.state.service = service or _build_service()

    def svc() -> FleetService:
        return app.state.service

    # ---- 既有接口（行为保持不变）----
    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    # ---- 任务提交 / 查询 / 取消 ----
    @app.post("/tasks", status_code=200)
    def submit_task(body: SubmitIn):
        try:
            spec = TaskSpec(
                task_id=body.task_id,
                priority=Priority(body.priority),
                steps=tuple(
                    StepSpec(
                        id=s.id,
                        required_capability=s.required_capability,
                        depends_on=tuple(s.depends_on),
                        capacity_cost=s.capacity_cost,
                        timeout=s.timeout,
                        max_retries=s.max_retries,
                    )
                    for s in body.steps
                ),
            )
            behaviors: dict[str, StepBehavior] = {}
            for s in body.steps:
                bad = [o for o in s.outcomes if o not in _OUTCOMES]
                if bad:
                    raise InvalidSpecError(
                        f"step {s.id}: unknown outcomes {bad}; "
                        f"allowed: {_OUTCOMES}"
                    )
                behaviors[s.id] = StepBehavior(
                    outcomes=tuple(s.outcomes) or ("succeeded",),
                    side_effects=s.side_effects,
                    fail_detail=s.fail_detail,
                    delay=s.delay,
                )
            view, created = svc().submit(spec, behaviors)
        except InvalidSpecError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        return {"created": created, "task": _task_payload(view)}

    @app.get("/tasks")
    def list_tasks():
        return {"tasks": svc().list_tasks()}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str):
        try:
            view = svc().get_task(task_id)
        except UnknownTaskError:
            raise HTTPException(status_code=404, detail=f"task {task_id} not found")
        return _task_payload(view)

    @app.post("/tasks/{task_id}/cancel")
    def cancel_task(task_id: str):
        try:
            view, changed = svc().cancel(task_id)
        except UnknownTaskError:
            raise HTTPException(status_code=404, detail=f"task {task_id} not found")
        return {"changed": changed, "task": _task_payload(view)}

    # ---- 执行单元管理 ----
    @app.post("/units", status_code=201)
    def register_unit(body: UnitIn):
        try:
            unit = svc().register_unit(
                UnitConfig(body.unit_id, frozenset(body.capabilities), body.capacity)
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return _unit_payload(unit.unit_id, svc())

    @app.get("/units")
    def list_units():
        return {"units": [_unit_payload(u.unit_id, svc()) for u in svc().registry.all_units()]}

    @app.post("/units/{unit_id}/offline")
    def unit_offline(unit_id: str):
        try:
            svc().unit_offline(unit_id)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"unit {unit_id} not found")
        return {"unit_id": unit_id, "online": False}

    @app.post("/units/{unit_id}/online")
    def unit_online(unit_id: str):
        try:
            svc().unit_online(unit_id)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"unit {unit_id} not found")
        return {"unit_id": unit_id, "online": True}

    return app


# ---- 视图序列化 ----
def _step_payload(s) -> dict:
    return {
        "id": s.id,
        "state": s.state.value,
        "required_capability": s.required_capability,
        "depends_on": list(s.depends_on),
        "capacity_cost": s.capacity_cost,
        "attempts": s.attempts,
        "max_retries": s.max_retries,
        "retries_used": s.retries_used,
        "preemptions": s.preemptions,
        "last_reject": s.last_reject.value if s.last_reject else None,
        "failure_code": s.failure_code.value if s.failure_code else None,
        "failure_detail": s.failure_detail,
        "assigned_unit": s.assigned_unit,
        "side_effects": s.side_effects,
    }


def _task_payload(view) -> dict:
    return {
        "task_id": view.task_id,
        "state": view.state.value,
        "rejection": view.rejection,
        "priority": view.priority.value,
        "steps": [_step_payload(view.steps[sid]) for sid in view.steps],
    }


def _unit_payload(unit_id: str, service: FleetService) -> dict:
    u = service.registry.get(unit_id)
    return {
        "unit_id": u.unit_id,
        "capabilities": sorted(u.capabilities),
        "capacity": u.capacity,
        "used": u.used_capacity(),
        "free": u.free_capacity(),
        "online": u.is_online(),
    }
