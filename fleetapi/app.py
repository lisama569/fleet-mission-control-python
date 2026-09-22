"""FastAPI application exposing the fleet control plane."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from fleet.engine import DispatchRejected, Engine
from fleet.models import StepSpec, TaskSpec
from fleet.simulator import WallClock
from fleetapi.schemas import SubmitIn, UnitIn, UnitStateIn


def create_app(engine: Engine | None = None,
               start_scheduler: bool | None = None) -> FastAPI:
    engine = engine or Engine(clock=WallClock())
    if start_scheduler is None:
        # Wall-clock deployments self-schedule; virtual-clock test engines
        # are driven explicitly (tick/drive_until_idle) for determinism.
        start_scheduler = isinstance(engine.clock, WallClock)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if start_scheduler:
            engine.start()
        yield
        if start_scheduler:
            engine.stop()

    app = FastAPI(title="Fleet Mission Control", lifespan=lifespan)
    app.state.engine = engine

    def _spec_from(body: SubmitIn) -> TaskSpec:
        return TaskSpec(
            priority=body.priority,
            steps=tuple(
                StepSpec(
                    key=s.key,
                    capability=s.capability,
                    capacity_cost=s.capacity_cost,
                    timeout_ms=s.timeout_ms,
                    max_retries=s.max_retries,
                    depends_on=tuple(s.depends_on),
                    behavior=s.behavior,
                    run_ms=getattr(s, "run_ms", 20),
                    flaky_fail_times=getattr(s, "flaky_fail_times", 1),
                    fail_message=s.fail_message,
                ) for s in body.steps),
        )

    @app.get("/")
    def read_root():
        # Preserved from the original service (behavior unchanged).
        return {"Hello": "World"}

    @app.post("/api/tasks", status_code=201)
    def submit_task(body: SubmitIn):
        try:
            task, created = engine.submit(body.idempotency_key,
                                          _spec_from(body))
        except Exception as exc:  # InvalidSpec -> 400
            raise HTTPException(status_code=400, detail=str(exc))
        view = engine.task_view(task.task_id)
        view["created"] = created
        return view

    @app.get("/api/tasks")
    def list_tasks():
        return {"tasks": engine.list_tasks()}

    @app.get("/api/tasks/{task_id}")
    def get_task(task_id: str):
        try:
            return engine.task_view(task_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="task not found")

    @app.post("/api/tasks/{task_id}/cancel", status_code=200)
    def cancel_task(task_id: str):
        try:
            task = engine.cancel_task(task_id)
            return engine.task_view(task.task_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="task not found")

    @app.post("/api/tasks/{task_id}/preempt", status_code=200)
    def preempt_task(task_id: str):
        try:
            task = engine.preempt_task(task_id)
            return engine.task_view(task.task_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="task not found")

    @app.post("/api/tasks/{task_id}/steps/{step_key}/dispatch",
              status_code=202)
    def dispatch_step(task_id: str, step_key: str, unit_id: str | None = None):
        try:
            result = engine.dispatch_step(task_id, step_key, unit_id)
        except DispatchRejected as exc:
            raise HTTPException(
                status_code=409,
                detail={"reason": exc.reason.value, "detail": exc.detail})
        except KeyError:
            raise HTTPException(status_code=404, detail="task not found")
        result["accepted"] = True
        return result

    @app.get("/api/rejects")
    def recent_rejects():
        return {"rejects": engine.recent_rejects()}

    @app.get("/api/events")
    def get_events(after: int = 0):
        return {"events": engine.events_since(after)}

    @app.post("/api/units", status_code=201)
    def register_unit(body: UnitIn):
        try:
            unit = engine.register_unit(
                body.unit_id, frozenset(body.capabilities),
                body.capacity_total)
        except Exception as exc:  # InvalidSpec -> 400
            raise HTTPException(status_code=400, detail=str(exc))
        return engine.unit_view(unit.unit_id)

    @app.get("/api/units")
    def list_units():
        return {"units": engine.list_units()}

    @app.get("/api/units/{unit_id}")
    def get_unit(unit_id: str):
        try:
            return engine.unit_view(unit_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="unit not found")

    @app.post("/api/units/{unit_id}/state", status_code=200)
    def set_unit_state(unit_id: str, body: UnitStateIn):
        try:
            unit = engine.set_unit_online(unit_id, body.online)
            return engine.unit_view(unit.unit_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="unit not found")

    @app.post("/api/scheduler/tick", status_code=200)
    def scheduler_tick():
        return {"counts": engine.tick()}

    return app


app = create_app()
