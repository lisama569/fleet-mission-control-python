"""In-memory state store. All mutable state lives behind one RLock."""
from __future__ import annotations

import threading
from collections import deque

from .models import (
    Event,
    EventType,
    RejectRecord,
    StepRecord,
    TaskRecord,
    TaskSpec,
    UnitRecord,
    UnitState,
    new_id,
)


class DuplicateSubmission(Exception):
    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"duplicate idempotency key, existing task {task_id}")


class InvalidSpec(Exception):
    pass


class FleetStore:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.tasks: dict[str, TaskRecord] = {}
        self.units: dict[str, UnitRecord] = {}
        self.events: deque[Event] = deque(maxlen=10000)
        self.rejects: deque[RejectRecord] = deque(maxlen=1000)
        self.effects: set[str] = set()
        self._idem: dict[str, str] = {}
        self._event_seq = 0

    # ------------------------------------------------------------------
    # event log
    # ------------------------------------------------------------------
    def append_event(self, at_ms: int, etype: EventType,
                     detail: dict) -> Event:
        with self.lock:
            self._event_seq += 1
            ev = Event(seq=self._event_seq, at_ms=at_ms,
                       type=etype, detail=dict(detail))
            self.events.append(ev)
            return ev

    def log_reject(self, at_ms: int, task_id: str, step_key: str,
                   unit_id: str | None, reason, detail: str) -> None:
        with self.lock:
            self.rejects.append(RejectRecord(
                at_ms=at_ms, task_id=task_id, step_key=step_key,
                unit_id=unit_id, reason=reason, detail=detail))

    # ------------------------------------------------------------------
    # units
    # ------------------------------------------------------------------
    def register_unit(self, unit_id: str | None,
                      capabilities: frozenset[str],
                      capacity_total: int, at_ms: int) -> UnitRecord:
        if capacity_total < 1:
            raise InvalidSpec("capacity_total must be >= 1")
        with self.lock:
            if unit_id is None:
                unit_id = new_id("unit")
            if unit_id in self.units:
                raise InvalidSpec(f"unit {unit_id} already registered")
            unit = UnitRecord(unit_id=unit_id,
                              capabilities=frozenset(capabilities),
                              capacity_total=capacity_total)
            self.units[unit_id] = unit
            self.append_event(at_ms, EventType.UNIT_REGISTERED, {
                "unit_id": unit_id,
                "capabilities": sorted(unit.capabilities),
                "capacity_total": capacity_total,
            })
            return unit

    def set_unit_state(self, unit_id: str, online: bool,
                       at_ms: int) -> UnitRecord:
        with self.lock:
            unit = self.units.get(unit_id)
            if unit is None:
                raise KeyError(unit_id)
            unit.state = UnitState.ONLINE if online else UnitState.OFFLINE
            self.append_event(
                at_ms,
                EventType.UNIT_ONLINE if online else EventType.UNIT_OFFLINE,
                {"unit_id": unit_id})
            return unit

    # ------------------------------------------------------------------
    # tasks
    # ------------------------------------------------------------------
    @staticmethod
    def validate_spec(spec: TaskSpec) -> None:
        if not spec.steps:
            raise InvalidSpec("task must declare at least one step")
        keys: set[str] = set()
        for st in spec.steps:
            if not st.key:
                raise InvalidSpec("step key must be non-empty")
            if st.key in keys:
                raise InvalidSpec(f"duplicate step key: {st.key}")
            keys.add(st.key)
            if st.capacity_cost < 1:
                raise InvalidSpec(f"step {st.key}: capacity_cost must be >= 1")
            if st.max_retries < 0:
                raise InvalidSpec(f"step {st.key}: max_retries must be >= 0")
        for st in spec.steps:
            for dep in st.depends_on:
                if dep not in keys:
                    raise InvalidSpec(
                        f"step {st.key} depends on unknown step {dep}")
                if dep == st.key:
                    raise InvalidSpec(f"step {st.key} depends on itself")
        # cycle detection (Kahn)
        indeg = {st.key: 0 for st in spec.steps}
        graph: dict[str, list[str]] = {k: [] for k in indeg}
        for st in spec.steps:
            for dep in st.depends_on:
                graph[dep].append(st.key)
                indeg[st.key] += 1
        queue = [k for k, d in indeg.items() if d == 0]
        seen = 0
        while queue:
            n = queue.pop()
            seen += 1
            for m in graph[n]:
                indeg[m] -= 1
                if indeg[m] == 0:
                    queue.append(m)
        if seen != len(indeg):
            raise InvalidSpec("dependency cycle detected")

    def submit_task(self, idem_key: str, spec: TaskSpec,
                    at_ms: int) -> tuple[TaskRecord, bool]:
        """Idempotent insert. Returns (record, created)."""
        if not idem_key:
            raise InvalidSpec("idempotency_key must be non-empty")
        self.validate_spec(spec)
        with self.lock:
            existing = self._idem.get(idem_key)
            if existing is not None:
                return self.tasks[existing], False
            task_id = new_id("task")
            task = TaskRecord(task_id=task_id, idempotency_key=idem_key,
                              spec=spec, created_at_ms=at_ms)
            for st in spec.steps:
                task.steps[st.key] = StepRecord(spec=st)
            self.tasks[task_id] = task
            self._idem[idem_key] = task_id
            self.append_event(at_ms, EventType.TASK_SUBMITTED, {
                "task_id": task_id,
                "idempotency_key": idem_key,
                "priority": spec.priority,
                "step_keys": [s.key for s in spec.steps],
            })
            return task, True

    def get_task(self, task_id: str) -> TaskRecord | None:
        with self.lock:
            return self.tasks.get(task_id)

    def tasks_by_submit_order(self) -> list[TaskRecord]:
        with self.lock:
            return sorted(self.tasks.values(), key=lambda t: t.created_at_ms)
