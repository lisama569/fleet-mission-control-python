"""Orchestration engine: scheduling, dispatch, retries, capacity.

Concurrency model: one RLock guards all state in FleetStore. Worker threads
only block (simulated execution / watchdog) and take the lock to finalize an
attempt, so dispatch ordering and capacity accounting stay atomic.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from .models import (
    Attempt,
    AttemptOutcome,
    EventType,
    FailureReason,
    RejectReason,
    StepRecord,
    StepState,
    TaskRecord,
    TaskState,
    TaskSpec,
    UnitRecord,
    UnitState,
)
from .simulator import Clock, Simulator, WallClock
from .store import FleetStore


class DispatchRejected(Exception):
    """An explicit dispatch request was refused (distinguishable reason)."""

    def __init__(self, reason, detail: str):
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}")

# Step states that may be (re)dispatched.
DISPATCHABLE = (StepState.READY, StepState.RETRYING)
TERMINAL_STEP_STATES = (
    StepState.SUCCEEDED, StepState.FAILED,
    StepState.TIMED_OUT, StepState.CANCELLED,
)


@dataclass
class _AttemptControl:
    attempt_no: int
    deadline_ms: int
    cancel: threading.Event = field(default_factory=threading.Event)
    preempt: threading.Event = field(default_factory=threading.Event)
    timed_out: threading.Event = field(default_factory=threading.Event)
    watchdog_stop: threading.Event = field(default_factory=threading.Event)


class Engine:
    def __init__(self, clock: Clock | None = None,
                 simulator: Simulator | None = None,
                 retry_backoff_ms: int = 0,
                 tick_interval_ms: int = 20) -> None:
        self.store = FleetStore()
        self.clock = clock or WallClock()
        self.simulator = simulator or Simulator()
        self.retry_backoff_ms = retry_backoff_ms
        self.tick_interval_ms = tick_interval_ms
        self._wakeup = threading.Event()
        self._stop = threading.Event()
        self._controls: dict[str, _AttemptControl] = {}
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self.run_forever, name="fleet-scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wakeup.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def run_forever(self) -> None:
        while not self._stop.is_set():
            self.tick()
            self._wakeup.wait(self.tick_interval_ms / 1000.0)
            self._wakeup.clear()

    def tick(self) -> dict:
        """One scheduling pass. Returns counters (used by tests and API)."""
        with self.store.lock:
            now = self.clock.now_ms()
            counts = self._promote_steps_locked(now)
            counts.update(self._schedule_locked(now))
            for task in self.store.tasks.values():
                self._recompute_task_state_locked(task, now)
            return counts

    def wait_idle(self, timeout: float = 2.0) -> bool:
        """Wait until no attempt threads are running (test helper)."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.store.lock:
                idle = not self._controls
            if idle:
                return True
            time.sleep(0.005)
        with self.store.lock:
            return not self._controls

    def drive_until_idle(self, max_ms: int = 60000) -> int:
        """Deterministic scheduler for VirtualClock-driven tests.

        Each iteration jumps virtual time to the next relevant deadline
        (nearest attempt completion or watchdog timeout) instead of stepping
        1ms at a time. Returns the total virtual time advanced.
        """
        advanced = 0
        idle_loops = 0
        while advanced < max_ms:
            counts = self.tick()
            with self.store.lock:
                active = list(self._controls.items())
            if not active:
                if counts.get("dispatched") or counts.get("promoted"):
                    idle_loops = 0
                    continue
                idle_loops += 1
                if idle_loops >= 2:
                    break
                continue
            idle_loops = 0
            now = self.clock.now_ms()
            jumps: list[int] = []
            for key, ctrl in active:
                tid, _, sk = key.partition(":")
                with self.store.lock:
                    step = self.store.tasks[tid].steps[sk]
                    started = step.attempts[ctrl.attempt_no - 1].started_at_ms
                    behavior = step.spec.behavior
                    run_ms = step.spec.run_ms
                if behavior == "timeout":
                    jumps.append(max(1, ctrl.deadline_ms - now))
                else:
                    jumps.append(max(1, min(run_ms,
                                           ctrl.deadline_ms - now)))
            delta = min(jumps)
            self.clock.wait_for_waiters(len(active) * 2)
            self.clock.advance(delta)
            advanced += delta
            if not self.wait_idle(2.0):
                continue
        self.tick()
        return advanced

    # ------------------------------------------------------------------
    # units
    # ------------------------------------------------------------------
    def register_unit(self, unit_id: str | None = None,
                      capabilities: frozenset[str] = frozenset(),
                      capacity_total: int = 1) -> UnitRecord:
        now = self.clock.now_ms()
        unit = self.store.register_unit(
            unit_id, frozenset(capabilities), capacity_total, now)
        self._wakeup.set()
        return unit

    def set_unit_online(self, unit_id: str, online: bool) -> UnitRecord:
        now = self.clock.now_ms()
        with self.store.lock:
            unit = self.store.set_unit_state(unit_id, online, now)
            if not online:
                # Interrupt and synchronously settle every attempt that is
                # running on the offline unit, releasing its capacity.
                for ckey, ctrl in list(self._controls.items()):
                    tid, _, step_key = ckey.partition(":")
                    if self._running_unit_locked(tid, step_key) == unit_id:
                        ctrl.cancel.set()
                        self._force_finalize_locked(
                            tid, step_key, AttemptOutcome.INTERRUPTED,
                            "unit went offline", now)
                self.clock.wake_all()
            else:
                self._wakeup.set()
            return unit

    # ------------------------------------------------------------------
    # tasks
    # ------------------------------------------------------------------
    def submit(self, idem_key: str, spec: TaskSpec) -> tuple[TaskRecord, bool]:
        now = self.clock.now_ms()
        task, created = self.store.submit_task(idem_key, spec, now)
        if created:
            self._wakeup.set()
        else:
            self.store.append_event(now, EventType.TASK_DUPLICATE, {
                "task_id": task.task_id,
                "idempotency_key": idem_key,
            })
        return task, created

    def cancel_task(self, task_id: str) -> TaskRecord:
        now = self.clock.now_ms()
        with self.store.lock:
            task = self._require_task_locked(task_id)
            if task.state in (TaskState.SUCCEEDED, TaskState.FAILED,
                              TaskState.CANCELLED):
                return task
            task.cancel_requested = True
            self.store.append_event(now, EventType.TASK_CANCELLED, {
                "task_id": task_id,
            })
            for step in task.steps.values():
                ctrl = self._controls.get(f"{task_id}:{step.key}")
                if ctrl is not None:
                    ctrl.cancel.set()
                    # Settle parked attempts immediately so capacity is
                    # released before the API call returns.
                    self._force_finalize_locked(
                        task_id, step.key,
                        AttemptOutcome.CANCELLED, "task cancelled", now)
                elif step.state in (StepState.PENDING, StepState.READY,
                                    StepState.RETRYING):
                    self._cancel_step_locked(task, step, now,
                                             "task cancelled before start")
            self.clock.wake_all()
            self._recompute_task_state_locked(task, now)
            return task

    def preempt_task(self, task_id: str) -> TaskRecord:
        """Release capacity held by running steps; they go back to READY."""
        now = self.clock.now_ms()
        with self.store.lock:
            task = self._require_task_locked(task_id)
            if task.state in (TaskState.SUCCEEDED, TaskState.FAILED):
                return task
            for step in task.steps.values():
                ctrl = self._controls.get(f"{task_id}:{step.key}")
                if ctrl is not None:
                    ctrl.preempt.set()
                    ctrl.cancel.set()
                    self._force_finalize_locked(
                        task_id, step.key, AttemptOutcome.INTERRUPTED,
                        "preempted by operator", now)
            self.clock.wake_all()
            self._recompute_task_state_locked(task, now)
            self._wakeup.set()
            return task

    def dispatch_step(self, task_id: str, step_key: str,
                      unit_id: str | None = None) -> dict:
        """Explicitly dispatch one step now.

        Raises DispatchRejected with a specific RejectReason otherwise.
        """
        now = self.clock.now_ms()
        with self.store.lock:
            task = self.store.tasks.get(task_id)
            if task is None or step_key not in task.steps:
                raise DispatchRejected(
                    RejectReason.STEP_NOT_DISPATCHABLE,
                    f"step {task_id}/{step_key} does not exist")
            step = task.steps[step_key]
            if step.state is StepState.PENDING:
                unmet = [d for d in step.spec.depends_on
                         if task.steps[d].state is not StepState.SUCCEEDED]
                if unmet:
                    self._reject_explicit(
                        task_id, step_key, None,
                        RejectReason.DEPENDENCIES_UNMET,
                        f"unmet dependencies: {unmet}", now)
                # Eligible: promote inline as a tick would.
                step.state = StepState.READY
                step.ready_at_ms = now
                self.store.append_event(now, EventType.STEP_READY, {
                    "task_id": task_id, "step_key": step_key,
                    "deps": list(step.spec.depends_on), "explicit": True,
                })
            if step.state not in DISPATCHABLE:
                self._reject_explicit(
                    task_id, step_key, None,
                    RejectReason.STEP_NOT_DISPATCHABLE,
                    f"step state is {step.state.value}", now)
            if f"{task_id}:{step_key}" in self._controls:
                self._reject_explicit(
                    task_id, step_key, step.assigned_unit,
                    RejectReason.CAPACITY_FULL,
                    "previous attempt has not released capacity yet", now)
            nbf = step.retry_not_before_ms
            if nbf is not None and now < nbf:
                self._reject_explicit(
                    task_id, step_key, None, RejectReason.CAPACITY_FULL,
                    f"retry backoff active until {nbf}ms", now)

            chosen: UnitRecord | None
            if unit_id is not None:
                chosen = self.store.units.get(unit_id)
                if chosen is None:
                    self._reject_explicit(
                        task_id, step_key, unit_id,
                        RejectReason.UNIT_NOT_FOUND,
                        f"unit {unit_id} is not registered", now)
                if chosen.state is UnitState.OFFLINE:
                    self._reject_explicit(
                        task_id, step_key, unit_id, RejectReason.UNIT_OFFLINE,
                        f"unit {unit_id} is OFFLINE", now)
                if step.spec.capability not in chosen.capabilities:
                    self._reject_explicit(
                        task_id, step_key, unit_id,
                        RejectReason.CAPABILITY_MISMATCH,
                        f"unit {unit_id} lacks capability "
                        f"{step.spec.capability!r}", now)
                if (chosen.capacity_used + step.spec.capacity_cost
                        > chosen.capacity_total):
                    self._reject_explicit(
                        task_id, step_key, unit_id, RejectReason.CAPACITY_FULL,
                        f"unit {unit_id} has "
                        f"{chosen.capacity_total - chosen.capacity_used} "
                        f"free slot(s), needs {step.spec.capacity_cost}", now)
            else:
                chosen, reason, detail = self._select_unit_locked(step)
                if chosen is None:
                    self._reject_explicit(task_id, step_key, None,
                                          reason, detail, now)

            attempt_no = len(step.attempts) + 1
            attempt = Attempt(attempt_no=attempt_no, unit_id=chosen.unit_id,
                              started_at_ms=now)
            step.attempts.append(attempt)
            step.state = StepState.DISPATCHED
            step.assigned_unit = chosen.unit_id
            step.last_reject_reason = None
            chosen.capacity_used += step.spec.capacity_cost
            chosen.running_steps.add(f"{task_id}:{step_key}")
            self.store.append_event(now, EventType.DISPATCH_ACCEPTED, {
                "task_id": task_id, "step_key": step_key,
                "unit_id": chosen.unit_id, "attempt_no": attempt_no,
                "capacity_used": chosen.capacity_used,
                "capacity_total": chosen.capacity_total,
                "explicit": True,
            })
            self._recompute_task_state_locked(task, now)
            self._launch_attempt(task_id, step_key, chosen.unit_id,
                                 attempt_no)
            return {"task_id": task_id, "step_key": step_key,
                    "unit_id": chosen.unit_id, "attempt_no": attempt_no}

    def _reject_explicit(self, task_id: str, step_key: str,
                         unit_id: str | None, reason, detail: str,
                         now: int) -> None:
        step = self.store.tasks[task_id].steps[step_key]
        step.last_reject_reason = reason
        self.store.log_reject(now, task_id, step_key, unit_id, reason, detail)
        self.store.append_event(now, EventType.DISPATCH_REJECTED, {
            "task_id": task_id, "step_key": step_key,
            "unit_id": unit_id, "reason": reason.value,
            "detail": detail, "explicit": True,
        })
        raise DispatchRejected(reason, detail)

    # ------------------------------------------------------------------
    # views
    # ------------------------------------------------------------------
    def task_view(self, task_id: str) -> dict:
        with self.store.lock:
            task = self._require_task_locked(task_id)
            return self._task_view_locked(task)

    def list_tasks(self) -> list[dict]:
        with self.store.lock:
            return [self._task_view_locked(t)
                    for t in self.store.tasks_by_submit_order()]

    def unit_view(self, unit_id: str) -> dict:
        with self.store.lock:
            unit = self.store.units.get(unit_id)
            if unit is None:
                raise KeyError(unit_id)
            return self._unit_view_locked(unit)

    def list_units(self) -> list[dict]:
        with self.store.lock:
            return [self._unit_view_locked(u)
                    for u in self.store.units.values()]

    def recent_rejects(self) -> list[dict]:
        with self.store.lock:
            return [vars(r) | {"reason": r.reason.value}
                    for r in self.store.rejects]

    def events_since(self, seq: int = 0) -> list[dict]:
        with self.store.lock:
            out = []
            for ev in self.store.events:
                if ev.seq > seq:
                    out.append({"seq": ev.seq, "at_ms": ev.at_ms,
                                "type": ev.type.value, "detail": ev.detail})
            return out

    # ==================================================================
    # internals: state promotion & scheduling
    # ==================================================================
    def _promote_steps_locked(self, now: int) -> dict:
        """PENDING -> READY (deps met) or CANCELLED (deps failed)."""
        promoted = 0
        for task in self.store.tasks.values():
            if task.state in (TaskState.SUCCEEDED, TaskState.FAILED,
                              TaskState.CANCELLED):
                continue
            for step in task.steps.values():
                if task.cancel_requested and step.state in (
                        StepState.PENDING, StepState.READY,
                        StepState.RETRYING):
                    self._cancel_step_locked(task, step, now,
                                             "task cancelled before start")
                    continue
                if step.state is not StepState.PENDING:
                    continue
                dep_states = [task.steps[d].state for d in step.spec.depends_on]
                failed_dep = next(
                    (d for d in step.spec.depends_on
                     if task.steps[d].state in TERMINAL_STEP_STATES
                     and task.steps[d].state is not StepState.SUCCEEDED),
                    None)
                if failed_dep is not None:
                    self._cancel_step_locked(
                        task, step, now,
                        f"dependency {failed_dep} did not succeed")
                    continue
                if all(s is StepState.SUCCEEDED for s in dep_states):
                    step.state = StepState.READY
                    step.ready_at_ms = now
                    step.retry_not_before_ms = None
                    promoted += 1
                    self.store.append_event(now, EventType.STEP_READY, {
                        "task_id": task.task_id,
                        "step_key": step.key,
                        "deps": list(step.spec.depends_on),
                    })
        return {"promoted": promoted}

    def _schedule_locked(self, now: int) -> dict:
        dispatched = 0
        rejected = 0
        launches: list[tuple[str, str, str, int]] = []

        candidates: list[tuple[int, int, int, str, str, StepRecord]] = []
        for order, task in enumerate(self.store.tasks_by_submit_order()):
            if task.cancel_requested or task.state in (
                    TaskState.SUCCEEDED, TaskState.FAILED,
                    TaskState.CANCELLED):
                continue
            for idx, step in enumerate(task.spec.steps):
                rec = task.steps[step.key]
                if rec.state not in DISPATCHABLE:
                    continue
                if f"{task.task_id}:{step.key}" in self._controls:
                    # A previous (preempted) attempt has not released yet.
                    continue
                nbf = rec.retry_not_before_ms
                if nbf is not None and now < nbf:
                    continue
                candidates.append(
                    (-task.spec.priority, order, idx,
                     task.task_id, step.key, rec))
        candidates.sort(key=lambda c: (c[0], c[1], c[2]))

        for _, _, _, task_id, step_key, step in candidates:
            task = self.store.tasks[task_id]
            chosen, reason, detail = self._select_unit_locked(step)
            if chosen is None:
                rejected += 1
                step.last_reject_reason = reason
                self.store.log_reject(now, task_id, step_key, None,
                                      reason, detail)
                self.store.append_event(now, EventType.DISPATCH_REJECTED, {
                    "task_id": task_id, "step_key": step_key,
                    "reason": reason.value, "detail": detail,
                    "capability": step.spec.capability,
                })
                continue
            attempt_no = len(step.attempts) + 1
            attempt = Attempt(attempt_no=attempt_no, unit_id=chosen.unit_id,
                              started_at_ms=now)
            step.attempts.append(attempt)
            step.state = StepState.DISPATCHED
            step.assigned_unit = chosen.unit_id
            step.last_reject_reason = None
            chosen.capacity_used += step.spec.capacity_cost
            chosen.running_steps.add(f"{task_id}:{step_key}")
            dispatched += 1
            self.store.append_event(now, EventType.DISPATCH_ACCEPTED, {
                "task_id": task_id, "step_key": step_key,
                "unit_id": chosen.unit_id, "attempt_no": attempt_no,
                "capacity_used": chosen.capacity_used,
                "capacity_total": chosen.capacity_total,
            })
            launches.append((task_id, step_key, chosen.unit_id, attempt_no))

        for launch in launches:
            self._launch_attempt(*launch)
        return {"dispatched": dispatched, "rejected": rejected}

    def _select_unit_locked(self, step: StepRecord):
        """Return (unit, None, '') or (None, reason, detail)."""
        cap = step.spec.capability
        cost = step.spec.capacity_cost
        capable = [u for u in self.store.units.values()
                   if cap in u.capabilities]
        if not capable:
            return None, RejectReason.CAPABILITY_MISMATCH, (
                f"no unit advertises capability {cap!r}")
        online = [u for u in capable if u.state is UnitState.ONLINE]
        if not online:
            return None, RejectReason.UNIT_OFFLINE, (
                f"all {len(capable)} capable unit(s) are offline "
                f"for capability {cap!r}")
        fitting = [u for u in online
                   if u.capacity_used + cost <= u.capacity_total]
        if not fitting:
            totals = ", ".join(
                f"{u.unit_id}:{u.capacity_used}/{u.capacity_total}"
                for u in online)
            return None, RejectReason.CAPACITY_FULL, (
                f"no online capable unit has {cost} free slot(s) [{totals}]")
        return fitting[0], None, ""

    # ==================================================================
    # internals: attempt execution
    # ==================================================================
    def _launch_attempt(self, task_id: str, step_key: str,
                        unit_id: str, attempt_no: int) -> None:
        with self.store.lock:
            task = self.store.tasks[task_id]
            step = task.steps[step_key]
            now = self.clock.now_ms()
            ctrl = _AttemptControl(
                attempt_no=attempt_no,
                deadline_ms=now + step.spec.timeout_ms)
            self._controls[f"{task_id}:{step_key}"] = ctrl
            attempt = step.attempts[attempt_no - 1]
            step.state = StepState.RUNNING
            self.store.append_event(now, EventType.ATTEMPT_STARTED, {
                "task_id": task_id, "step_key": step_key,
                "unit_id": unit_id, "attempt_no": attempt_no,
                "deadline_ms": ctrl.deadline_ms,
            })

        def _watchdog() -> None:
            remaining = max(0, ctrl.deadline_ms - self.clock.now_ms())
            interrupted = self.clock.sleep_ms(remaining, ctrl.watchdog_stop)
            if interrupted is False:
                return
            ctrl.timed_out.set()
            ctrl.cancel.set()

        t = threading.Thread(
            target=self._attempt_worker,
            args=(task_id, step_key, unit_id, attempt_no, ctrl),
            name=f"attempt-{task_id}-{step_key}-{attempt_no}", daemon=True)
        w = threading.Thread(target=_watchdog, name=f"watchdog-{step_key}",
                             daemon=True)
        w.start()
        t.start()

    def _attempt_worker(self, task_id: str, step_key: str, unit_id: str,
                        attempt_no: int, ctrl: _AttemptControl) -> None:
        with self.store.lock:
            spec = self.store.tasks[task_id].steps[step_key].spec
        outcome, detail = self.simulator.run_attempt(
            spec, ctrl.cancel, self.clock)
        ctrl.watchdog_stop.set()
        now = self.clock.now_ms()
        with self.store.lock:
            self._finalize_attempt_locked(task_id, step_key, unit_id,
                                          attempt_no, ctrl, outcome,
                                          detail, now)
        # Prompt the watchdog (parked on the virtual clock) to notice the
        # stopped flag; harmless for the wall clock.
        self.clock.wake_all()
        self._wakeup.set()

    def _force_finalize_locked(self, task_id: str, step_key: str,
                               outcome, detail: str, now: int) -> None:
        """Synchronously settle an in-flight attempt from cancel/offline.

        Used when the worker is parked on a virtual clock and cannot unwind
        on its own: releases capacity and sets the terminal state at once.
        The real worker later no-ops because its control is gone.
        """
        key = f"{task_id}:{step_key}"
        ctrl = self._controls.get(key)
        if ctrl is None:
            return
        step = self.store.tasks[task_id].steps[step_key]
        unit_id = step.attempts[ctrl.attempt_no - 1].unit_id
        ctrl.watchdog_stop.set()
        self._finalize_attempt_locked(
            task_id, step_key, unit_id, ctrl.attempt_no, ctrl,
            outcome, detail, now)

    def _finalize_attempt_locked(self, task_id: str, step_key: str,
                                 unit_id: str, attempt_no: int,
                                 ctrl: _AttemptControl, outcome,
                                 detail: str, now: int) -> None:
        key = f"{task_id}:{step_key}"
        if self._controls.get(key) is not ctrl:
            return  # superseded (should not happen); do not double-finalize
        del self._controls[key]

        task = self.store.tasks[task_id]
        step = task.steps[step_key]
        attempt = step.attempts[attempt_no - 1]
        unit = self.store.units[unit_id]

        def _release() -> None:
            token = f"{task_id}:{step_key}"
            if token in unit.running_steps:
                unit.running_steps.remove(token)
            unit.capacity_used = max(
                0, unit.capacity_used - step.spec.capacity_cost)
            self.store.append_event(now, EventType.CAPACITY_RELEASED, {
                "task_id": task_id, "step_key": step_key,
                "unit_id": unit_id,
                "capacity_used": unit.capacity_used,
                "capacity_total": unit.capacity_total,
            })

        attempt.finished_at_ms = now

        # 1) task cancellation wins
        if task.cancel_requested:
            attempt.outcome = AttemptOutcome.CANCELLED
            attempt.detail = "task cancelled"
            _release()
            step.state = StepState.CANCELLED
            step.assigned_unit = None
            self.store.append_event(now, EventType.STEP_CANCELLED, {
                "task_id": task_id, "step_key": step_key,
                "attempt_no": attempt_no,
            })
        # 1b) aborted by a sibling's terminal failure
        elif task.abort_running:
            attempt.outcome = AttemptOutcome.CANCELLED
            attempt.detail = "aborted after sibling failure"
            _release()
            step.state = StepState.CANCELLED
            step.assigned_unit = None
            self.store.append_event(now, EventType.STEP_CANCELLED, {
                "task_id": task_id, "step_key": step_key,
                "attempt_no": attempt_no,
                "detail": "aborted after sibling failure",
            })
        # 2) explicit preemption: release capacity, step becomes eligible
        elif ctrl.preempt.is_set():
            attempt.outcome = AttemptOutcome.PREEMPTED
            attempt.detail = "preempted by operator"
            _release()
            step.state = StepState.READY
            step.assigned_unit = None
            step.retry_not_before_ms = None
            self.store.append_event(now, EventType.STEP_PREEMPTED, {
                "task_id": task_id, "step_key": step_key,
                "unit_id": unit_id, "attempt_no": attempt_no,
            })
        # 3) execution unit went offline mid-flight: interrupt, no retry cost
        elif unit.state is UnitState.OFFLINE:
            attempt.outcome = AttemptOutcome.INTERRUPTED
            attempt.detail = "unit went offline"
            _release()
            step.state = StepState.READY
            step.assigned_unit = None
            step.retry_not_before_ms = None
            self.store.append_event(now, EventType.ATTEMPT_INTERRUPTED, {
                "task_id": task_id, "step_key": step_key,
                "unit_id": unit_id, "attempt_no": attempt_no,
            })
        # 4) timeout
        elif ctrl.timed_out.is_set():
            attempt.outcome = AttemptOutcome.TIMED_OUT
            attempt.detail = (f"exceeded timeout_ms={step.spec.timeout_ms}")
            _release()
            self._handle_retryable_end_locked(
                task, step, now, attempt_no, timed_out=True,
                detail=attempt.detail)
        # 5) natural simulator outcome
        elif outcome is AttemptOutcome.SUCCEEDED:
            attempt.outcome = AttemptOutcome.SUCCEEDED
            attempt.detail = detail
            _release()
            # Exactly-once side-effect accounting.
            effect_token = f"{task_id}:{step_key}"
            newly_recorded = effect_token not in self.store.effects
            if newly_recorded:
                self.store.effects.add(effect_token)
            step.state = StepState.SUCCEEDED
            step.assigned_unit = None
            step.effect_token = effect_token
            self.store.append_event(now, EventType.EFFECT_RECORDED, {
                "task_id": task_id, "step_key": step_key,
                "effect_token": effect_token,
                "dedup": not newly_recorded,
            })
            self.store.append_event(now, EventType.STEP_SUCCEEDED, {
                "task_id": task_id, "step_key": step_key,
                "attempt_no": attempt_no,
            })
        elif outcome is AttemptOutcome.FAILED:
            attempt.outcome = AttemptOutcome.FAILED
            attempt.detail = detail
            _release()
            non_retryable = step.spec.behavior == "fail_nonretryable"
            self._handle_retryable_end_locked(
                task, step, now, attempt_no, timed_out=False,
                detail=detail, non_retryable=non_retryable)
        else:  # INTERRUPTED with no offline/preempt/cancel reason
            attempt.outcome = AttemptOutcome.INTERRUPTED
            attempt.detail = detail
            _release()
            step.state = StepState.READY
            step.assigned_unit = None
            step.retry_not_before_ms = None
            self.store.append_event(now, EventType.ATTEMPT_INTERRUPTED, {
                "task_id": task_id, "step_key": step_key,
                "unit_id": unit_id, "attempt_no": attempt_no,
            })

        self._recompute_task_state_locked(task, now)

    def _handle_retryable_end_locked(self, task, step, now, attempt_no,
                                     timed_out: bool, detail: str,
                                     non_retryable: bool = False) -> None:
        # Budget accounting is based on failure/timeout outcomes only;
        # interrupted/preempted attempts never consume the retry budget.
        attempts_used = step.retry_count  # this attempt is now recorded
        max_attempts = 1 + step.spec.max_retries
        if non_retryable:
            step.state = StepState.FAILED
            step.failure_reason = FailureReason.NON_RETRYABLE_ERROR
            step.failure_detail = detail
            step.assigned_unit = None
            self._terminal_failure_event(task, step, now, attempt_no)
            return
        if attempts_used >= max_attempts:
            if timed_out:
                step.state = StepState.TIMED_OUT
                step.failure_reason = FailureReason.TIMEOUT_RETRIES_EXHAUSTED
            else:
                step.state = StepState.FAILED
                step.failure_reason = FailureReason.RETRIES_EXHAUSTED
            step.failure_detail = (
                f"{detail} (failed attempts {attempts_used}/{max_attempts})")
            step.assigned_unit = None
            self._terminal_failure_event(task, step, now, attempt_no)
            return
        step.state = StepState.RETRYING
        step.assigned_unit = None
        step.retry_not_before_ms = now + self.retry_backoff_ms
        etype = (EventType.ATTEMPT_TIMED_OUT if timed_out
                 else EventType.ATTEMPT_FAILED)
        self.store.append_event(now, etype, {
            "task_id": task.task_id, "step_key": step.key,
            "attempt_no": attempt_no,
            "failed_attempts": attempts_used,
            "max_attempts": max_attempts,
            "detail": detail,
        })
        self.store.append_event(now, EventType.STEP_RETRY_SCHEDULED, {
            "task_id": task.task_id, "step_key": step.key,
            "next_attempt_no": attempts_used + 1,
            "not_before_ms": step.retry_not_before_ms,
        })

    def _terminal_failure_event(self, task, step, now, attempt_no) -> None:
        self.store.append_event(now, EventType.STEP_FAILED_TERMINAL, {
            "task_id": task.task_id, "step_key": step.key,
            "attempt_no": attempt_no,
            "step_state": step.state.value,
            "failure_reason": step.failure_reason.value,
            "failure_detail": step.failure_detail,
        })

    # ==================================================================
    # internals: cancellation, task aggregation, views
    # ==================================================================
    def _cancel_step_locked(self, task, step, now, detail) -> None:
        if step.state in TERMINAL_STEP_STATES:
            return
        step.state = StepState.CANCELLED
        step.assigned_unit = None
        self.store.append_event(now, EventType.STEP_CANCELLED, {
            "task_id": task.task_id, "step_key": step.key,
            "detail": detail,
        })

    def _recompute_task_state_locked(self, task: TaskRecord, now: int) -> None:
        old = task.state
        states = [s.state for s in task.steps.values()]
        terminal_bad = next(
            (s for s in task.steps.values()
             if s.state in (StepState.FAILED, StepState.TIMED_OUT)),
            None)

        if terminal_bad is not None:
            new_state = TaskState.FAILED
            task.failure_reason = terminal_bad.failure_reason
            task.failure_detail = (
                f"step {terminal_bad.key}: {terminal_bad.failure_detail}")
            # Cascade: a terminal step failure settles every other step.
            for other in task.steps.values():
                if other is terminal_bad or other.state in TERMINAL_STEP_STATES:
                    continue
                ctrl = self._controls.get(f"{task.task_id}:{other.key}")
                if ctrl is not None:
                    task.abort_running = True
                    ctrl.cancel.set()  # running independent step
                elif other.state in (StepState.PENDING, StepState.READY,
                                     StepState.RETRYING):
                    self._cancel_step_locked(
                        task, other, now,
                        f"task failed at step {terminal_bad.key}")
        elif task.cancel_requested and all(
                s in TERMINAL_STEP_STATES for s in states):
            new_state = TaskState.CANCELLED
        elif all(s is StepState.SUCCEEDED for s in states):
            new_state = TaskState.SUCCEEDED
        elif all(s is StepState.CANCELLED for s in states):
            new_state = TaskState.CANCELLED
        elif any(s in (StepState.DISPATCHED, StepState.RUNNING,
                       StepState.READY, StepState.RETRYING)
                 for s in states):
            new_state = TaskState.RUNNING
        else:  # only PENDING (+ maybe CANCELLED) steps remain
            new_state = TaskState.PENDING
        if new_state != old:
            task.state = new_state
            self.store.append_event(now, EventType.TASK_STATE_CHANGED, {
                "task_id": task.task_id,
                "old_state": old.value,
                "new_state": new_state.value,
            })

    def _running_unit_locked(self, task_id: str, step_key: str):
        task = self.store.tasks.get(task_id)
        if task is None:
            return None
        return task.steps[step_key].assigned_unit

    def _require_task_locked(self, task_id: str) -> TaskRecord:
        task = self.store.tasks.get(task_id)
        if task is None:
            raise KeyError(task_id)
        return task

    def _task_view_locked(self, task: TaskRecord) -> dict:
        return {
            "task_id": task.task_id,
            "idempotency_key": task.idempotency_key,
            "state": task.state.value,
            "priority": task.spec.priority,
            "failure_reason": task.failure_reason.value,
            "failure_detail": task.failure_detail,
            "cancel_requested": task.cancel_requested,
            "created_at_ms": task.created_at_ms,
            "steps": {
                key: {
                    "key": key,
                    "state": rec.state.value,
                    "capability": rec.spec.capability,
                    "capacity_cost": rec.spec.capacity_cost,
                    "depends_on": list(rec.spec.depends_on),
                    "assigned_unit": rec.assigned_unit,
                    "attempts": [
                        {"attempt_no": a.attempt_no,
                         "unit_id": a.unit_id,
                         "started_at_ms": a.started_at_ms,
                         "finished_at_ms": a.finished_at_ms,
                         "outcome": a.outcome.value,
                         "detail": a.detail}
                        for a in rec.attempts],
                    "effect_token": rec.effect_token,
                    "failure_reason": rec.failure_reason.value,
                    "failure_detail": rec.failure_detail,
                    "last_reject_reason": (rec.last_reject_reason.value
                                           if rec.last_reject_reason else None),
                    "retry_count": rec.retry_count,
                }
                for key, rec in task.steps.items()
            },
        }

    def _unit_view_locked(self, unit: UnitRecord) -> dict:
        return {
            "unit_id": unit.unit_id,
            "state": unit.state.value,
            "capabilities": sorted(unit.capabilities),
            "capacity_total": unit.capacity_total,
            "capacity_used": unit.capacity_used,
            "capacity_free": unit.capacity_total - unit.capacity_used,
            "running_steps": sorted(unit.running_steps),
        }
