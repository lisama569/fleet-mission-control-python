"""Domain types: states, specs, records."""
from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field


class StepState(str, enum.Enum):
    PENDING = "PENDING"
    READY = "READY"
    DISPATCHED = "DISPATCHED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"


class TaskState(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class UnitState(str, enum.Enum):
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"


class AttemptOutcome(str, enum.Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    INTERRUPTED = "INTERRUPTED"
    PREEMPTED = "PREEMPTED"
    CANCELLED = "CANCELLED"


class RejectReason(str, enum.Enum):
    DEPENDENCIES_UNMET = "DEPENDENCIES_UNMET"
    CAPABILITY_MISMATCH = "CAPABILITY_MISMATCH"
    CAPACITY_FULL = "CAPACITY_FULL"
    UNIT_OFFLINE = "UNIT_OFFLINE"
    UNIT_NOT_FOUND = "UNIT_NOT_FOUND"
    STEP_NOT_DISPATCHABLE = "STEP_NOT_DISPATCHABLE"


class FailureReason(str, enum.Enum):
    NONE = "NONE"
    RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED"
    NON_RETRYABLE_ERROR = "NON_RETRYABLE_ERROR"
    TIMEOUT_RETRIES_EXHAUSTED = "TIMEOUT_RETRIES_EXHAUSTED"
    UNIT_OFFLINE_NO_CAPACITY = "UNIT_OFFLINE_NO_CAPACITY"


class EventType(str, enum.Enum):
    TASK_SUBMITTED = "TASK_SUBMITTED"
    TASK_DUPLICATE = "TASK_DUPLICATE"
    STEP_READY = "STEP_READY"
    DISPATCH_ATTEMPT = "DISPATCH_ATTEMPT"
    DISPATCH_REJECTED = "DISPATCH_REJECTED"
    DISPATCH_ACCEPTED = "DISPATCH_ACCEPTED"
    ATTEMPT_STARTED = "ATTEMPT_STARTED"
    ATTEMPT_SUCCEEDED = "ATTEMPT_SUCCEEDED"
    ATTEMPT_FAILED = "ATTEMPT_FAILED"
    ATTEMPT_TIMED_OUT = "ATTEMPT_TIMED_OUT"
    ATTEMPT_INTERRUPTED = "ATTEMPT_INTERRUPTED"
    STEP_RETRY_SCHEDULED = "STEP_RETRY_SCHEDULED"
    STEP_FAILED_TERMINAL = "STEP_FAILED_TERMINAL"
    STEP_SUCCEEDED = "STEP_SUCCEEDED"
    STEP_CANCELLED = "STEP_CANCELLED"
    TASK_STATE_CHANGED = "TASK_STATE_CHANGED"
    TASK_CANCELLED = "TASK_CANCELLED"
    CAPACITY_RELEASED = "CAPACITY_RELEASED"
    UNIT_REGISTERED = "UNIT_REGISTERED"
    UNIT_ONLINE = "UNIT_ONLINE"
    UNIT_OFFLINE = "UNIT_OFFLINE"
    STEP_PREEMPTED = "STEP_PREEMPTED"
    EFFECT_RECORDED = "EFFECT_RECORDED"


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


@dataclass
class StepSpec:
    key: str
    capability: str
    capacity_cost: int = 1
    timeout_ms: int = 5000
    max_retries: int = 2
    depends_on: tuple[str, ...] = ()
    behavior: str = "succeed"  # succeed|fail|fail_nonretryable|timeout|flaky
    run_ms: int = 20  # simulated execution duration
    flaky_fail_times: int = 1  # for behavior="flaky"
    fail_message: str = "simulated failure"


@dataclass
class TaskSpec:
    steps: tuple[StepSpec, ...]
    priority: int = 0


@dataclass
class Attempt:
    attempt_no: int
    unit_id: str
    started_at_ms: int
    outcome: AttemptOutcome = AttemptOutcome.RUNNING
    finished_at_ms: int | None = None
    detail: str = ""


@dataclass
class StepRecord:
    spec: StepSpec
    state: StepState = StepState.PENDING
    attempts: list[Attempt] = field(default_factory=list)
    effect_token: str | None = None
    failure_reason: FailureReason = FailureReason.NONE
    failure_detail: str = ""
    last_reject_reason: RejectReason | None = None
    ready_at_ms: int | None = None
    retry_not_before_ms: int | None = None
    assigned_unit: str | None = None

    @property
    def key(self) -> str:
        return self.spec.key

    @property
    def retry_count(self) -> int:
        # Only natural failure/timeout outcomes consume the retry budget;
        # preemption/offline interrupts do not count.
        return len([a for a in self.attempts if a.outcome in
                    (AttemptOutcome.FAILED, AttemptOutcome.TIMED_OUT)])


@dataclass
class TaskRecord:
    task_id: str
    idempotency_key: str
    spec: TaskSpec
    state: TaskState = TaskState.PENDING
    steps: dict[str, StepRecord] = field(default_factory=dict)
    created_at_ms: int = 0
    failure_reason: FailureReason = FailureReason.NONE
    failure_detail: str = ""
    cancel_requested: bool = False
    # Set when a sibling step failed terminally: abort other running steps.
    abort_running: bool = False


@dataclass
class UnitRecord:
    unit_id: str
    capabilities: frozenset[str]
    capacity_total: int
    state: UnitState = UnitState.ONLINE
    capacity_used: int = 0
    running_steps: set[str] = field(default_factory=set)


@dataclass
class Event:
    seq: int
    at_ms: int
    type: EventType
    detail: dict


@dataclass
class RejectRecord:
    at_ms: int
    task_id: str
    step_key: str
    unit_id: str | None
    reason: RejectReason
    detail: str
