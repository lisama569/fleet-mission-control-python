"""Pydantic request/response models for the HTTP API."""
from __future__ import annotations

from pydantic import BaseModel, Field


class StepIn(BaseModel):
    key: str
    capability: str
    capacity_cost: int = 1
    timeout_ms: int = 5000
    max_retries: int = 2
    depends_on: list[str] = Field(default_factory=list)
    behavior: str = "succeed"
    run_ms: int = 20
    flaky_fail_times: int = 1
    fail_message: str = "simulated failure"


class SubmitIn(BaseModel):
    idempotency_key: str
    priority: int = 0
    steps: list[StepIn]


class UnitIn(BaseModel):
    unit_id: str | None = None
    capabilities: list[str]
    capacity_total: int = 1


class UnitStateIn(BaseModel):
    online: bool


class ApiError(BaseModel):
    error: str
    detail: str = ""
