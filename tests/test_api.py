"""HTTP API tests via FastAPI TestClient.

Covers: unchanged root route, unit management, idempotent task submission,
scheduler-driven execution, state query, cancel, reject reasons, events.
Input requests and engine decisions are printed/logged.
"""
from __future__ import annotations

import time

from fastapi.testclient import TestClient

from fleet.engine import Engine
from fleet.simulator import VirtualClock, WallClock
from fleetapi.app import create_app

from conftest import record_case


def _drive_background(client, engine, task_id, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        v = client.get(f"/api/tasks/{task_id}").json()
        if v["state"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return v
        time.sleep(0.01)
    raise AssertionError("task did not finish in time")


def test_root_route_unchanged():
    with TestClient(create_app()) as c:
        r = c.get("/")
        assert r.status_code == 200
        assert r.json() == {"Hello": "World"}
    record_case("root_route", response={"Hello": "World"})


def test_full_http_happy_path():
    engine = Engine(clock=WallClock())
    with TestClient(create_app(engine)) as c:
        assert c.post("/api/units", json={
            "unit_id": "u1", "capabilities": ["nav", "arm"],
            "capacity_total": 2}).status_code == 201

        payload = {"idempotency_key": "http-1", "steps": [
            {"key": "a", "capability": "nav", "run_ms": 10,
             "timeout_ms": 5000},
            {"key": "b", "capability": "arm", "run_ms": 10,
             "depends_on": ["a"], "timeout_ms": 5000},
        ]}
        r1 = c.post("/api/tasks", json=payload)
        r2 = c.post("/api/tasks", json=payload)
        assert r1.status_code == 201 and r1.json()["created"] is True
        assert r2.json()["created"] is False
        assert r1.json()["task_id"] == r2.json()["task_id"]
        record_case("http_submit_duplicate",
                    idempotency_key="http-1", created=[True, False])

        final = _drive_background(c, engine, r1.json()["task_id"])
        assert final["state"] == "SUCCEEDED"
        assert [s["state"] for s in final["steps"].values()] == \
            ["SUCCEEDED", "SUCCEEDED"]
        unit = c.get("/api/units/u1").json()
        assert unit["capacity_used"] == 0
        events = c.get("/api/events").json()["events"]
        print(f"[TRACE] http happy path: {len(events)} events")
        for ev in events:
            print("  ", ev["seq"], ev["type"], ev["detail"])


def test_http_reject_reasons_distinguishable():
    engine = Engine(clock=VirtualClock())
    with TestClient(create_app(engine), ) as c:  # no background scheduler
        c.post("/api/units", json={
            "unit_id": "u-arm", "capabilities": ["arm"],
            "capacity_total": 1})
        payload = {"idempotency_key": "http-rej", "steps": [
            {"key": "a", "capability": "nav", "timeout_ms": 1000},
            {"key": "b", "capability": "arm", "depends_on": ["a"]},
        ]}
        tid = c.post("/api/tasks", json=payload).json()["task_id"]

        # dependency unmet
        r = c.post(f"/api/tasks/{tid}/steps/b/dispatch",
                   params={"unit_id": "u-arm"})
        assert r.status_code == 409
        assert r.json()["detail"]["reason"] == "DEPENDENCIES_UNMET"

        # capability mismatch
        r = c.post(f"/api/tasks/{tid}/steps/a/dispatch",
                   params={"unit_id": "u-arm"})
        assert r.json()["detail"]["reason"] == "CAPABILITY_MISMATCH"

        # unknown unit
        r = c.post(f"/api/tasks/{tid}/steps/a/dispatch",
                   params={"unit_id": "ghost"})
        assert r.json()["detail"]["reason"] == "UNIT_NOT_FOUND"

        rejects = c.get("/api/rejects").json()["rejects"]
        reasons = {r["reason"] for r in rejects}
        assert {"DEPENDENCIES_UNMET", "CAPABILITY_MISMATCH",
                "UNIT_NOT_FOUND"} <= reasons
        record_case("http_reject_reasons", reasons=sorted(reasons))


def test_http_cancel_and_unit_offline():
    engine = Engine(clock=VirtualClock())
    with TestClient(create_app(engine)) as c:
        c.post("/api/units", json={
            "unit_id": "u1", "capabilities": ["nav"], "capacity_total": 1})
        payload = {"idempotency_key": "http-cancel", "steps": [
            {"key": "a", "capability": "nav", "run_ms": 1000,
             "timeout_ms": 50000}]}
        tid = c.post("/api/tasks", json=payload).json()["task_id"]
        c.post("/api/scheduler/tick")
        assert c.get("/api/units/u1").json()["capacity_used"] == 1
        record_case("http_cancel", task_id=tid)
        r = c.post(f"/api/tasks/{tid}/cancel")
        assert r.json()["state"] == "CANCELLED"
        assert r.json()["steps"]["a"]["state"] == "CANCELLED"
        assert c.get("/api/units/u1").json()["capacity_used"] == 0


def test_http_offline_releases_capacity_then_redispatch():
    engine = Engine(clock=VirtualClock())
    with TestClient(create_app(engine)) as c:
        c.post("/api/units", json={
            "unit_id": "u1", "capabilities": ["nav"], "capacity_total": 1})
        payload = {"idempotency_key": "http-off", "steps": [
            {"key": "a", "capability": "nav", "run_ms": 1000,
             "timeout_ms": 50000}]}
        tid = c.post("/api/tasks", json=payload).json()["task_id"]
        c.post("/api/scheduler/tick")
        assert c.get("/api/units/u1").json()["capacity_used"] == 1
        record_case("http_offline", unit="u1")
        # Offline interrupts the parked attempt and frees capacity at once.
        r = c.post("/api/units/u1/state", json={"online": False})
        assert r.json()["capacity_used"] == 0
        v = c.get(f"/api/tasks/{tid}").json()
        assert v["steps"]["a"]["state"] == "READY"
        assert v["steps"]["a"]["attempts"][0]["outcome"] == "INTERRUPTED"
        # While offline the step cannot be dispatched.
        r = c.post(f"/api/tasks/{tid}/steps/a/dispatch",
                   params={"unit_id": "u1"})
        assert r.json()["detail"]["reason"] == "UNIT_OFFLINE"
        # Back online -> scheduler dispatches again.
        c.post("/api/units/u1/state", json={"online": True})
        c.post("/api/scheduler/tick")
        assert c.get("/api/units/u1").json()["capacity_used"] == 1


def test_http_invalid_spec_400_and_404():
    with TestClient(create_app()) as c:
        r = c.post("/api/tasks", json={
            "idempotency_key": "bad",
            "steps": [{"key": "s", "capability": "nav",
                       "depends_on": ["s"]}]})
        assert r.status_code == 400
        assert c.get("/api/tasks/nope").status_code == 404
        assert c.get("/api/units/nope").status_code == 404
        record_case("http_validation", status=400)
