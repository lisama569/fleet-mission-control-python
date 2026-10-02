"""HTTP API 测试：既有接口回归 + 新接口端到端。"""

from __future__ import annotations

import warnings

import pytest

warnings.filterwarnings("ignore")

from fastapi.testclient import TestClient

from fleet.api import create_app
from fleet.service import FleetService


@pytest.fixture
def client():
    svc = FleetService()
    app = create_app(svc)
    with TestClient(app) as c:
        yield c, svc


def test_legacy_root_unchanged(client):
    c, _ = client
    print("\nINPUT: GET / (legacy contract)")
    r = c.get("/")
    print(f"JUDGEMENT: {r.status_code} {r.json()}")
    assert r.status_code == 200
    assert r.json() == {"Hello": "World"}


def test_submit_query_duplicate_cancel_flow(client):
    c, svc = client
    # 装配单元
    r = c.post("/units", json={"unit_id": "u1", "capabilities": ["arm"], "capacity": 1})
    assert r.status_code == 201

    body = {
        "task_id": "api-1",
        "steps": [
            {"id": "a", "required_capability": "arm", "timeout": 5.0,
             "max_retries": 1, "outcomes": ["timeout", "succeeded"],
             "delay": 0.05, "side_effects": 2},
            {"id": "b", "required_capability": "arm", "depends_on": ["a"]},
        ],
    }
    r1 = c.post("/tasks", json=body).json()
    r2 = c.post("/tasks", json=body).json()
    print(f"\nINPUT/JUDGEMENT: created flags {r1['created']}, {r2['created']}")
    assert r1["created"] is True and r2["created"] is False

    import time
    for _ in range(200):
        v = c.get("/tasks/api-1").json()
        if v["state"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.02)
    print("FINAL:", v["state"], [(s["id"], s["state"], s["attempts"]) for s in v["steps"]])
    assert v["state"] == "SUCCEEDED"
    a = next(s for s in v["steps"] if s["id"] == "a")
    assert a["attempts"] == 2 and a["side_effects"] == 2

    # 取消不存在的任务 -> 404
    assert c.post("/tasks/nope/cancel").status_code == 404


def test_rejection_reasons_via_api(client):
    c, _ = client
    c.post("/units", json={"unit_id": "u1", "capabilities": ["arm"], "capacity": 1})

    # 非法声明 -> 422（环）
    bad = {"task_id": "bad", "steps": [
        {"id": "a", "required_capability": "arm", "depends_on": ["b"]},
        {"id": "b", "required_capability": "arm", "depends_on": ["a"]},
    ]}
    assert c.post("/tasks", json=bad).status_code == 422

    # 能力不匹配：步骤保持 PENDING，last_reject 可区分
    c.post("/tasks", json={"task_id": "cam", "steps": [
        {"id": "s", "required_capability": "camera", "timeout": 5.0}]})
    v = c.get("/tasks/cam").json()
    assert v["steps"][0]["state"] == "PENDING"
    assert v["steps"][0]["last_reject"] == "CAPABILITY_MISMATCH"


def test_units_endpoint_and_offline(client):
    c, _ = client
    c.post("/units", json={"unit_id": "u1", "capabilities": ["arm"], "capacity": 2})
    units = c.get("/units").json()["units"]
    assert any(u["unit_id"] == "u1" and u["capacity"] == 2 for u in units)

    # 长任务占住 u1，离线后占用必须释放
    c.post("/tasks", json={"task_id": "x", "steps": [
        {"id": "s", "required_capability": "arm", "timeout": 5.0,
         "delay": 0.5, "max_retries": 3}]})
    import time
    time.sleep(0.05)
    assert c.post("/units/u1/offline").status_code == 200
    u1 = next(u for u in c.get("/units").json()["units"] if u["unit_id"] == "u1")
    print("\nJUDGEMENT after offline:", u1)
    assert u1["online"] is False and u1["used"] == 0


def test_legacy_submit_without_priority_defaults_normal(client):
    """旧请求体（无 priority 字段）行为不变：默认 NORMAL，旧响应字段保留。"""
    c, _ = client
    c.post("/units", json={"unit_id": "u1", "capabilities": ["arm"], "capacity": 3})
    # 完全旧式的请求体：没有 priority
    body = {"task_id": "legacy", "steps": [
        {"id": "a", "required_capability": "arm"},
        {"id": "b", "required_capability": "arm", "depends_on": ["a"]},
    ]}
    r = c.post("/tasks", json=body)
    assert r.status_code == 200
    payload = r.json()
    task = payload["task"]
    # 新增字段
    assert task["priority"] == "NORMAL"
    assert task["submit_seq"] >= 1
    # 旧字段原样保留
    assert set(["task_id", "state", "rejection", "steps"]).issubset(task)
    step = task["steps"][0]
    for legacy_field in ("id", "state", "required_capability", "depends_on",
                         "capacity_cost", "attempts", "max_retries",
                         "last_reject", "failure_code", "failure_detail",
                         "assigned_unit", "side_effects"):
        assert legacy_field in step, legacy_field
    # 新增抢占字段也在
    assert step["preempt_count"] == 0 and step["last_preempt_reason"] is None


def test_api_priority_preempts_lower(client):
    """端到端：HIGH 通过 API 抢占 LOW，被让位步骤带可区分原因并最终成功一次。"""
    c, _ = client
    c.post("/units", json={"unit_id": "u1", "capabilities": ["arm"], "capacity": 1})
    # LOW 占满
    c.post("/tasks", json={
        "task_id": "low-api", "priority": "LOW",
        "steps": [{"id": "s", "required_capability": "arm", "timeout": 5.0,
                   "delay": 0.4, "side_effects": 1}],
    })
    import time
    time.sleep(0.05)
    # HIGH 抢占（给一点执行时长，保证 PREEMPTED 让位窗口可被观测）
    c.post("/tasks", json={
        "task_id": "high-api", "priority": "HIGH",
        "steps": [{"id": "s", "required_capability": "arm", "timeout": 5.0,
                   "delay": 0.1}],
    })

    # LOW 应进入 PREEMPTED
    seen_preempted = False
    for _ in range(100):
        v = c.get("/tasks/low-api").json()
        st = v["steps"][0]
        if st["state"] == "PREEMPTED":
            seen_preempted = True
            assert st["last_preempt_reason"] == "PREEMPT_FOR_HIGHER_PRIORITY"
            assert st["preempt_count"] == 1
            break
        time.sleep(0.005)
    assert seen_preempted

    for _ in range(300):
        v = c.get("/tasks/low-api").json()
        if v["state"] == "SUCCEEDED":
            break
        time.sleep(0.02)
    assert v["state"] == "SUCCEEDED"
    assert v["priority"] == "LOW"
    assert v["steps"][0]["side_effects"] == 1
    assert c.get("/tasks/high-api").json()["state"] == "SUCCEEDED"
    u1 = next(u for u in c.get("/units").json()["units"] if u["unit_id"] == "u1")
    assert u1["used"] == 0
