# Fleet Mission Control（本地机器人任务编排控制面）

一个**纯进程内模拟**的机器人任务编排控制面：用内存中的"执行单元"承载多步骤
任务，只依赖系统能力与仓库已有依赖（FastAPI、pytest、标准库），**不接入任何
外部服务、真实凭据或付费资源**。

## 1. 快速开始

```bash
# 安装依赖（uv）
uv sync

# 启动服务（保持原有启动方式：uvicorn main:app / fastapi run）
uv run fastapi dev main.py
# 或：uv run uvicorn main:app --reload

# 既有接口行为不变
curl -s http://127.0.0.1:8000/        # -> {"Hello":"World"}
curl -s http://127.0.0.1:8000/units   # 内置 3 个模拟执行单元
```

服务启动时自带 3 个本地模拟执行单元（可通过 API 增删/上下线）：

| unit_id      | capabilities | capacity |
|--------------|--------------|----------|
| unit-arm-1   | arm          | 2        |
| unit-arm-2   | arm          | 1        |
| unit-cam-1   | camera       | 2        |

## 2. 核心概念

- **任务（Task）**：一组步骤的 DAG 声明，以 `task_id` 作为幂等提交标识。
- **步骤（Step）**：声明 `required_capability`（能力）、`capacity_cost`
  （容量成本）、`depends_on`（依赖步骤）、`timeout`、`max_retries`。
- **执行单元（ExecutionUnit）**：进程内模拟的机器人 worker，具有能力集合与
  整数容量。派发步骤时按 `capacity_cost` 原子占用容量；步骤**成功、失败、
  取消/被抢占、单元离线**都会释放占用，杜绝悬空占用。
- **行为脚本（StepBehavior，仅模拟用）**：用 `outcomes` 序列（
  `succeeded/timeout/crashed/error`）按尝试序号决定每次执行结果，
  `delay` 模拟执行耗时，`side_effects` 为成功那次产生的副作用数。
  这让超时、崩溃、取消竞争等场景**完全确定、可复现**。

## 3. 状态机

### 3.1 步骤状态机（StepState）

```
PENDING ──派发成功──> DISPATCHED ──成功收口(副作用只计一次)──> SUCCEEDED
   ^                     │
   │  可恢复失败(超时/    ├──不可恢复 error──────────────────> FAILED
   │  异常退出)且仍有     ├──重试耗尽───────────────────────> FAILED
   └──重试额度           │
                         └──任务取消 / 被抢占 / 单元离线────> CANCELLED / 回 PENDING
```

- 依赖未满足或派发被拒时保持 `PENDING`，并在 `last_reject` 上记录
  **可区分的拒绝原因**：
  - `DEPENDENCY_NOT_MET`：依赖步骤尚未全部 `SUCCEEDED`；
  - `CAPABILITY_MISMATCH`：没有在线单元具备所需能力；
  - `INSUFFICIENT_CAPACITY`：有能力匹配的单元，但剩余容量不足；
  - `NO_CANDIDATE_UNIT`：当前没有任何在线单元；
  - `TASK_NOT_ACTIVE`：任务已终态或无待派发步骤。
- 失败终态带 **可区分失败码** `failure_code`：
  - `RETRIES_EXHAUSTED`：超时/异常退出重试到上限仍失败；
  - `NON_RECOVERABLE`：执行返回不可恢复错误（不消耗重试额度）。
- 取消/抢占/超时重派后，飞行中尝试的迟到结果按「状态 + 尝试代次」守卫
  **直接丢弃**，不会复活步骤，也不会重复计数副作用。

### 3.2 任务状态机（TaskState）

```
PENDING ─有步骤被派发─> RUNNING ─全部步骤 SUCCEEDED─> SUCCEEDED
  │                       │
  └───────────┬───────────┴──某步骤 FAILED──────────> FAILED
              └────────用户取消(任意非终态)──────────> CANCELLED
```

任务状态由步骤状态在每次流转时原子重算，对外视图（`GET /tasks/{id}`）
始终自洽。

## 4. 幂等与重试语义

- **幂等提交**：`task_id` 相同的重复请求（即使携带不同步骤声明）一律忽略，
  返回首次提交的同一视图，响应中 `created=false` 区分幂等命中；
  不产生重复步骤、不重复注册行为、不重复执行。并发重复提交（多线程同时打）
  由提交临界区保证只创建一次。
- **取消幂等**：已终态任务再次取消返回 `changed=false`，不报错。
- **有界重试**：`timeout`（看门狗超时）与 `crashed`（异常退出）属可恢复
  错误，仅当 `已用尝试次数(从1起) <= max_retries` 时释放容量、步骤回到
  `PENDING` 重新派发；否则进入 `FAILED/RETRIES_EXHAUSTED`。
- **副作用不重复**：成功收口是唯一的副作用计数点，成功即终态；
  超时/取消后的迟到成功一律丢弃。
- **派发顺序**：任务按 `task_id` 排序（FIFO）、步骤按声明顺序推进；
  「依赖检查 → 能力/容量筛选 → 容量占用 → 状态流转」在单一临界区内完成，
  并发提交下容量约束与派发顺序都不会被突破。
- **离线恢复**：执行单元离线时，其上在飞步骤回到 `PENDING` 并释放占用，
  随后可重新派发到其它满足约束的在线单元。

## 5. 能力与容量约束的配置方式

### API

```bash
# 登记执行单元：能力集合 + 总容量
curl -s -X POST http://127.0.0.1:8000/units \
  -H 'content-type: application/json' \
  -d '{"unit_id":"arm-3","capabilities":["arm","gripper"],"capacity":4}'

curl -s http://127.0.0.1:8000/units
curl -s -X POST http://127.0.0.1:8000/units/arm-3/offline   # 模拟掉线
curl -s -X POST http://127.0.0.1:8000/units/arm-3/online    # 恢复
```

### 提交任务（含可复现的模拟行为）

```bash
curl -s -X POST http://127.0.0.1:8000/tasks \
  -H 'content-type: application/json' -d '{
    "task_id": "demo",
    "steps": [
      {"id": "pick", "required_capability": "arm",
       "capacity_cost": 1, "timeout": 0.2, "max_retries": 2,
       "outcomes": ["timeout", "crashed", "succeeded"],
       "side_effects": 1, "delay": 0.05},
      {"id": "photo", "required_capability": "camera",
       "depends_on": ["pick"], "capacity_cost": 2}
    ]
  }'

curl -s http://127.0.0.1:8000/tasks/demo      # 查询自洽状态视图
curl -s -X POST http://127.0.0.1:8000/tasks/demo/cancel
```

步骤字段：`required_capability`（必须）、`capacity_cost`（默认 1，正整数）、
`depends_on`（前置步骤 id 列表）、`timeout`（秒）、`max_retries`
（不含首次的重试上限）、`outcomes`（每个尝试的结果序列，超长取末值）、
`delay`（模拟执行耗时）、`side_effects`（成功那次的副作用数）。

非法声明（重复步骤 id、依赖不存在、DAG 有环、参数越界）返回 `422`，
且不会在系统中留下任务。

### 代码方式

```python
from fleet.service import FleetService
from fleet.models import UnitConfig, TaskSpec, StepSpec
from fleet.executor import StepBehavior

svc = FleetService()
svc.register_unit(UnitConfig("u1", frozenset({"arm"}), capacity=2))
svc.submit(
    TaskSpec("t", (StepSpec("a", "arm", max_retries=1, timeout=0.2),)),
    behaviors={"a": StepBehavior(("timeout", "succeeded"), delay=0.05)},
)
svc.wait_idle(3.0)
print(svc.get_task("t").state)   # TaskState.SUCCEEDED
```

## 6. 本地验证

```bash
# 全部单测（23 个）：并发提交、重复提交、超时、取消、离线等
uv run pytest -q

# 查看带"输入事件 + 判定依据"的详细日志
uv run pytest -s -o log_cli=true --log-cli-level=INFO

# 单个场景
uv run pytest tests/test_retry.py -q
uv run pytest tests/test_concurrency_lifecycle.py -q
```

每个用例都会：
1. 用 `print` 显式输出 **INPUT**（提交声明、行为脚本、取消/离线动作）与
   **JUDGEMENT**（观察到的状态、拒绝原因、失败码、容量账本）；
2. 引擎通过 logger 输出有序事件（`submit/dispatch/reject/success/retry/
   fail/timeout/cancel/offline/release`），含派发尝试序号、容量占用与释放，
   便于复现判定路径。

## 7. 目录结构

```
main.py                # FastAPI 入口（app = create_app()），保留 GET /
fleet/
  models.py            # 状态机枚举、规格、视图、事件等类型
  errors.py            # 自定义异常（可恢复/不可恢复/非法声明等）
  executor.py          # 进程内执行单元 + 行为脚本 + 单元注册表
  store.py             # 线程安全内存存储（状态流转、容量账本、幂等）
  engine.py            # 编排引擎（调度/派发/看门狗超时/重试/回收/事件）
  service.py           # 应用服务（校验、幂等提交、查询、取消）
  api.py               # FastAPI 路由与序列化
tests/                 # 单测（conftest 提供夹具与日志）
docs/architecture.md   # 架构与并发/一致性设计说明
```
