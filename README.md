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
  可携带**优先级** `priority`（`high`/`medium`/`low`），不携带时按
  `medium`（普通）处理，与旧版提交行为完全一致。
- **步骤（Step）**：声明 `required_capability`（能力）、`capacity_cost`
  （容量成本）、`depends_on`（依赖步骤）、`timeout`、`max_retries`。
  步骤继承所属任务的优先级。
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
   ^  │                │
   │  │被高优先级抢占   ├──不可恢复 error──────────────────> FAILED
   │  └────────────────┤
   │  可恢复失败(超时/  ├──重试耗尽───────────────────────> FAILED
   │  异常退出)且仍有   │
   └──重试额度         └──任务取消 / 单元离线─────────────> CANCELLED / 回 PENDING
```

- 依赖未满足或派发被拒时保持 `PENDING`，并在 `last_reject` 上记录
  **可区分的拒绝原因**：
  - `DEPENDENCY_NOT_MET`：依赖步骤尚未全部 `SUCCEEDED`；
  - `CAPABILITY_MISMATCH`：没有在线单元具备所需能力；
  - `INSUFFICIENT_CAPACITY`：有能力匹配的单元，但剩余容量不足；
  - `NO_CANDIDATE_UNIT`：当前没有任何在线单元；
  - `TASK_NOT_ACTIVE`：任务已终态或无待派发步骤；
  - `PREEMPTED`：**在飞时被更高优先级步骤抢占让位**。这不是失败：
    `failure_code` 为空、不消耗重试次数（`retries_used` 不变），
    容量立即释放，步骤回到 `PENDING` 等待重新派发；该原因在重新派发前
    一直保留在视图上（不会被例行的容量不足拒绝覆盖），并通过
    `preemptions` 计数与 `preempt` 事件可查询、可解释。
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
  错误，仅当已消耗的重试次数 `retries_used < max_retries` 时释放容量、
  步骤回到 `PENDING` 重新派发；否则进入 `FAILED/RETRIES_EXHAUSTED`。
  抢占让位与单元离线重排队**不消耗**重试额度（`retries_used` 不变）。
- **副作用不重复**：成功收口是唯一的副作用计数点，成功即终态；
  超时/取消/抢占后的迟到成功一律丢弃。
- **派发顺序**：任务按「优先级高者先派，同优先级按提交先后 FIFO」排序，
  任务内步骤按声明顺序推进；「依赖检查 → 能力/容量筛选 → 容量占用 →
  状态流转」在单一临界区内完成，并发提交下容量约束与派发顺序都不会被
  突破。抢占让位后重新排队的步骤仍按原任务的（优先级， 提交序号）参与
  排序，顺序稳定、可复现。
- **优先级抢占**：高优先级步骤等容量时，若某能力匹配单元上的在飞步骤
  优先级**严格更低**，控制面会让足够的低优先级步骤让位（`PREEMPTED`），
  把容量交给高优先级步骤先跑。同优先级互不抢占（保持 FIFO）。让位步骤
  不算失败、不消耗重试次数，腾出容量后自动接着跑；其飞行中尝试的迟到
  结果按「状态 + 尝试代次」守卫丢弃，副作用不会重复计数。
- **终态自洽**：任务一旦进入 `FAILED`/`CANCELLED` 终态，同任务仍在飞的
  步骤立即被级联取消并释放容量，迟到结果不会改变任何对外视图——任何
  时刻查询都不会出现"任务已终态却还有步骤在飞或又变成成功"。
- **离线恢复**：执行单元离线时，其上在飞步骤回到 `PENDING` 并释放占用，
  随后可重新派发到其它满足约束的在线单元。

## 5. 优先级与抢占

### 5.1 优先级含义

- 任务级字段 `priority`：`high` / `medium` / `low`，缺省 `medium`。
  任务的所有步骤继承该优先级。
- **调度顺序**：优先级高者先派；同优先级按提交先后（FIFO）。该顺序由
  （优先级， 提交序号） 唯一决定，抢占重排队后不改变，因此可复现、可解释
  （每次派发都有带 `priority=` 的 `dispatch` 事件）。
- **兼容范围**：不携带 `priority` 的旧请求体照常工作（按 `medium` 处理）；
  响应仅在任务视图新增 `priority`、步骤视图新增 `preemptions` /
  `retries_used`，旧字段的名字、含义与默认值一律不变。

### 5.2 抢占与让位

- 触发条件：某步骤等待容量（`INSUFFICIENT_CAPACITY`），且某能力匹配单元
  上的在飞步骤优先级**严格更低**时，控制面让足够的低优先级步骤让位，
  把容量交给高优先级步骤先跑。同优先级不抢占；让位不足以腾出容量时不做
  无谓抢占。
- 让位步骤的处境：状态回到 `PENDING`，`last_reject=PREEMPTED`（明确、
  可区分，重新派发前一直保留），`preemptions` 计数 +1；**不算失败**
  （`failure_code` 为空）、**不消耗重试次数**（`retries_used` 不变）、
  容量立即释放。腾出容量后自动重新派发接着跑。
- 账目与副作用：让位、取消、离线、超时重试共用同一条容量释放路径，
  占用/释放严格配对；让位步骤飞行中尝试的迟到结果被代次守卫丢弃，
  已成功计入的副作用不会因重跑重复统计。
- 注意：持续涌入的高优先级任务可能让低优先级步骤反复让位（饥饿），
  这是本地模拟的有意语义，生产使用需自行加配额/老化策略。

### 5.3 本地复现

```bash
# 终端 1：启动服务
uv run fastapi dev main.py

# 终端 2：用低优先级长任务占满 arm 能力最大单元的容量（2）
curl -s -X POST http://127.0.0.1:8000/tasks -H 'content-type: application/json' -d '{
  "task_id": "low", "priority": "low",
  "steps": [{"id": "a", "required_capability": "arm", "capacity_cost": 2,
             "timeout": 30, "delay": 5}]}'

# 提交高优先级任务（容量 2 无法在其它单元满足）-> low 的步骤被让位
curl -s -X POST http://127.0.0.1:8000/tasks -H 'content-type: application/json' -d '{
  "task_id": "high", "priority": "high",
  "steps": [{"id": "a", "required_capability": "arm", "capacity_cost": 2,
             "timeout": 30, "delay": 1}]}'

# 观察：low 的步骤 PENDING + last_reject=PREEMPTED + preemptions=1；
# high 完成后 low 自动重派并最终 SUCCEEDED，副作用只计一次
watch -n0.2 'curl -s http://127.0.0.1:8000/tasks/low; curl -s http://127.0.0.1:8000/tasks/high'
```

自动化复现（每个用例打印 INPUT/JUDGEMENT，引擎事件含优先级与让位原因）：

```bash
uv run pytest tests/test_preemption.py -s -o log_cli=true --log-cli-level=INFO
```

## 6. 能力与容量约束的配置方式

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
    "priority": "high",
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

任务级字段：`task_id`（必须，幂等标识）、`priority`（可选，
`high`/`medium`/`low`，缺省 `medium`——与旧版请求行为一致）、
`steps`（必须）。

步骤字段：`required_capability`（必须）、`capacity_cost`（默认 1，正整数）、
`depends_on`（前置步骤 id 列表）、`timeout`（秒）、`max_retries`
（不含首次的重试上限）、`outcomes`（每个尝试的结果序列，超长取末值）、
`delay`（模拟执行耗时）、`side_effects`（成功那次的副作用数）。

响应在旧字段基础上只增不改：任务视图新增 `priority`；步骤视图新增
`preemptions`（被抢占让位次数）与 `retries_used`（已消耗重试次数）。
旧客户端不读取新字段即可照常工作。

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

## 7. 本地验证

```bash
# 全部单测（33 个）：并发提交、重复提交、超时、取消、离线、优先级抢占等
uv run pytest -q

# 查看带"输入事件 + 判定依据"的详细日志
uv run pytest -s -o log_cli=true --log-cli-level=INFO

# 单个场景
uv run pytest tests/test_retry.py -q
uv run pytest tests/test_concurrency_lifecycle.py -q
uv run pytest tests/test_preemption.py -q
```

每个用例都会：
1. 用 `print` 显式输出 **INPUT**（提交声明、行为脚本、取消/离线动作）与
   **JUDGEMENT**（观察到的状态、拒绝原因、失败码、容量账本）；
2. 引擎通过 logger 输出有序事件（`submit/dispatch/reject/success/retry/
   fail/timeout/cancel/offline/preempt/release`），含任务优先级、派发尝试
   序号、容量占用与释放，便于复现判定路径。

## 8. 目录结构

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
