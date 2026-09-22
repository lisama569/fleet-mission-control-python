# Fleet Mission Control（本地机器人任务编排控制面）

一个**纯进程内**的机器人任务编排控制面：用线程模拟执行单元（unit）承载任务步骤，
不依赖任何外部服务、真实凭据或付费资源。提供 FastAPI HTTP 接口，支持：

- 多步骤任务按**声明的依赖顺序**编排与派发；
- 派发必须同时满足**能力（capability）**与**容量（capacity）**约束，违例被拒绝并给出可区分原因；
- **幂等提交**与状态查询：相同 `idempotency_key` 不产生重复步骤 / 重复执行；
- 取消、抢占、执行单元离线后**立即释放容量**，不遗留悬空占用；
- 步骤**超时 / 失败**后的**有界重试与重新派发**；副作用**恰好计入一次**；
- 重试耗尽 / 不可恢复错误进入**明确终态**，任务与步骤状态视图始终自洽；
- 并发提交下容量约束与派发顺序（优先级 + FIFO）不被突破。

> 原始服务行为保持不变：`GET /` 仍返回 `{"Hello":"World"}`，ASGI 入口仍是 `main:app`。

---

## 1. 快速开始

```bash
uv sync                       # 安装依赖（fastapi + 标准库；pytest 为开发依赖）
uv run uvicorn main:app --reload
# 另开终端：
curl -s http://127.0.0.1:8000/
# {"Hello":"World"}
```

一个最小端到端例子：

```bash
# 1) 注册一个具备 nav/arm 能力、容量为 1 的执行单元
curl -s -X POST localhost:8000/api/units -H 'Content-Type: application/json' \
  -d '{"unit_id":"u1","capabilities":["nav","arm"],"capacity_total":1}'

# 2) 提交一个两步骤任务（b 依赖 a）
curl -s -X POST localhost:8000/api/tasks -H 'Content-Type: application/json' -d '{
  "idempotency_key":"job-1",
  "steps":[
    {"key":"a","capability":"nav","run_ms":50,"timeout_ms":5000,"max_retries":2},
    {"key":"b","capability":"arm","run_ms":50,"depends_on":["a"]}
  ]}'

# 3) 查询状态 / 事件 / 拒绝记录
curl -s localhost:8000/api/tasks/<task_id>
curl -s 'localhost:8000/api/events?after=0'
curl -s localhost:8000/api/rejects
```

---

## 2. 架构与目录

```
main.py                  # ASGI 入口：from fleetapi.app import app
fleet/
  models.py              # 枚举（状态/原因/事件）与数据结构、规格校验所需类型
  store.py               # 单一 RLock 保护的内存态：任务/单元/事件/幂等键/副作用
  simulator.py           # WallClock / VirtualClock 与模拟执行器（无真实副作用）
  engine.py              # 编排引擎：调度、派发、看门狗超时、重试、容量、取消
fleetapi/
  schemas.py             # Pydantic 请求/响应模型
  app.py                 # FastAPI 路由，create_app(engine) 可注入引擎
tests/                   # 编排、并发、HTTP 三套测试（含输入事件与判定日志）
```

**并发模型**：所有可变状态位于 `FleetStore`，由一把可重入锁保护。工作线程只在
"模拟执行 / 看门狗等待"时阻塞，结束后在锁内完成单次终结（finalize）。因此
"选择单元—校验容量—占用容量—记录派发"是原子的，并发提交不会超卖容量。

- `WallClock` 部署：应用 lifespan 自动启动一个调度线程，周期性 `tick()`。
- `VirtualClock`（测试）：时间由测试手动推进，引擎提供 `tick()` 与
  `drive_until_idle()`，保证场景**确定性、可复现**。

---

## 3. 状态机

### 3.1 步骤状态 `StepState`

```
PENDING ──(所有依赖 SUCCEEDED)──▶ READY ──派发成功──▶ DISPATCHED ──▶ RUNNING
   │                                 ▲  ▲                              │
   │                                 │  └──────────────┐               │
   │(依赖失败/任务取消/级联失败)      │                 │               │
   ▼                                 │                 │               ├─ 成功 ─▶ SUCCEEDED（终态）
CANCELLED（终态）                    │                 │               ├─ 可重试失败/超时 ─▶ RETRYING
                                     │                 │               │     （未超 max_retries；退避结束后回 READY）
                                     └─────离线/抢占────┘               ├─ 抢占 ─▶ READY（不耗重试预算）
   RETRYING ──(退避 not_before 到期)──▶ READY                            ├─ 单元离线中断 ─▶ READY（不耗重试预算）
                                                                       ├─ 重试耗尽 ─▶ FAILED / TIMED_OUT（终态）
                                                                       ├─ 不可恢复错误 ─▶ FAILED(NON_RETRYABLE_ERROR)
                                                                       └─ 任务取消/兄弟步骤终态失败 ─▶ CANCELLED（终态）
```

- `RETRYING` 表示"等待重试"，`retry_not_before_ms` 到期后下一个 tick 回到 `READY`。
- 抢占（PREEMPTED）与离线中断（INTERRUPTED）**不消耗重试预算**，步骤直接恢复为 `READY`。
- 看门狗在派发时按 `started_at_ms + timeout_ms` 设定截止时间；超时把尝试标记为
  `TIMED_OUT` 并释放容量，进入与普通失败相同的有界重试链路。

### 3.2 任务状态 `TaskState`

```
PENDING ──任一步骤 READY/DISPATCHED/RUNNING/RETRYING──▶ RUNNING
RUNNING ──全部步骤 SUCCEEDED──────────────────────────▶ SUCCEEDED（终态）
RUNNING ──任一步骤 FAILED/TIMED_OUT（级联取消其余步骤）▶ FAILED（终态，含 failure_reason/detail）
PENDING/RUNNING ──用户取消且所有步骤到达终态───────────▶ CANCELLED（终态）
```

任务终态失败原因 `FailureReason`：`RETRIES_EXHAUSTED`、
`TIMEOUT_RETRIES_EXHAUSTED`、`NON_RETRYABLE_ERROR`（`NONE` 表示无失败）。

### 3.3 派发拒绝原因 `RejectReason`（可区分、可查询）

| 原因 | 触发条件 |
| --- | --- |
| `DEPENDENCIES_UNMET` | 目标步骤尚有依赖未 `SUCCEEDED` |
| `CAPABILITY_MISMATCH` | 指定/候选单元不具备步骤所需能力 |
| `CAPACITY_FULL` | 单元在线且能力匹配，但剩余容量 < `capacity_cost` |
| `UNIT_OFFLINE` | 目标单元处于 OFFLINE（自动选择时：所有具备能力的单元都离线） |
| `UNIT_NOT_FOUND` | 指定的 unit_id 未注册 |
| `STEP_NOT_DISPATCHABLE` | 步骤已处于终态 / 正有尝试在飞 / 重试退避中 |

自动调度产生的拒绝写入 `/api/rejects` 与事件流（`DISPATCH_REJECTED`）；
显式派发接口在拒绝时返回 **HTTP 409**，body 为
`{"detail":{"reason":..., "detail":...}}`。

---

## 4. 幂等与重试语义

### 4.1 幂等提交

- `POST /api/tasks` 以 `idempotency_key` 去重：首次返回 `created=true`，
  重复键返回**同一份**任务视图且 `created=false`，不会产生重复步骤、重复尝试或重复副作用。
- 去重表在内存中，键映射到 task_id；并发重复提交由单锁保证"恰好一个创建者"。
- 事件流中首次为 `TASK_SUBMITTED`，后续为 `TASK_DUPLICATE`。

### 4.2 有界重试

- 每个步骤可配 `max_retries`（默认 2），**最大尝试次数 = 1 + max_retries**。
- 仅"自然失败（FAILED）/ 超时（TIMED_OUT）"计入预算；抢占与离线中断不计入。
- `behavior="fail_nonretryable"` 的失败**立即终态**，不重试。
- 可选退避：`Engine(retry_backoff_ms=...)`，期间派发被拒绝（退避到期自动恢复）。
- 重试耗尽：失败 → `FAILED / RETRIES_EXHAUSTED`；超时 →
  `TIMED_OUT / TIMEOUT_RETRIES_EXHAUSTED`。

### 4.3 恰好一次副作用

模拟执行器没有真实外部副作用；引擎以
`effect_token = "<task_id>:<step_key>"` 记录副作用，仅当步骤**成功**时写入一次去重集合，
并产生一条 `EFFECT_RECORDED` 事件（含 `dedup` 标记）。因此即便发生异常重入，
已完成的副作用也不会被重复计入。重试只发生在此前尝试未成功的情况下。

### 4.4 容量释放（无悬空占用）

取消、抢占、单元离线、成功、失败、超时——**每条终结路径都会**
从单元移除 `running_steps` 并扣减 `capacity_used`（`CAPACITY_RELEASED` 事件）。
取消 / 抢占 / 离线在锁内同步终结在飞尝试，因此 API 返回时容量已释放。

---

## 5. 能力与容量约束的配置方式

执行单元通过接口注册（或直接 `engine.register_unit(...)`）：

```jsonc
POST /api/units
{
  "unit_id": "u1",                 // 可选，缺省自动生成 unit_xxx
  "capabilities": ["nav", "arm"],  // 该单元广告的能力集合
  "capacity_total": 2              // 可并发承载的容量总量（>=1）
}
```

步骤在任务提交时声明需求：

```jsonc
{
  "key": "a",
  "capability": "nav",     // 必须落到广告含 "nav" 的在线单元
  "capacity_cost": 1,      // 占用容量；要求 used + cost <= total
  "timeout_ms": 5000,      // 看门狗截止
  "max_retries": 2,
  "depends_on": ["prev"],  // 仅当这些步骤 SUCCEEDED 后才可派发
  "behavior": "succeed",   // succeed | fail | fail_nonretryable | timeout | flaky
  "run_ms": 20,            // 模拟执行时长
  "flaky_fail_times": 1    // behavior=flaky 时，前 N 次失败后成功
}
```

派发选择规则：先按能力过滤，再要求在线，再要求容量足够；
自动选择取排序后的第一个可用单元。任务 `priority`（越大越优先）相同时按提交顺序 FIFO。
单元离线：`POST /api/units/{id}/state {"online":false}`，会中断并回收其上所有在飞步骤的容量。

---

## 6. HTTP 接口一览

| 方法 & 路径 | 说明 |
| --- | --- |
| `GET /` | 健康/兼容接口，返回 `{"Hello":"World"}` |
| `POST /api/units` | 注册执行单元 |
| `GET /api/units` / `GET /api/units/{id}` | 单元列表 / 详情（含容量与在跑步骤） |
| `POST /api/units/{id}/state` | 上线 / 下线（下线立即中断并回收容量） |
| `POST /api/tasks` | 幂等提交任务（响应含 `created`） |
| `GET /api/tasks` / `GET /api/tasks/{id}` | 任务列表 / 自洽状态视图 |
| `POST /api/tasks/{id}/cancel` | 取消任务（取消在飞与未开始步骤） |
| `POST /api/tasks/{id}/preempt` | 抢占：释放在飞步骤容量并使其回到 READY |
| `POST /api/tasks/{id}/steps/{key}/dispatch?unit_id=` | 显式派发；拒绝返回 409 + reason |
| `GET /api/rejects` | 最近的派发拒绝记录（含可区分原因） |
| `GET /api/events?after=<seq>` | 单调递增的事件流（输入与判定依据） |
| `POST /api/scheduler/tick` | 手动跑一遍调度（VirtualClock / 演示用） |

---

## 7. 本地验证方法

```bash
uv run pytest                       # 全量单测（确定性 VirtualClock）
uv run pytest -s -k timeout         # 只看超时场景，打印输入事件与判定依据
uv run pytest -k concurrent         # 并发提交 / 重复提交 / 并发派发
uv run pytest -k "cancel or preempt or offline"
```

测试特点：

- **确定性、可复现**：使用 `VirtualClock`，`Engine.drive_until_idle()` 直接把虚拟时间
  跳到最近的"尝试完成 / 看门狗超时"时刻，不依赖真实 sleep 的时序。
- **打印输入事件与判定依据**：`conftest.record_case(...)` 记录输入，
  `log_decisions(...)` 逐条打印引擎事件（状态迁移、派发接受/拒绝、容量释放、
  重试调度）与拒绝原因；同时通过 logging（`--log-cli`）与 stdout（`-s`）输出。
- 覆盖：依赖顺序、各类拒绝原因、幂等（单次+并发重复）、flaky 重试成功且副作用一次、
  重试耗尽、不可恢复错误、超时、运行中取消、取消未开始步骤、抢占重跑、
  离线中断不耗预算、并发提交容量不超卖、FIFO 派发顺序、完整 HTTP 链路。

交互式手动验证（真实线程调度器）：

```bash
uv run uvicorn main:app --port 8000
# 见第 1 节的 curl 示例；可用 /api/events 观察完整判定轨迹
```

---

## 8. 关键设计说明与边界

- **无外部依赖**：执行单元是进程内线程，"执行"只是经过注入时钟的定时等待，
  随后按声明的 `behavior` 返回结果；无网络、无凭据、无付费资源。
- **单锁 + 工作线程**：保证调度决策与容量记账原子；锁内不做阻塞等待。
- **看门狗**：每次派发启动独立看门狗，到点置 `timed_out + cancel`；尝试正常结束时看门狗被停止。
- **同步终结**：取消 / 抢占 / 下线在锁内同步结算在飞尝试，真实工作线程稍后退出时
  因 control 身份已失效而安全跳过，杜绝重复终结与容量重复扣减。
- **失败快速传播**：某步骤终态失败时，任务置 FAILED，并把其余未完成步骤级联取消；
  正在运行的无关步骤通过 `abort_running` 中断。
