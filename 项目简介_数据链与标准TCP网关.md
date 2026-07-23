# 项目简介、数据链与标准 TCP 网关

文档版本：v1.0
适用范围：单台 Windows、单个大 QMT 账户、本机外置策略接入

## 1. 项目简介

本项目把大 QMT 的交易能力安全地提供给同一台 Windows 上的普通 CPython 策略使用。策略、信号、风控、业务数据库和事件处理都运行在大 QMT 外部；大 QMT 内置 Python 仅保留必须在 QMT 上下文中运行的账户绑定、下单、撤单、查询和交易回调。

```text
外置策略 / 业务服务（普通 Windows CPython）
                 │
                 │ 127.0.0.1 TCP v2
                 ▼
本机 Gateway（asyncio + SQLite WAL）
                 │
                 │ NTFS 原子 JSON 文件队列
                 ▼
大 QMT 内置 Python Helper（QMT 单线程 timer）
                 │
                 ▼
QMT 交易接口 / 券商 / 交易账户
```

这是一个本机桥接方案，不是独立交易系统，也不会绕过 QMT、券商权限、交易时段或风控规则。它去除了跨机器网络和 Linux 中间层，但保留了网关的身份校验、可靠投递、幂等控制、状态持久化和背压保护。

### 1.1 设计目标

- 外置策略只依赖稳定的 `qmt_local_api`，或按 TCP v2 标准接入；不直接操作 Helper 队列。
- 所有交易副作用经 Gateway 的 SQLite 状态机和单写者控制，优先避免重复下单或重复撤单。
- 大 QMT 进程内不开放 TCP、不启动后台线程、不读取 `.env`，降低对 QMT 运行环境的干扰。
- Helper 与 Gateway 之间只使用同机 NTFS 上的原子 JSON 文件，避免跨进程共享内存或不受控网络通信。
- 对未就绪、身份不一致、队列过载和提交结果不确定等情形采用失败关闭（fail-closed）策略。

### 1.2 适用边界

| 维度 | 固定边界 |
|---|---|
| 部署 | 一台 Windows 主机、一个 QMT 账户、一个 Gateway 实例 |
| 网络 | 只监听 IPv4 回环地址 `127.0.0.1`，不提供 LAN、`0.0.0.0`、IPv6 或 TLS 模式 |
| 可靠事件消费者 | 同一账户同时只能有一个 primary 消费者；新完成握手的连接会替换旧连接 |
| 配置 | 项目根目录唯一 `.env` 是生产配置来源；生成的 JSON 与 Helper 文件不是人工配置入口 |
| Python | 外置端使用项目 `.venv` 的 Windows CPython；内置端使用生成后的 Python 3.6 兼容 Helper |
| 交易保证 | 提供持久化幂等与至少一次事件投递；不宣称断电、磁盘缓存丢失或人工回滚目录下的端到端 exactly-once |

## 2. 核心组件与职责

| 组件 | 所在进程 | 主要职责 | 不应承担的职责 |
|---|---|---|---|
| `qmt_local_api` / 外置策略 | 普通 CPython | 连接、身份校验、查询、提交命令、可靠回调落库与 ACK | 直接读写 runtime、绕过 Gateway 交易 |
| Gateway | 普通 CPython `asyncio` | TCP v2、令牌/账户校验、SQLite WAL、幂等、文件入队、查询聚合、回调重投与背压 | 调用 QMT 原生交易接口 |
| 文件 runtime | 本机 NTFS | 进程间请求、响应、事件和健康状态的原子文件交换 | 被人工删除、修改或作为公开 API 使用 |
| Helper | 大 QMT 内置 Python | 账户绑定、定时取件、`passorder`、撤单、查询、QMT 回调标准化 | 开 socket、启动线程、加载部署环境变量 |
| QMT / 券商 | 大 QMT 客户端 | 正式交易、账户查询、订单/成交/错误回调 | 为桥接层提供事务性 exactly-once 语义 |

Gateway 必须存在，因为它是唯一对外边界：它验证首帧身份、维护唯一写者和永久副作用记录，向 Helper 原子发布命令，并在外置策略断线后继续保存和重投可靠结果。外置策略直接写文件队列会破坏这些约束。

## 3. 配置、生成与启动链

```text
根 .env
  ├─ setup_venv.ps1：准备 .venv，并离线暴露 qmt_local_api
  ├─ generate_helper.ps1：生成并受控部署 Helper / Loader
  └─ start_gateway.ps1：生成 Gateway 配置、执行预检并启动网关
```

根 `.env` 中与链路直接相关的字段包括：

- `QMT_LOCAL_BIND_HOST`：必须精确为 `127.0.0.1`；
- `QMT_LOCAL_TCP_PORT`：Gateway 端口，默认 `9550`；
- `QMT_LOCAL_AUTH_TOKEN`：每次新 TCP 连接首帧使用的 32 字节随机令牌（64 个十六进制字符）；
- `QMT_LOCAL_RUNTIME_ROOT`：文件 IPC 根目录，实际账户 runtime 派生为 `<RUNTIME_ROOT>\<TCP_PORT>`；
- `QMT_LOCAL_ACCOUNT_ENABLED/NAME/ID/TYPE`：唯一账户身份；
- `QMT_LOCAL_HELPER_ENABLE_TRADING`、`QMT_LOCAL_HELPER_ENABLE_CANCEL_ORDER`：下单和撤单开关。

Helper 由生成器把账户、路径和交易开关固化为常量。修改账户、路径、交易开关、策略名或备注后，必须重新生成、部署并重新加载大 QMT 策略；只修改 `.env` 不会改变已运行的 Helper。

推荐启动顺序：登录大 QMT → 启动内置 Helper 策略并确认健康文件 → 执行 `./start_gateway.ps1` → 启动唯一外置策略/可靠回调消费者。停止时反向执行：先停止产生信号和下单的业务，再停止消费者、Gateway，最后停止 Helper。

## 4. 端到端数据链

### 4.1 下单与撤单链

```text
1. 外置策略
   NEW_ASYNC / CANCEL_ASYNC + request_id + client_order_id
                 │ TCP
                 ▼
2. Gateway
   身份、参数、健康、容量、幂等校验
   SQLite: PREPARED → DISPATCHING
   原子写入 inbox/commands
                 │ 文件移动
                 ▼
3. Helper
   timer 取最早命令 → request guard / sibling 预检
   → 调用 passorder 或 QMT 撤单接口 → 写 response 文件
                 │ 文件扫描
                 ▼
4. Gateway
   捕获 response，先持久化 pending ledger
   → 可靠发送 ASYNC_*_RESPONSE
                 │ TCP
                 ▼
5. 外置策略
   按 delivery_id 幂等落库 → DELIVERY_ACK
                 │
                 └─ Gateway 确认后清理已确认的可靠投递状态
```

下单建议使用 `NEW_ASYNC`。Gateway 的即时 `ASYNC_ORDER` 且 `status=SENT, stage=BRIDGE_QUEUED` 仅表示命令已经可靠排入 Helper 队列，不等于 QMT 已接受。随后收到的可靠 `ASYNC_ORDER_RESPONSE` 才表示 Helper/QMT 的提交结果；最终订单和成交状态仍以 `ORDER_UPDATE` 与 `TRADE_NOTIFY` 为准。

异步撤单对应 `CANCEL_ASYNC` 或 `CANCEL_SYSID_ASYNC`。即时 `ASYNC_CANCEL` 仅表示已排队；可靠 `ASYNC_CANCEL_RESPONSE` 中的 `CANCEL_SUBMITTED` 表示已调用撤单接口，但仍不是券商最终撤单终态，最终以订单回调为准。

### 4.2 查询链

```text
外置策略 QUERY
  → Gateway 唯一 TCP reader
  → bounded query broker / 同键 singleflight / 单并发查询
  → inbox/queries 原子文件
  → Helper 定时查询 QMT
  → responses 文件
  → Gateway QUERY_RESPONSE
  → 外置策略等待对应 msg_id
```

核心查询类型包括 `ACCOUNT_STATUS`、`ACCOUNT`、`ASSET`、`POSITION`、`ORDER`、`DEAL/TRADE` 等。查询实时失败时，Gateway 可能返回带 `cache_fallback=true` 的缓存结果；缓存降级或 `state=degraded` 不是可交易的 ready 状态，交易前必须拒绝。

### 4.3 回调与可靠投递链

```text
QMT 订单 / 成交 / 错误回调
  → Helper 标准化为原子事件文件（events/live）
  → Gateway 按 event_seq 读取、去重并发送带 delivery_id 的事件
  → 外置策略 handler 以 delivery_id 持久化去重
  → 全部 handler 成功返回
  → DELIVERY_ACK
  → Gateway 确认投递；否则断线并重投
```

带非空 `delivery_id` 的 `ASYNC_ORDER_RESPONSE`、`ASYNC_CANCEL_RESPONSE`、`ORDER_UPDATE`、`TRADE_NOTIFY`、`ORDER_ERROR` 等属于可靠事件，语义为至少一次。业务处理函数必须先以 `delivery_id` 落库去重并提交，再返回成功；处理函数抛错时 API 会不 ACK 并断开，让 Gateway 重投。慢计算应在落库后转交业务自身的队列。

同名但不带 `delivery_id` 的快照差分事件可能是 best-effort，不能将它们当成可靠账本。一个账户不能由多个进程并行消费可靠事件，否则新连接会替换旧 primary。

### 4.4 运行目录与健康链

```text
<runtime>/<tcp-port>/
├─ inbox/commands       Gateway → Helper：下单、撤单等副作用命令
├─ inbox/queries        Gateway → Helper：只读查询
├─ processing/*         Helper 已领取、等待处理或恢复的文件
├─ responses            Helper → Gateway：查询和异步提交结果
├─ events/live          Helper → Gateway：实时订单、成交、错误事件
├─ request_state        Helper 请求 guard
├─ heartbeat.json       Helper 心跳
├─ state.json           Helper 状态
└─ readiness.json       Helper 就绪与身份状态
```

Gateway 只有在 `heartbeat.json`、`state.json`、`readiness.json` 三份文件都新鲜，且账户、runtime、build、协议和性能基线一致时，才会允许产生交易副作用。文件发布使用同目录临时文件加原子替换，避免读取半个 JSON；不应手工编辑、删除或单独恢复 SQLite WAL/SHM/runtime 中的任意部分。

## 5. 幂等、状态和异常处理

### 5.1 三类关联键

| 键 | 用途 | 重连/重试规则 |
|---|---|---|
| `msg_id` | 一次 TCP 请求与响应的相关 ID | 可为一次网络重试创建新值 |
| `client_order_id` | 一笔业务订单的幂等键 | 相同业务订单必须保持稳定；同 ID 不同意图会被拒绝为 `IDEMPOTENCY_CONFLICT` |
| `request_id` | 可能产生 QMT 副作用的下单或撤单动作键 | 同一动作跨重连必须保持稳定；同 ID 不同动作或参数会被拒绝为 `REQUEST_ID_CONFLICT` |

不要把 `timestamp`、`msg_id` 或随机生成的订单备注当成业务幂等键。网络重试可更换 `msg_id`，但必须保留原 `client_order_id` 和原 `request_id`。

### 5.2 Gateway 副作用状态机

```text
PREPARED → DISPATCHING → ENQUEUED / UNKNOWN / TERMINAL
```

| 状态 | 含义 | 是否可自动再次执行 |
|---|---|---|
| `PREPARED` | 身份和副作用指纹已持久化，尚未跨过 Helper 调度屏障 | 仅相同指纹可接管 |
| `DISPATCHING` | 命令文件写入或 QMT 调用可能已经开始 | 否 |
| `ENQUEUED` | 队列或提交结果已经可证明 | 否，读取已有状态或等待回调 |
| `UNKNOWN` | 是否产生副作用无法证明 | 否，必须对账 |
| `TERMINAL` | 已明确拒绝或结束 | 否，读取既有结果 |

遇到超时、断线、`EFFECT_STATE_UNKNOWN`、`SUBMIT_UNKNOWN` 或 `POST_ENQUEUE_STATE_UNCERTAIN` 时，禁止换新 ID 自动重发。应使用原 `client_order_id`、`request_id`、QMT user order ID、订单查询和回调进行对账；确认无副作用后再由业务规则或人工决定下一步。

Helper 在调用 QMT 原生接口前，会检查同一 `request_id` 的队列、processing、恢复文件、response 和 guard 是否能够证明一致。目录不可读、扫描不足、元数据/内容异常等情况都失败关闭并返回需要对账的未知状态，不会继续调用 `passorder` 或撤单接口。

## 6. 标准 TCP 网关规范（v2）

本节是其他语言或自定义客户端接入本机 Gateway 的最小规范。生产 Python 策略优先使用 `qmt_local_api`，它已封装拆包、唯一 reader、发送锁、心跳、重连和可靠 ACK。

### 6.1 端点与连接约束

```text
host = 127.0.0.1
port = QMT_LOCAL_TCP_PORT（默认 9550）
```

- Gateway 与客户端都必须拒绝非 IPv4 loopback 地址。
- TCP 使用 `TCP_NODELAY`；客户端应启用 keepalive。
- Gateway 空闲连接上限为 60 秒；推荐客户端每 5 秒发送一次 PING，15 秒未收到任何入站数据即断线重连。
- 每次新 TCP 连接都必须重新执行身份握手，业务帧不得早于握手成功。
- 同一个 socket 只能有一个 reader；全部写入必须通过同一发送锁串行化，防止帧交错。

### 6.2 帧格式

每条消息都是“4 字节长度头 + UTF-8 JSON object”：

```text
0                   1                   2                   3
+-------------------+-------------------+-------------------+-------------------+
|              JSON body byte length N, unsigned 32-bit big-endian             |
+-------------------+-------------------+-------------------+-------------------+
|                         N bytes UTF-8 JSON object ...                        |
+-------------------------------------------------------------------------------+
```

固定要求：

- 长度头是无符号 32 位大端整数，即 `struct.pack(">I", N)`；
- `N` 必须在 `1` 到 `10 * 1024 * 1024`（10 MiB）之间；
- `N` 是 UTF-8 编码后的字节数，不是字符数；
- JSON 根节点必须是 object，拒绝 array、string、`null` 和 `NaN/Infinity`；
- 接收端必须支持半帧和粘包：一次 `recv()` 不等于一条消息。

最小编码示例：

```python
import json
import struct

def encode_frame(message: dict) -> bytes:
    body = json.dumps(
        message,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if not 0 < len(body) <= 10 * 1024 * 1024:
        raise ValueError("invalid frame size")
    return struct.pack(">I", len(body)) + body
```

### 6.3 连接状态机与首帧握手

```text
DISCONNECTED → TCP_CONNECTED → WAIT_PONG → IDENTITY_READY → BUSINESS_READY
      ▲                                                          │
      └────────── 断线、心跳超时、认证失败或 handler 失败 ────────┘
```

客户端连接后，第一帧必须是 `PING`，且同时携带协议、账户身份和令牌：

```json
{
  "type": "PING",
  "msg_id": "ping-001",
  "protocol_version": 2,
  "account_id": "<QMT_ACCOUNT_ID>",
  "account_name": "<ACCOUNT_NAME>",
  "auth_token": "<64_HEX_CHAR_TOKEN>",
  "timestamp": 1784257200.123
}
```

Gateway 校验令牌、`protocol_version=2`、`account_id` 与 `account_name`。成功后返回 `PONG`，客户端必须精确校验 `type`、原 `msg_id`、协议版本、Gateway `build_id` 和两个账户字段：

```json
{
  "type": "PONG",
  "msg_id": "ping-001",
  "protocol_version": 2,
  "gateway": "local_qmt_gateway",
  "build_id": "<EXPECTED_GATEWAY_BUILD_ID>",
  "account_id": "<QMT_ACCOUNT_ID>",
  "account_name": "<ACCOUNT_NAME>",
  "qmt_status": {"ready": true, "state": "ready"},
  "timestamp": 1784257200.124
}
```

认证失败或首帧不是 `PING` 时，Gateway 返回 `HANDSHAKE_REJECTED` 或 `HANDSHAKE_REQUIRED` 后关闭连接，不会注册 primary，也不会回显令牌。令牌只允许出现在每次新连接的首帧 PING，不得放进 `msg_id`、业务字段、日志或后续心跳。

### 6.4 公共字段与消息类型

| 字段 | 说明 |
|---|---|
| `type` | 消息类型，必填 |
| `msg_id` | 一次 TCP 请求/响应的关联键 |
| `protocol_version` | 首帧必须为 `2`；业务帧建议显式携带 |
| `account_id` / `account_name` | 所有查询、下单和撤单帧必填，且必须与已认证身份一致 |
| `request_id` | 下单、撤单的持久化副作用键 |
| `client_order_id` | 下单业务幂等键 |
| `delivery_id` | 可靠回调的投递去重与 ACK 键 |
| `timestamp` | Unix 秒时间戳，仅诊断用途 |

| 方向 | 类型 | 作用 |
|---|---|---|
| C → G | `PING` | 首帧认证或业务期心跳 |
| G → C | `PONG` | 握手/心跳响应与身份确认 |
| C → G | `QUERY` | 发起账户、资产、持仓、订单、成交或健康查询 |
| G → C | `QUERY_RESPONSE` | 查询结果；可带 `cache_fallback` |
| C → G | `NEW_ASYNC` | 异步下单请求 |
| G → C | `ASYNC_ORDER` | 下单的即时排队/拒绝结果 |
| G → C | `ASYNC_ORDER_RESPONSE` | Helper/QMT 提交结果；通常可靠 |
| C → G | `CANCEL_ASYNC` / `CANCEL_SYSID_ASYNC` | 按 QMT order ID 或市场/系统委托号撤单 |
| G → C | `ASYNC_CANCEL` | 撤单即时排队/拒绝结果 |
| G → C | `ASYNC_CANCEL_RESPONSE` | Helper/QMT 撤单调用结果；通常可靠 |
| G → C | `ORDER_UPDATE` / `TRADE_NOTIFY` / `ORDER_ERROR` | QMT 回调或状态变化 |
| C → G | `DELIVERY_ACK` | 确认已持久化可靠事件 |
| G → C | `QMT_STATUS` / `RECONCILE_REQUIRED` / `ERROR` | 健康、对账或拒绝信息 |

### 6.5 下单、撤单与 ACK 标准

`NEW_ASYNC` 至少包含 `symbol`、`side`（`BUY` 或 `SELL`）、正数 `quantity`、非负 `price`、稳定非空 `client_order_id`、账户字段和 `request_id`。同一 `client_order_id` 配合相同规范意图会返回已有结果，不会重复入队；参数不一致时拒绝。

可靠事件处理遵循以下顺序：读取完整帧 → 调用该类型的 handler → 以 `delivery_id` 幂等写入业务库并提交 → 所有 handler 成功 → 发送 ACK。ACK 格式如下：

```json
{
  "type": "DELIVERY_ACK",
  "msg_id": "ack-001",
  "delivery_id": "response:order-001:QMT_SUBMITTED",
  "timestamp": 1784257202.06
}
```

Gateway 的单次 ACK 等待窗口为 1 秒。未 ACK、连接断开或 handler 报错会导致可靠事件重投，因此 handler 必须幂等；不要仅打印事件后 ACK。

### 6.6 重试与错误处理

| 结果 | 客户端动作 |
|---|---|
| `GATEWAY_BUSY` 且 `effect_started=false` | 可退避重试，复用原 `request_id` 和 `client_order_id` |
| `HANDSHAKE_REJECTED` / `ACCOUNT_MISMATCH` | 停止重连，修正令牌、协议或账户配置 |
| `HELPER_NOT_READY` | 不交易，检查大 QMT 策略和三份健康文件 |
| `IDEMPOTENCY_CONFLICT` / `REQUEST_ID_CONFLICT` | 停止发送并调查关联键和参数 |
| `EFFECT_STATE_UNKNOWN` / `SUBMIT_UNKNOWN` / `POST_ENQUEUE_STATE_UNCERTAIN` | 禁止自动重发，使用原关联键查询和对账 |
| `RECONCILE_REQUIRED` | 查询订单、成交和账户状态后按业务规则处理 |
| `FRAME_TOO_LARGE` | 减小查询范围或响应大小 |

临时网络失败可按 0.5 秒、1 秒、2 秒、5 秒封顶退避重连；每次重连都必须重新握手。身份不匹配是永久配置错误，不应进行无休止重连。

## 7. 安全与运行要求

- 对 `.env`、生成配置、Helper 安装目录、runtime、日志和 SQLite 设置仅运行账户可读写的 NTFS ACL；回环地址不等于 Windows 权限隔离。
- 不提交 `.env`、生成的 Helper/loader、runtime、数据库、日志或包含真实账户身份的文件。
- Helper 命令基线为 25 ms 周期、每 tick 最多 4 条命令、15 ms 软预算；查询、对账和维护会为交易命令让路。
- Gateway 使用有界 worker、固定 pending 容量、目录扫描预算和 SQLite 单写者。容量不足在副作用开始前返回拒绝，而不是无限堆积。
- 实盘前应保持交易/撤单开关关闭完成连通性、健康、查询和 ACK 验收；开启后先在模拟或小额环境验证订单、成交、撤单、断线、重启和重复投递处理。

## 8. 快速接入

推荐使用项目自带 API：

```python
from qmt_local_api import LocalQmtApi

api = LocalQmtApi.from_env(r"D:\\path\\to\\project\\.env")

def persist_event(event):
    # 用 event["delivery_id"] 作为唯一键写入事务型业务库后再返回。
    pass

api.on("ORDER_UPDATE", persist_event)
api.on("TRADE_NOTIFY", persist_event)

if not api.connect():
    raise RuntimeError(api.identity_guard_reason or "Gateway unavailable")

try:
    status = api.query("ACCOUNT_STATUS")
    message_id = api.place_order_async(
        "600000.SH", "BUY", 100, 10.23,
        client_order_id="strategy-a-000001",
        request_id="strategy-a-000001",
    )
finally:
    api.stop()
```

请将可靠回调消费者与下单策略部署在同一个协调进程中，或显式保证同一账户只有一个 primary 连接。原始 socket 接入可参考 `示例/raw_tcp_query.py`；完整字段、原始帧和各类消息示例请参阅 [文档/TCP协议与外置API.md](文档/TCP协议与外置API.md)。

## 9. 相关入口

- [README.md](README.md)：部署入口与整体说明。
- [最简易部署文档.md](最简易部署文档.md)：最短部署流程。
- [文档/项目整体参考.md](文档/项目整体参考.md)：完整实现边界、参数和验收要求。
- [网关/README.md](网关/README.md)：Gateway 持久化与 IPC 细节。
- [大QMT内置python/README.md](大QMT内置python/README.md)：Helper 生成、加载与安全边界。
- [外置策略API/README.md](外置策略API/README.md)：Python 公共 API。
