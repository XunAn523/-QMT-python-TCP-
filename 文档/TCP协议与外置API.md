# Windows 本机大 QMT TCP v2 与外置 API

本文定义外置策略与本机 Gateway 之间的线协议。生产代码优先使用 `qmt_local_api`；原始 socket 适合诊断、其他语言接入和二次开发。

## 1. 连接端点

唯一端点来自项目根 `.env`：

```text
host = QMT_LOCAL_BIND_HOST = 127.0.0.1
port = QMT_LOCAL_TCP_PORT  = 9550（可修改）
```

Gateway 与客户端都拒绝非 IPv4 loopback。本方案没有局域网模式、TLS 模式或入站防火墙规则。如果策略移到另一台机器，应使用原 Windows/Linux 项目，不应把本项目改成 `0.0.0.0`。

回环不等于认证。首帧还必须携带根 `.env` 中的 `QMT_LOCAL_AUTH_TOKEN`：随机 32 字节、64 位十六进制。Gateway 配置中只保存其 SHA-256，首帧的 `auth_token` 字段会在分发前删除，PONG、业务帧和日志均不回显该字段。客户端也不得把令牌复制到 `msg_id` 或任何业务字段；Gateway 会拒绝把令牌用作 `msg_id` 的帧，但业务字段本身仍属于调用者负责的数据。

令牌可阻止不知密钥的本机进程，但不是 Windows 权限隔离的替代品：能读取 `.env` 的同权限进程或管理员仍然可获得它。必须用 NTFS ACL 将 `.env`、生成配置、runtime 和日志限定给运行账户。

同一账户只允许一个可靠事件消费者。新连接完成握手后会成为 primary，旧连接被替换。

## 2. TCP 帧

每条消息为：

```text
0                   1                   2                   3
0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      JSON body byte length N, unsigned 32-bit big-endian      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|              N bytes UTF-8 JSON object ...                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

固定合同：

- header 恰好 4 字节，`struct.pack(">I", N)`；
- `1 <= N <= 10 * 1024 * 1024`；
- N 是 UTF-8 编码后的字节数，不是字符数；
- body 根必须是 JSON object，不能是 array/string/null；
- 只接受有限数字，`NaN`、`Infinity`、`-Infinity` 非法；
- 一次 `recv()` 可能只得到半帧，也可能得到多帧；必须按长度累计解码；
- 连接使用 `TCP_NODELAY`，API 还启用 Windows keepalive；
- socket 上只能有一个 reader，所有 writer 必须经过同一发送锁。

编码函数：

```python
import json
import struct

MAX_FRAME_BYTES = 10 * 1024 * 1024

def encode_frame(message: dict) -> bytes:
    body = json.dumps(
        message,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if not 0 < len(body) <= MAX_FRAME_BYTES:
        raise ValueError("invalid frame size")
    return struct.pack(">I", len(body)) + body
```

完整增量解码器在 `外置策略API/qmt_local_api/protocol.py`，离线拆包/粘包演示在 `示例/protocol_demo.py`。

## 3. 连接状态机

```text
DISCONNECTED
  │ TCP connect 127.0.0.1
  ▼
TCP_CONNECTED
  │ 第一帧必须发送带令牌/协议/账户身份的 PING
  ▼
WAIT_PONG
  │ Gateway 先验令牌/protocol/account，客户再验 PONG msg_id/build/account
  ▼
IDENTITY_READY
  │ 启动唯一 reader、dispatcher、heartbeat
  ▼
BUSINESS_READY
  │ 断线/心跳超时/handler 失败
  └──────────────────────────────► DISCONNECTED
                                      │ 0.5/1/2/5s 重连
                                      └─ 每次重新校验 PONG
```

业务消息不能早于握手。Gateway 收到非 PING 首帧会返回 `HANDSHAKE_REQUIRED`；令牌、protocol、account ID/name 任一错配返回不泄露账户的 `HANDSHAKE_REJECTED`。两者都会关闭连接，且不会注册或替换 primary。

## 4. PING/PONG 身份握手

客户端首帧：

```json
{
  "type": "PING",
  "msg_id": "ping-unique-id",
  "protocol_version": 2,
  "account_id": "REPLACE_WITH_REAL_QMT_ACCOUNT_ID",
  "account_name": "account_main",
  "auth_token": "REPLACE_WITH_64_HEX_SECRET",
  "timestamp": 1784257200.123
}
```

Gateway 响应：

```json
{
  "type": "PONG",
  "msg_id": "ping-unique-id",
  "protocol_version": 2,
  "gateway": "local_qmt_gateway",
  "build_id": "xuanling_local_qmt_gateway_20260718_low_latency_v3_bounded_io",
  "account_id": "REPLACE_WITH_REAL_QMT_ACCOUNT_ID",
  "account_name": "account_main",
  "qmt_status": {
    "ready": true,
    "state": "ready"
  },
  "timestamp": 1784257200.124
}
```

客户端必须精确检查：

| 字段 | 期望 |
|---|---|
| `type` | `PONG` |
| `msg_id` | 等于刚发送的 PING ID |
| `protocol_version` | `2` |
| `build_id` | 当前包固定 Gateway build |
| `account_id` | 根 `.env` 的真实账户 ID |
| `account_name` | 根 `.env` 的账户逻辑名 |

缺字段也视为错配。身份错配是永久配置错误：关闭连接、停止自动重连和业务发送，等待人工修复。网络不可用是临时错误，可以退避重连。

令牌只出现在每次 TCP 新连接的首帧 PING，不在心跳中重发。业务期间是客户端每 5 秒发送 PING，Gateway 返回 PONG；当前 Gateway 不主动发 PING。API 15 秒没有任何入站消息就断开，Gateway idle 上限 60 秒。

## 5. 公共字段与相关键

| 字段 | 语义 |
|---|---|
| `type` | 消息类型，必填 |
| `msg_id` | 一次 TCP 请求/响应相关 ID |
| `protocol_version` | 首帧必须为 `2`；业务帧建议显式携带 |
| `account_id` | 首帧和所有 QUERY/交易/撤单帧必填，必须与已认证连接一致 |
| `account_name` | 首帧和所有 QUERY/交易/撤单帧必填的单账户逻辑名 |
| `auth_token` | 仅首帧 PING 携带的 64 位十六进制密钥，禁止记录/回显 |
| `timestamp` | Unix 秒，诊断用，不能作为幂等键 |
| `request_id` | Helper 文件请求相关 ID，通常由 Gateway 派生 |
| `client_order_id` | 业务订单幂等键，跨重连/状态查询保持稳定 |
| `qmt_user_order_id` | 传给 QMT 的关联键，最多 23 字符 |
| `delivery_id` | 可靠回调幂等键，持久化后用于 ACK |
| `trace_id` | 外置链路追踪 ID，不替代 `client_order_id` |

`msg_id` 标识一次线请求，`client_order_id` 标识业务订单；二者不能混用。网络重试可以产生新 `msg_id`，但同一业务订单必须保留原 `client_order_id`。

## 6. 查询

请求：

```json
{
  "type": "QUERY",
  "msg_id": "query-001",
  "account_id": "REPLACE_WITH_REAL_QMT_ACCOUNT_ID",
  "account_name": "account_main",
  "query_type": "ACCOUNT_STATUS",
  "params": {},
  "timestamp": 1784257201.0
}
```

响应：

```json
{
  "type": "QUERY_RESPONSE",
  "msg_id": "query-001",
  "success": true,
  "query_type": "ACCOUNT_STATUS",
  "account_status": {
    "ready": true,
    "state": "ready"
  },
  "reject_reason": "",
  "timestamp": 1784257201.01
}
```

支持的核心 `query_type`：

- `ACCOUNT_STATUS`；
- `ACCOUNT`、`ASSET`、`ACCOUNT_INFOS`、`COM_FUND`；
- `POSITION`、`COM_POSITION`；
- `ORDER`；
- `DEAL`、`TRADE`。

所有 QUERY 由唯一 TCP reader 交给 bounded query broker，再按 `msg_id` 唤醒 waiter；调用查询的线程不得自己读取 socket。实时成功响应不含 `cache_fallback=false`；只有缓存降级时才出现 `cache_fallback=true`。该字段或 `state=degraded` 表示不是实时 ready，交易前必须拒绝。

API：

```python
status = api.query("ACCOUNT_STATUS", timeout=6.0)
position = api.query("POSITION", {"stock_code": "600000.SH"})
```

## 7. 异步下单

推荐请求：

```json
{
  "type": "NEW_ASYNC",
  "msg_id": "wire-order-001",
  "request_id": "wire-order-001",
  "protocol_version": 2,
  "account_id": "REPLACE_WITH_REAL_QMT_ACCOUNT_ID",
  "account_name": "account_main",
  "symbol": "600000.SH",
  "side": "BUY",
  "quantity": 100,
  "price": 10.23,
  "price_type": 11,
  "order_type": 23,
  "strategy_name": "alpha",
  "order_remark": "signal-A",
  "client_order_id": "alpha-20260717-000001",
  "trace_id": "trace-000001",
  "created_at_ns": 1784257202000000000,
  "timestamp": 1784257202.0
}
```

必填业务字段：`symbol`、`side`、`quantity`、`price`、稳定非空的 `client_order_id`。`side` 为 `BUY/SELL`，数量必须大于 0，价格不能小于 0。API 默认 BUY/SELL 分别派生 QMT 操作类型 23/24；如使用其他 QMT 业务类型，应明确传入 `order_type` 并在仿真账户验证。

通常不要传 `intent_hash`。Gateway 会按账户、代码、方向、意图数量、有效价格、price type、业务订单类型、spread、credit mode、策略名、交易员字段和认证字段计算规范 SHA-256。客户端传入的 hash 不一致会收到 `INTENT_HASH_MISMATCH`。

第一阶段响应：

```json
{
  "type": "ASYNC_ORDER",
  "msg_id": "wire-order-001",
  "status": "SENT",
  "stage": "BRIDGE_QUEUED",
  "client_order_id": "alpha-20260717-000001",
  "request_id": "wire-order-001",
  "seq": 0,
  "timestamp": 1784257202.002
}
```

它只说明 Gateway 已写入 Helper 队列。最终 Helper 提交结果由可靠消息返回：

```json
{
  "type": "ASYNC_ORDER_RESPONSE",
  "delivery_id": "response:wire-order-001:QMT_SUBMITTED",
  "request_id": "wire-order-001",
  "client_order_id": "alpha-20260717-000001",
  "stage": "QMT_SUBMITTED",
  "submit_result": "KNOWN",
  "order_id": "QMT_ORDER_ID",
  "timestamp": 1784257202.05
}
```

可靠提交结果的 `stage` 为 `QMT_SUBMITTED`、`REJECTED` 或 `SUBMIT_UNKNOWN`，不使用 `status=ACCEPTED`。随后可能还有 `ORDER_UPDATE` 和 `TRADE_NOTIFY`；只有 QMT/券商回调能说明订单与成交最终状态。

API：

```python
msg_id = api.place_order_async(
    "600000.SH",
    "BUY",
    100,
    10.23,
    client_order_id="alpha-20260717-000001",
    strategy_name="alpha",
    order_remark="signal-A",
)
```

相同 `client_order_id` + 相同规范意图返回幂等结果，不重复入队；相同 ID + 不同意图返回 `IDEMPOTENCY_CONFLICT`。

## 8. 异步撤单

按 QMT order ID：

```json
{
  "type": "CANCEL_ASYNC",
  "msg_id": "cancel-001",
  "account_id": "REPLACE_WITH_REAL_QMT_ACCOUNT_ID",
  "account_name": "account_main",
  "order_id": "QMT_ORDER_ID",
  "timestamp": 1784257203.0
}
```

按市场和系统委托号：

```json
{
  "type": "CANCEL_SYSID_ASYNC",
  "msg_id": "cancel-002",
  "account_id": "REPLACE_WITH_REAL_QMT_ACCOUNT_ID",
  "account_name": "account_main",
  "market": 0,
  "order_sysid": "BROKER_ORDER_SYSID",
  "timestamp": 1784257203.0
}
```

异步响应 `ASYNC_CANCEL status=SENT cancel_status=queued` 仅表示已排队；异步响应没有 `final`。同步撤单的 `EXEC_REPORT cancel_status=cancel_sent final=false` 只表示已发送。两者最终都必须等待 `ORDER_UPDATE` 中券商/QMT 的真实终态。

API：

```python
api.cancel_order_async("QMT_ORDER_ID")
api.cancel_order_by_sysid_async(0, "BROKER_ORDER_SYSID")
```

## 9. 可靠回调与 DELIVERY_ACK

可靠与否以当前实例是否带非空 `delivery_id` 为准。常见可靠实例包括：

- `ASYNC_ORDER_RESPONSE`；
- Helper 文件事件产生的 `ORDER_UPDATE`/`TRADE_NOTIFY`；
- `ORDER_ERROR`；
- 文件队列产生的其他带 `delivery_id` 事件。

处理规则：

1. 读取完整帧；
2. dispatcher 按线序调用该类型的所有 handler；
3. 负责持久化的 handler 以 `delivery_id` 幂等写入业务数据库并提交；API 不内置业务库；
4. 全部 handler 成功返回后发送 ACK；
5. 任一 handler 失败，不 ACK 并关闭连接。

ACK：

```json
{
  "type": "DELIVERY_ACK",
  "msg_id": "ack-local-001",
  "delivery_id": "response:wire-order-001:QMT_SUBMITTED",
  "timestamp": 1784257202.06
}
```

这是至少一次语义，因此 handler 不能只做内存打印后 ACK。`示例/common.py` 使用 SQLite `delivery_id PRIMARY KEY`、WAL/FULL 和事务提交；`callback_client.py` 演示长期消费。Helper 实时文件事件投递失败累计三次后会移入 `events/failed`、停止自动重投并发送 `RECONCILE_REQUIRED`；`ASYNC_ORDER_RESPONSE` 使用独立的 pending response 重投机制。

`query_account.py` 是短命只读进程：如果意外收到可靠事件，它会让 handler 抛错并拒绝 ACK，避免吞掉生产回调。

## 10. Gateway 回调类型

外置 API 可通过 `api.on(type, handler)` 订阅：

| 类型 | 含义 |
|---|---|
| `ASYNC_ORDER` | Gateway 即时排队/拒绝结果 |
| `ASYNC_ORDER_RESPONSE` | Helper/QMT 提交结果，可靠 |
| `ASYNC_CANCEL` | 异步撤单排队/拒绝结果 |
| `EXEC_REPORT` | 同步动作响应或兼容执行回报 |
| `ORDER_UPDATE` | QMT 订单状态；带 `delivery_id` 的 Helper 文件回调可靠，无该字段的快照差分为 best-effort |
| `TRADE_NOTIFY` | QMT 成交状态；带 `delivery_id` 的 Helper 文件回调可靠，无该字段的快照差分为 best-effort |
| `ORDER_ERROR` | QMT 下单错误回调，可靠 |
| `ASSET_UPDATE` | 资产更新 |
| `POSITIONS_SNAPSHOT` | 持仓快照 |
| `QMT_STATUS` | Helper/Gateway 健康状态 |
| `RECONCILE_REQUIRED` | 状态不确定，需要对账 |
| `ERROR` | 协议、身份或业务拒绝 |

一个类型可以注册多个 handler；只有带 `delivery_id` 的实例在全部 handler 成功后 ACK。不要在 handler 内长时间阻塞；耗时业务应先可靠落盘，再交给独立业务队列。

## 11. 错误与未知状态

常见 `code/stage`：

| 值 | 处理 |
|---|---|
| `HANDSHAKE_REQUIRED` | 修正首帧顺序 |
| `HANDSHAKE_REJECTED` | 停止连接，检查令牌/protocol/账户身份；不要爆破重试 |
| `ACCOUNT_MISMATCH` | 停止发送，修复 `.env`/账户端口 |
| `CLIENT_ORDER_ID_REQUIRED` | 生成稳定业务幂等 ID 后再提交 |
| `HELPER_NOT_READY` | 不下单，检查大 QMT 策略和三份 health |
| `IDEMPOTENCY_CONFLICT` | 同一业务 ID 参数冲突，人工调查 |
| `INTENT_HASH_MISMATCH` | 删除自造 hash 或按规范计算 |
| `FRAME_TOO_LARGE` | 缩小查询范围/响应 |
| `GATEWAY_BUSY` | `effect_started=false` 表示尚未写交易队列、未调用 Helper；按退避策略使用相同幂等 ID 重试，不要生成新 ID |
| `SUBMIT_UNKNOWN` | 不自动重发，按关联键对账 |
| `POST_ENQUEUE_STATE_UNCERTAIN` | 可能已经产生副作用，不自动重发 |
| `RECONCILE_REQUIRED` | 查询订单/成交并人工或规则化对账 |

TCP 发送成功不等于 QMT 接受；`ASYNC_ORDER SENT` 也不等于 QMT 接受。Gateway 可能在即时 `ASYNC_ORDER` 中返回终止性 `REJECTED` 或不确定 `SUBMIT_UNKNOWN`；已排队后则以可靠 `ASYNC_ORDER_RESPONSE` 和后续 QMT 回调推进状态。

## 12. Public Python API

首次在项目根执行 `.\setup_venv.ps1`。脚本通过 `.pth` 离线暴露 `qmt_local_api`，无需安装本地 API；以下实例均使用项目 `.venv`。

```python
LocalQmtApi.from_env(env_file=<project-root/.env>)
api.connect(timeout=None) -> bool
api.stop(timeout=5.0)
api.on(message_type, handler)
api.off(message_type, handler)
api.query(query_type="", params=None, timeout=None) -> dict | None
api.place_order_async(symbol, side, quantity, price, *, client_order_id, ...) -> str
api.cancel_order_async(order_id) -> str
api.cancel_order_by_sysid_async(market, order_sysid) -> str
api.wait_delivery_acknowledged(delivery_id, timeout=2.0) -> bool
```

下单完整可选参数定义在 `qmt_local_api/client.py::build_order_request`，包括 `price_type`、`order_type`、`strategy_name`、`order_remark`、`trace_id`、`qmt_user_order_id`、认证字段、credit mode 和意图字段。二次开发应优先封装 public API，不应调用 transport 私有 reader。

## 13. 原始 socket 标准实例

`示例/raw_tcp_query.py` 是可运行的标准库实例，它：

- 从根 `.env` 和 `project_env.py` 取 host/port/身份/令牌；
- 设置 `TCP_NODELAY`；
- 实现 `recv_exact`；
- 首帧仅发送一次令牌，并检查六个 PONG 字段；
- 发送 `ACCOUNT_STATUS` QUERY；
- 兼容性处理意外 PING（当前 Gateway 不主动发送）；
- 遇到可靠事件时拒绝 ACK，提示改用正式 API。

原始诊断客户完成握手后必然成为新 primary。运行前先停止其他外置消费者，避免它替换正在工作的可靠回调消费者：

```powershell
& '.\.venv\Scripts\python.exe' .\示例\raw_tcp_query.py --query-type ACCOUNT_STATUS
```

生产策略不要复制一个简化 socket 循环后自行增加线程读取；直接使用 `qmt_local_api` 可以避免双 reader、乱序 ACK 和查询抢帧。

## 14. 可运行示例

完全离线：

```powershell
& '.\.venv\Scripts\python.exe' .\示例\protocol_demo.py
```

只读查询：

```powershell
& '.\.venv\Scripts\python.exe' .\示例\query_account.py --query-type ACCOUNT_STATUS
```

可靠回调消费者：

```powershell
& '.\.venv\Scripts\python.exe' .\示例\callback_client.py --query-on-start ACCOUNT_STATUS
```

下单 dry-run，不打开网络：

```powershell
& '.\.venv\Scripts\python.exe' .\示例\async_order.py `
  --symbol 600000.SH --side BUY --quantity 100 --price 10.23 `
  --client-order-id alpha-20260717-000001
```

实盘下单必须先在 `.env` 中启用账户和交易、重新生成/部署/重载 Helper，再显式执行：

```powershell
& '.\.venv\Scripts\python.exe' .\示例\async_order.py `
  --symbol 600000.SH --side BUY --quantity 100 --price 10.23 `
  --client-order-id alpha-20260717-000001 `
  --live --confirm I_UNDERSTAND_THIS_SENDS_A_LIVE_ORDER
```

撤单同样默认 dry-run，live 短语见 `& '.\.venv\Scripts\python.exe' .\示例\async_cancel.py --help`。

## 15. 性能与安全验收

二次开发不得改变：单 reader、全发送锁、10 MiB、首帧令牌+protocol+account 认证、PONG build/account 身份、带 `delivery_id` 实例在 handler 成功后 ACK、SQLite writer lease、10ms watcher、25ms Helper command、500ms query、15ms command budget、每周期最多4条交易命令，以及副作用前有界背压。

发布前测试至少覆盖：分片、粘包、非法长度、非 UTF-8、非 object、非有限数字、缺失/错误令牌、protocol/account 逐一错配、未认证连接无法抢占 primary、错误账户零副作用、Gateway 双开、缺失/相同/冲突订单 ID、ACK 丢失重投、handler 失败、查询 waiter、Helper 离线、断线重连和 `SUBMIT_UNKNOWN` 不重发。

真实大 QMT 验收还需要小额下单/撤单与 P50/P95/P99 非劣比较；离线 loopback 测试不能证明券商链路延迟或成交结果。
