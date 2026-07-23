# 外置策略 API

`qmt_local_api` 面向同一台 Windows 上的普通 CPython 策略。它只从项目根 `.env` 及 `tools/project_env.py` 获取配置，固定连接 IPv4 loopback。每次新连接首帧只发送一次 64 位令牌和账户身份，且只在 PONG build/账户完整匹配后暴露 ready；心跳不重发令牌。

首次在项目根初始化：

```powershell
.\setup_venv.ps1
```

脚本会通过项目 `.venv` 中的 `.pth` 文件离线暴露 `qmt_local_api`，本地 API 无需执行 `pip install -e`。策略需要第三方依赖时，才使用 `& '.\.venv\Scripts\python.exe' -m pip install <依赖名>` 安装到项目虚拟环境；策略进程也必须由该解释器启动。

最小代码：

```python
from qmt_local_api import LocalQmtApi

api = LocalQmtApi.from_env(r"D:\path\to\project\.env")
api.on("ORDER_UPDATE", lambda event: save_to_database(event))
api.on("TRADE_NOTIFY", lambda event: save_to_database(event))

if not api.connect():
    raise RuntimeError(api.identity_guard_reason or "Gateway unavailable")

try:
    status = api.query("ACCOUNT_STATUS")
    msg_id = api.place_order_async(
        "600000.SH",
        "BUY",
        100,
        10.23,
        client_order_id="strategy-a-20260717-0001",
    )
finally:
    api.stop()
```

回调函数返回成功后，客户端才发送 `DELIVERY_ACK`。生产策略必须在 handler 返回前完成数据库/日志的持久化；任一 handler 抛错会断开连接且不 ACK，让 Gateway 重投。不要在多个进程中同时消费同一个账户的可靠事件。

## Public API

- `LocalQmtApi.from_env(env_file=<项目根/.env>)`；
- `connect(timeout=None) -> bool`、`stop(timeout=5)`；
- `on(message_type, handler)`、`off(message_type, handler)`；
- `query(query_type='', params=None, timeout=None)`；
- `place_order_async(symbol, side, quantity, price, *, client_order_id, request_id='', ...) -> msg_id`；
- `cancel_order_async(order_id, *, request_id='') -> msg_id`；
- `cancel_order_by_sysid_async(market, order_sysid, *, request_id='') -> msg_id`；
- `wait_delivery_acknowledged(delivery_id, timeout=2) -> bool`。

完整下单构造签名：

```python
build_order_request(
    symbol, side, quantity, price,
    *,
    client_order_id,
    request_id="",
    price_type=11,
    order_type=0,
    strategy_name="qmt_local_api",
    order_remark="",
    trace_id="",
    qmt_user_order_id="",
    authenticated_trader_key="",
    intent_hash="",
    spread=0.0,
    business_order_type="limit",
    credit_mode="",
    intent_volume=None,
    intent_effective_price=None,
    async_mode=True,
)
```

`send_order_async`/`place_order_async` 使用同一组参数并直接发送。撤单构造为 `build_cancel_request(order_id, *, async_mode=False, request_id='')` 或 `build_cancel_sysid_request(market, order_sysid, *, async_mode=False, request_id='')`。

`client_order_id` 是下单业务幂等键；`request_id` 是 NEW/CANCEL 交易副作用的网关幂等键。同一交易意图跨连接重试时可使用新的 `msg_id`，但必须复用原 `request_id`；省略或只传空白时保持原行为。`qmt_user_order_id` 最长 23 字符。建议省略 `intent_hash`，由 Gateway 按规范字段计算。

## 多策略共用一个账户

多个策略进程不能各自直接连接 `LocalQmtApi`。Gateway 只允许一个可靠事件 primary；新连接会替换旧连接，导致订单/成交回调归属不稳定。

使用 `AccountCoordinator` 作为每账户唯一的外置接入者。它只保留一个 `LocalQmtApi` 连接，并将策略命令、账户级风险预占、可靠 Gateway 事件与每个策略的 durable outbox 写入自身 SQLite WAL。Gateway 的带 `delivery_id` 回调只有在 Coordinator 已完成 inbox、订单投影和 outbox 的同一事务后才 ACK。

```python
from pathlib import Path

from qmt_local_api import (
    AccountCoordinator,
    CoordinatorLocalServer,
    LocalQmtApi,
    RiskLimits,
)

api = LocalQmtApi.from_env(r"D:\bridge\.env")
coordinator = AccountCoordinator(
    api,
    Path(r"D:\bridge\coordinator_state.sqlite3"),
    account_limits=RiskLimits(max_pending_notional=1_000_000),
)
coordinator.register_strategy(
    "alpha",
    "<alpha 的独立本机凭据>",
    limits=RiskLimits(max_order_notional=100_000),
)
coordinator.register_strategy(
    "beta",
    "<beta 的独立本机凭据>",
    limits=RiskLimits(max_order_notional=50_000),
)

if not coordinator.start():
    raise RuntimeError("Gateway、Helper 或账户状态未就绪")

server = CoordinatorLocalServer(coordinator, host="127.0.0.1", port=9560)
server.start()
```

策略通过独立凭据连接 `CoordinatorLocalServer`：首帧为 `COORDINATOR_HELLO`，之后可发送 `ORDER_INTENT`、`CANCEL_INTENT`、`POLL_EVENTS`、`ACK_EVENT` 与 `ACCOUNT_STATUS`。`POLL_EVENTS` 为至少一次的持久化拉取；策略必须按 `coordinator_event_id` 先落库去重，再调用 `ACK_EVENT`。策略不得拥有根 `.env`、Gateway token 或 Helper runtime 的读写权限。

`strategy_order_id` 是策略侧稳定幂等键。Coordinator 为实际 Gateway 下单统一生成账户范围唯一的 `client_order_id` 与 `request_id`，并拒绝跨策略的额度冲突。发生 `SUBMIT_UNKNOWN`、`EFFECT_STATE_UNKNOWN` 或 `RECONCILE_REQUIRED` 时，它会冻结账户新交易并要求对账，绝不换新 ID 自动重发。

停止顺序为：先停止策略的命令入口，再停止 `CoordinatorLocalServer`，随后停止 `AccountCoordinator`，最后才停止 Gateway。Coordinator 数据库属于交易安全状态，不能与其 WAL/SHM 文件拆开删除或恢复。

## 示例

- `示例/protocol_demo.py`：完全离线的拆包/粘包帧演示；
- `示例/raw_tcp_query.py`：从根 `.env` 加载身份/令牌的原始 socket 诊断实例；
- `示例/query_account.py`：只读查询；
- `示例/callback_client.py`：回调先写 SQLite 再 ACK；
- `示例/async_order.py`：默认 dry-run 的异步下单；
- `示例/async_cancel.py`：默认 dry-run 的异步撤单。

异步下单的 `ASYNC_ORDER` 只表示 Gateway 排队结果，`ASYNC_ORDER_RESPONSE` 才表示 Helper/QMT 提交结果；异步撤单同理，`ASYNC_CANCEL` 之后还会收到可靠的 `ASYNC_CANCEL_RESPONSE`。撤单调用结果仍不是券商最终终态，最终订单/成交状态以 `ORDER_UPDATE`/`TRADE_NOTIFY` 为准。超时、断线或 `SUBMIT_UNKNOWN` 不得自动重复产生交易副作用。
