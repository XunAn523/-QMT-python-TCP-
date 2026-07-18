# 外置策略 API

`qmt_local_api` 面向同一台 Windows 上的普通 CPython 策略。它只从项目根 `.env` 及 `tools/project_env.py` 获取配置，固定连接 IPv4 loopback。每次新连接首帧只发送一次 64 位令牌和账户身份，且只在 PONG build/账户完整匹配后暴露 ready；心跳不重发令牌。

安装：

```powershell
python -m pip install -e ".\外置策略API"
```

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
- `place_order_async(symbol, side, quantity, price, *, client_order_id, ...) -> msg_id`；
- `cancel_order_async(order_id) -> msg_id`；
- `cancel_order_by_sysid_async(market, order_sysid) -> msg_id`；
- `wait_delivery_acknowledged(delivery_id, timeout=2) -> bool`。

完整下单构造签名：

```python
build_order_request(
    symbol, side, quantity, price,
    *,
    client_order_id,
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

`send_order_async`/`place_order_async` 使用同一组参数并直接发送。撤单构造为 `build_cancel_request(order_id, async_mode=False)` 或 `build_cancel_sysid_request(market, order_sysid, async_mode=False)`。

`client_order_id` 是业务幂等键，同一订单意图跨连接重试时必须保持不变；同一个键带不同订单参数会被拒绝。`qmt_user_order_id` 最长 23 字符。建议省略 `intent_hash`，由 Gateway 按规范字段计算。

## 示例

- `示例/protocol_demo.py`：完全离线的拆包/粘包帧演示；
- `示例/raw_tcp_query.py`：从根 `.env` 加载身份/令牌的原始 socket 诊断实例；
- `示例/query_account.py`：只读查询；
- `示例/callback_client.py`：回调先写 SQLite 再 ACK；
- `示例/async_order.py`：默认 dry-run 的异步下单；
- `示例/async_cancel.py`：默认 dry-run 的异步撤单。

异步下单的 `ASYNC_ORDER` 只表示 Gateway 排队结果，`ASYNC_ORDER_RESPONSE` 才表示 Helper/QMT 提交结果，最终成交状态以 `ORDER_UPDATE`/`TRADE_NOTIFY` 为准。超时、断线或 `SUBMIT_UNKNOWN` 不得自动重复下单。
