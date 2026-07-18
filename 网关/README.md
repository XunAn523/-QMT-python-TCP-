# 本机 Gateway

`bigqmt_gateway_proxy.py` 是外置策略与大 QMT Helper 之间唯一的桥。它只绑定根 `.env` 指定的 `127.0.0.1:<TCP_PORT>`，不读取第二份 `.env`，也不接受人工维护的部署 JSON。

启动入口位于项目根：

```powershell
.\start_gateway.ps1
```

脚本从根 `.env` 生成 `gateway_config.json`，执行生产预检，再传给网关。直接启动 Python 文件时仍必须同时提供生成配置和 `.env` 解析出的日志目录：

```powershell
python .\网关\bigqmt_gateway_proxy.py --config C:\Quant\QmtLocalBridge\generated\gateway_config.json --log-dir C:\Quant\QmtLocalBridge\logs
```

推荐始终使用根脚本。加载器会拒绝非回环监听、非单账户配置、缺失 runtime，以及被削弱的协议/性能常量。

## 保留的可靠性能力

- TCP v2、4 字节大端帧长、10 MiB 上限和 `TCP_NODELAY`；
- 首帧 PING 必须通过 `.env` 64 位随机令牌、protocol、账户 ID/name 校验，认证前不注册 primary；
- PONG 再由客户端校验 Gateway build、账户 ID 和账户名，令牌不回显且不进入心跳；
- Helper 的账户 ID/type/name/runtime/build/protocol/25ms 周期校验；
- 请求原子入队、命令/查询分流、响应与事件 10ms watcher；响应 watcher 每轮只做一次有界目录扫描；
- SQLite WAL/NORMAL 订单关联、`client_order_id` 幂等和单 writer lease；
- 可靠事件在收到 `DELIVERY_ACK` 前保留，失败或断线后重投；
- 查询 singleflight、缓存降级标记和未知提交状态保护；
- 固定的连接 dispatch、交易准备、pending response 和可靠投递背压，过载只在产生副作用前返回 `GATEWAY_BUSY`。

`order_correlation.py` 是网关内部实现，不是外置策略 API。外置代码只导入 `qmt_local_api`。

## 文件 IPC

runtime 为 `.env` 的 `QMT_LOCAL_RUNTIME_ROOT\<TCP_PORT>`。Gateway 写入 `inbox\commands` 或 `inbox\queries`，Helper 以原子移动方式取走；结果进入 `responses`，回调进入 `events\live`。`heartbeat.json`、`state.json` 和 `readiness.json` 必须同时报告一致身份并保持新鲜，网关才允许产生交易副作用。

Gateway 和 Helper 必须各只有一个实例。第二个 Gateway 会被 writer lease 拒绝。
