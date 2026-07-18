# 本机 Gateway

`bigqmt_gateway_proxy.py` 是外置策略与大 QMT Helper 之间唯一的桥。它只绑定根 `.env` 指定的 `127.0.0.1:<TCP_PORT>`，不读取第二份 `.env`，也不接受人工维护的部署 JSON。

首次部署先在项目根执行 `.\setup_venv.ps1`。日常启动入口也位于项目根：

```powershell
.\start_gateway.ps1
```

脚本从根 `.env` 生成 `gateway_config.json`，执行生产预检，再传给网关。直接启动 Python 文件时仍必须同时提供生成配置和 `.env` 解析出的日志目录：

```powershell
& '.\.venv\Scripts\python.exe' .\网关\bigqmt_gateway_proxy.py --config C:\Quant\QmtLocalBridge\generated\gateway_config.json --log-dir C:\Quant\QmtLocalBridge\logs
```

推荐始终使用根脚本。加载器会拒绝非回环监听、非单账户配置、缺失 runtime，以及被削弱的协议/性能常量。

## 保留的可靠性能力

- TCP v2、4 字节大端帧长、10 MiB 上限和 `TCP_NODELAY`；
- 首帧 PING 必须通过 `.env` 64 位随机令牌、protocol、账户 ID/name 校验，认证前不注册 primary；
- PONG 再由客户端校验 Gateway build、账户 ID 和账户名，令牌不回显且不进入心跳；
- Helper 的账户 ID/type/name/runtime/build/protocol/25ms 周期校验；
- 请求原子入队、命令/查询分流；Windows `ReadDirectoryChangesW` 只负责唤醒响应/事件的有界目录扫描，句柄或目录异常时自动回退到 10ms 轮询；
- Helper 健康由每账户单一采样器每 100ms 更新，交易热路径只读最多 200ms 的缓存，过期即 fail-closed；
- 文件 I/O 与 SQLite 分别进入固定 worker/固定 pending 的执行通道，不使用无界默认 executor 队列；
- SQLite WAL/NORMAL 订单关联、`client_order_id` 幂等、事件批量事务、空闲 PASSIVE checkpoint 和单 writer lease；自动维护不删除历史订单关联，避免旧 `client_order_id` 恢复下单能力；
- 可靠事件在收到 `DELIVERY_ACK` 前保留，失败或断线后重投；
- 查询 singleflight、缓存降级标记和未知提交状态保护；
- 固定的连接 dispatch、交易准备、命令磁盘队列高水位、pending response 和可靠投递背压，过载只在产生副作用前返回 `GATEWAY_BUSY`；
- `NEW_ASYNC` 与异步撤单都在文件入队前写入 SQLite pending ledger，并追踪 Helper 最终响应；Gateway 重启可恢复，即时 ACK 之后分别以可靠的 `ASYNC_ORDER_RESPONSE` 或 `ASYNC_CANCEL_RESPONSE` 回传。

`order_correlation.py` 是网关内部实现，不是外置策略 API。外置代码只导入 `qmt_local_api`。

## 持久化副作用状态机

每个下单或撤单 `request_id` 都会永久绑定到规范化后的动作类型和 SHA-256 副作用指纹。相同 ID、相同指纹只会读取已有状态；相同 ID、不同动作或参数会返回 `REQUEST_ID_CONFLICT`，不会触达 Helper。状态机固定为：

| 状态 | 含义 | 是否允许自动执行原请求 |
|---|---|---|
| `PREPARED` | SQLite 已绑定身份，但尚未跨过 Helper 调度屏障 | 仅相同指纹可接管 |
| `DISPATCHING` | 已跨过持久化调度屏障，文件写入或 QMT 调用可能已经发生 | 否；重启后返回 `EFFECT_STATE_UNKNOWN` |
| `ENQUEUED` | Helper 队列或 QMT 提交结果已知 | 否；读取持久化结果或对账 |
| `UNKNOWN` | 副作用结果无法证明 | 否；必须查询订单、成交和回调 |
| `TERMINAL` | 当前提交动作已明确拒绝或终结 | 否；读取持久化结果 |

命令在认证连接读取帧时获得单调序号，并按接收顺序依次完成“原子文件发布”；同步请求等待 Helper 响应时不会阻塞后续文件入队。这个顺序合同覆盖同一 Gateway 进程收到的新格式命令，不把系统时钟回拨、人工放入的旧格式文件或跨机器复制解释为全局事务顺序。

异步 Helper response 会先完整写入 SQLite pending ledger，再删除 Helper response 文件。即使此时没有外置策略连接，Gateway 重启后仍会继续以同一 `delivery_id` 投递；只有目标 primary 会话返回 `DELIVERY_ACK` 后才删除 pending ledger。实时事件按 `event_seq` 串行投递，某一事件失败时停止发送后续事件，等待较早事件重试。

## 文件 IPC

runtime 为 `.env` 的 `QMT_LOCAL_RUNTIME_ROOT\<TCP_PORT>`。Gateway 写入 `inbox\commands` 或 `inbox\queries`，Helper 以原子移动方式取走；结果进入 `responses`，回调进入 `events\live`。`heartbeat.json`、`state.json` 和 `readiness.json` 必须同时报告一致身份并保持新鲜，网关才允许产生交易副作用。

Gateway 和 Helper 必须各只有一个实例。第二个 Gateway 会被 writer lease 拒绝。

## 故障保证边界

原子 JSON 的同目录临时文件加 `os.replace` 防止进程正常运行时读到半个 JSON；SQLite 使用 WAL + `synchronous=NORMAL`。它们覆盖进程崩溃恢复，但没有逐文件 `fsync`，不承诺突然断电、磁盘控制器缓存丢失或状态目录被人工回滚后的端到端 exactly-once。发生 `DISPATCHING`、`SUBMIT_UNKNOWN`、超时或断线时，必须用原关联键对账，禁止自动生成新 ID 重发。

可靠回调是 at-least-once。外置 handler 必须先以 `delivery_id` 持久化去重再返回；慢业务应在落库后转交自己的队列。不得单独删除或恢复 `gateway_state.sqlite3`、其 WAL/SHM 或 runtime 文件。副作用与订单幂等记录默认永久保留，应监控磁盘并通过明确的离线归档流程处理，不能用自动 TTL 恢复旧 ID 的执行能力。
