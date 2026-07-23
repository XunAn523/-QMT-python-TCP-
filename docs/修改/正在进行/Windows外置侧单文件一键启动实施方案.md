# Windows 外置侧单文件一键启动实施方案

> 文档状态：待实施。
> 编写日期：2026-07-23。
> 适用范围：一台 Windows、一个大 QMT 账户、现有 Gateway、可选多策略 Coordinator。
> 目标：运维人员在外置 Windows 端只手动启动一个文件，即完成 Gateway、Coordinator 和已注册策略进程的受控启动、健康检查、监控与停止。

## 1. 结论与启动体验

新增项目根入口：

```powershell
.\start_external_windows.ps1
```

这是外置 Windows 端唯一需要人工启动的文件。它不是将全部代码压缩为一个 `.ps1`，而是一个受控的进程编排器：它依次准备环境、检查大 QMT Helper、启动 Gateway、启动 Coordinator，并在配置允许时启动多个策略工作进程。

```text
用户运行 start_external_windows.ps1
  ├─ 校验 .env / .venv / 本地路径 / Helper 健康
  ├─ 启动现有 TCP Gateway
  ├─ 启动唯一 Coordinator Host
  ├─ 验证 Coordinator 的 loopback 端点和账户 ready 状态
  ├─ 按配置启动 strategy-a、strategy-b …
  └─ 监控子进程；Ctrl+C 或 -Stop 时按安全顺序停止
```

大 QMT 客户端仍须已登录，且已加载并启动生成后的 Helper 策略；外置启动器只验证其 health/readiness，不自动登录、关闭或操作大 QMT GUI。

## 2. 现状与缺口

当前项目已有：

- `setup_venv.ps1`：一次性创建/验证项目 `.venv`，并写入本地 API `.pth`；
- `generate_helper.ps1`：生成并部署大 QMT 端 Helper；
- `start_gateway.ps1`：严格预检后以前台阻塞方式运行 Gateway；
- `AccountCoordinator` 与 `CoordinatorLocalServer`：多策略共享账户的库组件。

当前缺口是：

1. `start_gateway.ps1` 自身会阻塞，无法继续启动 Coordinator；
2. Coordinator 没有读取策略注册配置、持有生命周期和接收优雅停止信号的宿主程序；
3. 没有统一记录 Gateway/Coordinator/策略子进程 PID、健康状态和停止顺序；
4. 根 `.env` 是严格白名单，不能直接把策略凭据、Coordinator 端口或策略启动命令塞入其中。

因此，不能只修改 `start_gateway.ps1` 就满足目标；需要补齐下节定义的运行入口和非 QMT 配置。

## 3. 新增文件与职责

| 文件 | 是否由用户直接运行 | 职责 |
|---|---|---|
| `start_external_windows.ps1` | 是，唯一人工入口 | 生命周期编排、预检、子进程启动、健康等待、PID 清单、停止与状态查询 |
| `外置策略API/qmt_local_api/coordinator_host.py` | 否，只由启动器创建子进程 | 加载 Coordinator 配置，创建唯一 `LocalQmtApi`、`AccountCoordinator` 与 `CoordinatorLocalServer` |
| `coordinator_config.example.json` | 否，模板 | 可提交的脱敏 Coordinator 配置样例 |
| `coordinator_config.json` | 否，生产配置 | 策略身份、独立凭据哈希来源、额度、Coordinator 端口和可选策略进程；必须忽略并收紧 ACL |
| `runtime/external_windows_state.json` | 否，运行产物 | PID、启动时间、配置摘要、健康状态；不可作为交易账本恢复来源 |

根 `.env` 继续只保存账户、Gateway、Helper、路径和根 API 配置。`coordinator_config.json` 是外置策略层配置，不能替代或覆盖 `.env` 中任何 QMT 身份、端口、令牌或交易开关。

## 4. 目标进程拓扑

```text
start_external_windows.ps1（前台控制台、唯一人工入口）
  │
  ├─ Gateway child
  │    .venv\Scripts\python.exe -B 网关\bigqmt_gateway_proxy.py ...
  │    仅绑定 127.0.0.1:<QMT_LOCAL_TCP_PORT>
  │
  ├─ Coordinator Host child
  │    .venv\Scripts\python.exe -B 外置策略API\qmt_local_api\coordinator_host.py ...
  │    唯一 qmt_local_api → Gateway primary
  │    仅绑定 127.0.0.1:<coordinator_port>
  │
  └─ Strategy worker children（可选）
       strategy-a.py / strategy-b.py ...
       只连接 Coordinator，不读取 .env、不连接 Gateway
```

禁止的拓扑：

- 多个策略分别运行 `LocalQmtApi.from_env()`，抢占 Gateway primary；
- 策略进程读取根 `.env` 或持有 `QMT_LOCAL_AUTH_TOKEN`；
- 启动器向 Helper runtime 目录写命令；
- Coordinator 与 Gateway 绑定到 `0.0.0.0` 或局域网地址。

## 5. 配置设计

### 5.1 `coordinator_config.json`

生产配置示例；其中 token 是 Coordinator 本机策略凭据，不是 Gateway token，文件必须限制给 Coordinator 运行账户：

```json
{
  "version": 1,
  "server": {
    "host": "127.0.0.1",
    "port": 9560,
    "max_clients": 32
  },
  "state_db": "runtime\\coordinator_state.sqlite3",
  "account_limits": {
    "max_order_notional": 1000000.0,
    "max_pending_notional": 1000000.0
  },
  "strategies": [
    {
      "strategy_id": "alpha",
      "auth_token": "REPLACE_WITH_A_SEPARATE_RANDOM_LOCAL_SECRET",
      "enabled": true,
      "priority": 100,
      "limits": {
        "max_order_notional": 100000.0,
        "max_pending_notional": 200000.0
      },
      "worker": {
        "enabled": true,
        "program": "策略\\alpha.py",
        "arguments": [],
        "working_directory": "."
      }
    }
  ]
}
```

约束如下：

- 只允许 `version=1`、`server.host=127.0.0.1`、端口范围 `1..65535`、`max_clients=1..32`；
- `strategy_id` 必须匹配 `^[A-Za-z][A-Za-z0-9_-]{0,47}$`，且全局唯一；
- 每个 `auth_token` 至少 16 个字符，必须使用独立随机值，不能复用 Gateway token、账户密码或 `.env` 内容；
- `state_db`、worker `program` 与 `working_directory` 只能解析到项目根目录内的受控本地路径；拒绝 UNC、相对路径逃逸、NTFS ADS、盘符根和符号链接逃逸；
- worker 参数使用 JSON 字符串数组，禁止 `Invoke-Expression`、拼接 shell 字符串或由环境变量注入命令；
- worker 是可选项。设为 `enabled=false` 时，启动器仍会启动 Gateway 与 Coordinator，策略可由其他受控方式接入；
- 交易额度必须是有限且非负的数字；`0` 表示该维度不限制。

生产配置中的 `auth_token` 不应进入命令行、日志、PID 清单或异常输出。Coordinator 数据库只保存 token 的 SHA-256，不保存明文。

### 5.2 版本控制与 ACL

新增以下规则：

```gitignore
coordinator_config.json
runtime/external_windows_state.json
runtime/coordinator_state.sqlite3
runtime/coordinator_state.sqlite3-shm
runtime/coordinator_state.sqlite3-wal
```

`coordinator_config.example.json` 只包含占位值并可提交；生产 `coordinator_config.json` 应复制模板后填写。启动器在启动前检查生产配置不是示例 token，并提示运维人员使用 NTFS ACL 限制 `.env`、`coordinator_config.json`、Coordinator SQLite 和 runtime 目录。

## 6. `coordinator_host.py` 设计

Host 是一个普通 CPython 长进程，其启动参数固定为：

```text
--env-file <项目根/.env>
--config <项目根/coordinator_config.json>
--state-file <runtime/external_windows_state.json>
```

它必须按以下顺序执行：

1. 严格解析 Coordinator JSON，验证路径、端口、策略身份和额度；
2. 使用 `LocalQmtApi.from_env(env_file)` 创建唯一 Gateway 客户端；
3. 使用 `AccountCoordinator(api, state_db, account_limits=...)` 创建账本；
4. 对每个策略调用 `register_strategy()`；
5. 调用 `coordinator.start()`，其必须完成 Gateway PING/PONG 和非降级 `ACCOUNT_STATUS ready` 校验；
6. 创建 `CoordinatorLocalServer(..., host="127.0.0.1")` 并开始监听；
7. 向状态文件原子写入 `coordinator_ready=true`、PID、端口和不含凭据的健康摘要；
8. 等待 Ctrl+C、受控停止事件或父进程停止；
9. 停止时先停止接收策略命令，再停止 Server、Coordinator、Gateway 客户端，最后原子更新状态文件。

Host 启动失败、Gateway 身份失败、Helper 未 ready、查询降级、未知提交或 `RECONCILE_REQUIRED` 时不得启动策略 worker；它只应报告不可交易状态并以非零代码退出，交给启动器决定是否停止全部组件。

## 7. `start_external_windows.ps1` 设计

### 7.1 命令行

```powershell
# 默认完整启动：Gateway → Coordinator → 已启用 workers
.\start_external_windows.ps1

# 只启动外置基础设施，不自动拉起策略 worker
.\start_external_windows.ps1 -NoWorkers

# 查看由该启动器记录的状态，不改变进程
.\start_external_windows.ps1 -Status

# 按受控顺序停止本启动器管理的进程
.\start_external_windows.ps1 -Stop
```

可选参数仅包括 `-EnvFile`、`-CoordinatorConfig`、`-NoWorkers`、`-Status`、`-Stop` 和开发期显式 `-PythonExe`。所有相对路径以项目根解析，启动器必须拒绝不在项目根内的 Coordinator 配置与 worker 程序路径。

### 7.2 启动步骤

```text
0. 取得单实例启动锁，拒绝第二个启动器
1. 定位项目根，验证本机 Windows、根 .env 和 coordinator_config.json
2. 若 .venv 缺失，调用 setup_venv.ps1；存在时只验证，不重建
3. 用 project_env.py 解析 .env 并生成 Gateway/QMT 配置
4. 调用 preflight.py --deployment，验证端口、路径、SQLite 与 Helper 三份 health
5. 检查本启动器状态文件，不允许遗留 PID 未确认时直接双开
6. 后台启动 Gateway 子进程，写入单独日志文件
7. 等待 Gateway TCP PING/PONG 身份校验成功；不是仅检查端口已打开
8. 后台启动 coordinator_host.py，等待状态文件和 Coordinator PONG ready
9. 仅当 Coordinator 可交易时，按确定顺序启动 enabled worker
10. 原子写 external_windows_state.json，前台等待并监控全部子进程
```

第 4 步已经要求大 QMT Helper 已启动、身份一致且 ready。因此“单文件启动外置环境”不等于忽略 QMT 端准备；若 Helper 未就绪，启动器必须失败，不应等待无限时间或自行改写 Helper 文件。

启动器应直接以项目 `.venv\Scripts\python.exe -B` 创建 Gateway/Host 子进程，不激活系统 Python，也不依赖当前 PowerShell 的 `PATH`。子进程使用 `Start-Process -WindowStyle Hidden`，日志写入 `.env` 指定日志目录；唯一可见窗口保留为启动器控制台。

### 7.3 单实例、PID 与健康状态

启动器创建一个仅当前运行账户可访问的本机 mutex/lock 文件，并在 `runtime/external_windows_state.json` 原子记录：

```json
{
  "version": 1,
  "launcher_pid": 1234,
  "gateway_pid": 1235,
  "coordinator_pid": 1236,
  "worker_pids": {"alpha": 1237},
  "gateway_endpoint": "127.0.0.1:9550",
  "coordinator_endpoint": "127.0.0.1:9560",
  "started_at": 1784780000.0,
  "gateway_ready": true,
  "coordinator_ready": true
}
```

不得写入账户 ID、Gateway token、策略 token、完整 `.env`、命令行明文凭据或 Coordinator 数据库内容。`-Status` 只读取该文件并执行 loopback 健康探测；PID 存在不等于健康，端口可用也不等于身份正确。

### 7.4 异常与退出策略

| 情形 | 启动器动作 |
|---|---|
| `.venv` 缺失 | 调用 `setup_venv.ps1`；Python 3.12 x64 不可用则失败退出 |
| `.env` / Coordinator 配置非法 | 不创建任何子进程 |
| Helper 未 ready、身份不一致或 Gateway 预检失败 | 不启动 Gateway/Coordinator/worker |
| Gateway 启动后身份探测失败 | 停止 Gateway 子进程，失败退出 |
| Coordinator 未 ready / 已熔断 | 停止 Gateway 子进程，不启动 worker |
| 某个 worker 启动失败 | 停止已启动 worker、Coordinator、Gateway；不留下半运行账户 |
| worker 意外退出 | 默认停止全部组件并标记故障；后续如需重启策略，必须单独引入有界退避与人工告警 |
| Gateway/Coordinator 意外退出 | 立即停止所有 worker，保留 Coordinator/Gateway SQLite 与日志，禁止自动生成新订单 ID 重发 |
| Ctrl+C / `-Stop` | 执行第 8 节定义的优雅停止顺序 |

首个生产版本不实现无限自动重启。交易系统在未知状态下自动反复重启可能遮蔽副作用不确定窗口；应先停止新信号、检查账本和 QMT 回调，再由明确运维动作重启。

## 8. 优雅停止顺序

```text
停止 worker 的新信号与新订单
  → 等待短暂的本地 handler/outbox 提交窗口
  → Coordinator Host 停止接收新策略连接与新命令
  → Coordinator 保留未 ACK 的 outbox 和未知命令，关闭唯一 Gateway 客户端
  → 停止 Gateway
  → 删除 PID 状态文件与启动锁
```

`-Stop` 不得先强制杀死 Gateway，也不得删除 `gateway_state.sqlite3`、Coordinator SQLite、WAL/SHM 或 Helper runtime。若正常停止超时，启动器应留下故障状态文件、打印 PID 和日志路径，再要求人工确认；不要把 `Stop-Process -Force` 当作默认正常停止方式。

为支持跨 PowerShell 会话的优雅停止，Coordinator Host 应创建仅本机控制账户可访问的 Windows Named Pipe 控制通道，接受 `STATUS` 和 `SHUTDOWN` 两个已认证控制命令。`-Stop` 先向该 pipe 请求关闭，再等待 PID 退出；只有用户额外显式 `-Force` 时才允许终止进程。

## 9. 策略进程接入约定

自动启动的 worker 不能导入 `LocalQmtApi.from_env()`，而是从其独立配置读取：

```text
Coordinator endpoint = 127.0.0.1:9560
strategy_id          = alpha
strategy token       = alpha 的独立凭据
```

worker 的协议顺序为：

```text
COORDINATOR_HELLO
  → ORDER_INTENT / CANCEL_INTENT
  → POLL_EVENTS
  → 本地业务库按 coordinator_event_id 幂等提交
  → ACK_EVENT
```

对于只读策略，启动配置可设为 `worker.enabled=true` 但策略权限/额度为零；对于不希望由启动器托管的策略，设置 `worker.enabled=false`，由独立的受控服务启动，但仍只能连接 Coordinator。

## 10. 测试与验收

### 10.1 自动化测试

新增测试至少覆盖：

1. 缺失 `.venv` 时调用 `setup_venv.ps1`，存在时不重建；
2. `.env`、Coordinator JSON、路径逃逸、非回环监听、示例 token 和重复策略 ID 全部失败关闭；
3. 启动器只创建一个 Gateway、一个 Coordinator，且第二实例被 lock 拒绝；
4. 使用模拟 Gateway 验证 Gateway → Coordinator → 两策略 worker 的订单和可靠事件链；
5. Coordinator 未 ready、worker 启动失败、Gateway 崩溃和 Ctrl+C 时子进程按顺序停止；
6. `-Status` 不回显任何 token 或账户 ID；
7. `-Stop` 优先走 Named Pipe 优雅关闭，超时不删除账本；
8. 完整 `validate_project.ps1` 继续通过。

### 10.2 目标机器验收

1. 停止策略 worker 后，填写真实 `.env` 与 Coordinator 配置，先保持 QMT 交易/撤单开关关闭；
2. 启动大 QMT Helper 策略并确认三份 health；
3. 执行 `start_external_windows.ps1 -NoWorkers`，验证 Gateway/Coordinator 状态；
4. 再启动一个只读 worker，验证其只收到 Coordinator 事件，未直连 Gateway；
5. 在模拟账户验证两个策略的额度冲突、重复订单、ACK 丢失、策略重启、Coordinator 重启和对账熔断；
6. 获得审批后才进行小额真实订单、成交和撤单验收。

离线模拟成功不等于大 QMT、券商或实盘环境已经验收。

## 11. 实施顺序与回滚

1. 先实现 Coordinator JSON 严格解析器、`coordinator_config.example.json`、`.gitignore` 和 ACL 检查；
2. 实现 `coordinator_host.py`，补齐状态文件与 Named Pipe 控制；
3. 实现 `start_external_windows.ps1` 的 `-Start/-Status/-Stop/-NoWorkers`，先不托管 worker；
4. 完成 Gateway/Coordinator 模拟链路和停止测试；
5. 最后开启 worker 托管，先从一个只读策略开始；
6. 每阶段单独提交、离线验证、模拟账户验收后再进入下一阶段。

回滚时停止一键启动器管理的 worker、Coordinator 和 Gateway，保留所有状态库与日志；随后可回到当前手工顺序：启动 Helper → `start_gateway.ps1` → 单个受控策略。禁止让旧直连策略和新的 Coordinator 同时连接 Gateway。

## 12. 完成后的正确使用方式

完成本方案后，外置 Windows 日常启动只需要：

```powershell
cd <项目根目录>
.\start_external_windows.ps1
```

但首次部署和变更 Helper 时仍需要单独执行受控步骤：配置 `.env`、运行 `setup_venv.ps1`（首次）、停止 QMT 策略后运行 `generate_helper.ps1 -Deploy -ConfirmStoppedStrategy`、重新加载大 QMT Helper。该边界不能被一键运行脚本绕过。
