# 大 QMT 内置 Python 本机桥接

本目录是单台 Windows 方案的大 QMT 内置 Python 端。它保留成熟版的文件队列、请求幂等、账户身份绑定、helper readiness 和低延迟回调路径，但不在大 QMT 进程内开 TCP 服务、启动线程或读取 `.env`。普通 Windows Python 网关通过 `127.0.0.1` 对外置策略提供 TCP；网关与本 helper 仅通过同一本机 runtime 目录交换原子文件。

## 目录内容

```text
大QMT内置python/
├─ src/
│  ├─ bigqmt_file_queue_helper.py   # 完整源模板，不直接部署
│  └─ bigqmt_loader.py              # 带 SHA-256/身份校验的 loader 模板
├─ tools/generate_helpers.py             # 唯一生成器
├─ tests/                                # 生成器和 helper 运行时回归
├─ SOURCE_BASELINE.json                  # 成熟 helper 源基线
└─ run_tests.ps1
```

helper build ID 固定为 `xuanling_bigqmt_file_queue_helper_20260716_low_latency_v4_identity_guard`，与网关的 `expected_helper_build_id` 一致。

## 唯一配置入口

只编辑项目根目录 `../.env`，本目录没有第二份 `.env.example` 或账户 JSON。生成器调用根目录 `../tools/project_env.py` 的 `load_deployment()`，并严格消费其单账户 `qmt_config`。

与本端相关的根配置为：

- `QMT_LOCAL_ACCOUNT_ENABLED/NAME/ID/TYPE`；
- `QMT_LOCAL_TCP_PORT`，runtime 自动派生为 `QMT_LOCAL_RUNTIME_ROOT\<TCP_PORT>`；
- `QMT_LOCAL_HELPER_INSTALL_ROOT` 和 `QMT_LOCAL_HELPER_OUTPUT_DIR`；
- `QMT_LOCAL_HELPER_ENABLE_TRADING`、`QMT_LOCAL_HELPER_ENABLE_CANCEL_ORDER`、`QMT_LOCAL_HELPER_STRATEGY_NAME`、`QMT_LOCAL_HELPER_DEFAULT_REMARK`。

根 `.env.example` 默认 `ACCOUNT_ENABLED=false`、账号为占位值、TCP 令牌为全零占位，交易和撤单均为 `false`。生产生成会拒绝这些占位值；`--allow-example` 只能用于离线测试，不得部署其产物。

## 配置固化和性能基线

`src/bigqmt_file_queue_helper.py` 是生成源模板。生成器用 AST 定位配置赋值，将账户身份、runtime 路径和根 `.env` 解析结果安全编译为生成 helper 中的 Python 常量。模板和生成文件的配置块都不读取进程环境；模板本身还强制关闭下单/撤单，不能被环境变量解锁。大 QMT 策略无需、也不应手工配置进程环境变量。禁止直接加载 `src/` 模板。

新项目不将性能/协议参数暴露到 `.env`。生成器会精确拒绝任何弱化，固定基线包括：

- command 50ms、query 500ms、command budget 35ms、readiness 100ms；
- 每 tick 最多 8 个 command、1 个 query，交易时查询让路；
- heartbeat 1s、reconcile 30s、maintenance 60s；
- request guard TTL 604800s、文件寿命 86400s、每轮清理 100 个、低优先级安静期 1s；
- run-time timer 开启、quick trade 2、默认委托类型 1101、QMT user order ID 最长 23。

回调只做标准化和单次原子写入，不查询、不休眠、不重试、不联网。`request_id` guard 保证委托进入 processing 后不会重放；账号、类型、helper name、runtime、build 任一不一致都 fail closed。

## 生成、校验和加载

先把根 `.env.example` 复制为 `.env`，填入真实账号，再将 `QMT_LOCAL_ACCOUNT_ENABLED=true`。推荐在项目根执行受控生成、检查与安装：

```powershell
.\generate_helper.ps1 -Deploy -ConfirmStoppedStrategy
```

该脚本要求策略已停止，并使用 staging/backup/rename 完成可回滚目录切换。仅在开发生成器时，才在本目录直接执行：

```powershell
C:\Python312\python.exe .\tools\generate_helpers.py --env-file ..\.env
C:\Python312\python.exe .\tools\generate_helpers.py --env-file ..\.env --check
```

默认 staging 输出由 `QMT_LOCAL_HELPER_OUTPUT_DIR` 决定。生成器为唯一账户输出：

```text
<HELPER_OUTPUT_DIR>\<QMT_LOCAL_ACCOUNT_NAME>\bigqmt_file_queue_helper.py
<HELPER_OUTPUT_DIR>\<QMT_LOCAL_ACCOUNT_NAME>\bigqmt_loader.py
<HELPER_OUTPUT_DIR>\manifest.json
```

只生成 staging 不会改变已运行策略。必须先停止受影响的大 QMT 策略，核对 `--check` 成功，再将整个账户目录以 staging/备份/目录切换方式原子安装到：

```text
<QMT_LOCAL_HELPER_INSTALL_ROOT>\<QMT_LOCAL_ACCOUNT_NAME>
```

该账户的大 QMT 策略只加载：

```text
<QMT_LOCAL_HELPER_INSTALL_ROOT>\<QMT_LOCAL_ACCOUNT_NAME>\bigqmt_loader.py
```

loader 在执行 helper 前校验 SHA-256，执行后再核对 helper name、account ID 和 build ID。每次修改根 `.env` 后，都必须重新执行：生成 → `--check` → 停止策略 → 受控安装 → 重新加载 loader。

## 离线验证

```powershell
.\run_tests.ps1 -PythonExe python
```

验证包含 20 项回归：源模板 SHA-256、ASCII/Python 3.6 生成合同、单账户与占位阻断、固定性能参数、loader 防篡改、身份保护、幂等 guard、回调与撤单兼容性。测试使用根 `.env.example` 且显式开启 `--allow-example --ignore-process-env`，不会产生可部署配置。

## 安全边界

- 不提交根 `.env`、生成 helper/loader/manifest、runtime、SQLite 或日志；
- 生成 helper、loader 和 manifest 含真实 account ID，必须将 staging/安装目录按敏感文件收紧 ACL；
- helper 运行记录可能包含账号和委托关联信息，禁止外发或截图；
- 网络侧只由普通 Python 网关绑定 `127.0.0.1`，内置 helper 不开放端口。
