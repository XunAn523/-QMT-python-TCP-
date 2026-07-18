# 一台 Windows 的大 QMT 内置 Python 本机桥接方案

本项目把原 Windows/Linux 三端桥接收敛为一台 Windows 上的单账户方案。大 QMT 内置 Python 只调用 QMT 交易接口并输出回调；普通 Windows CPython 进程负责可靠文件队列、SQLite 和本机 TCP；你的策略完全运行在大 QMT 外部，只调用 `qmt_local_api`。

```text
外置策略（普通 CPython）
        │ 127.0.0.1 TCP，协议 v2
        ▼
本机 Gateway（asyncio + SQLite WAL）
        │ 本机原子 JSON 文件队列
        ▼
大 QMT 内置 Python Helper（QMT 单线程回调）
        │
        ▼
下单、撤单、查询与订单/成交/错误回调
```

Gateway 仍然保留，因为它负责首帧令牌/账户身份校验、幂等、单 writer、查询 singleflight、可靠事件投递和 ACK；外置策略不得直接写 Helper 队列。TCP 只允许绑定 `127.0.0.1`，项目不会创建 Windows 入站防火墙规则。当前固定低延迟基线使用 25ms Helper 命令周期、15ms 单周期软预算和每周期最多 4 条命令，并保留全部下单前/后 Guard。Gateway 使用 Windows 目录通知唤醒有界扫描，通知不可用时自动回退到原 10ms 轮询；Helper 健康、文件 I/O、SQLite 和磁盘队列背压均为固定的 fail-closed 合同。

## 从这里开始

最快部署直接阅读 [最简易部署文档](最简易部署文档.md)；需要协议和实现细节再阅读 [项目整体参考](文档/项目整体参考.md)。

1. 在项目根执行 `.\setup_venv.ps1`，创建或复用项目 `.venv`，并通过 `.pth` 离线暴露 `外置策略API`。无需激活环境，也不需要对本项目执行 `pip install -e`。
2. 把 [.env.example](.env.example) 复制为 `.env`，保持 `QMT_LOCAL_PYTHON_EXE=.venv\Scripts\python.exe`，填写路径和真实账户，用项目虚拟环境生成令牌并填入 `QMT_LOCAL_AUTH_TOKEN`；开始时保持交易和撤单关闭：

   ```powershell
   Copy-Item -LiteralPath '.env.example' -Destination '.env'
   & '.\.venv\Scripts\python.exe' -c "import secrets; print(secrets.token_hex(32))"
   ```

3. 执行离线验证：

   ```powershell
   .\validate_project.ps1
   & '.\.venv\Scripts\python.exe' -B .\tools\benchmark_local_bridge.py --self-test
   ```

   第二条是完全离线的文件队列性能与幂等自检：只使用系统临时目录和 mock `passorder`，不会打开 TCP、连接 QMT 或下单。

4. 停止对应的大 QMT 策略，生成并部署内置文件：

   ```powershell
   .\generate_helper.ps1 -Deploy -ConfirmStoppedStrategy
   ```

5. 在大 QMT 中加载 `<QMT_LOCAL_HELPER_INSTALL_ROOT>\<QMT_LOCAL_ACCOUNT_NAME>\bigqmt_loader.py` 并启动策略。
6. 启动本机网关：

   ```powershell
   .\start_gateway.ps1
   ```

7. 使用项目 `.venv` 运行只读/离线示例；`setup_venv.ps1` 已完成本地 API 暴露：

   ```powershell
   & '.\.venv\Scripts\python.exe' .\示例\protocol_demo.py
   & '.\.venv\Scripts\python.exe' .\示例\query_account.py
   & '.\.venv\Scripts\python.exe' .\示例\async_order.py --symbol 600000.SH --side BUY --quantity 100 --price 10.23 --client-order-id demo-001
   ```

   策略需要第三方依赖时，只使用 `& '.\.venv\Scripts\python.exe' -m pip install <依赖名>` 安装到项目虚拟环境。

最后一条默认只打印帧，不连接、不下单。实盘模式还需要 `.env` 中开启交易、重新生成和重载 Helper，并提供示例要求的精确确认短语。

## 交付内容

- [根 `.env.example`](.env.example)：唯一部署配置模板；
- `setup_venv.ps1`：创建项目 `.venv` 并通过 `.pth` 离线暴露本地 API；
- [大 QMT 内置 Python](大QMT内置python/README.md)：Helper、loader、生成器和完整离线回归；
- [本机 Gateway](网关/README.md)：TCP/文件队列网关与订单关联；
- [离线性能工具](tools/benchmark_local_bridge.py)：输出逐笔 JSONL 和 P50/P95/P99，并验证 SQLite 副作用状态机、Guard、响应与异常不重试；
- [外置策略 API](外置策略API/README.md)：稳定 Python API；
- [TCP 协议与示例](文档/TCP协议与外置API.md)：帧、消息、ACK、原始 socket 和 API 代码；
- [示例](示例)：查询、回调、异步下单、异步撤单和离线协议演示。

`.env`、`.venv`、生成 JSON、Helper build、runtime、日志、SQLite 和 Python 缓存都已排除在版本控制外。生产普通 Python 固定使用由 Windows CPython 3.12 x64 创建的项目 `.venv`；大 QMT 内置端使用生成的 ASCII/Python 3.6 兼容文件。
