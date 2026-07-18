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

Gateway 仍然保留，因为它负责首帧令牌/账户身份校验、幂等、单 writer、查询 singleflight、可靠事件投递和 ACK；外置策略不得直接写 Helper 队列。TCP 只允许绑定 `127.0.0.1`，项目不会创建 Windows 入站防火墙规则。当前固定低延迟基线使用 25ms Helper 命令周期、15ms 单周期软预算和每周期最多 4 条命令，并保留全部下单前/后 Guard。

## 从这里开始

最快部署直接阅读 [最简易部署文档](最简易部署文档.md)；需要协议和实现细节再阅读 [项目整体参考](文档/项目整体参考.md)。

1. 把 [.env.example](.env.example) 复制为 `.env`，填写解释器、路径和真实账户，用 `python -c "import secrets; print(secrets.token_hex(32))"` 生成令牌填入 `QMT_LOCAL_AUTH_TOKEN`；开始时保持交易和撤单关闭。
2. 执行离线验证：

   ```powershell
   .\validate_project.ps1 -PythonExe python
   python -B .\tools\benchmark_local_bridge.py --self-test
   ```

   第二条是完全离线的文件队列性能与幂等自检：只使用系统临时目录和 mock `passorder`，不会打开 TCP、连接 QMT 或下单。

3. 停止对应的大 QMT 策略，生成并部署内置文件：

   ```powershell
   .\generate_helper.ps1 -Deploy -ConfirmStoppedStrategy
   ```

4. 在大 QMT 中加载 `<QMT_LOCAL_HELPER_INSTALL_ROOT>\<QMT_LOCAL_ACCOUNT_NAME>\bigqmt_loader.py` 并启动策略。
5. 启动本机网关：

   ```powershell
   .\start_gateway.ps1
   ```

6. 安装或直接引用外置 API，并先运行只读/离线示例：

   ```powershell
   python -m pip install -e ".\外置策略API"
   python .\示例\protocol_demo.py
   python .\示例\query_account.py
   python .\示例\async_order.py --symbol 600000.SH --side BUY --quantity 100 --price 10.23 --client-order-id demo-001
   ```

最后一条默认只打印帧，不连接、不下单。实盘模式还需要 `.env` 中开启交易、重新生成和重载 Helper，并提供示例要求的精确确认短语。

## 交付内容

- [根 `.env.example`](.env.example)：唯一部署配置模板；
- [大 QMT 内置 Python](大QMT内置python/README.md)：Helper、loader、生成器和完整离线回归；
- [本机 Gateway](网关/README.md)：TCP/文件队列网关与订单关联；
- [离线性能工具](tools/benchmark_local_bridge.py)：输出逐笔 JSONL 和 P50/P95/P99，并验证 Guard、响应与异常不重试；
- [外置策略 API](外置策略API/README.md)：稳定 Python API；
- [TCP 协议与示例](文档/TCP协议与外置API.md)：帧、消息、ACK、原始 socket 和 API 代码；
- [示例](示例)：查询、回调、异步下单、异步撤单和离线协议演示。

`.env`、生成 JSON、Helper build、runtime、日志、SQLite 和 Python 缓存都已排除在版本控制外。生产普通 Python 固定要求 Windows CPython 3.12 x64；大 QMT 内置端使用生成的 ASCII/Python 3.6 兼容文件。
