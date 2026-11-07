# 首稿验收记录

日期：2026-11-07。开发位置：`/workspace/repos/polymarket-btc-5m-bot`，分支 `feature/mvp-paper-trading`，基于初始提交 `8a0029f`。本轮只在授权共享 box 操作，没有使用 Mac、参考仓库代码或任何账户凭证。未推送、未开 PR、未合并、未强推、未启动长期服务。

## 实际完成的检查

Python 3.11.17 与 Python 3.12.15 使用同一 `uv.lock` 安装。两者均完成：

```bash
uv sync --locked --python 3.11 --group dev
uv run --locked ruff format --check .
uv run --locked ruff check .
uv run --locked mypy src
uv run --locked pytest -q
```

3.12 使用独立临时虚拟环境后运行同样检查，以免改动 3.11 环境。结果：格式通过、lint通过、strict mypy 8个源文件通过；**两个版本各78项测试通过**。测试不依赖外部网络。CI 矩阵已配置；没有推送，故没有远端 Actions 运行结果。

本地 gitleaks v8.24.3（固定版本装入 `/tmp`）执行：

```bash
gitleaks git --redact --log-opts='--all' --exit-code=1 .
gitleaks dir --redact --exit-code=1 .
git diff --check
```

扫描本地所有 refs 的 Git 历史及工作树，没有发现密钥；无需任何 allowlist 或忽略规则压制告警。runtime/DB/日志/实际.env及密钥文件扩展名由 `.gitignore` 排除版本管理。扫描不是供应链安全的绝对保证。

## 测试覆盖

| 文件 | 核心行为 |
| --- | --- |
| test_config.py | 严格数值/未知键/环境覆盖；live=true 禁止，文件中的 true 不能被环境 false 掩盖 |
| test_domain.py | 300秒 slug、二元身份、crypto v2 fee/TWAP/tradeability schema、五位费用、快照/增量/旧/未来/交叉/stale/tick、真实变动才有信号、逐笔不刷新盘口新鲜度 |
| test_ledger.py | 多档深度/滑点/最低数量/tick、费用计入现金和风险、敞口和最坏日亏损预留、连续亏损/熔断持久化、事务回滚不耗深度、重启无重复、官方结果严格校验/幂等、只读一致 summary |
| test_public.py | GET 白名单/重试、WS 重订阅+REST resync、初始屏障、断线重连、PONG 超时、CLOB 非法约束与 tick 变大失效回归 |
| test_service.py | 进程排他锁、陈旧pid不代表运行、STOP启动无网络、两个连续窗口切换与官方结算/重启 |
| test_integration.py | MockTransport完整REST适配器+WS模拟流+两个窗口；成交后断线、重新订阅/REST重同步、持久熔断、官方结算、重启现金/计数不重复 |
| test_cli.py | live拒绝、status/summary/stop、STOP清理；真实子进程SIGTERM与文件停止中断启动网络等待；离线后台start/status/重复start拒绝/stop释放锁 |

后台生命周期测试只启动临时目录内受控的短命子进程，测试钩子将网络方法替换为等待；结束时释放/回收进程。没有启动连接真实市场的长期服务。

## 公开只读验证

通过官方 GET 重新核实 geoblock、服务端时间、Gamma 市场、费用/规则 schema、CLOB 最小数量/tick和订单簿。独立12秒 WS probe 成功发现当时当前及下一窗口，收到1612次回调、1606次完整可用盘口更新，发生2次重连。该探测没有调用 Service 或 Ledger，没有模拟/真实成交、授权、签名或钱包。

本机当时 geoblock 为 blocked=true，country=US，region=CA，公共数据可达。未使用代理/VPN绕过。详情、官方来源和未核实项见 [MARKET_RESEARCH.md](MARKET_RESEARCH.md)。

## 代码审查与修正

独立代理只读审查发现两个重要问题：CLOB 覆盖后的 tick/min_size 缺少严格校验；tick 变大时 invalidation 可能被流处理器恢复。均先写失败回归用例，再修正，并加入 health/execute 双重 tick 防护。后续补充了真实信号退出和离线后台生命周期、最新快照最低数量约束、只读 summary 和 live 配置不可被掩盖。最终整体复审未发现新的重要正确性/安全问题。

## 仍需协调者审查后的工作

- 本轮明确禁止持续运行，因此没有真实跨两个5分钟窗口的 paper PnL或长期运行证据；现有连续窗口证据来自离线回放。允许长期运行后，应观察切换、熔断、pending解决延迟及资源增长；零成交也是合法结果。
- 远端CI尚未运行；按要求没有push/PR。基线没有盈利验证。
- 只支持已核实的v1 token、crypto_fees_v2与BTC60秒TWAP规则。Protocol V2、新费用/结算schema需单独适配；当前行为是停新交易或保留pending。
- WS无连续序号，同毫秒顺序、显示深度可成交性、逐档费用聚合/HALF_UP等模拟假设见README；不是实盘执行质量证明。
- SQLite需运营者定期停机备份/归档。进程锁和信号依赖Linux/POSIX，不承诺Windows支持。

## 协调者反馈修正验证

在初稿独立验收后修正两个验收问题及相关运行行为：

- `Service.run` 保留信号使用的停止事件，`update` 在任何信号、成交或熔断写入之前检查该事件及STOP文件。健康/过期/异常三种盘口回调回归测试确认 stop.set 后 SQLite total_changes 不增加。
- 新增 `paper_fee_multiplier`，默认1、有限且>=1；官方逐市场0.07 schema校验保持严格，策略与每档撮合统一采用0.07乘模拟倍数。健康、启动日志/事件和成交事件记录该假设，不追改已有成交。
- 相同窗口/方向的未成交信号尝试最多每30秒一次；持续风控拒绝或持久熔断不再逐帧写信号。
- 健康预检逐档计算最小交易成本与假设费用，显示 `risk_blocked_reasons`；现金、日亏损预留、总敞口或单笔额度不足时不宣称 trading_ready。
- README改为持续适用的操作文档，历史交付状态保留在本验收记录。

新增20项回归测试；Python3.11/3.12各98项测试通过，ruff格式/lint、strict mypy通过，gitleaks工作树扫描无泄漏；修正经过独立只读复审，无新的重要问题。没有执行公开网络探测、推送、PR或长期服务。
