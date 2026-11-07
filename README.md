# Polymarket BTC 5 分钟模拟交易 MVP

完整的只读行情模拟程序：每个 300 秒窗口自动发现独立市场，CLOB WebSocket 维护盘口，Decimal 逐档模拟 taker 买入，SQLite 持久化现金、费用、风控和官方结算结果。**唯一模式是 paper；没有真实下单、钱包、签名、授权或认证 SDK。`live=true` 直接拒绝启动。没有收益保证。**


## 安装与检查

支持 Linux、Python 3.11/3.12（使用 POSIX flock 和信号），需要 [uv](https://docs.astral.sh/uv/)。选择 Python + httpx/websockets，而不引入含签名路径的交易 SDK；金融计算只用 Decimal。依赖与哈希锁定在 `uv.lock`。

```bash
uv sync --locked --python 3.11 --group dev
uv run --locked ruff format --check .
uv run --locked ruff check .
uv run --locked mypy src
uv run --locked pytest -q
```

所有测试离线运行，REST 使用 MockTransport，WS 使用模拟流。包括两个连续窗口、真实适配器、断线重订阅/REST 重同步、官方结算、事务回滚、重启去重及进程信号退出。GitHub Actions 对 Python 3.11/3.12 执行相同检查，并用 gitleaks 扫描完整 Git 历史和工作树。具体版本的本地验证和远端 CI 状态应以相应验收记录及 Actions 结果为准。

## 配置

默认无需配置。优先级：环境 `BTC5M_*` > 指定 TOML 文件 > 内置默认值。未知键、非有限数、无效类型和任何来源的 live=true 均失败（不能被另一来源的 false 掩盖）。`.env.example` 仅是模板，程序不会自动读取 `.env`；可在 shell 中 `export BTC5M_MAX_TRADE=8`。不需要任何 API key。

自定义配置示例（保存为本地 TOML，文件路径由 `--config` 指定）：

```toml
live = false
runtime_dir = "runtime"
initial_cash = "1000"
max_trade = "10"
max_exposure = "50"
daily_loss_limit = "20"
max_consecutive_losses = 3
shares = "5"
paper_fee_multiplier = "1"
signal_threshold = "0.03"
min_momentum = "0.03"
max_entry_price = "0.85"
max_spread = "0.05"
max_slippage = "0.02"
stale_seconds = 5
min_time_remaining = 30
```

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| initial_cash | 1000 | 首次建账模拟现金；重启和修改配置不会重置旧账 |
| max_trade | 10 | 单笔成本加费用上限 |
| max_exposure | 50 | 所有待结算持仓成本加费用上限 |
| daily_loss_limit | 20 | UTC 当日已实现亏损总额 + 全部待结算最坏损失 + 新单最坏损失上限；盈利不抵消亏损 |
| max_consecutive_losses | 3 | 连续已结算亏损触发持久熔断 |
| paper_fee_multiplier | 1 | 纯模拟费用保守倍数，有限 Decimal 且必须 >=1；模拟系数为已核实0.07乘此值，不改变官方schema校验 |
| shares | 5 | 每次信号目标份数；必须满足该市场最小数量 |
| signal_threshold / min_momentum | 0.03 / 0.03 | Up midpoint 相对本窗口首次健康观测的变动，须同时超过两者 |
| max_entry_price | 0.85 | 买入价格上限 |
| max_spread | 0.05 | 买卖价差异常阈值（绝对价格单位） |
| max_slippage | 0.02 | 相对最优 ask 的最大逐档价差；不是百分比 |
| stale_seconds | 5 | 本地接收和服务端盘口时间的双重新鲜度限额 |
| min_time_remaining | 30 秒 | 窗口结束前停止开仓 |
| discovery_seconds / resolution_seconds | 5 / 15 秒 | 当前/下一窗口发现与待结算重查间隔 |
| http_timeout | 10 秒 | 每个公开 GET 请求超时；429、5xx、传输错误最多四次请求 |
| retry_initial / retry_max | 1 / 30 秒 | 指数退避；WS 稳定 30 秒后恢复初始退避 |
| heartbeat_seconds / pong_timeout | 10 / 20 秒 | 文本 PING/PONG；行情流量不能代替 PONG |

策略接口是 `Strategy.signal()`。自带基线记录**本次进程在窗口内首次健康的 Up midpoint**，变动向上买 Up、向下买 Down；必须满足幅度、价差、价格、费用及滑点过滤。窗口中途启动不会假装知道开盘价格；重启重新建立观测锚点。无信号就不成交。每窗口最多一笔持久化成交，绝不为了展示结果强制交易。未成交时，同一窗口/方向的信号与尝试最多每30秒一次，防止持续风控拒绝淹没日志；新方向可独立评估，成功成交后仍由账本去重。

## 运行、状态、日志和损益

支持前台运行和独立后台进程，不依赖 cron/systemd。

```bash
# 前台，Ctrl-C/SIGTERM 干净退出
uv run --locked btc5m run
# 自定义配置（--config 放在子命令之前）
uv run --locked btc5m --config /absolute/path/paper.toml run
# 后台脱离终端，使用同一 Python 解释器；只有显式 start 才创建进程
uv run --locked btc5m start
uv run --locked btc5m status
uv run --locked btc5m health
uv run --locked btc5m summary
# 默认结构化日志
tail -f runtime/logs/paper.jsonl
```

`status/health` 以独占锁检测进程，pidfile 只是诊断信息；`trading_ready` 同时要求健康盘口、当前可入场时间、无停止请求/熔断，以及至少一个方向的最小交易通过现金、日亏损预留、总敞口和单笔预算检查。`risk_blocked_reasons` 显示阻止最小交易的原因；这是健康预检，不保证策略指定的更大数量一定可成交，实际成交仍在事务内重新检查。`reconnects` 是本次进程跨市场累计失败次数；`gap_started`/`last_gap_seconds` 和 `ws_recovered` 日志记录同步缺口及恢复时长。相同窗口的 tick 元数据刷新不重建订阅，盘口 tick 由有序 WS/快照更新；condition/token/窗口身份变化会熔断。`health.json` 的 `updated` 可识别进程卡住，`running=true` 仅说明仍持锁，不代表交易可用。地理查询原样记录（此 box 实测 `blocked=true, US/CA`），公共数据可达时允许只读 paper，不绕过限制。HTTP 忽略环境代理、WS 禁用代理。

运行目录固定包含 `ledger.sqlite3`（WAL/FULL 同步）、`process.lock`、`pid.json`、`health.json`、`STOP` 和日志。`paper.jsonl` 单文件 5 MB、保留 5 个备份；`launcher.log` 记录后台启动错误。SQLite 是审计事实来源；`events` 包含 signal、拒绝、成交、结算、熔断及状态事件，`orders/fills/settlements/daily/meta` 保存账本。行情原始帧不全量保存，DB 不自动删除审计记录。

`summary` 使用只读、一致 SQLite 快照。`cash` 为剩余模拟现金；`realized_pnl` 为已结算损益，`total_fees` 为总手续费；`exposure` 含待结算成本和费用，`pending` 为待结算市场数；`equity_at_cost` 为现金加待结算本金（不含已付费用，**不是市值或收益预测**），`equity_worst_case` 把全部待结算价值记零。提供成交/逐档/结算计数、UTC 日亏损、连续亏损及熔断原因。

## 紧急停止和恢复

```bash
uv run --locked btc5m stop
# 或直接创建配置 runtime_dir 下的文件：
touch runtime/STOP
# 或对已确认的服务进程发送 SIGTERM / 前台 Ctrl-C
```

STOP 文件和命令不会根据陈旧 pidfile 杀进程。信号/文件在启动网络阻塞时也能退出；停止后取消 WS、落盘状态、关闭数据库并释放锁。停止不伪造结算，也不删除未解决持仓。进程下次运行从数据库继续追踪官方结果；已成交窗口和已结算窗口均不会重复入账。

行情过期/断线（首次成功同步之后）、异常价差、日亏损和连续亏损熔断持久化；重启不会解除。排查后，必须在服务停止时显式恢复：

```bash
uv run --locked btc5m reset-circuit
# 确认风险后手工删除急停文件（如存在）
rm -f runtime/STOP
uv run --locked btc5m start
```

`reset-circuit` 有审计事件并重置亏损连续计数；不改变现金、日亏损、未结算敞口和订单去重。日亏损风控仍然有效。不要删 DB 来“修复”风控；应停机备份整个运行目录。SQLite 表格式升级目前需人工迁移，未知版本拒绝启动。

## 撮合、结算和限制

- 仅模拟买入 taker，按 ask 从低到高逐档成交。每档遵守 tick/滑点/限价，整单数量不足则拒绝；扣减深度与持久窗口去重共同避免重复使用。不是 maker 队列或部分成交模拟。
- 每个价格档默认手续费 `C × 0.07 × p × (1-p)`；可设置 `paper_fee_multiplier`（例如2）做保守压力测试，策略费用过滤和每档撮合统一使用 `0.07 × paper_fee_multiplier`，四舍五入至 5 位小数后求和；此逐档聚合及 HALF_UP 是明确的模拟假设，不模拟返佣、链上费用和真实 match 拆分。health、启动日志/事件和成交审计事件记录 `verified_fee_rate`、`paper_fee_multiplier`、`paper_fee_rate`；倍数只是paper假设，绝非实时费率声明。重启修改倍数只影响后续信号/新成交，不追改既有费用。
- 每次发现验证该市场 `crypto_fees_v2` / exponent=1 / rate=0.07 / takerOnly=true。仅支持已观察的 v1 token schema、BTC 60s TWAP 规则配置；v2/未知费用和规则均停止新交易。
- 重连时全盘口失效，先做 REST 重同步，再订阅并等待每个 token 的 WS 初始快照建立独立有序基线，避免把已排队的旧增量混入较新的 REST 盘口。完整 price_change 跨 token 原子应用，官方 best_bid/best_ask 边界只删除矛盾旧档位，不创造缺失深度；结果仍交叉或最优价缺档则重同步。后续旧时间戳不覆盖新数据。没有 WS 序号可做绝对缺口证明，同毫秒事件按接收顺序处理；异常时断开并 resync。
- 到窗口结束不再开仓；只接受官方 Gamma `closed=true`、`umaResolutionStatus=resolved` 且 token/condition 精确匹配、结果唯一为 1/0 的结算。该字段名不意味着本程序执行 UMA：当前 BTC 规则使用 Chainlink 60s TWAP，平局 Up。绝不拿现货或时间到期猜赢家。
- 官方字段迁移、结果未就绪、数据源不可达时持仓保持 pending、继续占用风险预算；无结算时限保证。这里只记录模拟 $1/份兑付，不发链上 redeem。
- 显示深度不保证真实可成交；延迟、抢跑、隐藏深度、队列、价格冲击没有完整建模。保守约束也不能证明模拟结果能复制到真实市场。
- 基线没有外部 BTC 现货或私有 TWAP 数据依赖，也不估计真实胜率。高频变化、重启锚点和严格熔断可能导致长期零成交或亏损。
- 日志轮转但 SQLite 长期增长；需运营者停机备份/归档。保持系统 UTC 时钟同步；未来时间戳偏差超过 2 秒会拒绝。
- runtime、数据库、日志、.env、密钥扩展名均忽略。不要在 checkout 添加真实账户资料；程序没有读取它们的路径。

官方核实、假设、参考仓库安全评审范围与本次验证证据见 [MARKET_RESEARCH](docs/MARKET_RESEARCH.md) 和 [VALIDATION](docs/VALIDATION.md)。
