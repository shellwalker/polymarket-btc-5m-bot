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
equity_snapshot_seconds = 15
valuation_stale_seconds = 5
# 独立的可靠性/网络预算，不改变交易阈值
diagnostic_heartbeat_seconds = 15
continuity_gap_seconds = 30
clock_tolerance_seconds = 2
http_response_seconds = 15
http_total_seconds = 60
ws_sync_seconds = 65
ws_send_seconds = 5
cleanup_seconds = 3
slow_operation_seconds = 0.1
slow_diagnostic_throttle_seconds = 60
initial_cash = "1000"
max_trade = "10"
max_exposure = "50"
daily_loss_limit = "20"
max_consecutive_losses = 3
max_entries_per_window = 1
market_auto_recovery = true
market_recovery_snapshots = 3
market_recovery_cooldown = 30
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
| equity_snapshot_seconds | 15 秒 | 仅观测：持久权益快照间隔，有限正数，不改变交易决策 |
| valuation_stale_seconds | 5 秒 | 仅观测：独立估值盘口的服务端/本地双重新鲜度限额，有限正数，不影响交易盘口或熔断 |
| initial_cash | 1000 | 首次建账模拟现金；重启和修改配置不会重置旧账 |
| max_trade | 10 | 单笔成本加费用上限 |
| max_exposure | 50 | 所有待结算持仓成本加费用上限 |
| daily_loss_limit | 20 | UTC 当日已实现亏损总额 + 全部待结算最坏损失 + 新单最坏损失上限；盈利不抵消亏损 |
| max_entries_per_window | 1 | 每窗口成功成交上限，正整数；Up/Down共享，跨重启、结算持久计数；拒绝与事务回滚不占次数 |
| market_auto_recovery | true | 仅允许两种行情熔断自动恢复；false 保留人工恢复模式 |
| market_recovery_snapshots | 3 | 恢复所需连续健康盘口观测次数，正整数；完整双token同步且至少一个服务端时间戳推进才计一次 |
| market_recovery_cooldown | 30 秒 | 从首次健康观测开始的连续健康冷却时间，必须为正；次数和时间须同时满足，并由新的健康观测完成恢复 |
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

策略接口是 `Strategy.signal()`。自带基线记录**本次进程在窗口内首次健康的 Up midpoint**，变动向上买 Up、向下买 Down；必须满足幅度、价差、价格、费用及滑点过滤。窗口中途启动不会假装知道开盘价格；重启重新建立观测锚点。无信号就不成交。每窗口最多 `max_entries_per_window` 笔持久化成交（默认1），绝不为了展示结果强制交易。未成交时，同一窗口/方向的信号与尝试最多每30秒一次，防止持续风控拒绝淹没日志；新方向可独立评估，达到成功成交额度后由服务层及账本事务双重拦截；提高额度不会放宽现金、敞口或亏损限制。

## 运行、状态、日志和损益

支持前台运行和独立后台进程，不依赖 cron/systemd。`btc5m start` 使用 `subprocess.Popen(..., start_new_session=True)`，通过 `setsid` 建立独立会话，脱离终端运行，无需额外使用 cron 或 nohup。以下命令使用默认 `runtime`；如使用自定义配置，所有命令统一写为 `uv run --locked btc5m --config /absolute/path/paper.toml <子命令>`，文件路径也改为该配置的 `runtime_dir`。

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

`summary` 使用只读、一致 SQLite 快照。`cash` 为剩余模拟现金；`realized_pnl` 为已结算损益，`total_fees` 为总手续费；`exposure` 含待结算成本和费用，`pending` 为待结算订单数；`equity_at_cost` 为现金加待结算本金（不含已付费用，**不是市值或收益预测**），`equity_worst_case` 把全部待结算价值记零。提供成交/逐档/结算计数、UTC 日亏损、连续亏损及熔断原因。

## 观测审计与只读统计

每个进程生成独立 UUID `run_id`。新 `events` 行的列和 JSON payload 均附带此值，结构化日志顶层和 `details` 也包含它，包括 HTTP 重试、发现错误、WS 重连/恢复及启动/退出。`health` 显示当前 `run_id`；旧事件的列为 `NULL`，原 JSON 不改写，也不归属于新进程。`run_id` 由程序生成，无需配置。

`signal` 保存信号时刻的 `signal_quote`：best bid/ask/mid、CLOB `source_at`、本地盘口接收应用时间 `received_at`、两种 age（秒）、健康状态和观测时刻，价格为精确 Decimal 字符串。未来的 `order_at/order_quote/order_id/attempt_id/fill_ladder/execution_slippage/signal_to_order_move` 明确为 `null`。后续 `order_attempt`、`order_result` 和 `paper_fill` 通过 `signal_id` 关联；每次实际订单尝试另有 `attempt_id`。订单尝试保存独立的订单时刻盘口，即使拒绝，也保留此快照和独立结果事件；原 `order_rejected` 人类可读日志的去重保持不变。只有成交才有非空 `order_id` 和逐档 `fill_ladder`（price/quantity/fee）。`paper_fill` 与资金/成交在同一事务内提交；进程若在尝试与结果之间崩溃，可能留下无结果的 `order_attempt`，不会回填成虚构成功/拒绝。直接调用账本而无上游信号时，信号信息为 `null`，不伪造信号。

`paper_fill` 的 `execution_slippage = average_price - order_quote.best_ask`，`signal_to_order_move = order_quote.best_ask - signal_quote.best_ask`，均为绝对价格单位，分开记录，不能混为同一种滑点。费用单列，不包含在这两个价格差中。`signal_at` 是既有决策时间，`order_at` 是调用模拟执行时的本地观测时间，`decision_at` 保留原决策时间，仅用于审计，不能授权晚到成交；时间窗口、风控、费用和滑点阈值保持原值，执行校验改用当前时间。它们不代表真实交易所的下单或成交时间。

权益默认每15秒写入 `equity_snapshots`，记录 cash、每笔未结算持仓、bid/mid 市值、两种 equity 及回撤；`health.equity_snapshot` 显示最近已记录快照和累计回撤，`equity_snapshot_age_seconds/overdue` 提示快照年龄/是否超过配置间隔。该值是带时间戳的历史观测，不承诺当前可平仓金额。估值工作在独立后台任务中，只对账本里所有未结算 token（包括旧窗口）读取公开 CLOB `/book`，按身份、tick、深度及服务端/本地时间验证；最多4个并发，每轮后等待 `min(equity_snapshot_seconds/2, valuation_stale_seconds/2)`。网络延迟可能使刷新变慢，决策和快照计时不等待估值请求。估值不读取 Gamma 价格，不更新策略盘口、不消耗其深度、不触发新行情熔断，也不参与任何交易过滤。

市值定义为份数乘显示 best bid 或 `(bid+ask)/2`，不扣假设平仓费用，不假设顶档深度可覆盖全部持仓。无持仓时 equity=cash；任一持仓缺报价、过期、断线、交叉或其他无效时，对应持仓市值及**组合总市值/equity 为 `null`，`complete=false`**，并列出每笔 `mark_status`。可用持仓仍显示各自市值，但不把不完整小计伪装成总值；不以成本、零或 Gamma 陈旧价格替代。结算仍只依赖原有官方证据路径。

回撤只基于启用观测之后的完整持久快照：分别记录 bid/mid 历史峰值、当前回撤、最大回撤（金额和 fraction 比例，0.1 表示10%）。首次完整快照是峰值起点，不把初始现金当作未观察到的历史权益。峰值/最大回撤跨重启延续；不完整快照当前回撤为 `null`，只保留此前已测峰值/最大值。缺失期间和两次快照之间的低点无法证明，采样回撤可能低估真实路径；不重新计算历史持仓价格或回填旧快照。

```bash
# 全部已记录观测，只读，不连接网络、不持锁启动服务、不迁移数据库
uv run --locked btc5m stats
# 左闭右开 [since, until)，ISO必须有Z或显式时区；也接受Unix epoch秒
uv run --locked btc5m stats --since 2026-11-07T10:00:00-08:00 --until 2026-11-07T11:00:00-08:00
uv run --locked btc5m stats --since 1799999999 --until 1800000300 --run-id '替换为实际UUID'
# 只查看没有进程归属的旧事件
uv run --locked btc5m stats --run-id legacy
```

`stats` 输出选定范围/进程的事件计数、run分组、成交滑点样本均值、快照数/完整数、最近快照和 `selected_drawdown`。该回撤只使用选定完整样本；`latest_equity.drawdown` 则是该快照记录时的全局累计值，可能包含范围外峰值，两者含义不同。无可用完整样本时回撤为 `null`。`stats` 和 `summary` 均使用一致只读 SQLite 事务，可读取旧 v1，不进行迁移；不存在的DB报错，不创建新账本。旧事件缺少的新指标保持缺失，不根据现时数据重算。

`circuit_intervals` 记录原因、已知 start/end、持续秒数和实际被熔断拒绝的信号尝试数；每次尝试另写 `circuit_signal_rejected`，不受 `order_rejected` 去重影响。这里“信号尝试”遵循原每30秒节流：被节流抑制的策略评估、因盘口无效提前返回或已达窗口上限的评估不算新订单。持久熔断跨重启保持同一区间、原起始进程和累计次数；各进程首次观察区间的记录保存在 `circuit_runs`。正常重启不会创建新的熔断起点。原因被更高优先级原因替换时关闭旧区间并打开新原因区间；手动/自动重置关闭区间，均不改变现有熔断决策。

升级前已活跃熔断的历史起点无法确定：`started/start_run_id/duration_seconds` 保持 `null`，只记录升级后 `observed_since` 和新尝试计数，不扫描旧事件推测起点/次数。已知区间的持续时间是持久熔断墙钟时间，包含进程停机时段；仍活跃时显示截至 `duration_as_of` 的时长。统计按区间与时间范围重叠及进程参与筛选；区间完整生命周期的 `rejected_signals/duration_seconds` 与范围内 `selected_rejected_signals` 分列，不把跨范围总数伪装成局部计数。

写入端识别 v1 后，将表结构以单个事务幂等升级至 **schema_version=2**：新增可空 `events.run_id`、索引和权益/熔断观测表，保留所有旧金额、订单、成交、结算和事件内容；DDL失败则回滚整个升级。未知未来版本在任何DDL/写入及WAL模式修改之前拒绝。只读命令不会升级；v2账本需要支持v2的写入程序，不能由旧v1程序继续写入。迁移仅由后续获准运行的新写入进程执行，文档中的统计命令不会重启当前服务。

## 最小可靠性诊断与请求期限

历史 **2026-11-07 10:57–11:21 PST 约24分钟记录空白的根因仍为 UNKNOWN**。事件循环/同步I/O阻塞、进程调度停顿、主机/VM暂停或checkpoint/restore、墙钟跳变，不能仅靠同一run_id和墙钟日志区分。以下诊断只记录新观测，不回填历史报价、窗口成交或过去心跳。

独立诊断任务默认每15秒记录 `heartbeat`，启动请求等待时也运行；主循环同时更新自身进度。诊断使用独立注入的系统时钟，不借用 `Strategy` 的决策时间：wall、monotonic、boottime、process_cpu（秒），以及 boot_id、PID、SID、原始 `/proc/self/stat` 的 `start_ticks`、`SC_CLK_TCK`、`/proc/stat` 的 btime。原始值不换算成ps启动日期；不可读取的字段为null并列出 `unavailable`，不读取任何进程environ。`workers` 为 main、diagnostics、discovery、resolution、valuation、stream 的内存进度：sequence、phase及四种时钟的最近进度时间。sequence表示阶段推进，不等于请求成功数。

超过 `continuity_gap_seconds`（默认30秒）的心跳间隔、时钟倒退、wall/boottime相对monotonic偏差超过 `clock_tolerance_seconds`（默认2秒），或采样时原始身份变化，会写中性的 `continuity_gap`。事件包含前后完整样本、deltas、两个时钟差分、identity_changed、recovered_at和跨越的完整墙钟窗口数量/slug（列表最多100个，另标记截断）。这些窗口表示心跳间跨过的时间，不证明存在可交易信号；`cause` 始终为unknown。新run的首样本不与旧run比较，也不从DB恢复同进程连续性。

`health.reliability` 显示最近心跳、当前worker进度、stream generation、cleanup_failed、owned_pending_tasks及慢操作统计；`stats` 的事件计数可查询 heartbeat/continuity_gap。owned_pending_tasks包括正常在途操作，不等同于失败数。

| 可靠性参数 | 默认（秒） | 作用 |
| --- | --- | --- |
| diagnostic_heartbeat_seconds | 15 | 独立诊断心跳间隔 |
| continuity_gap_seconds / clock_tolerance_seconds | 30 / 2 | 连续性缺口与时钟差分诊断阈值；现有时钟偏差限额也用于安全屏障 |
| http_response_seconds | 15 | 一次完整HTTP响应预算，包括持续分块读取的body |
| http_total_seconds | 60 | 最多4次请求及退避共享总预算，单次取剩余预算与响应预算较小值 |
| ws_sync_seconds | 65 | 分别用于REST重同步、订阅成功后的WS初始双token快照同步；两段各有独立总预算 |
| ws_send_seconds | 5 | 订阅消息及每次PING发送预算 |
| cleanup_seconds | 3 | 每次取消后确认任务结束的等待预算，另计于操作预算 |
| slow_operation_seconds | 0.1 | DB事务/汇总、日志emit/轮转和health写入的慢操作阈值 |
| slow_diagnostic_throttle_seconds | 60 | 同类slow_operation诊断日志的最短间隔 |

以上参数须为有限正数，仅影响可靠性、连续性安全和观测，不改变策略/风险阈值。原有HTTP阶段超时、重试类别/最多次数/指数退避、WS心跳与PONG阈值保持原值。HTTP 404不重试；慢速分块即使每次读取都未超过阶段超时，也不能无限延长完整响应预算。超时记录 `operation_deadline`；预算包含异步等待，不能抢占同步JSON解析/CPU或被阻塞的事件循环；解码后仍检查整体期限。库握手与关闭期限继续生效。离线测试通过HTTPX/httpcore的实际HTTP/1.1解析链注入缓慢chunked字节流，不用一次性假响应代替body读取；该测试不代表真实TCP网络验证。

初始WS快照同步与周期PONG存活检查是独立条件。订阅发送成功后即启动固定monotonic期限；PONG、持续消息、单个token快照都不能续期，必须在期限内收到全部token的有效初始盘口。接收轮询及初始阶段PING发送取剩余同步预算，解析后也重新检查期限，超时不发布ready状态。失败先使盘口无效、确认旧接收任务清理并退出旧连接上下文，再按既有退避重连；拒绝取消时保持隔离，不建立替代订阅。

HTTP/WS超时诊断使用实测monotonic `elapsed_seconds`，受监督操作还分列 `operation_elapsed_seconds`（请求取消前）和 `cleanup_elapsed_seconds`（取消确认）。`cleanup_incomplete` 也记录实际取消等待耗时。`health.reliability.operation_durations` 与 `cleanup_durations` 按固定操作名保存内存中的count、last/max秒数和最后结果；前者另含最近操作/清理拆分及timing_scope，成功操作不逐请求写日志。覆盖 http.complete_response、http.total、ws.rest_sync、ws.subscribe、ws.ping、ws.initial_sync及各取消清理标签；不记录每个普通recv的耗时，也不声称单独测量库内部握手/close。`http.total`包含全部尝试、退避及其清理，拆分字段为null（不把并发请求的清理时间混加），每次尝试的清理在http.complete_response及cleanup_durations下独立测量。初始快照同步成功的计时止于最后所需快照；失败时包括本阶段接收任务取消确认，排除前序REST/订阅和之后连接上下文关闭，其他阶段各自记录。

这些预算是各阶段的异步工作期限，**不是整条连接生命周期统一的一次总预算**。在事件循环可运行、同步处理/诊断不阻塞的前提下，受监督调用的可观察返回时间还包含最多cleanup_seconds的取消确认及调度开销。HTTP整体重试预算计入此前清理，最后一次仍可能额外消耗取消确认宽限；初始WS同步若恰在PING发送中到期，可能先等待PING取消确认，再等待接收任务取消确认，之后还有库关闭流程。嵌套阶段的宽限不可笼统当成“一次3秒”。所有日志给出实际耗时，不能从配置预算反推真实阻塞时间；整机暂停或同步阻塞时，期限和取消确认均无法按时执行。

取消使用 `asyncio.wait` 有界确认，而不把 `wait_for` 的取消请求误当作任务已经退出。若任务拒绝取消，会记录 `cleanup_incomplete` 与 `reliability_halted`，封住旧流代际、断开其本地盘口、阻止晚到回调和替代订阅；同窗口元数据更新也不能绕过隔离。进程不自动恢复交易。正常协作清理完成后才允许换流。关闭过程中若仍存在未完成任务，会保持运行锁和数据库，继续显示隔离状态，直到任务真实结束；重复取消关闭协程或隔离状态下health诊断写入失败，也不能提前释放锁。这可能导致退出等待无上限，**3秒仅是确认取消失败的预算，不是承诺强杀协程或完成进程退出**。同一Python进程无法安全强制结束非协作协程；不自动创建第二个实例来绕过隔离。清理不调用reset-circuit、不改变consecutive_losses或其他持久风险锁存。

`slow_operation` 使用monotonic耗时，只对超过阈值的操作累计count/max/last，按操作名限频；全局诊断重入保护防止“慢日志又记录慢日志”的递归。慢操作日志不另写DB，以免诊断放大数据库阻塞；诊断sink异常不覆盖原操作异常。DB计时标签涵盖对应事务/账本操作，不声称可分离SQL、fsync与计算时间。所有同步阻塞只能在返回后记录，无法靠同线程超时抢占。

**同主机/同事件循环的监测，在整台机器暂停期间不能运行或即时报警**；只能恢复后留下差分证据。宿主侧独立观测或平台暂停/恢复审计仍是区分VM暂停与进程卡顿的重要证据；本程序不配置外部监控、cron、routine、自动重启或新的交易熔断。

```bash
# 以下是操作者只读查看示例，不会重启服务或迁移账本
uv run --locked btc5m health
uv run --locked btc5m stats --since 2026-11-07T10:00:00-08:00 --run-id '替换为实际UUID'
```

## 连续性epoch安全屏障

`heartbeat/continuity_gap` 是诊断记录；交易保护另有一个进程内共享 `ContinuityBarrier`，由命令行服务固定传给WS和账本入口。它不等待诊断轮询：交易入口、WS每次等待前后/帧处理前后、REST快照返回后及成交事务都同步检查。首次启动处于blocked状态；新run重新建立自己的时钟基线和全量盘口，不从旧health/事件恢复开放状态。

屏障比较wall/monotonic/boottime的相邻差分和自当前epoch基线以来的累计时钟偏差。任意可观测倒退、无检查间隔超过 `min(stale_seconds, continuity_gap_seconds)`，或wall/boottime相对monotonic累计偏差超过原有 `clock_tolerance_seconds`，都会递增epoch并封锁入场。默认无检查上限是原有盘口新鲜度5秒，偏差限额2秒；30秒仍是中性心跳缺口诊断限额，所有既有配置值不变。屏障是数据连续性/执行安全检查，不改变动量信号、策略锚点、节流、费用、滑点或风险限额。

检测到异常后，先同步废弃旧订阅代际、断开并清空旧深度及估值缓存，再异步清理。屏障回调不写DB、不运行策略，避免在已有成交事务里意外提交；`continuity_barrier` 和 `continuity_resynced` 事件稍后由诊断路径持久化，原因仍标记unknown。带新时间戳的delta不能使旧asks恢复可用。每次同步另有 `sync_sequence` 和订阅租约；盘口同时标记epoch/同步序号，即便epoch未变，新订阅也不能使用上一订阅的快照。

确认旧任务取消和连接上下文关闭后才允许替代订阅。若旧流已在关闭中，先有界等待其有序退出；关闭未确认或取消失败则沿用进程隔离状态，不重叠订阅。新epoch重新执行公开REST全量同步，并等待两个token各自的新WS初始book快照，所有盘口新鲜、身份与epoch/序号匹配后才开放屏障。单token快照、PONG、旧回调、跨epoch返回的REST/WS响应均不能开放。窗口已结束则等待发现当前市场，不补跑过去窗口或伪造历史报价/成交。屏障开放只恢复数据准入；`consecutive_losses` 等人工亏损锁存保持原值，仍会独立拒绝交易，清理和恢复不调用reset-circuit。

账本 `execute()` 默认读取当前wall clock；服务注入当前clock和捕获的epoch/代际校验，离线回放需显式注入固定clock。`BEGIN IMMEDIATE` 等锁返回后、计算/风控之后、写入前以及全部SQL写入（含paper_fill审计INSERT）之后、COMMIT之前重查当前窗口、当前健康盘口、epoch/同步序号、代际和风险。SQL写入后检查风险时不重复预留已经插入的自身敞口。检查失败会回滚订单、逐档成交、现金和成交额度，拒绝审计仍可记录；观察与官方结算可继续。

审计继续保留 signal_at、decision_at、signal_quote。order_at/order_quote是账本入口实际观测，传入的订单观测另存requested_order_at；transaction_at是获得事务锁后的采样，execution_at/execution_quote是写入前采样，commit_check_at是可持久化的审计INSERT前采样，均不声称是真实COMMIT完成时刻。审计INSERT后还有不再追加SQL写入的最终保护检查；因此其精确时刻不伪装成已经提前写入的字段。新字段只用于新记录，无schema迁移、历史重算或旧金额/费用重写。

`health.reliability.continuity` 显示epoch、blocked、state、sync_sequence、所需token和最近异常；`continuity_recovery_needed` 显示是否等待替代同步，`generation/cleanup_failed` 继续显示代际/取消安全状态。`trading_ready` 还要求屏障开放；数据恢复与风险锁存是两种独立状态。

**应用侧已修复的是：可检测的连续性异常不能借新delta洗新旧深度、旧决策时间不能授权等待之后的成交。历史24分钟空白的平台根因仍为UNKNOWN。** 同线程检查不能抢占整机暂停、同步I/O或COMMIT内部阻塞；最后一次检查之后若立即挂起/跳时钟，仍存在不可消除的检查—提交间隙。程序不宣称可原子证明平台连续性，也不会事后编造成交或反写历史账本来掩盖这一限制。

## 紧急停止和恢复

```bash
uv run --locked btc5m stop
# 或直接创建配置 runtime_dir 下的文件：
touch runtime/STOP
# 或对已确认的服务进程发送 SIGTERM / 前台 Ctrl-C
```

STOP 文件和命令不会根据陈旧 pidfile 杀进程。信号/文件在启动网络阻塞时也能退出；停止后取消 WS、落盘状态、关闭数据库并释放锁。停止不伪造结算，也不删除未解决持仓。进程下次运行从数据库继续追踪官方结果；已成交和已结算记录均不重复入账，窗口额度不会因结算或重启恢复。

行情过期/断线（首次成功同步之后）与异常价差仍持久化，但默认允许在行情恢复后自动解除：只有当前窗口全部token已同步、新鲜且价差正常，连续健康观测次数和冷却时间同时达标，才写入 `circuit_reset`（`automatic=true`、原原因、健康次数和秒数）。周期轮询、同一盘口时间戳、成交消息不增加次数；任一不健康观测、窗口切换或重启均清空恢复进度，重启本身不会解除熔断，跨窗口也要重新证明健康。可设置 `market_auto_recovery=false` 禁用。

日亏损、连续亏损、市场身份变化及未知原因始终只允许人工恢复；发生时会优先覆盖已有行情熔断，防止被自动恢复误清除。自动恢复不重置连续亏损计数、日亏损、现金、持仓或成交额度。风控预算拒绝本身不消耗成交额度。

**Box 重启后不会自动启动服务**；后台进程也不会跨机器重启存活。本项目不配置 cron、routine 或开机自启。以下是操作者手动恢复/启动步骤：

```bash
cd /workspace/repos/polymarket-btc-5m-bot
uv run --locked btc5m status
uv run --locked btc5m summary
# 若仍在运行，先停止，再重复 status，直到 running=false：
uv run --locked btc5m stop
uv run --locked btc5m status
```

检查故障和风险后，仅在服务已停止且确需人工清除熔断时执行（行情自动恢复无需本步骤）：

```bash
uv run --locked btc5m reset-circuit
```

确认可以恢复paper运行后，手工清除STOP并显式后台启动；无需清除熔断时直接从这里继续：

```bash
rm -f runtime/STOP
uv run --locked btc5m start
uv run --locked btc5m status
uv run --locked btc5m health
# 查看后台启动错误及paper日志
tail -n 50 runtime/launcher.log
tail -n 50 runtime/logs/paper.jsonl
# 停止后台进程，再确认已经退出
uv run --locked btc5m stop
uv run --locked btc5m status
```

`reset-circuit` 有审计事件并重置亏损连续计数；不改变现金、日亏损、未结算敞口和订单去重。日亏损风控仍然有效。不要删 DB 来“修复”风控；应停机备份整个运行目录。账本保留既有 orders/fills/settlements 记录，`order_windows` 映射将旧订单计入原窗口额度；额外成交使用独立订单ID（窗口slug加序号），每笔单独结算，`pending`按未结算订单计数。旧记录的金额和审计事件不重写；schema 迁移规则见下文。

## 撮合、结算和限制

- 仅模拟买入 taker，按 ask 从低到高逐档成交。每档遵守 tick/滑点/限价，整单数量不足则拒绝；扣减深度与持久窗口额度共同限制重复使用（提高额度允许后续新快照提供新显示深度）。不是 maker 队列或部分成交模拟。
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

### 重同步失败与时钟采样诊断

`ws_resync_incomplete` 在清空盘口之前输出，每次失败的完整双 token dump 最多一条；
原错误文本 `current-epoch dual-token resync incomplete`、完整双 token gate、手动亏损锁存、
交易参数和重连退避上限 30 秒均保持不变。成功请求不会因此增加日志。
结构化 `details` 上限 8192 UTF-8 字节，最多检查两个预期 token；不记录原始帧。
`expected_tokens/actual_tokens/missing_tokens/unexpected_tokens` 配合数量及截断标志使用，
token/asset ID 最多 128 字符（正常 78 位 ID 完整保留），slug 文本最多 64 字符；
仅超长 ID 设置 token_text_truncated 及逐 token 的 token_truncated/asset_id_truncated。`market` 包含 slug、start、end、remaining_seconds。
`epochs`、`lease`（匹配/是否存在及 current_sync_sequence）、stream_epoch、
stream_sync_sequence 和每 token 的 epoch/sync_sequence 都是数值或布尔身份，不输出对象地址。

`reason_codes` 汇总所有检查出的失败条件：epoch_mismatch、lease_mismatch、tokens_mismatch、
no_books、missing_token、book_epoch_mismatch、book_sync_sequence_mismatch、disconnected、
invalid_depth、empty_bids/empty_asks、missing_best_bid/missing_best_ask、crossed_or_locked、
received_time_future/received_stale、source_time_future/source_stale、
received_age_nonfinite/source_age_nonfinite；价格读取异常为 best_prices_unavailable。
每 token 同时记录 bid_levels/ask_levels、best_bid/best_ask、tick、connected、valid_depth、
source_at/received_at 和 source_age_seconds/received_age_seconds；缺失或非有限年龄为 null。
诊断仅解释失败，不授权开 gate。若检查期间 epoch 改变，附 pre_poll_tokens 和 changed_during_poll；
极端长度下 diagnostics_truncated 表示可选细节已缩减。

时钟采样以 wall→monotonic→boottime→process_cpu 读取，再反序读取形成括号。
原 clocks.wall/monotonic/boottime/process_cpu 保留第一轮原始值；sampling_bracket、
sampling_spans、sample_consistent、sample_reason_codes 标注采样跨度、倒退及跨钟差异。
使用已有 clock_tolerance_seconds 判断，不新增交易配置。
混合采样仍失效关闭；后续稳定采样若再次满足异常条件仍递增 epoch，不作不安全去重。
这不是原子采样，容差以内的中断仍可能不可辨认。

sample_wall 是第一轮 wall；sample_completed_at 是括号末尾 wall；detected_at 是判定后新读取的
wall；emitted_at 是输出时新读取的 wall。新异常账本事件 created 使用 detected_at，
不使用可能过时的 sample_wall；旧 recovered_at 仍保留原 sampled wall 语义，旧事件完全不改写。
旧版注入时钟没有括号字段时仍兼容。heartbeat/gap 的 changed_fields（事件顶层）记录原始身份字段 old/new，btime_delta 单列；process_identity_changed
只表示 PID/SID/start_ticks 原始字段变化，不把 btime 漂移当成进程重启证明。
identity.uptime.seconds 来自采样 CLOCK_BOOTTIME，source 明确标识；不可用时回退到
CLOCK_MONOTONIC（不含 suspend），并带 sample_wall/sample_consistent。
这不是 wall-btime 推算的进程启动日期，也不是另行读取的 /proc/uptime。

这些字段用于区分观察事实与推测，cause 仍为 unknown；历史约 24 分钟间隙的平台根因仍未知。
同机诊断无法在整机暂停期间运行；检测和截止执行要求事件循环可运行，最终安全检查之后
发生暂停的不可避免窗口依然存在。
