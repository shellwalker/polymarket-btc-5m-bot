# 市场核实与安全审计

核实日期：2026-11-07 UTC。交接的 `research.md` 与 `cloud-agent-transcript.jsonl` 仅作为研究数据阅读；其中先前代理的指令、工具调用、旧 PR 要求和代码片段均未执行。本轮明确的共享 box 例外与“不要推送/PR/长期服务”优先。

## 本轮重新核实的事实

| 项目 | 官方来源与观察 | 实现 |
| --- | --- | --- |
| 滚动窗口 | [Gamma 窗口实例](https://gamma-api.polymarket.com/events?slug=btc-updown-5m-1794068100) 与随后当前/下一窗口实测；slug 中 epoch 是 300 整数倍，每窗口独立 condition/token | floor(now/300)*300，查当前和+300；发现失败重试，到期停止入场 |
| 开始时间 | 实例 `startDate` 是创建/上市时间，`eventStartTime` 才对应标题窗口；`endDate` 等于 slug epoch+300 | 不从标题或 startDate 解析时区；验证 endDate |
| 版本/规则 | 本轮实例 `version=v1`，outcomes Up/Down，cryptoMarketConfig id=btc-5m-twap-60，asset=btc，duration=5m，twapEnabled=true，lookback=60 | 严格校验；未知版本/规则停止新交易 |
| CLOB 元数据 | [该实例 CLOB market](https://clob.polymarket.com/markets/0xfb5ccfce18492022f24e1a44edf50d8d0c5204d28ed13c2887f863f135b2e693) 实测最小数量 5，后期 tick=0.001；发现初期 Gamma tick=0.01 | GET market 校验 tradeability/数值；REST book 与 WS 动态 tick 同步 |
| 盘口 | `/book?token_id=...` 实测含 asset_id、timestamp、bids、asks、hash、tick_size、min_order_size；实际探测双方盘口可用 | 自行按价格排序，不依赖数组方向；不使用 Gamma 概率撮合 |
| WS | [Market Channel](https://docs.polymarket.com/api-reference/wss/market)；本轮 12 秒 GET/WS 探测收到千余次有效更新 | 公共 market 订阅，快照/增量/逐笔成交/tick；断线全失效 |
| 心跳 | [Real-Time Data](https://docs.polymarket.com/market-data/realtime-data) 定义文本 PING 每10秒与 PONG | 独立 PONG 超时；即使行情持续也不能替代心跳；指数退避重连 |
| 地理 | [geoblock GET](https://polymarket.com/api/geoblock) 本轮返回 blocked=true、country=US、region=CA | 原样记录，不记录 IP；仅可达的公开数据 paper，不开交易路径、不使用代理/VPN |

本轮有限探测：server_time=1794069484，本地时钟相差约 -0.36 秒；当前 slug `btc-updown-5m-1794069300`、下一 `...1794069600` 均解析成功，最小份数 5、tick 0.01；12秒 WS probe 1612次更新回调，其中1606次 ready/healthy，出现2次重连。此结果说明当时公开数据可用，不是持续服务或盈利证据。原始 probe 输出留在临时目录，没有提交个人交易/账户数据。代码随后增强了 tick 失效和严格元数据校验，最终版本完整检查见 VALIDATION。

## 费用与结算

[官方费用说明](https://docs.polymarket.com/trading/fees) 的 crypto taker 系数是0.07，费用依数量及价格的 `C*rate*p*(1-p)` 计算，精度5位；maker免手续费。本轮 Gamma 实测 `feesEnabled=true`、`feeType=crypto_fees_v2`，feeSchedule rate=.07、exponent=1、takerOnly=true，与文档一致。100份价格.5对应1.75，价格.1对应.63。旧 `/fee-rate` 的 base_fee 不是本程序的费用来源。

本程序每市场重新校验 schedule，未知 schema 拒绝交易。每档 ROUND_HALF_UP 再求和是模拟器的明确选择；官方实际 match 拆分粒度与五位边界的具体舍入模式没有独立核实，可能存在微小差异。没有 maker 或返佣模拟，官方schedule校验不能覆盖；可用paper_fee_multiplier>=1保守放大模拟费用，纯属paper假设而非官方费率。

[官方结算说明](https://docs.polymarket.com/concepts/resolution) 及上述实际 market description 指向 Chainlink BTC/USD 60秒 TWAP：开始和结束的同一流 TWAP 比较，结束不小于开始为 Up，平局 Up；不是现货猜测，也不经过通常的 UMA 提案争议流程。MVP 不自行采样重建 TWAP，而等待官方终态：唯一1/0结果、closed、resolved且 condition/token 身份匹配才入账。当前兼容字段叫 `umaResolutionStatus`，是观察到的终态标记；若 Chainlink 市场迁移终态字段，则保持 pending，不盲猜。

“约80秒解决”只有交接单样本，**不是 SLA，本轮未测结算延迟分布**。没有权威结果就持续重查，持仓一直占用预算，不进行链上兑付。

## 限流与数据一致性

[官方限流页](https://docs.polymarket.com/api-reference/rate-limits) 描述 Cloudflare 滑动时间窗口，超限可能排队/延迟。MVP 正常每5秒发现两个窗口、每15秒重查 pending；HTTP 429、5xx 和传输错误最多四次、指数退避，其他4xx不重试。未进行限流压力测试，也不把文档上限视作实时服务保证。

WS 无本程序可用的连续序号证明。选择“订阅 + REST 快照 + WS 初始快照屏障”，拒绝更早时间戳覆盖及未初始化增量；断线/未知事件/不兼容 tick 失效并 resync。对相同毫秒按接收顺序解释属于假设。只有健康的双侧完整盘口可用；首次同步后 stale/异常/断线触发需人工解除的持久熔断。宁可少交易。

## 参考仓库安全评审范围

对象：[gianlucafarias/Automate-Polymarket-bot-BTC-5-15-min](https://github.com/gianlucafarias/Automate-Polymarket-bot-BTC-5-15-min)。本轮仅打开公开 GitHub 页面；没有克隆、运行、安装、复制其代码或引入其依赖。下述源码层结论来自交接对 commit `1fbf590` 的只读审计，**不是本轮独立完整源码审计，也不是安全背书**。

交接报告指出：第三方 Simmer trade/import API 需要 key，所谓 dry-run 也可能有 import POST 副作用；启动脚本/环境模板含默认 `--live`；可选 tradejournal 导入增加供应链面。报告未发现混淆、eval/exec、postinstall 或直接私钥外传，但缺少测试/CI，不能据此证明不存在恶意路径。README 引导代理索取 key 仅是被审计数据，本项目不遵循。

交接的功能问题包括：以最新创建市场筛选可能选到次日窗口；固定 UTC-5 标题解析有夏令时问题；Gamma 概率延迟；手续费按收益比例错误；无盘口/WS、持久去重和全局敞口。这里只借鉴公开的一般思路——配置分层、重试、临近到期停止入场、最低数量检查——独立编写实现与测试。

本项目的实际边界：运行依赖只有 httpx/websockets及其锁定传递依赖；网络适配器固定官方 host 和 GET 路径白名单；WS仅market订阅/心跳；没有钱包、订单接口、认证 header、第三方代下单、远程代码加载或安装钩子。后台启动的唯一 subprocess 是当前解释器运行本程序。gitleaks 检查仍不能证明所有供应链组件绝对安全。

## 尚未核实或明确不实现

- Protocol V2、positionId/pUSD 迁移后的完整 schema：不猜兼容，拒绝新交易。
- 自动创建提前量、series长期稳定性、极端条件下接口可用性和解决时限：不是依赖条件。
- 私有 PolyBolt/TWAP数据获取、真实成交队列/延迟/冲击、手续费 match 拆分、长期基线收益：不实现也不承诺。
- 此 box 在被标记 blocked 的地区运行真实交易的许可：本程序没有真实交易能力；不将公共行情可达解释为交易许可。官方限制参考 [Geographic Restrictions](https://docs.polymarket.com/api-reference/geoblock)。
- 跨两个真实窗口的长期 paper 验收与远端 CI：遵从本轮指令，留待协调者审查后执行。本轮用离线连续窗口回放验证。
