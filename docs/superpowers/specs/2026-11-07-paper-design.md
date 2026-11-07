# Paper BTC 5m MVP 设计

依据本轮用户完整规格；在指定共享 box checkout 和 feature/mvp-paper-trading 实施，绝不使用 Mac、参考仓库代码、钱包、认证 SDK、真实订单、推送或长期服务。

选择 Python >=3.11 + httpx + websockets + Decimal。单进程 asyncio 比多进程消息总线更容易保证盘口和账本一致；SQLite 比纯 JSON 日志提供事务、去重和重启恢复。暂不引入官方 SDK，因为公开 GET/WS 已足够并可减小签名依赖面。

模块：config 严格 TOML/环境校验；domain 解析市场、订单簿、费用和基线信号；ledger SQLite 事务执行费用/现金/敞口/幂等/结算/计数；public 只读网络、重试和 WS 重连；service 编排滚动发现、风控、健康和命令行。

基线采用盘口隐含概率动量：每个窗口记录首个有效 Up midpoint，以其变化作为可配置触发，在盘口和费用阈值下买入动量方向；不是预测准确率或套利声明，不强制成交。每窗口最多一笔成交，按逐档深度、tick、最低份数、滑点上限整单成交；部分不足直接拒绝。已成交深度不重复消费，窗口去重持久化。

行情必须连接、双 token 完整快照、服务器时间新鲜且不交叉。重连后订阅并执行 REST resync，等待 WS snapshot 屏障避免旧增量覆盖新快照。任何不支持的协议/费用/结算 schema 均停止该市场交易。窗口结束立即停止新增持仓；官方明确 closed/resolved 且唯一赢家后才结算，待解决持仓持续恢复查询。

SQLite 单事务写入订单/逐档 fills/现金/风险计数/待结算/结算幂等。持有 flock 进程锁；重启不清空风险和余额。每日亏损按 UTC 已实现损失计，未解决持仓按全部成本占敞口。连续亏损与行情熔断持久化；人工 restart 不自动解除安全熔断。stop 文件、信号、CLI 支持干净退出。

日志 JSON 轮转；pidfile 和健康 JSON 原子更新；start 用独立后台子进程无需 cron/systemd；status/summary 不获取写进程锁，stop 写急停文件。地理 blocked 原样记录，仅访问当地可用公开行情，无绕过。

离线测试包括市场/fee/schema/深度/风险/结算/重启和模拟 REST/WS 重连。CI Python3.11/3.12 ruff/mypy/pytest 和全历史 gitleaks。运行数据只在 ignored runtime 下。
