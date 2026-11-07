# Paper observability implementation plan

**Goal:** Add forward-only audit, valuation and read-only statistics without changing any trading decision.

**Architecture:** Transactional schema v2 adds nullable event run IDs, equity snapshots and circuit intervals. A pure telemetry module measures quotes/equity; independent background CLOB requests populate valuation-only books. Existing strategy/risk/fee/slippage decisions and timing inputs remain unchanged.

**Constraints:** Existing feature/mvp-paper-trading branch only. No commit/push, runtime access, service actions, credentials, wallet/order signing, proxy/VPN, Mac or schedules. Offline temporary databases only. User authorized autonomous implementation; no design approval gate or new checkout.

- [x] Migration tests first: v1 money/events preserved, legacy run_id null, idempotence, transactional rollback, unknown future rejected before any writes. Implement schema.py and integrate Ledger initialization.
- [x] Audit tests first: immutable signal quote with future fields null; correlated attempt/result/fill ladder, source/local ages, slippage versus order ask and separate signal-to-order move; rejected and deduplicated attempts counted in persistent circuit intervals. Add per-process run context to all event payloads and structured logging.
- [x] Valuation tests first: pending positions from all windows, bid/mid marks from validated isolated CLOB books, stale/missing totals null, complete-snapshot drawdown persisted through restart. Snapshot timer defaults to 15s; network work never awaited by decisions and never trips circuits.
- [x] Statistics tests first: read-only on v1/v2, time/runs filters, ISO aware/epoch parsing, no historical backfill, no writes/migrations; filtered drawdown based only on selected complete snapshots. Health reports latest snapshot with timestamp and cumulative measured drawdown.
- [x] Update Chinese README and env/TOML examples. Run all offline pytest, Ruff format/check, mypy, diff check. Independent final code review and resolve concrete findings.

**Measurement rulings:** Signal attempt means an actual post-existing-30s-throttle signal/order attempt, not a suppressed strategy evaluation. Circuit duration across process downtime is wall time while persisted circuit stayed active; unknown pre-upgrade start/duration remains null. Complete equity uses gross displayed best bid/mid, without hypothetical exit fees/depth/slippage; it is a mark, not executable liquidation value. Global peaks start at first complete recorded snapshot, survive restart, and never bridge missing marks by inventing values; filtered stats calculate selected-sample drawdown separately.

**Verification:** 162 offline tests passed; Ruff format/check, mypy (11 source files), and git diff --check passed. Independent source review found no strategy/risk/fee/decision changes; its interval end-at-since boundary finding was reproduced with a failing test and fixed. Legacy fixture layout was explicitly converted to v1 for migration testing. No runtime or backup database was opened, no service action or commit/push was performed.
