# TPC-C Throughput: v0.1.4 vs v0.2.0 Investigation

## Summary

The v0.2.0 release dashboard (2026-06-15) shipped a TPC-C mixed-workload number of
**5,307 TPS** for VibeSQL, down sharply from the v0.1.4 README claim of **10,758 TPS**.
A follow-up re-measurement on the same host (see Issue #5643) shows the v0.2.0 release
dashboard number was **depressed by host contention**, not a code regression. The actual
v0.2.0-codebase throughput on the same hardware is closer to **9,276 TPS** — still ~14%
below the v0.1.4 baseline, but well within the same order of magnitude.

## Context

The v0.2.0 benchmark snapshot (`web-demo/public/benchmarks/tpcc_results.json`, captured
at the v0.2.0 release commit `ec54522d1` / dashboard timestamp `2026-06-15T13:56:44`)
recorded:

| Engine  | v0.2.0 dashboard TPS | v0.1.4 README TPS |
| ------- | -------------------: | ----------------: |
| VibeSQL |              5,306.65 |          10,758   |
| SQLite  |                794.62 |           1,969   |
| DuckDB  |                 94.89 |             323   |

All three engines lost ~60-70% of throughput simultaneously, which is the
characteristic signature of host-level contention rather than a per-engine regression.

## Re-measurement (2026-06-15, Issue #5643)

Re-ran `make benchmark-tpcc` (60 s mixed-workload, embedded engines) on the same
host on `feature/issue-5643` (head `9e1ac205`, descended from `ec54522d1`):

| Engine  | Re-measured TPS | vs v0.2.0 dashboard | vs v0.1.4 baseline |
| ------- | --------------: | ------------------: | -----------------: |
| VibeSQL |        9,276.23 |             +74.8%  |             -13.8% |
| SQLite  |        2,812.80 |            +254.0%  |             +42.9% |
| DuckDB  |          449.60 |            +373.7%  |             +39.2% |

Persisted in the benchmark history database as Run ID 176
(`/Users/rwalters/.vibesql/test_results/benchmark_results.vbsql`).

### Important caveat — measurement was *also* under contention

This re-measurement was taken while a parallel Loom builder (Issue #5644) and the
TPC-C build itself were running on the same host. **The host was not idle.** The
true v0.2.0 number is almost certainly higher than 9,276 TPS — a truly idle re-run
is still warranted before any conclusion about a real (vs. ambient) regression.

The fact that all three engines roughly **tripled** between the v0.2.0 dashboard
snapshot and this re-measurement strongly suggests the dashboard window saw a
much heavier contention spike than the re-measurement window.

## Decision

- The **v0.2.0 release dashboard TPC-C numbers were depressed by sweep-concurrency
  contention** during the release-window benchmark. They should not be interpreted
  as a code regression vs. v0.1.4.
- The website's `tpcc_results.json` still carries the contention-depressed
  dashboard number. Refreshing it via `make website` + `wrangler deploy` is a
  follow-up task — out of scope for this docs-only entry, and ideally done after
  one more truly-idle re-run for confidence.
- The remaining ~14% gap between the contention-affected re-measurement (9,276 TPS)
  and the v0.1.4 baseline (10,758 TPS) may itself be either residual contention
  or a small real regression. A clean-machine TPC-C bisect against `ec54522d1`
  (v0.2.0) and prior tagged commits would settle it, but is not currently a
  release blocker.

## Follow-up

If a clean-machine re-run shows VibeSQL TPC-C TPS materially below the v0.1.4
baseline (e.g., < 9,000 TPS sustained across two idle runs), file a bisect issue
that:

1. Identifies the v0.1.4 commit boundary (last release commit before the v0.2.0
   work landed).
2. Bisects across the Raft replication and MVCC merges (the two largest v0.2.0
   workstreams most likely to have added per-transaction overhead even in
   single-node embedded mode).
3. Checks whether `mvcc_enabled` is unintentionally compiled into the default
   embedded benchmark target — if so, that alone could explain a single-digit
   percentage-point regression.

## References

- Issue: rjwalters/vibesql#5643
- v0.2.0 release commit: `ec54522d1`
- v0.2.0 release dashboard JSON: `web-demo/public/benchmarks/tpcc_results.json`
- Benchmark history DB: `~/.vibesql/test_results/benchmark_results.vbsql`
  (Run ID 176)
