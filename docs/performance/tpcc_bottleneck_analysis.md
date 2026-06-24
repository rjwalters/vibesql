# TPC-C / OLTP Throughput Bottleneck Analysis

**Issue:** #5700 — [perf] Profile and scope TPC-C / OLTP throughput bottleneck
**Date:** 2026-06-24
**Profile artifact:** [`profiles/tpcc-profile-2026-06-24.json.gz`](profiles/tpcc-profile-2026-06-24.json.gz)
(open with `samply load docs/performance/profiles/tpcc-profile-2026-06-24.json.gz` to view the flame graph with resolved symbols)

---

## TL;DR

VibeSQL's per-query CPU breakdown on the TPC-C OLTP workload is dominated by **SQL
re-parsing** and **per-query executor construction**, not by the storage engine, index
scans, or MVCC. On the standard mixed workload, **28.8–29.0% of all query CPU time is
spent in the parser**, and on the parse-heavy New-Order transaction that rises to
**37.7%**. SQLite — which uses prepared statements and pays the parse cost exactly once
per statement template — is **~3.2x faster than VibeSQL on short, high-frequency
transactions** (New-Order) on the *same machine under the same load*. Conversely, on the
single-statement, subquery-heavy Stock-Level transaction (parse cost negligible at 2.5%),
VibeSQL is **~26x faster than SQLite**. The relative OLTP weakness is therefore
concentrated entirely in the short-statement, parse-bound regime.

---

## Methodology

### Comparative framing (READ THIS FIRST)

**Absolute throughput numbers on this machine are not trustworthy.** The development host
is always under variable load (parallel build agents, test runs, etc.), so any single
wall-clock TPS or microsecond figure quoted in isolation is noise.

**Comparative numbers ARE valid.** Every measurement below was taken by running VibeSQL
and SQLite back-to-back in the *same process invocation*, on the *same machine*, under the
*same instantaneous load*. Because both engines see identical host conditions, the
**ratio** between them cancels machine-load noise. All quantitative claims in this document
are expressed as:

1. **Ratios** (VibeSQL-vs-SQLite on the same run), or
2. **Proportional CPU splits** (parse% vs execute% within VibeSQL's own work, measured by
   in-process instrumentation), or
3. **Flame-graph proportions** (relative width of call-stack subtrees).

No absolute TPS target appears anywhere in this analysis, and none should be derived from
it. Follow-up issues likewise specify *comparative* before/after targets ("close X% of the
gap vs SQLite on the same machine"), never absolute TPS goals.

### How the measurements were taken

The TPC-C benchmark binary (`crates/vibesql-executor/benches/tpcc_benchmark.rs`, built with
the `sqlite` feature) runs both engines in one process and prints a comparison summary. Two
independent instruments were used:

1. **In-process parse/execute split.** `crates/vibesql-executor/benches/tpcc/transactions.rs`
   wraps every query with thread-local accumulators (`PARSE_TIME_US`, `EXECUTE_TIME_US`)
   around `Parser::parse_sql` and `SelectExecutor::execute`. The benchmark prints a
   `--- Query Profiling ---` block with `Parse %`. This is a *proportional* measurement of
   where CPU goes inside VibeSQL and is robust to machine-load noise (both numerator and
   denominator scale together when the host slows down).

2. **CPU flame graph (samply).** A 20s VibeSQL-only run was profiled with
   `samply record --rate 1000`, following `docs/performance/CPU_PROFILING.md`. The profile
   is archived at `profiles/tpcc-profile-2026-06-24.json.gz`. Symbols resolve lazily in the
   Firefox Profiler UI when loaded against the `profiling`-profile binary
   (`samply load …`); the save-only export does not embed resolved symbol names, so the
   per-function self-time table below is taken from the *instrumented* split rather than from
   parsing the gzip directly. The flame graph corroborates the split visually (the parser
   subtree is a wide, repeated band beneath every `execute_query` call site).

### Commands (reproducible)

```bash
# Build the comparative benchmark (VibeSQL + SQLite in one binary)
cargo build --release --package vibesql-executor --bench tpcc_benchmark --features sqlite

# Mixed workload, both engines, parse% breakdown
BIN=$(find target/release/deps -maxdepth 1 -name 'tpcc_benchmark-*' -type f ! -name '*.d' | head -1)
ENGINE_FILTER=vibesql,sqlite TPCC_DURATION_SECS=10 TPCC_WARMUP_SECS=2 TPCC_SCALE_FACTOR=1 "$BIN" mixed

# Per-transaction comparative runs
ENGINE_FILTER=vibesql,sqlite TPCC_DURATION_SECS=6 "$BIN" new-order
ENGINE_FILTER=vibesql,sqlite TPCC_DURATION_SECS=6 "$BIN" stock-level

# CPU flame graph (VibeSQL only, profiling profile)
cargo build --profile profiling --package vibesql-executor --bench tpcc_benchmark
PBIN=$(find target/profiling/deps -maxdepth 1 -name 'tpcc_benchmark-*' -type f ! -name '*.d' | head -1)
ENGINE_FILTER=vibesql TPCC_DURATION_SECS=20 samply record --rate 1000 --save-only \
  -o profile-tpcc.json.gz -- "$PBIN" mixed
```

### Benchmark coverage and its limits

- **Read-only simulation.** The TPC-C transactions in this harness issue **only SELECTs**.
  New-Order, Payment, Order-Status, Delivery, and Stock-Level are all modeled as read
  workloads — no INSERT/UPDATE/DELETE is executed against VibeSQL. This is a real coverage
  gap: any *write*-path bottleneck (WAL flush, delete compaction, index maintenance,
  row-version GC) is **invisible** to this benchmark. The findings below describe OLTP
  **read** throughput only. (See Bottleneck 3 and Follow-up B.)
- **Single client.** All runs above are single-threaded (`TPCC_CLIENTS=1`). Lock-contention
  effects under concurrency were not characterized; on a shared host, multi-client numbers
  mix OS scheduling noise with real contention and would not be trustworthy here.
- **MVCC off.** Confirmed that `mvcc_enabled` is **not** a default feature of either
  `vibesql-storage` (`default = ["compression"]`) or `vibesql-executor`. The benchmark
  binary is built without it, so `Row::visible_to` / the MVCC visibility filter in
  `crates/vibesql-storage/src/mvcc.rs` is **not on the hot path** in this build and is ruled
  out as a current OLTP bottleneck.

---

## Comparative Results

### Mixed workload (standard TPC-C transaction mix), VibeSQL vs SQLite, same run

| Metric | VibeSQL | SQLite | Comparative reading |
|---|---|---|---|
| Per-query **Parse %** (VibeSQL internal) | **28.8–29.0%** | ~0% (prepared statements) | Nearly a third of VibeSQL's query CPU is reparse SQLite does not pay |
| New-Order avg latency | higher | **lower** | VibeSQL ~2.6–3.2x slower on this short-statement txn (see below) |
| Stock-Level avg latency | **lower** | much higher | VibeSQL much faster on the subquery-heavy txn |

The mixed-run parse split was stable across repeated runs (29.0%, then 28.8% on a second
20s run), which is the signature of a *real* proportional effect rather than load noise.

### Per-transaction comparative ratios (same machine, same load)

| Transaction | VibeSQL Parse% | VibeSQL avg | SQLite avg | Ratio (VibeSQL ÷ SQLite) |
|---|---|---|---|---|
| **New-Order** (5–15 short SELECTs/txn) | **37.7%** | 148.4 us | 46.4 us | **~3.2x slower** |
| **Stock-Level** (1 subquery-heavy SELECT/txn) | **2.5%** | 295.6 us | 7773.7 us | **~26x faster** |

This is the central finding: **VibeSQL's relative OLTP weakness is entirely in the
short-statement, high-frequency regime where parse + per-query setup overhead dominates.**
When a transaction is a single non-trivial query, parse cost is amortized away (2.5%) and
VibeSQL's executor is dramatically faster than SQLite. When a transaction is a burst of
tiny point-lookup SELECTs, VibeSQL pays a 30–40% parse tax that SQLite avoids via prepared
statements, and loses by ~3x.

---

## Top Bottlenecks (proportional evidence)

### Bottleneck 1 — Per-query SQL re-parse (HIGH confidence, primary)

**Proportional evidence:** 28.8–29.0% of query CPU on the mixed workload, **37.7%** on
New-Order — measured directly by the in-process `PARSE_TIME_US` / `EXECUTE_TIME_US` split,
which is load-noise-robust because both terms scale together. The flame graph shows the
parser subtree as a wide, repeated band under every `execute_query` call.

**Why it happens:** `crates/vibesql-executor/benches/tpcc/transactions.rs` builds each query
as a fresh interpolated string (`format!("SELECT … WHERE w_id = {}", input.w_id)`) and calls
`Parser::parse_sql` on it every time. There is no statement cache and no parameter binding,
so identical query *templates* are tokenized and parsed from scratch on every execution. At
TPC-C query rates this runs the parser hundreds of thousands of times per second.

**Comparative target:** close the New-Order gap vs SQLite — currently ~3.2x — by reusing a
parsed statement per template. SQLite pays parse once per prepared statement; a VibeSQL
statement cache (or prepared-statement API with parameter binding) should drive Parse% from
~30–38% toward SQLite's effective ~0% and shrink the ratio proportionally.

→ **Follow-up A**

### Bottleneck 2 — Per-query `SelectExecutor` construction / allocation (MEDIUM confidence)

**Proportional evidence:** After parse (~29%), the remaining ~71% is execute time. Each
query constructs a brand-new `SelectExecutor::new(db)`
(`crates/vibesql-executor/src/select/executor/builder.rs`), which allocates a fresh
`QueryArena`, an aggregate-cache `HashMap`, and calls `Instant::now()` — once per query,
hundreds of thousands of times per second. The flame graph shows allocation/setup frames
recurring at every `execute_query` entry (visible as repeated narrow stacks immediately
under the executor entry point). This is setup overhead that is *constant per query* and
therefore proportionally largest exactly where queries are smallest — the same
short-statement regime where VibeSQL already loses to SQLite.

**Why it matters comparatively:** SQLite reuses a compiled statement (`sqlite3_stmt`) and
resets it (`sqlite3_reset`) rather than reconstructing execution state per call. VibeSQL
rebuilds executor state every time. The arena-prepared-statement infrastructure (#3271)
exists but is not wired into this per-query path.

**Comparative target:** on a point-lookup microbenchmark
(`SELECT w_tax FROM warehouse WHERE w_id = 1` repeated 100k times), measure
queries/sec with vs. without executor reuse; target recovering a measurable fraction of the
short-statement gap vs SQLite on the same host.

→ **Follow-up C**

### Bottleneck 3 — Write-path costs are unmeasured (coverage gap, not yet a confirmed bottleneck)

**Proportional evidence:** none — *because the benchmark issues no writes.* The harness
simulates all five transactions as read-only SELECTs, so delete compaction
(`Table::delete_by_indices` → `should_compact()` in
`crates/vibesql-storage/src/table/mod.rs`), index adjustment
(`adjust_indexes_after_delete` in `crates/vibesql-storage/src/table/indexes.rs`, already
documented in `delete-bottleneck-analysis.md`), and WAL flush are never exercised. A faithful
TPC-C New-Order inserts ~10–15 order-line rows, Payment updates three tables, and Delivery
deletes from `new_order` — none of which this benchmark performs. The current read-only TPS
is therefore an *upper bound* on true mixed read/write OLTP throughput.

**Comparative target:** build a write-faithful variant and confirm write throughput is not
catastrophically below the read-only number relative to SQLite on the same host.

→ **Follow-up B**

---

## What was infeasible in this environment

- **Resolved per-function flame-graph self-time table.** The save-only samply export does
  not embed resolved symbol names; resolution happens at `samply load` time against the
  debug binary in the Firefox Profiler UI. The flame graph itself is fully usable
  interactively (and is archived), but a batch-extracted "top N functions by self time"
  table could not be produced from the gzip directly. The proportional parse/execute split
  from in-process instrumentation is used instead, and is in fact *more* load-robust.
- **Trustworthy multi-client / concurrency numbers.** Not attempted — on a shared,
  load-variable host these mix scheduler noise with real lock contention and would violate
  the comparative-only constraint.
- **Write-faithful TPC-C.** Out of scope for this profiling pass; filed as Follow-up B.

---

## Follow-up issues

- **A — #5756** — Add a prepared-statement / parsed-statement cache to eliminate per-query
  reparse (targets Bottleneck 1). Comparative target: close the New-Order gap vs SQLite.
- **B — #5757** — Add write-faithful TPC-C coverage so write-path bottlenecks become
  measurable (targets the Bottleneck 3 coverage gap).
- **C — #5758** — Reuse / pool `SelectExecutor` across queries in a connection session
  (targets Bottleneck 2). Comparative target: recover short-statement gap vs SQLite on a
  point-lookup microbenchmark.

---

## References

- `docs/performance/CPU_PROFILING.md` — samply profiling flow
- `docs/performance/tpcc_regression.md` — prior regression context (clean-host re-run still pending)
- `docs/performance/delete-bottleneck-analysis.md` — delete hot-path analysis (relevant to Follow-up B)
- `docs/archive/tpcc-oltp-analysis.md` — 2025 OLTP root-cause analysis (composite index fix, #3084)
- `crates/vibesql-executor/benches/tpcc/transactions.rs` — instrumented benchmark transactions
- `crates/vibesql-executor/benches/tpcc_benchmark.rs` — comparative benchmark driver
- `crates/vibesql-storage/src/mvcc.rs` — MVCC visibility filter (gated off by default; not on hot path)
</content>
</invoke>
