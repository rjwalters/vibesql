# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-06-15

This release introduces a **Raft-based replication track** (new `vibesql-consensus` crate) and turns on the **MVCC visibility + on-demand GC** subsystem end-to-end. 369 commits since v0.1.4 also push SQLite compatibility forward — full SQLITE_MAX_TRIGGER_DEPTH (1000) trigger semantics, window-function correctness, ALTER TABLE RENAME COLUMN with trigger-body rewrite — and bring EXPLAIN QUERY PLAN output much closer to sqlite3 parity.

### Replication & Consensus

A new `vibesql-consensus` workspace crate built on `openraft` introduces single-group whole-database replication. The MVCC state machine applies committed transactions from the Raft log; HTTP and prepared-statement writes can now be routed through consensus.

- **Durable Raft log + vote persistence** (#5357)
- **In-process channel network + multi-node test cluster** (#5364)
- **TCP transport + cluster config + `make test-cluster`** (#5368)
- **Network snapshot transfer + durable snapshots + purge safety** (#5369, #5374)
- **MVCC state machine — apply committed txns from the Raft log** (#5378, #5384)
- **Freeze nondeterministic SQL at propose time** (#5377, #5382)
- **Freeze volatile DEFAULTs + validate replicated trigger bodies** (#5396)
- **Linearizable leader reads + stale-leader fencing** (#5387)
- **Bounded-staleness follower reads + read-your-writes tokens** (#5390)
- **Reject residual TIME→TIMESTAMP casts over non-target operands** (#5398, #5405)
- **Boot TCP test clusters on pre-bound ephemeral listeners** (#5507, #5514)

### Replicated server surface

The server now exposes the consensus surface across its HTTP and PostgreSQL-wire endpoints.

- **Replicated operational surface** — prepared-stmt writes, /health role, HTTP gating (#5411)
- **Interactive transactions (BEGIN…COMMIT) in replicated sessions** (#5402)
- **Mid-transaction RYW + savepoints in replicated sessions** (#5413)
- **Route HTTP `/api/query` + CRUD-create writes through consensus** (#5423)
- **Route HTTP CRUD by-id endpoints through consensus** (#5441)
- **Route HTTP GraphQL queries + mutations through consensus** (#5447)
- **Route HTTP blob storage writes through consensus** (#5457)
- **Buffer prepared EXECUTE inside a replicated transaction** (#5430)
- **Route replicated sessions' writes through MvccRaftNode** (#5395)
- **Feed HTTP SSE subscriptions from applied consensus entries** (#5452)
- **Coalesce apply-path change events for replicated subscriptions** (#5456, #5467)
- **Names-only replicated Describe to avoid materializing rows** (#5503)
- **Prune replicated subscription re-queries by changed PK** (#5496)
- **Resolve real column names in replicated SELECT results** (#5428)

### MVCC

VibeSQL's MVCC subsystem now covers the read path end-to-end, with on-demand garbage collection wired to `VACUUM`.

- **Stamp xmin/xmax on INSERT/UPDATE/DELETE behind `mvcc_enabled`** (#5193)
- **TxnSnapshot + Row::visible_to predicate (Phase 1b)** (#5184)
- **Add xmin/xmax to Row + bump vbsql to v7 (Phase 1a)** (#5142)
- **Phase 1d read-path visible_to filtering + FK deferred-replay coordination** (#5209)
- **Extend visible_to to index scans, PK lookup, UNIQUE scans** (#5204, #5222)
- **Widen snapshots for autocommit + in-txn self-write visibility** (#5223)
- **On-demand `vacuum_mvcc` for old-version garbage collection** (#5224)
- **Thread visibility filter through DML read sites** (#5205, #5225)
- **Enable SIMD/columnar fast paths under `mvcc_enabled`** (#5227)
- **Parse `VACUUM` / `VACUUM INTO` and map execution to MVCC GC** (#5226)

### SQL Compatibility

#### Triggers

Trigger semantics got a heavy pass to align with sqlite3 3.51.

- **Honor `sqlite3_limit(SQLITE_LIMIT_TRIGGER_DEPTH)` at runtime** (#5605)
- **Reach SQLite's full SQLITE_MAX_TRIGGER_DEPTH (1000) via on-demand stack growth** (#5610)
- **Raise recursion depth cap 16 → 700 toward SQLite** (#5533)
- **Honor `PRAGMA recursive_triggers = off`** (#5550)
- **Schema-aware trigger firing for temp vs. main triggers** (#5592)
- **Parse `CREATE TEMP/TEMPORARY TRIGGER` + schema-qualified names** (#5532)
- **Parse `CREATE TRIGGER`: `IF NOT EXISTS`, reject `FOR EACH STATEMENT`, surface body syntax errors** (#5495)
- **Reject `CREATE TRIGGER` with bound parameter in body / `WHEN`** (#5499)
- **Accept unparenthesized `UPDATE OF` column list** (#5580)
- **Accept subquery in trigger `WHEN` clause** (#5586)
- **Support `RAISE()` in trigger bodies** (#5415)
- **Reject `RAISE()` outside trigger programs at parse time** (#5424)
- **Resolve `NEW`/`OLD` in trigger `WHEN`-clause subqueries** (#5585, #5593)
- **Resolve `NEW.rowid` / `OLD.rowid` (oid / _rowid_) in trigger bodies** (#5519)
- **Live columnar values for BEFORE/AFTER UPDATE triggers** (#5543, #5546)
- **Live `count(*)` for combined BEFORE+AFTER DELETE triggers** (#5542)
- **Interleave BEFORE/AFTER row triggers per-row in multi-row DML** (#5504)
- **Cascade-drop triggers ON a dropped table (temp lifecycle)** (#5597)
- **Cascade-drop INSTEAD OF triggers when their view is dropped** (#5604)
- **List temp views + temp triggers in `sqlite_temp_master`** (#5582)
- **Transactional DDL rollback + `DROP TRIGGER IF EXISTS`** (#5506)
- **`UPDATE OR REPLACE/IGNORE` conflict resolution + DELETE triggers on WITHOUT ROWID** (#5522)
- **Fire child UPDATE triggers on `ON DELETE SET NULL`/`SET DEFAULT` cascades** (#5518)
- **Statement-end orphan FK check after cascade-fired `RAISE(IGNORE)`** (#5498)
- **Auto-commit statement atomicity for cascade / `RAISE(ABORT)` and multi-row DML** (#5473)
- **`OLD/NEW` in `INSERT…SELECT` trigger bodies + trigger\*.test triage** (#5493)
- **Rewrite trigger table refs + verbatim SQL on `ALTER TABLE RENAME`** (#5510)

#### Window Functions

- **Match SQLite last-pass row order and sort `GROUP BY` output by key** (#5311)
- **Propagate `PARTITION BY` evaluation errors instead of NULL-keying** (#5305)
- **Honor `NULLS FIRST/LAST` and collation in sort and RANGE frames** (#5297)
- **Resolve aggregate args of embedded window functions via hidden columns** (#5280)
- **Evaluate window functions mixed with aggregates in one `SELECT`** (#5265)
- **Window IN-subqueries and ordinal `ORDER BY` through wildcards** (#5256)
- **window1.test residuals — nested scope binding, UNION-arm misuse, text coercion** (#5273)
- **WHERE push-down into window subqueries + covering-index window-sort EQP** (#5349)
- **Pre-compute aggregates in `OVER (PARTITION BY/ORDER BY/frame)`** (#5127)
- **Reject non-window aggregates in `ORDER BY` of window queries** (#5094, #5119)
- **Detect window-function misuse in positional `GROUP BY`** (#5111)
- **`COUNT()`/`COUNT(*)` over post-aggregate frames now counts all frame rows** (#5089)
- **Preserve REAL type in `sum()` over float inputs**
- **`GROUP_CONCAT` dispatch and `RANGE ORDER BY` validation** (#5058)
- **Thread `case_sensitive_like` PRAGMA through all filter paths**
- **Recurse through all expression variants for window detection** (#5095, #5121)

#### ALTER TABLE

- **`ALTER TABLE RENAME COLUMN` with trigger-body column rewrite** (#5602)
- **Abort `RENAME COLUMN` on ambiguous trigger-body column ref** (#5607)
- **Edit verbatim `sqlite_master.sql` in place for `DROP COLUMN` / `RENAME TO`** (#5635)
- **Sync catalog schema on ALTER + edit verbatim SQL in place** (#5633)
- **Match sqlite3 wording for `ALTER TABLE RENAME TO` collision** (#5560)

#### Foreign Keys

- **Partial-index awareness unblocks fkey1-6.0/6.1/6.2** (#5213)
- **Wire `sqlite3_db_status DBSTATUS_DEFERRED_FKS` bridge** (#5211)
- **Persist DEFERRABLE clause and skip auto-save inside txn** (#5185)
- **Self-ref multi-row INSERT + tcl-shim multi-table parse** (#5180)
- **Collation-aware comparison in CASCADE/SET NULL/SET DEFAULT helpers** (#5176)
- **Self-FK + deferral introspection (Phase C3 of #5085)** (#5141)
- **Deferred FK violation queue + COMMIT-time enforcement (Phase C2 of #5085)** (#5133)
- **Catalog metadata + `PRAGMA defer_foreign_keys` (Phase C1 of #5085)** (#5124)
- **Emit "foreign key mismatch" error class for invalid FK targets** (#5120)
- **Fire child row triggers on FK ON DELETE/UPDATE CASCADE** (#5463)

#### Parser

- **Parse `VACUUM` / `VACUUM INTO`** (#5226)
- **Support `WITH` clause before `VALUES` statement** (#5358)
- **Support `date()` and `time()` as scalar function calls** (#5316)
- **Accept `TEMP/TEMPORARY` modifier on `CREATE TRIGGER`** (#5229)
- **Support partial-index `WHERE` clauses in `CREATE INDEX`** (#5109)
- **Support `RESTRICT`, post-constraint `DEFAULT`, empty statements** (#5088)

### EXPLAIN QUERY PLAN

EQP output moved much closer to sqlite3 parity this cycle.

- **Expand views / subqueries in EQP output (windowpushd)** (#5354)
- **Flatten plain views in EQP output** (#5360)
- **Render blocked view bodies as CO-ROUTINE blocks in EQP** (#5366)
- **Emit temp B-tree annotations for GROUP BY/DISTINCT/compound** (#5370)
- **Ordering-index scan lines + compound `ORDER BY` shape** (#5372)
- **Suppress `ORDER BY` temp B-tree for rowid-alias natural-order scans** (#5380)
- **Emit EQP window-sort entries for `OVER`-clause sorting passes** (#5244)
- **Suppress statement `ORDER BY` temp-B-tree entry when covered by first window sort key** (#5249)
- **Restrict window-sort index suppression to the innermost pass** (#5250)

### Storage & Indexes

- **Schema-aware index manager — same-named indexes coexist across schemas** (#5540, #5555)
- **Schema-aware spatial indexes — same-name cross-schema coexistence** (#5558, #5576)
- **Schema-aware indexes + `sqlite_temp_master`** (#5513, #5539)
- **HNSW threshold-triggered graph compaction to restore recall** (#5461)
- **Vector index per-row maintenance — keep IVFFlat/HNSW in sync on non-compacting INSERT/UPDATE/DELETE** (#5453)
- **Rebuild IVFFlat/HNSW vector indexes on table compaction** (#5449)
- **Make full-tree index rebuild reversible mid-transaction** (#5443)
- **Undo disk-backed (spilled) index mutations on ROLLBACK** (#5433)
- **Scope spilled-index undo to statement savepoint (RAISE(ABORT))** (#5458)
- **Copy-on-write Operations snapshot for txn rollback** (#5426)
- **Preserve original index-name case in SQL dump** (#5579, #5590)
- **Quote identifiers in SQL-dump index DDL round-trip** (#5567)
- **Index-ordered scans wrong after mid-statement tombstone delete** (#5524, #5530)
- **Repopulate catalog index metadata on binary load** (#5220)
- **Preserve original-case identifier echo across dump/reload** (#5618, #5621)
- **Persist `CREATE TRIGGER` in SQL-dump format** (#5086)

### Planner & Optimizer

- **Select partial indexes via structural predicate implication** (#5331)
- **Preserve NULL semantics in scalar-comparison decorrelation** (#5281)
- **Implicit-outer-aggregate-collapse + correlated window subquery** (#5134)
- **Scope-aware outer-aggregate-collapse for nested FROM subqueries** (#5139)

### Performance

- **Add HNSW recall@k benchmark to the suite for regression tracking** (#5476)
- **Add explicit SIMD intrinsics for filtered `SUM` on f64** (#5042)
- **Incremental columnar maintenance for INSERT/UPDATE** (#5043)
- **Extend vectorized execution to LEFT/RIGHT JOINs and `ORDER BY`** (#5039)
- **Early bail-out checks in columnar JOIN dispatch** (#5051)

### Web Demo

- **Surface HNSW recall@k on the trends dashboard** (#5481)
- **Restore expectedRows/expectedCount payloads for example validation** (#5259)
- **Restore sqllogictest SQL payloads** (#5257)
- **Pin web-demo as standalone pnpm workspace and resync lockfile** (#5252)

### Infrastructure

- **New `vibesql-consensus` workspace crate** — published to crates.io alongside the rest of the workspace.
- **Consolidate `rusqlite` under `[workspace.dependencies]`** (#5171, follow-up clean-up #5237)
- **Migrate pnpm overrides to `pnpm-workspace.yaml`** (#5169)
- **Loom 0.10.0 installation** (#5168)
- **Add VibeSQL-specific `/loom:release` skill** — interactive release flow with awareness of the four version-bearing files and the dual crates.io / PyPI tag-triggered workflows.
- **Add `scripts/version.sh` for version sync + release tagging** (#5356)
- **`pyo3` 0.28 → 0.29** (#5379), **`arrow` 58.1 → 59.0** (#5242), **`rusqlite` 0.39 → 0.40** (#5163), **`rstar` 0.12 → 0.13** (#5162)
- **Routine dependabot grouped bumps** across web-demo, benchmarks, and root.

---

## [0.1.4] - 2026-01-18

This release focuses on **SQLite compatibility** and **morsel-driven parallel execution**. With 878 commits since v0.1.3, highlights include 100% TCL test pass rate on Priority 1 tests, memory-bounded operators with spill-to-disk, UPDATE FROM syntax, and CHECK constraint enforcement.

### Performance

#### Phase 10: Morsel-Driven Parallel Execution

- **Morsel-driven dispatcher** - Work-stealing parallel dispatcher with adaptive morsel sizing (#4160)
- **Parallel GROUP BY** - Morsel-driven work-stealing for GROUP BY operations (#4166)
- **Parallel hash join** - Morsel-driven build and probe phases (#4169, #4146)
- **Parallel sort** - Morsel-driven parallel sort with work-stealing (#4172)
- **Parallel nested loop joins** - Morsel-driven parallelism for nested loop joins (#4276, #4280)
- **Adaptive morsel sizing** - Query-characteristic-based morsel size adaptation (#4170)
- **Per-operation morsel sizes** - Tuned morsel sizes per operation type (#4291)
- **Tree-based parallel merge** - Efficient parallel merge for hash table build (#4279)

#### Memory-Bounded Operators

- **External sort** - Spill-to-disk sorting for datasets larger than memory (#4190)
- **Memory-bounded aggregation** - Graceful degradation under memory pressure (#4190)
- **Memory-bounded join** - Hash join with configurable memory limits (#4190)

#### Query Execution

- **Native columnar by default** - Enable native-columnar execution by default (#4204)
- **Columnar HAVING** - HAVING clause support for columnar GROUP BY execution (#4183, #4185)
- **Parallel columnar filter** - Parallel filter mask creation for columnar execution (#4210, #4248)
- **CTE filter-while-copy** - Filter optimization for CTE scans (#4207)
- **Bloom filter pre-filtering** - Scan-time pre-filtering for multi-way joins (#4372)
- **Cost-based INL decision** - Cost-based index nested loop decision for semi-joins (#4354, #4361)
- **Column pruning integration** - Integrate column pruning into aggregation execution (#4377, #4383)
- **Batch insert optimization** - Use batch insert for INSERT...SELECT bulk transfer
- **Early short-circuit** - Early termination for constant FALSE WHERE clauses

### Added

#### SQLite Compatibility

- **TCL test suite** - 100% pass rate on Priority 1 tests (select, where, join, aggregate, etc.)
- **UPDATE FROM** - Multi-table UPDATE syntax (SQLite extension) (#4969)
- **DELETE with ORDER BY LIMIT** - SQLite extension for ordered deletion (#4973)
- **CHECK constraint enforcement** - Runtime validation of CHECK constraints (#4967)
- **Row value comparisons** - Tuple comparison expressions (#4968)
- **DEFERRABLE constraints** - Foreign key deferral support (#4988)
- **ON CONFLICT DO NOTHING/UPDATE** - Upsert clause support (#4888)
- **RTRIM collation** - Collation that ignores trailing spaces (#4962)
- **json() function** - JSON validation and minification (#4932)
- **VALUES as view source** - VALUES clause in CREATE VIEW (#4800)
- **IN table_name syntax** - `column IN table_name` shorthand (#4885)
- **JSON extraction operators** - `->` and `->>` operators for JSON access
- **Multiple datetime modifiers** - Chain modifiers in DATETIME function (#4965)
- **SQLite multi-word type aliases** - Accept type names like `UNSIGNED BIG INT` (#4936)
- **Parenthesized join aliases** - Alias support for parenthesized join expressions

#### SQL Features

- **Recursive CTE support** - WITH RECURSIVE for hierarchical queries (#4480, #4481)
- **CREATE ASSERTION** - SQL:1999 F671/F672 runtime enforcement (#4238)
- **Aggregate ORDER BY** - ORDER BY clause support in aggregate functions (#4590)
- **Session-scoped temp tables** - Isolated temporary tables per session (#4797)
- **Temp schema resolution** - Qualified table references for temp schema (#4976)
- **Expression indexes** - Indexes on computed expressions (#4768, #4975)
- **Column-level collation** - COLLATE clause on column definitions (#4570)
- **COLLATE NOCASE** - Case-insensitive comparisons and BETWEEN
- **GROUPS frame unit** - Window function GROUPS frame support
- **Window function validation** - Validate argument counts and OVER clause requirement (#4978, #4979, #4982)
- **ROWID pseudo-column** - INTEGER PRIMARY KEY aliasing (#4562)

#### CLI Improvements

- **Dot-command support** - SQLite-style `.tables`, `.schema`, `.help` commands (#4197, #4203)
- **Positional database argument** - `vibesql mydb.vbsql` syntax (#4201)
- **Auto-detection** - Automatic database file format detection
- **Raw output format** - TCL test compatibility mode (#4222)

#### Developer Experience

- **SQL pretty-printer** - AST node pretty-printing (#4195)
- **Improved sqlite_master** - Better SQL column for views and triggers (#4189)
- **Concurrent read queries** - SharedDatabase for parallel read execution (#4306)
- **TCL test infrastructure** - Native TCL execution with skip list management
- **Benchmark improvements** - CLI-based TPC-H, MySQL dialect support, Docker auto-start

### Fixed

#### Query Correctness (488 bug fixes)

- **NATURAL/USING JOIN fixes** - Correct column ordering and COALESCE semantics for RIGHT/FULL JOIN (#4791, #4792, #4801, #4811, #4897)
- **Window function fixes** - Result ordering, context validation, argument count validation (#4987, #4989)
- **Type affinity handling** - SQLite-compatible coercion in UPDATE, IN expressions, comparisons (#4822, #4889)
- **LIKE/GLOB improvements** - Unicode support, escape character handling, multi-byte characters (#4809, #4830, #4832, #4884)
- **Aggregate fixes** - MIN/MAX return NULL for all-NULL values, detect misuse in scalar subqueries (#4823, #4864)
- **Trigger fixes** - Fire DELETE triggers during REPLACE INTO, default to ROW granularity (#4796, #4898)
- **Operator precedence** - Correct `||` precedence to match SQLite (#4963)
- **Generated columns** - Recompute on UPDATE (#4961)
- **WITHOUT ROWID** - Enforce table constraints (#4970)
- **Integer display** - Show large integers as exact values, not scientific notation (#4895)
- **NaN handling** - Convert NaN arithmetic results to NULL (#4820)
- **Rowid preservation** - Preserve row_id in index scan WHERE clauses (#4964)

#### Join & Subquery Fixes

- **Chained NATURAL JOIN** - Deduplicate joined columns in SELECT * (#4902)
- **N-way COALESCE** - Proper COALESCE chain for chained NATURAL FULL JOINs (#4903, #4904)
- **USING column resolution** - Deterministic column selection and ordering (#4842, #4856)
- **Predicate pushdown** - Prevent pushdown for unqualified columns in outer joins (#4919)
- **Subquery validation** - Validate ON clause subqueries for right-table references (#4812)
- **IN→EXISTS rewrite** - Qualify unqualified column refs in rewrite (#4894)

#### Parser & Evaluation

- **Parenthesized subqueries** - Handle in IN expressions
- **ORDER BY term limit** - Enforce maximum ORDER BY terms (#4933)
- **ESCAPE validation** - Reject empty and multi-character ESCAPE strings (#4935)
- **Set operation errors** - Propagate column count mismatch for INTERSECT/EXCEPT (#4931)
- **CASE WHEN truthiness** - Non-zero numbers as truthy (#4833)
- **Unary minus on strings** - SQLite compatibility (#4825)
- **Printf format specifiers** - Correct flags, width, and 32-bit representation (#4835, #4937, #4960)

### Changed

- **Default to native columnar** - Enable native-columnar execution by default for analytical workloads
- **Schema naming** - Use 'main' schema instead of 'public' for SQLite compatibility (#4623)
- **Case-sensitive tables** - SQL:1999 compliant table identifier handling (#4396, #4403)
- **Removed 25+ unused dependencies** - Dependency cleanup across 6 crates (#4860)

### Infrastructure

- **Cloudflare Pages deployment** - Replace GitHub Pages with Cloudflare Pages
- **TCL test CI integration** - Automated TCL test suite in CI (#4226)
- **macOS CI update** - Update from deprecated macos-13 to macos-15
- **vibesql-bench-common** - Shared benchmark infrastructure crate (#4398)

---

## [0.1.3] - 2025-12-08

This release focuses on **Phase 9 performance optimization** and **cross-connection subscription enhancements**. With over 500 merged PRs since v0.1.2, highlights include O(1) row deletion, skip-scan optimization, Bloom filter joins, selective column updates for subscriptions, and comprehensive observability metrics.

### Performance

#### Phase 9b Core Optimizations

- **Deletion bitmap** - O(1) row deletion via bitmap-tracked slots (#3789)
- **SimpleFastPath column caching** - Cache column names for prepared statements (#3788)
- **Streaming range scan** - Iterator-based range scans avoiding full materialization (#3793)
- **Single-row PK DELETE fast path** - Optimized deletion for single-row primary key lookups (#3801)
- **Skip unaffected indexes** - Skip updating user-defined indexes when UPDATE doesn't touch indexed columns (#3800)
- **Covering index scan** - Return results directly from indexes without table lookup (#3804)

#### Query Execution

- **Skip-scan optimization** - Non-prefix index usage for range queries (#4081, #4088, #4089)
- **Bloom filter joins** - Build-side Bloom filters for hash join optimization (#4079)
- **ONEPASS UPDATE** - Single-pass optimization for single-row UPDATE operations (#4080)
- **Streaming range SELECT** - Pre-allocation for streaming range queries (#4073)
- **Lazy FromResult materialization** - Deferred row cloning in query results (#4068)
- **DISTINCT containment check** - Avoid cloning when checking DISTINCT containment (#4070)
- **Streaming aggregation** - Fast path for SUM range queries (#3818)
- **Early projection** - Project columns early in PK range scans (#3814)
- **Range scan row cloning** - Reduce cloning overhead in range scan fast path (#3848)
- **Column-to-column predicates** - Support column comparisons in columnar filtering (#4048)
- **Columnar deduplication** - DISTINCT queries in columnar join path (#3787)
- **LIMIT/OFFSET in columnar** - Support LIMIT/OFFSET in columnar join path (#3782)
- **Column resolution caching** - 34% faster Sysbench point lookups (#3593)

#### Optimizer Improvements

- **Semi-join filter pushdown** - Early semi-join filter pushdown for aggregate IN subqueries (#3685)
- **Composite key selectivity** - Fix composite key selectivity estimation (#3680)
- **IN subquery conversion** - Convert IN subqueries with GROUP BY/HAVING to semi-joins (#3666)
- **Case-insensitive column lookup** - Optimize column_index_cache for case-insensitive lookups (#3726)

#### Storage Engine

- **SmallVec for Row.values** - Reduce heap allocations for rows with ≤8 columns (#3954)
- **String interning** - Intern low-cardinality string columns (#3973)
- **Arc\<str\> for strings** - Use Arc\<str\> for Varchar/Character values (#3904)
- **Batch index updates** - Batch optimize B+tree and spatial index operations (#3877, #3882, #3888, #3896, #3916)
- **DELETE hot path** - Direct delete_by_pk_fast avoiding double-cloning (#3860)
- **Lazy row ID adjustment** - O(1) single-row deletes (#3725)
- **Index point lookup** - Reduce allocation overhead in index point lookups (#3701)
- **Skip trigger overhead** - Skip trigger overhead when no triggers defined (#3704)
- **LIMIT-aware range scan** - Early termination for LIMIT queries (#3699)
- **Batch DELETE** - Batch index updates for DELETE operations (#3693)
- **Incremental index adjustment** - Incremental adjustment for DELETE operations (#3416)

#### Index Optimizations

- **O(n²) index creation fix** - Fix O(n²) index creation scalability issue (#3679)
- **Hash join row combination** - Optimize row combination in hash joins (#3677)
- **Plan caching** - SimpleFastPath plan caching for prepared statements (#3676)

#### Data Loading

- **TPC data loading** - Optimize TPC data loading for large scale factors (#3640)
- **Fix O(n²) batch insert** - Fix O(n²) data loading in batch insert (#3671)

### Added

#### Cross-Connection Subscriptions

- **Real-time notifications** - Notify subscribers across connections when data changes (#3825)
- **Delta updates** - Send only changed rows for cross-connection notifications (#3834)
- **Async select optimization** - Use async select for efficient notification dispatch (#3832)
- **Selective column updates** - Send only changed columns in subscription updates (#3843, #3854)
- **Partial event type** - New SSE event type for selective column updates (#3952)
- **PartialRowUpdate messages** - Wire protocol support for selective updates (#3930)
- **Per-subscription config** - Configure selective update thresholds per subscription (#3996)
- **PK-based delta computation** - Use primary key columns for efficient delta detection (#3895)
- **Subscription filtering** - Add filtering expressions to subscriptions (#3846)
- **Protocol extensions** - Add Ack, Pause, Resume subscription protocol messages (#3837)
- **PK detection** - Detect primary key columns for selective subscription updates (#3861)

#### Observability & Metrics

- **Subscription metrics** - Active subscriptions gauge metric (#4015)
- **Bytes saved counter** - Track bytes saved by selective updates (#4017)
- **Eligibility breakdown** - Metrics for selective update eligibility reasons (#4009)
- **Partial update efficiency** - Metrics for partial vs full update ratio (#3951, #3893)
- **Selective-eligible gauge** - Track subscriptions eligible for selective updates (#3922)
- **HTTP efficiency endpoint** - `/stats/subscriptions/efficiency` endpoint (#3986)
- **Observability documentation** - Comprehensive metrics documentation (#4003)

#### Server Configuration

- **Environment variable overrides** - Configure server via environment variables (#4020)
- **API key env vars** - Set API keys via `VIBESQL_API_KEY` (#4027)
- **HTTP auth env vars** - Configure HTTP auth via environment (#4024)
- **Selective update thresholds** - Make column update thresholds configurable (#3969)

#### Developer Experience

- **VIBESQL_DEBUG flag** - Umbrella flag for enabling all debug output (#4056)
- **JSON debug output** - Structured JSON format for debug output (#4057)
- **Profiling decision tree** - Documentation for choosing profiling tools (#4058)
- **Skip-scan in EXPLAIN** - Show skip-scan plans in EXPLAIN output (#4087)
- **CPU profiling guide** - Guide for using samply profiler
- **DELETE hot path profiling** - Profiling instrumentation for DELETE operations (#3873)
- **Range scan profiling** - Add range scan profiling instrumentation (#3830)

#### SQL Compatibility

- **CREATE TABLE IF NOT EXISTS** - Standard SQL syntax support (#3820)
- **PostgreSQL type conversion** - Expanded type conversion coverage (#3897)
- **Window functions in CASE** - Support window functions in CASE and IS NULL expressions (#3813)

#### Internationalization

- **19 language translations** - Complete translations for web demo:
  - Arabic (ar) (#3760)
  - Dutch (nl) (#3759)
  - Hindi (hi) (#3757)
  - Indonesian (id) (#3769)
  - Italian (it) (#3756)
  - Polish (pl) (#3758)
  - Russian (ru) (#3755)
  - Swedish (sv) (#3772)
  - Thai (th) (#3771)
  - Turkish (tr) (#3762)
  - Ukrainian (uk) (#3768)
  - Vietnamese (vi) (#3770)
- **Locale switching** - Runtime locale switching for web demo (#3819)
- **Conformance page i18n** - Locale support for conformance page

#### TypeScript Client

- **Subscription events** - Emit subscription events from Connection class (#3881)
- **PartialData parser** - Parse SubscriptionPartialData (0xF7) messages (#3870, #3886)
- **Type narrowing fix** - Resolve TypeScript type narrowing in Connection.query() (#3878)

### Changed

- **DML cost estimation** - Use statistics-based cost estimates for DML optimization (#3968, #3974)
- **Row size WAL cost** - Consider row size in WAL write cost estimation (#3976)
- **Statistics fallback** - Prefer actual avg_row_bytes over schema estimates (#4018)
- **Switched to samply** - Replace flamegraph with samply (no sudo required)
- **Separate benchmark features** - Separate benchmark comparison features to avoid DuckDB overhead (#3594)

### Fixed

#### Benchmark Integrity

- **Remove SQL bypasses** - Remove fast-path SQL bypasses from TPC-C benchmark (#4046)
- **Sysbench SQL** - Replace direct API bypass with SQL in sysbench update_index (#4047)
- **DELETE timing** - Fix sysbench DELETE benchmark to measure actual DELETE time (#4037)
- **TPC-DS deduplication** - Deduplicate TPC-DS benchmark results by averaging iterations (#3817)

#### Query Correctness

- **Columnar cache invalidation** - Add columnar cache invalidation for ALTER TABLE, INSERT, TRUNCATE (#3931, #3941, #3946, #3989)
- **Missing columnar invalidation** - Fix missing columnar cache invalidation in REPLACE path (#3890)
- **Ambiguous column resolution** - Resolve ambiguous columns to leftmost table in LEFT JOINs (#3783)
- **Constant folding** - Add constant folding before predicate extraction in columnar join (#3775)
- **Semi-join equijoin extraction** - Extract equijoin predicates for comma-separated tables in semi-join (#3744)
- **Multi-column LEFT JOIN** - Add multi-column LEFT OUTER JOIN support (#3723)
- **Multi-column hash join** - Add multi-column hash join for WHERE clause conditions (#3670)
- **Table name normalization** - Normalize table names to lowercase for case-insensitive lookups (#3661)
- **Predicate pushdown validation** - Validate predicate pushdown in covering index scan (#3811)
- **Window functions in aggregates** - Apply window functions to aggregate queries for AVG(SUM(...)) patterns (#3706)

#### Storage & Indexes

- **Deleted row filtering** - SELECT queries filter deleted rows from deletion bitmap (#3791)
- **Index rebuild after compaction** - Rebuild user-defined indexes after table compaction (#3808)
- **Index rebuild after load** - Rebuild indexes after loading data from binary format (#3607)

#### Server

- **Connection closure** - Fix connection closure after ~150-190 queries (#3669)
- **Cross-connection reliability** - Improve cross-connection subscription notification reliability (#3867)
- **Axum compatibility** - Update axum route parameter syntax for v0.7 compatibility (#3853)
- **HTTP SSE port overflow** - Fix HTTP SSE tests port overflow on high TCP ports (#3836)

#### TPC Benchmark Alignment

- **TPC-DS Q69** - Implement official TPC-DS Q69 query per specification (#3729)
- **TPC-DS Q69 cartesian** - Remove cartesian product bug from Q69 query (#3717)
- **DuckDB TPC-H compat** - Fix DuckDB compatibility for TPC-H Q7-Q9 (#3686)
- **TPC-DS data generation** - Align TPC-DS data generation with DuckDB for consistent validation (#3652)
- **Warehouse loader RNG** - Align warehouse loader RNG parameters with DuckDB (#3796)
- **TPC-DS RNG patterns** - Align RNG patterns in VibeSQL loaders with DuckDB (#3792)

#### CLI

- **Table output** - Show actual column names and values in table output (#3812)

### Documentation

- **TPC-H Q4 analysis** - Root cause analysis for Q4 performance gap (#4043)
- **Anti-gaming warnings** - Add benchmark integrity warnings (#4044, #4045, #4049)
- **Selective update docs** - Document configuration options (#4007)
- **Metrics documentation** - Document all observability metrics (#4003, #4022, #4025)
- **Cloudflare CDN setup** - Add Cloudflare CDN setup guide for Brotli compression

---

## [0.1.2] - 2024-12-04

### Added

#### Vector Search
- **Distance operators** - `<->` (L2/Euclidean), `<#>` (inner product), `<=>` (cosine)
- **Distance functions** - `l2_distance()`, `inner_product()`, `cosine_distance()`, `cosine_similarity()`
- **IVFFlat index** - Inverted file index for approximate nearest neighbor search
- **HNSW index** - Hierarchical Navigable Small World graph for high-performance ANN

#### HTTP Server & API
- **REST API** - Auto-generated CRUD endpoints for all tables
- **GraphQL endpoint** - Full query support with schema introspection
- **Relationship resolution** - Nested queries following foreign key relationships
- **Authentication** - API key and token-based HTTP authentication
- **SSE subscriptions** - Real-time data streaming with Server-Sent Events
- **Delta updates** - Efficient change notifications for subscriptions
- **Backpressure handling** - Configurable channel buffers and rate limiting
- **Subscription limits** - Per-connection and global quotas
- **Retry with exponential backoff** - Automatic recovery from transient errors
- **Pagination** - `limit` and `offset` query parameters

#### Blob Storage
- **SQL integration** - `vibesql_storage` system table for blob metadata
- **HTTP endpoints** - Upload and download blobs via REST API
- **OpenDAL integration** - Cloud storage backends (S3, GCS, Azure, local filesystem)
- **TypeScript SDK** - Storage methods in `@vibesql/client`

#### SDK & Tooling
- **Drizzle ORM adapter** - `@vibesql/drizzle` package using sqlite-proxy driver
- **TypeScript codegen** - `vibesql-cli codegen` command for type-safe database access

#### Scheduled Functions
- **Cron scheduling** - Execute SQL statements on a schedule
- **Job management** - Create, list, pause, resume scheduled jobs

### Changed

- Unified benchmark CLI (`scripts/bench`) with consistent interface
- Consolidated benchmark result processing into `process_results.py`
- Improved SQLite dialect compatibility for dogfooding scenarios

### Fixed

- Clippy warning for approximate PI constant in tests
- WASM UUID generation with `js` feature for RNG support

---

## [0.1.0] - Unreleased

### Added

#### Core SQL Engine
- **Complete SQL:1999 Core compliance** - All 169 mandatory Core features implemented
- **100% sqltest conformance** - 739/739 tests passing
- **100% SQLLogicTest conformance** - 623 files (~5.9M tests)
- **In-memory storage engine** with full CRUD operations
- **Type system** supporting all SQL:1999 data types (INTEGER, VARCHAR, NUMERIC, DATE, TIMESTAMP, BOOLEAN, etc.)
- **NULL handling** with proper three-valued logic

#### Query Features
- **SELECT queries** with full expression support
- **Complex JOINs** - INNER, LEFT, RIGHT, FULL OUTER, CROSS
- **Subqueries** - scalar, correlated, and in predicates
- **Common Table Expressions (CTEs)** with recursive support
- **Window functions** - ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, aggregates with OVER
- **Aggregate functions** - COUNT, SUM, AVG, MIN, MAX, with DISTINCT support
- **GROUP BY and HAVING** clauses
- **ORDER BY with multiple columns** and NULL ordering (NULLS FIRST/LAST)
- **LIMIT and OFFSET** for pagination
- **DISTINCT** queries
- **Set operations** - UNION, INTERSECT, EXCEPT (with ALL variants)

#### DML Operations
- **INSERT** - single row, multi-row, and INSERT...SELECT
- **UPDATE** - with WHERE clauses and subqueries
- **DELETE** - with WHERE clauses
- **TRUNCATE TABLE** optimization

#### DDL & Schema Management
- **CREATE TABLE** with comprehensive column options
- **ALTER TABLE** - ADD COLUMN, DROP COLUMN, RENAME COLUMN, etc.
- **DROP TABLE** with CASCADE/RESTRICT
- **CREATE/DROP INDEX** including B-tree, R-tree, and FULLTEXT indexes
- **CREATE/DROP VIEW** with OR REPLACE and column lists
- **CREATE/DROP SCHEMA**
- **Constraint support** - PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, NOT NULL
- **Default values** and auto-increment

#### Security & Privileges
- **GRANT/REVOKE** privilege system
- **User and role management**
- **Table and column-level permissions**
- **WITH GRANT OPTION** support

#### Advanced SQL Features
- **Transaction support** - BEGIN, COMMIT, ROLLBACK, SAVEPOINT
- **Foreign key constraints** with referential integrity
- **Check constraints** with complex expressions
- **Sequences** for auto-incrementing values
- **Domains** for custom type definitions
- **Stored procedures & functions** with IN/OUT/INOUT parameters
- **Triggers** (BEFORE/AFTER)
- **Full-text search** with MATCH AGAINST and FULLTEXT indexes
- **Spatial/geometric types** with ST_* functions and R-tree indexes

#### Server & APIs
- **PostgreSQL wire protocol** - Compatible with psql, JDBC, ODBC clients
- **HTTP REST API** - Full CRUD operations with JSON responses
- **GraphQL API** - Schema introspection and queries
- **Real-time subscriptions** - Convex-like reactivity with delta updates
- **Server-Sent Events (SSE)** for HTTP streaming
- **WebSocket support** for persistent connections
- **Authentication and session management**

#### Extended Features
- **Vector types** - VECTOR(n) for AI/ML embeddings
- **Vector similarity search** - Cosine, Euclidean, dot product distance functions
- **Vector indexes** - HNSW and IVFFlat for approximate nearest neighbor
- **File/blob storage** - STORAGE_URL, STORAGE_SIZE functions
- **Scheduled functions** - SCHEDULE AFTER/AT, CREATE CRON

#### Query Optimization
- **Columnar execution engine** - SIMD-accelerated analytical queries
- **Columnar cache** - Lazy conversion with automatic invalidation on mutations
- **Cost-based join reordering** for multi-way joins
- **Predicate pushdown** - Filters pushed to table scans
- **Subquery-to-join transformation** - Converts correlated subqueries to efficient joins
- **Hash joins** for equi-join conditions
- **Index-based query optimization**
- **Query plan caching** for repeated queries
- **Parallel execution** - Hardware-aware parallelism for scans, joins, aggregation

#### Functions & Operators
- **200+ built-in functions**
- **Arithmetic operators** - +, -, *, /, %
- **Comparison operators** - =, <>, <, >, <=, >=
- **Logical operators** - AND, OR, NOT
- **String functions** - CONCAT, SUBSTRING, LENGTH, UPPER, LOWER, TRIM, POSITION
- **Numeric functions** - ABS, CEIL, FLOOR, ROUND, POWER, SQRT, trigonometric functions
- **Date/time functions** - CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, EXTRACT, date arithmetic
- **Conversion functions** - CAST, COALESCE, NULLIF
- **Conditional expressions** - CASE...WHEN...THEN...ELSE...END
- **Pattern matching** - LIKE, BETWEEN, IN
- **NULL handling** - IS NULL, IS NOT NULL, COALESCE, NULLIF

#### Bindings & Interfaces
- **Rust library** - Full programmatic API
- **Command-line interface (vibesql-cli)** - Interactive SQL shell with:
  - PostgreSQL-compatible meta-commands (\d, \dt, \l, etc.)
  - Multiple output formats (table, CSV, JSON, markdown, HTML)
  - Query history and auto-completion
  - Import/export functionality (\copy command)
  - Persistence (\save command for SQL dumps)
- **WebAssembly bindings** - Run in browser with live demo
- **Python bindings** - DB-API 2.0 compatible interface via PyO3
- **TypeScript SDK** - React hooks (useSubscription, useQuery) and Drizzle ORM adapter

#### Testing & Benchmarks
- **4,800+ unit tests** with comprehensive coverage
- **SQLLogicTest integration** - 623 files (~5.9M individual tests)
- **TPC-H benchmark** - All 22 queries passing
- **TPC-C benchmark** - All OLTP transactions passing
- **TPC-DS benchmark** - 97/99 queries passing
- **Sysbench** - OLTP workload testing

### Architecture

- **11 modular crates**:
  - `vibesql-types` - SQL:1999 type system
  - `vibesql-ast` - Abstract Syntax Tree definitions
  - `vibesql-parser` - Hand-written SQL parser
  - `vibesql-storage` - Storage engine with B-tree, R-tree, FULLTEXT indexes
  - `vibesql-catalog` - Schema and metadata management
  - `vibesql-executor` - Query execution with columnar engine
  - `vibesql-server` - Network server (PostgreSQL protocol, HTTP, GraphQL)
  - `vibesql-cli` - Command-line interface
  - `vibesql-wasm-bindings` - WebAssembly bindings
  - `vibesql-python-bindings` - Python interface
  - `vibesql-sqllogictest` - Conformance testing infrastructure

### Documentation
- **API documentation** for all public interfaces
- **CLI Guide** with meta-commands and output formats
- **Python Bindings Guide** with DB-API 2.0 reference
- **HTTP/GraphQL API documentation**
- **Live browser demo** at https://rjwalters.github.io/vibesql/

### Links

- **Repository**: https://github.com/rjwalters/vibesql
- **Documentation**: https://docs.rs/vibesql
- **Live Demo**: https://rjwalters.github.io/vibesql/
- **Crates.io**: https://crates.io/crates/vibesql
