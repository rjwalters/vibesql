# VibeSQL

## Performance Profiling

When debugging performance issues, see **[docs/performance/CPU_PROFILING.md](docs/performance/CPU_PROFILING.md)** for a decision tree that helps you choose the right tool:
- **samply** (`make profile-tpch Q=X`) for CPU profiling / flame graphs
- **Environment variables** (`JOIN_REORDER_VERBOSE=1`, etc.) for optimizer decision logging

## SQLite TCL Test Suite

VibeSQL runs SQLite's canonical TCL test suite for conformance testing. The test suite contains 1,174 test files covering core SQL functionality.

### Running TCL Tests

Tests use native `tclsh` by default, which correctly handles TCL constructs like loops that cannot be statically parsed.

```bash
# Run Priority 1 tests (core SQL: select, where, join, etc.)
make test-tcl

# Run all 1,174 TCL test files
make test-tcl-all

# Run a specific test file
make test-tcl-file FILE=select1.test

# Show test status
make test-tcl-status

# Parse a test file to see extracted tests (static parsing)
./scripts/tcltest parse select1.test

# Use static parsing mode (for debugging)
./scripts/tcltest test select1.test --no-native-tcl
```

### Priority Levels

- **Priority 1** (core SQL): `select`, `insert`, `update`, `delete`, `where`, `join`, `aggregate`, `func`, `orderby`, `index`
- **Priority 2** (advanced): `window`, `cte`, `trigger`, `fkey`, `collate`, `subquery`, `union`, `view`
- **Priority 3** (SQLite-specific): `wal`, `vacuum`, `attach`, `vtab`, `fts` (may not apply to VibeSQL)

### Test Infrastructure

- **TCL Shim**: `scripts/tester_vibesql.tcl` - Compatibility layer for running SQLite's TCL tests against VibeSQL
- **Runner**: `scripts/tcl_runner.py` - Executes tests (supports both native TCL and static parsing modes)
- **Parser**: `scripts/tcl_parser.py` - Static parser for extracting tests (used with `--no-native-tcl`)
- **CLI**: `scripts/tcltest` - Unified command-line interface
- **Results**: `~/.vibesql/test_results/tcl_test_results.vbsql`

### Results Tables and the Canonical Pass-Rate Query

Results are stored in two tables in `~/.vibesql/test_results/tcl_test_results.vbsql`:

- **`tcl_test_runs`** — one summary row per run (totals only). This is what `make test-tcl-status` reads (`ORDER BY run_id DESC LIMIT 1`).
- **`tcl_test_results`** — one detail row per test (per-file, per-test status). Required for per-file failure analysis. The `id` column is `INTEGER PRIMARY KEY AUTOINCREMENT` so concurrent runs never collide.

Both native-TCL mode (the default) and static-parsing mode now write per-test detail rows, so the summary and detail tables reconcile exactly.

**Marker statuses**: a file that does not run to completion never silently vanishes from the universe. The runner writes a synthetic marker row per compromised file — `status='timeout'` (per-file timeout expired; any tests that completed before the kill are salvaged as normal rows), `status='incomplete'` (tclsh worker killed by a signal or the shim crashed before its summary trailer; partial rows are kept), or `status='error'` (runner-level failure). Marker rows count as failures in pass-rate math so a compromised run reads *worse*, never silently smaller. `make test-tcl-status` reports files-attempted vs files-with-results plus marker counts, and warns when the file universe shrinks vs the previous run.

> **Quiet-machine guidance:** full canonical runs must be done on a quiet machine (no concurrent builds/benchmarks). A loaded machine produces timeout/incomplete markers, and a run containing any markers is not comparable to a clean-run baseline — rerun the affected files (or the whole suite) on a quiet machine.

**Canonical "current pass rate" query** (run against `tcl_test_results`; its numbers match `make test-tcl-status` to within ±1 rounding):

```sql
SELECT
  run_id,
  COUNT(*) AS total,
  SUM(CASE WHEN status='passed'  THEN 1 ELSE 0 END) AS passed,
  SUM(CASE WHEN status='failed'  THEN 1 ELSE 0 END) AS failed,
  SUM(CASE WHEN status='skipped' THEN 1 ELSE 0 END) AS skipped,
  SUM(CASE WHEN status IN ('timeout','incomplete','error') THEN 1 ELSE 0 END) AS not_run_markers,
  ROUND(
    100.0 * SUM(CASE WHEN status='passed' THEN 1 ELSE 0 END)
      / NULLIF(SUM(CASE WHEN status IN ('passed','failed','timeout','incomplete','error') THEN 1 ELSE 0 END), 0),
    1
  ) AS pass_rate
FROM tcl_test_results
WHERE run_id = (SELECT MAX(run_id) FROM tcl_test_results)
GROUP BY run_id;
```

Run it with:

```bash
./target/release/vibesql ~/.vibesql/test_results/tcl_test_results.vbsql -c "<query above>"
```

**Per-file failure breakdown** (these per-file failure counts sum to the total-failed reported by `make test-tcl-status`; timeout/incomplete/error marker rows are included because the summary table counts them as failures):

```sql
SELECT file_path,
  SUM(CASE WHEN status IN ('failed','timeout','incomplete','error') THEN 1 ELSE 0 END) AS failed
FROM tcl_test_results
WHERE run_id = (SELECT MAX(run_id) FROM tcl_test_results)
GROUP BY file_path
ORDER BY failed DESC
LIMIT 10;
```

### Raw headline vs. file-weighted metrics (which number is epic-comparable?)

The headline "current pass rate" above is a **raw per-test-row ratio**: passed rows over scored rows (passed + failed + marker rows), summed across every file in the run. This is *the* epic-comparable number. Epic #5779's baseline ("Raw pass rate: 72.3% — 116,719 passed / 44,624 failed" across 728 files) is itself a raw-row ratio with the same inclusion rules: files that produced detail rows in the latest run, with timeout/incomplete/error marker rows counted as failures and `skipped` rows excluded from the denominator. **Compare only raw-to-raw** — the raw headline is the one to hold against 72.3%. Its inclusion rules must never change, or the comparison silently breaks.

The raw ratio has one interpretive hazard: it is dominated by whichever files emit the most rows. A handful of "monster" files (`fuzz.test` ~25k tests, `func.test` ~14.7k) that VibeSQL fails wholesale can drag the raw headline down to single digits even while normal SQL files pass 90-99%. To make that visible — *without* changing the raw headline — `make test-tcl-status` additionally reports two **supplementary** file-weighted metrics. These are reported *alongside* the raw number, never in place of it, and are **not** comparable to the epic's 72.3% (different weighting):

**File-weighted metric — mean per-file pass rate + clean/dirty file counts.** Every file counts equally regardless of row count. `mean_per_file_pass_rate` is the average of each file's own pass rate; `clean_files` vs `files_with_failures` matches the epic's "315 clean / 413 with failures" framing (marker rows count as a failure, same as the raw headline):

```sql
WITH per_file AS (
  SELECT
    file_path,
    SUM(CASE WHEN status='passed' THEN 1 ELSE 0 END) AS f_passed,
    SUM(CASE WHEN status IN ('passed','failed','timeout','incomplete','error') THEN 1 ELSE 0 END) AS f_scored,
    SUM(CASE WHEN status IN ('failed','timeout','incomplete','error') THEN 1 ELSE 0 END) AS f_failed
  FROM tcl_test_results
  WHERE run_id = (SELECT MAX(run_id) FROM tcl_test_results)
  GROUP BY file_path
)
SELECT
  COUNT(*) AS files,
  ROUND(AVG(CASE WHEN f_scored > 0 THEN 100.0 * f_passed / f_scored END), 1) AS mean_per_file_pass_rate,
  SUM(CASE WHEN f_failed = 0 THEN 1 ELSE 0 END) AS clean_files,
  SUM(CASE WHEN f_failed > 0 THEN 1 ELSE 0 END) AS files_with_failures
FROM per_file;
```

**Top-N files by test count — monster-file visibility.** Surfaces the high-volume files that dominate the raw aggregate, so a low raw headline can be attributed to a few monster files at a glance:

```sql
SELECT
  file_path,
  COUNT(*) AS tests,
  SUM(CASE WHEN status='passed' THEN 1 ELSE 0 END) AS passed,
  ROUND(
    100.0 * SUM(CASE WHEN status='passed' THEN 1 ELSE 0 END)
      / NULLIF(SUM(CASE WHEN status IN ('passed','failed','timeout','incomplete','error') THEN 1 ELSE 0 END), 0),
    1
  ) AS pass_rate
FROM tcl_test_results
WHERE run_id = (SELECT MAX(run_id) FROM tcl_test_results)
GROUP BY file_path
ORDER BY tests DESC
LIMIT 10;
```

> **Which number do I quote?** For "how does VibeSQL compare to the epic baseline / to a previous run" use the **raw** headline (unchanged inclusion rules). For "how broad is our SQL coverage, ignoring monster-file domination" use the **file-weighted** mean and the clean/dirty file counts. Never quote the file-weighted number against 72.3% — they are different denominators.

> **Source-of-truth note:** `make test-tcl-status` reads the `tcl_test_runs` summary table for the headline numbers. Any per-file or per-test analysis must query the `tcl_test_results` detail table using the queries above. The two reconcile because every run (native-TCL and static) now writes both. If a detail-row insert fails, `tcl_runner.py` logs it to stderr, counts it, and exits non-zero when more than 5% of inserts fail — so silent divergence between the tables cannot recur unnoticed.

### Exporting Results

```bash
# Export TCL results to website format
python3 scripts/export_tcl_results.py --verbose
```

## Replication and Consensus

VibeSQL ships single-group Raft replication via the `vibesql-consensus` crate (built on `openraft`). See [docs/decisions/0004-consensus-library.md](docs/decisions/0004-consensus-library.md) for the architectural decision.

```bash
# Spin up a local multi-node test cluster (TCP transport)
make test-cluster

# Enable MVCC (snapshot isolation + on-demand GC via VACUUM)
cargo build --release --features mvcc_enabled
```

The replicated state machine applies committed transactions from the Raft log. HTTP REST, GraphQL, and CRUD writes route through consensus when the server runs in replicated mode.

## CLI Durability (Write-Ahead Log)

The `vibesql` CLI keeps a Write-Ahead Log for file-backed databases so committed changes survive an unclean shutdown (crash, SIGKILL, power loss). WAL is **on by default**; set `[database] wal = false` in `~/.vibesqlrc` to opt out and use the snapshot-only path.

For a database file `mydata.vbsql`, WAL-active mode maintains two sibling files next to it:

```text
mydata.vbsql            — binary snapshot (last checkpoint, loaded on open)
mydata.wal              — active write-ahead log
mydata-checkpoints/     — checkpoint archive directory (checkpoint_*.vchk)
```

On open, the CLI recovers from the latest checkpoint and replays WAL entries written after it; on `\save` / clean exit it writes a fresh checkpoint and truncates the WAL. Recovery restores both table schemas (DDL) and committed row data (DML inserts, updates, deletes); uncommitted transactions at crash time are discarded, and a truncated WAL tail recovers up to the last complete, checksum-valid entry. A legacy snapshot-only `.vbsql` (no checkpoint archive yet) is loaded as the recovery base on first WAL open.

**Recovery failure policy (never silently empty):** a database written by a newer VibeSQL binary (forward format version) is a hard open error — "database written by a newer version of VibeSQL" — never an empty or stale database, and never masked by the SQL-dump fallback. An unreadable/corrupt newest checkpoint is also a hard error by default; pass `--recover-fallback` to explicitly opt into recovering from the newest readable older checkpoint (every skipped checkpoint file is reported on stderr). If a `.vbsql` mysteriously opens empty under an old binary, suspect format-version skew before assuming data loss — the checkpoint files are intact on disk.

The engine lives in `crates/vibesql-storage/src/wal/` (writer, reader, checkpoint, truncate, recovery). DML WAL ops carry an inline `table_name` (WAL format version 2) so `RecoveryManager::apply_op` can route each row mutation back to its table during replay. Server-mode durability is separate (the replicated server routes writes through the Raft log + MVCC state machine).

## Release Flow

Run `/loom:release` from this repo to drive a v0.X.Y cut interactively:

- Pre-flight checks (CI status, open PRs, version-file alignment)
- Gather changes since last tag, classify by conventional-commit scope
- Semver decision + CHANGELOG entry drafting
- Atomic bump across the four version sources: workspace `Cargo.toml`, `Cargo.lock`, root `pyproject.toml`, `crates/vibesql-python-bindings/pyproject.toml`
- Bulk-bump all internal `vibesql-* = { version = "..." }` pins so published artifacts carry self-consistent requirements
- Commit + annotated tag; pushing the tag triggers `release-crates.yml` (crates.io) and `release-pypi.yml` (PyPI) in parallel
- Final GitHub Release with the CHANGELOG block as notes

## Benchmarking and Website Updates

VibeSQL uses a dogfooded SQLite-compatible database (`~/.vibesql/test_results/benchmark_results.vbsql`) to store all benchmark results. The web demo at https://vibesql.org/ displays this data.

### Running Benchmarks

```bash
# Quick benchmark (CI mode, ~25 min)
make benchmark-quick

# Full benchmarks - VibeSQL only (~2.5 hours)
make benchmark

# Full matrix - all engines (VibeSQL, SQLite, DuckDB, MySQL) (~8+ hours)
make benchmark-all

# Individual benchmark suites
make benchmark-tpch       # TPC-H decision support (22 queries)
make benchmark-tpcds      # TPC-DS decision support (99 queries)
make benchmark-tpcc       # TPC-C OLTP transactions
make benchmark-sysbench   # Sysbench micro-benchmarks
```

### Updating Website Data

After running benchmarks, export the data for the web demo:

```bash
# Export all benchmark data to web-demo/public/
make website

# This runs: python3 ./scripts/export_website_data.py
# Exports: benchmark_results.json, tpcds_results.json, tpcc_results.json,
#          sysbench_results.json, trends_results.json, dashboard.json
```

### Committing Website Updates

```bash
# After make website, commit the updated data
git add web-demo/public/benchmarks/ web-demo/public/data/
git commit -m "chore(web): Update benchmark data"
```

To deploy the website to Cloudflare, run `wrangler deploy` from the `main` branch.

### Analyzing Benchmark Results

```bash
# Show analysis of all benchmark data
make analyze

# Individual analysis
make analyze-tests        # SQLLogicTest conformance
make analyze-benchmarks   # TPC-H, TPC-DS, TPC-C, Sysbench results

<!-- BEGIN LOOM ORCHESTRATION -->
This repository uses [Loom](https://github.com/rjwalters/loom) for AI-powered development orchestration. See `.loom/CLAUDE.md` for the full guide (roles, labels, worktrees, configuration).
<!-- END LOOM ORCHESTRATION -->