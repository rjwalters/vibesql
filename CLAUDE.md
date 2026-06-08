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

### Exporting Results

```bash
# Export TCL results to website format
python3 scripts/export_tcl_results.py --verbose
```

## Benchmarking and Website Updates

VibeSQL uses a dogfooded SQLite-compatible database (`~/.vibesql/test_results/benchmark_results.vbsql`) to store all benchmark results. The web demo at https://rjwalters.github.io/vibesql/ displays this data.

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
```

<!-- BEGIN LOOM ORCHESTRATION -->
This repository uses [Loom](https://github.com/rjwalters/loom) for AI-powered development orchestration. See `.loom/CLAUDE.md` for the full guide (roles, labels, worktrees, configuration).
<!-- END LOOM ORCHESTRATION -->
