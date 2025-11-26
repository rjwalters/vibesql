# Dogfooding: Benchmark Tracking

VibeSQL now tracks its own TPC-H benchmark performance using VibeSQL as the database! This extends our existing dogfooding approach (using VibeSQL for SQLLogicTest results) to include performance tracking over time.

## Overview

The benchmark tracking system stores TPC-H query performance in the same database as test results (`~/.vibesql/test_results/sqllogictest_results.vbsql`), enabling:

- **Historical performance tracking** - See how query performance changes over time
- **Regression detection** - Automatically identify queries that got slower
- **Improvement visibility** - Celebrate optimizations with measurable data
- **Performance trends** - Analyze variability and stability of queries
- **Git integration** - Link performance to specific commits and branches

## Quick Start

### Running Benchmarks

```bash
# Run TPC-H benchmarks and store results in database
make benchmark-tpch

# Or run all benchmarks with analysis
make benchmark
```

This will:
1. Run all 22 TPC-H queries with 30s timeout per query
2. Parse the benchmark output
3. Store results in the dogfooding database
4. Show analysis summary

### Viewing Results

```bash
# Show latest benchmark run
./scripts/query_benchmark_results.py --latest

# Show statistics for all queries
./scripts/query_benchmark_results.py --stats

# Show performance trend over time
./scripts/query_benchmark_results.py --trend

# Show performance regressions
./scripts/query_benchmark_results.py --regressions

# Show performance improvements
./scripts/query_benchmark_results.py --improvements

# Compare latest run to baseline
./scripts/query_benchmark_results.py --comparison

# Show all benchmark runs
./scripts/query_benchmark_results.py --history
```

### Using Make Targets

```bash
# Show test analysis
make analyze-tests

# Show benchmark analysis
make analyze-benchmarks

# Show both
make analyze
```

## Database Schema

### Tables

**`benchmark_runs`** - Metadata about each benchmark execution
- `run_id` - Unique identifier
- `timestamp` - When the benchmark was run
- `git_commit` - Short commit hash
- `git_branch` - Branch name
- `benchmark_suite` - Suite type ('tpch', 'sqllogictest_suite', 'custom')
- `timeout_secs` - Timeout used for queries
- `total_queries`, `passed_queries`, `failed_queries`, `timeout_queries` - Summary stats
- `notes` - Optional notes about the run

**`benchmark_results`** - Individual query performance measurements
- `result_id` - Unique identifier
- `run_id` - Foreign key to benchmark_runs
- `query_name` - Query identifier (e.g., 'Q1', 'Q2')
- `status` - 'passed', 'failed', 'timeout', 'error'
- `parse_time_ms` - Time to parse SQL
- `executor_creation_time_ms` - Time to create executor
- `execution_time_ms` - Time to execute query
- `total_time_ms` - Total time (parse + create + execute)
- `row_count` - Number of rows returned
- `error_message` - Error message if failed

### Views

**`latest_benchmark_summary`** - Latest benchmark run overview

**`query_performance_trend`** - Performance over time for each query with % change

**`performance_regressions`** - Queries that got slower (>10%)

**`performance_improvements`** - Queries that got faster (>10%)

**`tpch_query_stats`** - Statistics for each query across all runs (avg, min, max, variability)

**`benchmark_comparison`** - Latest run compared to baseline

## Example Queries

### Find slowest queries in latest run
```sql
SELECT query_name, execution_time_ms, row_count
FROM benchmark_results
WHERE run_id = (SELECT MAX(run_id) FROM benchmark_runs)
  AND status = 'passed'
ORDER BY execution_time_ms DESC
LIMIT 10;
```

### Track Q6 performance over time
```sql
SELECT timestamp, execution_time_ms, git_commit
FROM query_performance_trend
WHERE query_name = 'Q6'
ORDER BY timestamp;
```

### Find queries with high variability
```sql
SELECT query_name, variability_pct, avg_execution_ms
FROM tpch_query_stats
WHERE variability_pct > 20
ORDER BY variability_pct DESC;
```

### Compare performance between branches
```sql
SELECT
    br.query_name,
    main.execution_time_ms as main_time_ms,
    br.execution_time_ms as feature_time_ms,
    ROUND((br.execution_time_ms - main.execution_time_ms) * 100.0 / main.execution_time_ms, 2) as pct_change
FROM benchmark_results br
JOIN benchmark_results main ON br.query_name = main.query_name
JOIN benchmark_runs br_run ON br.run_id = br_run.run_id
JOIN benchmark_runs main_run ON main.run_id = main_run.run_id
WHERE br_run.git_branch = 'feature-xyz'
  AND main_run.git_branch = 'main'
  AND br.status = 'passed'
  AND main.status = 'passed'
ORDER BY pct_change DESC;
```

## Workflow Integration

### Development Workflow

1. **Baseline** - Run benchmarks on main branch
   ```bash
   git checkout main
   make benchmark-tpch
   ```

2. **Feature Development** - Make changes on feature branch
   ```bash
   git checkout -b feature-optimize-joins
   # ... make changes ...
   make benchmark-tpch
   ```

3. **Compare Results**
   ```bash
   ./scripts/query_benchmark_results.py --comparison
   ./scripts/query_benchmark_results.py --regressions
   ```

4. **Iterate** - If regressions found, investigate and fix
   ```bash
   # ... fix performance issues ...
   make benchmark-tpch
   ./scripts/query_benchmark_results.py --trend Q7
   ```

### CI Integration

Add to CI pipeline:

```yaml
- name: Run Benchmarks
  run: make benchmark-tpch

- name: Check for Regressions
  run: |
    ./scripts/query_benchmark_results.py --regressions > regressions.txt
    if grep -q "P0 - Critical" regressions.txt; then
      echo "Critical performance regression detected!"
      exit 1
    fi
```

## Makefile Targets

### Build Targets
- `make build` - Build all Rust crates
- `make build-wasm` - Build WebAssembly bindings
- `make build-python` - Build Python bindings

### Test Targets
- `make test` - Run all tests + analysis
- `make test-unit` - Run unit tests only
- `make test-workspace` - Run workspace tests
- `make test-sqllogictest` - Run SQLLogicTest suite

### Benchmark Targets
- `make benchmark` - Run all benchmarks + analysis
- `make benchmark-tpch` - Run TPC-H benchmarks

### Analysis Targets
- `make analyze` - Show all analysis
- `make analyze-tests` - Show test analysis
- `make analyze-benchmarks` - Show benchmark analysis

### Utility Targets
- `make clean` - Clean build artifacts
- `make all` - Build and test everything
- `make help` - Show help

## Benefits

1. **Data-Driven Optimization** - Use historical data to prioritize optimization work
2. **Regression Prevention** - Catch performance regressions before they reach production
3. **Progress Visibility** - See measurable improvements from optimization work
4. **Debugging** - Identify when performance changed and correlate with code changes
5. **Dogfooding** - VibeSQL stores and queries its own performance data

## Advanced Usage

### Custom Benchmark Runs

```bash
# Run with custom timeout
./scripts/bench-tpch.sh 60
./scripts/process_benchmark_results.py --input /tmp/tpch_results.txt --timeout 60 --notes "Testing with 60s timeout"

# Initialize schema manually
./scripts/process_benchmark_results.py --init-schema
```

### Query Specific Trends

```bash
# Track Q1 performance over time
./scripts/query_benchmark_results.py --trend Q1

# Compare Q6 to baseline
sqlite3 ~/.vibesql/test_results/sqllogictest_results.vbsql "
SELECT * FROM query_performance_trend WHERE query_name = 'Q6' ORDER BY timestamp
"
```

### Export Results

```bash
# Export to CSV
sqlite3 -csv ~/.vibesql/test_results/sqllogictest_results.vbsql "
SELECT * FROM query_performance_trend
" > performance_trend.csv

# Export to JSON
sqlite3 ~/.vibesql/test_results/sqllogictest_results.vbsql "
SELECT json_group_array(json_object(
    'query_name', query_name,
    'timestamp', timestamp,
    'execution_time_ms', execution_time_ms
)) FROM query_performance_trend
" > performance_trend.json
```

## Troubleshooting

### Database not found
```bash
# Initialize the database
make benchmark-tpch
```

### Schema issues
```bash
# Reinitialize schema
./scripts/process_benchmark_results.py --init-schema
```

### No results in queries
```bash
# Check if database has data
sqlite3 ~/.vibesql/test_results/sqllogictest_results.vbsql "SELECT COUNT(*) FROM benchmark_runs"

# Run benchmarks to populate
make benchmark-tpch
```

## See Also

- [SQLLogicTest Dogfooding](../docs/sqllogictest/SQLLOGICTEST_DATABASE.md)
- [TPC-H Benchmarking](../docs/performance/BENCHMARKING.md)
- [Performance Optimization Guide](../docs/performance/OPTIMIZATION.md)
