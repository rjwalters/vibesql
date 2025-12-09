# VibeSQL Benchmarking Guide

This is the authoritative documentation for all benchmarking in VibeSQL. It covers architecture, how to run benchmarks, adding new benchmarks, and CI integration.

## Quick Reference

```bash
# Industry-standard SQL engine performance (recommended)
cargo bench --package vibesql-executor --bench tpch_benchmark --features benchmark-comparison

# Or via make targets (includes result processing)
make benchmark-tpch
make benchmark-tpcc
make benchmark-tpcds
make benchmark-sysbench

# Python binding performance (PyO3 overhead)
cd benchmarks && pytest test_aggregates.py --benchmark-only
```

---

## Architecture Overview

VibeSQL uses **three complementary benchmarking systems** that measure different aspects of performance:

### 1. Rust Native Benchmarks (Primary)

**Location**: `crates/vibesql-executor/benches/*.rs`  
**Technology**: Criterion.rs (Rust native benchmarking framework)  
**What it measures**: Pure SQL engine performance without any language binding overhead  
**Databases tested**: VibeSQL, SQLite (rusqlite), DuckDB (duckdb-rs)

**Use cases**:
- Industry-standard TPC-H analytical queries (22 queries on 1GB dataset)
- TPC-C OLTP workload
- TPC-DS queries
- Sysbench OLTP operations

**Key characteristics**:
- Zero overhead (native Rust APIs only)
- Apples-to-apples comparisons (all databases tested via native APIs)
- Used in CI and web demo
- High quality, reproducible results

### 2. Python Binding Benchmarks (Development Tool)

**Location**: `benchmarks/*.py`  
**Technology**: pytest-benchmark framework  
**What it measures**: Python API performance including PyO3 overhead  
**Databases tested**: VibeSQL (via Python binding)

**Use cases**:
- Measuring PyO3 language binding overhead
- Optimizing Python bindings
- Python-specific profiling and memory analysis
- Development of Python API features

**Key characteristics**:
- Includes Python/FFI overhead (intentionally)
- Not run in CI (manual development tool)
- Lower baseline performance (expected due to overhead)
- Helps quantify binding performance impact

### 3. Suite & Conformance Benchmarks

**Location**: `benchmarks/suite/`, `benchmarks/micro/`  
**Technology**: Shell scripts + Python  
**What it measures**: Performance across comprehensive test suites (623 SQLLogicTest files)

**Use cases**:
- Full conformance + performance validation
- Tracking overall performance trends
- Pre-release performance validation

---

## Benchmark Types & When to Use Each

### TPC-H Benchmarks (Rust Native)

**Primary use case**: Measuring SQL engine performance, public benchmarking claims

```bash
# Run TPC-H benchmarks
make benchmark-tpch

# Or directly with Criterion
cargo bench --package vibesql-executor --bench tpch_benchmark --features benchmark-comparison

# With specific queries
cargo bench --package vibesql-executor --bench tpch_benchmark --features benchmark-comparison -- q1 q3 q6
```

**What it tests**:
- 22 industry-standard analytical queries
- 1GB TPC-H dataset
- Complex joins, aggregations, subqueries
- Realistic OLAP workloads

**Supported database engines**:
- VibeSQL
- SQLite (via rusqlite)
- DuckDB (via duckdb-rs)

**Key metrics**:
- Query execution time (seconds)
- Results stored in benchmark database
- Trend analysis available via `make query_benchmark_results.py`

**Example output**:
```
Q1:        2.45 ±  0.12 seconds
Q3:        1.89 ±  0.08 seconds
Q6:        0.56 ±  0.03 seconds
...
```

**Web demo**: Results power https://vibesql.dev/benchmarks.html

---

### TPC-C Benchmarks (OLTP Workload)

**Primary use case**: Measuring transaction processing performance

```bash
make benchmark-tpcc
```

**What it tests**:
- 10 warehouse TPC-C workload
- Mixed OLTP operations (INSERT, UPDATE, DELETE, SELECT)
- Transaction consistency
- 60-second duration with 10-second warmup

**Supported database engines**:
- VibeSQL
- MySQL (optional, via Docker)
- DuckDB

**Requirements**:
- Docker (optional, for MySQL)
- Maturin and PyO3 (for Python bindings)

**Configuration**:
```bash
# Set via environment variables
export TPCC_SCALE_FACTOR=10      # Number of warehouses
export TPCC_DURATION_SECS=60     # Benchmark duration
export TPCC_WARMUP_SECS=10       # Warmup period
export MYSQL_URL="mysql://..."   # Optional MySQL connection
```

---

### TPC-DS Benchmarks (Complex Analytical)

**Primary use case**: Testing complex analytical queries and performance stability

```bash
# Isolated execution (each database in separate process)
make benchmark-tpcds

# All engines simultaneously (may cause memory pressure)
make benchmark-tpcds-all
```

**What it tests**:
- 24 complex TPC-DS analytical queries
- 1GB dataset
- Large intermediate result sets
- Complex window functions and aggregations

**Isolation modes**:
- **Isolated** (recommended): Each database runs separately to avoid memory pressure
- **Simultaneous** (alternative): All databases run concurrently (may OOM on smaller systems)

**Memory considerations**:
- DuckDB is memory-intensive (may require 8GB+ RAM)
- VibeSQL uses storage-backed execution (lower memory)
- SQLite is lightweight

---

### Sysbench OLTP Benchmarks

**Primary use case**: Measuring OLTP performance at scale

```bash
make benchmark-sysbench
```

**What it tests**:
- Read/Write/Mixed OLTP workloads
- 100 table dataset
- Connection pool performance
- Transaction throughput

**Supported database engines**:
- VibeSQL
- MySQL (via Docker)
- DuckDB

**Requirements**:
- Docker (for MySQL)
- Sysbench installed (`brew install sysbench`)

**Configuration**:
```bash
export SYSBENCH_TABLES=100        # Number of tables
export SYSBENCH_ROWS=10000        # Rows per table
export MYSQL_URL="mysql://..."    # MySQL connection
```

---

## How to Run Benchmarks

### Quick Reference by Task

**I want to measure SQL engine performance:**
```bash
make benchmark-tpch
```

**I want to profile a specific query:**
```bash
cargo bench --package vibesql-executor --bench tpch_benchmark --features benchmark-comparison -- q1
```

**I want to compare VibeSQL vs SQLite vs DuckDB:**
```bash
make benchmark-tpch      # Uses all three databases
make benchmark-tpcds     # Uses all three databases
```

**I want to measure Python binding overhead:**
```bash
cd benchmarks
pip install -r requirements.txt
maturin develop --release
pytest test_aggregates.py --benchmark-only
```

**I want comprehensive performance analysis:**
```bash
make benchmark        # Runs TPC-H, TPC-C, TPC-DS, Sysbench
make analyze-benchmarks  # Show all results and analysis
```

### Detailed Steps for Each Benchmark Type

#### TPC-H Benchmarks

```bash
# Build the benchmark
cargo build --package vibesql-executor --bench tpch_benchmark \
  --features benchmark-comparison --release

# Run with default settings
cargo bench --package vibesql-executor --bench tpch_benchmark \
  --features benchmark-comparison -- --noplot

# Run specific queries only
cargo bench --package vibesql-executor --bench tpch_benchmark \
  --features benchmark-comparison -- q1 q3 q6 --noplot

# Run with verbose output
cargo bench --package vibesql-executor --bench tpch_benchmark \
  --features benchmark-comparison -- --verbose

# Via make (includes result processing)
make benchmark-tpch
```

**Environment variables**:
```bash
TPCH_SCALE=1              # TPC-H scale factor (1=1GB)
TPCH_TIMEOUT_SECS=30      # Timeout per query
TPCH_SAMPLE_SIZE=5        # Number of samples
BENCHMARK_COMPARISON=true # Compare all engines
```

#### TPC-C Benchmarks

```bash
# Build the benchmark
cargo build --package vibesql-executor --bench tpcc_benchmark \
  --features benchmark-comparison --release

# Run with optional MySQL
export MYSQL_URL="mysql://root:password@localhost:3306/tpcc"
export TPCC_DURATION_SECS=60
export TPCC_WARMUP_SECS=10
export TPCC_SCALE_FACTOR=1

cargo bench --package vibesql-executor --bench tpcc_benchmark \
  --features benchmark-comparison -- --noplot

# Via make (includes Docker setup)
make benchmark-tpcc
```

#### TPC-DS Benchmarks

```bash
# Isolated execution (recommended for smaller systems)
make benchmark-tpcds

# All engines simultaneously
make benchmark-tpcds-all

# Run specific queries
cargo bench --package vibesql-executor --bench tpcds_benchmark \
  --features benchmark-comparison -- ds_1 ds_3 ds_5 --noplot
```

#### Sysbench Benchmarks

```bash
# Ensure Sysbench is installed
brew install sysbench  # macOS
apt-get install sysbench  # Linux

# Run with optional MySQL
export MYSQL_URL="mysql://root:password@localhost:3306"
export SYSBENCH_TABLES=100
export SYSBENCH_ROWS=10000

make benchmark-sysbench
```

---

## Result Storage & Querying

All benchmark results are automatically stored in SQLite databases for analysis and trend tracking.

### Database Locations

```
# SQLite database with all benchmark results
target/benchmarks.db

# Schema files
scripts/benchmark_results_schema.sql
scripts/benchmark_results_schema_vibesql.sql
```

### Stored Data

**Benchmark runs table**:
```sql
SELECT id, engine, benchmark_type, query_id, execution_time_ms, created_at
FROM benchmark_runs
WHERE engine = 'VibeSQL' AND benchmark_type = 'TPC-H'
ORDER BY created_at DESC
LIMIT 10;
```

**Trend analysis**:
```sql
SELECT 
  DATE(created_at) as run_date,
  query_id,
  AVG(execution_time_ms) as avg_time_ms,
  MIN(execution_time_ms) as min_time_ms,
  MAX(execution_time_ms) as max_time_ms
FROM benchmark_runs
WHERE engine = 'VibeSQL'
GROUP BY DATE(created_at), query_id
ORDER BY run_date DESC;
```

### Query Results

Use the provided script to query results:

```bash
# Show latest benchmark run
./scripts/query_benchmark_results.py --latest

# Show trend analysis
./scripts/query_benchmark_results.py --trend

# Show TPC-H specific results
./scripts/query_benchmark_results.py --tpch

# Show all TPC-C results
./scripts/query_benchmark_results.py --tpcc

# Show TPC-DS results
./scripts/query_benchmark_results.py --tpcds

# Show Sysbench results
./scripts/query_benchmark_results.py --sysbench

# Export to CSV
./scripts/query_benchmark_results.py --latest --format csv > results.csv
```

### Web Dashboard

Generate data for the web dashboard:

```bash
# Regenerate dashboard JSON
make website

# The output is: web-demo/public/data/dashboard.json
# View at: https://vibesql.dev/benchmarks.html
```

---

## Adding New Benchmarks

### Adding a New Rust Native Benchmark

1. **Create benchmark file** in `crates/vibesql-executor/benches/`:

```rust
// my_benchmark.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use vibesql_executor::...;

fn my_benchmark(c: &mut Criterion) {
    c.bench_function("operation_name", |b| {
        b.iter(|| {
            // Benchmark code here
            black_box(my_operation())
        })
    });
}

criterion_group!(benches, my_benchmark);
criterion_main!(benches);
```

2. **Register in `Cargo.toml`**:

```toml
[[bench]]
name = "my_benchmark"
harness = false
required-features = ["benchmark-comparison"]
```

3. **Add Makefile target** (optional):

```makefile
benchmark-my-bench:
	cargo bench --package vibesql-executor --bench my_benchmark \
	  --features benchmark-comparison -- --noplot
```

4. **Run and verify**:

```bash
cargo bench --package vibesql-executor --bench my_benchmark \
  --features benchmark-comparison
```

### Adding a Python Benchmark

1. **Create test file** in `benchmarks/`:

```python
# test_my_operation.py
import pytest
from vibesql import vibesql

@pytest.fixture
def db():
    return vibesql.connect(":memory:")

@pytest.mark.benchmark(group="my_operation")
def test_my_operation(benchmark, db):
    result = benchmark(db.execute, "SELECT ...")
    assert result is not None
```

2. **Run benchmark**:

```bash
pytest test_my_operation.py --benchmark-only
```

3. **For micro-benchmarks**, place in `benchmarks/micro/`:

```python
# test_micro_operation.py
import pytest

class BenchmarkOperations:
    @pytest.mark.benchmark
    def test_operation(self, benchmark):
        benchmark(lambda: operation())
```

### Adding Engine Support to Existing Benchmarks

To add a new database engine (e.g., PostgreSQL) to existing benchmarks:

1. **Update benchmark source**:

```rust
// In tpch_benchmark.rs
use postgres::Client;

let engines = vec![
    ("VibeSQL", run_vibesql_query),
    ("SQLite", run_sqlite_query),
    ("DuckDB", run_duckdb_query),
    ("PostgreSQL", run_postgres_query),  // NEW
];
```

2. **Update result processing** script:

```python
# In scripts/process_benchmark_results.py
SUPPORTED_ENGINES = ['VibeSQL', 'SQLite', 'DuckDB', 'PostgreSQL']
```

3. **Update test matrix documentation** (see below)

---

## Engine × Test Matrix

Current support matrix:

| Benchmark | VibeSQL | SQLite | DuckDB | MySQL | Notes |
|-----------|---------|--------|--------|-------|-------|
| **TPC-H** | ✅ Full | ✅ Full | ✅ Full | ❌ | Industry-standard analytical queries |
| **TPC-C** | ✅ Full | ❌ | ✅ Limited | ✅ Opt. | OLTP workload, MySQL via Docker |
| **TPC-DS** | ✅ Full | ❌ | ✅ Full | ❌ | Complex analytical queries |
| **Sysbench** | ✅ Full | ❌ | ✅ Limited | ✅ Opt. | OLTP at scale, MySQL via Docker |
| **Suite** | ✅ Full | ✅ Full | ❌ | ❌ | 623 SQLLogicTest files |

**Notes**:
- **MySQL**: Optional Docker container, requires Docker installation
- **Sysbench**: Requires `sysbench` CLI tool
- **Suite**: Full 623-file SQLLogicTest suite
- ❌ = Not supported
- ✅ Full = Complete support
- ✅ Limited = Partial support or configuration needed

---

## DuckDB Comparison Benchmarks

### Overview

VibeSQL supports comparison benchmarks against DuckDB for OLAP workloads. DuckDB comparison is kept in separate jobs/features because DuckDB adds ~73MB to the binary size.

### Running Comparisons Locally

```bash
# TPC-H with SQLite comparison only (default, lighter weight)
cargo bench --package vibesql-executor --bench tpch_profiling \
  --features benchmark-comparison

# TPC-H with DuckDB comparison (adds ~73MB to binary)
cargo bench --package vibesql-executor --bench tpch_profiling \
  --features benchmark-comparison,duckdb-comparison

# TPC-DS with DuckDB validation mode
VALIDATE=1 cargo bench --bench tpcds_runner \
  --features benchmark-comparison,duckdb-comparison

# Run with specific scale factor
SCALE_FACTOR=0.1 cargo bench --package vibesql-executor --bench tpch_profiling \
  --features benchmark-comparison,duckdb-comparison
```

### Feature Flags

| Feature | Description | Binary Size Impact |
|---------|-------------|-------------------|
| `benchmark-comparison` | Enable SQLite comparison + in-memory indexes | ~2MB |
| `sqlite-comparison` | SQLite only (included in benchmark-comparison) | ~2MB |
| `duckdb-comparison` | Add DuckDB comparison | ~73MB |
| `mysql-comparison` | Add MySQL comparison (requires MYSQL_URL) | ~1MB |

### CI vs Local Differences

| Aspect | Local | CI (Nightly) |
|--------|-------|--------------|
| DuckDB jobs | Optional feature flag | Separate jobs for isolation |
| MySQL | Requires Docker/server | Uses service container |
| Scale factor | Configurable | SF=0.1 (TPC-H), SF=0.01 (TPC-DS) |
| Output format | Console | JSON + text artifacts |
| Results storage | Local benchmarks.db | GitHub artifacts (30-day retention) |

### Nightly Workflow Structure

The nightly benchmark workflow (`.github/workflows/nightly-benchmarks.yml`) runs:

1. **Standard benchmarks** (SQLite comparison):
   - `tpch-full`: TPC-H with SQLite
   - `tpcds-full`: TPC-DS with SQLite

2. **DuckDB comparison jobs** (separate for binary size isolation):
   - `tpch-duckdb-comparison`: TPC-H vs DuckDB
   - `tpcds-duckdb-comparison`: TPC-DS vs DuckDB (with validation)

3. **Other benchmarks**:
   - `tpcc-extended`: TPC-C OLTP workload
   - `sysbench-extended`: Sysbench OLTP
   - `sqllogictest-complete`: Full conformance suite

### Comparison Artifacts

Nightly runs produce JSON artifacts suitable for web-demo consumption:

```bash
# Download artifacts from GitHub Actions
gh run download <run-id> --name nightly-tpch-duckdb-comparison

# Artifacts include:
# - tpch_duckdb_output.txt (raw benchmark output)
# - tpch_duckdb_comparison.json (parsed JSON for web-demo)
```

JSON format matches web-demo schema:
```json
{
  "generated_at": "2024-12-05T02:00:00Z",
  "scale_factor": 0.1,
  "source": "nightly-benchmark",
  "benchmarks": [
    {
      "name": "tpch_q1_vibesql",
      "stats": { "mean": 0.245, "status": "passed" }
    }
  ]
}
```

---

## CI Integration

### GitHub Actions Workflow

Benchmarks are integrated into the CI pipeline. See `.github/workflows/benchmarks.yml`:

```yaml
- name: Run TPC-H Benchmarks
  run: make benchmark-tpch
  if: github.event_name == 'push' && github.ref == 'refs/heads/main'

- name: Upload Results
  uses: actions/upload-artifact@v3
  with:
    name: benchmark-results
    path: target/benchmarks.db
```

### What Runs in CI

**Full benchmark suite** (main branch only):
- TPC-H: All 22 queries
- TPC-C: 60-second duration
- TPC-DS: Isolated execution
- Sysbench: 100 tables

**Quick benchmarks** (pull requests):
- TPC-H: Queries 1, 3, 6 only (30-second timeout)
- Result stored but not compared

### Performance Regression Detection

The system automatically detects performance regressions:

```bash
# Check for regressions since last main
./scripts/check_regression.py --compare-to main

# Show performance trend
./scripts/query_benchmark_results.py --trend --days 7
```

**Regression threshold**: > 10% slowdown triggers alert

**Manual regression check**:

```sql
-- Compare latest run vs previous run
SELECT 
  current.query_id,
  previous.execution_time_ms as previous_time,
  current.execution_time_ms as current_time,
  ROUND(100.0 * (current.execution_time_ms - previous.execution_time_ms) / 
    previous.execution_time_ms, 2) as percent_change
FROM benchmark_runs current
JOIN benchmark_runs previous 
  ON current.query_id = previous.query_id
WHERE current.engine = 'VibeSQL'
  AND previous.engine = 'VibeSQL'
ORDER BY ABS(percent_change) DESC;
```

---

## Troubleshooting

### Benchmark Won't Start: Missing Dependencies

**Error**: `cargo bench: command not found` or `criterion not found`

**Solution**: Ensure you're using the correct Cargo command and features:

```bash
# Correct way
cargo bench --package vibesql-executor --bench tpch_benchmark \
  --features benchmark-comparison --release

# Make sure Criterion is in Cargo.toml
cat crates/vibesql-executor/Cargo.toml | grep criterion
```

### Out of Memory During TPC-DS

**Error**: Process killed or "memory allocation failed"

**Solution**: Use isolated execution instead of simultaneous:

```bash
# Isolated (recommended)
make benchmark-tpcds

# NOT simultaneous (may OOM)
# make benchmark-tpcds-all
```

If OOM persists, reduce the TPC-DS scale factor:

```bash
export TPCDS_SCALE=0.5  # 500MB instead of 1GB
make benchmark-tpcds
```

### MySQL Connection Failed

**Error**: `MySQL connection refused` or `Error: connect ECONNREFUSED`

**Solution**: MySQL support is optional. To enable it:

```bash
# Docker must be running
docker ps

# Start MySQL manually if auto-start failed
docker run --name vibesql-mysql -e MYSQL_ROOT_PASSWORD=password \
  -p 3306:3306 -d mysql:8.0

# Or use the helper script
./scripts/ensure-mysql-docker.sh
```

To skip MySQL and benchmark without it:

```bash
unset MYSQL_URL
make benchmark-tpcc  # Will run without MySQL
```

### Benchmark Results Not Saved

**Error**: "No results database found" or "results.db is empty"

**Solution**: Ensure the scripts are processing results:

```bash
# Run benchmark with explicit result processing
make benchmark-tpch

# Check if database was created
ls -lah target/benchmarks.db

# Verify results were inserted
sqlite3 target/benchmarks.db "SELECT COUNT(*) FROM benchmark_runs;"
```

### Sysbench Command Not Found

**Error**: `sysbench: command not found`

**Solution**: Install sysbench:

```bash
# macOS
brew install sysbench

# Linux (Debian/Ubuntu)
apt-get install sysbench

# Linux (Fedora/RHEL)
dnf install sysbench

# Verify installation
sysbench --version
```

### Performance Results Look Wrong

**Checklist**:
1. System load too high?
   ```bash
   # Check system load
   top -l 1 | grep -E "Load Average:|CPU usage"
   ```

2. Thermal throttling?
   ```bash
   # Monitor temperature during benchmark
   watch -n 1 "powermetrics | grep 'CPU die temperature'"
   ```

3. Power management interference?
   ```bash
   # Disable sleep during benchmark
   caffeinate make benchmark-tpch
   ```

4. Wrong scale factor?
   ```bash
   echo $TPCH_SCALE     # Should be 1 for 1GB
   echo $TPCDS_SCALE    # Should be 1 for 1GB
   ```

---

## Performance Best Practices

### For Consistent Results

1. **Close other applications** during benchmarks
2. **Disable power management**:
   ```bash
   sudo pmset -a sleep 0  # macOS
   sudo systemctl mask sleep.target suspend.target  # Linux
   ```

3. **Run multiple times** and report averages
4. **Use the same machine** for trending
5. **Run at similar times** (thermal conditions matter)

### For Public Benchmarking Claims

1. **Always use Rust native benchmarks** (TPC-H, TPC-C, TPC-DS)
2. **Never use Python benchmarks** for public claims (includes PyO3 overhead)
3. **Test all engines** fairly with the same dataset size
4. **Report error bars** (Criterion provides ± values)
5. **Document your system** (CPU, RAM, OS, JVM settings if applicable)
6. **Include timestamp** (performance changes over time)

**Example claim**:
> VibeSQL processes TPC-H Q1 in 9.0 ± 0.1 ms on SF 0.01, compared to SQLite's 32.6 ms (3.6x faster), on an M1 MacBook Pro, December 2025.

### For Development Iteration

1. **Use quick mode first**: Run 3 queries before 22 queries
2. **Profile hot spots**: Use flamegraph targets
3. **Compare before/after**: Track improvements
4. **Measure in isolation**: One change at a time

---

## Related Documentation

- **Dogfooding Benchmarks**: [docs/development/DOGFOODING_BENCHMARKS.md](./DOGFOODING_BENCHMARKS.md) - Internal performance tracking
- **Miri Testing**: [docs/development/MIRI.md](./MIRI.md) - Undefined behavior detection
- **TPC-H Details**: [crates/vibesql-executor/benches/TPCH_README.md](../../crates/vibesql-executor/benches/TPCH_README.md)
- **Python Benchmarks**: [benchmarks/README.md](../../benchmarks/README.md) - PyO3 overhead measurement
- **Suite Benchmarks**: [benchmarks/suite/README.md](../../benchmarks/suite/README.md) - Conformance testing

---

## Benchmark Methodology

This section documents how benchmarks are conducted for fair and reproducible results.

### Test Environment Standards

For official benchmark results (published in README, web demo, etc.):

| Parameter | Standard Value | Notes |
|-----------|----------------|-------|
| CPU | Apple M-series or x86_64 | Document specific model |
| RAM | 16GB+ | Sufficient for TPC-DS at SF=1 |
| Storage | SSD | Avoid HDDs for benchmarks |
| OS | macOS or Linux | Windows support varies |
| Background processes | Minimal | Close other applications |
| Power mode | High performance | Disable power saving |

### Scale Factors

Each benchmark supports multiple scale factors. Use these guidelines:

| Benchmark | Development | CI/Quick | Full Run | Production |
|-----------|-------------|----------|----------|------------|
| TPC-H | 0.001 | 0.01 | 0.1 | 1.0 |
| TPC-C | 1 warehouse | 1 warehouse | 1 warehouse | 10 warehouses |
| TPC-DS | 0.001 | 0.001 | 0.01 | 1.0 |
| Sysbench | 100 rows | 1000 rows | 10000 rows | 100000 rows |

### Measurement Protocol

1. **Warmup phase**: Always include warmup runs
   - TPC-C: 10 seconds warmup before measurement
   - TPC-H/TPC-DS: First query execution discarded

2. **Iteration count**: Multiple runs for statistical validity
   - TPC-H: 3-5 iterations per query
   - TPC-C: 60-second continuous measurement
   - Criterion benchmarks: Automatic statistical sampling

3. **Reporting**: Include confidence intervals
   - Report mean ± standard deviation
   - Note outliers explicitly
   - Document any anomalies

### Comparison Fairness

When comparing databases:

1. **Same scale factor** for all engines
2. **Same hardware** for all measurements
3. **Native APIs** only (no binding overhead)
4. **Cold start** measurements (restart between engine tests)
5. **Same query execution** (identical SQL where possible)

### JSON Export for CI

Use the unified bench CLI to export results for CI integration:

```bash
# Export results to JSON
./scripts/bench --all --json --output results.json

# Quick benchmark with JSON output for CI
./scripts/bench --quick --json --output ci_results.json
```

JSON output format:

```json
{
  "metadata": {
    "timestamp": "2024-12-05T12:00:00",
    "git_commit": "abc123",
    "git_branch": "main",
    "config": { "engines": ["vibesql"], "scale": 0.01 }
  },
  "benchmarks": {
    "tpch": { "status": "success", "duration_secs": 45.2 },
    "tpcc": { "status": "success", "tps": 75404, "duration_secs": 60 }
  }
}
```

### Reproducibility Checklist

Before publishing benchmark results:

- [ ] Document exact hardware specifications
- [ ] Record OS version and kernel
- [ ] Note Rust/compiler version
- [ ] Include git commit hash
- [ ] List any non-default settings
- [ ] Run at least 3 full iterations
- [ ] Verify results are within expected variance
- [ ] Compare against previous baseline

---

## Quick Links

- **Web Demo**: https://vibesql.dev/benchmarks.html
- **Benchmark Results Database**: `target/benchmarks.db`
- **CI Pipeline**: `.github/workflows/benchmarks.yml`
- **Makefile Targets**: `Makefile` (search for `benchmark`)
- **Result Scripts**: `scripts/query_benchmark_results.py`
- **JSON Export**: `./scripts/bench --json`
