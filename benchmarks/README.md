# VibeSQL Benchmarking

This directory contains comprehensive performance benchmarking tools for VibeSQL.

## Overview: Two Benchmark Systems

VibeSQL uses **two complementary benchmark systems** that measure different aspects of performance:

### 1. **TPC-H Benchmarks** (Rust Native) - *Used in CI & Web Demo*
- **Location**: `crates/vibesql-executor/benches/tpch_benchmark.rs`
- **What it measures**: Pure SQL engine performance
- **Technology**: Criterion.rs (Rust native benchmarking)
- **Databases**: VibeSQL, SQLite (rusqlite), DuckDB (duckdb-rs)
- **Purpose**: Industry-standard real-world analytical queries
- **Overhead**: Zero - native Rust APIs only
- **Run in CI**: ✅ Yes (quick mode: Q1, Q3, Q6)
- **Web Demo**: ✅ Yes - powers https://vibesql.dev/benchmarks.html

### 2. **Python Benchmarks** (this directory) - *Development Tool*
- **Location**: This directory (`benchmarks/`)
- **What it measures**: Python binding performance + PyO3 overhead
- **Technology**: pytest-benchmark
- **Purpose**: Optimize Python bindings, profile PyO3 overhead
- **Overhead**: Includes Python/FFI overhead (intentionally)
- **Run in CI**: ❌ No (run manually as needed)
- **Web Demo**: ❌ No

**Key Difference**: TPC-H benchmarks test "how fast is the SQL engine?" while Python benchmarks test "how fast is the Python API?" These are different questions!

## Directory Structure

```
benchmarks/
├── test_*.py       # Python binding benchmarks (PyO3 + SQL engine)
├── profile_*.py    # Python profiling tools
├── conftest.py     # pytest-benchmark configuration
├── requirements.txt # Python dependencies
├── utils/          # Helper utilities for benchmarks
└── suite/          # Full SQLLogicTest suite benchmarks
    ├── suite.sh    # Run all 623 test files
    ├── head-to-head.sh  # VibeSQL vs SQLite comparison
    ├── analyze.py  # Load results into database
    └── README.md   # Detailed documentation
```

## Quick Start

### TPC-H Benchmarks (Recommended - Pure SQL Engine Performance)

Run industry-standard TPC-H queries:

```bash
# Quick mode (3 queries: Q1, Q3, Q6)
python3 scripts/run_tpch_benchmarks.py --quick

# Full mode (all 22 TPC-H queries)
python3 scripts/run_tpch_benchmarks.py

# Or run directly with Criterion
cargo bench --bench tpch_benchmark --features benchmark-comparison
```

**Use this for**: Measuring SQL engine performance, comparing to other databases, web demo updates

### Python Binding Benchmarks (PyO3 Overhead Measurement)

Run Python API benchmarks:

```bash
# Install dependencies
pip install -r requirements.txt
maturin develop --release

# Run benchmarks
pytest test_aggregates.py --benchmark-only
pytest test_insert.py test_select.py --benchmark-only
```

**Use this for**: Optimizing Python bindings, measuring PyO3 overhead, Python-specific profiling

### Full Suite Benchmark

Run all 623 SQLLogicTest files:

```bash
cd suite
./suite.sh
```

Results saved to `../target/benchmarks/comparison_YYYYMMDD_HHMMSS.json`

## What to Use When

### TPC-H Benchmarks (Rust Native)

**Use for:**
- 🎯 **Primary recommendation** for SQL engine performance measurement
- Comparing VibeSQL vs SQLite vs DuckDB (apples-to-apples)
- Industry-standard real-world analytical workloads
- Web demo performance numbers
- CI/CD performance tracking
- Public benchmarking claims

**Why:**
- Zero overhead - pure SQL engine performance
- Industry-recognized benchmark suite
- All databases tested via native APIs (no language bindings)
- Realistic queries (joins, aggregations, subqueries)

**Location:** `crates/vibesql-executor/benches/tpch_benchmark.rs`

### Python Benchmarks (this directory)

**Use for:**
- Measuring Python binding overhead (PyO3)
- Optimizing Python API performance
- Finding PyO3 serialization bottlenecks
- Python-specific profiling and memory analysis
- Development of Python bindings

**Why:**
- Shows real Python user experience (including overhead)
- Helps optimize PyO3 bindings
- Profiling tools included

**Note:** These measure SQL engine + PyO3 overhead, not pure SQL performance

### Suite Benchmarks (`suite/`)

**Use for:**
- Comprehensive conformance + performance testing
- Tracking overall performance trends across 623 test files
- Pre-release performance validation

**Tools:**
- `suite.sh` - Full 623-file benchmark (VibeSQL only)
- `head-to-head.sh` - Interleaved comparison (VibeSQL vs SQLite)
- `analyze.py` - Store results in database for analysis

## Documentation

- **TPC-H benchmarks**: See [crates/vibesql-executor/benches/TPCH_README.md](../crates/vibesql-executor/benches/TPCH_README.md)
- **Suite benchmarks**: See [suite/README.md](suite/README.md)
- **General benchmarking**: See [docs/performance/BENCHMARKING.md](../docs/performance/BENCHMARKING.md)
- **Web demo**: See benchmarks at https://vibesql.dev/benchmarks.html

## Recent Performance

| Date | Total Time | Avg Time | Pass Rate | Notes |
|------|-----------|----------|-----------|-------|
| 2025-11-12 | 87.45s | 0.140s | 623/623 (100%) | After evaluator optimization |
| 2025-11-12 | 120.48s | 0.193s | 623/623 (100%) | Initial baseline |

## Measuring PyO3 Overhead

Want to know how much overhead PyO3 adds? Compare the same query in both systems:

```bash
# Python (SQL engine + PyO3 overhead)
pytest test_aggregates.py::test_sum_aggregation --benchmark-only

# Rust (pure SQL engine)
cargo bench --bench tpch_benchmark -- q1
```

**Example calculation:**
- Python: 35ms
- Rust: 25ms
- PyO3 overhead: (35 - 25) / 25 = 40% overhead

This is normal and expected for language bindings!

## Contributing

When adding benchmarks:
1. **For SQL engine performance**: Add TPC-H queries or modify `tpch_benchmark.rs`
2. **For Python binding optimization**: Add pytest benchmarks in this directory
3. **For conformance + performance**: Add to `suite/`
4. Document what you're measuring and why
5. Include baseline results in PR descriptions
