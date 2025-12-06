# TPC-H Benchmarking Guide

## Overview

The TPC-H benchmark suite measures decision support query performance across VibeSQL, SQLite, and DuckDB. The `scripts/bench-tpch.sh` script is the unified entry point for all TPC-H benchmark modes.

## Quick Start

```bash
# Standard benchmark run (all queries together)
./scripts/bench-tpch.sh

# Run with custom timeout
./scripts/bench-tpch.sh --timeout 60

# Run queries in isolation (prevents cascading failures)
./scripts/bench-tpch.sh --mode isolated

# Quick comparison across VibeSQL, SQLite, and DuckDB
./scripts/bench-tpch.sh --mode quick-compare
```

## Benchmark Modes

### 1. Standard Mode (Default)
```bash
./scripts/bench-tpch.sh [--timeout SECS]
```

Runs all 22 TPC-H queries sequentially in a single benchmark run.

**Features:**
- Fastest execution
- All queries in one process
- Cascading failures (one failure can affect subsequent queries)
- Output: Summary with pass/fail/timeout counts

**Use case:** Quick daily benchmarking during development sprints

**Example:**
```bash
./scripts/bench-tpch.sh --timeout 30
# Results in /tmp/tpch_results.txt
```

### 2. Isolated Mode
```bash
./scripts/bench-tpch.sh --mode isolated [--timeout SECS]
```

Runs each query in a separate subprocess to prevent cascading failures from crashes or memory issues.

**Features:**
- Each query in isolated subprocess
- Detects crashes vs timeouts independently
- Slower than standard mode but more reliable
- Output: Per-query status with crash detection

**Use case:** CI/CD pipelines, regression testing, spotting flaky queries

**Example:**
```bash
./scripts/bench-tpch.sh --mode isolated --timeout 30
# Results in /tmp/tpch_isolated_results.txt
```

### 3. Comparison Mode
```bash
./scripts/bench-tpch.sh --mode compare [--timeout SECS]
```

Full comparison across VibeSQL, SQLite, and DuckDB using Criterion benchmarking.

**Features:**
- Multiple sample runs per query per database
- Statistical comparison across implementations
- Detailed performance metrics
- Time: 10-15 minutes to complete

**Use case:** Formal performance reports, detailed analysis, publication

**Example:**
```bash
./scripts/bench-tpch.sh --mode compare
# Results in /tmp/tpch_comparison.txt
```

### 4. Quick Compare Mode
```bash
./scripts/bench-tpch.sh --mode quick-compare
```

Single-run comparison across VibeSQL, SQLite, and DuckDB (much faster).

**Features:**
- Single run per query per database
- Fast execution (1-2 minutes)
- Useful for rough comparisons
- Uses Criterion's `--test` mode

**Use case:** Quick performance sanity checks, PR validation

**Example:**
```bash
./scripts/bench-tpch.sh --mode quick-compare
# Results in /tmp/tpch_quick_comparison.txt
```

### 5. Analyze Mode
```bash
./scripts/bench-tpch.sh --mode analyze --output <file>
```

Analyze previously generated benchmark results.

**Features:**
- Processes Criterion output format
- Generates comparison tables across databases
- Calculates performance ratios
- No benchmarking required

**Use case:** Post-run analysis, converting results to human-readable format

**Example:**
```bash
./scripts/bench-tpch.sh --mode analyze --output /tmp/tpch_comparison.txt
# Outputs formatted table with ratios
```

### 6. Web Demo Mode
```bash
./scripts/bench-tpch.sh --mode web-demo --output <file.json>
```

Generates benchmark results in web demo format (JSON).

**Features:**
- Converts Criterion output to web demo format
- Includes query descriptions and metadata
- Ready for embedding in web dashboard
- Time: Same as standard mode + JSON conversion

**Use case:** Web dashboard integration, automated reporting

**Example:**
```bash
./scripts/bench-tpch.sh --mode web-demo --output benchmark_results.json
# Results in benchmark_results.json ready for web-demo
```

## Command Line Options

```
--mode MODE         Benchmark mode (default: standard)
                   Options: standard, isolated, compare, quick-compare, 
                            analyze, web-demo
--timeout SECS      Timeout per query in seconds (default: 30)
--output FILE       Output file path (varies by mode)
--help              Show this help message
```

## Output Locations

| Mode | Default Output | Format |
|------|-----------------|--------|
| standard | `/tmp/tpch_results.txt` | Text with pass/fail summary |
| isolated | `/tmp/tpch_isolated_results.txt` | Text with per-query details |
| compare | `/tmp/tpch_comparison.txt` | Criterion benchmark format |
| quick-compare | `/tmp/tpch_quick_comparison.txt` | Criterion benchmark format |
| analyze | Specified by `--output` | Formatted comparison table |
| web-demo | Specified by `--output` | JSON |

## Performance Assessment

According to `docs/performance/BENCHMARK_STRATEGY.md`:

| Ratio to SQLite | Assessment | Symbol |
|-----------------|-----------|--------|
| < 1.5x | Competitive | ✅ |
| 1.5x - 2.5x | Acceptable | ⚠️ |
| 2.5x - 5.0x | Needs improvement | ⚠️ |
| > 5.0x | Performance issue | ❌ |

## Integration with Make

The Makefile provides convenient targets:

```bash
# Run standard benchmark and process results
make benchmark-tpch

# Run all benchmarks (TPC-H, TPC-C, TPC-DS, SysBench)
make benchmark
```

## Integration with CI/CD

The GitHub Actions workflow uses web-demo mode:

```bash
python3 scripts/run_tpch_benchmarks.py --quick --output benchmark_results.json
```

For isolated testing in CI, use:

```bash
./scripts/bench-tpch.sh --mode isolated --timeout 30
```

## Troubleshooting

### Timeout too aggressive
Increase the timeout:
```bash
./scripts/bench-tpch.sh --timeout 60
```

### One query crashes all others
Use isolated mode:
```bash
./scripts/bench-tpch.sh --mode isolated
```

### Comparing with other databases
Use comparison mode:
```bash
./scripts/bench-tpch.sh --mode compare
```

### Conversion errors with web-demo format
Ensure `run_tpch_benchmarks.py` is executable and Python 3 is available:
```bash
python3 --version
./scripts/run_tpch_benchmarks.py --help
```

## Related Scripts

- `scripts/process_benchmark_results.py` - Store results in database
- `scripts/query_benchmark_results.py` - Query stored results
- `scripts/run_tpch_benchmarks.py` - Web demo format conversion (used by web-demo mode)
- `crates/vibesql-executor/benches/` - Benchmark source code

## See Also

- [Benchmark Strategy](../performance/BENCHMARK_STRATEGY.md)
- [Performance Profiling](../profiling/)
- [TPC-H Documentation](../reference/)
