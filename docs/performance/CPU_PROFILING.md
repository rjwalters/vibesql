# CPU Profiling Guide

This guide covers CPU profiling for VibeSQL using samply. This is essential for understanding where time is spent during query execution and identifying optimization opportunities.

## When to Use What: Decision Tree

VibeSQL has two complementary profiling approaches:
- **CPU profiling (samply)**: Shows WHERE time is spent in code
- **Semantic debug logging (env vars)**: Shows WHY the optimizer made decisions

Use this decision tree to quickly find the right tool:

```
Is the problem...
│
├─► "Query is slow, don't know why"
│   └─► make profile-tpch Q=X              (samply CPU profiling)
│
├─► "Join order seems wrong"
│   └─► JOIN_REORDER_VERBOSE=1             (shows optimizer reasoning)
│
├─► "Subquery not being optimized"
│   └─► SUBQUERY_TRANSFORM_VERBOSE=1       (shows IN/EXISTS → semi-join)
│
├─► "Index not being used"
│   └─► INDEX_SELECT_DEBUG=1               (shows selectivity analysis)
│
├─► "DELETE is slow"
│   └─► DELETE_PROFILE_VERBOSE=1           (shows phase breakdown)
│
├─► "Table not being eliminated"
│   └─► TABLE_ELIM_VERBOSE=1               (shows FK-based elimination)
│
├─► "DML choosing wrong strategy"
│   └─► DML_COST_DEBUG=1                   (shows cost model decisions)
│
├─► "Compare before/after"
│   └─► SAVE_ONLY=1 make profile-tpch Q=X  (saves for later comparison)
│
└─► "Need to profile in CI/agent"
    └─► (auto file output in non-TTY)      (samply load profile-*.json.gz)
```

### Quick Reference

| Symptom | Tool | Command |
|---------|------|---------|
| General slowness | samply | `make profile-tpch Q=X` |
| Bad join order | env var | `JOIN_REORDER_VERBOSE=1` |
| Index not used | env var | `INDEX_SELECT_DEBUG=1` |
| Subquery not optimized | env var | `SUBQUERY_TRANSFORM_VERBOSE=1` |
| Slow deletes | env var | `DELETE_PROFILE_VERBOSE=1` |
| Table not eliminated | env var | `TABLE_ELIM_VERBOSE=1` |
| DML wrong strategy | env var | `DML_COST_DEBUG=1` |
| Join execution timing | env var | `JOIN_PROFILE=1` |
| Range scan timing | env var | `RANGE_SCAN_PROFILE=1` |
| Memory issues | samply | Look for `alloc`, `Vec::push` |

### Example: Debugging a Slow Query

```bash
# Step 1: Profile with samply to find hot spots
make profile-query Q=Q13

# Step 2: If samply shows join code is hot, check optimizer decisions
JOIN_REORDER_VERBOSE=1 SCALE_FACTOR=0.01 QUERY_FILTER=Q13 \
  ./target/release/deps/tpch_profiling-* Q13

# Step 3: If index selection looks wrong
INDEX_SELECT_DEBUG=1 TABLE_SCAN_DEBUG=1 \
  ./target/release/deps/tpch_profiling-* Q13
```

## Quick Start

```bash
# Install samply (one-time setup)
cargo install samply

# Profile TPC-H queries (opens Firefox Profiler in browser)
make profile-tpch

# Profile a specific query
make profile-query Q=Q6
```

## Prerequisites

**Install samply:**
```bash
cargo install samply
```

No sudo required on macOS or Linux. Samply uses system profiling APIs that don't need elevated privileges.

## Basic Usage

### Profile TPC-H Queries

```bash
# Profile all TPC-H queries (scale factor 0.01, 3 iterations)
make profile-tpch

# Profile a specific query
make profile-query Q=Q6
make profile-query Q=Q13
```

### Profile Other Benchmarks

```bash
# Profile TPC-C transactions (30s duration, 5s warmup)
make profile-tpcc

# Profile Sysbench OLTP (10k rows, 10s duration)
make profile-sysbench

# Profile point SELECT operations
make profile-select
```

### Direct Script Usage

For more control, use the script directly:

```bash
# TPC-H with custom parameters
./scripts/flamegraph.sh tpch Q6

# TPC-C with custom duration/warmup/scale
./scripts/flamegraph.sh tpcc 60 10 2

# Sysbench with custom table size/duration/warmup
./scripts/flamegraph.sh sysbench 50000 20 5

# Profile any custom command
./scripts/flamegraph.sh custom ./target/release/my-benchmark
```

## Output Modes

The profiler automatically detects your environment:

| Environment | Default Behavior |
|------------|------------------|
| Interactive terminal (TTY) | Opens Firefox Profiler in browser |
| Non-interactive (agent/CI) | Saves to `.json.gz` file |

### Override Output Mode

```bash
# Force file output (useful for archiving)
SAVE_ONLY=1 make profile-tpch

# Force browser output (even in non-TTY)
SAVE_ONLY=0 ./scripts/flamegraph.sh tpch Q6
```

### Working with Saved Profiles

```bash
# Save profile to file
SAVE_ONLY=1 ./scripts/flamegraph.sh tpch Q6
# Creates: profile-tpch-q6.json.gz

# Load saved profile later
samply load profile-tpch-q6.json.gz
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROFILE_FREQ` | 1000 | Sampling frequency in Hz |
| `QUERY_TIMEOUT_SECS` | 30 | Timeout per query in seconds |
| `SAVE_ONLY` | auto | `1` for file output, `0` for browser |
| `DRY_RUN` | 0 | `1` to show commands without executing |

### Examples

```bash
# High-frequency sampling for short operations
PROFILE_FREQ=10000 ./scripts/flamegraph.sh tpch Q6

# Longer timeout for complex queries
QUERY_TIMEOUT_SECS=120 ./scripts/flamegraph.sh tpch Q4

# Preview what would run
DRY_RUN=1 ./scripts/flamegraph.sh tpch Q6
```

## Reading Firefox Profiler

When the profile opens in Firefox Profiler (profiler.firefox.com):

### Call Tree View
- Shows hierarchical breakdown of where time is spent
- **Self time**: Time in that function only (excluding callees)
- **Total time**: Time in function + all functions it called

### Flame Graph View
- Visual representation of the call stack
- **Width = time spent** (wider = more time)
- **Click to zoom** into specific call stacks

### Timeline View
- Shows activity over time
- Useful for identifying phases (parsing, execution, etc.)

### Common Patterns to Look For

1. **Hot loops**: Wide bars that repeat
2. **Allocation overhead**: Functions containing `alloc` or `Vec::push`
3. **Lock contention**: Functions with `mutex` or `lock`
4. **Hash operations**: `HashMap::get`, `hash` functions
5. **String operations**: `format!`, `to_string`, string comparisons

## Profiling for Agents

When running as a Loom agent or in CI, profiles are automatically saved to files:

```bash
# Agent mode (non-TTY): automatically saves to file
./scripts/flamegraph.sh tpch Q6
# Output: profile-tpch-q6.json.gz

# View the profile later in a browser
samply load profile-tpch-q6.json.gz
```

### Recommended Agent Workflow

1. **Profile the suspicious query:**
   ```bash
   ./scripts/flamegraph.sh tpch Q6
   ```

2. **Check the output file:**
   ```bash
   ls -la profile-*.json.gz
   ```

3. **Report findings** - Include the profile filename in your analysis

4. **View interactively** (human review):
   ```bash
   samply load profile-tpch-q6.json.gz
   ```

## Interpreting Results

### What's Normal

For TPC-H queries at scale factor 0.01:
- Q1 (aggregation): ~100-300ms
- Q6 (scan + filter): ~50-150ms
- Q13 (join + group): ~200-500ms

### Red Flags

| Pattern | Possible Issue |
|---------|----------------|
| >50% in `Vec::push` | Missing `with_capacity` |
| >30% in hash functions | Hash collision or bad key |
| >20% in `clone` | Unnecessary data copying |
| Lots of `Mutex::lock` | Lock contention |
| Deep recursion | Stack-heavy algorithm |

### Common Optimization Targets

1. **Filter pushdown**: Move filters earlier in the pipeline
2. **Column pruning**: Only read needed columns
3. **Batch processing**: Process rows in chunks
4. **Index utilization**: Ensure indexes are being used
5. **Memory allocation**: Pre-allocate vectors, avoid unnecessary clones

## Advanced Usage

### Profile Custom Binaries

```bash
# Profile a release build directly
./scripts/flamegraph.sh custom ./target/release/vibesql --sql "SELECT COUNT(*) FROM large_table"

# Profile with custom environment
./scripts/flamegraph.sh custom env RUST_LOG=debug ./target/release/my-test
```

### Profile with Debug Symbols

The `profiling` Cargo profile includes debug symbols for meaningful stack traces:

```toml
# In Cargo.toml
[profile.profiling]
inherits = "release"
debug = 2  # Full debug info for profiling
```

### Compare Before/After

```bash
# Profile before changes
SAVE_ONLY=1 ./scripts/flamegraph.sh tpch Q6
mv profile-tpch-q6.json.gz profile-before.json.gz

# Make changes, rebuild
cargo build --profile profiling ...

# Profile after changes
SAVE_ONLY=1 ./scripts/flamegraph.sh tpch Q6
mv profile-tpch-q6.json.gz profile-after.json.gz

# Compare visually
samply load profile-before.json.gz
# (in another terminal)
samply load profile-after.json.gz
```

## Troubleshooting

### "samply not found"

```bash
cargo install samply
```

### Profile is empty or too short

Increase the workload or decrease sampling frequency:
```bash
PROFILE_FREQ=100 ./scripts/flamegraph.sh tpch  # Lower frequency = less overhead
```

### Profile too large

Reduce sampling frequency or duration:
```bash
PROFILE_FREQ=500 QUERY_TIMEOUT_SECS=10 ./scripts/flamegraph.sh tpch Q6
```

### Missing function names

Ensure debug symbols are enabled:
```bash
# Force rebuild with profiling profile
cargo build --profile profiling --package vibesql-executor --bench tpch_profiling
```

## Built-in Debug Instrumentation

VibeSQL includes extensive built-in profiling via environment variables. These provide detailed timing and decision logging without external tools.

### Query Execution Profiling

| Variable | Description |
|----------|-------------|
| `VIBESQL_PROFILE=1` | Enable general query profiling output |
| `JOIN_PROFILE=1` | Profile join execution with timing breakdown |
| `JOIN_REORDER_VERBOSE=1` | Log join reordering decisions and costs |
| `TABLE_SCAN_DEBUG=1` | Log index vs table scan path selection |
| `COLUMNAR_DEBUG=1` | Log columnar filter optimization decisions |

### Index Operations

| Variable | Description |
|----------|-------------|
| `INDEX_SELECT_DEBUG=1` | Log index selection decisions with selectivity |
| `RANGE_SCAN_PROFILE=1` | Profile range scan timing |
| `RANGE_QUERY_BREAKDOWN=1` | Detailed range query timing |

### DML Operations

| Variable | Description |
|----------|-------------|
| `DELETE_PROFILE=1` | Collect delete timing statistics |
| `DELETE_PROFILE_VERBOSE=1` | Per-delete timing breakdown |
| `DELETE_PROFILE_SUMMARY=1` | Aggregate summary on thread exit |
| `DML_COST_DEBUG=1` | Log DML cost estimation decisions |

### Query Optimization

| Variable | Description |
|----------|-------------|
| `SUBQUERY_TRANSFORM_VERBOSE=1` | Log subquery-to-join transformations |
| `TABLE_ELIM_VERBOSE=1` | Log table elimination decisions |

### Benchmark Controls

| Variable | Description |
|----------|-------------|
| `SCALE_FACTOR=0.01` | TPC-H/TPC-DS scale factor |
| `PROFILING_ITERATIONS=3` | Number of benchmark iterations |
| `QUERY_FILTER=Q6` | Run specific query only |
| `QUERY_TIMEOUT_SECS=30` | Per-query timeout |
| `SKIP_SLOW=1` | Skip known slow queries |
| `VALIDATE=1` | Validate query results |

### Example: Debugging Join Performance

```bash
# See why a join is slow
JOIN_PROFILE=1 JOIN_REORDER_VERBOSE=1 \
  SCALE_FACTOR=0.01 QUERY_FILTER=Q13 \
  ./target/release/deps/tpch_profiling-* Q13

# Debug index selection for TPC-C
INDEX_SELECT_DEBUG=1 \
  TPCC_SCALE_FACTOR=1 TPCC_DURATION_SECS=5 \
  ./target/release/deps/tpcc_benchmark-*
```

### Example: Delete Performance Analysis

```bash
# Profile delete operations with full breakdown
DELETE_PROFILE=1 DELETE_PROFILE_VERBOSE=1 \
  cargo test delete_performance -- --nocapture
```

Output shows per-operation timing:
```
DELETE_PROFILE: total=45.2µs | pk_lookup=12.1µs (27%) | value_clone=3.2µs (7%) |
  wal=8.5µs (19%) | index_update=15.3µs (34%) | row_remove=4.8µs (11%) | cache=1.3µs (3%)
```

## See Also

- [PROFILING_GUIDE.md](PROFILING_GUIDE.md) - Python bindings profiling
- [BENCHMARK_STRATEGY.md](BENCHMARK_STRATEGY.md) - Benchmark design
- [OPTIMIZATION.md](OPTIMIZATION.md) - Optimization techniques
