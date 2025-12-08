# CPU Profiling Guide

This guide covers CPU profiling for VibeSQL using samply. This is essential for understanding where time is spent during query execution and identifying optimization opportunities.

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

## See Also

- [PROFILING_GUIDE.md](PROFILING_GUIDE.md) - Python bindings profiling
- [BENCHMARK_STRATEGY.md](BENCHMARK_STRATEGY.md) - Benchmark design
- [OPTIMIZATION.md](OPTIMIZATION.md) - Optimization techniques
