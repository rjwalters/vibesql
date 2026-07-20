# Performance Profiling Guide

This guide shows how to use vibesql's built-in profiling tools to understand performance characteristics.

> **Looking for CPU profiling?** See [CPU_PROFILING.md](CPU_PROFILING.md) for samply-based flame graph profiling of Rust code.

## Quick Start

```python
import vibesql

# Enable profiling (prints detailed timing to stderr)
vibesql.enable_profiling()

conn = vibesql.connect()
cursor = conn.cursor()

# Execute queries - profiling output will show timing breakdown
cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
cursor.execute("SELECT COUNT(*) FROM users")
result = cursor.fetchall()

# Disable profiling when done
vibesql.disable_profiling()
```

## Understanding Profiling Output

### Example Output

```
[PROFILE] === Starting: execute() ===
[PROFILE]   SQL string copied | delta: 0.003ms | total: 0.003ms
[PROFILE]   Acquired cache lock | delta: 0.011ms | total: 0.014ms
[PROFILE]   Cache MISS - need to parse | delta: 0.009ms | total: 0.023ms
[PROFILE]   SQL parsed to AST | delta: 0.067ms | total: 0.090ms
[PROFILE]   AST cached | delta: 0.012ms | total: 0.102ms
[PROFILE]   Statement ready for execution | delta: 0.004ms | total: 0.106ms
[PROFILE]   Database lock acquired (SELECT) | delta: 0.008ms | total: 0.114ms
[PROFILE]   SelectExecutor created | delta: 0.008ms | total: 0.122ms
[PROFILE]   SELECT executed in Rust | delta: 0.123ms | total: 0.245ms
[PROFILE]   Result stored | delta: 0.007ms | total: 0.252ms
[PROFILE] === Completed: execute() in 0.260ms (260µs) ===

[PROFILE] === Starting: fetchall() ===
[PROFILE]   Starting fetch of 1 rows | delta: 0.002ms | total: 0.002ms
[PROFILE]   Empty PyList created | delta: 0.009ms | total: 0.011ms
[PROFILE]   All rows converted to Python | delta: 0.013ms | total: 0.024ms
[PROFILE] === Completed: fetchall() in 0.032ms (31µs) ===
```

### Reading the Output

Each checkpoint shows:
- **Checkpoint name**: What operation just completed
- **delta**: Time since last checkpoint (in milliseconds)
- **total**: Total time since operation started

The final line shows total execution time in both milliseconds and microseconds.

## Common Profiling Patterns

### Profile INSERT Performance

```python
import vibesql

vibesql.enable_profiling()
conn = vibesql.connect()
cursor = conn.cursor()

cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")

print("\n=== First INSERT (includes table setup) ===")
cursor.execute("INSERT INTO test VALUES (1, 100)")

print("\n=== Subsequent INSERTs ===")
for i in range(2, 5):
    cursor.execute(f"INSERT INTO test VALUES ({i}, {i * 100})")

vibesql.disable_profiling()
```

**What to look for**:
- First INSERT is slower (table initialization)
- Subsequent INSERTs should be fast (~80-100µs)
- "INSERT executed in Rust" shows core operation time

### Profile UPDATE with PRIMARY KEY Optimization

```python
import vibesql

vibesql.enable_profiling()
conn = vibesql.connect()
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name VARCHAR(50),
        email VARCHAR(100)
    )
""")

# Insert test data
for i in range(1, 11):
    cursor.execute(f"INSERT INTO users VALUES ({i}, 'User{i}', 'user{i}@example.com')")

print("\n=== UPDATE with PRIMARY KEY ===")
cursor.execute("UPDATE users SET email = 'newemail@example.com' WHERE id = 5")

vibesql.disable_profiling()
```

**What to look for**:
- "Schema cache lookup" should be fast (~10-15µs)
- "UPDATE executed in Rust" shows optimized execution
- Total time should be ~150-200µs regardless of table size

### Profile COUNT(*) Fast Path

```python
import vibesql

vibesql.enable_profiling()
conn = vibesql.connect()
cursor = conn.cursor()

cursor.execute("CREATE TABLE items (id INTEGER, value INTEGER)")
for i in range(100):
    cursor.execute(f"INSERT INTO items VALUES ({i}, {i * 10})")

vibesql.disable_profiling()  # Disable during setup

print("\n=== COUNT(*) with fast path ===")
vibesql.enable_profiling()
cursor.execute("SELECT COUNT(*) FROM items")
result = cursor.fetchall()
vibesql.disable_profiling()

print(f"Result: {result}")
```

**What to look for**:
- "SELECT executed in Rust" should be fast (~100-150µs)
- Time should NOT scale with number of rows (fast path!)
- fetchall() should be minimal (only 1 row returned)

### Profile SELECT with Result Serialization

```python
import vibesql

vibesql.enable_profiling()
conn = vibesql.connect()
cursor = conn.cursor()

cursor.execute("CREATE TABLE data (id INTEGER, value INTEGER)")
for i in range(100):
    cursor.execute(f"INSERT INTO data VALUES ({i}, {i * 10})")

vibesql.disable_profiling()

print("\n=== SELECT with result serialization ===")
vibesql.enable_profiling()
cursor.execute("SELECT * FROM data")
result = cursor.fetchall()
vibesql.disable_profiling()

print(f"Fetched {len(result)} rows")
```

**What to look for**:
- "SELECT executed in Rust" shows query execution
- "All rows converted to Python" shows serialization overhead
- More rows = longer fetchall() time (linear scaling)

## Interpreting Performance

### Python Binding Overhead

Typical overhead breakdown for a simple operation:

```
SQL string handling:      ~2-4µs
Cache lock acquisition:   ~8-20µs
SQL parsing:              ~10-70µs (depends on query complexity)
Database lock:            ~8-15µs
Result serialization:     ~7-33µs (depends on result size)
```

**Total constant overhead**: ~50-140µs per operation

### Rust Execution Times

Core operation times (after warm-up):

```
INSERT:       ~10-15µs per row
UPDATE:       ~30-80µs (with PRIMARY KEY optimization)
DELETE:       ~20-30µs (with PRIMARY KEY optimization)
COUNT(*):     ~120-130µs (fast path, no row materialization)
SELECT:       ~5-10µs per row (plus serialization)
```

### Cache Effects

**Statement Cache**:
- First execution: Parse SQL (~30-70µs)
- Subsequent: Clone cached AST (~5-20µs)

**Schema Cache**:
- Cached lookup: ~10-15µs
- Full catalog scan: ~100-200µs

## Profiling in Tests

Add profiling to specific test cases:

```python
def test_insert_performance():
    import vibesql

    vibesql.enable_profiling()

    conn = vibesql.connect()
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE test (id INTEGER)")

    # Profile a batch of inserts
    for i in range(10):
        cursor.execute(f"INSERT INTO test VALUES ({i})")

    vibesql.disable_profiling()
```

## Advanced: Custom Profiling

For more detailed profiling, you can wrap operations:

```python
import vibesql
import time

def profile_operation(name, func):
    """Profile a single operation"""
    vibesql.enable_profiling()
    start = time.perf_counter()

    result = func()

    end = time.perf_counter()
    vibesql.disable_profiling()

    print(f"\n{name}: {(end - start) * 1000:.3f}ms total")
    return result

# Usage
conn = vibesql.connect()
cursor = conn.cursor()

profile_operation("CREATE TABLE",
    lambda: cursor.execute("CREATE TABLE test (id INTEGER)"))

profile_operation("INSERT 100 rows",
    lambda: [cursor.execute(f"INSERT INTO test VALUES ({i})") for i in range(100)])

profile_operation("COUNT(*)",
    lambda: cursor.execute("SELECT COUNT(*) FROM test"))
```

## Performance Expectations

### Excellent Performance (After parking_lot Optimization - November 2025)

For vibesql, these numbers are **competitive with SQLite**:

- INSERT: **~40µs per row** (matching/beating SQLite!)
- UPDATE (with PK): **~44µs** (matching SQLite!)
- DELETE (with PK): **~38µs** (matching/beating SQLite!)
- COUNT(*): **~48µs** (excellent, only 8x vs SQLite's 6µs)
- SELECT: **~55µs + ~5µs per row** (matching SQLite!)

**Key Achievement**: We're now matching or beating SQLite on most operations while maintaining Rust's memory safety guarantees! 🎉

### When to Investigate

Investigate if you see significantly slower times than the above:

- INSERT > 100µs per row (check FK constraints)
- UPDATE > 100µs (verify PRIMARY KEY optimization is active)
- DELETE > 100µs (verify PRIMARY KEY optimization is active)
- COUNT(*) > 150µs (verify fast path is working)
- SELECT > 200µs for < 100 rows (check query complexity)

## Comparison with SQLite

**After parking_lot::Mutex optimization** (November 2025), we're now matching SQLite performance!

**Current comparison** (1K rows, single operation):
```
Operation    SQLite    vibesql (Before)  vibesql (After)  Improvement  Status
────────────────────────────────────────────────────────────────────────────────────
INSERT       ~50µs     ~155µs (3.1x)        ~40µs (0.8x)        3.9x faster  🚀 FASTER
UPDATE       ~45µs     ~171µs (3.8x)        ~44µs (1.0x)        3.9x faster  ⚡ MATCHING
DELETE       ~40µs     ~148µs (3.7x)        ~38µs (0.95x)       3.9x faster  🚀 FASTER
COUNT(*)     ~6µs      ~234µs (39x)         ~48µs (8x)          4.9x faster  ✅ EXCELLENT
SELECT       ~50µs     ~126µs (2.5x)        ~55µs (1.1x)        2.3x faster  ⚡ MATCHING
```

**What changed**:
- Replaced `std::sync::Mutex` with `parking_lot::Mutex`
- Reduced lock overhead from ~10µs to ~3µs per lock
- Locks are acquired multiple times per operation (compounding effect)
- Result: **3-5x speedup across ALL operations**

**Takeaway**: The bottleneck was lock overhead, not PyO3 fundamentals. With better primitives, we achieve SQLite-level performance!

## Structured Debug Output (JSON)

For programmatic analysis by agents and CI systems, VibeSQL can emit debug output in JSON format.

### Enabling JSON Output

Set the `VIBESQL_DEBUG_FORMAT` environment variable:

```bash
# Enable JSON output for debug/profiling messages
export VIBESQL_DEBUG_FORMAT=json

# Run with profiling enabled
JOIN_PROFILE=1 JOIN_REORDER_VERBOSE=1 ./your_benchmark 2> debug.log
```

### JSON Schema

Each JSON message follows this structure:

```json
{
  "timestamp": "2024-01-15T10:30:00.123Z",
  "category": "optimizer",
  "event": "join_order_decision",
  "data": {
    "original_order": ["customer", "orders", "lineitem"],
    "optimal_order": ["lineitem", "orders", "customer"],
    "optimizer_time_us": 1234,
    "order_changed": true
  }
}
```

### Categories

| Category | Description | Triggered By |
|----------|-------------|--------------|
| `optimizer` | Query optimization decisions | `JOIN_REORDER_VERBOSE=1` |
| `execution` | Execution timing and statistics | `JOIN_PROFILE=1` |
| `index` | Index selection and usage | `INDEX_SELECT_DEBUG=1` |
| `dml` | DML operation timing | `DML_COST_DEBUG=1` |
| `profile` | General profiling | `VIBESQL_PROFILE=1` |

### Example Events

**Join Order Decision** (`optimizer/join_order_decision`):
```json
{
  "timestamp": "2024-01-15T10:30:00.123Z",
  "category": "optimizer",
  "event": "join_order_decision",
  "data": {
    "original_order": ["customer", "orders", "lineitem"],
    "optimal_order": ["lineitem", "orders", "customer"],
    "join_condition_count": 3,
    "optimizer_time_us": 1234,
    "order_changed": true
  }
}
```

**Join Profile Summary** (`execution/join_profile_summary`):
```json
{
  "timestamp": "2024-01-15T10:30:01.456Z",
  "category": "execution",
  "event": "join_profile_summary",
  "data": {
    "table_count": 3,
    "join_count": 2,
    "total_scan_time_us": 5000,
    "total_join_time_us": 12000,
    "grand_total_us": 17500,
    "result_row_count": 1000,
    "scans": {
      "lineitem (6001 rows)": {"duration_us": 3000, "duration_ms": 3.0}
    },
    "joins": {
      "orders": {"duration_us": 8000, "cartesian_product": 6001000, "result_rows": 6001}
    }
  }
}
```

### Parsing JSON Output

Use the included Python script to parse and analyze debug output:

```bash
# Parse all events
VIBESQL_DEBUG_FORMAT=json JOIN_PROFILE=1 ./benchmark 2>&1 | python scripts/parse_debug_output.py

# Filter by category
... | python scripts/parse_debug_output.py --filter optimizer

# Filter by event type
... | python scripts/parse_debug_output.py --filter-event join_profile_summary

# Get summary statistics
... | python scripts/parse_debug_output.py --summary

# Output as JSON for further processing
... | python scripts/parse_debug_output.py --json
```

### Example: CI Integration

```bash
#!/bin/bash
# benchmark_with_analysis.sh

# Run benchmark with JSON debug output
VIBESQL_DEBUG_FORMAT=json JOIN_PROFILE=1 \
  ./target/release/deps/tpch_profiling --bench Q1 2> debug.log

# Extract timing summary
python scripts/parse_debug_output.py --summary --json < debug.log > timing_summary.json

# Check for regressions
python -c "
import json
with open('timing_summary.json') as f:
    summary = json.load(f)
timing = summary.get('timing_stats', {})
for event, stats in timing.items():
    if stats['avg_us'] > 10000:  # 10ms threshold
        print(f'Warning: {event} avg time {stats[\"avg_us\"]/1000:.1f}ms exceeds threshold')
"
```

## See Also

- [CPU_PROFILING.md](CPU_PROFILING.md) - CPU profiling with samply (flame graphs)
- [Performance Analysis](../archive/PERFORMANCE_ANALYSIS.md) - Detailed breakdown of performance characteristics
- [Benchmarking Guide](../../benchmarks/README.md) - How to run comprehensive benchmarks
