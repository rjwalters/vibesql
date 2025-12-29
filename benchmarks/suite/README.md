# VibeSQL Benchmarking

Performance benchmarking tools for the VibeSQL database engine.

## Quick Start

### Run Full Suite Benchmark

Benchmark all 623 SQLLogicTest files:

```bash
cd benchmarks/suite
./suite.sh
```

Results saved to `../../target/benchmarks/comparison_YYYYMMDD_HHMMSS.json`

### Run Head-to-Head Comparison (Coming Soon)

Compare VibeSQL vs SQLite with interleaved execution:

```bash
cd benchmarks/suite
./head-to-head.sh
```

Features:
- **Interleaved execution** - Alternates engines to control for system load
- **Adaptive repetitions** - Runs fast tests more times (targets 1s minimum)
- **Fair comparison** - Same test harness for both engines

## Tools

### `suite.sh`
Full SQLLogicTest suite benchmark (VibeSQL only)

**Usage:**
```bash
./suite.sh                                    # Run all 622 files
./suite.sh --sample 50                        # Random sample of 50 files
./suite.sh --categories "select,random"       # Specific categories
```

**Features:**
- 3 iterations per file (min/max/avg statistics)
- JSON output format
- Graceful interruption handling
- Real-time progress

### `head-to-head.sh` (In Progress)
Interleaved VibeSQL vs SQLite comparison

**Planned features:**
- Alternating engine execution (V-S-V-S-V-S...)
- Adaptive repetition count based on test speed
- Control for thermal throttling and system load
- Statistical comparison

### `analyze.py`
Load benchmark results into VibeSQL database

**Usage:**
```bash
./analyze.py ../target/benchmarks/comparison_20251112_030627.json --notes "Baseline run"
```

**Status:** Blocked by SQL limitations (requires VIEW, INDEX, AUTO_INCREMENT support)

### `schema.sql`
Database schema for storing benchmark results

Tables:
- `benchmark_runs` - Metadata about each benchmark execution
- `benchmark_results` - Per-file performance data

## Output Format

Benchmark results are saved as JSON:

```json
{
  "timestamp": "2025-11-12T03:06:27Z",
  "total_files": 623,
  "results": [
    {
      "file": "evidence/slt_lang_aggfunc.test",
      "category": "evidence",
      "vibesql": {
        "success": true,
        "runs": [0.197825, 0.198664, 0.197615],
        "min_secs": 0.197615,
        "max_secs": 0.198664,
        "avg_secs": 0.198034
      }
    }
  ]
}
```

## Analysis

### Quick Python Analysis

```python
import json

# Load results
with open('../target/benchmarks/comparison_20251112_030627.json') as f:
    data = json.load(f)

# Overall stats
times = [r['vibesql']['avg_secs'] for r in data['results'] if r['vibesql']['success']]
print(f"Average: {sum(times)/len(times):.3f}s")
print(f"Total: {sum(times):.2f}s")
print(f"Pass rate: {len(times)}/{data['total_files']}")

# Category breakdown
from collections import defaultdict
by_category = defaultdict(list)
for r in data['results']:
    if r['vibesql']['success']:
        by_category[r['category']].append(r['vibesql']['avg_secs'])

for cat, times in sorted(by_category.items()):
    avg = sum(times) / len(times)
    print(f"{cat}: {avg:.3f}s avg ({len(times)} files)")
```

### Compare Two Runs

```python
import json

# Load two runs
with open('../target/benchmarks/run1.json') as f:
    run1 = json.load(f)
with open('../target/benchmarks/run2.json') as f:
    run2 = json.load(f)

# Calculate improvement
time1 = sum(r['vibesql']['avg_secs'] for r in run1['results'] if r['vibesql']['success'])
time2 = sum(r['vibesql']['avg_secs'] for r in run2['results'] if r['vibesql']['success'])

improvement = (1 - time2/time1) * 100
print(f"Performance change: {improvement:+.1f}%")
```

### Find Slowest Files

```bash
python3 << 'EOF'
import json
with open('../target/benchmarks/comparison_20251112_030627.json') as f:
    data = json.load(f)

slowest = sorted(data['results'],
                 key=lambda x: x['vibesql'].get('avg_secs', 0),
                 reverse=True)[:10]

for r in slowest:
    print(f"{r['vibesql']['avg_secs']:.3f}s - {r['file']}")
EOF
```

## Recent Results

| Date | Total Time | Avg Time | Pass Rate | Notes |
|------|-----------|----------|-----------|-------|
| 2025-11-12 | 87.45s | 0.140s | 623/623 (100%) | After evaluator optimization |
| 2025-11-12 | 120.48s | 0.193s | 623/623 (100%) | Initial baseline |

## Benchmark Workflow

When optimizing performance:

1. **Baseline** - Run benchmark before changes
   ```bash
   ./suite.sh
   cp ../target/benchmarks/comparison_*.json baseline.json
   ```

2. **Make changes** - Implement optimization

3. **Compare** - Run benchmark after changes
   ```bash
   ./suite.sh
   # Compare with baseline using Python script above
   ```

4. **Analyze** - Identify improvements and regressions

5. **Document** - Update benchmark history

## Documentation

Full documentation: [docs/BENCHMARKING.md](../docs/BENCHMARKING.md)

Includes:
- Test-level benchmarking (individual .slt files)
- Benchmark framework API (Rust)
- Writing benchmark tests
- CI integration
- Performance goals
- Troubleshooting

## Test Infrastructure

Rust benchmark harness: [tests/sqllogictest_benchmark.rs](../tests/sqllogictest_benchmark.rs)

Provides:
- `SqliteDB` wrapper - SQLite AsyncDB implementation
- `VibeSqlDB` wrapper - VibeSQL AsyncDB implementation
- `benchmark_engine()` - Run single engine benchmark
- `compare_engines()` - Head-to-head comparison

## Next Steps

- [ ] Integrate SQLite runner into head-to-head.sh
- [ ] Implement adaptive repetition logic
- [ ] Add statistical analysis (std dev, confidence intervals)
- [ ] Create visualization dashboard
- [ ] Add CI integration for tracking trends
- [ ] Memory profiling support
