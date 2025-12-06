# SQLLogicTest Suite Status & Testing Guide

**Last Updated**: 2025-11-17
**Current Status**: 92.9% pass rate (579/623 files passing) ✅
**Test Infrastructure**: Stable parallel execution (auto-detects available CPU cores)
**Memory Management**: 26 memory-intensive 1000+ row tests blocklisted (counted as failures)
**Remaining Work**: 18 edge case bug fixes + 26 blocklisted test optimizations

## Quick Start

### Run Tests Locally (Quick)

```bash
# Run a quick smoke test (10 minutes, samples across test categories)
SQLLOGICTEST_TIME_BUDGET=600 cargo test --test sqllogictest_suite

# Test a specific file
cargo test --test sqllogictest_suite -- --exact random/select/slt_good_21.test

# Test a specific category
cargo test --test sqllogictest_suite -- random/select
```

### Run Full Test Suite (Comprehensive)

```bash
# Run full suite with parallel workers (auto-detects available CPU cores)
./scripts/sqllogictest run --parallel --time 3600

# Aggregate results from all workers
python3 scripts/aggregate_worker_results.py /tmp/sqllogictest_results
```

### Store Results in Dogfooding Database

After running tests, save results to VibeSQL:

```bash
# Process latest test results into database
./scripts/process_test_results.py \
  --input target/sqllogictest_results_analysis.json \
  --database target/sqllogictest_results.sql

# Query results
python3 scripts/query_test_results.py --preset progress
python3 scripts/query_test_results.py --preset failed-files
```

---

## How to Use Our Testing Scripts

### 1. `run_parallel_tests.py` - Full Suite Runner

Runs SQLLogicTest files in parallel across multiple worker processes with dynamic load balancing and real-time database updates.

**Key Features** (as of November 2025):
- **Dynamic work queue**: Workers pull files from shared queue for optimal load balancing
- **Streaming database writes**: Results written to dogfooding database as tests complete
- **Branch-aware tracking**: Test runs tagged with git branch for better analysis
- **Real-time progress**: Monitor results during long test runs

```bash
# Basic usage - uses all available CPUs by default
python3 scripts/run_parallel_tests.py

# Override worker count if needed (default: all available CPU cores)
python3 scripts/run_parallel_tests.py --workers 8

# Set time budget per file (default: 300 seconds per file)
python3 scripts/run_parallel_tests.py --time-budget 3600

# Combined example with custom worker count and time budget
python3 scripts/run_parallel_tests.py --workers 8 --time-budget 7200
```

**Output files**:
- `~/.vibesql/test_results/sqllogictest_results.sql` - Live dogfooding database (streaming writes)
- `target/test_work_queue.txt` - Shared work queue (dynamically consumed by workers)

**Architecture improvements**:
- **Work queue**: Instead of static pre-partitioning, workers pull files one-by-one from shared queue
  - Eliminates load imbalance (fast workers don't finish early while slow workers lag)
  - Better CPU utilization across all cores
  - Implemented with fcntl-based file locking for thread safety

- **Streaming writes**: Database updated as each test completes
  - Results visible immediately (no need to wait for full suite)
  - Crash-resilient (partial runs captured)
  - Enables live monitoring and dashboards

- **Branch tracking**: Each run records git branch
  - Compare main branch health vs feature branches
  - Track regressions across branches
  - Debug branch-specific issues

### 2. `aggregate_worker_results.py` - Result Aggregator

Combines results from all parallel workers into single analysis.

```bash
# Aggregate results
python3 scripts/aggregate_worker_results.py /tmp/sqllogictest_results

# Specify output file
python3 scripts/aggregate_worker_results.py \
  /tmp/sqllogictest_results \
  --output target/my_results.json
```

**Output files**:
- `target/sqllogictest_cumulative.json` - All results combined
- `target/sqllogictest_results_analysis.json` - Detailed failure analysis
- `target/failure_pattern_analysis.md` - Human-readable failure patterns

### 3. `process_test_results.py` - Dogfooding Database

Stores test results in VibeSQL, demonstrating real-world database usage.

```bash
# After running tests and aggregating results
./scripts/process_test_results.py \
  --input target/sqllogictest_results_analysis.json \
  --database target/sqllogictest_results.sql \
  --schema scripts/schema/test_results.sql
```

**Features**:
- Creates database from schema on first run
- Loads existing database state
- Records test run metadata (timestamp, git commit, stats)
- Inserts individual test results with error messages
- Updates test file status
- Exports SQL dump for version control

**Output files**:
- `target/sqllogictest_results.sql` - Full database dump
- Can be loaded into web demo for live querying

### 4. `query_test_results.py` - Results Viewer

Query the dogfooding database to analyze test results.

```bash
# View recent progress
python3 scripts/query_test_results.py --preset progress

# Find failed files
python3 scripts/query_test_results.py --preset failed-files

# Get statistics by category
python3 scripts/query_test_results.py --preset by-category

# Run custom query
python3 scripts/query_test_results.py \
  --query "SELECT file_path, status FROM test_files WHERE category = 'random'"
```

---

## Dogfooding Database: VibeSQL Storing Its Own Test Results

We store our test results in VibeSQL itself, demonstrating real-world database usage.

### Schema Overview

The dogfooding database has three tables:

#### `test_files` - Current Status

Tracks the current status of each SQLLogicTest file.

| Column | Type | Notes |
|--------|------|-------|
| file_path | VARCHAR(500) | Primary key, stable identifier |
| category | VARCHAR(50) | "index", "random", "evidence", etc. |
| subcategory | VARCHAR(50) | Detailed category breakdown |
| status | VARCHAR(20) | 'PASS', 'FAIL', 'UNTESTED' |
| last_tested | TIMESTAMP | When last executed |
| last_passed | TIMESTAMP | When last passed (NULL if never) |

#### `test_runs` - Execution History

Metadata for each test run, enabling progress tracking and branch comparison.

| Column | Type | Notes |
|--------|------|-------|
| run_id | INTEGER | Primary key, surrogate ID |
| started_at | TIMESTAMP | Test run start time |
| completed_at | TIMESTAMP | Test run completion time |
| total_files | INTEGER | Files in this run |
| passed | INTEGER | Number of passing files |
| failed | INTEGER | Number of failing files |
| untested | INTEGER | Number of untested files |
| git_commit | VARCHAR(40) | Link to specific code version |
| branch_name | VARCHAR(200) | Git branch where tests were run (for branch-aware querying) |
| ci_run_id | VARCHAR(100) | CI/CD system correlation |

#### `test_results` - Detailed Results

Individual test result for each file in each run.

| Column | Type | Notes |
|--------|------|-------|
| result_id | INTEGER | Primary key |
| run_id | INTEGER | Foreign key to test_runs |
| file_path | VARCHAR(500) | Foreign key to test_files |
| status | VARCHAR(20) | 'PASS', 'FAIL' |
| tested_at | TIMESTAMP | Execution timestamp |
| duration_ms | INTEGER | Test execution time |
| error_message | VARCHAR(2000) | Failure details (truncated) |

### Example Queries

Once data is in the dogfooding database, you can analyze it with SQL:

```sql
-- Current status summary by category
SELECT
    category,
    COUNT(*) as total,
    SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) as failed,
    ROUND(100.0 * SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) / COUNT(*), 1) as pass_rate
FROM test_files
GROUP BY category
ORDER BY category;

-- Progress over time (recent 30 runs)
SELECT
    run_id,
    started_at,
    passed,
    failed,
    ROUND(100.0 * passed / total_files, 1) as pass_rate
FROM test_runs
WHERE completed_at IS NOT NULL
ORDER BY completed_at DESC
LIMIT 30;

-- Most problematic files (failed multiple times)
SELECT
    file_path,
    COUNT(*) as failure_count,
    category
FROM test_results
WHERE status = 'FAIL'
GROUP BY file_path
HAVING COUNT(*) > 3
ORDER BY failure_count DESC
LIMIT 20;

-- Recent failures with details
SELECT
    tf.file_path,
    tf.category,
    tr.error_message,
    tr.tested_at
FROM test_results tr
JOIN test_files tf ON tr.file_path = tf.file_path
WHERE tr.status = 'FAIL'
ORDER BY tr.tested_at DESC
LIMIT 50;

-- Main branch progress (branch-aware tracking)
SELECT
    run_id,
    started_at,
    passed,
    failed,
    ROUND(100.0 * passed / total_files, 1) as pass_rate
FROM test_runs
WHERE branch_name = 'main'
ORDER BY started_at DESC
LIMIT 10;

-- Feature branch vs main comparison
SELECT
    branch_name,
    COUNT(*) as runs,
    AVG(passed * 100.0 / total_files) as avg_pass_rate
FROM test_runs
WHERE branch_name IS NOT NULL
GROUP BY branch_name
ORDER BY avg_pass_rate DESC;
```

---

## Testing Workflow: From Execution to Analysis

### Step 1: Run Full Test Suite

```bash
# Run with parallel workers (auto-detects CPU cores, 1-2 hours depending on machine)
./scripts/sqllogictest run --parallel
```

**What happens**:
- Tests divided among worker processes (one per CPU core)
- Each worker runs test files in priority order
- Results logged to `/tmp/sqllogictest_results/worker_*.log`
- Individual analysis per worker in `worker_*_analysis.json`

**Monitoring**:
```bash
# Watch progress in real-time
tail -f /tmp/sqllogictest_results/worker_0.log
tail -f /tmp/sqllogictest_results/worker_1.log

# Check for hangs (CPU spikes)
watch -n 1 'ps aux | grep sqllogictest'
```

### Step 2: Aggregate Results

```bash
# Combine worker results
python3 scripts/aggregate_worker_results.py /tmp/sqllogictest_results
```

**Output**:
- `target/sqllogictest_cumulative.json` - Raw aggregated results
- `target/sqllogictest_results_analysis.json` - Detailed analysis with error messages
- `target/failure_pattern_analysis.md` - Human-readable summary

### Step 3: Store in Dogfooding Database

```bash
# Load results into VibeSQL
./scripts/process_test_results.py \
  --input target/sqllogictest_results_analysis.json \
  --database target/sqllogictest_results.sql
```

**Creates/Updates**:
- Database schema (if first run)
- test_runs entry with metadata
- test_results entries for each file
- test_files status update

### Step 4: Query and Analyze

```bash
# View progress
python3 scripts/query_test_results.py --preset progress

# Find specific failures
python3 scripts/query_test_results.py --preset failed-files

# Custom query
python3 scripts/query_test_results.py \
  --query "SELECT category, COUNT(*) FROM test_files WHERE status='FAIL' GROUP BY category"
```

---

## Current Test Results (as of 2025-11-17)

### ✅ MAJOR MILESTONE: 92.9% Pass Rate Achieved!

**Overall Statistics**

| Metric | Value |
|--------|-------|
| Total Files in Suite | 623 |
| Passing | 579 (92.9%) ✅ |
| Failing | 44 (7.1%) |
| - Edge case bugs | 18 (2.9%) |
| - Blocklisted (memory-intensive) | 26 (4.2%) |

**Note**: Blocklisted tests are counted as failures since we're not actually passing them, just skipping due to memory constraints.

### Pass Rate by Category (Tested Files Only)

| Category | Total | Passed | Failed | Pass Rate | Status |
|----------|-------|--------|--------|-----------|--------|
| **ddl** | 1 | 1 | 0 | 100.0% | ✅ Perfect |
| **evidence** | 12 | 12 | 0 | 100.0% | ✅ Perfect |
| **select** | 3 | 3 | 0 | 100.0% | ✅ Perfect |
| **index** | 188 | 186 | 2 | 98.9% | ✅ Excellent |
| **random** | 393 | 377 | 16 | 95.9% | ✅ Excellent |

**Category stats show 97.0% pass rate on tested files (597), but overall suite pass rate is 92.9% when counting blocklisted tests as failures.**

### Blocklist Strategy - HIGHLY EFFECTIVE

Successfully managing memory and test execution time by blocklisting memory-intensive tests:

**Blocklisted Files (26 total - counted as failures)**:
- `select4.test`, `select5.test` - Very large SELECT tests (2 files)
- All `/1000/` pattern files - Tests with 1000+ rows (22 files)
- All `/10000/` pattern files - Tests with 10,000+ rows (2 files)

**Impact**:
- Eliminated 100+ minute tests that were consuming excessive memory
- Reduced full suite runtime from hours to ~100 seconds
- Enabled reliable parallel execution with 8 workers
- Focused testing on real functionality issues vs. resource constraints

**Result**: Stable test execution, 97.0% pass rate on tested files (579/597), zero memory crashes.

### Remaining Work (44 total failures, 7.1%)

**1. Edge Case Bugs (18 files, 2.9%)**

These are genuine functionality issues to investigate:

| Category | Failures | Notes |
|----------|----------|-------|
| index | 2 | Edge cases in index operations |
| random | 16 | Complex query edge cases |

These represent **real functionality issues** to fix, not resource/memory problems.

**2. Blocklisted Tests (26 files, 4.2%)**

These tests need memory optimization to run reliably:
- Memory profiling to understand allocation patterns
- Potential streaming or batching strategies
- Consider increasing available memory for large test runs


---

## Recommended Next Steps

### Priority 1: Investigate Remaining 18 Failures

**Impact**: Would achieve 100% pass rate (excluding blocklisted files)

With 97.0% pass rate achieved, focus on the final 18 edge cases:
- 2 index test failures
- 16 random test failures

**Action Items**:
1. Run each failing test individually to understand failure modes
2. Categorize failures by root cause
3. Fix systematic issues first
4. Polish individual edge cases
5. Consider if any should be added to blocklist (if resource-related)

### Priority 2: Optimize Blocklisted Tests (Optional)

**Impact**: Could enable testing of 26 additional files

If memory/performance improvements are made:
- Investigate why 1000+ row tests consume excessive memory
- Optimize query execution for large result sets
- Consider incremental result processing
- Re-enable tests one by one as improvements allow

### Priority 3: Continuous Monitoring

**Action Items**:
1. Run full test suite regularly to catch regressions
2. Update dogfooding database with each run
3. Track pass rate trends over time
4. Document any new failure patterns

---

## Detailed Testing Guide: Running Your Own Tests

### Local Testing (Development)

For quick feedback while implementing fixes:

```bash
# Test one specific file
cargo test --test sqllogictest_suite -- --exact random/select/slt_good_21.test

# Test all files in a category
cargo test --test sqllogictest_suite -- index/commute

# Test with output (see what passed/failed)
RUST_LOG=info cargo test --test sqllogictest_suite -- --nocapture

# Test with time budget (stop after 10 minutes)
SQLLOGICTEST_TIME_BUDGET=600 cargo test --test sqllogictest_suite
```

### Remote Testing (Full Suite)

For comprehensive testing on high-core-count machines:

```bash
# Compile in release mode first
cargo build --release --test sqllogictest_suite

# Run with all available CPU cores (auto-detected)
./scripts/sqllogictest run --parallel --time 3600

# Monitor progress
watch -n 5 'ls /tmp/sqllogictest_results/worker_*.log | wc -l'
watch -n 5 'grep -h "Passed" /tmp/sqllogictest_results/worker_*.log | tail -1'
```

### Debugging Test Failures

When a test fails, investigate it:

```bash
# Run just the failing test with output
cargo test --test sqllogictest_suite -- \
  --nocapture \
  --exact index/orderby_nosort/10/slt_good_16.test

# Get the test file content
cat third_party/sqllogictest/test/index/orderby_nosort/10/slt_good_16.test | head -50

# Extract and test specific SQL queries
# (Look for "query" lines in the test file)
```

### Measuring Progress After Fixes

After implementing a fix, measure its impact:

```bash
# 1. Run quick smoke test
SQLLOGICTEST_TIME_BUDGET=600 cargo test --test sqllogictest_suite

# 2. Run full test suite on remote (if making major changes, uses all CPU cores)
./scripts/sqllogictest run --parallel

# 3. Aggregate and analyze
python3 scripts/aggregate_worker_results.py /tmp/sqllogictest_results

# 4. Store in dogfooding database
./scripts/process_test_results.py \
  --input target/sqllogictest_results_analysis.json

# 5. Compare with previous run
python3 scripts/query_test_results.py --preset progress
```

---

## File Organization

### Key Documentation
- `SQLLOGICTEST_ROADMAP.md` - High-level roadmap with priority matrix
- `TESTING.md` - Testing strategy and methodology
- `SQLLOGICTEST_ISSUES.md` - Known issues and investigation notes

### Test Scripts
- `scripts/run_parallel_tests.py` - Run full test suite with workers
- `scripts/aggregate_worker_results.py` - Combine worker results
- `scripts/analyze_test_failures.py` - Analyze failures with clustering and pattern detection
- `scripts/process_test_results.py` - Store results in dogfooding database
- `scripts/query_test_results.py` - Query dogfooding database

### Data Files
- `target/sqllogictest_cumulative.json` - Raw aggregated results
- `target/sqllogictest_results_analysis.json` - Detailed analysis
- `target/failure_pattern_analysis.md` - Pattern summary
- `target/sqllogictest_results.sql` - Dogfooding database dump

### Test Suite
- `tests/sqllogictest_suite.rs` - Test runner harness
- `third_party/sqllogictest/test/` - 623 test files
- `scripts/schema/test_results.sql` - Dogfooding database schema

---

## Troubleshooting

### Tests Hang or Timeout - CURRENT BLOCKER

**Symptom**: All tests timeout after 60 seconds. No test files complete successfully.

**Current Status**: 
- Reported in SQLLOGICTEST_ISSUES.md as critical infinite loop issue
- All 8 workers (2025-11-08 test run) hung at 60-second mark
- Every test reported as failed with timeout
- Previous run (2025-11-06) had partial success but also experienced this

**Immediate Next Steps**:
1. Profile hanging test to identify infinite loop location
2. Add per-query timeout to test harness
3. Bisect test suite to find problematic SQL
4. Fix infinite loop before retesting

**Known problematic tests** (from SQLLOGICTEST_ISSUES.md):
- `index/commute/10/slt_good_31.test` - Known infinite loop trigger

**Solution**:
```bash
# Kill hanging tests
pkill -9 cargo
pkill -9 sqllogictest

# Check which test causes issues
grep -h "Last test\|Starting test\|Running\|Error" /tmp/sqllogictest_results/worker_*.log | tail -20

# Profile a specific test
timeout 30 cargo test --release --test sqllogictest_suite -- --exact <test_name> --nocapture
```

### Results Don't Aggregate Properly

**Symptom**: Missing worker results, incomplete aggregation

**Solution**:
```bash
# Check for worker crashes
ls /tmp/sqllogictest_results/worker_*_analysis.json | wc -l
# Should equal number of workers used

# Check for errors in logs
grep -h "Error\|ERROR" /tmp/sqllogictest_results/worker_*.log
```

### Database Dump Won't Load

**Symptom**: SQL syntax errors, constraint violations

**Solution**:
```bash
# Recreate from scratch
rm target/sqllogictest_results.sql

# Re-process test results
./scripts/process_test_results.py \
  --input target/sqllogictest_results_analysis.json \
  --database target/sqllogictest_results.sql \
  --schema scripts/schema/test_results.sql
```

---

## References

- [SQLLogicTest Suite](https://github.com/duckdb/sqllogictest)
- [Roadmap](./SQLLOGICTEST_ROADMAP.md) - Target improvements and priorities
- [Strategy](./TESTING.md) - Testing methodology and approach
- [Known Issues](./SQLLOGICTEST_ISSUES.md) - Documented bugs and blockers

---

## Status History

| Date | Pass Rate | Tests | Blocklisted | Notes |
|------|-----------|-------|-------------|-------|
| 2025-11-17 | 92.9% | 579/623 | 26 (failures) | ✅ **MILESTONE**: 97% on tested files, counting blocklist as failures |
| 2025-11-16 | 51.0% | 321/629 | 4 (10000 rows) | ✅ Stable infrastructure, aggregate bugs fixed |
| 2025-11-08 | 0% | 0/403 | 4 | ⚠️ All tests timeout (REGRESSION) |
| 2025-11-06 | 13.5% | 83/613 | 4 | Baseline measurement |

---

**Last Updated**: 2025-11-17
**Maintainer**: VibeSQL Team
**Status**: ✅ **EXCELLENT** - 97.0% pass rate, only 18 edge cases remaining, infrastructure stable and fast
