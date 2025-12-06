# SQLLogicTest Database Integration

## Overview

As part of our dogfooding initiative, VibeSQL now stores its own SQLLogicTest results in a VibeSQL database! This demonstrates real-world usage and enables powerful querying capabilities for analyzing test results.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    SQLLogicTest Runner                      │
│                  (Rust: cargo test)                         │
└────────────┬────────────────────────────────────────────────┘
             │ Results (JSON)
             ↓
┌─────────────────────────────────────────────────────────────┐
│               Python Result Processor                       │
│          (scripts/process_test_results.py)                  │
│                                                             │
│  • Reads JSON from runner                                   │
│  • Opens/creates VibeSQL database (SQL dump format)        │
│  • Inserts test_runs, test_results, updates test_files    │
│  • Exports SQL dump for web demo                           │
└────────────┬────────────────────────────────────────────────┘
             │
             ↓
┌─────────────────────────────────────────────────────────────┐
│              VibeSQL Database (dogfooding!)                │
│           target/sqllogictest_results.sql                  │
│                                                             │
│  Tables: test_files, test_runs, test_results               │
└────────────┬────────────────────────────────────────────────┘
             │
             ├─→ Query Interface → scripts/query_test_results.py
             │                     (preset queries + custom SQL)
             │
             ├─→ Punchlist → scripts/generate_punchlist.py
             │               (reads from DB, not JSON)
             │
             └─→ Web Demo → Load SQL dump and explore interactively
```

## Database Schema

The database has three main tables:

### `test_files` - Current Status Tracking
Stores the current status of each SQLLogicTest file.

```sql
CREATE TABLE test_files (
    file_path VARCHAR(500) PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    subcategory VARCHAR(50),
    status VARCHAR(20) NOT NULL,  -- 'PASS', 'FAIL', 'UNTESTED'
    last_tested TIMESTAMP,
    last_passed TIMESTAMP
);
```

### `test_runs` - Execution History
Metadata for each test execution run.

```sql
CREATE TABLE test_runs (
    run_id INTEGER PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    total_files INTEGER,
    passed INTEGER,
    failed INTEGER,
    untested INTEGER,
    git_commit VARCHAR(40),
    ci_run_id VARCHAR(100)
);
```

### `test_results` - Detailed Results
Individual test execution results with error messages.

```sql
CREATE TABLE test_results (
    result_id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL,
    file_path VARCHAR(500) NOT NULL,
    status VARCHAR(20) NOT NULL,
    tested_at TIMESTAMP NOT NULL,
    duration_ms INTEGER,
    error_message VARCHAR(2000),
    FOREIGN KEY (run_id) REFERENCES test_runs(run_id),
    FOREIGN KEY (file_path) REFERENCES test_files(file_path)
);
```

## Usage

### Running Tests (Automatic Database Update)

When you run tests using the `./scripts/sqllogictest` wrapper, results are automatically stored in the database:

```bash
# Run full test suite
./scripts/sqllogictest run --time 300

# Results automatically written to:
#   - target/sqllogictest_results.json (JSON format)
#   - target/sqllogictest_results.sql (SQL database)
```

### Querying Test Results

Use the query tool to explore your test results:

```bash
# List available preset queries
./scripts/sqllogictest query --list-presets

# Show all currently failing tests
./scripts/sqllogictest query --preset failed-files

# Show pass rate by category
./scripts/sqllogictest query --preset by-category

# Show progress over time
./scripts/sqllogictest query --preset progress

# Find flaky tests (sometimes pass, sometimes fail)
./scripts/sqllogictest query --preset flaky-tests

# Custom SQL query
./scripts/sqllogictest query --query "SELECT * FROM test_files WHERE status='FAIL' AND category='random'"
```

### Available Preset Queries

| Preset | Description |
|--------|-------------|
| `failed-files` | Show all currently failing test files |
| `progress` | Show test pass rate over recent runs |
| `by-category` | Show pass rate grouped by category |
| `flaky-tests` | Find tests that sometimes pass, sometimes fail |
| `recent-runs` | Show recent test run summary |
| `untested` | Show files that have not been tested yet |
| `slow-tests` | Show slowest running tests (when duration data available) |
| `failing-by-category` | Show failing tests grouped by category |
| `latest-failures` | Show latest test failures with error messages |

## Manual Testing Workflow

The database integration is especially useful for manual testing:

### Example: Testing Random Select Files

```bash
# 1. Run a test
./scripts/sqllogictest test random/select/slt_good_19.test

# Results automatically stored in database

# 2. Query your progress
./scripts/sqllogictest query --query "
    SELECT
        COUNT(*) as tested,
        SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed
    FROM test_files
    WHERE category='random' AND subcategory='select'
"

# 3. Find next test to work on
./scripts/sqllogictest query --query "
    SELECT file_path
    FROM test_files
    WHERE category='random'
      AND subcategory='select'
      AND status='UNTESTED'
    LIMIT 10
"

# 4. See which tests are still failing
./scripts/sqllogictest query --query "
    SELECT file_path, last_tested
    FROM test_files
    WHERE category='random'
      AND subcategory='select'
      AND status='FAIL'
    ORDER BY last_tested DESC
"
```

## Useful SQL Queries

### Current Status Summary

```sql
SELECT
    category,
    COUNT(*) as total,
    SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) as failed,
    ROUND(100.0 * SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) / COUNT(*), 1) as pass_pct
FROM test_files
GROUP BY category
ORDER BY pass_pct DESC;
```

### Progress Over Time

```sql
SELECT
    DATE(completed_at) as date,
    passed,
    failed,
    ROUND(100.0 * passed / total_files, 1) as pass_rate
FROM test_runs
ORDER BY completed_at DESC
LIMIT 10;
```

### Most Problematic Files

```sql
SELECT
    file_path,
    COUNT(*) as failure_count
FROM test_results
WHERE status = 'FAIL'
GROUP BY file_path
HAVING COUNT(*) > 5
ORDER BY failure_count DESC
LIMIT 20;
```

### Recent Failures with Error Messages

```sql
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
```

## Integration Points

### 1. Automatic Database Updates

The `./scripts/sqllogictest run` command now automatically calls `process_test_results.py` after each test run:

```bash
# After tests complete:
./scripts/process_test_results.py \
    --input target/sqllogictest_results.json \
    --database target/sqllogictest_results.sql
```

### 2. Punchlist Generation

`generate_punchlist.py` now prefers the database over JSON:

```bash
./scripts/generate_punchlist.py
# Will use target/sqllogictest_results.sql if it exists
# Otherwise falls back to JSON files
```

### 3. Web Demo

The SQL dump can be loaded into the web demo for interactive exploration:

1. Load `target/sqllogictest_results.sql` into the web demo
2. Run any of the example queries above
3. Explore test results interactively

## File Locations

| File | Purpose |
|------|---------|
| `target/sqllogictest_results.sql` | Main database (SQL dump format) |
| `target/sqllogictest_results.json` | JSON results from last serial run |
| `target/sqllogictest_cumulative.json` | JSON results from last parallel run |
| `scripts/schema/test_results.sql` | Database schema definition |
| `scripts/process_test_results.py` | Converts JSON → SQL database |
| `scripts/query_test_results.py` | Query tool with preset queries |

## Benefits

### 1. Dogfooding
We use our own database for real work, demonstrating practical usage.

### 2. Powerful Querying
SQL queries provide much more flexibility than JSON parsing.

### 3. Historical Tracking
The `test_runs` table tracks progress over time.

### 4. Failure Analysis
Join queries can find patterns in failures.

### 5. Web Demo Integration
SQL dump can be loaded into the web demo for live exploration.

### 6. Manual Testing Support
Query your progress as you work through test files.

## Implementation Details

### SQL Dump Format

We use SQL dump format (not binary) for several reasons:
- **Human-readable**: Easy to inspect and debug
- **Version control friendly**: Can diff changes over time
- **Portable**: Works across platforms
- **Web demo compatible**: Can be loaded directly into WASM demo

### Database as File

The database is stored as a SQL dump file (`target/sqllogictest_results.sql`). To query it:

1. **Via Python scripts**: Use `query_test_results.py`
2. **Via web demo**: Load the SQL dump and run queries interactively
3. **Via CLI (future)**: Once Python bindings support execution, queries will run directly

### Backward Compatibility

All scripts maintain backward compatibility with JSON:
- If database doesn't exist, they fall back to JSON
- JSON files are still generated for compatibility
- Database is an enhancement, not a replacement (yet)

## Future Enhancements

- [ ] Python bindings with direct SQL execution (no CLI needed)
- [ ] Automated flaky test detection
- [ ] Performance trend tracking (duration_ms)
- [ ] Failure pattern clustering
- [ ] GitHub Actions integration for historical tracking
- [ ] Badges generated from SQL queries

## Related Documentation

- [Database Schema](../scripts/schema/test_results.sql) - Full schema with comments
- [Persistence Plan](planning/PERSISTENCE_AND_DOGFOODING.md) - Original design doc
- [SQLLogicTest Guide](../scripts/sqllogictest) - Main testing tool

## Questions?

See the implementation in:
- `scripts/process_test_results.py` - Database writing logic
- `scripts/query_test_results.py` - Query interface
- `scripts/schema/test_results.sql` - Schema definition
