# Persistence and Dogfooding Workplan

## Overview

Add SQL dump persistence to VibeSQL and use the database to store its own SQLLogicTest conformance results. This demonstrates real-world usage and provides a live, queryable dataset on the demo website.

## Goals

1. **SQL Dump Persistence** - Export/import database state as human-readable SQL
2. **Self-Hosting Test Results** - Store SQLLogicTest results in VibeSQL itself
3. **Live Query Interface** - Make test results explorable on the demo website
4. **Dogfooding** - Use VibeSQL for a real, production use case

## Architecture

### 1. SQL Dump Format (Rust)

**Location**: `crates/storage/src/persistence.rs`

**Functionality**:
- `Database::save_sql_dump(path)` - Export database as SQL statements
- Format: Standard SQL CREATE TABLE + INSERT statements
- Includes: schemas, tables, data, indexes, roles
- Human-readable, version-controllable, portable

**Test Coverage**:
- Unit tests for SQL generation
- Integration tests for round-trip (export → import)
- Edge cases: NULL values, special characters, quotes

### 2. Test Results Schema

**Tables**:
```sql
-- Main test file tracking
CREATE TABLE test_files (
    file_path VARCHAR(500) PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    subcategory VARCHAR(50),
    status VARCHAR(20) NOT NULL,  -- 'PASS', 'FAIL', 'UNTESTED'
    last_tested TIMESTAMP,
    last_passed TIMESTAMP
);

-- Test run metadata
CREATE TABLE test_runs (
    run_id INTEGER PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    total_files INTEGER,
    passed INTEGER,
    failed INTEGER,
    untested INTEGER,
    git_commit VARCHAR(40),
    branch_name VARCHAR(200),
    ci_run_id VARCHAR(100)
);

-- Individual test results
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

**Useful Queries**:
```sql
-- Current status summary
SELECT
    category,
    COUNT(*) as total,
    SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN status='UNTESTED' THEN 1 ELSE 0 END) as untested
FROM test_files
GROUP BY category
ORDER BY category;

-- Progress over time
SELECT
    DATE(completed_at) as date,
    passed,
    failed,
    ROUND(100.0 * passed / total_files, 1) as pass_rate
FROM test_runs
WHERE completed_at IS NOT NULL
ORDER BY completed_at DESC
LIMIT 30;

-- Most problematic files
SELECT
    file_path,
    COUNT(*) as failure_count
FROM test_results
WHERE status = 'FAIL'
GROUP BY file_path
HAVING COUNT(*) > 5
ORDER BY failure_count DESC
LIMIT 20;

-- Recent failures
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

-- Progress by branch (branch-aware tracking)
SELECT
    branch_name,
    COUNT(*) as total_runs,
    AVG(passed * 100.0 / total_files) as avg_pass_rate,
    MAX(completed_at) as last_run
FROM test_runs
WHERE branch_name IS NOT NULL
GROUP BY branch_name
ORDER BY last_run DESC;

-- Main branch progress over time
SELECT
    run_id,
    started_at,
    passed,
    failed,
    ROUND(100.0 * passed / total_files, 1) as pass_rate
FROM test_runs
WHERE branch_name = 'main' AND completed_at IS NOT NULL
ORDER BY completed_at DESC
LIMIT 30;

-- Feature branch vs main comparison
SELECT
    'main' as branch,
    AVG(passed * 100.0 / total_files) as pass_rate
FROM test_runs
WHERE branch_name = 'main'
UNION ALL
SELECT
    branch_name as branch,
    AVG(passed * 100.0 / total_files) as pass_rate
FROM test_runs
WHERE branch_name != 'main'
GROUP BY branch_name
ORDER BY pass_rate DESC;
```

### 3. Python Integration

**File**: `scripts/generate_punchlist.py`

**Changes**:
1. Import VibeSQL Python bindings
2. Create/load test results database
3. Insert test file statuses from cumulative results
4. Run summary queries to generate punchlist
5. Export SQL dump to `target/sqllogictest_results.sql`

**Example Code**:
```python
import vibesql

# Load or create database
try:
    db = vibesql.connect()
    # Load previous state if exists
    with open('target/test_results.sql', 'r') as f:
        for statement in parse_sql_dump(f):
            db.execute(statement)
except FileNotFoundError:
    # First run, create schema
    db.execute(open('scripts/schema/test_results.sql').read())

# Insert current test results
cursor = db.cursor()
for file_path, status in test_results.items():
    cursor.execute("""
        INSERT OR REPLACE INTO test_files (file_path, category, status, last_tested)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, (file_path, category, status))

# Export for web demo
with open('target/sqllogictest_results.sql', 'w') as f:
    f.write(db.dump_sql())
```

### 4. Web Demo Integration

**Location**: `web-demo/src/App.tsx`

**Changes**:
1. Add "SQLLogicTest Results" sample database
2. Pre-load SQL dump on page load
3. Add example queries for exploring test data
4. Show summary statistics in UI

**Example Queries for Demo**:
```sql
-- Status by category
SELECT category, COUNT(*) as total,
       SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed
FROM test_files GROUP BY category;

-- Recent progress
SELECT DATE(completed_at) as date, passed, failed
FROM test_runs ORDER BY date DESC LIMIT 10;

-- Find all index tests that are failing
SELECT file_path FROM test_files
WHERE category='index' AND status='FAIL'
ORDER BY file_path LIMIT 20;
```

## Implementation Plan (TDD)

### Phase 1: SQL Dump Persistence ✅ (Partially Complete)

**Tests** (in `crates/storage/src/persistence.rs`):
- [x] `test_save_sql_dump` - Basic export functionality
- [ ] `test_sql_dump_round_trip` - Export then re-import
- [ ] `test_sql_dump_with_nulls` - NULL value handling
- [ ] `test_sql_dump_with_quotes` - String escaping
- [ ] `test_sql_dump_empty_database` - Edge case
- [ ] `test_sql_dump_with_indexes` - Index export
- [ ] `test_sql_dump_with_roles` - Role/privilege export

**Status**: Basic functionality working. Can save SQL dumps.

### Phase 2: Test Results Schema ✅ (Complete)

**Implementation**: ✅ Complete
- [x] Created `scripts/schema/test_results.sql` with full schema
- [x] Documented all tables and columns
- [x] Added example queries
- [x] Schema includes: test_files, test_runs, test_results

**Status**: Schema is complete and well-documented.

### Phase 3: Python Integration ✅ (Complete)

**Implementation**: ✅ Complete
- [x] Created `scripts/process_test_results.py` - Converts JSON to SQL
- [x] Created `scripts/query_test_results.py` - Query interface with presets
- [x] Updated `scripts/generate_punchlist.py` - Reads from database
- [x] Modified `scripts/sqllogictest` - Auto-processes results
- [x] Backward compatibility maintained (falls back to JSON)

**Status**: Core functionality complete. Database is automatically updated after test runs.

### Phase 4: Web Demo Integration (Pending)

**Status**: Ready for implementation
- [ ] Add SQL dump to web demo assets
- [ ] Load on "SQLLogicTest Results" tab
- [ ] Add example queries
- [ ] Test in browser

**Next Steps**: Copy `target/sqllogictest_results.sql` to web demo and add UI for loading/querying.

### Phase 5: CI Integration (Pending)

**Status**: Ready for implementation
- [ ] Verify SQL dump is generated in CI
- [ ] Upload SQL dump as artifact
- [ ] Deploy to GitHub Pages

**Next Steps**: Add `process_test_results.py` call to CI workflow.

## Success Criteria

- [x] `scripts/schema/test_results.sql` - Schema created and documented
- [x] `scripts/process_test_results.py` - Converts JSON → SQL database
- [x] `scripts/query_test_results.py` - Query tool with presets
- [x] `scripts/sqllogictest` - Auto-processes results after test runs
- [x] `scripts/generate_punchlist.py` - Reads from database
- [x] Backward compatibility with JSON maintained
- [x] Documentation complete (see `docs/SQLLOGICTEST_DATABASE.md`)
- [ ] `cargo test --package storage` - All persistence tests pass
- [ ] Demo website shows live SQLLogicTest results
- [ ] Users can query test data with custom SQL in web demo
- [ ] CI integration complete

## Benefits

1. **Dogfooding** - We use our own database for real work
2. **Transparency** - Test results are queryable by users
3. **Debugging** - Easy to explore failure patterns
4. **Progress Tracking** - Historical data over time
5. **Demo Value** - Real dataset, not toy examples
6. **SQL Showcase** - Complex queries demonstrate capabilities

## Timeline

- **Phase 1**: 1-2 hours (SQL dump persistence)
- **Phase 2**: 1 hour (Schema design and tests)
- **Phase 3**: 2-3 hours (Python integration)
- **Phase 4**: 1-2 hours (Web demo)
- **Phase 5**: 1 hour (CI integration)

**Total**: 6-9 hours of development time

## Open Questions

1. Should we store historical test runs or only current status?
   - **Decision**: Store both for progress tracking

2. How to handle schema migrations as we add features?
   - **Decision**: For now, recreate from scratch each time (acceptable for test data)

3. Should Python bindings support executing SQL dump files directly?
   - **Decision**: Add `db.execute_script(sql_dump)` helper method

4. What about binary persistence for performance?
   - **Decision**: Deferred - SQL dump is sufficient for now, can add later with serde

## Recent Improvements (November 2025)

### Dynamic Work Queue for Load Balancing ✅

**Problem**: Static pre-partitioning caused load imbalance - fast workers would finish early while slow workers held up the entire test suite.

**Solution**: Implemented dynamic work queue using fcntl-based file locking.
- Workers pull test files from shared queue one at a time
- Fast workers automatically process more files
- Eliminates idle time at end of test runs
- Better CPU utilization across all cores

**Implementation**: `scripts/run_parallel_tests.py` - `WorkQueue` class

### Streaming Database Writes ✅

**Problem**: Test results were only available after full suite completion, making it hard to monitor progress during long test runs.

**Solution**: Write test results to database as tests complete.
- Real-time visibility into test progress
- Results survive crashes (partial runs still captured)
- Better dogfooding - database actively used during testing
- Enables live dashboards and monitoring

**Implementation**: `scripts/run_parallel_tests.py` - `StreamingDatabaseWriter` class

**Database location**: `~/.vibesql/test_results/sqllogictest_results.sql` (shared across all worktrees)

### Branch-Aware Tracking ✅

**Problem**: Running tests from feature branches mixed results with main branch, making it hard to:
- Track overall project health (main branch)
- Debug feature branch issues
- Detect regressions across branches

**Solution**: Added `branch_name` column to `test_runs` table.
- Each test run records which git branch it was executed from
- Query results by branch while maintaining unified history
- Compare feature branch results against main baseline
- Watch for regressions when merging changes

**Schema change**: Added `branch_name VARCHAR(200)` to `test_runs` table

**Example queries**:
```sql
-- Main branch health
SELECT * FROM test_runs WHERE branch_name = 'main' ORDER BY started_at DESC LIMIT 10;

-- Feature branch debugging
SELECT * FROM test_runs WHERE branch_name = 'fix/analyze-statement' ORDER BY started_at DESC;

-- Regression detection
SELECT branch_name, AVG(passed * 100.0 / total_files) as pass_rate
FROM test_runs GROUP BY branch_name;
```

## Future Enhancements

- Binary persistence with serde (faster load/save)
- Incremental updates (don't recreate entire database)
- Schema migrations support
- Performance metrics in test_results
- Failure pattern analysis queries
- Automated issue creation from failure patterns
- Live dashboard for monitoring test progress
- Historical trend analysis per branch
