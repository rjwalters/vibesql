# SQLLogicTest Quick Start

This guide covers running and querying SQLLogicTest results using VibeSQL's integrated tooling.

## Running Tests

### Full Suite (Parallel)

```bash
# Uses all available CPU cores
./scripts/sqllogictest run --parallel

# With time limit (seconds)
./scripts/sqllogictest run --parallel --time 300
```

### Individual Tests

```bash
# Test a single file
./scripts/sqllogictest test random/select/slt_good_19.test

# Test with verbose output
./scripts/sqllogictest test index/delete/10/slt_good_0.test --verbose
```

## Querying Results

VibeSQL stores test results in its own database (dogfooding!).

### Preset Queries

```bash
# Show all failing tests
./scripts/sqllogictest query --preset failed-files

# Pass rate by category
./scripts/sqllogictest query --preset by-category

# Recent test runs
./scripts/sqllogictest query --preset recent-runs

# List all presets
./scripts/sqllogictest query --list-presets
```

### Custom Queries

```bash
./scripts/sqllogictest query --query "
    SELECT category, COUNT(*) as total
    FROM test_files
    WHERE status='FAIL'
    GROUP BY category
"
```

## Database Schema

Three tables store test data:

| Table | Purpose |
|-------|---------|
| `test_files` | Current status of each test file |
| `test_runs` | History of test runs |
| `test_results` | Detailed results with error messages |

### Example Queries

**Find tests to work on:**
```sql
SELECT file_path FROM test_files
WHERE category='random' AND subcategory='select' AND status='FAIL'
ORDER BY file_path LIMIT 10;
```

**Track progress over time:**
```sql
SELECT DATE(completed_at) as date, passed, failed,
       ROUND(100.0 * passed / total_files, 1) as pass_rate
FROM test_runs ORDER BY completed_at DESC LIMIT 10;
```

**Find flaky tests:**
```sql
SELECT file_path,
       SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passes,
       SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) as fails
FROM test_results GROUP BY file_path
HAVING passes > 0 AND fails > 0;
```

## Files

| File | Purpose |
|------|---------|
| `~/.vibesql/test_results/sqllogictest_results.vbsql` | Results database |
| `scripts/sqllogictest` | CLI tool |
| `scripts/schema/test_results.sql` | Database schema |

## Categories

```
index     214 files   (best opportunity for quick wins)
evidence   12 files   (basic language features)
random    391 files   (comprehensive coverage)
ddl         1 file    (schema operations)
other       5 files   (miscellaneous)
```

## Workflow Example

```bash
# 1. Run tests to establish baseline
./scripts/sqllogictest run --parallel --time 60

# 2. See category breakdown
./scripts/sqllogictest query --preset by-category

# 3. Find specific failing tests
./scripts/sqllogictest query --query "
    SELECT file_path FROM test_files
    WHERE category='index' AND status='FAIL' LIMIT 5"

# 4. Test one file to see the error
./scripts/sqllogictest test index/delete/10/slt_good_0.test

# 5. Fix the issue, retest
# ... make code changes ...
./scripts/sqllogictest test index/delete/10/slt_good_0.test

# 6. Run broader test to check for regressions
./scripts/sqllogictest run --parallel --time 120
```

## Related

- [Testing Strategy](../TESTING_STRATEGY.md) - Overall testing approach
- [SQL:1999 Conformance](../SQL1999_CONFORMANCE.md) - Standards compliance status
