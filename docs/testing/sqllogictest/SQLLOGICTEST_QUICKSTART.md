# SQLLogicTest Database - Quick Start

## 🎯 What is this?

VibeSQL now stores its own test results in a VibeSQL database! This is **dogfooding** - using our own product to solve a real problem.

## ⚡ Quick Start (2 minutes)

### 1. Run tests (all 622 files)

```bash
# Uses all available CPU cores by default
./scripts/sqllogictest run --parallel
```

Output:
```
✓ Test binary compiled
✓ Work queue initialized: 622 files
All workers started! Waiting for completion...

=== All Workers Complete ===
Total time: 105.8s
Passed: 24, Failed: 598

✓ Database updated: target/sqllogictest_results.sql

Query your results:
  ./scripts/sqllogictest query --preset failed-files
  ./scripts/sqllogictest query --preset by-category
```

### 2. Query your results

```bash
# Show all failing tests
./scripts/sqllogictest query --preset failed-files

# Show pass rate by category
./scripts/sqllogictest query --preset by-category

# See available queries
./scripts/sqllogictest query --list-presets
```

### 3. Write custom queries

```bash
./scripts/sqllogictest query --query "
    SELECT category, COUNT(*) as total
    FROM test_files
    WHERE status='FAIL'
    GROUP BY category
"
```

## 📊 What's in the database?

Three tables:

1. **`test_files`** - Current status of each test file
   - file_path, category, status (PASS/FAIL/UNTESTED)
   - last_tested, last_passed timestamps

2. **`test_runs`** - History of test runs
   - run_id, started_at, completed_at
   - total_files, passed, failed
   - git_commit

3. **`test_results`** - Detailed results with errors
   - run_id, file_path, status
   - error_message, duration_ms

## 🔍 Example Queries

### Find tests to work on

```sql
SELECT file_path
FROM test_files
WHERE category='random'
  AND subcategory='select'
  AND status='FAIL'
ORDER BY file_path
LIMIT 10;
```

### Track progress over time

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

### Find flaky tests

```sql
SELECT
    file_path,
    SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passes,
    SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) as fails
FROM test_results
GROUP BY file_path
HAVING passes > 0 AND fails > 0;
```

## 🎨 Preset Queries

Use `--preset` for common queries:

| Preset | What it shows |
|--------|---------------|
| `failed-files` | All currently failing tests |
| `by-category` | Pass rate by category |
| `progress` | Pass rate over time |
| `flaky-tests` | Tests that sometimes fail |
| `recent-runs` | Recent test run summary |
| `untested` | Files not tested yet |
| `latest-failures` | Recent failures with errors |

## 🚀 Manual Testing Workflow

Working through random/select tests? Track your progress:

```bash
# 1. Test a file
./scripts/sqllogictest test random/select/slt_good_19.test

# 2. See your progress
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
    LIMIT 5
"
```

## 📁 Files

| File | What |
|------|------|
| `target/sqllogictest_results.sql` | Database (SQL dump) |
| `target/sqllogictest_results.json` | JSON results (backup) |
| `scripts/process_test_results.py` | JSON → SQL converter |
| `scripts/query_test_results.py` | Query tool |
| `scripts/schema/test_results.sql` | Database schema |

## 💡 Why SQL instead of JSON?

**JSON** (before):
```bash
# Hard to query
cat target/results.json | jq '.detailed_failures[] | select(.file_path | contains("random"))'
```

**SQL** (now):
```bash
# Easy to query
./scripts/sqllogictest query --query "
    SELECT file_path FROM test_files
    WHERE status='FAIL' AND category='random'
"
```

Plus:
- ✅ More powerful queries (JOINs, GROUP BY, etc.)
- ✅ Historical tracking (test_runs over time)
- ✅ Dogfooding (we use our own database!)
- ✅ Web demo ready (load SQL dump and explore)

## 📖 Full Documentation

See [SQLLOGICTEST_DATABASE.md](SQLLOGICTEST_DATABASE.md) for complete documentation including:
- Database schema details
- All preset queries
- Integration with web demo
- CI/CD integration
- Custom query examples

## 🎯 Next Steps

1. **Run tests**: `./scripts/sqllogictest run --time 60`
2. **Explore results**: `./scripts/sqllogictest query --preset by-category`
3. **Write queries**: Try the examples above!
4. **Read more**: Check out the full docs

Happy testing! 🚀
