# SQLLogicTest Results Database Backup

This directory contains backup snapshots of the SQLLogicTest results database.

## Files

- `sqllogictest_results-20251119-150650.sql` - Database backup capturing 100% SQLLogicTest conformance achievement (628/628 tests passing)

## Database Schema

The database tracks:
- **test_files**: Current status of each SQLLogicTest file (PASS/FAIL/UNTESTED)
- **test_runs**: Metadata for each test execution run
- **test_results**: Individual test result details with failure information

## Usage

To restore the database locally:

```bash
# Copy to local test results directory
cp test_results/sqllogictest_results-20251119-150650.sql \
   ~/.vibesql/test_results/sqllogictest_results.sql
```

To create a new backup:

```bash
scripts/backup_test_results.sh
```

## Milestone

This backup represents the achievement of 100% SQLLogicTest conformance on 2025-11-19, passing all 628 test files (~5.9M individual tests) from the official SQLite test corpus.

## Related

- Scripts: `scripts/process_test_results.py`, `scripts/generate_punchlist.py`
- Schema: `scripts/test_results_schema.sql`
- Config: `scripts/test_results_config.py`
