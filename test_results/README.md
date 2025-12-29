# Test Results Database Backups

This directory contains backup snapshots of the VibeSQL test results databases.

## Databases

### SQLLogicTest Results
- **Format**: `.vbsql` (VibeSQL native) or `.sql` (legacy SQLite)
- **Contents**: Test file status, run metadata, individual test results
- **Milestone**: 100% conformance achieved 2025-11-19 (622/622 test files, ~7.4M tests)

### Benchmark Results
- **Format**: `.db` (SQLite)
- **Contents**: TPC-H, TPC-C, TPC-DS, and Sysbench benchmark results
- **Tables**: `benchmark_runs`, `benchmark_results`, `tpcc_results`, `sysbench_results`
- **Views**: Performance trends, regressions, engine comparisons

## Usage

### Create a new backup

```bash
scripts/backup_test_results.sh
```

This backs up both databases and keeps the 5 most recent backups of each type.

### Restore databases locally

```bash
# Restore SQLLogicTest results
cp test_results/sqllogictest_results-TIMESTAMP.vbsql \
   ~/.vibesql/test_results/sqllogictest_results.vbsql

# Restore benchmark results
cp test_results/benchmark_results-TIMESTAMP.db \
   ~/.vibesql/test_results/benchmark_results.db
```

### Query benchmark results

```bash
# View latest benchmark summary
./scripts/query_benchmark_results.py --latest

# View performance trends
./scripts/query_benchmark_results.py --trend

# View TPC-C results
./scripts/query_benchmark_results.py --tpcc

# View TPC-DS results
./scripts/query_benchmark_results.py --tpcds

# View Sysbench results
./scripts/query_benchmark_results.py --sysbench
```

## Related Scripts

- `scripts/backup_test_results.sh` - Create backups
- `scripts/process_benchmark_results.py` - Process raw benchmark output
- `scripts/query_benchmark_results.py` - Query and display results
- `scripts/process_test_results.py` - Process SQLLogicTest results
- `scripts/generate_punchlist.py` - Generate test failure reports

## Schema

### benchmark_runs
| Column | Type | Description |
|--------|------|-------------|
| run_id | INTEGER | Primary key |
| timestamp | TEXT | ISO 8601 timestamp |
| git_commit | TEXT | Short commit hash |
| git_branch | TEXT | Branch name |
| benchmark_suite | TEXT | tpch, tpcc, tpcds, sysbench |
| scale_factor | TEXT | Dataset scale |
| total_queries | INTEGER | Number of queries run |
| passed_queries | INTEGER | Queries that passed |

### benchmark_results
| Column | Type | Description |
|--------|------|-------------|
| result_id | INTEGER | Primary key |
| run_id | INTEGER | Foreign key to benchmark_runs |
| query_name | TEXT | e.g., Q1, Q2 |
| status | TEXT | passed, failed, timeout, error |
| execution_time_ms | REAL | Query execution time |
| total_time_ms | REAL | Total time including parsing |

### tpcc_results
| Column | Type | Description |
|--------|------|-------------|
| result_id | INTEGER | Primary key |
| run_id | INTEGER | Foreign key to benchmark_runs |
| database_engine | TEXT | vibesql, sqlite, duckdb |
| transaction_type | TEXT | new_order, payment, etc. |
| transactions_per_second | REAL | TPS metric |

### sysbench_results
| Column | Type | Description |
|--------|------|-------------|
| result_id | INTEGER | Primary key |
| run_id | INTEGER | Foreign key to benchmark_runs |
| database_engine | TEXT | vibesql, sqlite, duckdb |
| test_name | TEXT | point_select, insert, read_write |
| mean_time_ns | REAL | Average operation time |
