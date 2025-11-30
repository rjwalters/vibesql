# MySQL TPC-H Benchmarks

This directory contains scripts for running TPC-H benchmarks against MySQL for fair performance comparison with VibeSQL and DuckDB.

## Quick Start

```bash
# Run the benchmark (starts MySQL via Docker automatically)
./scripts/mysql-benchmark/run-benchmark.sh

# Save results to JSON
./scripts/mysql-benchmark/run-benchmark.sh --output /tmp/mysql_tpch.json
```

## Prerequisites

- **Docker**: For running MySQL 8
- **Python 3**: With `mysql-connector-python`

```bash
pip install mysql-connector-python
```

## Files

| File | Description |
|------|-------------|
| `run-benchmark.sh` | Main script - starts MySQL, loads data, runs benchmarks |
| `bench_mysql_tpch.py` | Python benchmark runner |
| `tpch_schema.sql` | MySQL TPC-H schema with indexes |
| `docker-compose.yml` | Docker Compose for MySQL 8 |

## Usage

### Basic Benchmark

```bash
./scripts/mysql-benchmark/run-benchmark.sh
```

### Custom Scale Factor

```bash
./scripts/mysql-benchmark/run-benchmark.sh --scale-factor 0.1 --iterations 5
```

### With Existing MySQL

```bash
./scripts/mysql-benchmark/run-benchmark.sh --skip-docker
```

### Direct Python Script

```bash
python3 scripts/mysql-benchmark/bench_mysql_tpch.py \
    --host 127.0.0.1 \
    --port 3306 \
    --scale-factor 0.01 \
    --iterations 3 \
    --output results.json
```

## Data Generation

The benchmark generates TPC-H data matching the exact same deterministic pattern as our Rust benchmarks:

- Uses the same random seed (42)
- Same data generation algorithms
- Same row counts per scale factor

This ensures fair comparisons between VibeSQL, DuckDB, SQLite, and MySQL.

### Scale Factor 0.01 Row Counts

| Table | Rows |
|-------|------|
| Region | 5 |
| Nation | 25 |
| Customer | 1,500 |
| Supplier | 100 |
| Part | 2,000 |
| Partsupp | 8,000 |
| Orders | 15,000 |
| Lineitem | 60,000 |

## Comparison with Other Databases

Run benchmarks for all databases and compare:

```bash
# MySQL
./scripts/mysql-benchmark/run-benchmark.sh --output /tmp/mysql.json

# VibeSQL
./scripts/bench-tpch-isolated.sh 30 /tmp/vibesql.txt

# Compare results
python3 -c "
import json
with open('/tmp/mysql.json') as f:
    mysql = json.load(f)

print('| Query | MySQL (ms) |')
print('|-------|------------|')
for q, r in sorted(mysql['queries'].items(), key=lambda x: int(x[0][1:])):
    if r['success']:
        print(f'| {q} | {r[\"time_ms\"]:.2f} |')
"
```

## Cleanup

```bash
# Stop and remove MySQL container
docker compose -f scripts/mysql-benchmark/docker-compose.yml down -v
```

## Results Format

The benchmark outputs JSON in this format:

```json
{
  "timestamp": "2025-11-29T12:00:00.000000",
  "database": "MySQL 8",
  "scale_factor": 0.01,
  "iterations": 3,
  "summary": {
    "total_queries": 22,
    "successful": 22,
    "failed": 0,
    "total_time_ms": 1234.56,
    "avg_time_ms": 56.12
  },
  "queries": {
    "Q1": {
      "success": true,
      "time_ms": 45.67,
      "min_ms": 44.12,
      "max_ms": 47.89,
      "runs": [44.12, 45.01, 47.89]
    }
  }
}
```
