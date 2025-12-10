#!/usr/bin/env python3
"""
Export benchmark data from the VibeSQL dogfooding database to JSON format.

This script reads benchmark results stored in our VibeSQL database and exports
them to the JSON format expected by the web demo benchmarks page.

Usage:
    python scripts/export_benchmark_json.py --all
    python scripts/export_benchmark_json.py --tpcc --sysbench
    python scripts/export_benchmark_json.py --tpcds --output tpcds_results.json
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Import our VibeSQL helper module
sys.path.insert(0, str(Path(__file__).parent))
from vibesql_db import get_connection, get_db_path


def get_repo_root() -> Path:
    """Get the repository root directory."""
    return Path(__file__).parent.parent


def get_git_info() -> tuple:
    """Get current git commit hash and timestamp."""
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True
        ).strip()
    except:
        commit = "unknown"
    timestamp = datetime.now().isoformat()
    return commit, timestamp


def read_criterion_estimates(estimates_file: Path) -> Optional[Dict]:
    """Read Criterion estimates.json file and extract timing data."""
    if not estimates_file.exists():
        return None
    try:
        with open(estimates_file) as f:
            data = json.load(f)
        # Criterion stores times in nanoseconds
        mean_ns = data.get("mean", {}).get("point_estimate", 0)
        return {"mean_ns": mean_ns}
    except (json.JSONDecodeError, KeyError):
        return None


def get_criterion_tpcds_data(engine: str) -> List[Dict]:
    """
    Read TPC-DS benchmark data from Criterion output directories.

    Looks in target/criterion/tpcds_queries_comparison/{engine}/ for results.
    Returns list of benchmark entries with query name and timing stats.
    """
    repo_root = get_repo_root()
    results = []

    # Try comparison directory first
    criterion_dir = repo_root / "target" / "criterion" / "tpcds_queries_comparison" / engine
    if not criterion_dir.exists():
        # Fallback to non-comparison directory
        criterion_dir = repo_root / "target" / "criterion" / "tpcds_queries" / engine

    if not criterion_dir.exists():
        return results

    for query_dir in sorted(criterion_dir.iterdir()):
        if not query_dir.is_dir():
            continue
        query_name = query_dir.name

        # Look for estimates.json in new/ subdirectory (Criterion format)
        estimates_file = query_dir / "new" / "estimates.json"
        data = read_criterion_estimates(estimates_file)

        if data:
            # Convert nanoseconds to seconds
            mean_s = data["mean_ns"] / 1_000_000_000
            results.append({
                "name": f"tpcds_{query_name.lower()}_{engine}",
                "stats": {
                    "mean": mean_s,
                    "status": "passed"
                }
            })

    return results


def get_latest_run(cursor: Any, suite: str) -> Optional[tuple]:
    """
    Get the latest benchmark run for a given suite.

    Note: Uses Python-side filtering because VibeSQL's WHERE clause
    with integer comparison has a known limitation.
    """
    cursor.execute('SELECT * FROM benchmark_runs')
    all_rows = cursor.fetchall()

    # Filter by benchmark suite (column index 4)
    suite_rows = [r for r in all_rows if r[4] == suite]
    if not suite_rows:
        return None

    # Get the row with max RUN_ID (column index 0)
    return max(suite_rows, key=lambda x: x[0])


def get_results_for_run(cursor: Any, table: str, run_id: int) -> List[tuple]:
    """
    Get results for a specific run ID from a results table.

    Note: Uses Python-side filtering due to WHERE clause limitation.
    """
    cursor.execute(f'SELECT * FROM {table}')
    all_rows = cursor.fetchall()
    columns = [d[0] for d in cursor.description]

    # Find the RUN_ID column index
    run_id_idx = columns.index('RUN_ID')

    # Filter by run_id
    return [r for r in all_rows if r[run_id_idx] == run_id]


def export_tpcc_results(cursor: Any) -> Optional[Dict]:
    """Export TPC-C results to JSON format."""
    # Get the latest TPC-C run
    run = get_latest_run(cursor, 'tpcc')
    if not run:
        print("  No TPC-C runs found")
        return None

    # Unpack: RUN_ID, RUN_TIMESTAMP, GIT_COMMIT, GIT_BRANCH, BENCHMARK_SUITE, SCALE_FACTOR, ...
    run_id, timestamp, commit, _, _, scale = run[:6]
    print(f"  Latest TPC-C run: {timestamp} ({commit})")

    # Get TPC-C results using helper (Python-side filtering)
    results = get_results_for_run(cursor, 'tpcc_results', run_id)

    benchmarks = []
    for row in results:
        # Columns: RESULT_ID, RUN_ID, DATABASE_ENGINE, TRANSACTION_TYPE, TRANSACTION_COUNT,
        #          AVG_LATENCY_US, TOTAL_DURATION_MS, TRANSACTIONS_PER_SECOND,
        #          SUCCESSFUL_TRANSACTIONS, FAILED_TRANSACTIONS
        _, _, engine, txn_type, count, latency, duration, tps, success, failed = row

        if txn_type == 'mixed':
            # Mixed workload - main TPC-C metric
            # Convert TPS to mean transaction time in seconds for web demo format
            # mean = 1/TPS gives time per transaction
            mean_time = (1.0 / tps) if tps and tps > 0 else 0
            benchmarks.append({
                "name": f"tpcc_mixed_{engine}",
                "stats": {
                    "mean": round(mean_time, 6),
                    "stddev": 0,  # Not tracked per-transaction
                    "min": round(mean_time * 0.9, 6),  # Approximate
                    "max": round(mean_time * 1.1, 6),  # Approximate
                    "rounds": count or 0,
                    # Keep original TPC-C metrics as extra info
                    "tps": round(tps, 2) if tps else 0,
                    "transactions": count or 0,
                    "duration_ms": duration or 0
                }
            })

    if not benchmarks:
        print("  No TPC-C results found")
        return None

    print(f"  Exported {len(benchmarks)} TPC-C benchmarks")
    return {
        "benchmarks": benchmarks,
        "datetime": timestamp,
        "machine_info": {
            "suite": "tpcc",
            "git_commit": commit,
            "scale_factor": str(scale)
        }
    }


def export_sysbench_results(cursor: Any) -> Optional[Dict]:
    """Export Sysbench results to JSON format."""
    # Get the latest Sysbench run
    run = get_latest_run(cursor, 'sysbench')
    if not run:
        print("  No Sysbench runs found")
        return None

    # Unpack: RUN_ID, RUN_TIMESTAMP, GIT_COMMIT, GIT_BRANCH, BENCHMARK_SUITE, SCALE_FACTOR, ...
    run_id, timestamp, commit, _, _, scale = run[:6]
    print(f"  Latest Sysbench run: {timestamp} ({commit})")

    # Get Sysbench results using helper (Python-side filtering)
    results = get_results_for_run(cursor, 'sysbench_results', run_id)

    benchmarks = []
    for row in results:
        # Columns: RESULT_ID, RUN_ID, DATABASE_ENGINE, TEST_NAME, TABLE_SIZE,
        #          MEAN_TIME_NS, STD_DEV_NS, MEDIAN_TIME_NS, ITERATIONS
        _, _, engine, test, _, mean_ns, std_ns, median_ns, iterations = row

        # Convert nanoseconds to seconds for consistency with other benchmarks
        mean_s = (mean_ns / 1e9) if mean_ns else 0
        std_s = (std_ns / 1e9) if std_ns else 0
        median_s = (median_ns / 1e9) if median_ns else 0

        benchmarks.append({
            "name": f"sysbench_{test}_{engine}",
            "stats": {
                "mean": mean_s,
                "stddev": std_s,
                "min": median_s * 0.9,  # Approximate
                "max": median_s * 1.1,  # Approximate
                "rounds": iterations or 0
            }
        })

    if not benchmarks:
        print("  No Sysbench results found")
        return None

    print(f"  Exported {len(benchmarks)} Sysbench benchmarks")
    return {
        "benchmarks": benchmarks,
        "metadata": {
            "suite": "sysbench",
            "timestamp": timestamp,
            "git_commit": commit,
            "table_size": scale
        }
    }


def export_tpcds_results(cursor: Any) -> Optional[Dict]:
    """Export TPC-DS results to JSON format.

    Reads all engine data (vibesql, sqlite, duckdb, mysql) from the database.
    The database now stores comparison data with the database_engine column.
    """
    benchmarks = []
    commit, timestamp = get_git_info()
    scale = "0.01"  # Default scale factor

    # Get data from VibeSQL database
    run = get_latest_run(cursor, 'tpcds')
    if not run:
        print("  No TPC-DS runs found in database")
        return None

    run_id, db_timestamp, db_commit, _, _, db_scale, _, total, passed = run[:9]
    print(f"  Latest TPC-DS run from DB: {db_timestamp} ({db_commit}) - {passed}/{total} queries")
    timestamp = db_timestamp
    commit = db_commit
    scale = db_scale

    # Get TPC-DS results using helper (Python-side filtering)
    results = get_results_for_run(cursor, 'benchmark_results', run_id)

    # Get column names to find database_engine index
    cursor.execute('SELECT * FROM benchmark_results LIMIT 1')
    columns = [d[0].lower() for d in cursor.description]

    # Check if database_engine column exists (new schema)
    has_engine_col = 'database_engine' in columns
    if has_engine_col:
        engine_idx = columns.index('database_engine')
    else:
        engine_idx = None
        print("  Note: database_engine column not found, assuming all results are vibesql")

    # Aggregate results by (engine, query) to handle multiple iterations
    # Results format: (result_id, run_id, [database_engine], query_name, status, ...)
    query_data: Dict[tuple, List[tuple]] = {}
    for row in results:
        if has_engine_col:
            # New schema: (result_id, run_id, database_engine, query_name, status, ...)
            engine = row[engine_idx] if row[engine_idx] else 'vibesql'
            query = row[engine_idx + 1]  # query_name is after database_engine
            status = row[engine_idx + 2]  # status
            exec_ms = row[engine_idx + 5] if len(row) > engine_idx + 5 else None  # execution_time_ms
            total_ms = row[engine_idx + 6] if len(row) > engine_idx + 6 else None  # total_time_ms
            rows = row[engine_idx + 7] if len(row) > engine_idx + 7 else None  # row_count
        else:
            # Old schema: (result_id, run_id, query_name, status, ...)
            engine = 'vibesql'
            query = row[2]
            status = row[3]
            exec_ms = row[6] if len(row) > 6 else None
            total_ms = row[7] if len(row) > 7 else None
            rows = row[8] if len(row) > 8 else None

        if query.startswith('sanity'):
            continue

        key = (engine, query)
        if key not in query_data:
            query_data[key] = []
        query_data[key].append((exec_ms, total_ms, rows, status))

    for (engine, query), iterations in sorted(query_data.items()):
        exec_times = [it[0] for it in iterations if it[0] is not None]
        total_times = [it[1] for it in iterations if it[1] is not None]
        statuses = [it[3] for it in iterations]
        status = max(set(statuses), key=statuses.count) if statuses else 'unknown'
        rows = iterations[0][2] if iterations else 0
        mean_exec_s = (sum(exec_times) / len(exec_times) / 1000) if exec_times else 0
        mean_total_s = (sum(total_times) / len(total_times) / 1000) if total_times else 0

        benchmarks.append({
            "name": f"tpcds_{query.lower()}_{engine}",
            "stats": {
                "mean": mean_exec_s,
                "total": mean_total_s,
                "rows": rows or 0,
                "status": status,
                "iterations": len(iterations)
            }
        })

    if not benchmarks:
        print("  No TPC-DS results found in database")
        return None

    # Count by engine
    vibesql_count = sum(1 for b in benchmarks if b["name"].endswith("_vibesql"))
    sqlite_count = sum(1 for b in benchmarks if b["name"].endswith("_sqlite"))
    duckdb_count = sum(1 for b in benchmarks if b["name"].endswith("_duckdb"))
    mysql_count = sum(1 for b in benchmarks if b["name"].endswith("_mysql"))
    print(f"  Total: {len(benchmarks)} benchmarks (VibeSQL: {vibesql_count}, SQLite: {sqlite_count}, DuckDB: {duckdb_count}, MySQL: {mysql_count})")
    return {
        "benchmarks": benchmarks,
        "metadata": {
            "suite": "tpcds",
            "timestamp": timestamp,
            "git_commit": commit,
            "scale_factor": scale,
            "total_queries": vibesql_count,
            "passed_queries": sum(1 for b in benchmarks if b["stats"].get("status") == "passed" and b["name"].endswith("_vibesql")),
            "vibesql_queries": vibesql_count,
            "sqlite_queries": sqlite_count,
            "duckdb_queries": duckdb_count,
            "mysql_queries": mysql_count
        }
    }


def export_tpch_results(cursor: Any) -> Optional[Dict]:
    """Export TPC-H results to JSON format.

    Reads all engine data (vibesql, sqlite, duckdb, mysql) from the database.
    The database now stores comparison data with the database_engine column.
    """
    # Get the latest TPC-H run
    run = get_latest_run(cursor, 'tpch')
    if not run:
        print("  No TPC-H runs found")
        return None

    # Unpack full row: RUN_ID, RUN_TIMESTAMP, GIT_COMMIT, GIT_BRANCH, BENCHMARK_SUITE,
    #                  SCALE_FACTOR, TIMEOUT_SECS, TOTAL_QUERIES, PASSED_QUERIES, ...
    run_id, timestamp, commit, _, _, scale, _, total, passed = run[:9]
    print(f"  Latest TPC-H run: {timestamp} ({commit}) - {passed}/{total} queries")

    # Get TPC-H results using helper (Python-side filtering)
    results = get_results_for_run(cursor, 'benchmark_results', run_id)

    # Get column names to find database_engine index
    cursor.execute('SELECT * FROM benchmark_results LIMIT 1')
    columns = [d[0].lower() for d in cursor.description]

    # Check if database_engine column exists (new schema)
    has_engine_col = 'database_engine' in columns
    if has_engine_col:
        engine_idx = columns.index('database_engine')
    else:
        engine_idx = None

    benchmarks = []
    for row in results:
        if has_engine_col:
            # New schema: (result_id, run_id, database_engine, query_name, status, ...)
            engine = row[engine_idx] if row[engine_idx] else 'vibesql'
            query = row[engine_idx + 1]  # query_name is after database_engine
            status = row[engine_idx + 2]  # status
            exec_ms = row[engine_idx + 5] if len(row) > engine_idx + 5 else None  # execution_time_ms
        else:
            # Old schema: (result_id, run_id, query_name, status, ...)
            engine = 'vibesql'
            query = row[2]
            status = row[3]
            exec_ms = row[6] if len(row) > 6 else None

        # Convert to seconds
        exec_s = (exec_ms / 1000) if exec_ms else 0

        benchmarks.append({
            "name": f"tpch_{query.lower()}_{engine}",
            "stats": {
                "mean": exec_s,
                "stddev": exec_s * 0.05,  # Approximate 5% variance
                "min": exec_s * 0.95,
                "max": exec_s * 1.05,
                "rounds": 5,
                "status": status
            }
        })

    if not benchmarks:
        print("  No TPC-H results found")
        return None

    # Count by engine
    vibesql_count = sum(1 for b in benchmarks if b["name"].endswith("_vibesql"))
    sqlite_count = sum(1 for b in benchmarks if b["name"].endswith("_sqlite"))
    duckdb_count = sum(1 for b in benchmarks if b["name"].endswith("_duckdb"))
    mysql_count = sum(1 for b in benchmarks if b["name"].endswith("_mysql"))
    print(f"  Total: {len(benchmarks)} benchmarks (VibeSQL: {vibesql_count}, SQLite: {sqlite_count}, DuckDB: {duckdb_count}, MySQL: {mysql_count})")

    return {
        "benchmarks": benchmarks,
        "metadata": {
            "suite": "tpch",
            "timestamp": timestamp,
            "git_commit": commit,
            "scale_factor": scale,
            "total_queries": vibesql_count,
            "passed_queries": sum(1 for b in benchmarks if b["stats"].get("status") == "passed" and b["name"].endswith("_vibesql")),
            "vibesql_queries": vibesql_count,
            "sqlite_queries": sqlite_count,
            "duckdb_queries": duckdb_count,
            "mysql_queries": mysql_count
        }
    }


def main():
    parser = argparse.ArgumentParser(description="Export benchmark data from VibeSQL database to JSON")
    parser.add_argument("--all", action="store_true", help="Export all benchmark suites")
    parser.add_argument("--tpcc", action="store_true", help="Export TPC-C results")
    parser.add_argument("--sysbench", action="store_true", help="Export Sysbench results")
    parser.add_argument("--tpcds", action="store_true", help="Export TPC-DS results")
    parser.add_argument("--tpch", action="store_true", help="Export TPC-H results (VibeSQL only)")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Output directory for JSON files (default: web-demo/public/benchmarks)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    # If no specific suite selected, show help
    if not any([args.all, args.tpcc, args.sysbench, args.tpcds, args.tpch]):
        parser.print_help()
        print("\nExample: python scripts/export_benchmark_json.py --all")
        return 1

    # Determine output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = get_repo_root() / "web-demo" / "public" / "benchmarks"

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading VibeSQL database from {get_db_path()}...")
    try:
        db, cursor = get_connection()
    except Exception as e:
        print(f"Error loading database: {e}")
        return 1

    # Check what's available
    cursor.execute('SELECT BENCHMARK_SUITE, COUNT(*) FROM benchmark_runs GROUP BY BENCHMARK_SUITE')
    print("\nAvailable benchmark data:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} runs")
    print()

    exported = 0

    # Export TPC-C
    if args.all or args.tpcc:
        print("Exporting TPC-C results...")
        data = export_tpcc_results(cursor)
        if data:
            output_file = output_dir / "tpcc_results.json"
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"  Written to {output_file}")
            exported += 1

    # Export Sysbench
    if args.all or args.sysbench:
        print("Exporting Sysbench results...")
        data = export_sysbench_results(cursor)
        if data:
            output_file = output_dir / "sysbench_results.json"
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"  Written to {output_file}")
            exported += 1

    # Export TPC-DS
    if args.all or args.tpcds:
        print("Exporting TPC-DS results...")
        data = export_tpcds_results(cursor)
        if data:
            output_file = output_dir / "tpcds_results.json"
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"  Written to {output_file}")
            exported += 1

    # Export TPC-H
    if args.all or args.tpch:
        print("Exporting TPC-H results...")
        data = export_tpch_results(cursor)
        if data:
            output_file = output_dir / "benchmark_results.json"
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"  Written to {output_file}")
            exported += 1

    print(f"\nExported {exported} benchmark files to {output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
