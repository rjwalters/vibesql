#!/usr/bin/env python3
"""
Export historical benchmark trend data from the VibeSQL dogfooding database to JSON format.

This script reads benchmark results stored in our VibeSQL database and exports
them to a JSON format showing performance trends over time for the web demo trends page.

The output shows avg, min (best), and max (worst) performance for each benchmark suite
across all available runs.

Usage:
    python scripts/export_trend_json.py
    python scripts/export_trend_json.py --output trends_results.json
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from collections import defaultdict

# Import our VibeSQL helper module
sys.path.insert(0, str(Path(__file__).parent))
from vibesql_db import get_connection, get_db_path


def get_repo_root() -> Path:
    """Get the repository root directory."""
    return Path(__file__).parent.parent


def collapse_by_commit(trend_data: List[Dict], metric_type: str = "time") -> List[Dict]:
    """
    Collapse multiple runs of the same commit into a single data point.

    Uses the earliest timestamp for each commit and aggregates metrics:
    - For time metrics: average the averages, min of mins, max of maxes
    - For TPS metrics: average the TPS values

    Args:
        trend_data: List of data points with commit, date, and metrics
        metric_type: "time" for latency metrics, "tps" for throughput metrics

    Returns:
        Collapsed list with one data point per commit, sorted by date
    """
    if not trend_data:
        return []

    by_commit: Dict[str, List[Dict]] = defaultdict(list)
    for point in trend_data:
        by_commit[point["commit"]].append(point)

    collapsed = []
    for commit, points in by_commit.items():
        # Use earliest timestamp
        earliest = min(p["date"] for p in points)

        if metric_type == "tps":
            # TPC-C style: average TPS, average latency
            tps_values = [p["tps"] for p in points if p.get("tps") is not None]
            latency_values = [p["latency_us"] for p in points if p.get("latency_us") is not None]
            collapsed.append({
                "date": earliest,
                "commit": commit,
                "tps": round(sum(tps_values) / len(tps_values), 2) if tps_values else None,
                "latency_us": round(sum(latency_values) / len(latency_values), 2) if latency_values else None,
            })
        else:
            # Time-based metrics (TPC-H, TPC-DS, Sysbench)
            avg_values = [p["avg_ms"] for p in points if p.get("avg_ms") is not None]
            min_values = [p["min_ms"] for p in points if p.get("min_ms") is not None]
            max_values = [p["max_ms"] for p in points if p.get("max_ms") is not None]
            geomean_values = [p["geomean_ms"] for p in points if p.get("geomean_ms") is not None]

            result = {
                "date": earliest,
                "commit": commit,
                "avg_ms": round(sum(avg_values) / len(avg_values), 2) if avg_values else None,
                "min_ms": round(min(min_values), 2) if min_values else None,
                "max_ms": round(max(max_values), 2) if max_values else None,
            }

            # Optional fields
            if geomean_values:
                result["geomean_ms"] = round(sum(geomean_values) / len(geomean_values), 2)

            # queries_passed/total_queries: use values from run with most passed queries
            if any("queries_passed" in p for p in points):
                best_run = max(points, key=lambda p: p.get("queries_passed", 0))
                result["queries_passed"] = best_run.get("queries_passed")
                result["total_queries"] = best_run.get("total_queries")

            # Sysbench workloads: merge all workloads, averaging duplicates
            if any("workloads" in p for p in points):
                all_workloads: Dict[str, List[float]] = defaultdict(list)
                for p in points:
                    for k, v in p.get("workloads", {}).items():
                        all_workloads[k].append(v)
                result["workloads"] = {k: round(sum(v) / len(v), 4) for k, v in all_workloads.items()}

            collapsed.append(result)

    # Sort by date (earliest first)
    return sorted(collapsed, key=lambda x: x["date"])


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


def get_all_runs(cursor: Any, suite: str) -> List[tuple]:
    """
    Get all benchmark runs for a given suite, ordered by timestamp.

    Columns: RUN_ID (0), RUN_TIMESTAMP (1), GIT_COMMIT (2), GIT_BRANCH (3),
             BENCHMARK_SUITE (4), SCALE_FACTOR (5), TIMEOUT_SECS (6),
             TOTAL_QUERIES (7), PASSED_QUERIES (8), FAILED_QUERIES (9),
             TIMEOUT_QUERIES (10), NOTES (11)
    """
    cursor.execute('SELECT * FROM benchmark_runs ORDER BY RUN_TIMESTAMP')
    all_rows = cursor.fetchall()

    # Filter by benchmark suite (column index 4)
    suite_rows = [r for r in all_rows if r[4] == suite]
    return suite_rows


def get_results_for_run(cursor: Any, table: str, run_id: int) -> List[tuple]:
    """
    Get results for a specific run ID from a results table.
    """
    cursor.execute(f'SELECT * FROM {table}')
    all_rows = cursor.fetchall()
    columns = [d[0] for d in cursor.description]

    # Find the RUN_ID column index
    run_id_idx = columns.index('RUN_ID')

    # Filter by run_id
    return [r for r in all_rows if r[run_id_idx] == run_id]


def calculate_geometric_mean(values: List[float]) -> float:
    """Calculate the geometric mean of a list of positive values."""
    if not values:
        return 0.0
    product = 1.0
    for v in values:
        if v > 0:
            product *= v
    return product ** (1.0 / len(values)) if values else 0.0


def export_tpch_trends(cursor: Any) -> Optional[Dict]:
    """Export TPC-H performance trends."""
    runs = get_all_runs(cursor, 'tpch')
    if not runs:
        print("  No TPC-H runs found")
        return None

    print(f"  Found {len(runs)} TPC-H runs")

    trend_data = []

    for run in runs:
        # Columns: RUN_ID, RUN_TIMESTAMP, GIT_COMMIT, GIT_BRANCH, BENCHMARK_SUITE,
        #          SCALE_FACTOR, TIMEOUT_SECS, TOTAL_QUERIES, PASSED_QUERIES
        run_id = run[0]
        timestamp = run[1]
        commit = run[2]
        total = run[7]
        passed = run[8]

        results = get_results_for_run(cursor, 'benchmark_results', run_id)

        # Collect execution times for passed queries (VibeSQL only for trends)
        exec_times = []
        for row in results:
            # New schema: RESULT_ID, RUN_ID, DATABASE_ENGINE, QUERY_NAME, STATUS,
            #             PARSE_TIME_MS, EXECUTOR_CREATION_TIME_MS, EXECUTION_TIME_MS,
            #             TOTAL_TIME_MS, ROW_COUNT, ERROR_MESSAGE
            _, _, engine, query, status, _, _, exec_ms, total_ms, rows, _ = row
            # Only include VibeSQL results for trend tracking
            if engine == 'vibesql' and status == 'passed' and exec_ms is not None:
                exec_times.append(exec_ms)

        if exec_times:
            trend_data.append({
                "date": timestamp[:10] if timestamp else "",  # YYYY-MM-DD
                "commit": commit or "",
                "avg_ms": round(sum(exec_times) / len(exec_times), 2),
                "min_ms": round(min(exec_times), 2),
                "max_ms": round(max(exec_times), 2),
                "geomean_ms": round(calculate_geometric_mean(exec_times), 2),
                "queries_passed": len(exec_times),
                "total_queries": total or 22
            })

    # Collapse multiple runs of the same commit into single data points
    collapsed_data = collapse_by_commit(trend_data, metric_type="time")

    return {
        "suite": "tpch",
        "display_name": "TPC-H",
        "description": "Decision support queries",
        "data": collapsed_data
    }


def export_tpcds_trends(cursor: Any) -> Optional[Dict]:
    """Export TPC-DS performance trends."""
    runs = get_all_runs(cursor, 'tpcds')
    if not runs:
        print("  No TPC-DS runs found")
        return None

    print(f"  Found {len(runs)} TPC-DS runs")

    trend_data = []

    for run in runs:
        # Columns: RUN_ID, RUN_TIMESTAMP, GIT_COMMIT, GIT_BRANCH, BENCHMARK_SUITE,
        #          SCALE_FACTOR, TIMEOUT_SECS, TOTAL_QUERIES, PASSED_QUERIES
        run_id = run[0]
        timestamp = run[1]
        commit = run[2]
        total = run[7]
        passed = run[8]

        results = get_results_for_run(cursor, 'benchmark_results', run_id)

        # Collect execution times for passed queries (VibeSQL only for trends)
        exec_times = []
        for row in results:
            # New schema: RESULT_ID, RUN_ID, DATABASE_ENGINE, QUERY_NAME, STATUS, ...
            _, _, engine, query, status, _, _, exec_ms, total_ms, rows, _ = row
            # Only include VibeSQL results for trend tracking
            if engine == 'vibesql' and status == 'passed' and exec_ms is not None:
                exec_times.append(exec_ms)

        if exec_times:
            trend_data.append({
                "date": timestamp[:10] if timestamp else "",
                "commit": commit or "",
                "avg_ms": round(sum(exec_times) / len(exec_times), 2),
                "min_ms": round(min(exec_times), 2),
                "max_ms": round(max(exec_times), 2),
                "geomean_ms": round(calculate_geometric_mean(exec_times), 2),
                "queries_passed": len(exec_times),
                "total_queries": total or 99
            })

    # Collapse multiple runs of the same commit into single data points
    collapsed_data = collapse_by_commit(trend_data, metric_type="time")

    return {
        "suite": "tpcds",
        "display_name": "TPC-DS",
        "description": "Decision support queries (complex)",
        "data": collapsed_data
    }


def export_tpcc_trends(cursor: Any) -> Optional[Dict]:
    """Export TPC-C performance trends (transactions per second)."""
    runs = get_all_runs(cursor, 'tpcc')
    if not runs:
        print("  No TPC-C runs found")
        return None

    print(f"  Found {len(runs)} TPC-C runs")

    trend_data = []

    for run in runs:
        # Columns: RUN_ID, RUN_TIMESTAMP, GIT_COMMIT, GIT_BRANCH, BENCHMARK_SUITE, SCALE_FACTOR
        run_id = run[0]
        timestamp = run[1]
        commit = run[2]

        results = get_results_for_run(cursor, 'tpcc_results', run_id)

        # Find VibeSQL mixed workload results (main TPC-C metric)
        vibesql_tps = None
        vibesql_latency = None
        for row in results:
            # RESULT_ID, RUN_ID, DATABASE_ENGINE, TRANSACTION_TYPE, TRANSACTION_COUNT,
            # AVG_LATENCY_US, TOTAL_DURATION_MS, TRANSACTIONS_PER_SECOND, ...
            _, _, engine, txn_type, count, latency, duration, tps, _, _ = row
            if engine == 'vibesql' and txn_type == 'mixed':
                vibesql_tps = tps
                vibesql_latency = latency
                break

        if vibesql_tps is not None:
            trend_data.append({
                "date": timestamp[:10] if timestamp else "",
                "commit": commit or "",
                "tps": round(vibesql_tps, 2),
                "latency_us": round(vibesql_latency, 2) if vibesql_latency else None
            })

    # Collapse multiple runs of the same commit into single data points
    collapsed_data = collapse_by_commit(trend_data, metric_type="tps")

    return {
        "suite": "tpcc",
        "display_name": "TPC-C",
        "description": "OLTP transactions (mixed workload)",
        "metric": "tps",
        "metric_label": "Transactions/sec",
        "data": collapsed_data
    }


def export_sysbench_trends(cursor: Any) -> Optional[Dict]:
    """Export Sysbench performance trends."""
    runs = get_all_runs(cursor, 'sysbench')
    if not runs:
        print("  No Sysbench runs found")
        return None

    print(f"  Found {len(runs)} Sysbench runs")

    trend_data = []

    for run in runs:
        # Columns: RUN_ID, RUN_TIMESTAMP, GIT_COMMIT, GIT_BRANCH, BENCHMARK_SUITE, SCALE_FACTOR
        run_id = run[0]
        timestamp = run[1]
        commit = run[2]

        results = get_results_for_run(cursor, 'sysbench_results', run_id)

        # Collect VibeSQL times for all workloads
        vibesql_times = {}
        for row in results:
            # RESULT_ID, RUN_ID, DATABASE_ENGINE, TEST_NAME, TABLE_SIZE,
            # MEAN_TIME_NS, STD_DEV_NS, MEDIAN_TIME_NS, ITERATIONS
            _, _, engine, test, _, mean_ns, std_ns, median_ns, iterations = row
            if engine == 'vibesql' and mean_ns is not None:
                vibesql_times[test] = mean_ns / 1e6  # Convert ns to ms

        if vibesql_times:
            times = list(vibesql_times.values())
            trend_data.append({
                "date": timestamp[:10] if timestamp else "",
                "commit": commit or "",
                "avg_ms": round(sum(times) / len(times), 4),
                "min_ms": round(min(times), 4),
                "max_ms": round(max(times), 4),
                "workloads": {k: round(v, 4) for k, v in vibesql_times.items()}
            })

    # Collapse multiple runs of the same commit into single data points
    collapsed_data = collapse_by_commit(trend_data, metric_type="time")

    return {
        "suite": "sysbench",
        "display_name": "Sysbench",
        "description": "OLTP micro-benchmarks",
        "data": collapsed_data
    }


def main():
    parser = argparse.ArgumentParser(description="Export benchmark trend data from VibeSQL database to JSON")
    parser.add_argument("--output", "-o", type=str, default=None,
                        help="Output file path (default: web-demo/public/benchmarks/trends_results.json)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    # Determine output file
    if args.output:
        output_file = Path(args.output)
    else:
        output_file = get_repo_root() / "web-demo" / "public" / "benchmarks" / "trends_results.json"

    output_file.parent.mkdir(parents=True, exist_ok=True)

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

    # Export trends for each suite
    trends = {}

    print("Exporting TPC-H trends...")
    tpch = export_tpch_trends(cursor)
    if tpch:
        trends["tpch"] = tpch

    print("Exporting TPC-DS trends...")
    tpcds = export_tpcds_trends(cursor)
    if tpcds:
        trends["tpcds"] = tpcds

    print("Exporting TPC-C trends...")
    tpcc = export_tpcc_trends(cursor)
    if tpcc:
        trends["tpcc"] = tpcc

    print("Exporting Sysbench trends...")
    sysbench = export_sysbench_trends(cursor)
    if sysbench:
        trends["sysbench"] = sysbench

    if not trends:
        print("\nNo trend data found to export")
        return 1

    # Add metadata
    commit, timestamp = get_git_info()
    output_data = {
        "generated_at": timestamp,
        "git_commit": commit,
        "description": "VibeSQL Embedded performance trends over time",
        "benchmarks": trends
    }

    # Write output
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)

    print(f"\nExported trend data to {output_file}")

    # Summary
    total_points = sum(len(b["data"]) for b in trends.values())
    print(f"  Total data points: {total_points}")
    for suite, data in trends.items():
        print(f"    {suite}: {len(data['data'])} runs")

    return 0


if __name__ == "__main__":
    sys.exit(main())
