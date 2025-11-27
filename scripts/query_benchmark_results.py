#!/usr/bin/env python3
"""
Query benchmark results from the dogfooding database.

This script provides various views of TPC-H benchmark performance over time.
"""

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import List, Tuple


def get_db_path() -> Path:
    """Get the path to the dogfooding database."""
    return Path.home() / ".vibesql" / "test_results" / "sqllogictest_results.vbsql"


def execute_query(db_path: Path, query: str) -> List[Tuple]:
    """Execute a query and return results."""
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        print("Run benchmarks first: make benchmark-tpch")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        conn.close()


def format_table(headers: List[str], rows: List[Tuple]):
    """Format query results as an ASCII table."""
    if not rows:
        print("No results found")
        return

    # Calculate column widths
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))

    # Print header
    header_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    separator = "-+-".join("-" * w for w in col_widths)

    print(header_line)
    print(separator)

    # Print rows
    for row in rows:
        row_line = " | ".join(str(val).ljust(col_widths[i]) for i, val in enumerate(row))
        print(row_line)


def query_latest(db_path: Path):
    """Show latest benchmark run summary."""
    print("=== Latest Benchmark Run ===\n")

    query = "SELECT * FROM latest_benchmark_summary"
    results = execute_query(db_path, query)

    if results:
        headers = ["Run ID", "Timestamp", "Suite", "Commit", "Branch", "Total", "Passed", "Failed", "Timeout", "Pass Rate", "Notes"]
        format_table(headers, results)


def query_trend(db_path: Path, query_name: str = None):
    """Show performance trend for queries."""
    print("=== Performance Trend ===\n")

    if query_name:
        query = f"""
            SELECT query_name, timestamp, total_time_ms, execution_time_ms,
                   prev_total_time_ms, pct_change, status
            FROM query_performance_trend
            WHERE query_name = '{query_name}'
            ORDER BY timestamp DESC
            LIMIT 10
        """
        headers = ["Query", "Timestamp", "Total (ms)", "Exec (ms)", "Prev Total (ms)", "% Change", "Status"]
    else:
        query = """
            SELECT query_name, timestamp, total_time_ms, execution_time_ms, pct_change
            FROM query_performance_trend
            WHERE pct_change IS NOT NULL
            ORDER BY timestamp DESC, query_name
            LIMIT 20
        """
        headers = ["Query", "Timestamp", "Total (ms)", "Exec (ms)", "% Change"]

    results = execute_query(db_path, query)
    format_table(headers, results)


def query_regressions(db_path: Path):
    """Show performance regressions (queries that got slower)."""
    print("=== Performance Regressions (>10% slower) ===\n")

    query = """
        SELECT query_name, timestamp, current_time_ms, prev_execution_time_ms,
               pct_change, severity
        FROM performance_regressions
        ORDER BY pct_change DESC
        LIMIT 10
    """
    results = execute_query(db_path, query)

    headers = ["Query", "Timestamp", "Current (ms)", "Previous (ms)", "% Change", "Severity"]
    format_table(headers, results)


def query_improvements(db_path: Path):
    """Show performance improvements (queries that got faster)."""
    print("=== Performance Improvements (>10% faster) ===\n")

    query = """
        SELECT query_name, timestamp, current_time_ms, prev_execution_time_ms,
               pct_improvement
        FROM performance_improvements
        ORDER BY pct_improvement DESC
        LIMIT 10
    """
    results = execute_query(db_path, query)

    headers = ["Query", "Timestamp", "Current (ms)", "Previous (ms)", "% Improvement"]
    format_table(headers, results)


def query_stats(db_path: Path):
    """Show statistics for each TPC-H query across all runs."""
    print("=== TPC-H Query Statistics (All Runs) ===\n")

    query = """
        SELECT query_name, total_runs, passed_runs, timeout_runs, failed_runs,
               avg_execution_ms, min_execution_ms, max_execution_ms, variability_pct
        FROM tpch_query_stats
        ORDER BY query_name
    """
    results = execute_query(db_path, query)

    headers = ["Query", "Runs", "Passed", "Timeout", "Failed", "Avg (ms)", "Min (ms)", "Max (ms)", "Var %"]
    format_table(headers, results)


def query_comparison(db_path: Path):
    """Compare latest run to baseline (first run)."""
    print("=== Benchmark Comparison (Latest vs Baseline) ===\n")

    query = """
        SELECT query_name, baseline_time_ms, current_time_ms, pct_change, trend
        FROM benchmark_comparison
        ORDER BY pct_change DESC
    """
    results = execute_query(db_path, query)

    headers = ["Query", "Baseline (ms)", "Current (ms)", "% Change", "Trend"]
    format_table(headers, results)


def query_history(db_path: Path):
    """Show all benchmark runs."""
    print("=== Benchmark Run History ===\n")

    query = """
        SELECT run_id, timestamp, benchmark_suite, git_commit, git_branch,
               total_queries, passed_queries, failed_queries, timeout_queries,
               ROUND(passed_queries * 100.0 / NULLIF(total_queries, 0), 1) as pass_rate
        FROM benchmark_runs
        ORDER BY run_id DESC
        LIMIT 20
    """
    results = execute_query(db_path, query)

    headers = ["Run ID", "Timestamp", "Suite", "Commit", "Branch", "Total", "Passed", "Failed", "Timeout", "Pass %"]
    format_table(headers, results)


def query_tpcc(db_path: Path):
    """Show latest TPC-C benchmark results."""
    print("=== Latest TPC-C Benchmark Results ===\n")

    query = """
        SELECT database_engine, transaction_type, transaction_count,
               ROUND(transactions_per_second, 2) as tps,
               ROUND(avg_latency_us, 2) as avg_latency_us,
               successful_transactions, failed_transactions
        FROM latest_tpcc_summary
    """
    try:
        results = execute_query(db_path, query)
        headers = ["Engine", "Txn Type", "Count", "TPS", "Avg Latency (us)", "Success", "Failed"]
        format_table(headers, results)
    except:
        print("No TPC-C results found. Run 'make benchmark-tpcc' first.")
        return

    print("\n=== TPC-C Engine Comparison ===\n")
    query = """
        SELECT transaction_type,
               ROUND(vibesql_tps, 2) as vibesql_tps,
               ROUND(sqlite_tps, 2) as sqlite_tps,
               ROUND(duckdb_tps, 2) as duckdb_tps,
               ROUND(vibesql_latency_us, 2) as vibesql_us,
               ROUND(sqlite_latency_us, 2) as sqlite_us,
               ROUND(duckdb_latency_us, 2) as duckdb_us
        FROM tpcc_engine_comparison
    """
    try:
        results = execute_query(db_path, query)
        headers = ["Txn Type", "VibeSQL TPS", "SQLite TPS", "DuckDB TPS", "VibeSQL us", "SQLite us", "DuckDB us"]
        format_table(headers, results)
    except:
        pass


def query_sysbench(db_path: Path):
    """Show latest Sysbench OLTP benchmark results."""
    print("=== Latest Sysbench OLTP Results ===\n")

    query = """
        SELECT database_engine, test_name, table_size,
               mean_time_us, std_dev_us, iterations
        FROM latest_sysbench_summary
    """
    try:
        results = execute_query(db_path, query)
        headers = ["Engine", "Test", "Table Size", "Mean (us)", "Std Dev (us)", "Iterations"]
        format_table(headers, results)
    except:
        print("No Sysbench results found. Run 'make benchmark-sysbench' first.")
        return

    print("\n=== Sysbench Engine Comparison ===\n")
    query = """
        SELECT test_name, table_size,
               ROUND(vibesql_us, 2) as vibesql_us,
               ROUND(sqlite_us, 2) as sqlite_us,
               ROUND(duckdb_us, 2) as duckdb_us,
               vibesql_vs_sqlite
        FROM sysbench_engine_comparison
    """
    try:
        results = execute_query(db_path, query)
        headers = ["Test", "Table Size", "VibeSQL (us)", "SQLite (us)", "DuckDB (us)", "VibeSQL/SQLite"]
        format_table(headers, results)
    except:
        pass


def main():
    parser = argparse.ArgumentParser(
        description="Query benchmark results from dogfooding database"
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Show latest TPC-H benchmark run summary"
    )
    parser.add_argument(
        "--trend",
        nargs="?",
        const=True,
        metavar="QUERY",
        help="Show performance trend (optionally for specific query like 'Q1')"
    )
    parser.add_argument(
        "--regressions",
        action="store_true",
        help="Show performance regressions (queries that got slower)"
    )
    parser.add_argument(
        "--improvements",
        action="store_true",
        help="Show performance improvements (queries that got faster)"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show statistics for all TPC-H queries across all runs"
    )
    parser.add_argument(
        "--comparison",
        action="store_true",
        help="Compare latest run to baseline"
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="Show all benchmark runs"
    )
    parser.add_argument(
        "--tpcc",
        action="store_true",
        help="Show TPC-C OLTP benchmark results"
    )
    parser.add_argument(
        "--sysbench",
        action="store_true",
        help="Show Sysbench OLTP benchmark results"
    )

    args = parser.parse_args()

    # Get database path
    db_path = get_db_path()

    # Execute requested query
    if args.latest:
        query_latest(db_path)
    elif args.trend is not None:
        query_name = args.trend if isinstance(args.trend, str) else None
        query_trend(db_path, query_name)
    elif args.regressions:
        query_regressions(db_path)
    elif args.improvements:
        query_improvements(db_path)
    elif args.stats:
        query_stats(db_path)
    elif args.comparison:
        query_comparison(db_path)
    elif args.history:
        query_history(db_path)
    elif args.tpcc:
        query_tpcc(db_path)
    elif args.sysbench:
        query_sysbench(db_path)
    else:
        # Default: show latest TPC-H
        query_latest(db_path)
        print("\n")
        query_stats(db_path)


if __name__ == "__main__":
    main()
