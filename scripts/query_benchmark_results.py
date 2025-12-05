#!/usr/bin/env python3
"""
Query benchmark results from the dogfooding database.

This script provides various views of TPC-H benchmark performance over time.

NOTE: This script dogfoods VibeSQL - we use our own database to store results!
"""

import argparse
import sys
from typing import Any, List, Optional, Tuple

# Import our VibeSQL helper module
from vibesql_db import get_connection, get_db_path


def execute_query(cursor: Any, query: str) -> List[Tuple]:
    """Execute a query and return results."""
    cursor.execute(query)
    return cursor.fetchall()


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


def query_latest(cursor: Any):
    """Show latest benchmark run summary."""
    print("=== Latest Benchmark Run ===\n")

    # Direct query instead of view (VibeSQL doesn't support views)
    # Note: Using run_timestamp column (timestamp is reserved)
    # WORKAROUND: ORDER BY returns empty results in VibeSQL, use MAX(run_id) subquery instead
    query = """
        SELECT run_id, run_timestamp, benchmark_suite, git_commit, git_branch,
               total_queries, passed_queries, failed_queries, timeout_queries, notes
        FROM benchmark_runs
        WHERE run_id = (SELECT MAX(run_id) FROM benchmark_runs)
    """
    results = execute_query(cursor, query)

    if results:
        # Calculate pass rate manually
        formatted_results = []
        for row in results:
            run_id, ts, suite, commit, branch, total, passed, failed, timeout, notes = row
            # Handle None values for passed/total (some benchmark types don't track these)
            if passed is not None and total is not None and total > 0:
                pass_rate = round(passed * 100.0 / total, 1)
            else:
                pass_rate = None
            formatted_results.append((run_id, ts, suite, commit, branch, total, passed, failed, timeout, pass_rate, notes))

        headers = ["Run ID", "Timestamp", "Suite", "Commit", "Branch", "Total", "Passed", "Failed", "Timeout", "Pass Rate", "Notes"]
        format_table(headers, formatted_results)


def query_trend(cursor: Any, query_name: Optional[str] = None):
    """Show performance trend for queries."""
    print("=== Performance Trend ===\n")

    # WORKAROUND: ORDER BY returns empty results in VibeSQL (#3602)
    # Fetch all results and sort in Python
    if query_name:
        query = f"""
            SELECT br.query_name, r.run_timestamp, br.total_time_ms, br.execution_time_ms, br.status
            FROM benchmark_results br
            JOIN benchmark_runs r ON br.run_id = r.run_id
            WHERE br.query_name = '{query_name}'
        """
        headers = ["Query", "Timestamp", "Total (ms)", "Exec (ms)", "Status"]
        results = execute_query(cursor, query)
        # Sort by timestamp DESC, take last 10
        results = sorted(results, key=lambda x: x[1] or '', reverse=True)[:10]
    else:
        query = """
            SELECT br.query_name, r.run_timestamp, br.total_time_ms, br.execution_time_ms, br.status
            FROM benchmark_results br
            JOIN benchmark_runs r ON br.run_id = r.run_id
            WHERE r.benchmark_suite = 'tpch'
        """
        headers = ["Query", "Timestamp", "Total (ms)", "Exec (ms)", "Status"]
        results = execute_query(cursor, query)
        # Sort by timestamp DESC then query_name, take last 20
        results = sorted(results, key=lambda x: (x[1] or '', x[0] or ''), reverse=True)[:20]

    format_table(headers, results)


def query_regressions(cursor: Any):
    """Show performance regressions (queries that got slower)."""
    print("=== Performance Regressions (>10% slower) ===\n")
    print("(Note: Regression detection requires historical comparison - showing recent results instead)")

    # WORKAROUND: ORDER BY returns empty results in VibeSQL (#3602)
    # Fetch all and sort in Python
    query = """
        SELECT br.query_name, r.run_timestamp, br.execution_time_ms, br.status, br.error_message
        FROM benchmark_results br
        JOIN benchmark_runs r ON br.run_id = r.run_id
        WHERE br.status != 'passed' OR br.execution_time_ms > 1000
    """
    results = execute_query(cursor, query)
    # Sort by timestamp DESC, take last 10
    results = sorted(results, key=lambda x: x[1] or '', reverse=True)[:10]

    headers = ["Query", "Timestamp", "Exec (ms)", "Status", "Error"]
    format_table(headers, results)


def query_improvements(cursor: Any):
    """Show performance improvements (queries that got faster)."""
    print("=== Performance Improvements ===\n")
    print("(Note: Improvement detection requires historical comparison - showing fastest queries instead)")

    # WORKAROUND: ORDER BY returns empty results in VibeSQL (#3602)
    # Fetch all and sort in Python
    query = """
        SELECT br.query_name, r.run_timestamp, br.execution_time_ms, br.total_time_ms
        FROM benchmark_results br
        JOIN benchmark_runs r ON br.run_id = r.run_id
        WHERE br.status = 'passed' AND br.execution_time_ms IS NOT NULL
    """
    results = execute_query(cursor, query)
    # Sort by execution_time ASC, take first 10
    results = sorted(results, key=lambda x: x[2] or float('inf'))[:10]

    headers = ["Query", "Timestamp", "Exec (ms)", "Total (ms)"]
    format_table(headers, results)


def query_stats(cursor: Any):
    """Show statistics for each TPC-H query across all runs."""
    print("=== TPC-H Query Statistics (All Runs) ===\n")

    # WORKAROUND: ORDER BY returns empty results in VibeSQL (#3602)
    # Remove ORDER BY from SQL and sort in Python
    query = """
        SELECT br.query_name,
               COUNT(*) as total_runs,
               SUM(CASE WHEN br.status = 'passed' THEN 1 ELSE 0 END) as passed_runs,
               SUM(CASE WHEN br.status = 'timeout' THEN 1 ELSE 0 END) as timeout_runs,
               SUM(CASE WHEN br.status = 'error' THEN 1 ELSE 0 END) as failed_runs,
               AVG(br.execution_time_ms) as avg_execution_ms,
               MIN(br.execution_time_ms) as min_execution_ms,
               MAX(br.execution_time_ms) as max_execution_ms
        FROM benchmark_results br
        JOIN benchmark_runs r ON br.run_id = r.run_id
        WHERE r.benchmark_suite = 'tpch'
        GROUP BY br.query_name
    """
    results = execute_query(cursor, query)
    # Sort by query_name
    results = sorted(results, key=lambda x: x[0] or '')

    # Calculate variability manually
    formatted_results = []
    for row in results:
        query_name, total, passed, timeout, failed, avg_ms, min_ms, max_ms = row
        # Variability = (max - min) / avg * 100
        if avg_ms and avg_ms > 0 and max_ms and min_ms:
            variability = round((max_ms - min_ms) / avg_ms * 100, 1)
        else:
            variability = None
        formatted_results.append((
            query_name, total, passed, timeout, failed,
            round(avg_ms, 2) if avg_ms else None,
            round(min_ms, 2) if min_ms else None,
            round(max_ms, 2) if max_ms else None,
            variability
        ))

    headers = ["Query", "Runs", "Passed", "Timeout", "Failed", "Avg (ms)", "Min (ms)", "Max (ms)", "Var %"]
    format_table(headers, formatted_results)


def query_comparison(cursor: Any):
    """Compare latest run to baseline (first run)."""
    print("=== Benchmark Comparison (Latest vs First Run) ===\n")

    # WORKAROUND: ORDER BY returns empty results in VibeSQL (#3602)
    # Use MIN/MAX subqueries to get first and latest run_ids

    # Get first run results (earliest run_id for tpch)
    query_first = """
        SELECT br.query_name, br.execution_time_ms
        FROM benchmark_results br
        JOIN benchmark_runs r ON br.run_id = r.run_id
        WHERE r.benchmark_suite = 'tpch' AND br.status = 'passed'
          AND r.run_id = (SELECT MIN(run_id) FROM benchmark_runs WHERE benchmark_suite = 'tpch')
    """
    first_results = execute_query(cursor, query_first)
    baseline = {row[0]: row[1] for row in first_results}

    # Get latest run results (latest run_id for tpch)
    query_latest = """
        SELECT br.query_name, br.execution_time_ms
        FROM benchmark_results br
        JOIN benchmark_runs r ON br.run_id = r.run_id
        WHERE r.benchmark_suite = 'tpch' AND br.status = 'passed'
          AND r.run_id = (SELECT MAX(run_id) FROM benchmark_runs WHERE benchmark_suite = 'tpch')
    """
    latest_results = execute_query(cursor, query_latest)
    current = {row[0]: row[1] for row in latest_results}

    # Calculate comparison
    formatted_results = []
    for query_name in sorted(set(baseline.keys()) | set(current.keys())):
        baseline_ms = baseline.get(query_name)
        current_ms = current.get(query_name)

        if baseline_ms and current_ms and baseline_ms > 0:
            pct_change = round((current_ms - baseline_ms) / baseline_ms * 100, 1)
            if pct_change < -10:
                trend = "faster"
            elif pct_change > 10:
                trend = "slower"
            else:
                trend = "stable"
        else:
            pct_change = None
            trend = "N/A"

        formatted_results.append((
            query_name,
            round(baseline_ms, 2) if baseline_ms else None,
            round(current_ms, 2) if current_ms else None,
            pct_change,
            trend
        ))

    headers = ["Query", "Baseline (ms)", "Current (ms)", "% Change", "Trend"]
    format_table(headers, formatted_results)


def query_history(cursor: Any):
    """Show all benchmark runs."""
    print("=== Benchmark Run History ===\n")

    # WORKAROUND: ORDER BY returns empty results in VibeSQL (#3602)
    # Fetch all and sort in Python
    query = """
        SELECT run_id, run_timestamp, benchmark_suite, git_commit, git_branch,
               total_queries, passed_queries, failed_queries, timeout_queries
        FROM benchmark_runs
    """
    results = execute_query(cursor, query)
    # Sort by run_id DESC, take last 20
    results = sorted(results, key=lambda x: x[0] or 0, reverse=True)[:20]

    # Calculate pass rate manually
    formatted_results = []
    for row in results:
        run_id, ts, suite, commit, branch, total, passed, failed, timeout = row
        pass_rate = round(passed * 100.0 / total, 1) if total and total > 0 and passed is not None else 0
        formatted_results.append((run_id, ts, suite, commit, branch, total, passed, failed, timeout, pass_rate))

    headers = ["Run ID", "Timestamp", "Suite", "Commit", "Branch", "Total", "Passed", "Failed", "Timeout", "Pass %"]
    format_table(headers, formatted_results)


def query_tpcc(cursor: Any):
    """Show latest TPC-C benchmark results."""
    print("=== Latest TPC-C Benchmark Results ===\n")

    # WORKAROUND: ORDER BY returns empty results in VibeSQL (#3602)
    # Use MAX subquery to get latest TPC-C run
    query = """
        SELECT tc.database_engine, tc.transaction_type, tc.transaction_count,
               tc.transactions_per_second, tc.avg_latency_us,
               tc.successful_transactions, tc.failed_transactions
        FROM tpcc_results tc
        JOIN benchmark_runs r ON tc.run_id = r.run_id
        WHERE r.benchmark_suite = 'tpcc'
          AND r.run_id = (SELECT MAX(run_id) FROM benchmark_runs WHERE benchmark_suite = 'tpcc')
    """
    try:
        results = execute_query(cursor, query)
        if not results:
            print("No TPC-C results found. Run 'make benchmark-tpcc' first.")
            return

        # Format results
        formatted_results = []
        for row in results:
            engine, txn_type, count, tps, latency, success, failed = row
            formatted_results.append((
                engine, txn_type, count,
                round(tps, 2) if tps else None,
                round(latency, 2) if latency else None,
                success, failed
            ))

        headers = ["Engine", "Txn Type", "Count", "TPS", "Avg Latency (us)", "Success", "Failed"]
        format_table(headers, formatted_results)
    except Exception as e:
        print(f"No TPC-C results found. Run 'make benchmark-tpcc' first. ({e})")


def query_sysbench(cursor: Any):
    """Show latest Sysbench OLTP benchmark results."""
    print("=== Latest Sysbench OLTP Results ===\n")

    # WORKAROUND: ORDER BY returns empty results in VibeSQL (#3602)
    # Use MAX subquery to get latest Sysbench run
    query = """
        SELECT sb.database_engine, sb.test_name, sb.table_size,
               sb.mean_time_ns, sb.std_dev_ns, sb.iterations
        FROM sysbench_results sb
        JOIN benchmark_runs r ON sb.run_id = r.run_id
        WHERE r.benchmark_suite = 'sysbench'
          AND r.run_id = (SELECT MAX(run_id) FROM benchmark_runs WHERE benchmark_suite = 'sysbench')
    """
    try:
        results = execute_query(cursor, query)
        if not results:
            print("No Sysbench results found. Run 'make benchmark-sysbench' first.")
            return

        # Convert ns to us for display
        formatted_results = []
        for row in results:
            engine, test, table_size, mean_ns, std_ns, iterations = row
            formatted_results.append((
                engine, test, table_size,
                round(mean_ns / 1000, 2) if mean_ns else None,
                round(std_ns / 1000, 2) if std_ns else None,
                iterations
            ))

        headers = ["Engine", "Test", "Table Size", "Mean (us)", "Std Dev (us)", "Iterations"]
        format_table(headers, formatted_results)
    except Exception as e:
        print(f"No Sysbench results found. Run 'make benchmark-sysbench' first. ({e})")


def query_tpcds(cursor: Any):
    """Show latest TPC-DS benchmark results."""
    print("=== Latest TPC-DS Benchmark Results ===\n")

    # WORKAROUND: ORDER BY returns empty results in VibeSQL (#3602)
    # Use MAX subquery to get latest TPC-DS run
    query = """
        SELECT br.query_name, br.status, br.execution_time_ms, br.total_time_ms
        FROM benchmark_results br
        JOIN benchmark_runs r ON br.run_id = r.run_id
        WHERE r.benchmark_suite = 'tpcds'
          AND r.run_id = (SELECT MAX(run_id) FROM benchmark_runs WHERE benchmark_suite = 'tpcds')
    """
    try:
        results = execute_query(cursor, query)
        if not results:
            print("No TPC-DS results found. Run 'make benchmark-tpcds' first.")
            return

        # Sort by query_name in Python (workaround for ORDER BY bug)
        results = sorted(results, key=lambda x: x[0] or '')

        # Format results
        formatted_results = []
        for row in results:
            query_name, status, exec_ms, total_ms = row
            formatted_results.append((
                query_name, status,
                round(exec_ms, 2) if exec_ms else None,
                round(total_ms, 2) if total_ms else None
            ))

        headers = ["Query", "Status", "Exec (ms)", "Total (ms)"]
        format_table(headers, formatted_results)
    except Exception as e:
        print(f"No TPC-DS results found. Run 'make benchmark-tpcds' first. ({e})")

    print("\n=== TPC-DS Query Statistics (All Runs) ===\n")
    # WORKAROUND: ORDER BY returns empty results in VibeSQL (#3602)
    # Remove ORDER BY from SQL and sort in Python
    query = """
        SELECT br.query_name,
               COUNT(*) as total_runs,
               SUM(CASE WHEN br.status = 'passed' THEN 1 ELSE 0 END) as passed_runs,
               SUM(CASE WHEN br.status = 'timeout' THEN 1 ELSE 0 END) as timeout_runs,
               SUM(CASE WHEN br.status = 'error' THEN 1 ELSE 0 END) as failed_runs,
               AVG(br.execution_time_ms) as avg_execution_ms,
               MIN(br.execution_time_ms) as min_execution_ms,
               MAX(br.execution_time_ms) as max_execution_ms
        FROM benchmark_results br
        JOIN benchmark_runs r ON br.run_id = r.run_id
        WHERE r.benchmark_suite = 'tpcds'
        GROUP BY br.query_name
    """
    try:
        results = execute_query(cursor, query)
        if results:
            # Sort by query_name in Python
            results = sorted(results, key=lambda x: x[0] or '')
            # Calculate variability manually
            formatted_results = []
            for row in results:
                query_name, total, passed, timeout, failed, avg_ms, min_ms, max_ms = row
                if avg_ms and avg_ms > 0 and max_ms and min_ms:
                    variability = round((max_ms - min_ms) / avg_ms * 100, 1)
                else:
                    variability = None
                formatted_results.append((
                    query_name, total, passed, timeout, failed,
                    round(avg_ms, 2) if avg_ms else None,
                    round(min_ms, 2) if min_ms else None,
                    round(max_ms, 2) if max_ms else None,
                    variability
                ))

            headers = ["Query", "Runs", "Passed", "Timeout", "Failed", "Avg (ms)", "Min (ms)", "Max (ms)", "Var %"]
            format_table(headers, formatted_results)
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
    parser.add_argument(
        "--tpcds",
        action="store_true",
        help="Show TPC-DS benchmark results"
    )

    args = parser.parse_args()

    # Get database connection
    db_path = get_db_path()
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        print("Run benchmarks first: make benchmark-tpch")
        sys.exit(1)

    db, cursor = get_connection()

    # Execute requested query
    if args.latest:
        query_latest(cursor)
    elif args.trend is not None:
        query_name = args.trend if isinstance(args.trend, str) else None
        query_trend(cursor, query_name)
    elif args.regressions:
        query_regressions(cursor)
    elif args.improvements:
        query_improvements(cursor)
    elif args.stats:
        query_stats(cursor)
    elif args.comparison:
        query_comparison(cursor)
    elif args.history:
        query_history(cursor)
    elif args.tpcc:
        query_tpcc(cursor)
    elif args.sysbench:
        query_sysbench(cursor)
    elif args.tpcds:
        query_tpcds(cursor)
    else:
        # Default: show latest TPC-H
        query_latest(cursor)
        print("\n")
        query_stats(cursor)


if __name__ == "__main__":
    main()
