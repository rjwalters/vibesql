#!/usr/bin/env python3
"""
Process TPC-H benchmark results and store them in the dogfooding database.

This script parses the output from bench-tpch.sh and stores the results
in the VibeSQL database for performance tracking over time.
"""

import argparse
import re
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def get_repo_root() -> Path:
    """Get the repository root directory."""
    script_dir = Path(__file__).parent
    return script_dir.parent


def get_git_info() -> Tuple[Optional[str], Optional[str]]:
    """Get current git commit hash and branch name."""
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True
        ).strip()[:8]  # Short hash

        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True
        ).strip()

        return commit, branch
    except:
        return None, None


def get_db_path() -> Path:
    """Get the path to the benchmark results database (SQLite format)."""
    return Path.home() / ".vibesql" / "test_results" / "benchmark_results.db"


def init_benchmark_schema(db_path: Path):
    """Initialize benchmark tables in the database if they don't exist."""
    schema_path = get_repo_root() / "scripts" / "benchmark_results_schema.sql"

    if not schema_path.exists():
        print(f"❌ Schema file not found: {schema_path}")
        sys.exit(1)

    with open(schema_path) as f:
        schema_sql = f.read()

    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(schema_sql)
        conn.commit()
        print(f"✓ Benchmark schema initialized in {db_path}")
    finally:
        conn.close()


def parse_tpch_output(output_file: Path) -> Tuple[List[Dict], Dict]:
    """
    Parse TPC-H benchmark output file.

    Returns:
        - List of query results (dicts with query_name, status, timings, etc.)
        - Summary stats (total, passed, failed, timeout)
    """
    if not output_file.exists():
        print(f"❌ Output file not found: {output_file}")
        sys.exit(1)

    with open(output_file) as f:
        content = f.read()

    results = []
    current_query = None

    # Parse line by line
    for line in content.split('\n'):
        # Query header: === Q1 ===
        query_match = re.match(r'^=== (Q\d+) ===$', line)
        if query_match:
            current_query = query_match.group(1)
            continue

        if not current_query:
            continue

        # Parse timing: Parse: 1.234ms
        parse_match = re.match(r'^\s*Parse:\s+([\d.]+)(ms|s|µs)', line)
        if parse_match:
            parse_time = float(parse_match.group(1))
            unit = parse_match.group(2)
            if unit == 's':
                parse_time *= 1000
            elif unit == 'µs':
                parse_time /= 1000
            results.append({
                'query_name': current_query,
                'parse_time_ms': parse_time,
                'executor_creation_time_ms': None,
                'execution_time_ms': None,
                'total_time_ms': None,
                'row_count': None,
                'status': None,
                'error_message': None
            })
            continue

        # Executor creation: Executor: 0.456ms (timeout: 30s)
        executor_match = re.match(r'^\s*Executor:\s+([\d.]+)(ms|s|µs)', line)
        if executor_match and results and results[-1]['query_name'] == current_query:
            exec_time = float(executor_match.group(1))
            unit = executor_match.group(2)
            if unit == 's':
                exec_time *= 1000
            elif unit == 'µs':
                exec_time /= 1000
            results[-1]['executor_creation_time_ms'] = exec_time
            continue

        # Execution with success: Execute: 123.456ms (1000 rows)
        execute_success_match = re.match(r'^\s*Execute:\s+([\d.]+)(ms|s|µs)\s+\((\d+)\s+rows?\)', line)
        if execute_success_match and results and results[-1]['query_name'] == current_query:
            exec_time = float(execute_success_match.group(1))
            unit = execute_success_match.group(2)
            if unit == 's':
                exec_time *= 1000
            elif unit == 'µs':
                exec_time /= 1000
            results[-1]['execution_time_ms'] = exec_time
            results[-1]['row_count'] = int(execute_success_match.group(3))
            results[-1]['status'] = 'passed'
            continue

        # Execution with error: Execute: ERROR: <message>
        execute_error_match = re.match(r'^\s*Execute:\s+ERROR:\s+(.+)$', line)
        if execute_error_match and results and results[-1]['query_name'] == current_query:
            results[-1]['status'] = 'error'
            results[-1]['error_message'] = execute_error_match.group(1)
            continue

        # Parse error: ERROR: <message>
        parse_error_match = re.match(r'^\s*ERROR:\s+(.+)$', line)
        if parse_error_match and results and results[-1]['query_name'] == current_query:
            results[-1]['status'] = 'error'
            results[-1]['error_message'] = parse_error_match.group(1)
            continue

        # Total time: TOTAL: 125.678ms
        total_match = re.match(r'^\s*TOTAL:\s+([\d.]+)(ms|s|µs)', line)
        if total_match and results and results[-1]['query_name'] == current_query:
            total_time = float(total_match.group(1))
            unit = total_match.group(2)
            if unit == 's':
                total_time *= 1000
            elif unit == 'µs':
                total_time /= 1000
            results[-1]['total_time_ms'] = total_time
            continue

        # Timeout: TIMEOUT
        if 'TIMEOUT' in line and results and results[-1]['query_name'] == current_query:
            results[-1]['status'] = 'timeout'
            continue

    # Mark incomplete queries (have parse time but no status) as 'incomplete'
    for result in results:
        if result['status'] is None:
            result['status'] = 'incomplete'
            result['error_message'] = 'Query output was truncated or incomplete'

    # Calculate summary stats
    total_queries = len(results)
    passed_queries = sum(1 for r in results if r['status'] == 'passed')
    failed_queries = sum(1 for r in results if r['status'] == 'error')
    timeout_queries = sum(1 for r in results if r['status'] == 'timeout')

    summary = {
        'total_queries': total_queries,
        'passed_queries': passed_queries,
        'failed_queries': failed_queries,
        'timeout_queries': timeout_queries
    }

    return results, summary


def insert_benchmark_results(db_path: Path, results: List[Dict], summary: Dict,
                              timeout_secs: int = 30, notes: Optional[str] = None):
    """Insert benchmark results into the database."""
    git_commit, git_branch = get_git_info()

    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()

        # Insert benchmark run
        cursor.execute("""
            INSERT INTO benchmark_runs (
                timestamp, git_commit, git_branch, benchmark_suite,
                timeout_secs, total_queries, passed_queries,
                failed_queries, timeout_queries, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().isoformat(),
            git_commit,
            git_branch,
            'tpch',
            timeout_secs,
            summary['total_queries'],
            summary['passed_queries'],
            summary['failed_queries'],
            summary['timeout_queries'],
            notes
        ))

        run_id = cursor.lastrowid

        # Insert individual query results
        for result in results:
            cursor.execute("""
                INSERT INTO benchmark_results (
                    run_id, query_name, status,
                    parse_time_ms, executor_creation_time_ms,
                    execution_time_ms, total_time_ms,
                    row_count, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                result['query_name'],
                result['status'],
                result['parse_time_ms'],
                result['executor_creation_time_ms'],
                result['execution_time_ms'],
                result['total_time_ms'],
                result['row_count'],
                result['error_message']
            ))

        conn.commit()

        print(f"\n✅ Benchmark results stored in database")
        print(f"   Run ID: {run_id}")
        print(f"   Commit: {git_commit or 'unknown'}")
        print(f"   Branch: {git_branch or 'unknown'}")
        print(f"   Passed: {summary['passed_queries']}/{summary['total_queries']}")

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Process TPC-H benchmark results and store in database"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="/tmp/tpch_results.txt",
        help="Path to TPC-H benchmark output file (default: /tmp/tpch_results.txt)"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout in seconds used for benchmarks (default: 30)"
    )
    parser.add_argument(
        "--notes",
        type=str,
        help="Optional notes about this benchmark run"
    )
    parser.add_argument(
        "--init-schema",
        action="store_true",
        help="Initialize benchmark schema (run once)"
    )

    args = parser.parse_args()

    # Get database path
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Initialize schema if requested
    if args.init_schema:
        init_benchmark_schema(db_path)
        return

    # Parse benchmark output
    input_path = Path(args.input)
    print(f"📊 Processing TPC-H benchmark results from: {input_path}")

    results, summary = parse_tpch_output(input_path)

    if not results:
        print("❌ No benchmark results found in input file")
        sys.exit(1)

    print(f"   Found {summary['total_queries']} queries")
    print(f"   Passed: {summary['passed_queries']}")
    print(f"   Failed: {summary['failed_queries']}")
    print(f"   Timeout: {summary['timeout_queries']}")

    # Initialize schema if tables don't exist
    init_benchmark_schema(db_path)

    # Insert results
    insert_benchmark_results(db_path, results, summary, args.timeout, args.notes)


if __name__ == "__main__":
    main()
