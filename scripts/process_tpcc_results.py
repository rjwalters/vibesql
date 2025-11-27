#!/usr/bin/env python3
"""
Process TPC-C benchmark results and store them in the dogfooding database.

This script parses the output from the TPC-C benchmark executable and stores
the results in the VibeSQL database for performance tracking over time.
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
        ).strip()[:8]

        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True
        ).strip()

        return commit, branch
    except:
        return None, None


def get_db_path() -> Path:
    """Get the path to the dogfooding database."""
    return Path.home() / ".vibesql" / "test_results" / "sqllogictest_results.vbsql"


def init_benchmark_schema(db_path: Path):
    """Initialize benchmark tables in the database if they don't exist."""
    schema_path = get_repo_root() / "scripts" / "benchmark_results_schema.sql"

    if not schema_path.exists():
        print(f"Error: Schema file not found: {schema_path}")
        sys.exit(1)

    with open(schema_path) as f:
        schema_sql = f.read()

    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(schema_sql)
        conn.commit()
    finally:
        conn.close()


def parse_tpcc_output(output: str) -> Tuple[List[Dict], Dict]:
    """
    Parse TPC-C benchmark output.

    Returns:
        - List of per-engine results
        - Summary stats
    """
    results = []
    current_engine = None
    current_result = None

    lines = output.split('\n')

    for line in lines:
        # Detect engine section
        if '--- VibeSQL Benchmark ---' in line:
            current_engine = 'vibesql'
            current_result = {'database_engine': 'vibesql'}
        elif '--- SQLite Benchmark ---' in line:
            current_engine = 'sqlite'
            current_result = {'database_engine': 'sqlite'}
        elif '--- DuckDB Benchmark ---' in line:
            current_engine = 'duckdb'
            current_result = {'database_engine': 'duckdb'}

        if not current_result:
            continue

        # Parse transaction type
        txn_type_match = re.match(r'^Transaction type:\s+(.+)$', line)
        if txn_type_match:
            current_result['transaction_type'] = txn_type_match.group(1).lower().replace('-', '_')

        # Parse total transactions
        total_match = re.match(r'^Total transactions:\s+(\d+)', line)
        if total_match:
            current_result['transaction_count'] = int(total_match.group(1))

        # Parse successful transactions
        success_match = re.match(r'^Successful:\s+(\d+)', line)
        if success_match:
            current_result['successful_transactions'] = int(success_match.group(1))

        # Parse failed transactions
        failed_match = re.match(r'^Failed:\s+(\d+)', line)
        if failed_match:
            current_result['failed_transactions'] = int(failed_match.group(1))

        # Parse duration
        duration_match = re.match(r'^Duration:\s+(\d+)\s+ms', line)
        if duration_match:
            current_result['total_duration_ms'] = int(duration_match.group(1))

        # Parse throughput
        tps_match = re.match(r'^Throughput:\s+([\d.]+)\s+TPS', line)
        if tps_match:
            current_result['transactions_per_second'] = float(tps_match.group(1))

        # Parse individual transaction averages
        txn_avg_match = re.match(r'^(\w+(?:-\w+)?):\s+\d+\s+txns,\s+avg\s+([\d.]+)\s+us', line)
        if txn_avg_match:
            txn_name = txn_avg_match.group(1).lower().replace('-', '_')
            avg_us = float(txn_avg_match.group(2))
            if 'avg_latencies' not in current_result:
                current_result['avg_latencies'] = {}
            current_result['avg_latencies'][txn_name] = avg_us

        # Detect end of engine section (next engine or comparison summary)
        if ('=== Comparison Summary ===' in line or
            ('=== Done ===' in line)) and current_result and 'transaction_count' in current_result:
            # Calculate overall average latency
            if 'avg_latencies' in current_result and current_result.get('transaction_count', 0) > 0:
                total_latency = sum(current_result['avg_latencies'].values())
                current_result['avg_latency_us'] = total_latency / len(current_result['avg_latencies'])
            results.append(current_result)
            current_result = None

    # Handle case where last engine wasn't added
    if current_result and 'transaction_count' in current_result:
        if 'avg_latencies' in current_result and current_result.get('transaction_count', 0) > 0:
            total_latency = sum(current_result['avg_latencies'].values())
            current_result['avg_latency_us'] = total_latency / len(current_result['avg_latencies'])
        results.append(current_result)

    summary = {
        'total_engines': len(results),
        'total_transactions': sum(r.get('transaction_count', 0) for r in results)
    }

    return results, summary


def insert_tpcc_results(db_path: Path, results: List[Dict],
                         scale_factor: int = 1, duration_secs: int = 60,
                         notes: Optional[str] = None):
    """Insert TPC-C results into the database."""
    git_commit, git_branch = get_git_info()

    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()

        # Insert benchmark run
        cursor.execute("""
            INSERT INTO benchmark_runs (
                timestamp, git_commit, git_branch, benchmark_suite,
                scale_factor, timeout_secs, total_queries, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().isoformat(),
            git_commit,
            git_branch,
            'tpcc',
            str(scale_factor),
            duration_secs,
            sum(r.get('transaction_count', 0) for r in results),
            notes
        ))

        run_id = cursor.lastrowid

        # Insert individual results
        for result in results:
            cursor.execute("""
                INSERT INTO tpcc_results (
                    run_id, database_engine, transaction_type,
                    transaction_count, avg_latency_us, total_duration_ms,
                    transactions_per_second, successful_transactions, failed_transactions
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                result.get('database_engine', 'unknown'),
                result.get('transaction_type', 'mixed'),
                result.get('transaction_count', 0),
                result.get('avg_latency_us'),
                result.get('total_duration_ms'),
                result.get('transactions_per_second'),
                result.get('successful_transactions', 0),
                result.get('failed_transactions', 0)
            ))

        conn.commit()

        print(f"\nTPC-C results stored in database")
        print(f"   Run ID: {run_id}")
        print(f"   Commit: {git_commit or 'unknown'}")
        print(f"   Engines: {len(results)}")
        for r in results:
            print(f"   {r.get('database_engine', 'unknown')}: {r.get('transactions_per_second', 0):.2f} TPS")

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Process TPC-C benchmark results and store in database"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="/tmp/tpcc_results.txt",
        help="Path to TPC-C benchmark output file (default: /tmp/tpcc_results.txt)"
    )
    parser.add_argument(
        "--scale-factor",
        type=int,
        default=1,
        help="Scale factor (number of warehouses) used for benchmarks (default: 1)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Duration in seconds used for benchmarks (default: 60)"
    )
    parser.add_argument(
        "--notes",
        type=str,
        help="Optional notes about this benchmark run"
    )
    parser.add_argument(
        "--stdin",
        action="store_true",
        help="Read benchmark output from stdin instead of file"
    )

    args = parser.parse_args()

    # Get database path
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Initialize schema
    init_benchmark_schema(db_path)

    # Read benchmark output
    if args.stdin:
        output = sys.stdin.read()
    else:
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"Error: Input file not found: {input_path}")
            sys.exit(1)
        with open(input_path) as f:
            output = f.read()

    print(f"Processing TPC-C benchmark results...")

    results, summary = parse_tpcc_output(output)

    if not results:
        print("Error: No TPC-C benchmark results found in input")
        sys.exit(1)

    print(f"   Found {summary['total_engines']} engines, {summary['total_transactions']} total transactions")

    # Insert results
    insert_tpcc_results(db_path, results, args.scale_factor, args.duration, args.notes)


if __name__ == "__main__":
    main()
