#!/usr/bin/env python3
"""
Process Sysbench OLTP benchmark results and store them in the dogfooding database.

This script parses the Criterion benchmark output from sysbench_oltp and stores
the results in the VibeSQL database for performance tracking over time.
"""

import argparse
import json
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


def parse_criterion_estimates(criterion_dir: Path) -> List[Dict]:
    """
    Parse Criterion benchmark results from the target/criterion directory.

    Returns list of benchmark results with timing data.
    """
    results = []

    # Look for sysbench benchmark directories
    for bench_group in criterion_dir.glob("sysbench_*"):
        if not bench_group.is_dir():
            continue

        test_name = bench_group.name.replace("sysbench_", "")

        # Each benchmark group has subdirectories for each database/size combo
        for bench_run in bench_group.iterdir():
            if not bench_run.is_dir():
                continue

            # Parse engine and table size from directory name
            # Format: "vibesql/10000" or "sqlite/10000"
            parts = bench_run.name.split("/") if "/" in bench_run.name else [bench_run.name]
            if len(parts) >= 1:
                engine = parts[0].lower()
                table_size = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 10000
            else:
                continue

            # Read estimates.json for timing data
            estimates_file = bench_run / "new" / "estimates.json"
            if not estimates_file.exists():
                continue

            try:
                with open(estimates_file) as f:
                    estimates = json.load(f)

                mean_ns = estimates.get("mean", {}).get("point_estimate", 0)
                std_dev_ns = estimates.get("std_dev", {}).get("point_estimate", 0)
                median_ns = estimates.get("median", {}).get("point_estimate", 0)

                # Read sample.json for iteration count
                sample_file = bench_run / "new" / "sample.json"
                iterations = 0
                if sample_file.exists():
                    with open(sample_file) as f:
                        sample = json.load(f)
                        iterations = len(sample.get("times", []))

                results.append({
                    'database_engine': engine,
                    'test_name': test_name,
                    'table_size': table_size,
                    'mean_time_ns': mean_ns,
                    'std_dev_ns': std_dev_ns,
                    'median_time_ns': median_ns,
                    'iterations': iterations
                })

            except (json.JSONDecodeError, KeyError) as e:
                print(f"Warning: Failed to parse {estimates_file}: {e}")
                continue

    return results


def parse_criterion_output(output: str) -> List[Dict]:
    """
    Parse Criterion benchmark output from stdout.

    Handles format like:
    sysbench_point_select/vibesql/10000  time:   [45.123 us 45.456 us 45.789 us]
    """
    results = []

    # Pattern: group/engine/size  time:   [low mean high]
    pattern = re.compile(
        r'^(\w+)/(\w+)/(\d+)\s+'
        r'time:\s+\[([\d.]+)\s+(us|ms|ns)\s+'
        r'([\d.]+)\s+(us|ms|ns)\s+'
        r'([\d.]+)\s+(us|ms|ns)\]',
        re.MULTILINE
    )

    for match in pattern.finditer(output):
        test_name = match.group(1).replace("sysbench_", "")
        engine = match.group(2).lower()
        table_size = int(match.group(3))

        # Parse mean time
        mean_val = float(match.group(6))
        mean_unit = match.group(7)

        # Convert to nanoseconds
        if mean_unit == 'us':
            mean_ns = mean_val * 1000
        elif mean_unit == 'ms':
            mean_ns = mean_val * 1_000_000
        else:  # ns
            mean_ns = mean_val

        results.append({
            'database_engine': engine,
            'test_name': test_name,
            'table_size': table_size,
            'mean_time_ns': mean_ns,
            'std_dev_ns': None,
            'median_time_ns': None,
            'iterations': None
        })

    return results


def insert_sysbench_results(db_path: Path, results: List[Dict],
                             table_size: int = 10000,
                             notes: Optional[str] = None):
    """Insert Sysbench results into the database."""
    git_commit, git_branch = get_git_info()

    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()

        # Insert benchmark run
        cursor.execute("""
            INSERT INTO benchmark_runs (
                timestamp, git_commit, git_branch, benchmark_suite,
                scale_factor, total_queries, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().isoformat(),
            git_commit,
            git_branch,
            'sysbench',
            str(table_size),
            len(results),
            notes
        ))

        run_id = cursor.lastrowid

        # Insert individual results
        for result in results:
            cursor.execute("""
                INSERT INTO sysbench_results (
                    run_id, database_engine, test_name, table_size,
                    mean_time_ns, std_dev_ns, median_time_ns, iterations
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                result.get('database_engine', 'unknown'),
                result.get('test_name', 'unknown'),
                result.get('table_size', table_size),
                result.get('mean_time_ns'),
                result.get('std_dev_ns'),
                result.get('median_time_ns'),
                result.get('iterations')
            ))

        conn.commit()

        print(f"\nSysbench results stored in database")
        print(f"   Run ID: {run_id}")
        print(f"   Commit: {git_commit or 'unknown'}")
        print(f"   Results: {len(results)}")

        # Group by test for summary
        by_test = {}
        for r in results:
            test = r.get('test_name', 'unknown')
            if test not in by_test:
                by_test[test] = []
            by_test[test].append(r)

        for test, test_results in by_test.items():
            print(f"   {test}:")
            for r in test_results:
                mean_us = r.get('mean_time_ns', 0) / 1000
                print(f"      {r.get('database_engine', 'unknown')}: {mean_us:.2f} us")

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Process Sysbench OLTP benchmark results and store in database"
    )
    parser.add_argument(
        "--criterion-dir",
        type=str,
        default="target/criterion",
        help="Path to Criterion output directory (default: target/criterion)"
    )
    parser.add_argument(
        "--table-size",
        type=int,
        default=10000,
        help="Table size used for benchmarks (default: 10000)"
    )
    parser.add_argument(
        "--notes",
        type=str,
        help="Optional notes about this benchmark run"
    )
    parser.add_argument(
        "--stdin",
        action="store_true",
        help="Read Criterion output from stdin instead of directory"
    )

    args = parser.parse_args()

    # Get database path
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Initialize schema
    init_benchmark_schema(db_path)

    # Parse benchmark results
    if args.stdin:
        output = sys.stdin.read()
        results = parse_criterion_output(output)
    else:
        criterion_path = Path(args.criterion_dir)
        if not criterion_path.exists():
            print(f"Error: Criterion directory not found: {criterion_path}")
            print("Run benchmarks first: cargo bench --bench sysbench_oltp")
            sys.exit(1)
        results = parse_criterion_estimates(criterion_path)

    print(f"Processing Sysbench benchmark results...")

    if not results:
        print("Error: No Sysbench benchmark results found")
        sys.exit(1)

    print(f"   Found {len(results)} benchmark results")

    # Insert results
    insert_sysbench_results(db_path, results, args.table_size, args.notes)


if __name__ == "__main__":
    main()
