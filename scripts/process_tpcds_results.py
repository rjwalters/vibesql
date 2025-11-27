#!/usr/bin/env python3
"""
Process TPC-DS benchmark results and store them in the dogfooding database.

This script parses the Criterion benchmark output from tpcds_benchmark and stores
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

    # Look for TPC-DS benchmark directories
    # Pattern: tpcds_queries/vibesql/Q1, tpcds_sanity/vibesql/table_scan, etc.
    for bench_group in criterion_dir.glob("tpcds_*"):
        if not bench_group.is_dir():
            continue

        group_name = bench_group.name  # e.g., "tpcds_queries" or "tpcds_sanity"

        # Each benchmark group has subdirectories for each engine/query combo
        for engine_dir in bench_group.iterdir():
            if not engine_dir.is_dir():
                continue

            engine = engine_dir.name.lower()  # e.g., "vibesql", "sqlite", "duckdb"

            for query_dir in engine_dir.iterdir():
                if not query_dir.is_dir():
                    continue

                query_name = query_dir.name  # e.g., "Q1", "Q2", etc.

                # Read estimates.json for timing data
                estimates_file = query_dir / "new" / "estimates.json"
                if not estimates_file.exists():
                    continue

                try:
                    with open(estimates_file) as f:
                        estimates = json.load(f)

                    mean_ns = estimates.get("mean", {}).get("point_estimate", 0)
                    std_dev_ns = estimates.get("std_dev", {}).get("point_estimate", 0)
                    median_ns = estimates.get("median", {}).get("point_estimate", 0)

                    # Read sample.json for iteration count
                    sample_file = query_dir / "new" / "sample.json"
                    iterations = 0
                    if sample_file.exists():
                        with open(sample_file) as f:
                            sample = json.load(f)
                            iterations = len(sample.get("times", []))

                    # Convert ns to ms for consistency with TPC-H
                    mean_ms = mean_ns / 1_000_000
                    std_dev_ms = std_dev_ns / 1_000_000
                    median_ms = median_ns / 1_000_000

                    results.append({
                        'database_engine': engine,
                        'query_name': query_name,
                        'group_name': group_name,
                        'mean_time_ms': mean_ms,
                        'std_dev_ms': std_dev_ms,
                        'median_time_ms': median_ms,
                        'iterations': iterations,
                        'status': 'passed'
                    })

                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Warning: Failed to parse {estimates_file}: {e}")
                    continue

    return results


def parse_criterion_output(output: str) -> List[Dict]:
    """
    Parse Criterion benchmark output from stdout.

    Handles format like:
    tpcds_queries/vibesql/Q1  time:   [45.123 ms 45.456 ms 45.789 ms]
    tpcds_sanity/vibesql/table_scan  time:   [1.234 us 1.345 us 1.456 us]
    """
    results = []

    # Pattern: group/engine/query  time:   [low mean high]
    pattern = re.compile(
        r'^(tpcds_\w+)/(\w+)/(\w+)\s+'
        r'time:\s+\[([\d.]+)\s+(us|ms|ns|s)\s+'
        r'([\d.]+)\s+(us|ms|ns|s)\s+'
        r'([\d.]+)\s+(us|ms|ns|s)\]',
        re.MULTILINE
    )

    for match in pattern.finditer(output):
        group_name = match.group(1)  # e.g., "tpcds_queries"
        engine = match.group(2).lower()  # e.g., "vibesql"
        query_name = match.group(3)  # e.g., "Q1"

        # Parse mean time (middle value)
        mean_val = float(match.group(6))
        mean_unit = match.group(7)

        # Convert to milliseconds
        if mean_unit == 'ns':
            mean_ms = mean_val / 1_000_000
        elif mean_unit == 'us':
            mean_ms = mean_val / 1000
        elif mean_unit == 's':
            mean_ms = mean_val * 1000
        else:  # ms
            mean_ms = mean_val

        results.append({
            'database_engine': engine,
            'query_name': query_name,
            'group_name': group_name,
            'mean_time_ms': mean_ms,
            'std_dev_ms': None,
            'median_time_ms': None,
            'iterations': None,
            'status': 'passed'
        })

    return results


def insert_tpcds_results(db_path: Path, results: List[Dict],
                          scale_factor: float = 0.01,
                          notes: Optional[str] = None):
    """Insert TPC-DS results into the database."""
    git_commit, git_branch = get_git_info()

    # Filter to only vibesql results for the main tracking (comparison data stored separately)
    vibesql_results = [r for r in results if r.get('database_engine') == 'vibesql']

    if not vibesql_results:
        vibesql_results = results  # Use all if no vibesql-specific results

    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()

        # Insert benchmark run
        cursor.execute("""
            INSERT INTO benchmark_runs (
                timestamp, git_commit, git_branch, benchmark_suite,
                scale_factor, total_queries, passed_queries, failed_queries,
                timeout_queries, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().isoformat(),
            git_commit,
            git_branch,
            'tpcds',
            str(scale_factor),
            len(vibesql_results),
            len([r for r in vibesql_results if r.get('status') == 'passed']),
            len([r for r in vibesql_results if r.get('status') == 'failed']),
            len([r for r in vibesql_results if r.get('status') == 'timeout']),
            notes
        ))

        run_id = cursor.lastrowid

        # Insert individual results into benchmark_results table
        for result in vibesql_results:
            cursor.execute("""
                INSERT INTO benchmark_results (
                    run_id, query_name, status,
                    execution_time_ms, total_time_ms, row_count
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                result.get('query_name', 'unknown'),
                result.get('status', 'passed'),
                result.get('mean_time_ms'),
                result.get('mean_time_ms'),  # Using mean as total
                None  # row_count not available from Criterion
            ))

        conn.commit()

        print(f"\nTPC-DS results stored in database")
        print(f"   Run ID: {run_id}")
        print(f"   Commit: {git_commit or 'unknown'}")
        print(f"   Queries: {len(vibesql_results)}")
        print(f"   Scale Factor: {scale_factor}")

        # Show summary by group
        by_group = {}
        for r in vibesql_results:
            group = r.get('group_name', 'unknown')
            if group not in by_group:
                by_group[group] = []
            by_group[group].append(r)

        for group, group_results in sorted(by_group.items()):
            avg_ms = sum(r.get('mean_time_ms', 0) for r in group_results) / len(group_results)
            print(f"   {group}: {len(group_results)} queries, avg {avg_ms:.2f} ms")

        # Show slowest queries
        sorted_results = sorted(vibesql_results, key=lambda r: r.get('mean_time_ms', 0), reverse=True)
        print("\n   Slowest queries:")
        for r in sorted_results[:5]:
            print(f"      {r.get('query_name')}: {r.get('mean_time_ms', 0):.2f} ms")

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Process TPC-DS benchmark results and store in database"
    )
    parser.add_argument(
        "--criterion-dir",
        type=str,
        default="target/criterion",
        help="Path to Criterion output directory (default: target/criterion)"
    )
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=0.01,
        help="TPC-DS scale factor used for benchmarks (default: 0.01)"
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
            print("Run benchmarks first: cargo bench --bench tpcds_benchmark")
            sys.exit(1)
        results = parse_criterion_estimates(criterion_path)

    print(f"Processing TPC-DS benchmark results...")

    if not results:
        print("Error: No TPC-DS benchmark results found")
        sys.exit(1)

    print(f"   Found {len(results)} benchmark results")

    # Insert results
    insert_tpcds_results(db_path, results, args.scale_factor, args.notes)


if __name__ == "__main__":
    main()
