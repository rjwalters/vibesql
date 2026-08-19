#!/usr/bin/env python3
"""
Analyze benchmark stability over time by computing aggregate metrics.

This script computes the sum of all SQLite benchmark timings at each point in time
to detect changes that might indicate bugs being fixed or benchmark conditions changing.

Usage:
    ./scripts/analyze_benchmark_stability.py
    ./scripts/analyze_benchmark_stability.py --engine vibesql
    ./scripts/analyze_benchmark_stability.py --threshold 0.2  # 20% change detection
"""

import argparse
import os
import sys
from typing import Dict, List, Tuple
from collections import defaultdict

# Import our VibeSQL helper module (works from any cwd)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from vibesql_db import get_connection


def get_sysbench_aggregate(cursor, engine: str = 'sqlite') -> List[Tuple]:
    """Get aggregate sysbench timing per run for specified engine."""
    cursor.execute(f"""
        SELECT r.run_id, r.run_timestamp, r.git_commit,
               SUM(sb.mean_time_ns) as total_time_ns,
               COUNT(*) as test_count
        FROM sysbench_results sb
        JOIN benchmark_runs r ON sb.run_id = r.run_id
        WHERE sb.database_engine = '{engine}'
          AND sb.table_size = 10000
        GROUP BY r.run_id, r.run_timestamp, r.git_commit
    """)
    return cursor.fetchall()


def get_tpch_aggregate(cursor) -> List[Tuple]:
    """Get aggregate TPC-H timing per run (VibeSQL only since SQLite doesn't run TPC-H)."""
    cursor.execute("""
        SELECT r.run_id, r.run_timestamp, r.git_commit,
               SUM(br.execution_time_ms) as total_time_ms,
               COUNT(*) as query_count
        FROM benchmark_results br
        JOIN benchmark_runs r ON br.run_id = r.run_id
        WHERE r.benchmark_suite = 'tpch'
          AND br.status = 'passed'
        GROUP BY r.run_id, r.run_timestamp, r.git_commit
    """)
    return cursor.fetchall()


def detect_changes(data: List[Tuple], threshold: float = 0.15) -> List[Dict]:
    """
    Detect significant changes in aggregate timing.

    Args:
        data: List of (run_id, timestamp, commit, total_time, count) tuples
        threshold: Percentage change to flag as significant (0.15 = 15%)

    Returns:
        List of change events with metadata
    """
    changes = []
    sorted_data = sorted(data, key=lambda x: x[1] if x[1] else '')

    prev_time = None
    prev_run = None
    prev_commit = None

    for run_id, ts, commit, total_time, count in sorted_data:
        if total_time is None or total_time == 0:
            continue

        total_time = float(total_time)

        if prev_time is not None:
            pct_change = (total_time - prev_time) / prev_time

            if abs(pct_change) > threshold:
                changes.append({
                    'run_id': run_id,
                    'timestamp': ts,
                    'commit': commit,
                    'prev_run_id': prev_run,
                    'prev_commit': prev_commit,
                    'prev_time': prev_time,
                    'new_time': total_time,
                    'pct_change': pct_change,
                    'direction': 'FASTER' if pct_change < 0 else 'SLOWER'
                })

        prev_time = total_time
        prev_run = run_id
        prev_commit = commit

    return changes


def print_timeline(data: List[Tuple], engine: str, unit: str = 'ns'):
    """Print timeline of aggregate timings."""
    sorted_data = sorted(data, key=lambda x: x[1] if x[1] else '')

    divisor = 1_000_000 if unit == 'ms' else 1_000 if unit == 'us' else 1

    total_header = f"Total ({unit})"
    print(f"\n{'Run':<6} {'DateTime':<18} {'Commit':<10} {total_header:<14} {'Tests':<6} {'Status'}")
    print("-" * 70)

    prev_time = None
    for run_id, ts, commit, total_time, count in sorted_data:
        if total_time is None:
            continue

        total_time = float(total_time) / divisor
        date = ts[:16] if ts else 'N/A'

        status = ""
        if prev_time:
            pct = (total_time - prev_time) / prev_time * 100
            if abs(pct) > 15:
                status = f"{'↑' if pct > 0 else '↓'} {abs(pct):.0f}%"

        print(f"{run_id:<6} {date:<18} {commit or 'N/A':<10} {total_time:<14.1f} {count:<6} {status}")
        prev_time = total_time


def main():
    parser = argparse.ArgumentParser(description="Analyze benchmark stability over time")
    parser.add_argument("--engine", default="sqlite", help="Engine to analyze (default: sqlite)")
    parser.add_argument("--threshold", type=float, default=0.15, help="Change threshold (default: 0.15 = 15%%)")
    parser.add_argument("--tpch", action="store_true", help="Also analyze TPC-H results")
    args = parser.parse_args()

    db, cursor = get_connection()

    # Accumulates change events across all analyzed suites; stays empty when
    # there is no data, so the summary degrades gracefully instead of raising
    # NameError.
    changes = []

    # Analyze Sysbench
    print(f"=== {args.engine.upper()} Sysbench Aggregate Timeline ===")
    try:
        sysbench_data = get_sysbench_aggregate(cursor, args.engine)
    except Exception as e:
        print(f"Could not query sysbench results ({e}).")
        sysbench_data = []

    if sysbench_data:
        print_timeline(sysbench_data, args.engine, unit='us')

        changes = detect_changes(sysbench_data, args.threshold)
        if changes:
            print(f"\n=== Significant Changes Detected (>{args.threshold*100:.0f}% change) ===\n")
            for c in changes:
                print(f"Run {c['prev_run_id']} → {c['run_id']}: {c['direction']} by {abs(c['pct_change'])*100:.1f}%")
                print(f"  Commits: {c['prev_commit']} → {c['commit']}")
                print(f"  Time: {c['prev_time']/1000:.1f}us → {c['new_time']/1000:.1f}us")
                print()
    else:
        print(f"No {args.engine} sysbench data found.")

    # Optionally analyze TPC-H
    if args.tpch:
        print("\n" + "="*70)
        print("=== TPC-H Aggregate Timeline ===")
        try:
            tpch_data = get_tpch_aggregate(cursor)
        except Exception as e:
            print(f"Could not query TPC-H results ({e}).")
            tpch_data = []

        if tpch_data:
            print_timeline(tpch_data, 'vibesql', unit='ms')

            tpch_changes = detect_changes(tpch_data, args.threshold)
            changes.extend(tpch_changes)
            if tpch_changes:
                print(f"\n=== TPC-H Significant Changes (>{args.threshold*100:.0f}%) ===\n")
                for c in tpch_changes:
                    print(f"Run {c['prev_run_id']} → {c['run_id']}: {c['direction']} by {abs(c['pct_change'])*100:.1f}%")
                    print(f"  Commits: {c['prev_commit']} → {c['commit']}")
                    print()
        else:
            print("No TPC-H data found.")

    # Summary recommendation
    print("\n" + "="*70)
    print("=== Recommendation ===")

    if changes:
        first_change_run = min(c['run_id'] for c in changes)
        print(f"\nFirst significant change detected at run {first_change_run}")
        print(f"Consider keeping data from run {first_change_run} onwards if that")
        print("represents when the benchmark infrastructure stabilized.")
    else:
        print("\nNo significant changes detected - data appears stable.")


if __name__ == "__main__":
    main()
