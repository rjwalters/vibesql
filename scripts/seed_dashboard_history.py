#!/usr/bin/env python3
"""
Seed the dashboard with historical data from existing benchmark runs.

This is a one-time script to backfill the dashboard.json with historical
data points from all benchmark runs stored in the database.
"""

import argparse
import json
import math
import os
import platform
import sqlite3
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# Schema version for the dashboard.json format
DASHBOARD_VERSION = "2.0"

# Thresholds for detecting improvements/regressions
CHANGE_THRESHOLD_PCT = 10.0


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
    except Exception:
        return None, None


def get_db_path() -> Path:
    """Get the path to the dogfooding database."""
    return Path.home() / ".vibesql" / "test_results" / "sqllogictest_results.vbsql"


def get_machine_info() -> Dict[str, str]:
    """Get information about the current machine."""
    system = platform.system()
    machine = platform.machine()

    # Check if running in GitHub Actions
    if os.environ.get("GITHUB_ACTIONS"):
        return {
            "system": f"GitHub Actions {platform.platform()}",
            "cpu": os.environ.get("RUNNER_ARCH", machine),
            "memory": "Standard GH Actions runner",
            "notes": "CI environment"
        }

    return {
        "system": f"{system} {platform.release()}",
        "cpu": machine,
        "memory": "Unknown",
        "notes": "Local development (seeded history)"
    }


def geometric_mean(values: List[float]) -> Optional[float]:
    """Calculate geometric mean of a list of values."""
    if not values:
        return None
    positive_values = [v for v in values if v and v > 0]
    if not positive_values:
        return None
    log_sum = sum(math.log(v) for v in positive_values)
    return math.exp(log_sum / len(positive_values))


def calculate_trend(current: Optional[float], previous: Optional[float]) -> Tuple[Optional[str], Optional[float]]:
    """
    Calculate trend direction and percentage change.

    Returns:
        (trend_direction, pct_change) where trend is "improving", "regressing", or "stable"
    """
    if current is None or previous is None or previous == 0:
        return None, None

    pct_change = ((current - previous) / previous) * 100

    if pct_change < -CHANGE_THRESHOLD_PCT:
        return "improving", round(pct_change, 2)
    elif pct_change > CHANGE_THRESHOLD_PCT:
        return "regressing", round(pct_change, 2)
    else:
        return "stable", round(pct_change, 2)


def parse_timestamp(ts: str) -> datetime:
    """Parse timestamp from various formats."""
    # Try ISO format with T separator
    if 'T' in ts:
        # Handle with or without microseconds
        try:
            return datetime.fromisoformat(ts.replace('Z', '+00:00'))
        except ValueError:
            pass
        try:
            return datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass
    # Fallback to date only
    try:
        return datetime.strptime(ts[:10], "%Y-%m-%d")
    except ValueError:
        return datetime.now()


def query_all_benchmark_runs(conn: sqlite3.Connection, days: Optional[int] = None) -> List[Dict]:
    """Query all benchmark runs from the database."""
    cursor = conn.cursor()

    # Build the query with optional date filter
    query = """
        SELECT run_id, timestamp, git_commit, git_branch, benchmark_suite,
               scale_factor, total_queries, passed_queries, failed_queries, timeout_queries
        FROM benchmark_runs
        ORDER BY timestamp ASC
    """

    if days:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        query = f"""
            SELECT run_id, timestamp, git_commit, git_branch, benchmark_suite,
                   scale_factor, total_queries, passed_queries, failed_queries, timeout_queries
            FROM benchmark_runs
            WHERE timestamp >= '{cutoff}'
            ORDER BY timestamp ASC
        """

    cursor.execute(query)
    runs = []
    for row in cursor.fetchall():
        runs.append({
            "run_id": row[0],
            "timestamp": row[1],
            "git_commit": row[2],
            "git_branch": row[3],
            "benchmark_suite": row[4],
            "scale_factor": row[5],
            "total_queries": row[6],
            "passed_queries": row[7],
            "failed_queries": row[8],
            "timeout_queries": row[9]
        })
    return runs


def query_tpch_results_for_run(conn: sqlite3.Connection, run_id: int) -> Dict[str, Any]:
    """Query TPC-H results for a specific run."""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT query_name, status, execution_time_ms, total_time_ms, error_message
        FROM benchmark_results
        WHERE run_id = ?
        ORDER BY query_name
    """, (run_id,))

    queries = {}
    passing_times = []

    for row in cursor.fetchall():
        query_name, status, exec_time, total_time, error = row
        queries[query_name] = {
            "status": status,
            "execution_time_ms": round(exec_time, 2) if exec_time else None,
            "total_time_ms": round(total_time, 2) if total_time else None,
            "error": error
        }
        if status == "passed" and exec_time:
            passing_times.append(exec_time)

    return {
        "queries": queries,
        "geo_mean_ms": round(geometric_mean(passing_times), 2) if passing_times else None,
        "passing_count": len(passing_times)
    }


def query_tpcc_results_for_run(conn: sqlite3.Connection, run_id: int) -> Dict[str, Any]:
    """Query TPC-C results for a specific run."""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT database_engine, transaction_type, transactions_per_second, avg_latency_us
        FROM tpcc_results
        WHERE run_id = ?
    """, (run_id,))

    results = {}
    vibesql_tps = None

    for row in cursor.fetchall():
        engine, txn_type, tps, latency = row
        if txn_type not in results:
            results[txn_type] = {}
        results[txn_type][engine] = {
            "tps": round(tps, 2) if tps else None,
            "latency_us": round(latency, 2) if latency else None
        }
        if engine == "vibesql" and txn_type == "mixed":
            vibesql_tps = round(tps, 2) if tps else None

    return {
        "transactions": results,
        "vibesql_tps": vibesql_tps
    }


def query_sysbench_results_for_run(conn: sqlite3.Connection, run_id: int) -> Dict[str, Any]:
    """Query Sysbench results for a specific run."""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT database_engine, test_name, table_size, mean_time_ns, std_dev_ns
        FROM sysbench_results
        WHERE run_id = ?
    """, (run_id,))

    tests = {}
    for row in cursor.fetchall():
        engine, test_name, table_size, mean_ns, std_ns = row
        key = f"{test_name}_{table_size}" if table_size else test_name
        if key not in tests:
            tests[key] = {"test_name": test_name, "table_size": table_size, "engines": {}}
        tests[key]["engines"][engine] = {
            "mean_us": round(mean_ns / 1000, 2) if mean_ns else None,
            "std_dev_us": round(std_ns / 1000, 2) if std_ns else None
        }

    return {"tests": tests}


def query_conformance_results(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Query SQLLogicTest conformance results from the database."""
    cursor = conn.cursor()

    # Check if test_results table exists
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='test_results'
    """)
    if not cursor.fetchone():
        return {}

    # Get total tests and pass rate
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as passed
        FROM test_results
    """)
    result = cursor.fetchone()

    if not result or not result[0]:
        return {}

    total, passed = result
    pass_rate = (passed / total * 100) if total > 0 else 0

    # Get file-level summary
    cursor.execute("""
        SELECT
            COUNT(DISTINCT file_name) as total_files,
            COUNT(DISTINCT CASE
                WHEN status = 'fail' THEN file_name
            END) as files_with_failures
        FROM test_results
    """)
    file_result = cursor.fetchone()
    total_files = file_result[0] if file_result else 0
    files_with_failures = file_result[1] if file_result else 0
    files_passing = total_files - files_with_failures

    return {
        "summary": {
            "total_tests": total,
            "passing": passed,
            "failing": total - passed,
            "pass_rate": round(pass_rate, 2)
        },
        "files": {
            "total": total_files,
            "passing": files_passing,
            "pass_rate": round(files_passing / total_files * 100, 2) if total_files > 0 else 0
        },
        "categories": {},
        "history": []
    }


def group_runs_by_date(runs: List[Dict]) -> Dict[str, List[Dict]]:
    """Group benchmark runs by date, keeping latest run per suite per day."""
    by_date = defaultdict(list)
    for run in runs:
        date = run["timestamp"][:10]  # Extract date from timestamp
        by_date[date].append(run)
    return dict(by_date)


def build_timeline_from_history(
    conn: sqlite3.Connection,
    runs: List[Dict],
    verbose: bool = False
) -> List[Dict]:
    """Build timeline entries from all historical runs."""
    # Group runs by date
    by_date = group_runs_by_date(runs)

    timeline = []
    seen_dates = set()

    # Process each day
    for date in sorted(by_date.keys()):
        if date in seen_dates:
            continue
        seen_dates.add(date)

        day_runs = by_date[date]

        # Get the latest TPC-H run for this day
        tpch_runs = [r for r in day_runs if r["benchmark_suite"] == "tpch"]
        tpch_geo_mean = None
        tpch_passing = None
        commit = None

        if tpch_runs:
            latest_tpch = max(tpch_runs, key=lambda x: x["timestamp"])
            commit = latest_tpch["git_commit"]
            tpch_passing = latest_tpch["passed_queries"]

            # Get detailed results to calculate geo mean
            tpch_data = query_tpch_results_for_run(conn, latest_tpch["run_id"])
            tpch_geo_mean = tpch_data.get("geo_mean_ms")

        # Get the latest TPC-C run for this day
        tpcc_runs = [r for r in day_runs if r["benchmark_suite"] == "tpcc"]
        tpcc_tps = None

        if tpcc_runs:
            latest_tpcc = max(tpcc_runs, key=lambda x: x["timestamp"])
            if not commit:
                commit = latest_tpcc["git_commit"]

            tpcc_data = query_tpcc_results_for_run(conn, latest_tpcc["run_id"])
            tpcc_tps = tpcc_data.get("vibesql_tps")

        # Use first available commit if we have none
        if not commit and day_runs:
            commit = day_runs[0].get("git_commit")

        entry = {
            "date": date,
            "commit": commit,
            "conformance_pass_rate": None,  # Could be populated if we track this per-run
            "tpch_geo_mean_ms": tpch_geo_mean,
            "tpch_passing": tpch_passing,
            "tpcc_tps": tpcc_tps,
            "events": []
        }

        timeline.append(entry)

        if verbose:
            print(f"  {date}: TPC-H geo={tpch_geo_mean}, TPC-C tps={tpcc_tps}, commit={commit}")

    # Sort by date descending (most recent first)
    timeline.sort(key=lambda x: x["date"], reverse=True)

    return timeline


def build_tpch_benchmark_data(conn: sqlite3.Connection, runs: List[Dict]) -> Dict[str, Any]:
    """Build TPC-H benchmark data from all historical runs."""
    tpch_runs = [r for r in runs if r["benchmark_suite"] == "tpch"]

    if not tpch_runs:
        return {}

    # Get latest run
    latest = max(tpch_runs, key=lambda x: x["timestamp"])
    latest_data = query_tpch_results_for_run(conn, latest["run_id"])

    # Build query data with history
    all_query_history = defaultdict(list)

    for run in sorted(tpch_runs, key=lambda x: x["timestamp"], reverse=True):
        run_data = query_tpch_results_for_run(conn, run["run_id"])
        date = run["timestamp"][:10]

        for query_name, query_info in run_data.get("queries", {}).items():
            if query_info.get("status") == "passed" and query_info.get("execution_time_ms"):
                all_query_history[query_name].append({
                    "date": date,
                    "ms": query_info["execution_time_ms"]
                })

    # Build final query structure
    queries = {}
    for query_name, query_info in latest_data.get("queries", {}).items():
        history = all_query_history.get(query_name, [])[:30]  # Keep last 30 entries

        stats = None
        if history:
            times = [h["ms"] for h in history]
            avg_7d = sum(times[:7]) / min(len(times), 7)
            avg_30d = sum(times[:30]) / min(len(times), 30)
            best = min(times)
            trend, pct = calculate_trend(times[0], times[1] if len(times) > 1 else None)

            stats = {
                "avg_7d_ms": round(avg_7d, 2),
                "avg_30d_ms": round(avg_30d, 2),
                "best_ms": round(best, 2),
                "trend": trend,
                "trend_pct": pct
            }

        queries[query_name] = {
            "latest": {
                "vibesql_ms": query_info.get("execution_time_ms"),
                "status": query_info.get("status"),
                "timestamp": latest["timestamp"]
            },
            "history": history,
            "stats": stats
        }

        if query_info.get("error"):
            queries[query_name]["latest"]["error"] = query_info["error"]

    return {
        "description": "TPC-H Decision Support - 22 analytical queries",
        "scale_factor": latest.get("scale_factor") or 0.01,
        "latest_run": {
            "timestamp": latest["timestamp"],
            "commit": latest["git_commit"],
            "branch": latest["git_branch"]
        },
        "queries_passing": latest["passed_queries"] or 0,
        "queries_total": latest["total_queries"] or 22,
        "geo_mean_ms": latest_data.get("geo_mean_ms"),
        "queries": queries
    }


def build_tpcds_benchmark_data(conn: sqlite3.Connection, runs: List[Dict]) -> Dict[str, Any]:
    """Build TPC-DS benchmark data from historical runs."""
    tpcds_runs = [r for r in runs if r["benchmark_suite"] == "tpcds"]

    if not tpcds_runs:
        return {}

    # Get latest run
    latest = max(tpcds_runs, key=lambda x: x["timestamp"])

    cursor = conn.cursor()
    cursor.execute("""
        SELECT query_name, status, execution_time_ms, error_message
        FROM benchmark_results
        WHERE run_id = ?
        ORDER BY query_name
    """, (latest["run_id"],))

    queries = {}
    passing_times = []

    for row in cursor.fetchall():
        query_name, status, exec_time, error = row
        queries[query_name] = {
            "latest": {
                "vibesql_ms": round(exec_time, 2) if exec_time else None,
                "status": status,
                "timestamp": latest["timestamp"]
            }
        }
        if error:
            queries[query_name]["latest"]["error"] = error
        if status == "passed" and exec_time:
            passing_times.append(exec_time)

    return {
        "description": "TPC-DS Decision Support - 99 complex queries",
        "queries_passing": latest["passed_queries"] or 0,
        "queries_total": latest["total_queries"] or 99,
        "geo_mean_ms": round(geometric_mean(passing_times), 2) if passing_times else None,
        "queries": queries
    }


def build_tpcc_benchmark_data(conn: sqlite3.Connection, runs: List[Dict]) -> Dict[str, Any]:
    """Build TPC-C benchmark data from historical runs."""
    tpcc_runs = [r for r in runs if r["benchmark_suite"] == "tpcc"]

    if not tpcc_runs:
        return {}

    # Get latest run
    latest = max(tpcc_runs, key=lambda x: x["timestamp"])
    latest_data = query_tpcc_results_for_run(conn, latest["run_id"])

    # Build history from all runs
    history = []
    for run in sorted(tpcc_runs, key=lambda x: x["timestamp"], reverse=True):
        run_data = query_tpcc_results_for_run(conn, run["run_id"])
        if run_data.get("vibesql_tps"):
            history.append({
                "date": run["timestamp"][:10],
                "tps": run_data["vibesql_tps"]
            })

    return {
        "description": "TPC-C OLTP - Mixed read/write transactions",
        "scale_factor": latest.get("scale_factor") or 1,
        "latest": {
            "timestamp": latest["timestamp"],
            "commit": latest["git_commit"],
            "vibesql_tps": latest_data.get("vibesql_tps"),
            "transactions": latest_data.get("transactions", {})
        },
        "history": history[:30]
    }


def build_sysbench_benchmark_data(conn: sqlite3.Connection, runs: List[Dict]) -> Dict[str, Any]:
    """Build Sysbench benchmark data from historical runs."""
    sysbench_runs = [r for r in runs if r["benchmark_suite"] == "sysbench"]

    if not sysbench_runs:
        return {}

    # Get latest run
    latest = max(sysbench_runs, key=lambda x: x["timestamp"])
    latest_data = query_sysbench_results_for_run(conn, latest["run_id"])

    return {
        "description": "Sysbench OLTP - Point operations",
        "latest": {
            "timestamp": latest["timestamp"],
            "commit": latest["git_commit"]
        },
        "tests": latest_data.get("tests", {})
    }


def detect_historical_changes(timeline: List[Dict]) -> List[Dict]:
    """Detect significant changes in historical timeline data."""
    changes = []

    # Need at least 2 entries to detect changes
    if len(timeline) < 2:
        return changes

    # Sort by date ascending for proper comparison
    sorted_timeline = sorted(timeline, key=lambda x: x["date"])

    for i in range(1, len(sorted_timeline)):
        current = sorted_timeline[i]
        previous = sorted_timeline[i - 1]

        # Check TPC-H geo mean
        curr_geo = current.get("tpch_geo_mean_ms")
        prev_geo = previous.get("tpch_geo_mean_ms")

        if curr_geo and prev_geo:
            trend, pct = calculate_trend(curr_geo, prev_geo)

            if trend == "improving" and pct and abs(pct) >= CHANGE_THRESHOLD_PCT:
                changes.append({
                    "date": current["date"],
                    "type": "improvement",
                    "category": "tpch",
                    "query": "overall",
                    "description": f"TPC-H geo mean improved {abs(pct):.1f}%",
                    "before_ms": round(prev_geo, 2),
                    "after_ms": round(curr_geo, 2),
                    "commit": current.get("commit")
                })
            elif trend == "regressing" and pct and pct >= CHANGE_THRESHOLD_PCT:
                changes.append({
                    "date": current["date"],
                    "type": "regression",
                    "category": "tpch",
                    "query": "overall",
                    "description": f"TPC-H geo mean regressed {pct:.1f}%",
                    "before_ms": round(prev_geo, 2),
                    "after_ms": round(curr_geo, 2),
                    "commit": current.get("commit")
                })

    # Sort changes by date descending, keep most recent 30
    changes.sort(key=lambda x: x["date"], reverse=True)
    return changes[:30]


def generate_seeded_dashboard(
    db_path: Path,
    days: Optional[int] = None,
    verbose: bool = False
) -> Dict:
    """Generate a seeded dashboard.json from historical data."""

    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        return {
            "generated_at": datetime.now().isoformat() + "Z",
            "version": DASHBOARD_VERSION,
            "summary": {},
            "timeline": [],
            "benchmarks": {},
            "conformance": {},
            "changes": [],
            "machine_info": get_machine_info()
        }

    if verbose:
        print(f"Reading from database: {db_path}")
        if days:
            print(f"Limiting to last {days} days")

    conn = sqlite3.connect(str(db_path))
    try:
        # Query all historical runs
        runs = query_all_benchmark_runs(conn, days)

        if verbose:
            print(f"Found {len(runs)} benchmark runs")
            by_suite = defaultdict(int)
            for r in runs:
                by_suite[r["benchmark_suite"]] += 1
            for suite, count in by_suite.items():
                print(f"  {suite}: {count} runs")

        # Build timeline from history
        timeline = build_timeline_from_history(conn, runs, verbose)

        if verbose:
            print(f"Generated {len(timeline)} timeline entries")

        # Build benchmark data
        tpch = build_tpch_benchmark_data(conn, runs)
        tpcds = build_tpcds_benchmark_data(conn, runs)
        tpcc = build_tpcc_benchmark_data(conn, runs)
        sysbench = build_sysbench_benchmark_data(conn, runs)

        # Get conformance data (current snapshot)
        conformance = query_conformance_results(conn)

        # Detect historical changes
        changes = detect_historical_changes(timeline)

        if verbose:
            print(f"Detected {len(changes)} significant changes")

    finally:
        conn.close()

    # Calculate 7-day trend from timeline
    trend_7d_pct = None
    if len(timeline) >= 2:
        current_geo = timeline[0].get("tpch_geo_mean_ms")
        # Find entry from ~7 days ago
        for entry in timeline[1:8]:
            prev_geo = entry.get("tpch_geo_mean_ms")
            if current_geo and prev_geo:
                _, pct = calculate_trend(current_geo, prev_geo)
                trend_7d_pct = pct
                break

    # Build final dashboard structure
    dashboard: Dict[str, Any] = {
        "generated_at": datetime.now().isoformat() + "Z",
        "version": DASHBOARD_VERSION,
        "summary": {
            "conformance": {
                "pass_rate": conformance.get("summary", {}).get("pass_rate"),
                "tests_passing": conformance.get("summary", {}).get("passing"),
                "tests_total": conformance.get("summary", {}).get("total_tests"),
                "files_passing": conformance.get("files", {}).get("passing"),
                "files_total": conformance.get("files", {}).get("total")
            },
            "tpch": {
                "queries_passing": tpch.get("queries_passing"),
                "queries_total": tpch.get("queries_total"),
                "geo_mean_ms": tpch.get("geo_mean_ms"),
                "trend_7d_pct": trend_7d_pct
            }
        },
        "timeline": timeline,
        "benchmarks": {
            "tpch": tpch,
            "tpcds": tpcds,
            "tpcc": tpcc,
            "sysbench": sysbench
        },
        "conformance": conformance,
        "changes": changes,
        "machine_info": get_machine_info()
    }

    return dashboard


def main():
    parser = argparse.ArgumentParser(
        description="Seed dashboard.json with historical benchmark data"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output path for dashboard.json (default: web-demo/public/data/dashboard.json)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Limit to last N days of history (default: all available)"
    )
    parser.add_argument(
        "--db",
        type=str,
        help="Path to database (default: ~/.vibesql/test_results/sqllogictest_results.vbsql)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be generated without writing output"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )

    args = parser.parse_args()

    # Determine database path
    if args.db:
        db_path = Path(args.db)
    else:
        db_path = get_db_path()

    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = get_repo_root() / "web-demo" / "public" / "data" / "dashboard.json"

    if args.verbose or args.dry_run:
        print(f"Seeding dashboard.json from historical data")
        print(f"  Database: {db_path}")
        print(f"  Output: {output_path}")
        if args.days:
            print(f"  Days limit: {args.days}")
        print()

    # Generate seeded dashboard
    dashboard = generate_seeded_dashboard(db_path, args.days, args.verbose or args.dry_run)

    # Print summary
    summary = dashboard.get("summary", {})
    tpch = summary.get("tpch", {})
    conformance = summary.get("conformance", {})

    print(f"\nDashboard Summary:")
    print(f"  TPC-H: {tpch.get('queries_passing')}/{tpch.get('queries_total')} queries, geo mean: {tpch.get('geo_mean_ms')}ms")
    print(f"  Conformance: {conformance.get('pass_rate')}% ({conformance.get('tests_passing')}/{conformance.get('tests_total')} tests)")
    print(f"  Timeline entries: {len(dashboard.get('timeline', []))}")
    print(f"  Detected changes: {len(dashboard.get('changes', []))}")

    if args.dry_run:
        print(f"\n[DRY RUN] Would write to: {output_path}")
        print(f"\nTimeline preview (first 5 entries):")
        for entry in dashboard.get("timeline", [])[:5]:
            print(f"  {entry.get('date')}: TPC-H={entry.get('tpch_geo_mean_ms')}ms, "
                  f"passing={entry.get('tpch_passing')}, commit={entry.get('commit')}")
        return

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write output
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)

    print(f"\nGenerated {output_path}")


if __name__ == "__main__":
    main()
