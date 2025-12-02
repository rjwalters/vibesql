#!/usr/bin/env python3
"""
Generate unified dashboard.json file for the web demo performance dashboard.

This script reads benchmark data from the SQLite database and generates a
single JSON file that powers the web demo's performance dashboard.
"""

import argparse
import json
import math
import os
import platform
import sqlite3
import subprocess
import sys
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# Schema version for the dashboard.json format
DASHBOARD_VERSION = "2.0"

# Thresholds for detecting improvements/regressions
CHANGE_THRESHOLD_PCT = 10.0

# Data retention policy
RETENTION_DAILY_DAYS = 90
RETENTION_WEEKLY_DAYS = 365


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
    """Get the path to the benchmark results database (SQLite)."""
    return Path.home() / ".vibesql" / "test_results" / "benchmark_results.db"


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
        "notes": "Local development"
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


def fetch_previous_dashboard(url: str, verbose: bool = False) -> Optional[Dict]:
    """Download previous dashboard.json from URL."""
    if verbose:
        print(f"Fetching previous dashboard from: {url}")

    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))
            if verbose:
                print(f"  Successfully fetched version {data.get('version', 'unknown')}")
            return data
    except urllib.error.HTTPError as e:
        if verbose:
            print(f"  HTTP error: {e.code} - {e.reason}")
        return None
    except urllib.error.URLError as e:
        if verbose:
            print(f"  URL error: {e.reason}")
        return None
    except json.JSONDecodeError as e:
        if verbose:
            print(f"  JSON decode error: {e}")
        return None
    except Exception as e:
        if verbose:
            print(f"  Unexpected error: {e}")
        return None


def query_tpch_results(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Query TPC-H benchmark results from the database."""
    cursor = conn.cursor()

    # Get latest TPC-H run
    cursor.execute("""
        SELECT run_id, timestamp, git_commit, git_branch, passed_queries, total_queries
        FROM benchmark_runs
        WHERE benchmark_suite = 'tpch'
        ORDER BY run_id DESC
        LIMIT 1
    """)
    run = cursor.fetchone()

    if not run:
        return {}

    run_id, timestamp, commit, branch, passed, total = run

    # Get individual query results
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

        query_data = {
            "latest": {
                "vibesql_ms": round(exec_time, 2) if exec_time else None,
                "status": status,
                "timestamp": timestamp
            },
            "history": [],
            "stats": None
        }

        if error:
            query_data["latest"]["error"] = error

        queries[query_name] = query_data

        if status == "passed" and exec_time:
            passing_times.append(exec_time)

    # Get historical data for each query
    for query_name in queries:
        cursor.execute("""
            SELECT br.timestamp, res.execution_time_ms
            FROM benchmark_results res
            JOIN benchmark_runs br ON res.run_id = br.run_id
            WHERE br.benchmark_suite = 'tpch'
              AND res.query_name = ?
              AND res.status = 'passed'
            ORDER BY br.timestamp DESC
            LIMIT 30
        """, (query_name,))

        history = []
        times = []
        for ts, exec_time in cursor.fetchall():
            if exec_time:
                date = ts.split('T')[0] if 'T' in ts else ts[:10]
                history.append({"date": date, "ms": round(exec_time, 2)})
                times.append(exec_time)

        queries[query_name]["history"] = history

        if times:
            avg_7d = sum(times[:7]) / min(len(times), 7)
            avg_30d = sum(times[:30]) / min(len(times), 30)
            best = min(times)
            trend, pct = calculate_trend(times[0], times[1] if len(times) > 1 else None)

            queries[query_name]["stats"] = {
                "avg_7d_ms": round(avg_7d, 2),
                "avg_30d_ms": round(avg_30d, 2),
                "best_ms": round(best, 2),
                "trend": trend,
                "trend_pct": pct
            }

    geo_mean = geometric_mean(passing_times)

    return {
        "description": "TPC-H Decision Support - 22 analytical queries",
        "scale_factor": 0.01,
        "latest_run": {
            "timestamp": timestamp,
            "commit": commit,
            "branch": branch
        },
        "queries_passing": passed or 0,
        "queries_total": total or 22,
        "geo_mean_ms": round(geo_mean, 2) if geo_mean else None,
        "queries": queries
    }


def query_tpcds_results(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Query TPC-DS benchmark results from the database."""
    cursor = conn.cursor()

    # Get latest TPC-DS run
    cursor.execute("""
        SELECT run_id, timestamp, git_commit, passed_queries, total_queries
        FROM benchmark_runs
        WHERE benchmark_suite = 'tpcds'
        ORDER BY run_id DESC
        LIMIT 1
    """)
    run = cursor.fetchone()

    if not run:
        return {}

    run_id, timestamp, commit, passed, total = run

    # Get individual query results
    cursor.execute("""
        SELECT query_name, status, execution_time_ms, error_message
        FROM benchmark_results
        WHERE run_id = ?
        ORDER BY query_name
    """, (run_id,))

    queries = {}
    passing_times = []

    for row in cursor.fetchall():
        query_name, status, exec_time, error = row

        query_data = {
            "latest": {
                "vibesql_ms": round(exec_time, 2) if exec_time else None,
                "status": status,
                "timestamp": timestamp
            }
        }

        if error:
            query_data["latest"]["error"] = error

        queries[query_name] = query_data

        if status == "passed" and exec_time:
            passing_times.append(exec_time)

    geo_mean = geometric_mean(passing_times)

    return {
        "description": "TPC-DS Decision Support - 99 complex queries",
        "queries_passing": passed or 0,
        "queries_total": total or 99,
        "geo_mean_ms": round(geo_mean, 2) if geo_mean else None,
        "queries": queries
    }


def query_tpcc_results(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Query TPC-C benchmark results from the database."""
    cursor = conn.cursor()

    # Get latest TPC-C run
    cursor.execute("""
        SELECT run_id, timestamp, git_commit
        FROM benchmark_runs
        WHERE benchmark_suite = 'tpcc'
        ORDER BY run_id DESC
        LIMIT 1
    """)
    run = cursor.fetchone()

    if not run:
        return {}

    run_id, timestamp, commit = run

    # Get TPC-C results for all engines
    cursor.execute("""
        SELECT database_engine, transaction_type, transactions_per_second, avg_latency_us
        FROM tpcc_results
        WHERE run_id = ?
    """, (run_id,))

    results = {}
    for row in cursor.fetchall():
        engine, txn_type, tps, latency = row
        if txn_type not in results:
            results[txn_type] = {}
        results[txn_type][engine] = {
            "tps": round(tps, 2) if tps else None,
            "latency_us": round(latency, 2) if latency else None
        }

    # Get VibeSQL mixed TPS for summary
    vibesql_tps = None
    if "mixed" in results and "vibesql" in results["mixed"]:
        vibesql_tps = results["mixed"]["vibesql"]["tps"]

    return {
        "description": "TPC-C OLTP - Mixed read/write transactions",
        "scale_factor": 1,
        "latest": {
            "timestamp": timestamp,
            "commit": commit,
            "vibesql_tps": vibesql_tps,
            "transactions": results
        },
        "history": []
    }


def query_sysbench_results(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Query Sysbench benchmark results from the database."""
    cursor = conn.cursor()

    # Get latest Sysbench run
    cursor.execute("""
        SELECT run_id, timestamp, git_commit
        FROM benchmark_runs
        WHERE benchmark_suite = 'sysbench'
        ORDER BY run_id DESC
        LIMIT 1
    """)
    run = cursor.fetchone()

    if not run:
        return {}

    run_id, timestamp, commit = run

    # Get Sysbench results
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

    return {
        "description": "Sysbench OLTP - Point operations",
        "latest": {
            "timestamp": timestamp,
            "commit": commit
        },
        "tests": tests
    }


def get_conformance_json_paths() -> List[Path]:
    """Get paths to conformance JSON files in priority order."""
    repo_root = get_repo_root()
    return [
        # Local test results (freshest)
        Path.home() / ".vibesql" / "test_results" / "sqllogictest_results.json",
        # Cumulative results in web-demo (comprehensive)
        repo_root / "web-demo" / "public" / "badges" / "sqllogictest_cumulative.json",
        # Summary results
        repo_root / "web-demo" / "public" / "badges" / "sqllogictest_summary.json",
    ]


def query_conformance_results(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Query SQLLogicTest conformance results from JSON files or database."""
    # First try to load from JSON files (preferred source)
    conformance_data = load_conformance_from_json()
    if conformance_data:
        return conformance_data

    # Fall back to database query if JSON not available
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


def load_conformance_from_json() -> Optional[Dict[str, Any]]:
    """Load conformance data from JSON files.

    Attempts to merge data from multiple sources:
    - Local test results provide fresh dialect stats
    - Cumulative results provide comprehensive file-level data
    """
    local_data = None
    cumulative_data = None

    for json_path in get_conformance_json_paths():
        if not json_path.exists():
            continue

        try:
            with open(json_path, 'r') as f:
                data = json.load(f)

            # Handle sqllogictest_results.json format (from local test runs)
            if "dialect_stats" in data and local_data is None:
                local_data = data

            # Handle sqllogictest_cumulative.json format
            elif "tested_files" in data and cumulative_data is None:
                cumulative_data = data

            # Handle sqllogictest_summary.json format as fallback
            elif "by_category" in data and cumulative_data is None:
                cumulative_data = data

        except (json.JSONDecodeError, KeyError, TypeError):
            continue

    # Build result by merging data sources
    result: Dict[str, Any] = {
        "summary": {},
        "files": {},
        "categories": {},
        "history": []
    }

    # Use cumulative data for file-level stats (most comprehensive)
    if cumulative_data:
        if "tested_files" in cumulative_data:
            summary = cumulative_data.get("summary", {})
            categories = cumulative_data.get("categories", {})

            total_files = summary.get("total_available_files", 0)
            passed_files = summary.get("passed", 0)
            failed_files = summary.get("failed", 0)
            pass_rate = summary.get("pass_rate", 0)

            result["files"] = {
                "total": total_files,
                "passing": passed_files,
                "pass_rate": round(pass_rate, 2)
            }

            # Use file counts as test counts if no better data
            result["summary"] = {
                "total_tests": total_files,
                "passing": passed_files,
                "failing": failed_files,
                "pass_rate": round(pass_rate, 2)
            }

            # Build categories (use "passing" to match DashboardConformanceCategory type)
            for cat_name, cat_info in categories.items():
                if cat_info:
                    cat_total = cat_info.get("total", 0)
                    cat_passed = cat_info.get("passed", 0)
                    result["categories"][cat_name] = {
                        "total": cat_total,
                        "passing": cat_passed,
                        "pass_rate": round(cat_passed / cat_total * 100, 2) if cat_total > 0 else 0
                    }

        elif "by_category" in cumulative_data:
            summary = cumulative_data.get("summary", {})
            by_category = cumulative_data.get("by_category", {})
            total_files = cumulative_data.get("total_files", 0)

            passed_files = summary.get("passed", 0)
            failed_files = summary.get("failed", 0)
            pass_rate = (passed_files / total_files * 100) if total_files > 0 else 0

            result["files"] = {
                "total": total_files,
                "passing": passed_files,
                "pass_rate": round(pass_rate, 2)
            }

            result["summary"] = {
                "total_tests": total_files,
                "passing": passed_files,
                "failing": failed_files,
                "pass_rate": round(pass_rate, 2)
            }

            for cat_name, cat_info in by_category.items():
                if cat_info:
                    cat_total = cat_info.get("total", 0)
                    cat_passed = cat_info.get("passed", 0)
                    result["categories"][cat_name] = {
                        "total": cat_total,
                        "passing": cat_passed,
                        "pass_rate": round(cat_passed / cat_total * 100, 2) if cat_total > 0 else 0
                    }

    # Enhance with local dialect stats if available
    if local_data:
        dialect_stats = local_data.get("dialect_stats", {})
        total_records = dialect_stats.get("total_records", 0)
        mysql_stats = dialect_stats.get("mysql", {})
        sqlite_stats = dialect_stats.get("sqlite", {})

        # Update test counts from dialect stats (more accurate)
        if total_records > 0:
            result["summary"]["total_tests"] = total_records
            result["summary"]["passing"] = mysql_stats.get("passed", 0) + sqlite_stats.get("passed", 0)
            result["summary"]["failing"] = mysql_stats.get("failed", 0) + sqlite_stats.get("failed", 0)

        # Add dialect breakdown
        result["dialect_stats"] = {
            "mysql": mysql_stats,
            "sqlite": sqlite_stats
        }

    # If we only have local data and no cumulative
    if local_data and not cumulative_data:
        summary = local_data.get("summary", {})
        dialect_stats = local_data.get("dialect_stats", {})
        categories = local_data.get("categories", {})

        total_records = dialect_stats.get("total_records", 0)
        mysql_stats = dialect_stats.get("mysql", {})
        sqlite_stats = dialect_stats.get("sqlite", {})

        total_files = summary.get("total", 1)
        passed_files = summary.get("passed", 0)
        pass_rate = summary.get("pass_rate", 0)

        result["files"] = {
            "total": total_files,
            "passing": passed_files,
            "pass_rate": round(pass_rate, 2)
        }

        result["summary"] = {
            "total_tests": total_records,
            "passing": mysql_stats.get("passed", 0) + sqlite_stats.get("passed", 0),
            "failing": mysql_stats.get("failed", 0) + sqlite_stats.get("failed", 0),
            "pass_rate": round(pass_rate, 2)
        }

        for cat_name, cat_info in categories.items():
            if cat_info:
                result["categories"][cat_name] = {
                    "total": cat_info.get("total", 0),
                    "passing": cat_info.get("passed", 0),
                    "pass_rate": cat_info.get("pass_rate", 0)
                }

        result["dialect_stats"] = {
            "mysql": mysql_stats,
            "sqlite": sqlite_stats
        }

    if result["summary"]:
        return result

    return None


def detect_changes(current: Dict, previous: Optional[Dict]) -> List[Dict]:
    """
    Detect significant changes between current and previous results.

    Returns list of change objects with type, description, etc.
    """
    changes = []
    commit, _ = get_git_info()
    today = datetime.now().strftime("%Y-%m-%d")

    if not previous:
        return changes

    # Check TPC-H query changes
    current_tpch = current.get("benchmarks", {}).get("tpch", {}).get("queries", {})
    previous_tpch = previous.get("benchmarks", {}).get("tpch", {}).get("queries", {})

    for query_name, query_data in current_tpch.items():
        current_ms = query_data.get("latest", {}).get("vibesql_ms")
        prev_query = previous_tpch.get(query_name, {})
        prev_ms = prev_query.get("latest", {}).get("vibesql_ms")

        if current_ms and prev_ms:
            trend, pct = calculate_trend(current_ms, prev_ms)

            if trend == "improving" and pct and abs(pct) >= CHANGE_THRESHOLD_PCT:
                changes.append({
                    "date": today,
                    "type": "improvement",
                    "category": "tpch",
                    "query": query_name,
                    "description": f"{query_name} improved {abs(pct):.1f}%",
                    "before_ms": round(prev_ms, 2),
                    "after_ms": round(current_ms, 2),
                    "commit": commit
                })
            elif trend == "regressing" and pct and pct >= CHANGE_THRESHOLD_PCT:
                changes.append({
                    "date": today,
                    "type": "regression",
                    "category": "tpch",
                    "query": query_name,
                    "description": f"{query_name} regressed {pct:.1f}%",
                    "before_ms": round(prev_ms, 2),
                    "after_ms": round(current_ms, 2),
                    "commit": commit
                })

    return changes


def build_timeline_entry(data: Dict) -> Dict:
    """Build a single timeline entry from current data."""
    commit, _ = get_git_info()
    today = datetime.now().strftime("%Y-%m-%d")

    tpch = data.get("benchmarks", {}).get("tpch", {})
    conformance = data.get("conformance", {}).get("summary", {})
    tpcc = data.get("benchmarks", {}).get("tpcc", {})

    events = []
    for change in data.get("changes", []):
        if change.get("date") == today:
            events.append(change.get("description", ""))

    return {
        "date": today,
        "commit": commit,
        "conformance_pass_rate": conformance.get("pass_rate"),
        "tpch_geo_mean_ms": tpch.get("geo_mean_ms"),
        "tpch_passing": tpch.get("queries_passing"),
        "tpcc_tps": tpcc.get("latest", {}).get("vibesql_tps"),
        "events": events
    }


def merge_timeline(current_entry: Dict, previous_timeline: List[Dict]) -> List[Dict]:
    """
    Merge current timeline entry with previous timeline data.

    Applies retention policy:
    - Keep daily data for 90 days
    - Keep weekly aggregates for 1 year
    """
    today = datetime.now()
    cutoff_daily = today - timedelta(days=RETENTION_DAILY_DAYS)
    cutoff_weekly = today - timedelta(days=RETENTION_WEEKLY_DAYS)

    # Start with current entry
    timeline = [current_entry]

    # Track which dates we've seen (for deduplication)
    seen_dates = {current_entry.get("date")}

    # Add previous entries, applying retention policy
    for entry in previous_timeline:
        entry_date_str = entry.get("date")
        if not entry_date_str or entry_date_str in seen_dates:
            continue

        try:
            entry_date = datetime.strptime(entry_date_str, "%Y-%m-%d")
        except ValueError:
            continue

        # Within daily retention window - keep all
        if entry_date >= cutoff_daily:
            timeline.append(entry)
            seen_dates.add(entry_date_str)
        # Within weekly retention window - keep only if it's a Monday (weekly aggregate)
        elif entry_date >= cutoff_weekly and entry_date.weekday() == 0:
            timeline.append(entry)
            seen_dates.add(entry_date_str)
        # Beyond yearly retention - drop

    # Sort by date descending
    timeline.sort(key=lambda x: x.get("date", ""), reverse=True)

    return timeline


def merge_changes(current_changes: List[Dict], previous_changes: List[Dict]) -> List[Dict]:
    """Merge current changes with previous changes, keeping most recent."""
    # Keep last 30 changes
    all_changes = current_changes + previous_changes
    all_changes.sort(key=lambda x: x.get("date", ""), reverse=True)
    return all_changes[:30]


def generate_dashboard(
    db_path: Path,
    previous_url: Optional[str] = None,
    verbose: bool = False
) -> Dict:
    """Generate the complete dashboard.json structure."""

    # Fetch previous dashboard if URL provided
    previous = None
    if previous_url:
        previous = fetch_previous_dashboard(previous_url, verbose)

    # Check if database exists
    if not db_path.exists():
        if verbose:
            print(f"Database not found: {db_path}")
        # Return minimal structure if no database
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

    conn = sqlite3.connect(str(db_path))
    try:
        # Query all benchmark suites
        tpch = query_tpch_results(conn)
        tpcds = query_tpcds_results(conn)
        tpcc = query_tpcc_results(conn)
        sysbench = query_sysbench_results(conn)
        conformance = query_conformance_results(conn)

        if verbose:
            print(f"  TPC-H: {tpch.get('queries_passing', 0)}/{tpch.get('queries_total', 0)} queries passing")
            print(f"  TPC-DS: {tpcds.get('queries_passing', 0)}/{tpcds.get('queries_total', 0)} queries passing")
            print(f"  Conformance: {conformance.get('summary', {}).get('pass_rate', 0):.2f}% pass rate")

    finally:
        conn.close()

    # Build the dashboard structure
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
                "trend_7d_pct": None  # Calculated from timeline
            },
            "tpcds": {
                "queries_passing": tpcds.get("queries_passing"),
                "queries_total": tpcds.get("queries_total"),
                "geo_mean_ms": tpcds.get("geo_mean_ms")
            } if tpcds else None,
            "tpcc": {
                "vibesql_tps": tpcc.get("latest", {}).get("vibesql_tps"),
                "scale_factor": tpcc.get("scale_factor")
            } if tpcc else None,
            "sysbench": {
                "tests_count": len(sysbench.get("tests", {})) if sysbench else 0
            } if sysbench else None
        },
        "benchmarks": {
            "tpch": tpch,
            "tpcds": tpcds,
            "tpcc": tpcc,
            "sysbench": sysbench
        },
        "conformance": conformance,
        "changes": [],
        "machine_info": get_machine_info()
    }

    # Detect changes compared to previous
    changes = detect_changes(dashboard, previous)
    dashboard["changes"] = changes

    # Build timeline entry and merge with previous
    timeline_entry = build_timeline_entry(dashboard)
    previous_timeline = previous.get("timeline", []) if previous else []
    dashboard["timeline"] = merge_timeline(timeline_entry, previous_timeline)

    # Merge changes with previous
    previous_changes = previous.get("changes", []) if previous else []
    dashboard["changes"] = merge_changes(changes, previous_changes)

    # Calculate 7-day trend from timeline
    if len(dashboard["timeline"]) >= 2:
        current_geo = dashboard["timeline"][0].get("tpch_geo_mean_ms")
        # Find entry from ~7 days ago
        for entry in dashboard["timeline"][1:8]:
            prev_geo = entry.get("tpch_geo_mean_ms")
            if current_geo and prev_geo:
                _, pct = calculate_trend(current_geo, prev_geo)
                dashboard["summary"]["tpch"]["trend_7d_pct"] = pct
                break

    return dashboard


def main():
    parser = argparse.ArgumentParser(
        description="Generate dashboard.json for web demo performance dashboard"
    )
    parser.add_argument(
        "--previous",
        type=str,
        metavar="URL",
        help="URL to previous dashboard.json for merging historical data"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output path for dashboard.json (default: web-demo/public/data/dashboard.json)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--db",
        type=str,
        help="Path to database (default: ~/.vibesql/test_results/sqllogictest_results.vbsql)"
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

    if args.verbose:
        print(f"Generating dashboard.json")
        print(f"  Database: {db_path}")
        print(f"  Output: {output_path}")
        if args.previous:
            print(f"  Previous URL: {args.previous}")

    # Generate dashboard
    dashboard = generate_dashboard(db_path, args.previous, args.verbose)

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write output
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)

    print(f"Generated {output_path}")

    # Print summary
    summary = dashboard.get("summary", {})
    tpch = summary.get("tpch", {})
    tpcds = summary.get("tpcds", {})
    tpcc = summary.get("tpcc", {})
    sysbench = summary.get("sysbench", {})
    conformance = summary.get("conformance", {})

    print(f"  TPC-H: {tpch.get('queries_passing')}/{tpch.get('queries_total')} queries, geo mean: {tpch.get('geo_mean_ms')}ms")
    if tpcds:
        print(f"  TPC-DS: {tpcds.get('queries_passing')}/{tpcds.get('queries_total')} queries, geo mean: {tpcds.get('geo_mean_ms')}ms")
    if tpcc:
        print(f"  TPC-C: {tpcc.get('vibesql_tps')} TPS (scale factor {tpcc.get('scale_factor')})")
    if sysbench:
        print(f"  Sysbench: {sysbench.get('tests_count')} tests")
    print(f"  Conformance: {conformance.get('pass_rate')}% ({conformance.get('tests_passing')}/{conformance.get('tests_total')} tests)")
    print(f"  Timeline entries: {len(dashboard.get('timeline', []))}")
    print(f"  Recent changes: {len(dashboard.get('changes', []))}")


if __name__ == "__main__":
    main()
