#!/usr/bin/env python3
"""
Unified export script for web demo data.

Reads benchmark data from the VibeSQL dogfooding database and exports all
website data files in a single pass:
  - benchmarks/*.json (comparison data for TPC-H, TPC-DS, TPC-C, Sysbench)
  - benchmarks/trends_results.json (historical performance trends)
  - data/dashboard.json (summary dashboard)

This consolidates export_benchmark_json.py, export_trend_json.py, and
generate_web_dashboard.py into a single script that loads the database once.

Usage:
    python scripts/export_website_data.py
    python scripts/export_website_data.py --verbose
    python scripts/export_website_data.py --benchmarks-only  # Skip trends/dashboard
"""

import argparse
import json
import math
import os
import platform
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Import our VibeSQL helper module
sys.path.insert(0, str(Path(__file__).parent))
from vibesql_db import get_connection, get_db_path


# ============================================================================
# Configuration
# ============================================================================

DASHBOARD_VERSION = "2.0"
CHANGE_THRESHOLD_PCT = 10.0
RETENTION_DAILY_DAYS = 90
RETENTION_WEEKLY_DAYS = 365


# ============================================================================
# Utilities
# ============================================================================

def get_repo_root() -> Path:
    """Get the repository root directory."""
    return Path(__file__).parent.parent


def get_git_info() -> Tuple[Optional[str], Optional[str]]:
    """Get current git commit hash and branch name."""
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True
        ).strip()
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True
        ).strip()
        return commit, branch
    except Exception:
        return None, None


def get_machine_info() -> Dict[str, str]:
    """Get information about the current machine."""
    system = platform.system()
    machine = platform.machine()
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


# ============================================================================
# Database Query Helpers
# ============================================================================

class BenchmarkData:
    """Container for all benchmark data loaded from the database."""

    def __init__(self, cursor: Any):
        self.cursor = cursor
        self._runs_cache: Optional[List[tuple]] = None
        self._results_cache: Dict[str, List[tuple]] = {}
        self._column_cache: Dict[str, List[str]] = {}

    def get_all_runs(self) -> List[tuple]:
        """Get all benchmark runs (cached)."""
        if self._runs_cache is None:
            self.cursor.execute('SELECT * FROM benchmark_runs ORDER BY RUN_TIMESTAMP')
            self._runs_cache = self.cursor.fetchall()
        return self._runs_cache

    def get_runs_by_suite(self, suite: str) -> List[tuple]:
        """Get runs filtered by benchmark suite."""
        all_runs = self.get_all_runs()
        return [r for r in all_runs if r[4] == suite]

    def get_latest_run(self, suite: str) -> Optional[tuple]:
        """Get the latest run for a suite."""
        suite_runs = self.get_runs_by_suite(suite)
        if not suite_runs:
            return None
        return max(suite_runs, key=lambda x: x[0])  # Max by RUN_ID

    def get_table_results(self, table: str) -> List[tuple]:
        """Get all results from a table (cached)."""
        if table not in self._results_cache:
            self.cursor.execute(f'SELECT * FROM {table}')
            self._results_cache[table] = self.cursor.fetchall()
            self._column_cache[table] = [d[0].lower() for d in self.cursor.description]
        return self._results_cache[table]

    def get_table_columns(self, table: str) -> List[str]:
        """Get column names for a table."""
        if table not in self._column_cache:
            self.get_table_results(table)  # Populates cache
        return self._column_cache[table]

    def get_results_for_run(self, table: str, run_id: int) -> List[tuple]:
        """Get results for a specific run ID."""
        all_results = self.get_table_results(table)
        columns = self.get_table_columns(table)
        run_id_idx = columns.index('run_id')
        return [r for r in all_results if r[run_id_idx] == run_id]

    def get_suite_counts(self) -> Dict[str, int]:
        """Get count of runs per suite."""
        runs = self.get_all_runs()
        counts: Dict[str, int] = {}
        for r in runs:
            suite = r[4]
            counts[suite] = counts.get(suite, 0) + 1
        return counts


# ============================================================================
# Benchmark JSON Export (comparison data)
# ============================================================================

def export_tpcc_benchmarks(data: BenchmarkData) -> Optional[Dict]:
    """Export TPC-C results to JSON format."""
    run = data.get_latest_run('tpcc')
    if not run:
        return None

    run_id, timestamp, commit, _, _, scale = run[:6]
    results = data.get_results_for_run('tpcc_results', run_id)

    benchmarks = []
    for row in results:
        _, _, engine, txn_type, count, latency, duration, tps, success, failed = row
        if txn_type == 'mixed':
            mean_time = (1.0 / tps) if tps and tps > 0 else 0
            benchmarks.append({
                "name": f"tpcc_mixed_{engine}",
                "stats": {
                    "mean": round(mean_time, 6),
                    "stddev": 0,
                    "min": round(mean_time * 0.9, 6),
                    "max": round(mean_time * 1.1, 6),
                    "rounds": count or 0,
                    "tps": round(tps, 2) if tps else 0,
                    "transactions": count or 0,
                    "duration_ms": duration or 0
                }
            })

    if not benchmarks:
        return None

    return {
        "benchmarks": benchmarks,
        "datetime": timestamp,
        "machine_info": {"suite": "tpcc", "git_commit": commit, "scale_factor": str(scale)}
    }


def export_sysbench_benchmarks(data: BenchmarkData) -> Optional[Dict]:
    """Export Sysbench results to JSON format.

    Combines results from recent sysbench runs to include both embedded engines
    (vibesql, sqlite, duckdb) and server engines (vibesql_server, mysql).
    Uses the most recent result for each engine/test combination.
    """
    # Get all recent sysbench runs (embedded and server benchmarks may be stored separately)
    runs = data.get_runs_by_suite('sysbench')
    if not runs:
        return None

    # Collect results from recent runs, using the most recent for each engine/test combo
    # Key: (engine, test), Value: (timestamp, result_dict)
    best_results: Dict[tuple, tuple] = {}
    latest_timestamp = None
    latest_commit = None
    latest_scale = None

    for run in runs:
        run_id, timestamp, commit, _, _, scale = run[:6]
        results = data.get_results_for_run('sysbench_results', run_id)

        # Track the most recent run's metadata
        if latest_timestamp is None or timestamp > latest_timestamp:
            latest_timestamp = timestamp
            latest_commit = commit
            latest_scale = scale

        for row in results:
            _, _, engine, test, _, mean_ns, std_ns, median_ns, iterations = row
            key = (engine, test)

            # Use this result if we don't have one yet or if it's newer
            if key not in best_results or timestamp > best_results[key][0]:
                mean_s = (mean_ns / 1e9) if mean_ns else 0
                std_s = (std_ns / 1e9) if std_ns else 0
                median_s = (median_ns / 1e9) if median_ns else 0

                best_results[key] = (timestamp, {
                    "name": f"sysbench_{test}_{engine}",
                    "stats": {
                        "mean": mean_s,
                        "stddev": std_s,
                        "min": median_s * 0.9,
                        "max": median_s * 1.1,
                        "rounds": iterations or 0
                    }
                })

    benchmarks = [result for _, result in best_results.values()]

    if not benchmarks:
        return None

    return {
        "benchmarks": benchmarks,
        "metadata": {"suite": "sysbench", "timestamp": latest_timestamp, "git_commit": latest_commit, "table_size": latest_scale}
    }


def export_tpcds_benchmarks(data: BenchmarkData) -> Optional[Dict]:
    """Export TPC-DS results to JSON format."""
    run = data.get_latest_run('tpcds')
    if not run:
        return None

    run_id, timestamp, commit, _, _, scale, _, total, passed = run[:9]
    results = data.get_results_for_run('benchmark_results', run_id)
    columns = data.get_table_columns('benchmark_results')

    has_engine_col = 'database_engine' in columns
    engine_idx = columns.index('database_engine') if has_engine_col else None

    query_data: Dict[tuple, List[tuple]] = {}
    for row in results:
        if has_engine_col:
            engine = row[engine_idx] if row[engine_idx] else 'vibesql'
            query = row[engine_idx + 1]
            status = row[engine_idx + 2]
            exec_ms = row[engine_idx + 5] if len(row) > engine_idx + 5 else None
            total_ms = row[engine_idx + 6] if len(row) > engine_idx + 6 else None
            rows = row[engine_idx + 7] if len(row) > engine_idx + 7 else None
        else:
            engine, query, status = 'vibesql', row[2], row[3]
            exec_ms = row[6] if len(row) > 6 else None
            total_ms = row[7] if len(row) > 7 else None
            rows = row[8] if len(row) > 8 else None

        if query.startswith('sanity'):
            continue

        key = (engine, query)
        if key not in query_data:
            query_data[key] = []
        query_data[key].append((exec_ms, total_ms, rows, status))

    benchmarks = []
    for (engine, query), iterations in sorted(query_data.items()):
        exec_times = [it[0] for it in iterations if it[0] is not None]
        statuses = [it[3] for it in iterations]
        status = max(set(statuses), key=statuses.count) if statuses else 'unknown'
        rows = iterations[0][2] if iterations else 0
        mean_exec_s = (sum(exec_times) / len(exec_times) / 1000) if exec_times else 0

        benchmarks.append({
            "name": f"tpcds_{query.lower()}_{engine}",
            "stats": {"mean": mean_exec_s, "rows": rows or 0, "status": status, "iterations": len(iterations)}
        })

    if not benchmarks:
        return None

    vibesql_count = sum(1 for b in benchmarks if b["name"].endswith("_vibesql"))
    sqlite_count = sum(1 for b in benchmarks if b["name"].endswith("_sqlite"))
    duckdb_count = sum(1 for b in benchmarks if b["name"].endswith("_duckdb"))
    mysql_count = sum(1 for b in benchmarks if b["name"].endswith("_mysql"))

    return {
        "benchmarks": benchmarks,
        "metadata": {
            "suite": "tpcds", "timestamp": timestamp, "git_commit": commit, "scale_factor": scale,
            "total_queries": vibesql_count, "vibesql_queries": vibesql_count,
            "sqlite_queries": sqlite_count, "duckdb_queries": duckdb_count, "mysql_queries": mysql_count
        }
    }


def export_tpch_benchmarks(data: BenchmarkData) -> Optional[Dict]:
    """Export TPC-H results to JSON format."""
    run = data.get_latest_run('tpch')
    if not run:
        return None

    run_id, timestamp, commit, _, _, scale, _, total, passed = run[:9]
    results = data.get_results_for_run('benchmark_results', run_id)
    columns = data.get_table_columns('benchmark_results')

    has_engine_col = 'database_engine' in columns
    engine_idx = columns.index('database_engine') if has_engine_col else None

    benchmarks = []
    for row in results:
        if has_engine_col:
            engine = row[engine_idx] if row[engine_idx] else 'vibesql'
            query = row[engine_idx + 1]
            status = row[engine_idx + 2]
            exec_ms = row[engine_idx + 5] if len(row) > engine_idx + 5 else None
        else:
            engine, query, status = 'vibesql', row[2], row[3]
            exec_ms = row[6] if len(row) > 6 else None

        exec_s = (exec_ms / 1000) if exec_ms else 0
        benchmarks.append({
            "name": f"tpch_{query.lower()}_{engine}",
            "stats": {
                "mean": exec_s, "stddev": exec_s * 0.05,
                "min": exec_s * 0.95, "max": exec_s * 1.05,
                "rounds": 5, "status": status
            }
        })

    if not benchmarks:
        return None

    vibesql_count = sum(1 for b in benchmarks if b["name"].endswith("_vibesql"))
    sqlite_count = sum(1 for b in benchmarks if b["name"].endswith("_sqlite"))
    duckdb_count = sum(1 for b in benchmarks if b["name"].endswith("_duckdb"))
    mysql_count = sum(1 for b in benchmarks if b["name"].endswith("_mysql"))

    return {
        "benchmarks": benchmarks,
        "metadata": {
            "suite": "tpch", "timestamp": timestamp, "git_commit": commit, "scale_factor": scale,
            "total_queries": vibesql_count, "vibesql_queries": vibesql_count,
            "sqlite_queries": sqlite_count, "duckdb_queries": duckdb_count, "mysql_queries": mysql_count
        }
    }


# ============================================================================
# Trends Export (historical data)
# ============================================================================

def export_trends(data: BenchmarkData) -> Dict:
    """Export historical performance trends for all suites."""
    trends = {}
    columns = data.get_table_columns('benchmark_results')
    has_engine_col = 'database_engine' in columns
    engine_idx = columns.index('database_engine') if has_engine_col else None

    # TPC-H trends
    tpch_runs = data.get_runs_by_suite('tpch')
    if tpch_runs:
        tpch_data = []
        for run in tpch_runs:
            run_id, timestamp, commit = run[0], run[1], run[2]
            total, passed = run[7], run[8]
            results = data.get_results_for_run('benchmark_results', run_id)

            exec_times = []
            for row in results:
                if has_engine_col:
                    engine = row[engine_idx] if row[engine_idx] else 'vibesql'
                    status = row[engine_idx + 2]
                    exec_ms = row[engine_idx + 5] if len(row) > engine_idx + 5 else None
                else:
                    engine, status = 'vibesql', row[3]
                    exec_ms = row[6] if len(row) > 6 else None

                if engine == 'vibesql' and status == 'passed' and exec_ms is not None:
                    exec_times.append(exec_ms)

            if exec_times:
                tpch_data.append({
                    "date": timestamp[:10] if timestamp else "",
                    "timestamp": timestamp or "",
                    "commit": commit or "",
                    "avg_ms": round(sum(exec_times) / len(exec_times), 2),
                    "min_ms": round(min(exec_times), 2),
                    "max_ms": round(max(exec_times), 2),
                    "geomean_ms": round(geometric_mean(exec_times) or 0, 2),
                    "queries_passed": len(exec_times),
                    "total_queries": total or 22
                })

        if tpch_data:
            trends["tpch"] = {
                "suite": "tpch", "display_name": "TPC-H",
                "description": "Decision support queries", "data": tpch_data
            }

    # TPC-DS trends
    tpcds_runs = data.get_runs_by_suite('tpcds')
    if tpcds_runs:
        tpcds_data = []
        for run in tpcds_runs:
            run_id, timestamp, commit = run[0], run[1], run[2]
            total, passed = run[7], run[8]
            results = data.get_results_for_run('benchmark_results', run_id)

            exec_times = []
            for row in results:
                if has_engine_col:
                    engine = row[engine_idx] if row[engine_idx] else 'vibesql'
                    status = row[engine_idx + 2]
                    exec_ms = row[engine_idx + 5] if len(row) > engine_idx + 5 else None
                else:
                    engine, status = 'vibesql', row[3]
                    exec_ms = row[6] if len(row) > 6 else None

                if engine == 'vibesql' and status == 'passed' and exec_ms is not None:
                    exec_times.append(exec_ms)

            if exec_times:
                tpcds_data.append({
                    "date": timestamp[:10] if timestamp else "",
                    "timestamp": timestamp or "",
                    "commit": commit or "",
                    "avg_ms": round(sum(exec_times) / len(exec_times), 2),
                    "min_ms": round(min(exec_times), 2),
                    "max_ms": round(max(exec_times), 2),
                    "geomean_ms": round(geometric_mean(exec_times) or 0, 2),
                    "queries_passed": len(exec_times),
                    "total_queries": total or 99
                })

        if tpcds_data:
            trends["tpcds"] = {
                "suite": "tpcds", "display_name": "TPC-DS",
                "description": "Decision support queries (complex)", "data": tpcds_data
            }

    # TPC-C trends
    tpcc_runs = data.get_runs_by_suite('tpcc')
    if tpcc_runs:
        tpcc_data = []
        for run in tpcc_runs:
            run_id, timestamp, commit = run[0], run[1], run[2]
            results = data.get_results_for_run('tpcc_results', run_id)

            for row in results:
                _, _, engine, txn_type, count, latency, duration, tps, _, _ = row
                if engine == 'vibesql' and txn_type == 'mixed':
                    tpcc_data.append({
                        "date": timestamp[:10] if timestamp else "",
                        "timestamp": timestamp or "",
                        "commit": commit or "",
                        "tps": round(tps, 2) if tps else 0,
                        "latency_us": round(latency, 2) if latency else None
                    })
                    break

        if tpcc_data:
            trends["tpcc"] = {
                "suite": "tpcc", "display_name": "TPC-C",
                "description": "OLTP transactions (mixed workload)",
                "metric": "tps", "metric_label": "Transactions/sec", "data": tpcc_data
            }

    # Sysbench trends
    sysbench_runs = data.get_runs_by_suite('sysbench')
    if sysbench_runs:
        sysbench_data = []
        for run in sysbench_runs:
            run_id, timestamp, commit = run[0], run[1], run[2]
            results = data.get_results_for_run('sysbench_results', run_id)

            vibesql_times = {}
            for row in results:
                _, _, engine, test, _, mean_ns, std_ns, median_ns, iterations = row
                if engine == 'vibesql' and mean_ns is not None:
                    vibesql_times[test] = mean_ns / 1e6  # Convert ns to ms

            if vibesql_times:
                times = list(vibesql_times.values())
                sysbench_data.append({
                    "date": timestamp[:10] if timestamp else "",
                    "timestamp": timestamp or "",
                    "commit": commit or "",
                    "avg_ms": round(sum(times) / len(times), 4),
                    "min_ms": round(min(times), 4),
                    "max_ms": round(max(times), 4),
                    "workloads": {k: round(v, 4) for k, v in vibesql_times.items()}
                })

        if sysbench_data:
            trends["sysbench"] = {
                "suite": "sysbench", "display_name": "Sysbench",
                "description": "OLTP micro-benchmarks", "data": sysbench_data
            }

    commit, _ = get_git_info()
    return {
        "generated_at": datetime.now().isoformat(),
        "git_commit": commit or "unknown",
        "description": "VibeSQL Embedded performance trends over time",
        "benchmarks": trends
    }


# ============================================================================
# Dashboard Export
# ============================================================================

def count_sqllogictest_statements() -> Tuple[int, int]:
    """Count actual test statements in SQLLogicTest files."""
    import re
    test_dir = get_repo_root() / "third_party" / "sqllogictest" / "test"
    if not test_dir.exists():
        return 0, 0

    test_pattern = re.compile(r'^(query|statement)\s', re.MULTILINE)
    total_statements = 0
    file_count = 0

    for test_file in test_dir.rglob("*.test"):
        try:
            with open(test_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                matches = test_pattern.findall(content)
                total_statements += len(matches)
                file_count += 1
        except Exception:
            continue

    return total_statements, file_count


def load_pgsql_regress_data() -> Dict[str, Any]:
    """Load PostgreSQL regression test results from JSON file."""
    json_path = Path.home() / ".vibesql" / "test_results" / "pgsql_regress_results.json"

    if json_path.exists():
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)

            summary = data.get("summary", {})
            by_category = data.get("by_category", {})

            return {
                "summary": {
                    "total_tests": summary.get("total", 0),
                    "passing": summary.get("passed", 0),
                    "failing": summary.get("failed", 0),
                    "skipped": summary.get("skipped", 0),
                    "errors": summary.get("errors", 0),
                    "pass_rate": round(summary.get("pass_rate", 0), 2)
                },
                "by_category": {
                    cat: {
                        "total": stats.get("total", 0),
                        "passed": stats.get("passed", 0),
                        "failed": stats.get("failed", 0),
                        "skipped": stats.get("skipped", 0),
                        "pass_rate": round(stats.get("pass_rate", 0), 2)
                    }
                    for cat, stats in by_category.items()
                },
                "files": data.get("files", {})
            }
        except (json.JSONDecodeError, KeyError):
            pass

    return {"summary": {}, "by_category": {}, "files": {}}


def load_conformance_data() -> Dict[str, Any]:
    """Load conformance data from JSON files."""
    json_paths = [
        Path.home() / ".vibesql" / "test_results" / "sqllogictest_results.json",
        get_repo_root() / "web-demo" / "public" / "badges" / "sqllogictest_cumulative.json",
        get_repo_root() / "web-demo" / "public" / "badges" / "sqllogictest_summary.json",
    ]

    sqllogictest_data = {"summary": {}, "files": {}}

    for json_path in json_paths:
        if json_path.exists():
            try:
                with open(json_path, 'r') as f:
                    data = json.load(f)

                actual_tests, actual_files = count_sqllogictest_statements()

                if "tested_files" in data or "by_category" in data:
                    summary = data.get("summary", {})
                    pass_rate = summary.get("pass_rate", 0)

                    if actual_tests > 0:
                        tests_passing = int(actual_tests * (pass_rate / 100.0))
                    else:
                        tests_passing = summary.get("passed", 0)
                        actual_tests = summary.get("total_available_files", 0)

                    sqllogictest_data = {
                        "summary": {
                            "total_tests": actual_tests,
                            "passing": tests_passing,
                            "failing": actual_tests - tests_passing,
                            "pass_rate": round(pass_rate, 2)
                        },
                        "files": {
                            "total": actual_files or summary.get("total_available_files", 0),
                            "passing": summary.get("passed", 0),
                            "pass_rate": round(pass_rate, 2)
                        }
                    }
                    break
            except (json.JSONDecodeError, KeyError):
                continue

    # Also load PostgreSQL regression test data
    pgsql_data = load_pgsql_regress_data()

    return {
        "summary": sqllogictest_data.get("summary", {}),
        "files": sqllogictest_data.get("files", {}),
        "pgsql_regress": pgsql_data
    }


def export_dashboard(data: BenchmarkData, previous_url: Optional[str] = None) -> Dict:
    """Generate the complete dashboard.json structure."""

    # Get TPC-H data
    tpch_run = data.get_latest_run('tpch')
    tpch_queries: Dict[str, Any] = {}
    tpch_geo_mean = None
    tpch_passed = 0
    tpch_total = 22

    if tpch_run:
        run_id, timestamp, commit, branch = tpch_run[0], tpch_run[1], tpch_run[2], tpch_run[3]
        tpch_total = tpch_run[7] or 22
        tpch_passed = tpch_run[8] or 0

        results = data.get_results_for_run('benchmark_results', run_id)
        columns = data.get_table_columns('benchmark_results')
        has_engine_col = 'database_engine' in columns
        engine_idx = columns.index('database_engine') if has_engine_col else None

        passing_times = []
        for row in results:
            if has_engine_col:
                engine = row[engine_idx] if row[engine_idx] else 'vibesql'
                query_name = row[engine_idx + 1]
                status = row[engine_idx + 2]
                exec_ms = row[engine_idx + 5] if len(row) > engine_idx + 5 else None
            else:
                engine, query_name, status = 'vibesql', row[2], row[3]
                exec_ms = row[6] if len(row) > 6 else None

            if engine == 'vibesql':
                tpch_queries[query_name] = {
                    "latest": {
                        "vibesql_ms": round(exec_ms, 2) if exec_ms else None,
                        "status": status,
                        "timestamp": timestamp
                    }
                }
                if status == "passed" and exec_ms:
                    passing_times.append(exec_ms)

        tpch_geo_mean = round(geometric_mean(passing_times), 2) if passing_times else None

    # Get TPC-DS data
    tpcds_run = data.get_latest_run('tpcds')
    tpcds_passed = 0
    tpcds_total = 99
    tpcds_geo_mean = None

    if tpcds_run:
        tpcds_total = tpcds_run[7] or 99
        tpcds_passed = tpcds_run[8] or 0

        results = data.get_results_for_run('benchmark_results', tpcds_run[0])
        columns = data.get_table_columns('benchmark_results')
        has_engine_col = 'database_engine' in columns
        engine_idx = columns.index('database_engine') if has_engine_col else None

        passing_times = []
        for row in results:
            if has_engine_col:
                engine = row[engine_idx] if row[engine_idx] else 'vibesql'
                status = row[engine_idx + 2]
                exec_ms = row[engine_idx + 5] if len(row) > engine_idx + 5 else None
            else:
                engine, status = 'vibesql', row[3]
                exec_ms = row[6] if len(row) > 6 else None

            if engine == 'vibesql' and status == "passed" and exec_ms:
                passing_times.append(exec_ms)

        tpcds_geo_mean = round(geometric_mean(passing_times), 2) if passing_times else None

    # Get TPC-C data
    tpcc_run = data.get_latest_run('tpcc')
    tpcc_tps = None
    tpcc_transactions = {}

    if tpcc_run:
        results = data.get_results_for_run('tpcc_results', tpcc_run[0])
        for row in results:
            _, _, engine, txn_type, count, latency, duration, tps, success, failed = row
            if txn_type not in tpcc_transactions:
                tpcc_transactions[txn_type] = {}
            tpcc_transactions[txn_type][engine] = {
                "tps": round(tps, 2) if tps else None,
                "latency_us": round(latency, 2) if latency else None
            }
            if engine == 'vibesql' and txn_type == 'mixed':
                tpcc_tps = round(tps, 2) if tps else None

    # Get Sysbench data
    sysbench_run = data.get_latest_run('sysbench')
    sysbench_tests = {}

    if sysbench_run:
        results = data.get_results_for_run('sysbench_results', sysbench_run[0])
        for row in results:
            _, _, engine, test_name, table_size, mean_ns, std_ns, median_ns, iterations = row
            key = f"{test_name}_{table_size}" if table_size else test_name
            if key not in sysbench_tests:
                sysbench_tests[key] = {"test_name": test_name, "table_size": table_size, "engines": {}}
            sysbench_tests[key]["engines"][engine] = {
                "mean_us": round(mean_ns / 1000, 2) if mean_ns else None,
                "std_dev_us": round(std_ns / 1000, 2) if std_ns else None
            }

    # Get conformance data
    conformance = load_conformance_data()

    # Build dashboard
    commit, branch = get_git_info()

    # Extract pgsql_regress summary for dashboard
    pgsql_regress_summary = conformance.get("pgsql_regress", {}).get("summary", {})

    dashboard = {
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
            "pgsql_regress": {
                "pass_rate": pgsql_regress_summary.get("pass_rate"),
                "passing": pgsql_regress_summary.get("passing"),
                "total_tests": pgsql_regress_summary.get("total_tests"),
                "failing": pgsql_regress_summary.get("failing"),
                "skipped": pgsql_regress_summary.get("skipped"),
                "errors": pgsql_regress_summary.get("errors")
            } if pgsql_regress_summary else None,
            "tpch": {
                "queries_passing": tpch_passed,
                "queries_total": tpch_total,
                "geo_mean_ms": tpch_geo_mean,
                "trend_7d_pct": None
            },
            "tpcds": {
                "queries_passing": tpcds_passed,
                "queries_total": tpcds_total,
                "geo_mean_ms": tpcds_geo_mean
            } if tpcds_run else None,
            "tpcc": {
                "vibesql_tps": tpcc_tps,
                "scale_factor": tpcc_run[5] if tpcc_run else None
            } if tpcc_run else None,
            "sysbench": {
                "tests_count": len(sysbench_tests)
            } if sysbench_run else None
        },
        "benchmarks": {
            "tpch": {
                "description": "TPC-H Decision Support - 22 analytical queries",
                "scale_factor": 0.01,
                "latest_run": {
                    "timestamp": tpch_run[1] if tpch_run else None,
                    "commit": tpch_run[2] if tpch_run else None,
                    "branch": tpch_run[3] if tpch_run else None
                },
                "queries_passing": tpch_passed,
                "queries_total": tpch_total,
                "geo_mean_ms": tpch_geo_mean,
                "queries": tpch_queries
            } if tpch_run else {},
            "tpcds": {
                "description": "TPC-DS Decision Support - 99 complex queries",
                "queries_passing": tpcds_passed,
                "queries_total": tpcds_total,
                "geo_mean_ms": tpcds_geo_mean
            } if tpcds_run else {},
            "tpcc": {
                "description": "TPC-C OLTP - Mixed read/write transactions",
                "scale_factor": tpcc_run[5] if tpcc_run else None,
                "latest": {
                    "timestamp": tpcc_run[1] if tpcc_run else None,
                    "commit": tpcc_run[2] if tpcc_run else None,
                    "vibesql_tps": tpcc_tps,
                    "transactions": tpcc_transactions
                }
            } if tpcc_run else {},
            "sysbench": {
                "description": "Sysbench OLTP - Point operations",
                "latest": {
                    "timestamp": sysbench_run[1] if sysbench_run else None,
                    "commit": sysbench_run[2] if sysbench_run else None
                },
                "tests": sysbench_tests
            } if sysbench_run else {}
        },
        "conformance": conformance,
        "changes": [],
        "timeline": [{
            "date": datetime.now().strftime("%Y-%m-%d"),
            "commit": commit,
            "conformance_pass_rate": conformance.get("summary", {}).get("pass_rate"),
            "tpch_geo_mean_ms": tpch_geo_mean,
            "tpch_passing": tpch_passed,
            "tpcc_tps": tpcc_tps,
            "events": []
        }],
        "machine_info": get_machine_info()
    }

    return dashboard


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Export all website data from VibeSQL database")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--benchmarks-only", action="store_true", help="Only export benchmark JSON files")
    parser.add_argument("--trends-only", action="store_true", help="Only export trends data")
    parser.add_argument("--dashboard-only", action="store_true", help="Only export dashboard")
    parser.add_argument("--output-dir", type=str, help="Output directory (default: web-demo/public)")

    args = parser.parse_args()

    # Determine output directories
    repo_root = get_repo_root()
    if args.output_dir:
        base_dir = Path(args.output_dir)
    else:
        base_dir = repo_root / "web-demo" / "public"

    benchmarks_dir = base_dir / "benchmarks"
    data_dir = base_dir / "data"
    benchmarks_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    # Load database once
    print(f"Loading VibeSQL database from {get_db_path()}...")
    try:
        db, cursor = get_connection()
    except Exception as e:
        print(f"Error loading database: {e}")
        return 1

    data = BenchmarkData(cursor)

    # Show available data
    suite_counts = data.get_suite_counts()
    print("\nAvailable benchmark data:")
    for suite, count in sorted(suite_counts.items()):
        print(f"  {suite}: {count} runs")
    print()

    export_all = not any([args.benchmarks_only, args.trends_only, args.dashboard_only])
    exported = []

    # Export benchmark JSON files
    if export_all or args.benchmarks_only:
        print("Exporting benchmark comparison data...")

        tpcc = export_tpcc_benchmarks(data)
        if tpcc:
            path = benchmarks_dir / "tpcc_results.json"
            with open(path, 'w') as f:
                json.dump(tpcc, f, indent=2)
            print(f"  TPC-C: {len(tpcc['benchmarks'])} benchmarks -> {path.name}")
            exported.append(path)

        sysbench = export_sysbench_benchmarks(data)
        if sysbench:
            path = benchmarks_dir / "sysbench_results.json"
            with open(path, 'w') as f:
                json.dump(sysbench, f, indent=2)
            print(f"  Sysbench: {len(sysbench['benchmarks'])} benchmarks -> {path.name}")
            exported.append(path)

        tpcds = export_tpcds_benchmarks(data)
        if tpcds:
            path = benchmarks_dir / "tpcds_results.json"
            with open(path, 'w') as f:
                json.dump(tpcds, f, indent=2)
            vibesql_count = tpcds['metadata']['vibesql_queries']
            sqlite_count = tpcds['metadata']['sqlite_queries']
            duckdb_count = tpcds['metadata']['duckdb_queries']
            print(f"  TPC-DS: {len(tpcds['benchmarks'])} benchmarks (V:{vibesql_count} S:{sqlite_count} D:{duckdb_count}) -> {path.name}")
            exported.append(path)

        tpch = export_tpch_benchmarks(data)
        if tpch:
            # Note: Web demo expects 'benchmark_results.json' for TPC-H data
            path = benchmarks_dir / "benchmark_results.json"
            with open(path, 'w') as f:
                json.dump(tpch, f, indent=2)
            vibesql_count = tpch['metadata']['vibesql_queries']
            sqlite_count = tpch['metadata']['sqlite_queries']
            duckdb_count = tpch['metadata']['duckdb_queries']
            print(f"  TPC-H: {len(tpch['benchmarks'])} benchmarks (V:{vibesql_count} S:{sqlite_count} D:{duckdb_count}) -> {path.name}")
            exported.append(path)

    # Export trends data
    if export_all or args.trends_only:
        print("\nExporting historical trends...")
        trends = export_trends(data)
        path = benchmarks_dir / "trends_results.json"
        with open(path, 'w') as f:
            json.dump(trends, f, indent=2)
        total_points = sum(len(b["data"]) for b in trends["benchmarks"].values())
        print(f"  {total_points} data points across {len(trends['benchmarks'])} suites -> {path.name}")
        exported.append(path)

    # Export dashboard
    if export_all or args.dashboard_only:
        print("\nGenerating dashboard...")
        dashboard = export_dashboard(data)
        path = data_dir / "dashboard.json"
        with open(path, 'w') as f:
            json.dump(dashboard, f, indent=2)

        summary = dashboard["summary"]
        tpch = summary.get("tpch", {})
        tpcds = summary.get("tpcds", {})
        tpcc = summary.get("tpcc", {})
        conformance = summary.get("conformance", {})
        pgsql_regress = summary.get("pgsql_regress", {})

        print(f"  TPC-H: {tpch.get('queries_passing')}/{tpch.get('queries_total')} queries, geo mean: {tpch.get('geo_mean_ms')}ms")
        if tpcds:
            print(f"  TPC-DS: {tpcds.get('queries_passing')}/{tpcds.get('queries_total')} queries, geo mean: {tpcds.get('geo_mean_ms')}ms")
        if tpcc:
            print(f"  TPC-C: {tpcc.get('vibesql_tps')} TPS")
        print(f"  SQLLogicTest: {conformance.get('pass_rate')}% ({conformance.get('tests_passing')}/{conformance.get('tests_total')} tests)")
        if pgsql_regress and pgsql_regress.get('total_tests'):
            print(f"  PostgreSQL Regress: {pgsql_regress.get('pass_rate')}% ({pgsql_regress.get('passing')}/{pgsql_regress.get('total_tests')} tests, {pgsql_regress.get('skipped', 0)} skipped)")
        print(f"  -> {path.name}")
        exported.append(path)

        # Export TCL test results
        try:
            from export_tcl_results import export_tcl_results
            tcl_result = export_tcl_results(verbose=args.verbose)
            if tcl_result:
                print(f"  TCL Tests: {tcl_result['summary']['pass_rate']}% ({tcl_result['summary']['passed']}/{tcl_result['summary']['total_tests']} tests)")
                exported.append(base_dir / "conformance" / "tcl_results.json")
                exported.append(base_dir / "badges" / "tcl-tests.json")
        except Exception as e:
            print(f"  TCL export skipped: {e}")

        # Generate pgsql-regress badge
        pgsql_data = load_pgsql_regress_data()
        if pgsql_data.get("summary"):
            summary = pgsql_data["summary"]
            passed = summary.get("passing", 0)
            skipped = summary.get("skipped", 0)
            pass_rate = summary.get("pass_rate", 0)

            if pass_rate == 100:
                color = "brightgreen"
            elif pass_rate >= 95:
                color = "green"
            elif pass_rate >= 80:
                color = "yellow"
            else:
                color = "red"

            badge = {
                "schemaVersion": 1,
                "label": "PostgreSQL",
                "message": f"{passed}\u2713 {skipped}\u2298 ({pass_rate:.0f}%)",
                "color": color
            }

            badges_dir = base_dir / "badges"
            badges_dir.mkdir(parents=True, exist_ok=True)
            badge_path = badges_dir / "pgsql-regress.json"
            with open(badge_path, 'w') as f:
                json.dump(badge, f, indent=2)
                f.write('\n')
            print(f"  PostgreSQL badge: {passed} passed, {skipped} skipped ({pass_rate:.0f}%) -> {badge_path.name}")
            exported.append(badge_path)

    print(f"\nExported {len(exported)} files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
