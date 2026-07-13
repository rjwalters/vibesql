#!/usr/bin/env python3
"""
Unified benchmark result processor for VibeSQL.

Consolidates result processing for all benchmark types (TPC-H, TPC-C, TPC-DS, Sysbench)
into a single script with auto-detection and consistent interface.

Usage:
    # Auto-detect benchmark type from input content
    ./scripts/process_results.py --input /tmp/results.txt

    # Explicit type specification
    ./scripts/process_results.py --type=tpch --input /tmp/tpch_results.txt
    ./scripts/process_results.py --type=tpcc --input /tmp/tpcc_results.txt
    ./scripts/process_results.py --type=tpcds --stdin
    ./scripts/process_results.py --type=sysbench --criterion-dir target/criterion

NOTE: This script dogfoods VibeSQL - we use our own database to store results!
"""

import argparse
import csv
import io
import json
import os
import re
import subprocess
import sys
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Import our VibeSQL helper module
from vibesql_db import (
    get_connection,
    save_connection,
    get_db_path,
    get_last_insert_id,
    init_schema,
    execute_insert,
)


# =============================================================================
# Common Utilities (shared across all benchmark types)
# =============================================================================

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


def get_machine_tag() -> Optional[str]:
    """Return the machine tag for this run, or None ("unknown/local").

    Sourced from the VIBESQL_MACHINE_TAG environment variable so a remote run
    (see scripts/remote-run.sh) can attribute its results to the dedicated host
    while local/CI runs leave it unset (NULL) and stay backward compatible.
    """
    tag = os.environ.get("VIBESQL_MACHINE_TAG")
    return tag if tag else None


def init_benchmark_schema(cursor) -> None:
    """Initialize benchmark tables in the database if they don't exist.

    Also applies additive migrations for older databases (e.g. adding the
    nullable ``machine_tag`` column to ``benchmark_runs``)."""
    schema_path = get_repo_root() / "scripts" / "benchmark_results_schema_vibesql.sql"

    if not schema_path.exists():
        print(f"Error: Schema file not found: {schema_path}")
        sys.exit(1)

    with open(schema_path) as f:
        schema_sql = f.read()

    init_schema(cursor, schema_sql)

    # Additive migration: a pre-existing benchmark_runs table is left untouched
    # by the CREATE TABLE above ("already exists"), so add the machine_tag
    # column explicitly for older databases. Nullable and idempotent — a second
    # run (column already present) is a no-op.
    try:
        cursor.execute("ALTER TABLE benchmark_runs ADD COLUMN machine_tag TEXT")
    except Exception:
        # Column already exists (or ALTER unsupported on this build) — safe to
        # ignore; the CREATE TABLE path already produced the column for new DBs.
        pass


def convert_time_to_ms(value: float, unit: str) -> float:
    """Convert time value to milliseconds."""
    unit = unit.lower()
    if unit in ('µs', 'μs', 'us'):
        return value / 1000
    elif unit == 'ms':
        return value
    elif unit == 's':
        return value * 1000
    elif unit == 'ns':
        return value / 1_000_000
    else:
        return value


# =============================================================================
# Parser Base Class
# =============================================================================

class BenchmarkParser(ABC):
    """Abstract base class for benchmark result parsers."""

    @property
    @abstractmethod
    def benchmark_suite(self) -> str:
        """Return the benchmark suite name (e.g., 'tpch', 'tpcc')."""
        pass

    @abstractmethod
    def parse(self, content: str) -> Tuple[List[Dict], Dict]:
        """
        Parse benchmark output.

        Returns:
            - List of individual results (dicts)
            - Summary statistics (dict)
        """
        pass

    @abstractmethod
    def insert_results(self, db, cursor, results: List[Dict], summary: Dict,
                       config: Dict) -> int:
        """
        Insert results into the database.

        Returns:
            The run_id of the inserted benchmark run
        """
        pass

    @classmethod
    def can_parse(cls, content: str) -> bool:
        """Check if this parser can handle the given content."""
        return False


# =============================================================================
# TPC-H Parser
# =============================================================================

class TPCHParser(BenchmarkParser):
    """Parser for TPC-H benchmark results."""

    @property
    def benchmark_suite(self) -> str:
        return 'tpch'

    @classmethod
    def can_parse(cls, content: str) -> bool:
        """Check for TPC-H specific markers."""
        return ('=== Q1 ===' in content or '=== Q2 ===' in content or
                'TPC-H Benchmark' in content or '--- VibeSQL ---' in content)

    def parse(self, content: str) -> Tuple[List[Dict], Dict]:
        """Parse TPC-H benchmark output.

        Supports two formats:
        1. Profiling format (VibeSQL only with Parse/Executor/Execute breakdown)
        2. Comparison format (multi-engine with "--- Engine ---" headers)
        """
        # Check if this is comparison format (has engine headers)
        if '--- VibeSQL ---' in content or '--- SQLite ---' in content or '--- DuckDB ---' in content:
            return self._parse_comparison_format(content)

        # Otherwise use profiling format
        return self._parse_profiling_format(content)

    def _parse_comparison_format(self, content: str) -> Tuple[List[Dict], Dict]:
        """Parse comparison format output with multiple engines.

        Format:
            --- VibeSQL ---
              Q1                       3.26ms avg (    2.88ms -     3.71ms)
              Q2                      87.47ms avg (   86.49ms -    89.29ms)
            --- SQLite ---
              Q1                      31.55ms avg (   30.62ms -    32.11ms)
        """
        results = []
        current_engine = None
        scale_factor = None

        # Pattern for engine section headers: "--- VibeSQL ---"
        engine_pattern = re.compile(r'^---\s+(\w+)\s+---$')

        # Pattern for query results: "  Q1                       3.26ms avg (    2.88ms -     3.71ms)"
        query_pattern = re.compile(
            r'^\s+(Q\d+)\s+'
            r'([\d.]+)(ms|s|µs|us)\s+avg\s+\(\s*([\d.]+)(ms|s|µs|us)\s+-\s+([\d.]+)(ms|s|µs|us)\s*\)'
        )
        # Pattern for timeout/error lines
        error_pattern = re.compile(r'^\s+(Q\d+)\s+(TIMEOUT|ERROR|SKIP|FAILED).*')

        # Extract scale factor
        for line in content.split('\n'):
            sf_match = re.match(r'.*Scale factor:\s*([\d.]+)', line)
            if sf_match:
                scale_factor = float(sf_match.group(1))
                break

        for line in content.split('\n'):
            # Check for engine header
            engine_match = engine_pattern.match(line)
            if engine_match:
                current_engine = engine_match.group(1).lower()
                continue

            if not current_engine:
                continue

            # Check for successful query result
            query_match = query_pattern.match(line)
            if query_match:
                query_name = query_match.group(1)
                mean_val = float(query_match.group(2))
                mean_unit = query_match.group(3)

                # Convert to milliseconds
                mean_ms = convert_time_to_ms(mean_val, mean_unit)

                results.append({
                    'database_engine': current_engine,
                    'query_name': query_name,
                    'parse_time_ms': None,
                    'executor_creation_time_ms': None,
                    'execution_time_ms': mean_ms,
                    'total_time_ms': mean_ms,
                    'row_count': None,
                    'status': 'passed',
                    'error_message': None
                })
                continue

            # Check for error/timeout results
            error_match = error_pattern.match(line)
            if error_match:
                query_name = error_match.group(1)
                error_type = error_match.group(2).lower()
                status = 'timeout' if error_type == 'timeout' else 'failed'

                results.append({
                    'database_engine': current_engine,
                    'query_name': query_name,
                    'parse_time_ms': None,
                    'executor_creation_time_ms': None,
                    'execution_time_ms': None,
                    'total_time_ms': None,
                    'row_count': None,
                    'status': status,
                    'error_message': line.strip()
                })

        # Calculate summary (using vibesql results)
        vibesql_results = [r for r in results if r.get('database_engine') == 'vibesql']
        summary = {
            'total_queries': len(vibesql_results),
            'passed_queries': sum(1 for r in vibesql_results if r['status'] == 'passed'),
            'failed_queries': sum(1 for r in vibesql_results if r['status'] in ('error', 'failed')),
            'timeout_queries': sum(1 for r in vibesql_results if r['status'] == 'timeout'),
            'scale_factor': scale_factor
        }

        return results, summary

    def _parse_profiling_format(self, content: str) -> Tuple[List[Dict], Dict]:
        """Parse profiling format output (VibeSQL only with detailed timing)."""
        results = []
        current_query = None
        scale_factor = None

        for line in content.split('\n'):
            # Scale factor: Loading TPC-H database (SF 0.01)...
            sf_match = re.match(r'.*Loading TPC-H database \(SF ([\d.]+)\)', line)
            if sf_match:
                scale_factor = float(sf_match.group(1))
                continue
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
                parse_time = convert_time_to_ms(float(parse_match.group(1)), parse_match.group(2))
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

            # Executor creation: Executor: 0.456ms
            executor_match = re.match(r'^\s*Executor:\s+([\d.]+)(ms|s|µs)', line)
            if executor_match and results and results[-1]['query_name'] == current_query:
                exec_time = convert_time_to_ms(float(executor_match.group(1)), executor_match.group(2))
                results[-1]['executor_creation_time_ms'] = exec_time
                continue

            # Execution with success: Execute: 123.456ms (1000 rows)
            execute_success_match = re.match(r'^\s*Execute:\s+([\d.]+)(ms|s|µs)\s+\((\d+)\s+rows?\)', line)
            if execute_success_match and results and results[-1]['query_name'] == current_query:
                exec_time = convert_time_to_ms(float(execute_success_match.group(1)), execute_success_match.group(2))
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
                total_time = convert_time_to_ms(float(total_match.group(1)), total_match.group(2))
                results[-1]['total_time_ms'] = total_time
                continue

            # Timeout: TIMEOUT
            if 'TIMEOUT' in line and results and results[-1]['query_name'] == current_query:
                results[-1]['status'] = 'timeout'
                continue

        # Mark incomplete queries
        for result in results:
            if result['status'] is None:
                result['status'] = 'incomplete'
                result['error_message'] = 'Query output was truncated or incomplete'

        # Calculate summary
        summary = {
            'total_queries': len(results),
            'passed_queries': sum(1 for r in results if r['status'] == 'passed'),
            'failed_queries': sum(1 for r in results if r['status'] == 'error'),
            'timeout_queries': sum(1 for r in results if r['status'] == 'timeout'),
            'scale_factor': scale_factor
        }

        return results, summary

    def insert_results(self, db, cursor, results: List[Dict], summary: Dict,
                       config: Dict) -> int:
        """Insert TPC-H results into the database."""
        git_commit, git_branch = get_git_info()
        timeout_secs = config.get('timeout', 30)
        notes = config.get('notes')

        execute_insert(cursor, "benchmark_runs", [
            "run_timestamp", "git_commit", "git_branch", "benchmark_suite",
            "scale_factor", "timeout_secs", "total_queries", "passed_queries",
            "failed_queries", "timeout_queries", "notes", "machine_tag"
        ], [
            datetime.now().isoformat(),
            git_commit,
            git_branch,
            'tpch',
            str(summary.get('scale_factor')) if summary.get('scale_factor') is not None else None,
            timeout_secs,
            summary['total_queries'],
            summary['passed_queries'],
            summary['failed_queries'],
            summary['timeout_queries'],
            notes,
            get_machine_tag()
        ])

        run_id = get_last_insert_id(cursor, "benchmark_runs", "run_id")

        for result in results:
            execute_insert(cursor, "benchmark_results", [
                "run_id", "database_engine", "query_name", "status",
                "parse_time_ms", "executor_creation_time_ms",
                "execution_time_ms", "total_time_ms",
                "row_count", "error_message"
            ], [
                run_id,
                result.get('database_engine', 'vibesql'),
                result['query_name'],
                result['status'],
                result['parse_time_ms'],
                result['executor_creation_time_ms'],
                result['execution_time_ms'],
                result['total_time_ms'],
                result['row_count'],
                result['error_message']
            ])

        save_connection(db)
        return run_id


# =============================================================================
# TPC-C Parser
# =============================================================================

class TPCCParser(BenchmarkParser):
    """Parser for TPC-C benchmark results."""

    @property
    def benchmark_suite(self) -> str:
        return 'tpcc'

    @classmethod
    def can_parse(cls, content: str) -> bool:
        """Check for TPC-C specific markers."""
        return ('--- VibeSQL Benchmark ---' in content or
                '--- SQLite Benchmark ---' in content or
                'Transaction type:' in content)

    def parse(self, content: str) -> Tuple[List[Dict], Dict]:
        """Parse TPC-C benchmark output."""
        results = []
        current_engine = None
        current_result = None
        scale_factor = None

        def save_current_result():
            nonlocal current_result
            if current_result and 'transaction_count' in current_result:
                if 'avg_latencies' in current_result and current_result.get('transaction_count', 0) > 0:
                    total_latency = sum(current_result['avg_latencies'].values())
                    current_result['avg_latency_us'] = total_latency / len(current_result['avg_latencies'])
                results.append(current_result)
                current_result = None

        for line in content.split('\n'):
            # Scale factor: "Scale factor: 1" or "Loading VibeSQL TPC-C database (SF 1)..."
            sf_match = re.match(r'^\s*Scale factor:\s*([\d.]+)', line)
            if sf_match:
                scale_factor = float(sf_match.group(1))
                continue
            sf_match2 = re.match(r'.*Loading.*TPC-C.*\(SF ([\d.]+)\)', line)
            if sf_match2:
                scale_factor = float(sf_match2.group(1))
                continue
            # Detect engine section
            if '--- VibeSQL Benchmark ---' in line:
                save_current_result()
                current_engine = 'vibesql'
                current_result = {'database_engine': 'vibesql'}
            elif '--- SQLite Benchmark ---' in line:
                save_current_result()
                current_engine = 'sqlite'
                current_result = {'database_engine': 'sqlite'}
            elif '--- DuckDB Benchmark ---' in line:
                save_current_result()
                current_engine = 'duckdb'
                current_result = {'database_engine': 'duckdb'}
            elif '--- MySQL Benchmark ---' in line:
                save_current_result()
                current_engine = 'mysql'
                current_result = {'database_engine': 'mysql'}

            if not current_result:
                continue

            # Parse fields
            txn_type_match = re.match(r'^Transaction type:\s+(.+)$', line)
            if txn_type_match:
                current_result['transaction_type'] = txn_type_match.group(1).lower().replace('-', '_')

            total_match = re.match(r'^Total transactions:\s+(\d+)', line)
            if total_match:
                current_result['transaction_count'] = int(total_match.group(1))

            success_match = re.match(r'^Successful:\s+(\d+)', line)
            if success_match:
                current_result['successful_transactions'] = int(success_match.group(1))

            failed_match = re.match(r'^Failed:\s+(\d+)', line)
            if failed_match:
                current_result['failed_transactions'] = int(failed_match.group(1))

            duration_match = re.match(r'^Duration:\s+(\d+)\s+ms', line)
            if duration_match:
                current_result['total_duration_ms'] = int(duration_match.group(1))

            tps_match = re.match(r'^Throughput:\s+([\d.]+)\s+TPS', line)
            if tps_match:
                current_result['transactions_per_second'] = float(tps_match.group(1))

            txn_avg_match = re.match(r'^(\w+(?:-\w+)?):\s+\d+\s+txns,\s+avg\s+([\d.]+)\s+us', line)
            if txn_avg_match:
                txn_name = txn_avg_match.group(1).lower().replace('-', '_')
                avg_us = float(txn_avg_match.group(2))
                if 'avg_latencies' not in current_result:
                    current_result['avg_latencies'] = {}
                current_result['avg_latencies'][txn_name] = avg_us

            if '=== Comparison Summary ===' in line or '=== Done ===' in line:
                save_current_result()

        save_current_result()

        summary = {
            'total_engines': len(results),
            'total_transactions': sum(r.get('transaction_count', 0) for r in results),
            'scale_factor': scale_factor
        }

        return results, summary

    def insert_results(self, db, cursor, results: List[Dict], summary: Dict,
                       config: Dict) -> int:
        """Insert TPC-C results into the database."""
        git_commit, git_branch = get_git_info()
        # Prefer parsed scale_factor from output, fall back to config
        scale_factor = summary.get('scale_factor') or config.get('scale_factor', 1)
        duration_secs = config.get('duration', 60)
        notes = config.get('notes')

        execute_insert(cursor, "benchmark_runs", [
            "run_timestamp", "git_commit", "git_branch", "benchmark_suite",
            "scale_factor", "timeout_secs", "total_queries", "notes", "machine_tag"
        ], [
            datetime.now().isoformat(),
            git_commit,
            git_branch,
            'tpcc',
            str(scale_factor),
            duration_secs,
            sum(r.get('transaction_count', 0) for r in results),
            notes,
            get_machine_tag()
        ])

        run_id = get_last_insert_id(cursor, "benchmark_runs", "run_id")

        for result in results:
            execute_insert(cursor, "tpcc_results", [
                "run_id", "database_engine", "transaction_type",
                "transaction_count", "avg_latency_us", "total_duration_ms",
                "transactions_per_second", "successful_transactions", "failed_transactions"
            ], [
                run_id,
                result.get('database_engine', 'unknown'),
                result.get('transaction_type', 'mixed'),
                result.get('transaction_count', 0),
                result.get('avg_latency_us'),
                result.get('total_duration_ms'),
                result.get('transactions_per_second'),
                result.get('successful_transactions', 0),
                result.get('failed_transactions', 0)
            ])

        save_connection(db)
        return run_id


# =============================================================================
# TPC-DS Parser
# =============================================================================

class TPCDSParser(BenchmarkParser):
    """Parser for TPC-DS benchmark results."""

    @property
    def benchmark_suite(self) -> str:
        return 'tpcds'

    @classmethod
    def can_parse(cls, content: str) -> bool:
        """Check for TPC-DS specific markers."""
        return ('=== CSV Output ===' in content or
                'tpcds_queries' in content or
                'TPC-DS' in content or
                '--- VibeSQL ---' in content)  # Custom harness format

    def parse(self, content: str) -> Tuple[List[Dict], Dict]:
        """Parse TPC-DS benchmark output."""
        # Extract scale factor from output
        # Format: "Scale factor: 0.001" or "Loading VibeSQL database (SF 0.001)..."
        scale_factor = None
        for line in content.split('\n'):
            sf_match = re.match(r'^\s*Scale factor:\s*([\d.]+)', line)
            if sf_match:
                scale_factor = float(sf_match.group(1))
                break
            sf_match2 = re.match(r'.*Loading.*database.*\(SF ([\d.]+)\)', line)
            if sf_match2:
                scale_factor = float(sf_match2.group(1))
                break

        # Try CSV format first (from tpcds_runner)
        if '=== CSV Output ===' in content:
            results = self._parse_csv_output(content)
        # Try custom harness format (post-Criterion migration)
        elif '--- VibeSQL ---' in content or '--- SQLite ---' in content:
            results = self._parse_custom_harness_output(content)
        # Try Criterion stdout format (legacy)
        elif 'tpcds_queries' in content and 'time:' in content:
            results = self._parse_criterion_output(content)
        else:
            results = []

        # Calculate summary
        summary = {
            'total_queries': len(results),
            'passed_queries': sum(1 for r in results if r.get('status') == 'passed'),
            'failed_queries': sum(1 for r in results if r.get('status') == 'failed'),
            'timeout_queries': sum(1 for r in results if r.get('status') == 'timeout'),
            'error_queries': sum(1 for r in results if r.get('status') == 'error'),
            'incomplete_queries': sum(1 for r in results if r.get('status') == 'incomplete'),
            'scale_factor': scale_factor
        }

        return results, summary

    def _parse_csv_output(self, content: str) -> List[Dict]:
        """Parse CSV output from tpcds_runner."""
        results = []

        csv_start = content.find("=== CSV Output ===")
        if csv_start == -1:
            return results

        csv_content = content[csv_start:]
        lines = csv_content.strip().split('\n')

        if len(lines) < 2:
            return results

        csv_lines = lines[2:]  # Skip header lines
        reader = csv.reader(io.StringIO('\n'.join(csv_lines)))

        for row in reader:
            if len(row) < 4:
                continue

            query_name, time_str, rows_str, status = row[0], row[1], row[2], row[3]

            # Determine status category
            if status == "OK":
                status_cat = "passed"
            elif "SKIPPED" in status:
                status_cat = "incomplete"
            elif "Parse error" in status:
                status_cat = "error"
            else:
                status_cat = "failed"

            # Parse timing
            exec_time_ms = None
            if time_str != "-":
                try:
                    exec_time_ms = float(time_str)
                except ValueError:
                    pass

            # Parse row count
            row_count = None
            if rows_str != "-" and rows_str != "0":
                try:
                    row_count = int(rows_str)
                except ValueError:
                    pass

            # Error message
            error_msg = status if status_cat in ("failed", "error") else None

            results.append({
                'database_engine': 'vibesql',
                'query_name': query_name,
                'group_name': 'tpcds_queries',
                'mean_time_ms': exec_time_ms,
                'std_dev_ms': None,
                'median_time_ms': None,
                'iterations': 1,
                'row_count': row_count,
                'status': status_cat,
                'error_message': error_msg
            })

        return results

    def _parse_custom_harness_output(self, content: str) -> List[Dict]:
        """Parse custom benchmark harness output (post-Criterion migration).

        Format:
            --- VibeSQL ---
              Q1                       6.57ms avg (    6.05ms -     7.01ms)
              Q2                      87.47ms avg (   86.49ms -    89.29ms)
            --- SQLite ---
              Q1                      12.34ms avg (   11.00ms -    13.50ms)
        """
        results = []
        current_engine = None

        # Pattern for engine section headers: "--- VibeSQL ---"
        engine_pattern = re.compile(r'^---\s+(\w+)\s+---$')

        # Pattern for query results: "  Q1                       6.57ms avg (    6.05ms -     7.01ms)"
        # Also handles seconds: "  Q6                        3.63s avg (     3.59s -      3.65s)"
        # Also handles microseconds: "  Q82                    316.61µs avg (  280.17µs -   392.29µs)"
        # And timeout/error lines: "  Q13                    TIMEOUT (30s)"
        query_pattern = re.compile(
            r'^\s+(Q\d+|sanity_\w+)\s+'
            r'([\d.]+)(ms|s|µs)\s+avg\s+\(\s*([\d.]+)(ms|s|µs)\s+-\s+([\d.]+)(ms|s|µs)\s*\)'
        )
        error_pattern = re.compile(
            r'^\s+(Q\d+|sanity_\w+)\s+(TIMEOUT|ERROR|SKIP|FAILED).*'
        )

        for line in content.split('\n'):
            # Check for engine header
            engine_match = engine_pattern.match(line)
            if engine_match:
                current_engine = engine_match.group(1).lower()
                continue

            if not current_engine:
                continue

            # Check for successful query result
            query_match = query_pattern.match(line)
            if query_match:
                query_name = query_match.group(1)
                mean_val = float(query_match.group(2))
                mean_unit = query_match.group(3)

                # Convert to milliseconds
                if mean_unit == 's':
                    mean_ms = mean_val * 1000
                elif mean_unit == 'µs':
                    mean_ms = mean_val / 1000
                else:  # ms
                    mean_ms = mean_val

                results.append({
                    'database_engine': current_engine,
                    'query_name': query_name,
                    'group_name': 'tpcds_queries',
                    'mean_time_ms': mean_ms,
                    'std_dev_ms': None,
                    'median_time_ms': None,
                    'iterations': 10,  # Default for custom harness
                    'status': 'passed'
                })
                continue

            # Check for error/timeout results
            error_match = error_pattern.match(line)
            if error_match:
                query_name = error_match.group(1)
                error_type = error_match.group(2).lower()

                status = 'timeout' if error_type == 'timeout' else 'failed'

                results.append({
                    'database_engine': current_engine,
                    'query_name': query_name,
                    'group_name': 'tpcds_queries',
                    'mean_time_ms': None,
                    'std_dev_ms': None,
                    'median_time_ms': None,
                    'iterations': 0,
                    'status': status,
                    'error_message': line.strip()
                })

        return results

    def _parse_criterion_output(self, content: str) -> List[Dict]:
        """Parse Criterion benchmark output from stdout (legacy)."""
        results = []

        pattern = re.compile(
            r'^(tpcds_\w+)/(\w+)/(\w+)\s+'
            r'time:\s+\[([\d.]+)\s+(us|ms|ns|s)\s+'
            r'([\d.]+)\s+(us|ms|ns|s)\s+'
            r'([\d.]+)\s+(us|ms|ns|s)\]',
            re.MULTILINE
        )

        for match in pattern.finditer(content):
            group_name = match.group(1)
            engine = match.group(2).lower()
            query_name = match.group(3)
            mean_val = float(match.group(6))
            mean_unit = match.group(7)
            mean_ms = convert_time_to_ms(mean_val, mean_unit)

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

    def parse_criterion_directory(self, criterion_dir: Path) -> List[Dict]:
        """Parse Criterion benchmark results from directory."""
        results = []

        for bench_group in criterion_dir.glob("tpcds_*"):
            if not bench_group.is_dir():
                continue

            group_name = bench_group.name

            for engine_dir in bench_group.iterdir():
                if not engine_dir.is_dir():
                    continue

                engine = engine_dir.name.lower()

                for query_dir in engine_dir.iterdir():
                    if not query_dir.is_dir():
                        continue

                    query_name = query_dir.name
                    estimates_file = query_dir / "new" / "estimates.json"

                    if not estimates_file.exists():
                        continue

                    try:
                        with open(estimates_file) as f:
                            estimates = json.load(f)

                        mean_ns = estimates.get("mean", {}).get("point_estimate", 0)
                        std_dev_ns = estimates.get("std_dev", {}).get("point_estimate", 0)
                        median_ns = estimates.get("median", {}).get("point_estimate", 0)

                        sample_file = query_dir / "new" / "sample.json"
                        iterations = 0
                        if sample_file.exists():
                            with open(sample_file) as f:
                                sample = json.load(f)
                                iterations = len(sample.get("times", []))

                        results.append({
                            'database_engine': engine,
                            'query_name': query_name,
                            'group_name': group_name,
                            'mean_time_ms': mean_ns / 1_000_000,
                            'std_dev_ms': std_dev_ns / 1_000_000,
                            'median_time_ms': median_ns / 1_000_000,
                            'iterations': iterations,
                            'status': 'passed'
                        })
                    except (json.JSONDecodeError, KeyError) as e:
                        print(f"Warning: Failed to parse {estimates_file}: {e}")
                        continue

        return results

    def insert_results(self, db, cursor, results: List[Dict], summary: Dict,
                       config: Dict) -> int:
        """Insert TPC-DS results into the database."""
        git_commit, git_branch = get_git_info()
        # Prefer parsed scale_factor from output, fall back to config
        scale_factor = summary.get('scale_factor') or config.get('scale_factor', 0.01)
        notes = config.get('notes')

        # Get vibesql results for run summary (total_queries count)
        vibesql_results = [r for r in results if r.get('database_engine') == 'vibesql']
        if not vibesql_results:
            # If no engine specified, assume all results are vibesql
            vibesql_results = results

        execute_insert(cursor, "benchmark_runs", [
            "run_timestamp", "git_commit", "git_branch", "benchmark_suite",
            "scale_factor", "total_queries", "passed_queries", "failed_queries",
            "timeout_queries", "notes", "machine_tag"
        ], [
            datetime.now().isoformat(),
            git_commit,
            git_branch,
            'tpcds',
            str(scale_factor),
            len(vibesql_results),
            len([r for r in vibesql_results if r.get('status') == 'passed']),
            len([r for r in vibesql_results if r.get('status') == 'failed']),
            len([r for r in vibesql_results if r.get('status') == 'timeout']),
            notes,
            get_machine_tag()
        ])

        run_id = get_last_insert_id(cursor, "benchmark_runs", "run_id")

        # Insert ALL results (vibesql, sqlite, duckdb, mysql) - not just vibesql
        for result in results:
            execute_insert(cursor, "benchmark_results", [
                "run_id", "database_engine", "query_name", "status",
                "execution_time_ms", "total_time_ms", "row_count", "error_message"
            ], [
                run_id,
                result.get('database_engine', 'vibesql'),
                result.get('query_name', 'unknown'),
                result.get('status', 'passed'),
                result.get('mean_time_ms'),
                result.get('mean_time_ms'),
                result.get('row_count'),
                result.get('error_message')
            ])

        # Log comparison data counts
        engines = {}
        for r in results:
            eng = r.get('database_engine', 'vibesql')
            engines[eng] = engines.get(eng, 0) + 1
        if len(engines) > 1:
            print(f"   Stored results for engines: {engines}")

        save_connection(db)
        return run_id


# =============================================================================
# Sysbench Parser
# =============================================================================

class SysbenchParser(BenchmarkParser):
    """Parser for Sysbench OLTP benchmark results."""

    # Map display names to database field names
    WORKLOAD_NAME_MAP = {
        'point select': 'point_select',
        'insert': 'insert',
        'update index': 'update_index',
        'update non-index': 'update_non_index',
        'delete': 'delete',
        'range queries': 'range_queries',
        'range': 'range_queries',
    }

    @property
    def benchmark_suite(self) -> str:
        return 'sysbench'

    # Map engine display names to database field names (for server mode)
    ENGINE_NAME_MAP = {
        'vibesql server': 'vibesql_server',
        'mysql': 'mysql',
        'vibesql': 'vibesql',
        'sqlite': 'sqlite',
        'duckdb': 'duckdb',
    }

    @classmethod
    def can_parse(cls, content: str) -> bool:
        """Check for Sysbench specific markers."""
        # New custom harness format (post-Criterion migration)
        if '--- VibeSQL Results ---' in content and 'Workload' in content:
            return True
        # Server mode format (vibesql-server vs MySQL)
        if '--- VibeSQL Server Results ---' in content and 'Workload' in content:
            return True
        # Legacy Criterion format (for backwards compatibility)
        return ('sysbench_' in content or
                'point_select' in content or
                'oltp_read' in content)

    def _latency_to_ns(self, value: float, unit: str) -> float:
        """Convert latency value to nanoseconds."""
        unit = unit.lower()
        if unit in ('µs', 'μs', 'us'):
            return value * 1000
        elif unit == 'ms':
            return value * 1_000_000
        elif unit == 's':
            return value * 1_000_000_000
        elif unit == 'ns':
            return value
        # Default to microseconds (most common in sysbench output)
        return value * 1000

    def _parse_custom_harness(self, content: str) -> Tuple[List[Dict], Dict]:
        """Parse new custom harness output format (post-Criterion migration).

        Expected format (embedded mode):
            --- VibeSQL Results ---
            Workload               Client   Operations     Avg Latency      Ops/sec
            -------------------- -------- ------------ --------------- ------------
            Point Select              all       500000         2.00 us       500000
            Insert                    all       100000        10.00 us       100000
            ...

            --- SQLite Results ---
            ...

        Expected format (server mode):
            --- VibeSQL Server Results ---
            Workload               Operations     Avg Latency      Ops/sec
            -------------------- ------------ --------------- ------------
            Point Select               644340        46.01 us        21732
            ...

            --- MySQL Results ---
            ...
        """
        results = []
        current_engine = None
        in_comparison_section = False

        # Extract table size from configuration section
        table_size = 10000  # Default
        config_match = re.search(r'Table size:\s+(\d+)\s+rows', content)
        if config_match:
            table_size = int(config_match.group(1))

        # Engine section header pattern: "--- VibeSQL Results ---" or "--- VibeSQL Server Results ---"
        # Captures multi-word engine names like "VibeSQL Server"
        engine_pattern = re.compile(r'^---\s+([\w\s]+?)\s+Results.*---', re.MULTILINE)

        # Comparison summary section marker - stop parsing here
        comparison_pattern = re.compile(r'^===\s+Comparison Summary\s+===', re.MULTILINE)

        # Result row pattern for embedded mode (with client column):
        # "Point Select              all       500000         2.00 us       500000"
        result_pattern_embedded = re.compile(
            r'^([\w\s-]+?)\s{2,}'        # Workload name (greedy until 2+ spaces)
            r'(?:all|\d+|TOTAL)\s+'      # Client column (all, number, or TOTAL)
            r'(\d+)\s+'                  # Operations
            r'([\d.]+)\s+(us|ms|ns)\s+'  # Avg Latency + unit
            r'([\d.]+)',                 # Ops/sec
            re.MULTILINE
        )

        # Result row pattern for server mode (no client column):
        # "Point Select               644340        46.01 us        21732"
        result_pattern_server = re.compile(
            r'^([\w\s-]+?)\s{2,}'        # Workload name (greedy until 2+ spaces)
            r'(\d+)\s+'                  # Operations
            r'([\d.]+)\s+(us|ms|ns)\s+'  # Avg Latency + unit
            r'([\d.]+)',                 # Ops/sec
            re.MULTILINE
        )

        # Detect if this is server mode (no client column in header)
        is_server_mode = '--- VibeSQL Server Results ---' in content

        for line in content.split('\n'):
            # Check for comparison summary section - stop parsing here
            if comparison_pattern.match(line):
                in_comparison_section = True
                continue

            # Skip everything after comparison summary
            if in_comparison_section:
                continue

            # Check for engine section header
            engine_match = engine_pattern.match(line)
            if engine_match:
                engine_display = engine_match.group(1).strip().lower()
                # Map display name to database field name
                current_engine = self.ENGINE_NAME_MAP.get(engine_display, engine_display.replace(' ', '_'))
                continue

            # Skip lines without an active engine context
            if not current_engine:
                continue

            # Skip header and separator lines
            if 'Workload' in line or line.startswith('---') or line.startswith('==='):
                continue

            # Try to match result row (use appropriate pattern based on mode)
            result_pattern = result_pattern_server if is_server_mode else result_pattern_embedded
            result_match = result_pattern.match(line)
            if result_match:
                workload_display = result_match.group(1).strip().lower()
                operations = int(result_match.group(2))
                latency_val = float(result_match.group(3))
                latency_unit = result_match.group(4)
                # ops_per_sec = float(result_match.group(5))  # Not stored in DB

                # Map display name to database field name
                test_name = self.WORKLOAD_NAME_MAP.get(workload_display, workload_display.replace(' ', '_'))

                # Convert latency to nanoseconds
                mean_time_ns = self._latency_to_ns(latency_val, latency_unit)

                results.append({
                    'database_engine': current_engine,
                    'test_name': test_name,
                    'table_size': table_size,
                    'mean_time_ns': mean_time_ns,
                    'std_dev_ns': None,  # Not available in new format
                    'median_time_ns': mean_time_ns,  # Use mean as approximation
                    'iterations': operations
                })

        summary = {
            'total_results': len(results),
            'engines': list(set(r.get('database_engine', 'unknown') for r in results))
        }

        return results, summary

    def _parse_criterion_stdout(self, content: str) -> Tuple[List[Dict], Dict]:
        """Parse legacy Criterion benchmark output from stdout."""
        results = []

        # Build iterations map
        iterations_map = {}
        collect_pattern = re.compile(
            r'Benchmarking\s+(\S+)\s*\n.*?Collecting\s+(\d+)\s+samples.*?\(([0-9.]+)([kMG]?)\s+iterations\)',
            re.DOTALL
        )
        for match in collect_pattern.finditer(content):
            bench_name = match.group(1)
            iter_val = float(match.group(3))
            iter_suffix = match.group(4)
            if iter_suffix == 'k':
                iterations = int(iter_val * 1000)
            elif iter_suffix == 'M':
                iterations = int(iter_val * 1_000_000)
            elif iter_suffix == 'G':
                iterations = int(iter_val * 1_000_000_000)
            else:
                iterations = int(iter_val)
            iterations_map[bench_name] = iterations

        # Parse timing results
        pattern = re.compile(
            r'^(\w+)/(\w+)/(\d+)\s*\n\s*'
            r'time:\s+\[([\d.]+)\s+([µμu]?s|ms|ns)\s+'
            r'([\d.]+)\s+([µμu]?s|ms|ns)\s+'
            r'([\d.]+)\s+([µμu]?s|ms|ns)\]',
            re.MULTILINE
        )

        for match in pattern.finditer(content):
            test_name = match.group(1).replace("sysbench_", "")
            engine = match.group(2).lower()
            table_size = int(match.group(3))

            low_val = float(match.group(4))
            low_unit = match.group(5)
            mean_val = float(match.group(6))
            mean_unit = match.group(7)
            high_val = float(match.group(8))
            high_unit = match.group(9)

            low_ns = self._latency_to_ns(low_val, low_unit)
            mean_ns = self._latency_to_ns(mean_val, mean_unit)
            high_ns = self._latency_to_ns(high_val, high_unit)

            std_dev_ns = (high_ns - low_ns) / 4.0

            bench_key = f"{match.group(1)}/{engine}/{table_size}"
            iterations = iterations_map.get(bench_key)

            results.append({
                'database_engine': engine,
                'test_name': test_name,
                'table_size': table_size,
                'mean_time_ns': mean_ns,
                'std_dev_ns': std_dev_ns,
                'median_time_ns': mean_ns,
                'iterations': iterations
            })

        summary = {
            'total_results': len(results),
            'engines': list(set(r.get('database_engine', 'unknown') for r in results))
        }

        return results, summary

    def parse(self, content: str) -> Tuple[List[Dict], Dict]:
        """Parse Sysbench benchmark output from stdout.

        Supports:
        1. New custom harness format - embedded mode (post-Criterion migration)
        2. Server mode format (vibesql-server vs MySQL)
        3. Legacy Criterion format (for backwards compatibility)
        """
        # Check for new custom harness format first (embedded or server mode)
        if ('--- VibeSQL Results ---' in content or '--- VibeSQL Server Results ---' in content) and 'Workload' in content:
            return self._parse_custom_harness(content)

        # Fall back to legacy Criterion format
        return self._parse_criterion_stdout(content)

    def parse_criterion_directory(self, criterion_dir: Path) -> List[Dict]:
        """Parse Criterion benchmark results from directory.

        Criterion directory structure:
            target/criterion/sysbench_point_select/vibesql/10000/new/estimates.json
                             ^benchmark_group       ^engine  ^table_size
        """
        results = []

        for bench_group in criterion_dir.glob("sysbench_*"):
            if not bench_group.is_dir():
                continue

            test_name = bench_group.name.replace("sysbench_", "")

            # Iterate over engine directories (vibesql, sqlite, duckdb)
            for engine_dir in bench_group.iterdir():
                if not engine_dir.is_dir():
                    continue

                engine = engine_dir.name.lower()

                # Iterate over table_size directories (e.g., 10000)
                for size_dir in engine_dir.iterdir():
                    if not size_dir.is_dir():
                        continue

                    table_size = int(size_dir.name) if size_dir.name.isdigit() else 10000

                    estimates_file = size_dir / "new" / "estimates.json"
                    if not estimates_file.exists():
                        continue

                    try:
                        with open(estimates_file) as f:
                            estimates = json.load(f)

                        mean_ns = estimates.get("mean", {}).get("point_estimate", 0)
                        std_dev_ns = estimates.get("std_dev", {}).get("point_estimate", 0)
                        median_ns = estimates.get("median", {}).get("point_estimate", 0)

                        sample_file = size_dir / "new" / "sample.json"
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

    def insert_results(self, db, cursor, results: List[Dict], summary: Dict,
                       config: Dict) -> int:
        """Insert Sysbench results into the database."""
        git_commit, git_branch = get_git_info()
        table_size = config.get('table_size', 10000)
        notes = config.get('notes')

        execute_insert(cursor, "benchmark_runs", [
            "run_timestamp", "git_commit", "git_branch", "benchmark_suite",
            "scale_factor", "total_queries", "notes", "machine_tag"
        ], [
            datetime.now().isoformat(),
            git_commit,
            git_branch,
            'sysbench',
            str(table_size),
            len(results),
            notes,
            get_machine_tag()
        ])

        run_id = get_last_insert_id(cursor, "benchmark_runs", "run_id")

        for result in results:
            execute_insert(cursor, "sysbench_results", [
                "run_id", "database_engine", "test_name", "table_size",
                "mean_time_ns", "std_dev_ns", "median_time_ns", "iterations"
            ], [
                run_id,
                result.get('database_engine', 'unknown'),
                result.get('test_name', 'unknown'),
                result.get('table_size', table_size),
                result.get('mean_time_ns'),
                result.get('std_dev_ns'),
                result.get('median_time_ns'),
                result.get('iterations')
            ])

        save_connection(db)
        return run_id


# =============================================================================
# HNSW Recall Parser
# =============================================================================

class HnswRecallParser(BenchmarkParser):
    """Parser for HNSW recall@k quality benchmark results.

    Unlike the latency-oriented parsers this stores a *quality* metric
    (recall@k) for three index states (fresh / degraded / compacted) across a
    sweep of ef_search and delete ratio. See #5466.

    Expected output (from crates/vibesql-storage/benches/hnsw_recall_benchmark.rs):

        === HNSW Recall@10 Benchmark ===
        Dataset: 3000 vectors, dim=16, queries=100, M=16, ef_construction=64

        --- VibeSQL Recall Results ---
        EfSearch    DeleteRatio     Live   Deleted      Fresh   Degraded  Compacted
        ---------- ------------ -------- --------- ---------- ---------- ----------
        12                 0.50     1500      1500     0.9980     0.9850     0.9970
        ...
    """

    @property
    def benchmark_suite(self) -> str:
        return 'hnsw'

    @classmethod
    def can_parse(cls, content: str) -> bool:
        return ('HNSW Recall@' in content or
                '--- VibeSQL Recall Results ---' in content)

    def parse(self, content: str) -> Tuple[List[Dict], Dict]:
        results: List[Dict] = []

        # Header: "=== HNSW Recall@10 Benchmark ==="
        k = 10
        k_match = re.search(r'HNSW Recall@(\d+)', content)
        if k_match:
            k = int(k_match.group(1))

        # Config: "Dataset: 3000 vectors, dim=16, queries=100, ..."
        dataset_size = None
        dimensions = None
        cfg_match = re.search(r'Dataset:\s+(\d+)\s+vectors,\s+dim=(\d+)', content)
        if cfg_match:
            dataset_size = int(cfg_match.group(1))
            dimensions = int(cfg_match.group(2))

        # Data rows:
        # "12   0.50   1500   1500   0.9980   0.9850   0.9970"
        # ef_search delete_ratio live deleted fresh degraded compacted
        row_pattern = re.compile(
            r'^\s*(\d+)\s+'        # ef_search
            r'([\d.]+)\s+'        # delete_ratio
            r'(\d+)\s+'           # live
            r'(\d+)\s+'           # deleted
            r'([\d.]+)\s+'        # fresh
            r'([\d.]+)\s+'        # degraded
            r'([\d.]+)\s*$',      # compacted
            re.MULTILINE
        )

        for m in row_pattern.finditer(content):
            ef_search = int(m.group(1))
            delete_ratio = float(m.group(2))
            live = int(m.group(3))
            deleted = int(m.group(4))
            recalls = {
                'fresh': float(m.group(5)),
                'degraded': float(m.group(6)),
                'compacted': float(m.group(7)),
            }
            for state, recall in recalls.items():
                results.append({
                    'database_engine': 'vibesql',
                    'k': k,
                    'dataset_size': dataset_size,
                    'dimensions': dimensions,
                    'ef_search': ef_search,
                    'delete_ratio': delete_ratio,
                    'live_count': live,
                    'deleted_count': deleted,
                    'index_state': state,
                    'recall': recall,
                })

        summary = {
            'k': k,
            'dataset_size': dataset_size,
            'dimensions': dimensions,
            'total_results': len(results),
            'total_queries': len(results),
        }
        return results, summary

    def insert_results(self, db, cursor, results: List[Dict], summary: Dict,
                       config: Dict) -> int:
        """Insert HNSW recall results into the database."""
        git_commit, git_branch = get_git_info()
        notes = config.get('notes')

        execute_insert(cursor, "benchmark_runs", [
            "run_timestamp", "git_commit", "git_branch", "benchmark_suite",
            "scale_factor", "total_queries", "notes", "machine_tag"
        ], [
            datetime.now().isoformat(),
            git_commit,
            git_branch,
            'hnsw',
            str(summary.get('dataset_size') or ''),
            len(results),
            notes,
            get_machine_tag()
        ])

        run_id = get_last_insert_id(cursor, "benchmark_runs", "run_id")

        for result in results:
            execute_insert(cursor, "hnsw_recall_results", [
                "run_id", "database_engine", "k", "dataset_size", "dimensions",
                "ef_search_param", "delete_ratio", "live_count", "deleted_count",
                "index_state", "recall"
            ], [
                run_id,
                result.get('database_engine', 'vibesql'),
                result.get('k'),
                result.get('dataset_size'),
                result.get('dimensions'),
                result.get('ef_search'),
                result.get('delete_ratio'),
                result.get('live_count'),
                result.get('deleted_count'),
                result.get('index_state', 'unknown'),
                result.get('recall'),
            ])

        save_connection(db)
        return run_id


# =============================================================================
# Auto-detection and Parser Registry
# =============================================================================

PARSERS = [TPCHParser, TPCCParser, TPCDSParser, SysbenchParser, HnswRecallParser]

def detect_benchmark_type(content: str) -> Optional[BenchmarkParser]:
    """Auto-detect benchmark type from content and return appropriate parser."""
    for parser_cls in PARSERS:
        if parser_cls.can_parse(content):
            return parser_cls()
    return None


def get_parser(benchmark_type: str) -> Optional[BenchmarkParser]:
    """Get parser by explicit type name."""
    type_map = {
        'tpch': TPCHParser,
        'tpcc': TPCCParser,
        'tpcds': TPCDSParser,
        'sysbench': SysbenchParser,
        'hnsw': HnswRecallParser,
    }
    parser_cls = type_map.get(benchmark_type.lower())
    return parser_cls() if parser_cls else None


# =============================================================================
# CLI Interface
# =============================================================================

def print_summary(benchmark_type: str, results: List[Dict], summary: Dict, run_id: int):
    """Print summary of processed results."""
    git_commit, git_branch = get_git_info()

    print(f"\n{benchmark_type.upper()} results stored in database")
    print(f"   Run ID: {run_id}")
    print(f"   Commit: {git_commit or 'unknown'}")
    print(f"   Branch: {git_branch or 'unknown'}")
    print(f"   Database: {get_db_path()}")

    if benchmark_type == 'tpch':
        print(f"   Passed: {summary.get('passed_queries', 0)}/{summary.get('total_queries', 0)}")
    elif benchmark_type == 'tpcc':
        print(f"   Engines: {summary.get('total_engines', 0)}")
        for r in results:
            tps = r.get('transactions_per_second', 0)
            print(f"   {r.get('database_engine', 'unknown')}: {tps:.2f} TPS")
    elif benchmark_type == 'tpcds':
        print(f"   Queries: {summary.get('total_queries', 0)}")
        print(f"   Passed: {summary.get('passed_queries', 0)}, Failed: {summary.get('failed_queries', 0)}")
    elif benchmark_type == 'sysbench':
        print(f"   Results: {summary.get('total_results', 0)}")


def main():
    parser = argparse.ArgumentParser(
        description="Unified benchmark result processor for VibeSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  ./scripts/process_results.py --input /tmp/results.txt              Auto-detect type
  ./scripts/process_results.py --type=tpch --input results.txt       Explicit type
  ./scripts/process_results.py --type=tpcc --stdin                   Read from stdin
  ./scripts/process_results.py --type=sysbench --criterion-dir dir   From Criterion
        """
    )

    # Input options
    parser.add_argument(
        "--input", "-i",
        type=str,
        help="Path to benchmark output file"
    )
    parser.add_argument(
        "--stdin",
        action="store_true",
        help="Read benchmark output from stdin"
    )
    parser.add_argument(
        "--criterion-dir",
        type=str,
        help="Path to Criterion output directory (for TPC-DS/Sysbench)"
    )

    # Type specification
    parser.add_argument(
        "--type", "-t",
        choices=["tpch", "tpcc", "tpcds", "sysbench", "hnsw"],
        help="Benchmark type (auto-detected if not specified)"
    )

    # Configuration options
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Per-query timeout in seconds (TPC-H, default: 30)"
    )
    parser.add_argument(
        "--scale-factor",
        type=float,
        help="Scale factor (TPC-C/TPC-DS)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        help="Benchmark duration in seconds (TPC-C)"
    )
    parser.add_argument(
        "--table-size",
        type=int,
        default=10000,
        help="Table size (Sysbench, default: 10000)"
    )
    parser.add_argument(
        "--notes",
        type=str,
        help="Optional notes about this benchmark run"
    )
    parser.add_argument(
        "--init-schema",
        action="store_true",
        help="Initialize benchmark schema only (run once)"
    )

    args = parser.parse_args()

    # Get database connection
    db, cursor = get_connection()

    # Initialize schema if requested
    if args.init_schema:
        init_benchmark_schema(cursor)
        save_connection(db)
        print("Benchmark schema initialized")
        return

    # Initialize schema (idempotent)
    init_benchmark_schema(cursor)

    # Read input
    content = None
    results = None

    if args.stdin:
        content = sys.stdin.read()
    elif args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"Error: Input file not found: {input_path}")
            sys.exit(1)
        with open(input_path) as f:
            content = f.read()
    elif args.criterion_dir:
        # Special case: Criterion directory parsing
        criterion_path = Path(args.criterion_dir)
        if not criterion_path.exists():
            print(f"Error: Criterion directory not found: {criterion_path}")
            sys.exit(1)

        # Determine which parser to use
        if args.type == 'sysbench':
            bench_parser = SysbenchParser()
            results = bench_parser.parse_criterion_directory(criterion_path)
        elif args.type == 'tpcds':
            bench_parser = TPCDSParser()
            results = bench_parser.parse_criterion_directory(criterion_path)
        else:
            # Try to auto-detect
            if list(criterion_path.glob("sysbench_*")):
                bench_parser = SysbenchParser()
                results = bench_parser.parse_criterion_directory(criterion_path)
            elif list(criterion_path.glob("tpcds_*")):
                bench_parser = TPCDSParser()
                results = bench_parser.parse_criterion_directory(criterion_path)
            else:
                print("Error: Could not detect benchmark type from Criterion directory")
                sys.exit(1)

        summary = {
            'total_results': len(results),
            'total_queries': len(results)
        }
    else:
        # Default input paths for backward compatibility
        default_inputs = {
            'tpch': '/tmp/tpch_results.txt',
            'tpcc': '/tmp/tpcc_results.txt',
            'tpcds': '/tmp/tpcds_results.txt',
            'sysbench': '/tmp/sysbench_results.txt',
            'hnsw': '/tmp/hnsw_results.txt',
        }

        if args.type and args.type in default_inputs:
            input_path = Path(default_inputs[args.type])
            if input_path.exists():
                with open(input_path) as f:
                    content = f.read()
            else:
                print(f"Error: Default input file not found: {input_path}")
                print("Use --input to specify a file or --stdin to read from stdin")
                sys.exit(1)
        else:
            parser.print_help()
            sys.exit(1)

    # Parse content if we have it
    if content is not None:
        # Get or detect parser
        if args.type:
            bench_parser = get_parser(args.type)
            if not bench_parser:
                print(f"Error: Unknown benchmark type: {args.type}")
                sys.exit(1)
        else:
            bench_parser = detect_benchmark_type(content)
            if not bench_parser:
                print("Error: Could not auto-detect benchmark type")
                print("Use --type to specify: tpch, tpcc, tpcds, or sysbench")
                sys.exit(1)

        print(f"Processing {bench_parser.benchmark_suite.upper()} benchmark results...")
        results, summary = bench_parser.parse(content)

    if not results:
        print("Error: No benchmark results found")
        sys.exit(1)

    print(f"   Found {len(results)} results")

    # Build config
    config = {
        'timeout': args.timeout,
        'scale_factor': args.scale_factor or (1 if bench_parser.benchmark_suite == 'tpcc' else 0.01),
        'duration': args.duration or 60,
        'table_size': args.table_size,
        'notes': args.notes,
    }

    # Insert results
    run_id = bench_parser.insert_results(db, cursor, results, summary, config)

    # Print summary
    print_summary(bench_parser.benchmark_suite, results, summary, run_id)


if __name__ == "__main__":
    main()
