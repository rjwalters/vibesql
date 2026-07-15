#!/usr/bin/env python3
"""
TCL Test Runner for VibeSQL

Executes parsed TCL tests against VibeSQL and records results.
Supports two modes:
1. Static parsing mode (default): Extract tests from TCL files and execute them
2. Native TCL mode (--native-tcl): Run test files directly with tclsh and our shim

Use --native-tcl for test files that contain TCL constructs like for loops
that generate data dynamically.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import our parser
sys.path.insert(0, str(Path(__file__).parent))
from tcl_parser import TclTestParser, ParsedTest, ParsedFile, TestType

# Set to True by save_to_database() when more than 5% of detail-row inserts
# fail. main() inspects this to exit non-zero so CI/callers notice that the
# tcl_test_results detail table is incomplete.
_SAVE_HAD_FATAL_INSERT_FAILURES = False


@dataclass
class TestResult:
    """Result of running a single test."""
    test_name: str
    file_path: str
    test_type: str
    status: str  # 'passed', 'failed', 'skipped', 'error'
    sql: str
    expected_output: Optional[str] = None
    actual_output: Optional[str] = None
    error_message: Optional[str] = None
    execution_time_ms: float = 0.0
    line_number: int = 0

    def to_dict(self) -> dict:
        return {
            "test_name": self.test_name,
            "file_path": self.file_path,
            "test_type": self.test_type,
            "status": self.status,
            "sql": self.sql,
            "expected_output": self.expected_output,
            "actual_output": self.actual_output,
            "error_message": self.error_message,
            "execution_time_ms": self.execution_time_ms,
            "line_number": self.line_number,
        }


@dataclass
class RunSummary:
    """Summary of a test run."""
    started_at: str
    completed_at: str = ""
    git_commit: str = ""
    total_files: int = 0
    total_tests: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    skipped_setup_failed: int = 0  # Tests skipped due to setup failure
    parse_errors: int = 0
    setup_failures: int = 0  # Number of files with setup failures
    # machine_tag: identifies the host that produced this run (None -> NULL,
    # understood as "unknown/local"). Populated from VIBESQL_MACHINE_TAG so a
    # remote run (scripts/remote-run.sh) tags its results automatically.
    machine_tag: Optional[str] = None
    results: list[TestResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "git_commit": self.git_commit,
            "machine_tag": self.machine_tag,
            "total_files": self.total_files,
            "total_tests": self.total_tests,
            "passed": self.passed,
            "failed": self.failed,
            "skipped": self.skipped,
            "skipped_setup_failed": self.skipped_setup_failed,
            "parse_errors": self.parse_errors,
            "setup_failures": self.setup_failures,
            "pass_rate": (self.passed / self.total_tests * 100) if self.total_tests > 0 else 0,
            "results": [r.to_dict() for r in self.results],
        }


class VibeSQL:
    """Interface to VibeSQL for test execution."""

    def __init__(self, vibesql_path: str, timeout: float = 5.0):
        self.vibesql_path = vibesql_path
        self.timeout = timeout
        self._db_file = None

    def __enter__(self):
        # Create a temporary database file
        self._db_file = tempfile.NamedTemporaryFile(suffix='.vbsql', delete=False)
        self._db_file.close()
        return self

    def __exit__(self, *args):
        # Clean up temporary database
        if self._db_file and os.path.exists(self._db_file.name):
            os.unlink(self._db_file.name)

    def execute(self, sql: str) -> tuple[bool, str, str]:
        """
        Execute SQL and return (success, stdout, stderr).
        Uses --format raw for TCL-compatible output (space-separated values).
        """
        try:
            result = subprocess.run(
                [self.vibesql_path, self._db_file.name, "-c", sql, "--format", "raw"],
                capture_output=True,
                text=True,
                timeout=self.timeout,
            )
            return (result.returncode == 0, result.stdout.strip(), result.stderr.strip())
        except subprocess.TimeoutExpired:
            return (False, "", f"Timeout after {self.timeout}s")
        except Exception as e:
            return (False, "", str(e))

    def _should_skip_setup_sql(self, sql: str) -> bool:
        """
        Check if a setup SQL statement should be skipped.

        Skips SQLite-specific statements that we don't support.
        """
        sql_upper = sql.upper().strip()

        # Skip PRAGMA statements (SQLite configuration)
        if sql_upper.startswith('PRAGMA'):
            return True

        # Skip ANALYZE statements (SQLite-specific)
        if sql_upper.startswith('ANALYZE'):
            return True

        # Skip REINDEX statements (SQLite-specific)
        if sql_upper.startswith('REINDEX'):
            return True

        # Skip EXPLAIN statements
        if sql_upper.startswith('EXPLAIN'):
            return True

        # Skip SELECT statements (don't modify state)
        if sql_upper.startswith('SELECT'):
            return True

        # Skip statements that reference sqlite_master
        if 'SQLITE_MASTER' in sql_upper or 'SQLITE_SCHEMA' in sql_upper:
            return True

        # Skip standalone transaction control statements
        # These are typically used with TCL control structures we can't parse
        if sql_upper in ('BEGIN', 'BEGIN TRANSACTION', 'COMMIT', 'ROLLBACK',
                         'BEGIN;', 'COMMIT;', 'ROLLBACK;'):
            return True

        return False

    def execute_setup(self, sql_statements: list[str]) -> tuple[bool, str]:
        """Execute setup SQL statements, skipping SQLite-specific ones."""
        for sql in sql_statements:
            # Skip SQLite-specific statements
            if self._should_skip_setup_sql(sql):
                continue

            success, _, stderr = self.execute(sql)
            if not success:
                return (False, f"Setup failed: {stderr}")
        return (True, "")


def parse_tcl_list(tcl_list: str) -> list[str]:
    """
    Parse a TCL list string into a Python list of strings.

    TCL lists are space-separated values where:
    - {} represents an empty string
    - {text with spaces} represents a string with spaces
    - Unquoted words are strings

    Examples:
    - "abc {} {} xyz" -> ["abc", "", "", "xyz"]
    - "{hello world} foo" -> ["hello world", "foo"]
    """
    result = []
    i = 0
    tcl_list = tcl_list.strip()
    n = len(tcl_list)

    while i < n:
        # Skip whitespace
        while i < n and tcl_list[i] in ' \t\n':
            i += 1

        if i >= n:
            break

        if tcl_list[i] == '{':
            # Braced element - find matching close brace
            depth = 1
            start = i + 1
            i += 1
            while i < n and depth > 0:
                if tcl_list[i] == '{':
                    depth += 1
                elif tcl_list[i] == '}':
                    depth -= 1
                i += 1
            # Extract content between braces
            content = tcl_list[start:i-1] if i > start else ""
            result.append(content)
        else:
            # Unquoted element - read until whitespace
            start = i
            while i < n and tcl_list[i] not in ' \t\n':
                i += 1
            result.append(tcl_list[start:i])

    return result


def normalize_output(output: str) -> str:
    """Normalize output for comparison."""
    # Strip whitespace
    output = output.strip()
    # Normalize whitespace within
    output = re.sub(r'\s+', ' ', output)
    # Remove trailing whitespace on each line
    lines = [l.strip() for l in output.split('\n')]
    return ' '.join(lines)


def compare_outputs(expected: str, actual: str) -> bool:
    """
    Compare expected and actual output, handling TCL format quirks.

    TCL uses {} to represent empty strings in lists. The raw output format
    uses consecutive spaces for empty values (NULLs). This function handles
    the conversion between these formats.
    """
    # First try simple normalized comparison
    expected_norm = normalize_output(expected)
    actual_norm = normalize_output(actual)

    if expected_norm == actual_norm:
        return True

    # Parse TCL list format for expected (handles {} as empty string)
    exp_parts = parse_tcl_list(expected)

    # Check if expected has empty strings (NULL values)
    has_empty_expected = any(part == "" for part in exp_parts)

    if has_empty_expected:
        # Parse actual output preserving empty values
        # The raw format outputs values space-separated, with empty strings for NULLs
        # So "abc   xyz" means: "abc", "", "", "xyz" (3 spaces = 2 empty values between)
        # " abc xyz" (leading space) means: "", "abc", "xyz"
        # "abc xyz " (trailing space) means: "abc", "xyz", ""
        #
        # We need to:
        # 1. Normalize newlines to spaces (multiline output)
        # 2. Strip trailing newline only (not leading/trailing spaces that represent NULLs)
        # 3. Split on single space to preserve empty strings
        actual_lines = actual.rstrip('\n').split('\n')
        actual_flat = ' '.join(actual_lines)
        act_parts = actual_flat.split(' ')
    else:
        # No empty values expected, use simple split
        act_parts = actual_norm.split()

    if len(exp_parts) != len(act_parts):
        return False

    for e, a in zip(exp_parts, act_parts):
        # Try numeric comparison
        try:
            if float(e) != float(a):
                return False
        except ValueError:
            if e != a:
                return False

    return True


class TclTestRunner:
    """Runs TCL tests against VibeSQL."""

    def __init__(self, vibesql_path: str, verbose: bool = False, timeout: float = 5.0):
        self.vibesql_path = vibesql_path
        self.verbose = verbose
        self.timeout = timeout
        self.parser = TclTestParser(verbose=verbose)

    def run_single_test(self, test: ParsedTest, vibesql: VibeSQL, file_path: str) -> Optional[TestResult]:
        """Run a single test and return the result.

        Returns None for successful SETUP tests (they don't count as test results).
        """
        start_time = time.time()

        # Handle skipped tests
        if test.test_type == TestType.SKIPPED:
            return TestResult(
                test_name=test.name,
                file_path=file_path,
                test_type=test.test_type.value,
                status="skipped",
                sql=test.sql,
                error_message=test.skip_reason,
                line_number=test.line_number,
            )

        # Handle SETUP tests - execute SQL but don't count as pass/fail
        # Return None to signal this shouldn't be counted as a test result
        if test.test_type == TestType.SETUP:
            success, _, stderr = vibesql.execute(test.sql)
            if not success:
                # Setup failed - return a result so we can track the failure
                return TestResult(
                    test_name=test.name,
                    file_path=file_path,
                    test_type=test.test_type.value,
                    status="setup_failed",
                    sql=test.sql,
                    error_message=f"Setup SQL failed: {stderr}",
                    line_number=test.line_number,
                )
            # Setup succeeded - return None to indicate no test result
            return None

        # Execute the SQL
        success, stdout, stderr = vibesql.execute(test.sql)
        elapsed_ms = (time.time() - start_time) * 1000

        # Check result based on test type
        if test.test_type == TestType.CATCHSQL:
            # For catchsql tests, we expect an error
            if test.error_code == 0:
                # Expect success
                if not success:
                    return TestResult(
                        test_name=test.name,
                        file_path=file_path,
                        test_type=test.test_type.value,
                        status="failed",
                        sql=test.sql,
                        expected_output=test.expected_error or "success",
                        actual_output=stderr,
                        error_message=f"Expected success but got error: {stderr}",
                        execution_time_ms=elapsed_ms,
                        line_number=test.line_number,
                    )
                else:
                    return TestResult(
                        test_name=test.name,
                        file_path=file_path,
                        test_type=test.test_type.value,
                        status="passed",
                        sql=test.sql,
                        expected_output="success",
                        actual_output=stdout,
                        execution_time_ms=elapsed_ms,
                        line_number=test.line_number,
                    )
            else:
                # Expect error with specific message
                if success:
                    return TestResult(
                        test_name=test.name,
                        file_path=file_path,
                        test_type=test.test_type.value,
                        status="failed",
                        sql=test.sql,
                        expected_output=f"error: {test.expected_error}",
                        actual_output=stdout,
                        error_message="Expected error but query succeeded",
                        execution_time_ms=elapsed_ms,
                        line_number=test.line_number,
                    )
                else:
                    # Check if error message matches
                    # Be lenient - just check if the key part of the error is present
                    expected_error = (test.expected_error or "").lower()
                    actual_error = stderr.lower()

                    # Extract key error keywords
                    if expected_error and any(keyword in actual_error for keyword in expected_error.split()[:3]):
                        return TestResult(
                            test_name=test.name,
                            file_path=file_path,
                            test_type=test.test_type.value,
                            status="passed",
                            sql=test.sql,
                            expected_output=test.expected_error,
                            actual_output=stderr,
                            execution_time_ms=elapsed_ms,
                            line_number=test.line_number,
                        )
                    else:
                        return TestResult(
                            test_name=test.name,
                            file_path=file_path,
                            test_type=test.test_type.value,
                            status="failed",
                            sql=test.sql,
                            expected_output=f"error: {test.expected_error}",
                            actual_output=f"error: {stderr}",
                            error_message=f"Error message mismatch",
                            execution_time_ms=elapsed_ms,
                            line_number=test.line_number,
                        )

        else:
            # For execsql and general tests, compare output
            if not success:
                return TestResult(
                    test_name=test.name,
                    file_path=file_path,
                    test_type=test.test_type.value,
                    status="failed",
                    sql=test.sql,
                    expected_output=test.expected_output,
                    actual_output=stderr,
                    error_message=f"Query failed: {stderr}",
                    execution_time_ms=elapsed_ms,
                    line_number=test.line_number,
                )

            expected = test.expected_output or ""
            if compare_outputs(expected, stdout):
                return TestResult(
                    test_name=test.name,
                    file_path=file_path,
                    test_type=test.test_type.value,
                    status="passed",
                    sql=test.sql,
                    expected_output=expected,
                    actual_output=stdout,
                    execution_time_ms=elapsed_ms,
                    line_number=test.line_number,
                )
            else:
                return TestResult(
                    test_name=test.name,
                    file_path=file_path,
                    test_type=test.test_type.value,
                    status="failed",
                    sql=test.sql,
                    expected_output=expected,
                    actual_output=stdout,
                    error_message="Output mismatch",
                    execution_time_ms=elapsed_ms,
                    line_number=test.line_number,
                )

    def run_file(self, file_path: str) -> tuple[list[TestResult], bool]:
        """
        Run all tests in a single TCL file.

        Tests are executed in line-number order, which includes interleaved
        SETUP tests (standalone execsql/db eval blocks) that set up database state.

        Returns:
            Tuple of (results list, setup_failed boolean)
        """
        results = []
        setup_failed = False
        setup_error_message = None

        # Parse the file
        parsed = self.parser.parse_file(file_path)

        if parsed.parse_errors:
            for error in parsed.parse_errors:
                results.append(TestResult(
                    test_name="parse_error",
                    file_path=file_path,
                    test_type="parse",
                    status="error",
                    sql="",
                    error_message=error,
                ))
            if not parsed.tests:
                return results, False

        # Create a fresh database for this file
        with VibeSQL(self.vibesql_path, timeout=self.timeout) as vibesql:
            # Track missing tables to detect cascading failures
            # When a test fails with "Table 'X' not found", subsequent tests
            # that also fail with the same error are marked as skipped
            missing_tables: set[str] = set()

            # Run each test in order (including interleaved SETUP tests)
            for test in parsed.tests:
                # Check if this test would fail due to a known missing table
                if missing_tables and test.test_type != TestType.SETUP:
                    referenced_tables = self._extract_table_references(test.sql)
                    missing_in_test = referenced_tables & missing_tables
                    if missing_in_test:
                        # Skip this test - it depends on a table that wasn't created
                        table_list = ', '.join(sorted(missing_in_test))
                        results.append(TestResult(
                            test_name=test.name,
                            file_path=file_path,
                            test_type=test.test_type.value,
                            status="skipped",
                            sql=test.sql,
                            error_message=f"Setup failed: missing table(s) {table_list} not created by earlier test",
                            line_number=test.line_number,
                        ))
                        if self.verbose:
                            print(f"  ○ {test.name}: skipped (missing table: {table_list})")
                        continue

                result = self.run_single_test(test, vibesql, file_path)

                # SETUP tests return None on success (we don't count them as test results)
                if result is None:
                    if self.verbose:
                        print(f"  ⚙ {test.name}: setup OK")
                    continue

                # Check if this was a setup failure
                if result.status == "setup_failed":
                    setup_failed = True
                    setup_error_message = result.error_message
                    # Track the missing table if applicable
                    table_name = self._extract_missing_table(result.error_message)
                    if table_name:
                        missing_tables.add(table_name)
                    if self.verbose:
                        print(f"  ⚠ {test.name}: setup FAILED - {result.error_message}")
                    # Don't add setup failures to results - they aren't tests
                    continue

                # Check if this failure reveals a missing table
                if result.status == "failed" and result.error_message:
                    table_name = self._extract_missing_table(result.error_message)
                    if table_name:
                        missing_tables.add(table_name)

                results.append(result)

                if self.verbose:
                    status_icon = "✓" if result.status == "passed" else "✗" if result.status == "failed" else "○"
                    print(f"  {status_icon} {test.name}: {result.status}")

        return results, setup_failed

    def _extract_missing_table(self, error_message: str) -> Optional[str]:
        """Extract table name from 'Table X not found' error messages."""
        # Match patterns like "Table 'T1' not found" or "Table 't1' not found"
        match = re.search(r"Table\s+'([^']+)'\s+not found", error_message, re.IGNORECASE)
        if match:
            return match.group(1).upper()  # Normalize to uppercase
        return None

    def _extract_table_references(self, sql: str) -> set[str]:
        """Extract table names referenced in a SQL statement."""
        tables = set()
        sql_upper = sql.upper()

        # Match FROM clause tables
        from_matches = re.findall(r'\bFROM\s+([A-Z_][A-Z0-9_]*)', sql_upper)
        tables.update(from_matches)

        # Match JOIN clause tables
        join_matches = re.findall(r'\bJOIN\s+([A-Z_][A-Z0-9_]*)', sql_upper)
        tables.update(join_matches)

        # Match UPDATE table
        update_matches = re.findall(r'\bUPDATE\s+([A-Z_][A-Z0-9_]*)', sql_upper)
        tables.update(update_matches)

        # Match DELETE FROM table
        delete_matches = re.findall(r'\bDELETE\s+FROM\s+([A-Z_][A-Z0-9_]*)', sql_upper)
        tables.update(delete_matches)

        # Match INSERT INTO table
        insert_matches = re.findall(r'\bINSERT\s+INTO\s+([A-Z_][A-Z0-9_]*)', sql_upper)
        tables.update(insert_matches)

        return tables

    def run_files(self, file_paths: list[str], parallel: bool = False) -> RunSummary:
        """Run tests from multiple files."""
        summary = RunSummary(
            started_at=datetime.now().isoformat(),
            total_files=len(file_paths),
            machine_tag=os.environ.get("VIBESQL_MACHINE_TAG"),
        )

        # Get git commit
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            summary.git_commit = result.stdout.strip()
        except:
            pass

        def process_file_results(results: list[TestResult], file_setup_failed: bool):
            """Process results from a single file."""
            if file_setup_failed:
                summary.setup_failures += 1

            for result in results:
                summary.results.append(result)
                summary.total_tests += 1
                if result.status == "passed":
                    summary.passed += 1
                elif result.status == "failed":
                    summary.failed += 1
                elif result.status == "skipped":
                    # Track setup-failed skips separately
                    if result.error_message and result.error_message.startswith("Setup failed:"):
                        summary.skipped_setup_failed += 1
                    summary.skipped += 1
                elif result.status == "error":
                    summary.parse_errors += 1

        def runner_error_marker(file_path: str, e: Exception) -> TestResult:
            """Synthetic detail row for a file the runner failed to process.

            Without this the file contributed zero detail rows and silently
            vanished from the run universe (issue #5821).
            """
            return TestResult(
                test_name="RUNNER ERROR",
                file_path=file_path,
                test_type="runner",
                status="error",
                sql="",
                error_message=str(e)[:500],
            )

        if parallel:
            # Parallel execution
            with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                futures = {executor.submit(self.run_file, f): f for f in file_paths}
                for future in as_completed(futures):
                    file_path = futures[future]
                    try:
                        results, file_setup_failed = future.result()
                        process_file_results(results, file_setup_failed)
                    except Exception as e:
                        print(f"Error processing {file_path}: {e}")
                        process_file_results([runner_error_marker(file_path, e)], False)
        else:
            # Sequential execution
            for i, file_path in enumerate(file_paths):
                if self.verbose:
                    print(f"[{i+1}/{len(file_paths)}] {Path(file_path).name}")

                try:
                    results, file_setup_failed = self.run_file(file_path)
                    process_file_results(results, file_setup_failed)
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
                    process_file_results([runner_error_marker(file_path, e)], False)

        summary.completed_at = datetime.now().isoformat()
        return summary


def allocate_run_id(db_path: str, vibesql_path: str) -> int:
    """Reserve the next run_id (MAX(run_id)+1) from tcl_test_runs.

    Used both by the single-shot save path and by the batched/incremental
    path, which allocates one run_id up front so every batch appends under the
    same logical run.
    """
    result = subprocess.run(
        [vibesql_path, db_path, "-c", "SELECT COALESCE(MAX(run_id), 0) + 1 FROM tcl_test_runs", "--format", "raw"],
        capture_output=True,
        text=True,
    )
    try:
        # Parse output - look for the number in the result
        lines = [l.strip() for l in result.stdout.strip().split('\n') if l.strip()]
        return int(lines[-1])
    except (ValueError, IndexError):
        return 1


def upsert_run_summary(summary: RunSummary, run_id: int, db_path: str, vibesql_path: str) -> bool:
    """Write (or overwrite) the tcl_test_runs summary row for `run_id`.

    Uses INSERT OR REPLACE so it is idempotent: the batched path calls this
    once per completed batch with rolled-up totals, and a plain re-INSERT would
    otherwise hit the `run_id INTEGER PRIMARY KEY` constraint on the 2nd batch.
    Returns True on success.
    """
    # Machine tag SQL literal: NULL when unset, single-quote-escaped otherwise.
    # Sourced (in priority order) from an explicit summary.machine_tag, else the
    # VIBESQL_MACHINE_TAG environment variable, else NULL ("unknown/local").
    machine_tag_value = summary.machine_tag or os.environ.get("VIBESQL_MACHINE_TAG")
    if machine_tag_value:
        machine_tag_literal = "'" + machine_tag_value.replace("'", "''") + "'"
    else:
        machine_tag_literal = "NULL"

    # INSERT OR REPLACE keyed on run_id (PRIMARY KEY) makes the summary write
    # idempotent so it can be refreshed after every batch with the latest
    # rolled-up totals.
    run_sql = f"""
        INSERT OR REPLACE INTO tcl_test_runs (
            run_id, started_at, completed_at, git_commit,
            total_files, total_tests, passed, failed, skipped, skipped_setup_failed, parse_errors, setup_failures,
            machine_tag
        ) VALUES (
            {run_id}, '{summary.started_at}', '{summary.completed_at}', '{summary.git_commit}',
            {summary.total_files}, {summary.total_tests},
            {summary.passed}, {summary.failed}, {summary.skipped}, {summary.skipped_setup_failed}, {summary.parse_errors}, {summary.setup_failures},
            {machine_tag_literal}
        );
    """

    try:
        subprocess.run(
            [vibesql_path, db_path, "-c", run_sql],
            capture_output=True,
            check=True,
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"Warning: Failed to save run summary: {e.stderr}")
        return False


def insert_detail_rows(
    results: list[TestResult], run_id: int, db_path: str, vibesql_path: str
) -> None:
    """Append per-test detail rows under `run_id` to tcl_test_results.

    Detail rows carry the AUTOINCREMENT `id` PK, so many rows may share one
    `run_id` via plain INSERTs — this is what makes incremental per-batch
    appends work without a PK conflict.

    Persist strategy (issue #6149): all rows for this call are inserted by a
    SINGLE `vibesql` process inside ONE transaction, so the CLI's expensive
    full-DB checkpoint happens exactly ONCE (at COMMIT / process exit) rather
    than once per 200-row batch. The previous implementation spawned a fresh
    `vibesql <db> -c ...` process per batch, which re-checkpointed the entire
    growing results DB every batch — O(rows^2) checkpoint work that ran for
    hours and torned every rebaseline. A malformed value falls back to a
    granular per-batch/per-row retry that still counts failures exactly.
    """
    # Helper to escape SQL string literals. Returns the full SQL literal
    # including surrounding quotes (or NULL for empty values).
    def sql_literal(s: Optional[str], max_len: int = 1000) -> str:
        if not s:
            return "NULL"
        # Remove null bytes and other control chars that break subprocess/SQL
        s = s.replace('\x00', '')
        # Collapse newlines/tabs so the INSERT stays on a manageable single
        # logical value (the detail table is for analysis, not byte-fidelity).
        s = s.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        # Truncate before escaping so the doubled quotes are not split.
        s = s[:max_len]
        # SQL-escape single quotes by doubling them.
        s = s.replace("'", "''")
        return f"'{s}'"

    def row_values(r: TestResult) -> str:
        return (
            f"({run_id}, "
            f"{sql_literal(r.file_path, 500)}, "
            f"{sql_literal(r.test_name, 200)}, "
            f"{sql_literal(r.test_type, 64)}, "
            f"{sql_literal(r.status, 32)}, "
            f"{sql_literal(r.sql, 1000)}, "
            f"{sql_literal(r.expected_output, 1000)}, "
            f"{sql_literal(r.actual_output, 1000)}, "
            f"{sql_literal(r.error_message, 500)}, "
            f"{float(r.execution_time_ms or 0.0)}, "
            f"{int(r.line_number or 0)})"
        )

    # Column list intentionally omits `id`: the schema declares it
    # INTEGER PRIMARY KEY AUTOINCREMENT, so the engine assigns unique ids
    # atomically. This removes the non-atomic SELECT MAX(id)+1 pattern that
    # caused id collisions (and silently-dropped rows) under concurrent runs.
    columns = (
        "run_id, file_path, test_name, test_type, status, "
        "sql_text, expected_output, actual_output, error_message, "
        "execution_time_ms, line_number"
    )

    # Rows-per-INSERT: bound each statement's size to keep the multi-row VALUES
    # list manageable while still amortizing per-statement overhead.
    BATCH_SIZE = 200

    def batch_insert_sql(rows: list[TestResult]) -> str:
        """Build one multi-row INSERT statement for `rows`."""
        values = ",\n".join(row_values(r) for r in rows)
        return f"INSERT INTO tcl_test_results ({columns}) VALUES\n{values};"

    def run_command(sql: str) -> bool:
        """Run a single statement in its own process. Returns True on success.

        Used only by the rare per-row failure-isolation fallback below.
        """
        try:
            subprocess.run(
                [vibesql_path, db_path, "-c", sql],
                capture_output=True,
                check=True,
            )
            return True
        except subprocess.CalledProcessError:
            return False

    def run_txn_script(insert_statements: list[str]) -> bool:
        """Execute many INSERTs in ONE vibesql process wrapped in a single
        transaction. Returns True on a clean exit (every insert succeeded).

        This is the O(n^2) -> O(n) fix (issue #6149). Spawning a fresh
        `vibesql <db> -c ...` process per 200-row batch made the CLI reload and
        re-checkpoint the ENTIRE growing results DB on every batch's clean exit,
        so a full run's persist did checkpoint work proportional to rows^2 and
        ran for hours at 100% CPU (torning every rebaseline). The CLI's script
        executor suppresses its per-modification-statement auto-save while a
        transaction is open and checkpoints exactly ONCE at COMMIT, so feeding
        all batches to a single process inside one BEGIN/COMMIT collapses the
        whole persist to a single full-DB checkpoint at process exit.
        """
        if not insert_statements:
            return True
        script = "BEGIN;\n" + "\n".join(insert_statements) + "\nCOMMIT;\n"
        try:
            proc = subprocess.run(
                [vibesql_path, db_path, "--stdin"],
                input=script,
                capture_output=True,
                text=True,
            )
            return proc.returncode == 0
        except OSError:
            return False

    def delete_this_calls_rows() -> None:
        """Remove any rows THIS call may have already landed, scoped to this
        call's files under `run_id`.

        A batched transaction that hits a malformed value is NOT atomic in the
        VibeSQL CLI: the script executor continues past the failed statement
        while the transaction is open and still COMMITs the survivors. Before
        the granular retry re-inserts everything we must therefore clear the
        partial writes, or good rows would be double-inserted. The delete is
        scoped to `file_path IN (this call's files)` so it never touches earlier
        file-batches persisted under the same shared `run_id` (the #6136
        incremental durability model persists each file exactly once).
        """
        distinct_files = sorted({r.file_path for r in results if r.file_path})
        if not distinct_files:
            return
        in_list = ", ".join(sql_literal(f, 500) for f in distinct_files)
        run_command(
            f"DELETE FROM tcl_test_results "
            f"WHERE run_id = {run_id} AND file_path IN ({in_list});"
        )

    total_rows = len(results)
    inserted = 0
    failed_inserts = 0
    failure_samples: list[str] = []

    # Partition into bounded multi-row INSERT statements once; both the fast
    # single-transaction path and the granular fallback reuse the same batches.
    batches = [
        results[start:start + BATCH_SIZE]
        for start in range(0, total_rows, BATCH_SIZE)
    ]

    # Fast path: ALL batches in ONE process inside ONE transaction -> ONE
    # checkpoint at COMMIT. This is the common case (rows are well-formed).
    if batches and run_txn_script([batch_insert_sql(b) for b in batches]):
        inserted = total_rows
    elif batches:
        # Slow path (rare): a malformed value aborted the batched transaction,
        # possibly leaving partial writes. Clean this call's rows, then retry
        # per batch and finally per row so we lose at most the genuinely-bad
        # rows and can count/report exactly how many failed.
        delete_this_calls_rows()
        for batch in batches:
            if run_txn_script([batch_insert_sql(batch)]):
                inserted += len(batch)
                continue
            # Batch failed: retry each row individually.
            for r in batch:
                if run_command(batch_insert_sql([r])):
                    inserted += 1
                else:
                    failed_inserts += 1
                    if len(failure_samples) < 5:
                        failure_samples.append(f"{r.file_path}::{r.test_name}")

    if total_rows > 0:
        fail_pct = 100.0 * failed_inserts / total_rows
        print(
            f"Detail rows: {inserted}/{total_rows} inserted into tcl_test_results "
            f"(run_id={run_id}); {failed_inserts} failed ({fail_pct:.1f}%)."
        )
        if failed_inserts > 0:
            for sample in failure_samples:
                print(f"  failed insert: {sample}", file=sys.stderr)
            if fail_pct > 5.0:
                print(
                    f"ERROR: {fail_pct:.1f}% of detail-row inserts failed "
                    f"(threshold 5%). The tcl_test_results detail table is "
                    f"incomplete for run_id={run_id}.",
                    file=sys.stderr,
                )
                # Surface the failure to callers/CI without aborting before the
                # run summary has been recorded.
                global _SAVE_HAD_FATAL_INSERT_FAILURES
                _SAVE_HAD_FATAL_INSERT_FAILURES = True
            else:
                print(
                    f"WARNING: {failed_inserts} detail-row insert(s) failed for "
                    f"run_id={run_id} (within 5% tolerance).",
                    file=sys.stderr,
                )


def save_to_database(summary: RunSummary, db_path: str, vibesql_path: str):
    """Save a complete run (summary + all detail rows) under a fresh run_id.

    Single-shot convenience wrapper used by the static-parsing path. The
    native-TCL path instead allocates a run_id up front and calls the batched
    primitives (allocate_run_id / upsert_run_summary / insert_detail_rows)
    directly so results are durable per batch.
    """
    run_id = allocate_run_id(db_path, vibesql_path)
    if not upsert_run_summary(summary, run_id, db_path, vibesql_path):
        return
    insert_detail_rows(summary.results, run_id, db_path, vibesql_path)


# Sentinel prefix the TCL shim uses for machine-readable per-test detail lines.
# Format emitted by tester_vibesql.tcl: "##TCLTEST## <status>\t<name>"
TCL_DETAIL_PREFIX = "##TCLTEST## "

# Detail-row statuses used for synthetic "the file did not run to completion"
# marker rows. These keep compromised files visible in the run universe so
# per-run pass rates remain comparable (see issue #5821 / CLAUDE.md):
#   timeout    - the per-file wall-clock timeout expired (subprocess killed)
#   incomplete - the tclsh worker died early (killed by a signal, or the shim
#                crashed before printing its "Tests run:" summary trailer)
#   error      - the runner itself failed to launch/process the file
MARKER_STATUSES = ("timeout", "incomplete", "error")


def _parse_shim_output(output: str, test_file: str) -> tuple[int, int, int, list[str], list["TestResult"], str, bool]:
    """
    Parse tclsh shim output (possibly partial) into per-test results.

    Returns:
        (passed, failed, skipped, failed_test_names, per_test_results,
         human_output, saw_summary)

    `saw_summary` is True when the shim's "Tests run:" trailer is present,
    i.e. the file ran to completion (finish_test was reached). Partial output
    from a killed/crashed worker lacks the trailer.
    """
    passed = 0
    failed = 0
    skipped = 0
    failed_tests: list[str] = []
    per_test_results: list[TestResult] = []
    human_lines: list[str] = []
    saw_summary = False

    for line in output.split('\n'):
        # Machine-readable per-test detail line
        if line.startswith(TCL_DETAIL_PREFIX):
            payload = line[len(TCL_DETAIL_PREFIX):]
            if '\t' in payload:
                status, test_name = payload.split('\t', 1)
            else:
                status, test_name = payload, ""
            status = status.strip()
            test_name = test_name.strip()
            per_test_results.append(TestResult(
                test_name=test_name,
                file_path=test_file,
                test_type="native-tcl",
                status=status,
                sql="",
            ))
            continue

        # Non-detail line: keep for human output and summary parsing
        human_lines.append(line)

        if 'Tests run:' in line:
            saw_summary = True
        elif 'Tests passed:' in line:
            match = re.search(r'Tests passed:\s*(\d+)', line)
            if match:
                passed = int(match.group(1))
        elif 'Tests failed:' in line:
            match = re.search(r'Tests failed:\s*(\d+)', line)
            if match:
                failed = int(match.group(1))
        elif 'Tests skipped:' in line:
            match = re.search(r'Tests skipped:\s*(\d+)', line)
            if match:
                skipped = int(match.group(1))

    # Extract failed test names if present
    human_output = '\n'.join(human_lines)
    if 'Failed tests:' in human_output:
        in_failed_section = False
        for line in human_lines:
            if 'Failed tests:' in line:
                in_failed_section = True
                continue
            if in_failed_section and line.strip().startswith('-'):
                failed_tests.append(line.strip().lstrip('- '))

    return passed, failed, skipped, failed_tests, per_test_results, human_output, saw_summary


def _counts_from_details(per_test_results: list["TestResult"]) -> tuple[int, int, int]:
    """Derive (passed, failed, skipped) counts from per-test detail rows.

    Used when the shim's summary trailer is absent (worker killed/crashed),
    so the trailer counts are missing or untrustworthy.
    """
    passed = sum(1 for r in per_test_results if r.status == "passed")
    failed = sum(1 for r in per_test_results if r.status == "failed")
    skipped = sum(1 for r in per_test_results if r.status == "skipped")
    return passed, failed, skipped


def run_native_tcl(test_file: str, shim_path: str, verbose: bool = False, timeout: float = 1200.0) -> tuple[int, int, int, list[str], list[TestResult]]:
    """
    Run a TCL test file using the native tclsh interpreter with our VibeSQL shim.

    This mode is for test files that use TCL constructs (like for loops) that
    can't be statically parsed.

    The shim is invoked with --emit-detail so it prints one machine-readable
    "##TCLTEST## <status>\t<name>" line per test. We parse those into per-test
    TestResult rows so native-TCL runs populate the tcl_test_results detail
    table (not just the tcl_test_runs summary).

    A file that does not run to completion never silently vanishes from the
    universe (issue #5821). Three escape paths are covered:
      1. Wall-clock timeout: partial output from e.stdout/e.stderr is salvaged
         and a synthetic `timeout` marker row is appended.
      2. Worker killed by a signal or shim crash (subprocess.run returns, but
         returncode < 0 or the "Tests run:" trailer is missing): the partial
         results are kept and an `incomplete` marker row is appended.
      3. Runner-level exception: an `error` marker row is emitted.
    Marker rows count toward the failed total so compromised runs exit
    non-zero and their totals are visibly worse, never silently smaller.

    Returns: (passed, failed, skipped, failed_test_names, per_test_results)
    """
    # Isolate each file's worker in its own throwaway working directory. The
    # shim's sqlite3 command only pid-isolates the filenames ":memory:" and
    # "test.db"; every other DB filename the suite uses (test2.db, corrupt.db,
    # bak.db, ...) resolves via `file normalize` against the worker's cwd. If
    # all files shared the caller's cwd, one file's leftover DB/WAL/lock/
    # checkpoint siblings could contaminate a later file's fresh open and cause
    # spurious worker deaths (issue #6132). A fresh per-file tmpdir gives those
    # non-isolated filenames a private namespace.
    #
    # test_file arrives repo-root-RELATIVE (TCL_TEST_DIR is a relative path), so
    # it must be absolutized before we hand it to a worker running in a
    # different cwd — otherwise `source $testdir/...` and the file open in the
    # shim (which derives testdir from this argument) would fail to resolve. The
    # VibeSQL binary path is already cwd-independent (derived from the shim's own
    # [info script]), so it is unaffected.
    abs_test_file = os.path.abspath(test_file)
    cmd = ["tclsh", shim_path, abs_test_file, "--emit-detail"]
    if verbose:
        cmd.append("--verbose")

    workdir = tempfile.mkdtemp(prefix="vibesql_tcl_")
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=workdir,
        )
        output = result.stdout + result.stderr

        passed, failed, skipped, failed_tests, per_test_results, human_output, saw_summary = \
            _parse_shim_output(output, test_file)

        # Print human-readable output only (detail markers are stripped so they
        # don't clutter the console).
        if human_output.strip():
            print(human_output)

        # Detect an incomplete run: the worker was killed by a signal
        # (returncode < 0, e.g. OOM-kill / SIGKILL under load) or the shim
        # crashed before printing its summary trailer. Either way the partial
        # ##TCLTEST## rows must NOT be treated as the file's complete result
        # set: keep them, derive counts from them (the trailer is missing),
        # and append an explicit `incomplete` marker row.
        incomplete = (result.returncode is not None and result.returncode < 0) or not saw_summary
        if incomplete:
            if result.returncode is not None and result.returncode < 0:
                reason = f"worker killed by signal {-result.returncode}"
            else:
                reason = f"worker exited (code {result.returncode}) without summary trailer"
            passed, failed, skipped = _counts_from_details(per_test_results)
            print(
                f"WARNING: incomplete run for {test_file}: {reason}; "
                f"kept {len(per_test_results)} partial result(s), writing 'incomplete' marker row"
            )
            per_test_results.append(TestResult(
                test_name=f"INCOMPLETE ({reason})",
                file_path=test_file,
                test_type="native-tcl",
                status="incomplete",
                sql="",
                error_message=f"File did not run to completion: {reason}. "
                              f"{len(per_test_results)} partial result(s) salvaged.",
            ))
            failed_tests.append(f"INCOMPLETE: {reason}")
            return passed, failed + 1, skipped, failed_tests, per_test_results

        return passed, failed, skipped, failed_tests, per_test_results

    except subprocess.TimeoutExpired as e:
        # Salvage whatever completed before the kill: subprocess.run captures
        # the partial stdout/stderr on TimeoutExpired. Note: TimeoutExpired
        # carries raw bytes even when text=True. The trailer is never present
        # here, so derive counts from the salvaged detail rows.
        def _to_text(data) -> str:
            if data is None:
                return ""
            if isinstance(data, bytes):
                return data.decode("utf-8", errors="replace")
            return data

        partial_output = _to_text(e.stdout) + _to_text(e.stderr)
        _, _, _, failed_tests, per_test_results, human_output, _ = \
            _parse_shim_output(partial_output, test_file)
        passed, failed, skipped = _counts_from_details(per_test_results)

        if human_output.strip():
            print(human_output)
        print(
            f"Timeout running {test_file} (exceeded {timeout:.0f}s); "
            f"salvaged {len(per_test_results)} partial result(s), writing 'timeout' marker row"
        )

        # Surface the timeout as a visible non-pass result instead of silently
        # dropping the file from the totals. The synthetic `timeout` detail row
        # keeps the file in tcl_test_results and counts toward the summary's
        # failed total, keeping the summary and detail tables reconciled (see
        # CLAUDE.md). Without this a timed-out file contributed zero rows and
        # vanished from the measured suite.
        per_test_results.append(TestResult(
            test_name=f"TIMEOUT (exceeded {timeout:.0f}s)",
            file_path=test_file,
            test_type="native-tcl",
            status="timeout",
            sql="",
            error_message=f"Test file timed out after {timeout:.0f}s. "
                          f"{len(per_test_results)} partial result(s) salvaged.",
        ))
        failed_tests.append("TIMEOUT")
        return passed, failed + 1, skipped, failed_tests, per_test_results

    except Exception as e:
        # Runner-level failure (tclsh missing, OS error, ...). Emit an explicit
        # `error` marker row so the file stays in the universe instead of
        # contributing zero rows, and count it as a failure so the run exits
        # non-zero.
        print(f"Error running {test_file}: {e}")
        error_result = TestResult(
            test_name="RUNNER ERROR",
            file_path=test_file,
            test_type="native-tcl",
            status="error",
            sql="",
            error_message=str(e)[:500],
        )
        return 0, 1, 0, [f"RUNNER ERROR: {e}"], [error_result]

    finally:
        # Always remove the per-file working directory, on every exit path
        # (success, timeout, incomplete, exception). ignore_errors keeps a
        # stubborn leftover file from masking the real result.
        shutil.rmtree(workdir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description="Run TCL tests against VibeSQL")
    parser.add_argument("--file", "-f", help="Single file to test")
    parser.add_argument("--files", nargs="+", help="Multiple files to test")
    parser.add_argument("--vibesql", required=True, help="Path to VibeSQL binary")
    parser.add_argument("--results-db", help="Path to results database")
    parser.add_argument("--output", "-o", help="Output JSON file")
    parser.add_argument("--parallel", action="store_true", help="Run tests in parallel")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--timeout", type=float, default=None, help="Per-file timeout in seconds (default: 5 for static mode, 1200 for native TCL)")
    parser.add_argument("--native-tcl", action="store_true",
                        help="Run tests using native tclsh instead of parsing (for files with TCL loops)")
    parser.add_argument("--tcl-shim", default=None,
                        help="Path to TCL shim (default: scripts/tester_vibesql.tcl)")
    parser.add_argument("--batch-size", type=int, default=50,
                        help="Native-TCL mode: persist results to the DB every "
                             "N files under one shared run_id, making a killed "
                             "run durable at batch granularity (default: 50; "
                             "<=0 disables batching and saves once at the end)")
    parser.add_argument("--jobs", "-j", type=int, default=1,
                        help="Native-TCL mode: run up to N test files "
                             "concurrently (one tclsh worker per job). Each file "
                             "already runs in its own isolated temp cwd (#6132), "
                             "so files are independent units of work. Defaults to "
                             "1 (sequential — no behavior change). Use "
                             "--jobs $(nproc) to parallelize across cores. Values "
                             "<1 are clamped to 1.")

    args = parser.parse_args()

    if not os.path.exists(args.vibesql):
        print(f"Error: VibeSQL binary not found: {args.vibesql}", file=sys.stderr)
        sys.exit(1)

    # Set appropriate timeout defaults based on mode
    if args.timeout is None:
        if args.native_tcl:
            # 20 minutes for native TCL. The per-statement-process shim makes
            # large auto-generated files (e.g. the Bloom-filter join suite
            # joinB/C/D/E.test, ~1,974 tests) slow; the prior 300s default timed
            # them out and silently dropped ~80% of the suite from the totals.
            args.timeout = 1200.0
        else:
            args.timeout = 5.0   # 5 seconds for static parsing mode

    # Determine files to run
    if args.file:
        file_paths = [args.file]
    elif args.files:
        file_paths = args.files
    else:
        print("Error: No files specified", file=sys.stderr)
        sys.exit(1)

    # Native TCL mode - run with tclsh and our shim
    if args.native_tcl:
        # Find the shim
        script_dir = Path(__file__).parent
        shim_path = args.tcl_shim or str(script_dir / "tester_vibesql.tcl")

        if not os.path.exists(shim_path):
            print(f"Error: TCL shim not found: {shim_path}", file=sys.stderr)
            sys.exit(1)

        total_passed = 0
        total_failed = 0
        total_skipped = 0
        all_failed_tests = []
        all_results: list[TestResult] = []
        # Run-universe accounting (issue #5821): every attempted file must be
        # visible in the summary, either via real results or a marker row.
        files_attempted = len(file_paths)
        files_with_results = 0
        files_timeout = 0
        files_incomplete = 0
        files_error = 0
        # Files that ran to completion but produced only skipped rows: a
        # capability self-skip (`ifcapable !cap { finish_test; return }`) or a
        # whole-file vibesql_skip_files skip. These are clean, intentional
        # skips — NOT compromised runs — so they are reported separately and do
        # not trigger the "did not run to completion" warning (#5887).
        files_self_skipped = 0
        files_processed = 0

        # Durable, resumable batching (issue #6136): instead of buffering every
        # file's results in memory and saving once at the very end (a multi-hour
        # process whose results were all lost if it was killed early), we persist
        # incrementally. A single run_id is allocated up front so every batch
        # appends detail rows and refreshes the summary row under ONE logical
        # run; the canonical `WHERE run_id = MAX(run_id)` status query therefore
        # covers the whole suite, and a kill mid-run leaves already-completed
        # batches durably queryable.
        batch_size = args.batch_size
        run_id: Optional[int] = None
        results_pending_persist: list[TestResult] = []
        git_commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True
        ).stdout.strip()[:8]
        run_started_at = datetime.now().isoformat()

        if args.results_db:
            run_id = allocate_run_id(args.results_db, args.vibesql)

        def build_running_summary() -> RunSummary:
            """Snapshot of rolled-up totals across all files processed so far."""
            return RunSummary(
                started_at=run_started_at,
                completed_at=datetime.now().isoformat(),
                git_commit=git_commit,
                total_files=files_processed,
                total_tests=total_passed + total_failed + total_skipped,
                passed=total_passed,
                failed=total_failed,
                skipped=total_skipped,
                skipped_setup_failed=0,
                parse_errors=0,
                setup_failures=0,
                machine_tag=os.environ.get("VIBESQL_MACHINE_TAG"),
            )

        def persist_batch() -> None:
            """Flush pending detail rows + refresh the summary row for run_id.

            Idempotent on the summary (INSERT OR REPLACE) and append-only on the
            detail rows, so after this returns the DB durably reflects every file
            processed so far under the shared run_id.
            """
            if run_id is None:
                return
            # Refresh the rolled-up summary FIRST so the FK target row exists
            # before its detail rows, then append the new detail rows.
            upsert_run_summary(
                build_running_summary(), run_id, args.results_db, args.vibesql
            )
            if results_pending_persist:
                insert_detail_rows(
                    results_pending_persist, run_id, args.results_db, args.vibesql
                )
                results_pending_persist.clear()

        # Number of files to run concurrently. Each file's tclsh worker runs in
        # its own isolated temp cwd (#6132), so files are independent units of
        # work and safe to run in parallel. jobs<=1 preserves the original
        # strictly-sequential control flow (default, no behavior change).
        jobs = max(1, args.jobs)

        def process_completed_file(file_path: str, result_tuple) -> None:
            """Fold one file's completed results into the shared run accounting.

            Runs on ONE thread (the driver): in sequential mode it is called
            inline, and in parallel mode it is called from the single
            `as_completed` drain loop — never concurrently. All shared-state
            mutation (counters, pending-persist buffer, checkpointing) therefore
            stays single-threaded and lock-free, exactly as in the original
            sequential loop. Result rows carry `file_path`, so folding files in
            completion order (which differs from input order under --jobs>1)
            yields the same totals and the same set of detail rows.
            """
            nonlocal total_passed, total_failed, total_skipped
            nonlocal files_with_results, files_timeout, files_incomplete
            nonlocal files_error, files_self_skipped, files_processed

            passed, failed, skipped, failed_tests, results = result_tuple
            total_passed += passed
            total_failed += failed
            total_skipped += skipped
            all_failed_tests.extend(failed_tests)
            all_results.extend(results)
            results_pending_persist.extend(results)
            files_processed += 1

            statuses = {r.status for r in results}
            if results:
                files_with_results += 1
            if "timeout" in statuses:
                files_timeout += 1
            if "incomplete" in statuses:
                files_incomplete += 1
            if "error" in statuses:
                files_error += 1
            # A file whose only rows are 'skipped' ran cleanly to completion
            # and intentionally skipped everything (capability self-skip or
            # whole-file skip); count it separately from compromised files.
            if results and statuses == {"skipped"}:
                files_self_skipped += 1

            # Checkpoint after every `batch_size` COMPLETED files (completion
            # order, not dispatch order — under --jobs>1 files finish out of
            # order but each checkpoint still reflects exactly files_processed
            # completed files). Batching disabled (batch_size <= 0) falls
            # through to the single end-of-run save.
            if run_id is not None and batch_size > 0 and files_processed % batch_size == 0:
                persist_batch()
                print(
                    f"[checkpoint] Persisted {files_processed}/{files_attempted} "
                    f"files under run_id={run_id} (durable)."
                )
                # Test-only crash seam (unset in normal operation): abort hard
                # right after a checkpoint to deterministically simulate a
                # SIGKILL mid-run. The final end-of-run save is never reached,
                # so a passing durability test proves already-persisted batches
                # survive a kill. os._exit skips atexit/cleanup like a real kill.
                _abort_after = os.environ.get("VIBESQL_TCL_TEST_ABORT_AFTER_FILES")
                if _abort_after and files_processed >= int(_abort_after):
                    print(
                        f"[test-abort] Simulating kill after {files_processed} "
                        f"files (VIBESQL_TCL_TEST_ABORT_AFTER_FILES).",
                        file=sys.stderr,
                    )
                    os._exit(137)

        if jobs <= 1:
            # Sequential execution (default): identical control flow to the
            # pre-parallel implementation.
            for file_path in file_paths:
                if args.verbose:
                    print(f"\nRunning: {file_path}")

                result_tuple = run_native_tcl(
                    file_path, shim_path, verbose=args.verbose, timeout=args.timeout
                )
                process_completed_file(file_path, result_tuple)
        else:
            # Parallel execution: dispatch up to `jobs` tclsh workers at once.
            # run_native_tcl is 100% subprocess-bound (subprocess.run releases
            # the GIL during the blocking wait), so threads give real wall-clock
            # concurrency. The as_completed drain runs on this (driver) thread,
            # so process_completed_file — and thus every checkpoint under the
            # single shared run_id — stays single-threaded and order-independent.
            if args.verbose:
                print(f"\nRunning {files_attempted} file(s) with up to {jobs} concurrent worker(s)")
            with ThreadPoolExecutor(max_workers=jobs) as executor:
                futures = {
                    executor.submit(
                        run_native_tcl,
                        file_path,
                        shim_path,
                        verbose=args.verbose,
                        timeout=args.timeout,
                    ): file_path
                    for file_path in file_paths
                }
                for future in as_completed(futures):
                    file_path = futures[future]
                    try:
                        result_tuple = future.result()
                    except Exception as e:
                        # Runner-level failure that escaped run_native_tcl's own
                        # guard: keep the file in the universe with an error
                        # marker row so it never silently vanishes (#5821).
                        print(f"Error running {file_path}: {e}")
                        error_result = TestResult(
                            test_name="RUNNER ERROR",
                            file_path=file_path,
                            test_type="native-tcl",
                            status="error",
                            sql="",
                            error_message=str(e)[:500],
                        )
                        result_tuple = (0, 1, 0, [f"RUNNER ERROR: {e}"], [error_result])
                    process_completed_file(file_path, result_tuple)

        # Print summary
        total_tests = total_passed + total_failed + total_skipped
        files_compromised = files_timeout + files_incomplete + files_error
        print()
        print("=" * 50)
        print(f"Total tests: {total_tests}")
        print(f"Passed:      {total_passed} ({total_passed/total_tests*100:.1f}%)" if total_tests > 0 else "Passed: 0")
        print(f"Failed:      {total_failed}")
        print(f"Skipped:     {total_skipped}")
        print("-" * 50)
        print(f"Files attempted:    {files_attempted}")
        print(f"Files with results: {files_with_results}")
        if files_self_skipped > 0:
            print(f"Files self-skipped: {files_self_skipped} (capability/whole-file skip; marked status='skipped')")
        if files_timeout > 0:
            print(f"Files timed out:    {files_timeout} (marked status='timeout')")
        if files_incomplete > 0:
            print(f"Files incomplete:   {files_incomplete} (worker died; marked status='incomplete')")
        if files_error > 0:
            print(f"Files runner-error: {files_error} (marked status='error')")
        print("=" * 50)

        # Only warn about a compromised run when real marker rows were written
        # (#5887). A clean capability self-skip leaves files_with_results <
        # files_attempted historically; that alone must not read like a
        # compromised run. After the shim's zero-test marker fix every completed
        # file emits at least one row, so a genuine shortfall only happens with
        # a marker (which sets files_compromised > 0).
        if files_compromised > 0:
            print(
                f"\nWARNING: {files_compromised} of {files_attempted} attempted file(s) "
                f"did not run to completion (timeout/kill/error markers written). "
                f"Totals from this run are NOT comparable to a clean-run baseline. "
                f"Re-run the affected files on a quiet machine.",
                file=sys.stderr,
            )
        elif files_with_results < files_attempted:
            # No markers, yet some file produced zero rows — after the marker
            # guarantee this should not happen; surface it distinctly rather
            # than mislabeling it a compromised run.
            print(
                f"\nWARNING: {files_attempted - files_with_results} of {files_attempted} "
                f"attempted file(s) produced no detail rows and no marker "
                f"(unexpected — the shim should emit a row for every completed file).",
                file=sys.stderr,
            )

        if all_failed_tests:
            print("\nFailed tests:")
            for t in all_failed_tests[:20]:  # Limit to first 20
                print(f"  - {t}")
            if len(all_failed_tests) > 20:
                print(f"  ... and {len(all_failed_tests) - 20} more")

        # Final durable save under the pre-allocated run_id. Any files left over
        # since the last batch checkpoint (the trailing partial batch, or every
        # file when batching is disabled) are flushed here, and the summary row
        # is refreshed with the final rolled-up totals. Because we reuse the
        # up-front run_id — never allocating a new one — all batches share ONE
        # logical run and `WHERE run_id = MAX(run_id)` covers the whole suite.
        if args.results_db and run_id is not None:
            persist_batch()
            print(f"\nResults saved to: {args.results_db} (run_id={run_id})")

        if _SAVE_HAD_FATAL_INSERT_FAILURES:
            sys.exit(2)
        if total_failed > 0:
            sys.exit(1)
        sys.exit(0)

    # Standard parsing mode
    runner = TclTestRunner(
        vibesql_path=args.vibesql,
        verbose=args.verbose,
        timeout=args.timeout,
    )

    # Run tests
    summary = runner.run_files(file_paths, parallel=args.parallel)

    # Print summary
    print()
    print("=" * 50)
    print(f"Total tests: {summary.total_tests}")
    print(f"Passed:      {summary.passed} ({summary.passed/summary.total_tests*100:.1f}%)" if summary.total_tests > 0 else "Passed: 0")
    print(f"Failed:      {summary.failed}")
    print(f"Skipped:     {summary.skipped}")
    if summary.skipped_setup_failed > 0:
        print(f"  (setup failed: {summary.skipped_setup_failed})")
    print(f"Errors:      {summary.parse_errors}")
    if summary.setup_failures > 0:
        print(f"Setup failures: {summary.setup_failures} files")
    print("-" * 50)
    files_with_results = len({r.file_path for r in summary.results})
    print(f"Files attempted:    {summary.total_files}")
    print(f"Files with results: {files_with_results}")
    print("=" * 50)
    if files_with_results < summary.total_files:
        print(
            f"\nWARNING: {summary.total_files - files_with_results} of "
            f"{summary.total_files} attempted file(s) produced no detail rows. "
            f"Totals from this run are NOT comparable to a clean-run baseline.",
            file=sys.stderr,
        )

    # Save to database if specified
    if args.results_db:
        save_to_database(summary, args.results_db, args.vibesql)
        print(f"Results saved to: {args.results_db}")

    # Output JSON if specified
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(summary.to_dict(), f, indent=2)
        print(f"JSON output: {args.output}")

    # Exit with error if there were failures. status='error' rows (parse
    # errors and runner-error marker rows) also fail the run so a compromised
    # static run exits non-zero, matching native mode (#5822 review note).
    if _SAVE_HAD_FATAL_INSERT_FAILURES:
        sys.exit(2)
    if summary.failed > 0 or summary.parse_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
