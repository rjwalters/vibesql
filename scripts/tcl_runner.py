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
    results: list[TestResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "git_commit": self.git_commit,
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
                        summary.parse_errors += 1
                        print(f"Error processing {file_path}: {e}")
        else:
            # Sequential execution
            for i, file_path in enumerate(file_paths):
                if self.verbose:
                    print(f"[{i+1}/{len(file_paths)}] {Path(file_path).name}")

                try:
                    results, file_setup_failed = self.run_file(file_path)
                    process_file_results(results, file_setup_failed)
                except Exception as e:
                    summary.parse_errors += 1
                    print(f"Error processing {file_path}: {e}")

        summary.completed_at = datetime.now().isoformat()
        return summary


def save_to_database(summary: RunSummary, db_path: str, vibesql_path: str):
    """Save run results to the database."""
    # First, get the next run_id
    result = subprocess.run(
        [vibesql_path, db_path, "-c", "SELECT COALESCE(MAX(run_id), 0) + 1 FROM tcl_test_runs", "--format", "raw"],
        capture_output=True,
        text=True,
    )
    try:
        # Parse output - look for the number in the result
        lines = [l.strip() for l in result.stdout.strip().split('\n') if l.strip()]
        run_id = int(lines[-1])
    except (ValueError, IndexError):
        run_id = 1

    # Create run record
    run_sql = f"""
        INSERT INTO tcl_test_runs (
            run_id, started_at, completed_at, git_commit,
            total_files, total_tests, passed, failed, skipped, skipped_setup_failed, parse_errors, setup_failures
        ) VALUES (
            {run_id}, '{summary.started_at}', '{summary.completed_at}', '{summary.git_commit}',
            {summary.total_files}, {summary.total_tests},
            {summary.passed}, {summary.failed}, {summary.skipped}, {summary.skipped_setup_failed}, {summary.parse_errors}, {summary.setup_failures}
        );
    """

    try:
        subprocess.run(
            [vibesql_path, db_path, "-c", run_sql],
            capture_output=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Warning: Failed to save run summary: {e.stderr}")
        return

    # Helper to escape SQL strings
    def escape_sql(s: str, max_len: int = 1000) -> str:
        if not s:
            return ""
        # Remove null bytes and other control chars that break subprocess/SQL
        s = s.replace('\x00', '')
        # Replace single quotes with doubled quotes
        s = s.replace("'", "''")
        # Remove newlines and other control chars that might break the INSERT
        s = s.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        # Truncate
        return s[:max_len]

    # Get next result ID
    result = subprocess.run(
        [vibesql_path, db_path, "-c", "SELECT COALESCE(MAX(id), 0) FROM tcl_test_results", "--format", "raw"],
        capture_output=True,
        text=True,
    )
    try:
        lines = [l.strip() for l in result.stdout.strip().split('\n') if l.strip()]
        next_id = int(lines[-1]) + 1
    except (ValueError, IndexError):
        next_id = 1

    # Insert results one at a time to avoid batch issues
    for r in summary.results:
        sql_escaped = escape_sql(r.sql, 1000)
        expected = escape_sql(r.expected_output, 1000)
        actual = escape_sql(r.actual_output, 1000)
        error = escape_sql(r.error_message, 500)
        file_path = escape_sql(r.file_path, 500)
        test_name = escape_sql(r.test_name, 200)

        insert_sql = f"""
            INSERT INTO tcl_test_results (
                id, run_id, file_path, test_name, test_type, status,
                sql_text, expected_output, actual_output, error_message,
                execution_time_ms, line_number
            ) VALUES (
                {next_id},
                {run_id},
                '{file_path}',
                '{test_name}',
                '{r.test_type}',
                '{r.status}',
                '{sql_escaped}',
                '{expected}',
                '{actual}',
                '{error}',
                {r.execution_time_ms},
                {r.line_number}
            );
        """
        next_id += 1

        try:
            subprocess.run(
                [vibesql_path, db_path, "-c", insert_sql],
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            # Silently skip problematic records
            pass


def run_native_tcl(test_file: str, shim_path: str, verbose: bool = False, timeout: float = 60.0) -> tuple[int, int, int, list[str]]:
    """
    Run a TCL test file using the native tclsh interpreter with our VibeSQL shim.

    This mode is for test files that use TCL constructs (like for loops) that
    can't be statically parsed.

    Returns: (passed, failed, skipped, failed_test_names)
    """
    cmd = ["tclsh", shim_path, test_file]
    if verbose:
        cmd.append("--verbose")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        output = result.stdout + result.stderr

        # Parse the summary from output
        # Expected format:
        # Tests run:    N
        # Tests passed: N
        # Tests failed: N
        # Tests skipped: N

        passed = 0
        failed = 0
        skipped = 0
        failed_tests = []

        for line in output.split('\n'):
            if 'Tests passed:' in line:
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
        if 'Failed tests:' in output:
            in_failed_section = False
            for line in output.split('\n'):
                if 'Failed tests:' in line:
                    in_failed_section = True
                    continue
                if in_failed_section and line.strip().startswith('-'):
                    failed_tests.append(line.strip().lstrip('- '))

        # Always print output - in non-verbose mode, TCL shim only outputs failures
        # In verbose mode, it outputs all test results
        if output.strip():
            print(output)

        return passed, failed, skipped, failed_tests

    except subprocess.TimeoutExpired:
        print(f"Timeout running {test_file}")
        return 0, 0, 0, ["TIMEOUT"]
    except Exception as e:
        print(f"Error running {test_file}: {e}")
        return 0, 0, 0, [str(e)]


def main():
    parser = argparse.ArgumentParser(description="Run TCL tests against VibeSQL")
    parser.add_argument("--file", "-f", help="Single file to test")
    parser.add_argument("--files", nargs="+", help="Multiple files to test")
    parser.add_argument("--vibesql", required=True, help="Path to VibeSQL binary")
    parser.add_argument("--results-db", help="Path to results database")
    parser.add_argument("--output", "-o", help="Output JSON file")
    parser.add_argument("--parallel", action="store_true", help="Run tests in parallel")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--timeout", type=float, default=None, help="Per-test timeout in seconds (default: 5 for static mode, 300 for native TCL)")
    parser.add_argument("--native-tcl", action="store_true",
                        help="Run tests using native tclsh instead of parsing (for files with TCL loops)")
    parser.add_argument("--tcl-shim", default=None,
                        help="Path to TCL shim (default: scripts/tester_vibesql.tcl)")

    args = parser.parse_args()

    if not os.path.exists(args.vibesql):
        print(f"Error: VibeSQL binary not found: {args.vibesql}", file=sys.stderr)
        sys.exit(1)

    # Set appropriate timeout defaults based on mode
    if args.timeout is None:
        if args.native_tcl:
            args.timeout = 300.0  # 5 minutes for native TCL (handles heavy INSERT tests)
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

        for file_path in file_paths:
            if args.verbose:
                print(f"\nRunning: {file_path}")

            passed, failed, skipped, failed_tests = run_native_tcl(
                file_path, shim_path, verbose=args.verbose, timeout=args.timeout
            )
            total_passed += passed
            total_failed += failed
            total_skipped += skipped
            all_failed_tests.extend(failed_tests)

        # Print summary
        total_tests = total_passed + total_failed + total_skipped
        print()
        print("=" * 50)
        print(f"Total tests: {total_tests}")
        print(f"Passed:      {total_passed} ({total_passed/total_tests*100:.1f}%)" if total_tests > 0 else "Passed: 0")
        print(f"Failed:      {total_failed}")
        print(f"Skipped:     {total_skipped}")
        print("=" * 50)

        if all_failed_tests:
            print("\nFailed tests:")
            for t in all_failed_tests[:20]:  # Limit to first 20
                print(f"  - {t}")
            if len(all_failed_tests) > 20:
                print(f"  ... and {len(all_failed_tests) - 20} more")

        # Save to database if specified (native TCL mode)
        if args.results_db:
            # Create a RunSummary for native TCL mode
            from datetime import datetime
            summary = RunSummary(
                started_at=datetime.now().isoformat(),
                completed_at=datetime.now().isoformat(),
                git_commit=subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()[:8],
                total_files=len(file_paths),
                total_tests=total_tests,
                passed=total_passed,
                failed=total_failed,
                skipped=total_skipped,
                skipped_setup_failed=0,
                parse_errors=0,
                setup_failures=0,
                results=[]  # Native TCL mode doesn't have individual test results yet
            )
            save_to_database(summary, args.results_db, args.vibesql)
            print(f"\nResults saved to: {args.results_db}")

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
    print("=" * 50)

    # Save to database if specified
    if args.results_db:
        save_to_database(summary, args.results_db, args.vibesql)
        print(f"Results saved to: {args.results_db}")

    # Output JSON if specified
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(summary.to_dict(), f, indent=2)
        print(f"JSON output: {args.output}")

    # Exit with error if there were failures
    if summary.failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
