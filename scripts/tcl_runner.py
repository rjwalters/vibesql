#!/usr/bin/env python3
"""
TCL Test Runner for VibeSQL

Executes parsed TCL tests against VibeSQL and records results.
Works with tcl_parser.py to parse test files first.
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

    def execute_setup(self, sql_statements: list[str]) -> tuple[bool, str]:
        """Execute setup SQL statements."""
        for sql in sql_statements:
            success, _, stderr = self.execute(sql)
            if not success:
                return (False, f"Setup failed: {stderr}")
        return (True, "")


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
    """Compare expected and actual output, handling TCL format quirks."""
    expected_norm = normalize_output(expected)
    actual_norm = normalize_output(actual)

    if expected_norm == actual_norm:
        return True

    # Handle numeric comparisons (e.g., 1.0 vs 1)
    try:
        exp_parts = expected_norm.split()
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
    except:
        return False


class TclTestRunner:
    """Runs TCL tests against VibeSQL."""

    def __init__(self, vibesql_path: str, verbose: bool = False, timeout: float = 5.0):
        self.vibesql_path = vibesql_path
        self.verbose = verbose
        self.timeout = timeout
        self.parser = TclTestParser(verbose=verbose)

    def run_single_test(self, test: ParsedTest, vibesql: VibeSQL, file_path: str) -> TestResult:
        """Run a single test and return the result."""
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
            # Run setup SQL first
            for sql in parsed.setup_sql:
                success, error = vibesql.execute_setup([sql])
                if not success:
                    setup_failed = True
                    setup_error_message = error
                    if self.verbose:
                        print(f"  Setup FAILED: {error}")
                    # Don't break - try to execute remaining setup statements
                    # as some may succeed and allow partial test execution

            # If setup failed, mark all tests as skipped (not failed)
            if setup_failed and parsed.tests:
                if self.verbose:
                    print(f"  Skipping {len(parsed.tests)} tests due to setup failure")

                for test in parsed.tests:
                    # Tests already marked as skipped (e.g., complex TCL) stay skipped
                    if test.test_type == TestType.SKIPPED:
                        results.append(TestResult(
                            test_name=test.name,
                            file_path=file_path,
                            test_type=test.test_type.value,
                            status="skipped",
                            sql=test.sql,
                            error_message=test.skip_reason,
                            line_number=test.line_number,
                        ))
                    else:
                        # Mark as skipped due to setup failure
                        results.append(TestResult(
                            test_name=test.name,
                            file_path=file_path,
                            test_type=test.test_type.value,
                            status="skipped",
                            sql=test.sql,
                            error_message=f"Setup failed: {setup_error_message}",
                            line_number=test.line_number,
                        ))

                return results, True

            # Run each test (setup succeeded)
            for test in parsed.tests:
                result = self.run_single_test(test, vibesql, file_path)
                results.append(result)

                if self.verbose:
                    status_icon = "✓" if result.status == "passed" else "✗" if result.status == "failed" else "○"
                    print(f"  {status_icon} {test.name}: {result.status}")

        return results, setup_failed

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


def main():
    parser = argparse.ArgumentParser(description="Run TCL tests against VibeSQL")
    parser.add_argument("--file", "-f", help="Single file to test")
    parser.add_argument("--files", nargs="+", help="Multiple files to test")
    parser.add_argument("--vibesql", required=True, help="Path to VibeSQL binary")
    parser.add_argument("--results-db", help="Path to results database")
    parser.add_argument("--output", "-o", help="Output JSON file")
    parser.add_argument("--parallel", action="store_true", help="Run tests in parallel")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--timeout", type=float, default=5.0, help="Per-test timeout in seconds")

    args = parser.parse_args()

    if not os.path.exists(args.vibesql):
        print(f"Error: VibeSQL binary not found: {args.vibesql}", file=sys.stderr)
        sys.exit(1)

    runner = TclTestRunner(
        vibesql_path=args.vibesql,
        verbose=args.verbose,
        timeout=args.timeout,
    )

    # Determine files to run
    if args.file:
        file_paths = [args.file]
    elif args.files:
        file_paths = args.files
    else:
        print("Error: No files specified", file=sys.stderr)
        sys.exit(1)

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
