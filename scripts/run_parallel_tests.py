#!/usr/bin/env python3
"""
Parallel SQLLogicTest runner for VibeSQL.

This script orchestrates multiple worker processes to run SQLLogicTest files in parallel,
dramatically reducing test suite execution time from ~30+ minutes (serial) to ~2 minutes (parallel).

Architecture:
- Main process discovers all 623 test files and initializes a shared work queue
- Workers dynamically pull test files from the queue one at a time (dynamic load balancing)
- Each worker runs test files individually with per-file timeouts (prevents worker hangs)
- Workers write individual JSON results to target/sqllogictest_results_worker_N.json
- Main process merges all worker results into target/sqllogictest_cumulative.json
- Results are compatible with existing database integration (process_test_results.py)

Work Queue Strategy:
- File-based queue with fcntl locking for thread-safe dequeue operations
- Workers pull files round-robin until queue is empty
- Ensures balanced load even when test files have varying execution times
- Fast workers process more files; slow workers don't hold up the suite

Per-File Timeout Strategy:
- Each file has a 60s timeout to prevent hangs on slow/infinite-loop queries
- If a file times out, the worker continues testing remaining files
- Time budget is enforced across all files in the partition
- Workers gracefully stop when time budget is exhausted

Usage:
    python3 scripts/run_parallel_tests.py --workers 8 --time-budget 300

Environment Variables (set by runner for each test):
    SQLLOGICTEST_WORKER_ID: Worker number (0-indexed)
    SQLLOGICTEST_FILES: Test file to run (set per-file by worker)
    SQLLOGICTEST_TIME_BUDGET: Time budget in seconds per file
    RAYON_NUM_THREADS: Number of rayon threads (prevents CPU contention)

CPU Contention Prevention:
    When running many parallel workers, each spawning rayon threads, CPU contention
    can significantly degrade performance. Benchmarks show that 4 rayon threads is
    optimal - 31% faster than using all cores (default). Each worker is limited to
    4 rayon threads by default (configurable via --rayon-threads).

Example:
    # Run with 8 workers, 5 minutes per worker
    ./scripts/sqllogictest run --parallel --workers 8 --time 300

    # Run with all available CPUs
    ./scripts/sqllogictest run --parallel
"""

import argparse
import fcntl
import json
import multiprocessing
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Import shared configuration for test results storage
sys.path.insert(0, str(Path(__file__).parent))
from test_results_config import (
    get_default_database_path,
    get_git_branch,
    get_git_commit,
    get_repo_root,
)


class WorkQueue:
    """
    Thread-safe work queue using file-based locking.

    Workers pull test files one at a time from the queue, ensuring balanced
    load distribution even when test files have varying execution times.
    """

    def __init__(self, queue_file: Path):
        """
        Initialize work queue.

        Args:
            queue_file: Path to queue file (will be created/overwritten)
        """
        self.queue_file = queue_file
        self.lock_file = queue_file.with_suffix('.lock')

    def initialize(self, test_files: List[str]):
        """
        Initialize queue with test files.

        Args:
            test_files: List of test file paths to add to queue
        """
        # Create queue file with all test files (one per line)
        self.queue_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.queue_file, 'w') as f:
            for test_file in test_files:
                f.write(f"{test_file}\n")

        # Create lock file
        self.lock_file.touch()

    def dequeue(self) -> Optional[str]:
        """
        Atomically dequeue next test file from queue.

        Returns:
            Next test file path, or None if queue is empty
        """
        # Open lock file for exclusive locking
        with open(self.lock_file, 'r') as lock_fd:
            # Acquire exclusive lock
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)

            try:
                # Read all remaining files
                if not self.queue_file.exists():
                    return None

                with open(self.queue_file, 'r') as f:
                    lines = f.readlines()

                if not lines:
                    return None

                # Pop first file
                next_file = lines[0].strip()
                remaining_files = lines[1:]

                # Write remaining files back
                with open(self.queue_file, 'w') as f:
                    f.writelines(remaining_files)

                return next_file

            finally:
                # Release lock (automatically released when lock_fd closes)
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)

    def size(self) -> int:
        """
        Get current queue size (number of remaining test files).

        Returns:
            Number of files remaining in queue
        """
        with open(self.lock_file, 'r') as lock_fd:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)

            try:
                if not self.queue_file.exists():
                    return 0

                with open(self.queue_file, 'r') as f:
                    return sum(1 for line in f if line.strip())

            finally:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)


class StreamingDatabaseWriter:
    """
    Thread-safe streaming database writer for test results.

    Workers write test results to the database incrementally as they complete,
    rather than waiting for all tests to finish. Uses file locking for safe
    concurrent writes.
    """

    def __init__(self, db_path: Path, run_id: int, git_commit: Optional[str]):
        """
        Initialize streaming database writer.

        Args:
            db_path: Path to SQL database file
            run_id: Unique ID for this test run
            git_commit: Git commit hash for this run
        """
        self.db_path = db_path
        self.lock_file = db_path.with_suffix('.lock')
        self.run_id = run_id
        self.git_commit = git_commit

        # Ensure database and lock file exist
        db_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.lock_file.exists():
            self.lock_file.touch()

    def write_test_result(self, file_path: str, status: str, duration_ms: Optional[int], error_message: Optional[str] = None):
        """
        Write a single test result to the database.

        Args:
            file_path: Test file path
            status: Test status ('PASS', 'FAIL', 'TIMEOUT')
            duration_ms: Test duration in milliseconds
            error_message: Error message if test failed
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        # Categorize test file
        category, subcategory = self._categorize_test_file(file_path)

        # Generate SQL statements
        statements = []

        # 1. Upsert into test_files
        last_passed = f"TIMESTAMP '{timestamp}'" if status == 'PASS' else "NULL"
        statements.append(f"""
INSERT OR REPLACE INTO test_files (file_path, category, subcategory, status, last_tested, last_passed)
VALUES ({self._sql_escape(file_path)}, {self._sql_escape(category)}, {self._sql_escape(subcategory)}, '{status}', TIMESTAMP '{timestamp}', {last_passed});
""")

        # 2. Insert into test_results
        result_id = abs(hash(f'{self.run_id}_{file_path}_{timestamp}'))
        duration_val = duration_ms if duration_ms is not None else "NULL"
        error_val = self._sql_escape(error_message)

        statements.append(f"""
INSERT INTO test_results (result_id, run_id, file_path, status, tested_at, duration_ms, error_message)
VALUES ({result_id}, {self.run_id}, {self._sql_escape(file_path)}, '{status}', TIMESTAMP '{timestamp}', {duration_val}, {error_val});
""")

        # Write to database with locking
        with open(self.lock_file, 'r') as lock_fd:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)

            try:
                with open(self.db_path, 'a') as f:
                    for stmt in statements:
                        f.write(stmt)
                        f.write("\n")
            finally:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)

    def _categorize_test_file(self, file_path: str) -> tuple:
        """Categorize test file by path."""
        parts = Path(file_path).parts

        if len(parts) == 0:
            return "other", "root"

        category = parts[0] if parts[0] in ["index", "random", "evidence", "select", "ddl"] else "other"
        subcategory = parts[1] if len(parts) >= 2 else "root"

        return category, subcategory

    def _sql_escape(self, value: Optional[str]) -> str:
        """Escape a value for SQL, handling NULL and special characters."""
        if value is None:
            return "NULL"

        value = str(value)

        # Escape backslashes first
        escaped = value.replace('\\', '\\\\')

        # Escape single quotes
        escaped = escaped.replace("'", "''")

        # Replace control characters
        escaped = escaped.replace('\n', ' ')
        escaped = escaped.replace('\r', ' ')
        escaped = escaped.replace('\t', ' ')
        escaped = escaped.replace('\x00', '')

        # Collapse multiple spaces
        escaped = ' '.join(escaped.split())

        # Truncate to 2000 chars for error messages
        if len(escaped) > 2000:
            escaped = escaped[:2000]

        return f"'{escaped}'"

    def finalize(self, cumulative_results: Dict):
        """
        Finalize the test run by updating the test_runs table with final counts.

        Args:
            cumulative_results: Dictionary containing test results summary with keys:
                               'summary' with 'total', 'passed', 'failed', 'errors', etc.
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        summary = cumulative_results.get('summary', {})

        total = summary.get('total', 0)
        passed = summary.get('passed', 0)
        failed = summary.get('failed', 0) + summary.get('errors', 0)
        untested = total - passed - failed

        # Update test_runs table with final counts
        update_stmt = f"""
-- Test run finalized at {timestamp}
UPDATE test_runs
SET completed_at = TIMESTAMP '{timestamp}',
    passed = {passed},
    failed = {failed},
    untested = {untested}
WHERE run_id = {self.run_id};
"""

        # Write to database with locking
        with open(self.lock_file, 'r') as lock_fd:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)

            try:
                with open(self.db_path, 'a') as f:
                    f.write(update_stmt)
                    f.write("\n")
            finally:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)


def discover_test_files(repo_root: Path) -> List[str]:
    """
    Discover all SQLLogicTest files in the repository.

    Returns:
        List of test file paths relative to third_party/sqllogictest/test/
    """
    test_dir = repo_root / "third_party" / "sqllogictest" / "test"

    if not test_dir.exists():
        raise RuntimeError(
            f"SQLLogicTest directory not found: {test_dir}\n"
            "Run: git submodule update --init --recursive"
        )

    # Find all .test files
    test_files = []
    for test_file in test_dir.rglob("*.test"):
        # Get path relative to test directory
        relative_path = test_file.relative_to(test_dir)
        test_files.append(str(relative_path))

    return sorted(test_files)


def initialize_work_queue(repo_root: Path, work_queue_dir: Path) -> int:
    """
    Initialize work queue with all test files.

    This function is called by the serial test runner when --force is specified.
    It creates a work queue directory and populates it with all test files.

    Args:
        repo_root: Repository root directory
        work_queue_dir: Directory to store work queue files

    Returns:
        Number of test files added to queue
    """
    # Discover all test files
    test_files = discover_test_files(repo_root)

    # Create work queue directory
    work_queue_dir.mkdir(parents=True, exist_ok=True)

    # Write test files to queue (one file per line)
    queue_file = work_queue_dir / "test_files.txt"
    with open(queue_file, 'w') as f:
        for test_file in test_files:
            f.write(f"{test_file}\n")

    return len(test_files)


def run_worker(worker_id: int, work_queue: WorkQueue, db_writer: Optional[StreamingDatabaseWriter], time_budget: int, repo_root: Path, release_mode: bool = True, per_file_timeout: int = 500, rayon_threads: int = 4) -> Tuple[int, Optional[Dict]]:
    """
    Run a single worker process, pulling test files from queue until empty.

    Args:
        worker_id: Worker number (0-indexed)
        work_queue: Shared work queue to pull test files from
        db_writer: Optional database writer for streaming results
        time_budget: Time budget in seconds
        repo_root: Repository root directory
        release_mode: Whether to use release binary (default: True for performance)
        per_file_timeout: Timeout in seconds for each test file (default: 500s)
        rayon_threads: Number of rayon threads per worker (default: 4, optimal for performance)

    Returns:
        (worker_id, results_dict) or (worker_id, None) on failure
    """
    print(f"[Worker {worker_id}] Starting (pulling from shared queue)", flush=True)
    print(f"[Worker {worker_id}] Time budget: {time_budget}s", flush=True)
    if db_writer:
        print(f"[Worker {worker_id}] Streaming results to database", flush=True)

    # Find the test binary (built with --no-run earlier)
    # Location depends on build mode (release is 10-15x faster)
    build_type = "release" if release_mode else "debug"
    test_binary_pattern = repo_root / "target" / build_type / "deps" / "sqllogictest_suite-*"
    import glob as glob_module
    test_binaries = sorted(glob_module.glob(str(test_binary_pattern)), key=os.path.getmtime, reverse=True)

    # Filter to executables only (no .d files)
    test_binaries = [b for b in test_binaries if os.access(b, os.X_OK) and not b.endswith('.d')]

    if not test_binaries:
        print(f"[Worker {worker_id}] ERROR: Test binary not found at {test_binary_pattern}", flush=True)
        print(f"[Worker {worker_id}] ERROR: Expected binary in target/{build_type}/deps/", flush=True)
        print(f"[Worker {worker_id}] ERROR: Build may have failed or binary was not created", flush=True)
        return (worker_id, None)

    test_binary = test_binaries[0]

    # Run files with per-file timeout to prevent hangs
    worker_start_time = time.time()
    files_tested = 0
    files_timed_out = 0
    all_results = []

    # Clean up any stale per-file result files from previous runs
    import glob as glob_cleanup
    stale_files = glob_cleanup.glob(str(repo_root / "target" / f"sqllogictest_results_worker_{worker_id}_file_*.json"))
    for stale_file in stale_files:
        try:
            Path(stale_file).unlink()
        except:
            pass  # Ignore cleanup failures

    # Per-file timeout is now configurable via --per-file-timeout argument (default: 500s)
    # Most files complete in <10s, but high-volume index tests (10,000+ queries) need more time
    # Slow tests may require higher timeout values:
    #   Development: 300s (5 min) - fast feedback
    #   CI/Standard: 500s (8.3 min) - covers 99% of tests
    #   Comprehensive: 2000s (33 min) - covers all known slow tests + margin
    #   Debug: 5000s (83 min) - for investigation only
    print(f"[Worker {worker_id}] Per-file timeout: {per_file_timeout}s", flush=True)

    # Pull files from queue until empty or time budget exhausted
    while True:
        # Check if we've exceeded our time budget
        elapsed = time.time() - worker_start_time
        if elapsed >= time_budget:
            print(f"[Worker {worker_id}] Time budget exhausted ({elapsed:.1f}s / {time_budget}s), stopping", flush=True)
            print(f"[Worker {worker_id}] Tested {files_tested} files", flush=True)
            break

        # Dequeue next test file
        test_file = work_queue.dequeue()
        if test_file is None:
            print(f"[Worker {worker_id}] Queue empty, all work complete", flush=True)
            print(f"[Worker {worker_id}] Tested {files_tested} files", flush=True)
            break

        # Set environment variables for this file
        env = os.environ.copy()
        env["SQLLOGICTEST_WORKER_ID"] = str(worker_id)
        env["SQLLOGICTEST_FILES"] = test_file  # Single file
        env["SQLLOGICTEST_TIME_BUDGET"] = str(per_file_timeout)
        # Limit rayon threads to prevent CPU contention when running parallel workers
        # Each worker gets a limited number of threads instead of all CPUs
        env["RAYON_NUM_THREADS"] = str(rayon_threads)

        # Run the test binary for this single file
        cmd = [test_binary]

        # Unique result file for this run
        run_results_file = repo_root / "target" / f"sqllogictest_results_worker_{worker_id}_file_{files_tested}.json"

        try:
            file_start_time = time.time()

            # Redirect stdout/stderr to files to avoid blocking
            stdout_file = repo_root / "target" / f"worker_{worker_id}_file_{files_tested}_stdout.log"
            stderr_file = repo_root / "target" / f"worker_{worker_id}_file_{files_tested}_stderr.log"

            with open(stdout_file, 'w') as stdout_f, open(stderr_file, 'w') as stderr_f:
                result = subprocess.run(
                    cmd,
                    env=env,
                    cwd=repo_root,
                    stdout=stdout_f,
                    stderr=stderr_f,
                    timeout=per_file_timeout + 10  # Small grace period for process startup/cleanup
                )

            file_elapsed = time.time() - file_start_time
            files_tested += 1

            # Get remaining queue size for progress tracking
            queue_remaining = work_queue.size()

            # Determine test status
            test_status = "PASS" if result.returncode == 0 else "FAIL"
            duration_ms = int(file_elapsed * 1000)

            # Write to database immediately
            if db_writer:
                try:
                    db_writer.write_test_result(test_file, test_status, duration_ms)
                except Exception as e:
                    print(f"[Worker {worker_id}] Warning: Failed to write to database: {e}", flush=True)

            # Check if test succeeded
            if result.returncode != 0:
                print(f"[Worker {worker_id}] File {files_tested} (queue: {queue_remaining} remaining): {test_file} FAILED (exit {result.returncode}) in {file_elapsed:.1f}s", flush=True)
            else:
                print(f"[Worker {worker_id}] File {files_tested} (queue: {queue_remaining} remaining): {test_file} completed in {file_elapsed:.1f}s", flush=True)

            # Rename the standard result file to our unique per-file name
            # The Rust test binary writes to sqllogictest_results_worker_{worker_id}.json
            # We need to rename it to avoid overwriting results from previous files
            standard_results_file = repo_root / "target" / f"sqllogictest_results_worker_{worker_id}.json"
            if standard_results_file.exists():
                try:
                    standard_results_file.rename(run_results_file)
                except Exception as e:
                    print(f"[Worker {worker_id}] Warning: Failed to rename result file: {e}", flush=True)

            # Read results if they exist
            if run_results_file.exists():
                with open(run_results_file, 'r') as f:
                    file_results = json.load(f)
                    all_results.append(file_results)
                # Clean up individual result file after reading
                run_results_file.unlink()
            else:
                # Log if results file doesn't exist (test may have crashed or failed to write results)
                print(f"[Worker {worker_id}] Warning: No results file found for {test_file}", flush=True)

        except subprocess.TimeoutExpired:
            files_tested += 1
            files_timed_out += 1
            queue_remaining = work_queue.size()

            # Write timeout to database
            if db_writer:
                try:
                    db_writer.write_test_result(test_file, "TIMEOUT", int(per_file_timeout * 1000), "Test exceeded time limit")
                except Exception as e:
                    print(f"[Worker {worker_id}] Warning: Failed to write timeout to database: {e}", flush=True)

            print(f"[Worker {worker_id}] File {files_tested} (queue: {queue_remaining} remaining): {test_file} TIMEOUT after {per_file_timeout}s", flush=True)
            # Clean up any partial result file that may have been written
            standard_results_file = repo_root / "target" / f"sqllogictest_results_worker_{worker_id}.json"
            if standard_results_file.exists():
                try:
                    standard_results_file.unlink()
                except:
                    pass  # Ignore cleanup failures
            # Continue to next file instead of failing entire worker

        except Exception as e:
            files_tested += 1
            queue_remaining = work_queue.size()
            print(f"[Worker {worker_id}] File {files_tested} (queue: {queue_remaining} remaining): {test_file} ERROR: {type(e).__name__}: {e}", flush=True)
            # Clean up any partial result file that may have been written
            standard_results_file = repo_root / "target" / f"sqllogictest_results_worker_{worker_id}.json"
            if standard_results_file.exists():
                try:
                    standard_results_file.unlink()
                except:
                    pass  # Ignore cleanup failures
            # Continue to next file

    total_elapsed = time.time() - worker_start_time

    print(f"[Worker {worker_id}] Completed: {files_tested} files tested in {total_elapsed:.1f}s", flush=True)
    if files_timed_out > 0:
        print(f"[Worker {worker_id}] Warning: {files_timed_out} files timed out", flush=True)

    # Merge all results from individual file runs
    if not all_results:
        print(f"[Worker {worker_id}] No results collected", flush=True)
        return (worker_id, None)

    merged_results = merge_file_results(all_results)

    # Write merged results to worker result file
    final_results_file = repo_root / "target" / f"sqllogictest_results_worker_{worker_id}.json"
    with open(final_results_file, 'w') as f:
        json.dump(merged_results, f, indent=2)

    return (worker_id, merged_results)


def merge_file_results(file_results: List[Dict]) -> Dict:
    """
    Merge results from multiple individual file test runs into a single result.

    Args:
        file_results: List of result dictionaries from individual file runs

    Returns:
        Merged result dictionary compatible with merge_worker_results()
    """
    if not file_results:
        return {
            "summary": {
                "total": 0,
                "passed": 0,
                "failed": 0,
                "errors": 0,
                "skipped": 0,
                "pass_rate": 0.0,
                "total_available_files": 0,
                "tested_files": 0,
            },
            "tested_files": {
                "passed": [],
                "failed": [],
            },
            "categories": {},
            "detailed_failures": [],
        }

    # Initialize merged result
    merged = {
        "summary": {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "errors": 0,
            "skipped": 0,
            "pass_rate": 0.0,
            "total_available_files": 0,
            "tested_files": 0,
        },
        "tested_files": {
            "passed": [],
            "failed": [],
        },
        "categories": {},
        "detailed_failures": [],
    }

    # Merge each file result
    for result in file_results:
        summary = result.get("summary", {})
        merged["summary"]["total"] += summary.get("total", 0)
        merged["summary"]["passed"] += summary.get("passed", 0)
        merged["summary"]["failed"] += summary.get("failed", 0)
        merged["summary"]["errors"] += summary.get("errors", 0)
        merged["summary"]["skipped"] += summary.get("skipped", 0)
        merged["summary"]["total_available_files"] += summary.get("total_available_files", 0)
        merged["summary"]["tested_files"] += summary.get("tested_files", 0)

        # Merge tested files lists
        tested_files = result.get("tested_files", {})
        merged["tested_files"]["passed"].extend(tested_files.get("passed", []))
        merged["tested_files"]["failed"].extend(tested_files.get("failed", []))

        # Merge detailed failures
        merged["detailed_failures"].extend(result.get("detailed_failures", []))

        # Merge categories
        categories = result.get("categories", {})
        for category_name, category_data in categories.items():
            if category_data is None:
                continue
            if category_name not in merged["categories"]:
                merged["categories"][category_name] = {
                    "total": 0,
                    "passed": 0,
                    "failed": 0,
                    "errors": 0,
                    "skipped": 0,
                    "pass_rate": 0.0,
                }
            merged["categories"][category_name]["total"] += category_data.get("total", 0)
            merged["categories"][category_name]["passed"] += category_data.get("passed", 0)
            merged["categories"][category_name]["failed"] += category_data.get("failed", 0)
            merged["categories"][category_name]["errors"] += category_data.get("errors", 0)
            merged["categories"][category_name]["skipped"] += category_data.get("skipped", 0)

    # Calculate overall pass rate
    total = merged["summary"]["total"]
    if total > 0:
        merged["summary"]["pass_rate"] = round((merged["summary"]["passed"] / total) * 100, 2)

    # Calculate category pass rates
    for category_name, category_data in merged["categories"].items():
        total = category_data["total"]
        if total > 0:
            category_data["pass_rate"] = round((category_data["passed"] / total) * 100, 2)

    return merged


def merge_worker_results(worker_results: List[Tuple[int, Optional[Dict]]]) -> Dict:
    """
    Merge results from all workers into a single cumulative result.

    Args:
        worker_results: List of (worker_id, results_dict) tuples

    Returns:
        Merged results dictionary compatible with process_test_results.py
    """
    # Initialize cumulative results
    cumulative = {
        "summary": {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "errors": 0,
            "skipped": 0,
            "pass_rate": 0.0,
            "total_available_files": 0,
            "tested_files": 0,
        },
        "tested_files": {
            "passed": [],
            "failed": [],
        },
        "categories": {
            "select": None,
            "evidence": None,
            "index": None,
            "random": None,
            "ddl": None,
            "other": None,
        },
        "detailed_failures": [],
    }

    # Track category stats for merging
    category_stats = {
        "select": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0},
        "evidence": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0},
        "index": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0},
        "random": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0},
        "ddl": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0},
        "other": {"total": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0},
    }

    # Merge results from each worker
    for worker_id, results in worker_results:
        if results is None:
            continue

        # Merge summary
        summary = results.get("summary", {})
        cumulative["summary"]["total"] += summary.get("total", 0)
        cumulative["summary"]["passed"] += summary.get("passed", 0)
        cumulative["summary"]["failed"] += summary.get("failed", 0)
        cumulative["summary"]["errors"] += summary.get("errors", 0)
        cumulative["summary"]["skipped"] += summary.get("skipped", 0)
        cumulative["summary"]["total_available_files"] += summary.get("total_available_files", 0)
        cumulative["summary"]["tested_files"] += summary.get("tested_files", 0)

        # Merge tested files
        tested_files = results.get("tested_files", {})
        cumulative["tested_files"]["passed"].extend(tested_files.get("passed", []))
        cumulative["tested_files"]["failed"].extend(tested_files.get("failed", []))

        # Merge detailed failures
        cumulative["detailed_failures"].extend(results.get("detailed_failures", []))

        # Merge category stats
        categories = results.get("categories", {})
        for category_name, category_data in categories.items():
            if category_data and category_name in category_stats:
                category_stats[category_name]["total"] += category_data.get("total", 0)
                category_stats[category_name]["passed"] += category_data.get("passed", 0)
                category_stats[category_name]["failed"] += category_data.get("failed", 0)
                category_stats[category_name]["errors"] += category_data.get("errors", 0)
                category_stats[category_name]["skipped"] += category_data.get("skipped", 0)

    # Calculate cumulative pass rate
    total = cumulative["summary"]["total"]
    passed = cumulative["summary"]["passed"]
    if total > 0:
        cumulative["summary"]["pass_rate"] = round((passed / total) * 100, 2)

    # Calculate category pass rates and add to cumulative results
    for category_name, stats in category_stats.items():
        if stats["total"] > 0:
            pass_rate = round((stats["passed"] / stats["total"]) * 100, 2)
            cumulative["categories"][category_name] = {
                "total": stats["total"],
                "passed": stats["passed"],
                "failed": stats["failed"],
                "errors": stats["errors"],
                "skipped": stats["skipped"],
                "pass_rate": pass_rate,
            }

    return cumulative


def run_parallel_tests(num_workers: int, time_budget: int, repo_root: Path, release_mode: bool = True, per_file_timeout: int = 500, rayon_threads: int = 4) -> bool:
    """
    Run SQLLogicTest suite in parallel across multiple workers.

    Args:
        num_workers: Number of worker processes to spawn
        time_budget: Time budget in seconds per worker
        repo_root: Repository root directory
        release_mode: Whether to build in release mode (default: True for 10-15x speedup)
        per_file_timeout: Timeout in seconds for each test file (default: 500s)
        rayon_threads: Number of rayon threads per worker (default: 4, optimal for performance)

    Returns:
        True if successful, False otherwise
    """
    build_type = "release" if release_mode else "debug"

    print(f"\n=== Parallel SQLLogicTest Runner ===")
    print(f"Workers: {num_workers}")
    print(f"Time budget: {time_budget}s per worker")
    print(f"Per-file timeout: {per_file_timeout}s")
    print(f"Rayon threads per worker: {rayon_threads}")
    print(f"Build mode: {build_type}")
    print()

    # Build the test binary first to avoid cargo lock contention
    # IMPORTANT: Use --release for 10-15x performance improvement
    print(f"Building test binary ({'release mode - this may take 30-60s' if release_mode else 'debug mode - faster build, slower tests'})...")
    build_cmd = [
        "cargo", "test",
        "--package", "vibesql",
        "--test", "sqllogictest_suite",
        "--no-run"
    ]
    if release_mode:
        build_cmd.append("--release")

    result = subprocess.run(build_cmd, cwd=repo_root, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ Build failed!", file=sys.stderr)
        print(f"\nBuild command: {' '.join(build_cmd)}", file=sys.stderr)
        print(f"\nStderr:\n{result.stderr}", file=sys.stderr)
        print(f"\nStdout:\n{result.stdout}", file=sys.stderr)
        return False
    print(f"✓ Test binary built in {build_type} mode")

    # Verify binary exists
    binary_pattern = repo_root / "target" / build_type / "deps" / "sqllogictest_suite-*"
    import glob as glob_module
    test_binaries = [b for b in glob_module.glob(str(binary_pattern)) if os.access(b, os.X_OK) and not b.endswith('.d')]

    if not test_binaries:
        print(f"❌ ERROR: Test binary not found after build!", file=sys.stderr)
        print(f"Expected at: {binary_pattern}", file=sys.stderr)
        print(f"\nDirectory listing:", file=sys.stderr)
        deps_dir = repo_root / "target" / build_type / "deps"
        if deps_dir.exists():
            sqllogictest_files = list(deps_dir.glob("sqllogictest_suite*"))
            print(f"Found {len(sqllogictest_files)} sqllogictest_suite files:", file=sys.stderr)
            for f in sqllogictest_files:
                executable = "✓" if os.access(f, os.X_OK) else "✗"
                print(f"  {executable} {f.name}", file=sys.stderr)
        else:
            print(f"Directory doesn't exist: {deps_dir}", file=sys.stderr)
        return False

    print(f"✓ Found test binary: {Path(test_binaries[0]).name}")
    print()

    # Discover all test files
    print("Discovering test files...")
    try:
        test_files = discover_test_files(repo_root)
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return False

    print(f"Found {len(test_files)} test files")

    # Initialize work queue with all test files
    print(f"Initializing work queue with {len(test_files)} files...")
    queue_file = repo_root / "target" / "test_work_queue.txt"
    work_queue = WorkQueue(queue_file)
    work_queue.initialize(test_files)
    print(f"✓ Work queue initialized")
    print()

    # Initialize streaming database writer
    db_path = get_default_database_path()
    git_commit = get_git_commit(repo_root)
    git_branch = get_git_branch(repo_root)
    run_id = int(datetime.now().timestamp())

    print(f"Initializing database writer...")
    print(f"  Database: {db_path}")
    print(f"  Run ID: {run_id}")
    print(f"  Git commit: {git_commit or 'unknown'}")
    print(f"  Git branch: {git_branch or 'unknown'}")

    # Ensure schema exists
    schema_path = repo_root / "scripts" / "schema" / "test_results.sql"
    if not db_path.exists() and schema_path.exists():
        print(f"  Creating database from schema...")
        import shutil
        shutil.copy(schema_path, db_path)
        print(f"  ✓ Database created from schema")

    # Create database writer
    db_writer = StreamingDatabaseWriter(db_path, run_id, git_commit)

    # Write test_runs record
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    git_commit_escaped = f"'{git_commit}'" if git_commit else "NULL"
    git_branch_escaped = f"'{git_branch}'" if git_branch else "NULL"
    test_run_stmt = f"""
-- Test run started at {timestamp}
INSERT INTO test_runs (run_id, started_at, completed_at, total_files, passed, failed, untested, git_commit, branch_name)
VALUES ({run_id}, TIMESTAMP '{timestamp}', NULL, {len(test_files)}, 0, 0, {len(test_files)}, {git_commit_escaped}, {git_branch_escaped});
"""
    with open(db_path, 'a') as f:
        f.write(test_run_stmt)
        f.write("\n")

    print(f"✓ Database initialized - results will stream as tests complete")
    print()

    # Run workers in parallel (all pulling from same queue)
    print(f"Starting {num_workers} workers (dynamic load balancing via shared queue)...")
    worker_results = []

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit all workers (they all share the same work queue and database writer)
        futures = {
            executor.submit(run_worker, i, work_queue, db_writer, time_budget, repo_root, release_mode, per_file_timeout, rayon_threads): i
            for i in range(num_workers)
        }

        # Collect results as they complete
        for future in as_completed(futures):
            worker_id = futures[future]
            try:
                result = future.result()
                worker_results.append(result)
            except Exception as e:
                print(f"[Worker {worker_id}] Exception: {e}")
                worker_results.append((worker_id, None))

    print()
    print("All workers completed")
    print()

    # Merge results
    print("Merging results...")
    cumulative_results = merge_worker_results(worker_results)

    # Write cumulative results to JSON file
    output_file = repo_root / "target" / "sqllogictest_cumulative.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump(cumulative_results, f, indent=2)

    print(f"✓ Results written to: {output_file}")
    print()

    # Finalize database: update test_runs table with final counts
    print("Finalizing database...")
    db_writer.finalize(cumulative_results)
    print(f"✓ Database finalized: {db_path}")
    print()

    # Print summary
    summary = cumulative_results["summary"]
    print("=== Summary ===")
    print(f"Total files:  {summary['total']}")
    print(f"Passed:       {summary['passed']}")
    print(f"Failed:       {summary['failed']}")
    print(f"Errors:       {summary['errors']}")
    print(f"Skipped:      {summary['skipped']}")
    print(f"Pass rate:    {summary['pass_rate']:.1f}%")
    print()

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Run SQLLogicTest suite in parallel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--workers",
        type=int,
        required=True,
        help="Number of parallel workers",
    )
    parser.add_argument(
        "--time-budget",
        type=int,
        required=True,
        help="Time budget in seconds per worker",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Use debug build instead of release (faster build, but 10-15x slower tests)",
    )
    parser.add_argument(
        "--per-file-timeout",
        type=int,
        default=500,
        help="Timeout in seconds for each test file (default: 500s)",
    )
    parser.add_argument(
        "--rayon-threads",
        type=int,
        default=4,
        help="Rayon threads per worker (default: 4, optimal for performance)",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.workers < 1:
        print("Error: --workers must be >= 1", file=sys.stderr)
        return 1

    if args.time_budget < 1:
        print("Error: --time-budget must be >= 1", file=sys.stderr)
        return 1

    if args.per_file_timeout < 1:
        print("Error: --per-file-timeout must be >= 1", file=sys.stderr)
        return 1

    if args.rayon_threads < 1:
        print("Error: --rayon-threads must be >= 1", file=sys.stderr)
        return 1

    # Get repository root
    try:
        repo_root = get_repo_root()
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    # Run parallel tests (default to release mode for performance)
    release_mode = not args.debug
    success = run_parallel_tests(args.workers, args.time_budget, repo_root, release_mode, args.per_file_timeout, args.rayon_threads)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
