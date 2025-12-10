#!/usr/bin/env python3
"""
TCL Test Regression Checker for CI

Compares current TCL test pass rate against a stored baseline and fails CI
if there's a regression beyond the allowed threshold.

Usage:
    python3 scripts/check_tcl_regression.py [output_file]

    If output_file is provided, parses pass rate from tcltest output.
    Otherwise, queries the results database directly.

Exit codes:
    0 - Pass rate is at or above baseline (or improved)
    1 - Regression detected (pass rate dropped beyond threshold)
    2 - Error (couldn't read baseline or results)
"""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path


# Default paths
REPO_ROOT = Path(__file__).parent.parent
BASELINE_FILE = REPO_ROOT / ".github" / "tcl_baseline.json"
RESULTS_DB = Path.home() / ".vibesql" / "test_results" / "tcl_test_results.vbsql"
VIBESQL_BIN = REPO_ROOT / "target" / "release" / "vibesql"

# Regression threshold (percentage points)
REGRESSION_THRESHOLD = 2.0


def load_baseline(baseline_path: Path = None) -> dict:
    """Load the baseline from the JSON file."""
    if baseline_path is None:
        baseline_path = BASELINE_FILE
    if not baseline_path.exists():
        print(f"Warning: Baseline file not found: {baseline_path}")
        print("Creating default baseline with 0% pass rate")
        return {
            "priority1_pass_rate": 0.0,
            "total_tests": 0,
            "passed": 0,
            "updated_at": "initial",
            "git_commit": "",
            "note": "Default baseline - no previous data"
        }

    with open(baseline_path) as f:
        return json.load(f)


def parse_output_file(output_file: str) -> dict:
    """Parse pass rate from tcltest output file."""
    with open(output_file) as f:
        content = f.read()

    # Look for the summary output pattern
    # Total tests: N
    # Passed:      N (X.X%)
    total_match = re.search(r'Total tests:\s*(\d+)', content)
    passed_match = re.search(r'Passed:\s*(\d+)\s*\((\d+\.?\d*)%\)', content)

    if not total_match or not passed_match:
        # Try alternative format
        total_match = re.search(r'total_tests["\s:]+(\d+)', content)
        passed_match = re.search(r'passed["\s:]+(\d+)', content)

        if total_match and passed_match:
            total = int(total_match.group(1))
            passed = int(passed_match.group(1))
            pass_rate = (passed / total * 100) if total > 0 else 0.0
            return {
                "total_tests": total,
                "passed": passed,
                "pass_rate": pass_rate
            }

        return None

    return {
        "total_tests": int(total_match.group(1)),
        "passed": int(passed_match.group(1)),
        "pass_rate": float(passed_match.group(2))
    }


def query_results_db() -> dict:
    """Query the latest results from the database."""
    if not RESULTS_DB.exists():
        return None

    if not VIBESQL_BIN.exists():
        print(f"Error: VibeSQL binary not found: {VIBESQL_BIN}")
        return None

    query = """
        SELECT total_tests, passed,
               ROUND(100.0 * passed / NULLIF(total_tests, 0), 2) as pass_rate
        FROM tcl_test_runs
        ORDER BY run_id DESC
        LIMIT 1
    """

    try:
        result = subprocess.run(
            [str(VIBESQL_BIN), str(RESULTS_DB), "-c", query, "--format", "raw"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            return None

        # Parse output (raw format: value per line, separated by |)
        lines = [l.strip() for l in result.stdout.strip().split('\n') if l.strip()]
        if not lines:
            return None

        # Skip header line if present
        data_line = lines[-1]
        parts = [p.strip() for p in data_line.split('|')]

        if len(parts) >= 3:
            return {
                "total_tests": int(parts[0]),
                "passed": int(parts[1]),
                "pass_rate": float(parts[2]) if parts[2] else 0.0
            }
    except Exception as e:
        print(f"Error querying database: {e}")

    return None


def check_commit_for_baseline_update() -> bool:
    """Check if the current commit message subject contains [tcl-baseline] flag.

    Only checks the subject line (first line) to avoid false positives from
    documentation text in the commit body that mentions the flag.
    """
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%s"],  # %s = subject line only
            capture_output=True,
            text=True,
            timeout=5
        )
        return "[tcl-baseline]" in result.stdout.lower()
    except:
        return False


def main():
    parser = argparse.ArgumentParser(description="Check TCL test pass rate for regression")
    parser.add_argument("output_file", nargs="?", help="Optional tcltest output file to parse")
    parser.add_argument("--threshold", type=float, default=REGRESSION_THRESHOLD,
                        help=f"Regression threshold in percentage points (default: {REGRESSION_THRESHOLD})")
    parser.add_argument("--baseline", type=str, help="Path to baseline JSON file")
    parser.add_argument("--update-baseline", action="store_true",
                        help="Update baseline with current results (for CI after improvements)")
    args = parser.parse_args()

    # Load baseline
    baseline_path = Path(args.baseline) if args.baseline else BASELINE_FILE

    baseline = load_baseline(baseline_path)
    baseline_pass_rate = baseline.get("priority1_pass_rate", 0.0)

    print(f"TCL Test Regression Check")
    print(f"=" * 50)
    print(f"Baseline pass rate: {baseline_pass_rate:.1f}%")
    print(f"Regression threshold: {args.threshold}% points")
    print()

    # Get current results
    if args.output_file:
        current = parse_output_file(args.output_file)
        if not current:
            print(f"Error: Could not parse results from {args.output_file}")
            sys.exit(2)
    else:
        current = query_results_db()
        if not current:
            print("No test results found. Run TCL tests first:")
            print("  ./scripts/tcltest run --priority 1")
            sys.exit(2)

    current_pass_rate = current["pass_rate"]
    total_tests = current["total_tests"]
    passed = current["passed"]

    print(f"Current results:")
    print(f"  Total tests: {total_tests}")
    print(f"  Passed: {passed}")
    print(f"  Pass rate: {current_pass_rate:.1f}%")
    print()

    # Check for baseline update request
    if args.update_baseline or check_commit_for_baseline_update():
        print("Baseline update requested!")

        # Get git commit
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                timeout=5
            )
            git_commit = result.stdout.strip()
        except:
            git_commit = ""

        new_baseline = {
            "priority1_pass_rate": current_pass_rate,
            "total_tests": total_tests,
            "passed": passed,
            "updated_at": subprocess.run(
                ["date", "-u", "+%Y-%m-%d"],
                capture_output=True,
                text=True
            ).stdout.strip(),
            "git_commit": git_commit,
            "note": "Updated via CI"
        }

        # Ensure directory exists
        baseline_path.parent.mkdir(parents=True, exist_ok=True)

        with open(baseline_path, 'w') as f:
            json.dump(new_baseline, f, indent=2)
            f.write('\n')

        print(f"Baseline updated to {current_pass_rate:.1f}%")
        print(f"Saved to: {baseline_path}")
        sys.exit(0)

    # Calculate regression
    regression = baseline_pass_rate - current_pass_rate

    if regression > args.threshold:
        print(f"REGRESSION DETECTED!")
        print(f"  Pass rate dropped by {regression:.1f} percentage points")
        print(f"  (threshold: {args.threshold}% points)")
        print()
        print("To update the baseline after intentional changes, either:")
        print("  1. Add [tcl-baseline] to your commit message")
        print("  2. Run: python3 scripts/check_tcl_regression.py --update-baseline")
        sys.exit(1)
    elif regression > 0:
        print(f"Minor decrease: {regression:.1f} percentage points (within threshold)")
        print("PASS")
        sys.exit(0)
    elif current_pass_rate > baseline_pass_rate:
        improvement = current_pass_rate - baseline_pass_rate
        print(f"IMPROVEMENT: +{improvement:.1f} percentage points!")
        print("Consider updating the baseline to lock in this improvement.")
        print("PASS")
        sys.exit(0)
    else:
        print("No change from baseline.")
        print("PASS")
        sys.exit(0)


if __name__ == "__main__":
    main()
