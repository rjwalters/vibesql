#!/usr/bin/env python3
"""
Skip List Verification Tool

Tests whether skipped TCL tests can now pass.
For each skip entry, temporarily removes it and runs just that test.

Usage:
    ./scripts/verify_skips.py                    # Check all skips
    ./scripts/verify_skips.py --category GLOB    # Check GLOB-related skips
    ./scripts/verify_skips.py --test index-23.0  # Check specific test
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

# Repository root
REPO_ROOT = Path(__file__).parent.parent
SHIM_FILE = REPO_ROOT / "scripts" / "tester_vibesql.tcl"
TCL_TEST_DIR = REPO_ROOT / "docs" / "reference" / "sqlite" / "test"


def parse_skip_list(shim_content: str) -> dict:
    """Extract skip entries from the shim file."""
    skips = {}

    # Find the vibesql_skip_tests array
    match = re.search(r'array set vibesql_skip_tests \{(.*?)\}', shim_content, re.DOTALL)
    if not match:
        print("ERROR: Could not find vibesql_skip_tests in shim file")
        return skips

    skip_section = match.group(1)

    # Parse each skip entry: test_name "reason"
    pattern = r'^\s*([a-zA-Z0-9_.-]+)\s+"([^"]+)"'
    for line in skip_section.split('\n'):
        m = re.match(pattern, line.strip())
        if m:
            test_name = m.group(1)
            reason = m.group(2)
            skips[test_name] = reason

    return skips


def parse_partial_skip_files(shim_content: str) -> dict:
    """Extract partial-file documented-skip entries from the shim file.

    These are files (keyed by basename) where a documented subset of tests is
    auto-skipped by a regex detector while the rest of the file keeps running.
    They are recorded in the vibesql_partial_skip_files array purely for
    discoverability (audit-by-file-name); the actual skipping is done elsewhere,
    so we only report them here rather than attempting to unskip-and-rerun.
    """
    partial = {}

    match = re.search(
        r'array set vibesql_partial_skip_files \{(.*?)\n\}',
        shim_content,
        re.DOTALL,
    )
    if not match:
        # Not an error: the array is optional.
        return partial

    section = match.group(1)

    # Each entry: file_basename "reason" — reason may contain escaped chars but
    # no unescaped double-quote, matching the TCL array literal convention.
    pattern = r'^\s*([a-zA-Z0-9_.-]+)\s+"([^"]+)"'
    for line in section.split('\n'):
        m = re.match(pattern, line.strip())
        if m:
            partial[m.group(1)] = m.group(2)

    return partial


def get_test_file(test_name: str) -> str:
    """Determine which test file contains a test based on naming convention."""
    # Test names follow pattern: filename-N.N or filename-N.N.N
    # e.g., "index-23.0" -> "index.test"
    # e.g., "insert2-3.2" -> "insert2.test"
    match = re.match(r'^([a-zA-Z0-9]+)-', test_name)
    if match:
        base = match.group(1)
        # Handle numbered test files (e.g., "select1", "insert2")
        test_file = f"{base}.test"
        if (TCL_TEST_DIR / test_file).exists():
            return test_file
    return None


def run_single_test(test_name: str, test_file: str, temp_shim: Path) -> tuple:
    """
    Run a single test using a modified shim that doesn't skip it.
    Returns (passed: bool, output: str)
    """
    # Run the test file and grep for the specific test result
    cmd = [
        "tclsh",
        str(temp_shim),
        str(TCL_TEST_DIR / test_file)
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(REPO_ROOT)
        )
        output = result.stdout + result.stderr

        # Check if the test passed
        # Look for patterns like "index-23.0... Ok" or "FAIL: index-23.0"
        if re.search(rf'{re.escape(test_name)}.*Ok\b', output):
            return (True, output)
        elif re.search(rf'FAIL.*{re.escape(test_name)}|{re.escape(test_name)}.*FAIL', output):
            return (False, output)
        elif re.search(rf'{re.escape(test_name)}.*SKIP', output, re.IGNORECASE):
            return (None, output)  # Still skipped
        else:
            # Unknown result - look for any output about the test
            return (False, output)
    except subprocess.TimeoutExpired:
        return (False, "TIMEOUT")
    except Exception as e:
        return (False, str(e))


def create_modified_shim(original_content: str, tests_to_unskip: list) -> str:
    """Create a modified shim with certain tests removed from the skip list."""
    modified = original_content

    for test_name in tests_to_unskip:
        # Remove the skip entry for this test
        # Match: test_name "reason"
        pattern = rf'^\s*{re.escape(test_name)}\s+"[^"]+"\n?'
        modified = re.sub(pattern, '', modified, flags=re.MULTILINE)

    return modified


def categorize_skips(skips: dict) -> dict:
    """Categorize skips by reason type."""
    categories = {
        "GLOB": [],
        "TEMP_TABLE": [],
        "DB_CHANGES": [],
        "EXPLAIN": [],
        "CASCADE": [],
        "SQLITE_API": [],
        "EXPRESSION_INDEX": [],
        "OTHER": []
    }

    for test_name, reason in skips.items():
        reason_lower = reason.lower()

        if "glob" in reason_lower:
            categories["GLOB"].append(test_name)
        elif "temp" in reason_lower and "table" in reason_lower:
            categories["TEMP_TABLE"].append(test_name)
        elif "changes" in reason_lower or "total_changes" in reason_lower:
            categories["DB_CHANGES"].append(test_name)
        elif "explain" in reason_lower or "eqp" in reason_lower:
            categories["EXPLAIN"].append(test_name)
        elif "cascade" in reason_lower:
            categories["CASCADE"].append(test_name)
        elif "sqlite" in reason_lower and ("api" in reason_lower or "internal" in reason_lower or "stat" in reason_lower):
            categories["SQLITE_API"].append(test_name)
        elif "expression" in reason_lower and "index" in reason_lower:
            categories["EXPRESSION_INDEX"].append(test_name)
        else:
            categories["OTHER"].append(test_name)

    return categories


def verify_tests(tests_to_check: list, shim_content: str, verbose: bool = False) -> dict:
    """Verify a list of tests and return results."""
    results = {
        "passed": [],
        "failed": [],
        "still_skipped": [],
        "error": []
    }

    # Group tests by file for efficiency
    tests_by_file = {}
    for test_name in tests_to_check:
        test_file = get_test_file(test_name)
        if test_file:
            if test_file not in tests_by_file:
                tests_by_file[test_file] = []
            tests_by_file[test_file].append(test_name)
        else:
            results["error"].append((test_name, "Could not determine test file"))

    # Process each test file
    for test_file, tests in tests_by_file.items():
        print(f"\nTesting {len(tests)} skip(s) from {test_file}...")

        # Create modified shim with these tests unskipped
        modified_shim = create_modified_shim(shim_content, tests)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.tcl', delete=False) as f:
            f.write(modified_shim)
            temp_shim_path = Path(f.name)

        try:
            for test_name in tests:
                passed, output = run_single_test(test_name, test_file, temp_shim_path)

                if passed is True:
                    print(f"  OUTDATED SKIP: {test_name}")
                    results["passed"].append(test_name)
                elif passed is False:
                    if verbose:
                        print(f"  STILL FAILS: {test_name}")
                    results["failed"].append(test_name)
                else:
                    if verbose:
                        print(f"  STILL SKIPPED: {test_name}")
                    results["still_skipped"].append(test_name)
        finally:
            temp_shim_path.unlink()

    return results


def main():
    parser = argparse.ArgumentParser(description="Verify TCL test skip entries")
    parser.add_argument("--category", help="Check only skips in this category")
    parser.add_argument("--test", help="Check only this specific test")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--list-categories", action="store_true", help="List skip categories and counts")
    args = parser.parse_args()

    # Read the shim file
    with open(SHIM_FILE, 'r') as f:
        shim_content = f.read()

    # Parse skip list
    skips = parse_skip_list(shim_content)
    print(f"Found {len(skips)} skip entries")

    # Parse partial-file documented skips (discoverability records; not rerun-verified)
    partial_files = parse_partial_skip_files(shim_content)
    if partial_files:
        print(f"Found {len(partial_files)} partial-file documented skip(s)")

    # Categorize skips
    categories = categorize_skips(skips)

    if args.list_categories:
        print("\nSkip categories:")
        for cat, tests in sorted(categories.items(), key=lambda x: -len(x[1])):
            print(f"  {cat}: {len(tests)} tests")
        if partial_files:
            print(f"  PARTIAL_FILE: {len(partial_files)} files (documented per-test/regex subset skips)")
            for fname, reason in sorted(partial_files.items()):
                # Show a truncated reason so the record is auditable at a glance.
                summary = reason if len(reason) <= 100 else reason[:97] + "..."
                print(f"    - {fname}: {summary}")
        return 0

    # Determine which tests to check
    tests_to_check = []

    if args.test:
        if args.test in skips:
            tests_to_check = [args.test]
        else:
            print(f"ERROR: Test '{args.test}' not found in skip list")
            return 1
    elif args.category:
        cat_upper = args.category.upper()
        if cat_upper in categories:
            tests_to_check = categories[cat_upper]
        else:
            print(f"ERROR: Category '{args.category}' not found")
            print(f"Available categories: {', '.join(categories.keys())}")
            return 1
    else:
        # Check all skips - but start with likely candidates
        # Priority: GLOB, DB_CHANGES, TEMP_TABLE
        tests_to_check = (
            categories["GLOB"] +
            categories["DB_CHANGES"] +
            categories["TEMP_TABLE"]
        )

    if not tests_to_check:
        print("No tests to check")
        return 0

    print(f"\nChecking {len(tests_to_check)} skip entries...")

    # Verify tests
    results = verify_tests(tests_to_check, shim_content, verbose=args.verbose)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Outdated skips (tests now pass): {len(results['passed'])}")
    print(f"Still failing:                   {len(results['failed'])}")
    print(f"Still skipped:                   {len(results['still_skipped'])}")
    print(f"Errors:                          {len(results['error'])}")

    if results['passed']:
        print("\nOUTDATED SKIPS TO REMOVE:")
        for test in results['passed']:
            print(f"  - {test}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
