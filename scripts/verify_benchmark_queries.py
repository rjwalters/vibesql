#!/usr/bin/env python3
"""
Verify TPC-H and TPC-DS benchmark queries against known checksums.

This script extracts SQL queries from our Rust source files and computes
SHA-256 hashes to ensure they haven't been accidentally modified from
the official benchmark specifications.

Usage:
    ./scripts/verify_benchmark_queries.py          # Verify queries
    ./scripts/verify_benchmark_queries.py --update # Update checksums after intentional changes
"""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parent
CHECKSUMS_FILE = SCRIPT_DIR / "benchmark_query_checksums.json"

TPCH_QUERIES_FILE = REPO_ROOT / "crates/vibesql-executor/benches/tpch/queries.rs"
TPCDS_QUERIES_FILE = REPO_ROOT / "crates/vibesql-executor/benches/tpcds/queries.rs"


def extract_queries_from_rust(file_path: Path, prefix: str) -> dict[str, str]:
    """Extract SQL queries from Rust const definitions.

    Looks for patterns like:
        pub const TPCH_Q1: &str = r#"..."#;
    """
    content = file_path.read_text()

    # Pattern to match: pub const PREFIX_QN: &str = r#"..."#;
    # Using non-greedy match and accounting for nested raw strings
    pattern = rf'pub const ({prefix}_Q\d+): &str = r#"(.*?)"#;'

    queries = {}
    for match in re.finditer(pattern, content, re.DOTALL):
        name = match.group(1)
        sql = match.group(2).strip()
        queries[name] = sql

    return queries


def normalize_sql(sql: str) -> str:
    """Normalize SQL for consistent hashing.

    - Strip leading/trailing whitespace
    - Normalize internal whitespace (multiple spaces/newlines to single space)
    - Convert to lowercase for case-insensitive comparison
    """
    # Strip and normalize whitespace
    normalized = ' '.join(sql.split())
    # Lowercase for case-insensitive comparison
    normalized = normalized.lower()
    return normalized


def compute_hash(sql: str) -> str:
    """Compute SHA-256 hash of normalized SQL."""
    normalized = normalize_sql(sql)
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:16]


def load_checksums() -> dict:
    """Load existing checksums from file."""
    if CHECKSUMS_FILE.exists():
        return json.loads(CHECKSUMS_FILE.read_text())
    return {}


def save_checksums(checksums: dict):
    """Save checksums to file."""
    CHECKSUMS_FILE.write_text(json.dumps(checksums, indent=2, sort_keys=True) + "\n")


def verify_queries(update_mode: bool = False) -> bool:
    """Verify all benchmark queries against stored checksums.

    Returns True if all queries match, False otherwise.
    """
    # Extract queries
    tpch_queries = extract_queries_from_rust(TPCH_QUERIES_FILE, "TPCH")
    tpcds_queries = extract_queries_from_rust(TPCDS_QUERIES_FILE, "TPCDS")

    print(f"Found {len(tpch_queries)} TPC-H queries")
    print(f"Found {len(tpcds_queries)} TPC-DS queries")
    print()

    # Compute current hashes
    current_checksums = {
        "tpch": {name: compute_hash(sql) for name, sql in sorted(tpch_queries.items())},
        "tpcds": {name: compute_hash(sql) for name, sql in sorted(tpcds_queries.items())},
    }

    if update_mode:
        save_checksums(current_checksums)
        print(f"Updated checksums saved to {CHECKSUMS_FILE}")
        return True

    # Load stored checksums
    stored_checksums = load_checksums()

    if not stored_checksums:
        print("ERROR: No stored checksums found.")
        print("Run with --update to generate initial checksums.")
        return False

    # Compare
    all_match = True

    for suite in ["tpch", "tpcds"]:
        suite_name = suite.upper()
        current = current_checksums.get(suite, {})
        stored = stored_checksums.get(suite, {})

        # Check for missing queries
        missing = set(stored.keys()) - set(current.keys())
        if missing:
            print(f"ERROR: Missing {suite_name} queries: {sorted(missing)}")
            all_match = False

        # Check for new queries
        new = set(current.keys()) - set(stored.keys())
        if new:
            print(f"WARNING: New {suite_name} queries not in checksums: {sorted(new)}")
            all_match = False

        # Check for modified queries
        modified = []
        for name in sorted(set(current.keys()) & set(stored.keys())):
            if current[name] != stored[name]:
                modified.append(name)

        if modified:
            print(f"ERROR: Modified {suite_name} queries detected:")
            for name in modified:
                print(f"  - {name}: expected {stored[name]}, got {current[name]}")
            all_match = False

    if all_match:
        total = len(current_checksums.get("tpch", {})) + len(current_checksums.get("tpcds", {}))
        print(f"All {total} benchmark queries verified successfully.")

    return all_match


def main():
    parser = argparse.ArgumentParser(
        description="Verify TPC-H and TPC-DS benchmark queries against known checksums"
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update stored checksums (use after intentional query changes)"
    )
    args = parser.parse_args()

    success = verify_queries(update_mode=args.update)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
