#!/usr/bin/env python3
"""
Export TCL test results to website data format.

Reads test results from the TCL test database and exports conformance data
for the web demo.

Usage:
    python scripts/export_tcl_results.py
    python scripts/export_tcl_results.py --output web-demo/public/conformance/
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


# Default paths
RESULTS_DB = os.path.expanduser("~/.vibesql/test_results/tcl_test_results.vbsql")
VIBESQL_BIN = "./target/release/vibesql"


def get_repo_root() -> Path:
    """Get the repository root directory."""
    return Path(__file__).parent.parent


def query_db(sql: str, db_path: str = RESULTS_DB) -> List[str]:
    """Query the database and return results as lines."""
    try:
        result = subprocess.run(
            [VIBESQL_BIN, db_path, "-c", sql],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            return []
        # Skip header lines and return data
        # Table format:
        # +--------+-----+
        # | col1   | col2|  <-- Header row (skip)
        # +--------+-----+
        # | val1   | val2|  <-- Data rows
        # +--------+-----+
        lines = result.stdout.strip().split('\n')
        data_lines = []
        separator_count = 0
        for line in lines:
            if line.startswith('+'):
                separator_count += 1
                if separator_count >= 3:
                    break  # End of data (closing border)
                continue
            if separator_count >= 2 and line.startswith('|'):
                # After 2nd separator, we're in data rows
                parts = [p.strip() for p in line.split('|')[1:-1]]
                data_lines.append(parts)
        return data_lines
    except Exception as e:
        print(f"Error querying database: {e}", file=sys.stderr)
        return []


def get_latest_run() -> Optional[Dict]:
    """Get the latest test run info."""
    rows = query_db("""
        SELECT run_id, started_at, completed_at, git_commit,
               total_files, total_tests, passed, failed, skipped, parse_errors
        FROM tcl_test_runs
        ORDER BY run_id DESC
        LIMIT 1
    """)

    if not rows:
        return None

    row = rows[0]
    return {
        "run_id": int(row[0]) if row[0] else 0,
        "started_at": row[1],
        "completed_at": row[2],
        "git_commit": row[3],
        "total_files": int(row[4]) if row[4] else 0,
        "total_tests": int(row[5]) if row[5] else 0,
        "passed": int(row[6]) if row[6] else 0,
        "failed": int(row[7]) if row[7] else 0,
        "skipped": int(row[8]) if row[8] else 0,
        "parse_errors": int(row[9]) if row[9] else 0,
    }


def get_results_by_file(run_id: int) -> List[Dict]:
    """Get test results grouped by file."""
    rows = query_db(f"""
        SELECT file_path,
               COUNT(*) as total,
               SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) as passed,
               SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
               SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) as skipped
        FROM tcl_test_results
        WHERE run_id = {run_id}
        GROUP BY file_path
        ORDER BY file_path
    """)

    return [
        {
            "file": row[0].split('/')[-1] if '/' in row[0] else row[0],
            "path": row[0],
            "total": int(row[1]) if row[1] else 0,
            "passed": int(row[2]) if row[2] else 0,
            "failed": int(row[3]) if row[3] else 0,
            "skipped": int(row[4]) if row[4] else 0,
        }
        for row in rows
    ]


def get_results_by_category(run_id: int) -> Dict[str, Dict]:
    """Get test results grouped by category (test file prefix)."""
    rows = query_db(f"""
        SELECT file_path,
               status,
               COUNT(*) as count
        FROM tcl_test_results
        WHERE run_id = {run_id}
        GROUP BY file_path, status
    """)

    # Group by category (file name prefix like 'select', 'where', etc.)
    categories: Dict[str, Dict] = {}

    for row in rows:
        file_path = row[0]
        status = row[1]
        count = int(row[2]) if row[2] else 0

        # Extract category from filename
        filename = file_path.split('/')[-1] if '/' in file_path else file_path
        # Remove .test extension and trailing numbers
        import re
        category = re.sub(r'\d*\.test$', '', filename)
        if not category:
            category = "other"

        if category not in categories:
            categories[category] = {
                "total": 0,
                "passed": 0,
                "failed": 0,
                "skipped": 0,
            }

        categories[category]["total"] += count
        if status == "passed":
            categories[category]["passed"] += count
        elif status == "failed":
            categories[category]["failed"] += count
        elif status == "skipped":
            categories[category]["skipped"] += count

    # Calculate pass rates
    for cat in categories.values():
        testable = cat["total"] - cat["skipped"]
        cat["pass_rate"] = round(cat["passed"] / testable * 100, 1) if testable > 0 else 0.0

    return categories


def get_failure_patterns(run_id: int, limit: int = 20) -> List[Dict]:
    """Get common failure patterns."""
    rows = query_db(f"""
        SELECT error_message,
               COUNT(*) as count
        FROM tcl_test_results
        WHERE run_id = {run_id}
        AND status = 'failed'
        AND error_message IS NOT NULL
        GROUP BY error_message
        ORDER BY count DESC
        LIMIT {limit}
    """)

    return [
        {
            "error": row[0][:100] if row[0] else "Unknown",
            "count": int(row[1]) if row[1] else 0,
        }
        for row in rows
    ]


def get_run_history(limit: int = 10) -> List[Dict]:
    """Get recent test run history."""
    rows = query_db(f"""
        SELECT run_id, started_at, git_commit,
               total_tests, passed, failed, skipped
        FROM tcl_test_runs
        ORDER BY run_id DESC
        LIMIT {limit}
    """)

    return [
        {
            "run_id": int(row[0]) if row[0] else 0,
            "date": row[1],
            "commit": row[2],
            "total": int(row[3]) if row[3] else 0,
            "passed": int(row[4]) if row[4] else 0,
            "failed": int(row[5]) if row[5] else 0,
            "skipped": int(row[6]) if row[6] else 0,
            "pass_rate": round(int(row[4]) / (int(row[3]) - int(row[6])) * 100, 1)
                if row[3] and row[6] and int(row[3]) > int(row[6]) else 0.0,
        }
        for row in rows
    ]


def export_tcl_results(output_dir: Optional[Path] = None, verbose: bool = False) -> Dict:
    """Export TCL test results to JSON format."""
    if output_dir is None:
        output_dir = get_repo_root() / "web-demo" / "public" / "conformance"

    output_dir.mkdir(parents=True, exist_ok=True)

    # Check if database exists
    if not os.path.exists(RESULTS_DB):
        print(f"TCL results database not found: {RESULTS_DB}", file=sys.stderr)
        print("Run: make test-tcl to generate test results", file=sys.stderr)
        return {}

    # Get latest run
    latest_run = get_latest_run()
    if not latest_run:
        print("No test runs found in database", file=sys.stderr)
        return {}

    run_id = latest_run["run_id"]

    if verbose:
        print(f"Exporting run {run_id} from {latest_run['started_at']}")

    # Build export data
    export_data = {
        "version": "1.0",
        "generated_at": datetime.now().isoformat(),
        "latest_run": latest_run,
        "summary": {
            "total_tests": latest_run["total_tests"],
            "passed": latest_run["passed"],
            "failed": latest_run["failed"],
            "skipped": latest_run["skipped"],
            "pass_rate": round(latest_run["passed"] / (latest_run["total_tests"] - latest_run["skipped"]) * 100, 1)
                if latest_run["total_tests"] > latest_run["skipped"] else 0.0,
        },
        "categories": get_results_by_category(run_id),
        "by_file": get_results_by_file(run_id),
        "failure_patterns": get_failure_patterns(run_id),
        "history": get_run_history(),
    }

    # Write JSON file
    output_file = output_dir / "tcl_results.json"
    with open(output_file, 'w') as f:
        json.dump(export_data, f, indent=2)

    if verbose:
        print(f"Exported to: {output_file}")
        print(f"  Pass rate: {export_data['summary']['pass_rate']}%")
        print(f"  Categories: {len(export_data['categories'])}")

    # Write badge JSON for shields.io
    badge_dir = get_repo_root() / "web-demo" / "public" / "badges"
    badge_dir.mkdir(parents=True, exist_ok=True)

    pass_rate = export_data['summary']['pass_rate']
    if pass_rate >= 90:
        color = "brightgreen"
    elif pass_rate >= 70:
        color = "green"
    elif pass_rate >= 50:
        color = "yellow"
    else:
        color = "red"

    badge = {
        "schemaVersion": 1,
        "label": "TCL Tests",
        "message": f"{pass_rate:.1f}%",
        "color": color
    }

    badge_file = badge_dir / "tcl-tests.json"
    with open(badge_file, 'w') as f:
        json.dump(badge, f, indent=2)

    if verbose:
        print(f"Badge written to: {badge_file}")

    return export_data


def main():
    parser = argparse.ArgumentParser(description="Export TCL test results to website format")
    parser.add_argument("--output", "-o", help="Output directory", default=None)
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    output_dir = Path(args.output) if args.output else None
    result = export_tcl_results(output_dir, args.verbose)

    if not result:
        return 1

    print(f"TCL test results exported: {result['summary']['pass_rate']}% pass rate")
    return 0


if __name__ == "__main__":
    sys.exit(main())
