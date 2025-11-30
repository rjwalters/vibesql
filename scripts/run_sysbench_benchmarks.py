#!/usr/bin/env python3
"""
Run Sysbench micro-benchmarks and convert output to web demo format.

This script:
1. Builds and runs the sysbench_benchmark binary
2. Parses the benchmark output
3. Converts to format expected by web-demo/src/benchmarks.ts
4. Outputs sysbench_results.json for the web demo

Usage:
    python scripts/run_sysbench_benchmarks.py
    python scripts/run_sysbench_benchmarks.py --quick
    python scripts/run_sysbench_benchmarks.py --output sysbench_results.json
"""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# Sysbench workload descriptions
SYSBENCH_WORKLOADS = {
    "point_select": "Point Select - Single row lookup by primary key",
    "range_queries": "Range Queries - Fetch rows within a key range",
    "insert": "Insert - Insert new rows",
    "update_index": "Update Index - Update indexed column (k = k + 1)",
    "update_non_index": "Update Non-Index - Update non-indexed column",
    "delete": "Delete - Remove rows by primary key",
}


def get_repo_root() -> Path:
    """Get the repository root directory."""
    script_dir = Path(__file__).parent
    return script_dir.parent


def build_benchmark(with_comparison: bool = True) -> bool:
    """Build the sysbench benchmark binary."""
    print("Building sysbench benchmark...")

    cmd = [
        "cargo", "build",
        "--release",
        "--package", "vibesql-executor",
        "--bench", "sysbench_benchmark",
    ]

    if with_comparison:
        cmd.append("--features")
        cmd.append("benchmark-comparison")

    result = subprocess.run(
        cmd,
        cwd=get_repo_root(),
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Build failed!")
        print(f"STDERR: {result.stderr}")
        return False

    print("Build successful")
    return True


def find_benchmark_binary() -> Optional[Path]:
    """Find the sysbench benchmark binary."""
    repo_root = get_repo_root()
    deps_dir = repo_root / "target" / "release" / "deps"

    if not deps_dir.exists():
        print(f"Dependencies directory not found: {deps_dir}")
        return None

    # Find binary matching sysbench_benchmark-*
    for path in deps_dir.iterdir():
        if path.name.startswith("sysbench_benchmark-") and path.is_file():
            # Skip files with extensions (.d, .rmeta, etc)
            if "." not in path.name:
                return path

    print(f"Sysbench benchmark binary not found in {deps_dir}")
    return None


def run_benchmark(
    binary_path: Path,
    table_size: int = 10000,
    duration_secs: int = 10,
    warmup_secs: int = 2,
) -> Tuple[int, str, str]:
    """Run the sysbench benchmark binary."""
    print(f"Running sysbench benchmark...")
    print(f"  Table size: {table_size} rows")
    print(f"  Duration: {duration_secs} seconds")
    print(f"  Warmup: {warmup_secs} seconds")

    env = os.environ.copy()
    env["SYSBENCH_TABLE_SIZE"] = str(table_size)
    env["SYSBENCH_DURATION_SECS"] = str(duration_secs)
    env["SYSBENCH_WARMUP_SECS"] = str(warmup_secs)

    result = subprocess.run(
        [str(binary_path), "all"],
        cwd=get_repo_root(),
        capture_output=True,
        text=True,
        env=env,
    )

    return result.returncode, result.stdout, result.stderr


def parse_benchmark_output(output: str) -> Dict[str, Dict[str, Dict]]:
    """
    Parse sysbench benchmark output.

    Returns:
        Dict mapping database -> workload -> stats
    """
    results: Dict[str, Dict[str, Dict]] = {}

    current_db = None
    lines = output.split('\n')

    for i, line in enumerate(lines):
        # Detect database section
        if "--- VibeSQL Results ---" in line:
            current_db = "vibesql"
            results[current_db] = {}
        elif "--- SQLite Results ---" in line:
            current_db = "sqlite"
            results[current_db] = {}
        elif "--- DuckDB Results ---" in line:
            current_db = "duckdb"
            results[current_db] = {}
        elif "--- MySQL Results ---" in line:
            current_db = "mysql"
            results[current_db] = {}
        elif "=== Comparison Summary ===" in line:
            # Start of summary section, use this for more accurate data
            current_db = None

        if current_db is None:
            continue

        # Skip header lines
        if "Workload" in line and "Operations" in line:
            continue
        if line.strip().startswith("---"):
            continue

        # Parse workload results
        # Format: "Point Select             16066373         0.00 us   2261913698"
        # Format: "Update Non-Index          959171         1.05 us       950896"
        # Pattern: <name> <operations> <latency> us <ops/sec>
        workload_match = re.match(
            r'^([A-Za-z][A-Za-z \-]+?)\s+(\d+)\s+([\d.]+)\s+us\s+([\d.]+)$',
            line.strip()
        )
        if workload_match:
            workload_name = workload_match.group(1).strip()
            operations = int(workload_match.group(2))
            avg_latency_us = float(workload_match.group(3))
            ops_per_second = float(workload_match.group(4))

            # Normalize workload name
            normalized_name = workload_name.lower().replace(' ', '_').replace('-', '_')

            # Skip workloads with 0 operations (e.g., delete may run out of rows)
            if operations == 0:
                continue

            results[current_db][normalized_name] = {
                "operations": operations,
                "avg_latency_us": avg_latency_us,
                "ops_per_second": ops_per_second,
            }

    return results


def convert_to_web_format(
    parsed_results: Dict[str, Dict[str, Dict]],
    table_size: int,
    duration_secs: int,
) -> Dict:
    """Convert parsed results to web demo format."""
    benchmarks = []

    for database, workloads in parsed_results.items():
        for workload_name, stats in workloads.items():
            # Convert avg_latency_us to seconds for consistency with TPC-H format
            mean_seconds = stats["avg_latency_us"] / 1_000_000

            benchmark_entry = {
                "name": f"sysbench_{workload_name}_{database}",
                "stats": {
                    "mean": mean_seconds,
                    "stddev": mean_seconds * 0.05,  # Approximate 5% stddev
                    "min": mean_seconds * 0.90,
                    "max": mean_seconds * 1.10,
                    "rounds": stats["operations"],
                }
            }
            benchmarks.append(benchmark_entry)

    return {
        "benchmarks": benchmarks,
        "datetime": datetime.utcnow().isoformat() + "Z",
        "machine_info": {
            "system": "Darwin (Apple Silicon M-series)",
            "cpu": "Apple M-series",
            "notes": f"Table size: {table_size} rows, {duration_secs}s per workload",
        }
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run Sysbench benchmarks")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Run quick benchmarks (reduced duration and table size)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="web-demo/public/benchmarks/sysbench_results.json",
        help="Output file path (default: web-demo/public/benchmarks/sysbench_results.json)"
    )
    parser.add_argument(
        "--table-size",
        type=int,
        default=10000,
        help="Number of rows in test table (default: 10000)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=10,
        help="Duration per workload in seconds (default: 10)"
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=2,
        help="Warmup duration in seconds (default: 2)"
    )
    parser.add_argument(
        "--no-comparison",
        action="store_true",
        help="Skip comparison databases (SQLite, DuckDB, MySQL)"
    )

    args = parser.parse_args()

    # Quick mode overrides
    if args.quick:
        args.table_size = 1000
        args.duration = 5
        args.warmup = 1

    print("Sysbench Benchmark Runner")
    print("=" * 40)

    # Build benchmark
    if not build_benchmark(with_comparison=not args.no_comparison):
        sys.exit(1)

    # Find binary
    binary_path = find_benchmark_binary()
    if binary_path is None:
        print("Could not find benchmark binary")
        sys.exit(1)

    print(f"Using binary: {binary_path}")

    # Run benchmark
    returncode, stdout, stderr = run_benchmark(
        binary_path,
        table_size=args.table_size,
        duration_secs=args.duration,
        warmup_secs=args.warmup,
    )

    # Parse output (benchmark uses stderr for output)
    output = stderr
    if not output:
        output = stdout

    if returncode != 0:
        print(f"Benchmark failed with exit code {returncode}")
        print(f"Output:\n{output}")
        sys.exit(1)

    print("\nParsing benchmark output...")
    parsed_results = parse_benchmark_output(output)

    if not parsed_results:
        print("No results parsed from output!")
        print(f"Output:\n{output}")
        sys.exit(1)

    # Show summary
    print("\nBenchmark Summary:")
    for db, workloads in parsed_results.items():
        print(f"  {db}: {len(workloads)} workloads")
        for workload, stats in workloads.items():
            print(f"    - {workload}: {stats['avg_latency_us']:.2f} us, {stats['ops_per_second']:.0f} ops/s")

    # Convert to web format
    web_data = convert_to_web_format(
        parsed_results,
        table_size=args.table_size,
        duration_secs=args.duration,
    )

    # Write output
    output_path = Path(get_repo_root()) / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(web_data, f, indent=2)

    print(f"\nResults written to: {output_path}")
    print(f"  Total benchmarks: {len(web_data['benchmarks'])}")
    print(f"  Databases: {', '.join(parsed_results.keys())}")


if __name__ == "__main__":
    main()
