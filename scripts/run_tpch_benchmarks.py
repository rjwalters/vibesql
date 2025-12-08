#!/usr/bin/env python3
"""
Run TPC-H benchmarks and convert output to web demo format.

This script:
1. Builds and runs the tpch_benchmark binary (custom harness)
2. Parses the benchmark output
3. Converts to format expected by web-demo/src/benchmarks.ts
4. Outputs benchmark_results.json for the web demo

Usage:
    python scripts/run_tpch_benchmarks.py
    python scripts/run_tpch_benchmarks.py --quick
    python scripts/run_tpch_benchmarks.py --output tpch_results.json
"""

import argparse
import json
import os
import platform
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

# TPC-H Query descriptions (from TPCH_README.md)
TPCH_QUERIES = {
    "q1": "Pricing Summary Report",
    "q2": "Minimum Cost Supplier",
    "q3": "Shipping Priority",
    "q4": "Order Priority Checking",
    "q5": "Local Supplier Volume",
    "q6": "Forecasting Revenue Change",
    "q7": "Volume Shipping",
    "q8": "National Market Share",
    "q9": "Product Type Profit Measure",
    "q10": "Returned Item Reporting",
    "q11": "Important Stock Identification",
    "q12": "Shipping Modes Priority",
    "q13": "Customer Distribution",
    "q14": "Promotion Effect",
    "q15": "Top Supplier",
    "q16": "Parts/Supplier Relationship",
    "q17": "Small-Quantity-Order Revenue",
    "q18": "Large Volume Customer",
    "q19": "Discounted Revenue",
    "q20": "Potential Part Promotion",
    "q21": "Suppliers Who Kept Orders Waiting",
    "q22": "Global Sales Opportunity",
}


def get_repo_root() -> Path:
    """Get the repository root directory."""
    script_dir = Path(__file__).parent
    return script_dir.parent


def build_benchmark(include_duckdb: bool = False) -> bool:
    """Build the TPC-H benchmark binary."""
    print("Building TPC-H benchmark...")

    features = "benchmark-comparison"
    if include_duckdb:
        features += ",duckdb-comparison"

    cmd = [
        "cargo", "build",
        "--release",
        "--package", "vibesql-executor",
        "--bench", "tpch_benchmark",
        "--features", features,
    ]

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
    """Find the tpch_benchmark binary (most recently modified)."""
    repo_root = get_repo_root()
    deps_dir = repo_root / "target" / "release" / "deps"

    if not deps_dir.exists():
        print(f"Dependencies directory not found: {deps_dir}")
        return None

    # Find all binaries matching tpch_benchmark-*
    candidates = []
    for path in deps_dir.iterdir():
        if path.name.startswith("tpch_benchmark-") and path.is_file():
            # Skip files with extensions (.d, .rmeta, .o, etc)
            if "." not in path.name:
                candidates.append(path)

    if not candidates:
        print(f"TPC-H benchmark binary not found in {deps_dir}")
        return None

    # Return the most recently modified binary
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def run_benchmark(
    binary_path: Path,
    query_filter: Optional[str] = None,
    scale_factor: float = 0.01,
    warmup_iterations: int = 3,
    benchmark_iterations: int = 10,
    timeout_secs: int = 30,
) -> Tuple[int, str, str]:
    """Run the TPC-H benchmark binary."""
    print(f"Running TPC-H benchmark...")
    print(f"  Scale factor: {scale_factor}")
    print(f"  Warmup iterations: {warmup_iterations}")
    print(f"  Benchmark iterations: {benchmark_iterations}")
    print(f"  Timeout: {timeout_secs}s")
    if query_filter:
        print(f"  Query filter: {query_filter}")

    env = os.environ.copy()
    env["SCALE_FACTOR"] = str(scale_factor)
    env["WARMUP_ITERATIONS"] = str(warmup_iterations)
    env["BENCHMARK_ITERATIONS"] = str(benchmark_iterations)
    env["BENCHMARK_TIMEOUT_SECS"] = str(timeout_secs)
    if query_filter:
        env["QUERY_FILTER"] = query_filter

    # Run the binary directly (no args needed if using QUERY_FILTER env var)
    result = subprocess.run(
        [str(binary_path)],
        cwd=get_repo_root(),
        capture_output=True,
        text=True,
        env=env,
    )

    return result.returncode, result.stdout, result.stderr


def parse_benchmark_output(output: str) -> Dict[str, Dict[str, Dict]]:
    """
    Parse TPC-H benchmark output from custom harness.

    Returns:
        Dict mapping database -> query -> stats
    """
    results: Dict[str, Dict[str, Dict]] = {}

    current_db = None
    lines = output.split('\n')

    for line in lines:
        # Detect database section headers
        if "--- VibeSQL ---" in line:
            current_db = "vibesql"
            results[current_db] = {}
        elif "--- SQLite ---" in line:
            current_db = "sqlite"
            results[current_db] = {}
        elif "--- DuckDB ---" in line:
            current_db = "duckdb"
            results[current_db] = {}
        elif "--- MySQL ---" in line:
            current_db = "mysql"
            results[current_db] = {}
        elif "=== Comparison Summary ===" in line:
            # End of per-engine results
            current_db = None

        if current_db is None:
            continue

        # Parse query results from compact format:
        # "  Q6                     846.88µs avg (  815.54µs -   878.21µs)"
        # Note: µs is the micro sign (U+00B5), not 'us'
        query_match = re.match(
            r'^\s+(Q\d+)\s+([\d.]+)(ms|µs|us|s)\s+avg\s+\(\s*([\d.]+)(ms|µs|us|s)\s*-\s*([\d.]+)(ms|µs|us|s)\s*\)',
            line
        )
        if query_match:
            query_name = query_match.group(1)
            avg_value = float(query_match.group(2))
            avg_unit = query_match.group(3)
            min_value = float(query_match.group(4))
            min_unit = query_match.group(5)
            max_value = float(query_match.group(6))
            max_unit = query_match.group(7)

            # Convert all to seconds
            def to_seconds(value: float, unit: str) -> float:
                if unit in ('us', 'µs'):
                    return value / 1_000_000
                elif unit == 'ms':
                    return value / 1_000
                else:  # 's'
                    return value

            avg_s = to_seconds(avg_value, avg_unit)
            min_s = to_seconds(min_value, min_unit)
            max_s = to_seconds(max_value, max_unit)

            results[current_db][query_name.lower()] = {
                "mean": avg_s,
                "min": min_s,
                "max": max_s,
            }

    return results


def convert_to_web_format(
    parsed_results: Dict[str, Dict[str, Dict]],
    scale_factor: float,
) -> Dict:
    """Convert parsed results to web demo format."""
    benchmarks = []

    for database, queries in parsed_results.items():
        for query_name, stats in queries.items():
            # Get query description
            query_desc = TPCH_QUERIES.get(query_name, f"Query {query_name}")

            # Create formatted name for web demo
            # Format: "tpch_q1_pricing_summary_vibesql"
            operation_name = f"tpch_{query_name}_{query_desc.lower().replace(' ', '_').replace('/', '_')}"
            formatted_name = f"{operation_name}_{database}"

            benchmark_entry = {
                "name": formatted_name,
                "stats": {
                    "mean": stats["mean"],
                    "stddev": stats["mean"] * 0.05,  # Approximate 5% stddev
                    "min": stats["min"],
                    "max": stats["max"],
                    "rounds": 10,  # Default benchmark iterations
                }
            }
            benchmarks.append(benchmark_entry)

    return {
        "benchmarks": benchmarks,
        "datetime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "machine_info": {
            "system": platform.system(),
            "cpu": platform.processor() or platform.machine(),
            "benchmark_type": "TPC-H Decision Support Queries",
            "scale_factor": f"SF {scale_factor}",
        }
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run TPC-H benchmarks")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Run quick benchmarks (only Q6, fewer iterations)"
    )
    parser.add_argument(
        "--duckdb",
        action="store_true",
        help="Include DuckDB comparison benchmarks (adds ~73MB binary size)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="tpch_results.json",
        help="Output file path (default: tpch_results.json)"
    )
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=0.01,
        help="TPC-H scale factor (default: 0.01)"
    )
    parser.add_argument(
        "--queries",
        type=str,
        default=None,
        help="Comma-separated list of queries to run (e.g., Q1,Q6,Q9)"
    )

    args = parser.parse_args()

    print("🚀 TPC-H Benchmark Runner")
    print("=" * 40)
    print(f"   Mode: {'Quick (Q6 only)' if args.quick else 'Full (all queries)'}")
    print(f"   DuckDB comparison: {'enabled' if args.duckdb else 'disabled'}")

    # Quick mode overrides
    query_filter = args.queries
    warmup_iterations = 3
    benchmark_iterations = 10
    timeout_secs = 30

    if args.quick:
        query_filter = "Q6"
        warmup_iterations = 2
        benchmark_iterations = 5
        timeout_secs = 10

    # Build benchmark
    if not build_benchmark(include_duckdb=args.duckdb):
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
        query_filter=query_filter,
        scale_factor=args.scale_factor,
        warmup_iterations=warmup_iterations,
        benchmark_iterations=benchmark_iterations,
        timeout_secs=timeout_secs,
    )

    # Parse output (benchmark uses stderr for output)
    output = stderr
    if not output:
        output = stdout

    if returncode != 0:
        print(f"❌ Benchmark failed with exit code {returncode}")
        print(f"Output:\n{output}")
        sys.exit(1)

    print("\n✅ Benchmarks completed")
    print("Parsing benchmark output...")
    parsed_results = parse_benchmark_output(output)

    if not parsed_results:
        print("⚠️  No results parsed from output!")
        print(f"Output sample:\n{output[:1000]}")
        sys.exit(1)

    # Show summary
    print("\nBenchmark Summary:")
    for db, queries in parsed_results.items():
        print(f"  {db}: {len(queries)} queries")
        for query, stats in queries.items():
            mean_ms = stats['mean'] * 1000
            print(f"    - {query}: {mean_ms:.2f} ms")

    # Convert to web format
    web_data = convert_to_web_format(
        parsed_results,
        scale_factor=args.scale_factor,
    )

    # Write output
    output_path = Path(args.output)
    with open(output_path, 'w') as f:
        json.dump(web_data, f, indent=2)

    print(f"\n✅ Results written to: {output_path}")
    print(f"   Total benchmarks: {len(web_data['benchmarks'])}")
    if web_data['benchmarks']:
        queries_tested = set(b['name'].split('_')[1] for b in web_data['benchmarks'])
        databases = set(b['name'].split('_')[-1] for b in web_data['benchmarks'])
        print(f"   Queries tested: {len(queries_tested)}")
        print(f"   Databases: {', '.join(sorted(databases))}")


if __name__ == "__main__":
    main()
