#!/usr/bin/env python3
"""
Parse Criterion benchmark data and update web demo JSON files.

This script reads Criterion's estimates.json files from target/criterion/
and generates comparison data for the web demo benchmarks page.

Usage:
    python scripts/parse_criterion_data.py --tpch
    python scripts/parse_criterion_data.py --sysbench
    python scripts/parse_criterion_data.py --all
"""

import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def get_repo_root() -> Path:
    """Get the repository root directory."""
    return Path(__file__).parent.parent


def parse_criterion_estimates(estimates_path: Path) -> Optional[Dict]:
    """Parse a Criterion estimates.json file and return timing data."""
    try:
        with open(estimates_path) as f:
            data = json.load(f)

        # Criterion stores times in nanoseconds
        mean_ns = data.get('mean', {}).get('point_estimate', 0)
        std_dev_ns = data.get('std_dev', {}).get('point_estimate', 0)

        # Convert to seconds for web demo format
        mean_s = mean_ns / 1_000_000_000
        std_dev_s = std_dev_ns / 1_000_000_000

        return {
            'mean': mean_s,
            'stddev': std_dev_s,
            'min': mean_s * 0.95,  # Approximation
            'max': mean_s * 1.05,  # Approximation
            'rounds': 100  # Default Criterion sample size
        }
    except Exception as e:
        print(f"  Warning: Failed to parse {estimates_path}: {e}")
        return None


def find_criterion_benchmarks(criterion_dir: Path, pattern: str) -> Dict[str, Dict[str, Dict]]:
    """
    Find all Criterion benchmark results matching a pattern.

    Returns: {benchmark_name: {database: stats}}
    """
    results = {}

    for benchmark_dir in criterion_dir.iterdir():
        if not benchmark_dir.is_dir():
            continue

        name = benchmark_dir.name
        if not re.match(pattern, name):
            continue

        # Check for estimates.json in new/ subdirectory
        estimates_path = benchmark_dir / 'new' / 'estimates.json'
        if estimates_path.exists():
            # Parse the benchmark name to extract query and database
            # Format: tpch_q1_vibesql or tpch_q1_sqlite
            match = re.match(r'(tpch_q\d+)_(vibesql|sqlite|duckdb|mysql)', name)
            if match:
                query = match.group(1)
                database = match.group(2)

                stats = parse_criterion_estimates(estimates_path)
                if stats:
                    if query not in results:
                        results[query] = {}
                    results[query][database] = stats

        # Also check for nested structure: tpch_q1/vibesql/SF0.01/
        for subdir in benchmark_dir.iterdir():
            if subdir.is_dir() and subdir.name in ['vibesql', 'sqlite', 'duckdb', 'mysql']:
                database = subdir.name
                # Look in any scale factor subdirectory
                for sf_dir in subdir.iterdir():
                    if sf_dir.is_dir():
                        estimates_path = sf_dir / 'new' / 'estimates.json'
                        if estimates_path.exists():
                            query = benchmark_dir.name
                            stats = parse_criterion_estimates(estimates_path)
                            if stats:
                                if query not in results:
                                    results[query] = {}
                                results[query][database] = stats

    return results


def find_sysbench_benchmarks(criterion_dir: Path) -> Dict[str, Dict[str, Dict]]:
    """Find all Sysbench benchmark results."""
    results = {}

    sysbench_patterns = [
        'sysbench_point_select',
        'sysbench_insert',
        'sysbench_update_index',
        'sysbench_update_non_index',
        'sysbench_delete',
        'sysbench_read_write',
        'sysbench_write_only',
        'sysbench_range_queries',
        'oltp_read_only',
        'select_random_points',
        'select_random_ranges',
    ]

    for benchmark_dir in criterion_dir.iterdir():
        if not benchmark_dir.is_dir():
            continue

        name = benchmark_dir.name
        if not any(name.startswith(p) for p in sysbench_patterns):
            continue

        # Look for database subdirectories
        for db_dir in benchmark_dir.iterdir():
            if not db_dir.is_dir():
                continue

            database = db_dir.name
            if database not in ['vibesql', 'vibesql_direct', 'sqlite', 'duckdb', 'mysql']:
                continue

            # Look for scale factor subdirectories (e.g., 10000)
            for sf_dir in db_dir.iterdir():
                if sf_dir.is_dir():
                    estimates_path = sf_dir / 'new' / 'estimates.json'
                    if estimates_path.exists():
                        stats = parse_criterion_estimates(estimates_path)
                        if stats:
                            if name not in results:
                                results[name] = {}
                            results[name][database] = stats

    return results


def update_tpch_benchmark_results(criterion_data: Dict, output_path: Path):
    """Update the TPC-H benchmark_results.json with Criterion data."""
    # TPC-H query descriptions for name formatting
    TPCH_QUERIES = {
        "tpch_q1": "pricing_summary_report",
        "tpch_q2": "minimum_cost_supplier",
        "tpch_q3": "shipping_priority",
        "tpch_q4": "order_priority_checking",
        "tpch_q5": "local_supplier_volume",
        "tpch_q6": "forecasting_revenue_change",
        "tpch_q7": "volume_shipping",
        "tpch_q8": "national_market_share",
        "tpch_q9": "product_type_profit_measure",
        "tpch_q10": "returned_item_reporting",
        "tpch_q11": "important_stock_identification",
        "tpch_q12": "shipping_modes_priority",
        "tpch_q13": "customer_distribution",
        "tpch_q14": "promotion_effect",
        "tpch_q15": "top_supplier",
        "tpch_q16": "parts_supplier_relationship",
        "tpch_q17": "small-quantity-order_revenue",
        "tpch_q18": "large_volume_customer",
        "tpch_q19": "discounted_revenue",
        "tpch_q20": "potential_part_promotion",
        "tpch_q21": "suppliers_who_kept_orders_waiting",
        "tpch_q22": "global_sales_opportunity",
    }

    benchmarks = []

    # Sort queries by number
    sorted_queries = sorted(criterion_data.keys(), key=lambda x: int(re.search(r'q(\d+)', x).group(1)))

    for query in sorted_queries:
        databases = criterion_data[query]
        query_desc = TPCH_QUERIES.get(query, query)

        # Add each database's results
        for database in ['vibesql', 'sqlite', 'duckdb', 'mysql']:
            if database in databases:
                name = f"{query}_{query_desc}_{database}"
                benchmarks.append({
                    "name": name,
                    "stats": databases[database]
                })

    result = {
        "benchmarks": benchmarks,
        "datetime": datetime.utcnow().isoformat() + "Z",
        "machine_info": {
            "note": "Local development machine (parsed from Criterion)",
            "benchmark_type": "TPC-H Decision Support Queries",
            "scale_factor": "SF 0.01 (~60,000 rows)"
        }
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)

    print(f"  Written {len(benchmarks)} benchmarks to {output_path}")


def update_sysbench_results(criterion_data: Dict, output_path: Path):
    """Update the sysbench_results.json with Criterion data."""
    benchmarks = []

    for workload, databases in sorted(criterion_data.items()):
        for database in ['vibesql', 'vibesql_direct', 'sqlite', 'duckdb', 'mysql']:
            if database in databases:
                name = f"{workload}_{database}"
                benchmarks.append({
                    "name": name,
                    "stats": databases[database]
                })

    result = {
        "benchmarks": benchmarks,
        "metadata": {
            "suite": "sysbench",
            "timestamp": datetime.utcnow().isoformat(),
            "git_commit": "criterion_parsed",
            "table_size": "10000"
        }
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)

    print(f"  Written {len(benchmarks)} benchmarks to {output_path}")


def find_tpcds_benchmarks(criterion_dir: Path) -> Dict[str, Dict[str, Dict]]:
    """Find all TPC-DS benchmark results from tpcds_queries_comparison group."""
    results = {}

    # Look for tpcds_queries_comparison group
    comparison_dir = criterion_dir / 'tpcds_queries_comparison'
    if comparison_dir.exists():
        for db_dir in comparison_dir.iterdir():
            if not db_dir.is_dir():
                continue
            database = db_dir.name
            if database not in ['vibesql', 'sqlite', 'duckdb', 'mysql']:
                continue

            for query_dir in db_dir.iterdir():
                if query_dir.is_dir():
                    estimates_path = query_dir / 'new' / 'estimates.json'
                    if estimates_path.exists():
                        query = query_dir.name
                        stats = parse_criterion_estimates(estimates_path)
                        if stats:
                            if query not in results:
                                results[query] = {}
                            results[query][database] = stats

    # Also check for tpcds_queries (vibesql-only)
    queries_dir = criterion_dir / 'tpcds_queries'
    if queries_dir.exists():
        for db_dir in queries_dir.iterdir():
            if not db_dir.is_dir():
                continue
            database = db_dir.name
            if database not in ['vibesql', 'sqlite', 'duckdb', 'mysql']:
                continue

            for query_dir in db_dir.iterdir():
                if query_dir.is_dir():
                    estimates_path = query_dir / 'new' / 'estimates.json'
                    if estimates_path.exists():
                        query = query_dir.name
                        stats = parse_criterion_estimates(estimates_path)
                        if stats:
                            if query not in results:
                                results[query] = {}
                            # Only add if not already present from comparison data
                            if database not in results[query]:
                                results[query][database] = stats

    return results


def update_tpcds_results(criterion_data: Dict, output_path: Path):
    """Update the tpcds_results.json with Criterion data."""
    benchmarks = []

    # Sort queries by number (Q1, Q2, ... Q99)
    def query_sort_key(q):
        match = re.search(r'Q(\d+)', q)
        return int(match.group(1)) if match else 0

    sorted_queries = sorted(criterion_data.keys(), key=query_sort_key)

    for query in sorted_queries:
        databases = criterion_data[query]

        # Add each database's results
        for database in ['vibesql', 'sqlite', 'duckdb', 'mysql']:
            if database in databases:
                name = f"tpcds_{query.lower()}_{database}"
                stats = databases[database].copy()
                stats['status'] = 'passed'
                stats['total'] = stats['mean']
                stats['rows'] = 0
                benchmarks.append({
                    "name": name,
                    "stats": stats
                })

    result = {
        "benchmarks": benchmarks,
        "metadata": {
            "suite": "tpcds",
            "timestamp": datetime.utcnow().isoformat(),
            "git_commit": "criterion_parsed",
            "scale_factor": "0.001",
            "total_queries": len(sorted_queries),
            "passed_queries": len(sorted_queries),
            "note": "TPC-DS benchmark data parsed from Criterion results."
        }
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)

    print(f"  Written {len(benchmarks)} benchmarks to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Parse Criterion benchmark data")
    parser.add_argument('--all', action='store_true', help="Parse all benchmark types")
    parser.add_argument('--tpch', action='store_true', help="Parse TPC-H benchmarks")
    parser.add_argument('--tpcds', action='store_true', help="Parse TPC-DS benchmarks")
    parser.add_argument('--sysbench', action='store_true', help="Parse Sysbench benchmarks")
    parser.add_argument('--output-dir', type=str, default=None,
                        help="Output directory (default: web-demo/public/benchmarks)")
    parser.add_argument('--verbose', '-v', action='store_true', help="Verbose output")

    args = parser.parse_args()

    if not (args.all or args.tpch or args.tpcds or args.sysbench):
        args.all = True

    repo_root = get_repo_root()
    criterion_dir = repo_root / 'target' / 'criterion'

    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = repo_root / 'web-demo' / 'public' / 'benchmarks'

    if not criterion_dir.exists():
        print(f"Error: Criterion directory not found: {criterion_dir}")
        print("Run benchmarks first: cargo bench --features benchmark-comparison")
        return 1

    print(f"Parsing Criterion data from {criterion_dir}")
    print()

    if args.all or args.tpch:
        print("Parsing TPC-H benchmarks...")
        tpch_data = find_criterion_benchmarks(criterion_dir, r'tpch_q\d+')

        if tpch_data:
            print(f"  Found {len(tpch_data)} TPC-H queries")
            for query, databases in sorted(tpch_data.items()):
                dbs = ', '.join(sorted(databases.keys()))
                if args.verbose:
                    print(f"    {query}: {dbs}")

            update_tpch_benchmark_results(tpch_data, output_dir / 'benchmark_results.json')
        else:
            print("  No TPC-H data found")
        print()

    if args.all or args.tpcds:
        print("Parsing TPC-DS benchmarks...")
        tpcds_data = find_tpcds_benchmarks(criterion_dir)

        if tpcds_data:
            print(f"  Found {len(tpcds_data)} TPC-DS queries")
            for query, databases in sorted(tpcds_data.items(), key=lambda x: int(re.search(r'Q(\d+)', x[0]).group(1)) if re.search(r'Q(\d+)', x[0]) else 0):
                dbs = ', '.join(sorted(databases.keys()))
                if args.verbose:
                    print(f"    {query}: {dbs}")

            update_tpcds_results(tpcds_data, output_dir / 'tpcds_results.json')
        else:
            print("  No TPC-DS data found")
        print()

    if args.all or args.sysbench:
        print("Parsing Sysbench benchmarks...")
        sysbench_data = find_sysbench_benchmarks(criterion_dir)

        if sysbench_data:
            print(f"  Found {len(sysbench_data)} Sysbench workloads")
            for workload, databases in sorted(sysbench_data.items()):
                dbs = ', '.join(sorted(databases.keys()))
                if args.verbose:
                    print(f"    {workload}: {dbs}")

            update_sysbench_results(sysbench_data, output_dir / 'sysbench_results.json')
        else:
            print("  No Sysbench data found")
        print()

    print("Done!")
    return 0


if __name__ == "__main__":
    exit(main())
