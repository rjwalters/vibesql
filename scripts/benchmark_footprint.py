#!/usr/bin/env python3
"""
Benchmark binary footprint: size, startup time, and memory usage.

This script measures:
1. Binary size (bytes)
2. Cold startup time (time to first query result)
3. Peak memory usage during initialization

Databases tested:
- VibeSQL (native CLI)
- SQLite (sqlite3)
- DuckDB (duckdb CLI)
- MySQL (mysql CLI connecting to local server, optional)

Usage:
    ./scripts/benchmark_footprint.py
    ./scripts/benchmark_footprint.py --output web-demo/public/benchmarks/footprint_results.json
"""

import gzip
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

# Number of runs for averaging startup/memory measurements
NUM_RUNS = 10


@dataclass
class FootprintResult:
    """Result for a single database's footprint measurement."""
    database: str
    binary_path: str
    binary_size_bytes: int
    startup_time_ms: float  # Average cold startup time
    startup_time_stddev_ms: float
    peak_memory_kb: float  # Average peak RSS
    peak_memory_stddev_kb: float
    version: str
    available: bool
    error: Optional[str] = None
    # WASM-specific fields (only for VibeSQL)
    wasm_size_bytes: Optional[int] = None
    wasm_size_gzip_bytes: Optional[int] = None
    wasm_size_brotli_bytes: Optional[int] = None


@dataclass
class FootprintResults:
    """Collection of all footprint measurements."""
    benchmarks: List[Dict[str, Any]]
    datetime: str
    machine_info: Dict[str, str]
    num_runs: int


def get_machine_info() -> Dict[str, str]:
    """Get information about the current machine."""
    return {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
    }


def get_binary_size(path: str) -> int:
    """Get the size of a binary in bytes."""
    if not os.path.exists(path):
        return 0
    return os.path.getsize(path)


def get_compressed_size(path: str, compression: str = 'gzip') -> Optional[int]:
    """Get the compressed size of a file.

    Args:
        path: Path to the file to compress
        compression: 'gzip' or 'brotli'

    Returns:
        Compressed size in bytes, or None if compression fails
    """
    if not os.path.exists(path):
        return None

    try:
        with open(path, 'rb') as f:
            data = f.read()

        if compression == 'gzip':
            compressed = gzip.compress(data, compresslevel=9)
            return len(compressed)
        elif compression == 'brotli':
            # Try to import brotli, but don't fail if not available
            try:
                import brotli
                compressed = brotli.compress(data, quality=11)
                return len(compressed)
            except ImportError:
                return None
    except Exception:
        return None

    return None


def measure_wasm_sizes(repo_root: Path) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """Measure WASM binary sizes (raw, gzip, brotli).

    Returns:
        Tuple of (raw_size, gzip_size, brotli_size) in bytes.
        Returns (None, None, None) if WASM file not found.
    """
    wasm_path = repo_root / "web-demo" / "public" / "pkg" / "vibesql_wasm_bg.wasm"

    if not wasm_path.exists():
        print(f"    WASM file not found: {wasm_path}")
        print("    Run 'make build-wasm' to generate WASM bindings")
        return None, None, None

    raw_size = get_binary_size(str(wasm_path))
    gzip_size = get_compressed_size(str(wasm_path), 'gzip')
    brotli_size = get_compressed_size(str(wasm_path), 'brotli')

    return raw_size, gzip_size, brotli_size


def get_binary_version(db_name: str, binary_path: str) -> str:
    """Get version string for a database binary."""
    try:
        if db_name == "vibesql":
            result = subprocess.run(
                [binary_path, "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            return result.stdout.strip() or result.stderr.strip() or "unknown"
        elif db_name == "sqlite":
            result = subprocess.run(
                [binary_path, "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            return result.stdout.strip().split()[0] if result.stdout else "unknown"
        elif db_name == "duckdb":
            result = subprocess.run(
                [binary_path, "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            return result.stdout.strip() or "unknown"
        elif db_name == "mysql":
            result = subprocess.run(
                [binary_path, "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            # Extract version number from mysql client output
            match = re.search(r'(\d+\.\d+\.\d+)', result.stdout)
            return match.group(1) if match else "unknown"
    except Exception as e:
        return f"error: {e}"
    return "unknown"


def measure_startup_and_memory_macos(
    db_name: str,
    binary_path: str,
    num_runs: int = NUM_RUNS
) -> tuple[float, float, float, float]:
    """
    Measure startup time and peak memory on macOS.

    Returns: (avg_startup_ms, stddev_startup_ms, avg_memory_kb, stddev_memory_kb)
    """
    startup_times = []
    peak_memories = []

    for _ in range(num_runs):
        with tempfile.NamedTemporaryFile(suffix='.db', delete=True) as tmp:
            db_path = tmp.name

            # Build command based on database type
            if db_name == "vibesql":
                # VibeSQL: create table and query
                cmd = [
                    "/usr/bin/time", "-l",
                    binary_path, db_path,
                    "-c", "CREATE TABLE t(x INT); INSERT INTO t VALUES(1); SELECT * FROM t;"
                ]
            elif db_name == "sqlite":
                cmd = [
                    "/usr/bin/time", "-l",
                    binary_path, db_path,
                    "CREATE TABLE t(x INT); INSERT INTO t VALUES(1); SELECT * FROM t;"
                ]
            elif db_name == "duckdb":
                cmd = [
                    "/usr/bin/time", "-l",
                    binary_path, db_path,
                    "-c", "CREATE TABLE t(x INT); INSERT INTO t VALUES(1); SELECT * FROM t;"
                ]
            elif db_name == "mysql":
                # MySQL client connects to local server - different pattern
                cmd = [
                    "/usr/bin/time", "-l",
                    binary_path, "-e", "SELECT 1;"
                ]
            else:
                continue

            try:
                start = time.perf_counter()
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                elapsed = time.perf_counter() - start

                # Parse /usr/bin/time output for peak memory (macOS format)
                # Look for "maximum resident set size" in stderr
                stderr = result.stderr

                # macOS time -l format: "NNNN  maximum resident set size"
                # The number is in bytes, we convert to KB
                match = re.search(r'(\d+)\s+maximum resident set size', stderr)
                if match:
                    peak_memory_bytes = int(match.group(1))
                    peak_memories.append(peak_memory_bytes / 1024)  # Convert to KB

                startup_times.append(elapsed * 1000)  # Convert to ms

            except subprocess.TimeoutExpired:
                continue
            except Exception as e:
                print(f"  Warning: Run failed for {db_name}: {e}", file=sys.stderr)
                continue

    if not startup_times:
        return 0, 0, 0, 0

    # Calculate averages and standard deviations
    avg_startup = sum(startup_times) / len(startup_times)
    avg_memory = sum(peak_memories) / len(peak_memories) if peak_memories else 0

    if len(startup_times) > 1:
        variance_startup = sum((x - avg_startup) ** 2 for x in startup_times) / (len(startup_times) - 1)
        stddev_startup = variance_startup ** 0.5
    else:
        stddev_startup = 0

    if len(peak_memories) > 1:
        variance_memory = sum((x - avg_memory) ** 2 for x in peak_memories) / (len(peak_memories) - 1)
        stddev_memory = variance_memory ** 0.5
    else:
        stddev_memory = 0

    return avg_startup, stddev_startup, avg_memory, stddev_memory


def measure_startup_and_memory_linux(
    db_name: str,
    binary_path: str,
    num_runs: int = NUM_RUNS
) -> tuple[float, float, float, float]:
    """
    Measure startup time and peak memory on Linux using GNU time.

    Returns: (avg_startup_ms, stddev_startup_ms, avg_memory_kb, stddev_memory_kb)
    """
    startup_times = []
    peak_memories = []

    # Check if GNU time is available
    time_cmd = "/usr/bin/time"
    if not os.path.exists(time_cmd):
        time_cmd = "time"

    for _ in range(num_runs):
        with tempfile.NamedTemporaryFile(suffix='.db', delete=True) as tmp:
            db_path = tmp.name

            # Build command based on database type
            if db_name == "vibesql":
                cmd = [
                    time_cmd, "-v",
                    binary_path, db_path,
                    "-c", "CREATE TABLE t(x INT); INSERT INTO t VALUES(1); SELECT * FROM t;"
                ]
            elif db_name == "sqlite":
                cmd = [
                    time_cmd, "-v",
                    binary_path, db_path,
                    "CREATE TABLE t(x INT); INSERT INTO t VALUES(1); SELECT * FROM t;"
                ]
            elif db_name == "duckdb":
                cmd = [
                    time_cmd, "-v",
                    binary_path, db_path,
                    "-c", "CREATE TABLE t(x INT); INSERT INTO t VALUES(1); SELECT * FROM t;"
                ]
            elif db_name == "mysql":
                cmd = [
                    time_cmd, "-v",
                    binary_path, "-e", "SELECT 1;"
                ]
            else:
                continue

            try:
                start = time.perf_counter()
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                elapsed = time.perf_counter() - start

                # Parse GNU time -v output for peak memory
                # Format: "Maximum resident set size (kbytes): NNNN"
                stderr = result.stderr
                match = re.search(r'Maximum resident set size.*?:\s*(\d+)', stderr)
                if match:
                    peak_memories.append(int(match.group(1)))  # Already in KB

                startup_times.append(elapsed * 1000)  # Convert to ms

            except subprocess.TimeoutExpired:
                continue
            except Exception as e:
                print(f"  Warning: Run failed for {db_name}: {e}", file=sys.stderr)
                continue

    if not startup_times:
        return 0, 0, 0, 0

    avg_startup = sum(startup_times) / len(startup_times)
    avg_memory = sum(peak_memories) / len(peak_memories) if peak_memories else 0

    if len(startup_times) > 1:
        variance_startup = sum((x - avg_startup) ** 2 for x in startup_times) / (len(startup_times) - 1)
        stddev_startup = variance_startup ** 0.5
    else:
        stddev_startup = 0

    if len(peak_memories) > 1:
        variance_memory = sum((x - avg_memory) ** 2 for x in peak_memories) / (len(peak_memories) - 1)
        stddev_memory = variance_memory ** 0.5
    else:
        stddev_memory = 0

    return avg_startup, stddev_startup, avg_memory, stddev_memory


def measure_database(
    db_name: str,
    binary_path: str,
    repo_root: Path,
    num_runs: int = NUM_RUNS
) -> FootprintResult:
    """Measure footprint for a single database."""
    print(f"  Measuring {db_name}...")

    # Check if binary exists
    if not os.path.exists(binary_path):
        print(f"    ⚠️  Binary not found: {binary_path}")
        return FootprintResult(
            database=db_name,
            binary_path=binary_path,
            binary_size_bytes=0,
            startup_time_ms=0,
            startup_time_stddev_ms=0,
            peak_memory_kb=0,
            peak_memory_stddev_kb=0,
            version="not found",
            available=False,
            error=f"Binary not found: {binary_path}"
        )

    # Get binary size
    binary_size = get_binary_size(binary_path)
    print(f"    Binary size: {binary_size / (1024*1024):.2f} MB")

    # Get version
    version = get_binary_version(db_name, binary_path)
    print(f"    Version: {version}")

    # Measure startup time and memory
    if platform.system() == "Darwin":
        avg_startup, stddev_startup, avg_memory, stddev_memory = \
            measure_startup_and_memory_macos(db_name, binary_path, num_runs)
    else:
        avg_startup, stddev_startup, avg_memory, stddev_memory = \
            measure_startup_and_memory_linux(db_name, binary_path, num_runs)

    print(f"    Startup time: {avg_startup:.2f} ± {stddev_startup:.2f} ms")
    print(f"    Peak memory: {avg_memory:.0f} ± {stddev_memory:.0f} KB ({avg_memory/1024:.1f} MB)")

    # Measure WASM sizes (only for VibeSQL)
    wasm_size = None
    wasm_gzip_size = None
    wasm_brotli_size = None

    if db_name == "vibesql":
        wasm_size, wasm_gzip_size, wasm_brotli_size = measure_wasm_sizes(repo_root)
        if wasm_size:
            print(f"    WASM size: {wasm_size / (1024*1024):.2f} MB")
            if wasm_gzip_size:
                print(f"    WASM gzip: {wasm_gzip_size / (1024*1024):.2f} MB ({wasm_gzip_size/wasm_size*100:.1f}%)")
            if wasm_brotli_size:
                print(f"    WASM brotli: {wasm_brotli_size / (1024*1024):.2f} MB ({wasm_brotli_size/wasm_size*100:.1f}%)")

    return FootprintResult(
        database=db_name,
        binary_path=binary_path,
        binary_size_bytes=binary_size,
        startup_time_ms=avg_startup,
        startup_time_stddev_ms=stddev_startup,
        peak_memory_kb=avg_memory,
        peak_memory_stddev_kb=stddev_memory,
        version=version,
        available=True,
        wasm_size_bytes=wasm_size,
        wasm_size_gzip_bytes=wasm_gzip_size,
        wasm_size_brotli_bytes=wasm_brotli_size
    )


def find_binaries() -> Dict[str, str]:
    """Find database binaries on the system."""
    repo_root = Path(__file__).parent.parent

    binaries = {}

    # VibeSQL - from cargo build
    vibesql_path = repo_root / "target" / "release" / "vibesql"
    if vibesql_path.exists():
        binaries["vibesql"] = str(vibesql_path)
    else:
        # Try debug build
        vibesql_debug = repo_root / "target" / "debug" / "vibesql"
        if vibesql_debug.exists():
            binaries["vibesql"] = str(vibesql_debug)

    # SQLite
    sqlite_path = shutil.which("sqlite3")
    if sqlite_path:
        binaries["sqlite"] = sqlite_path
    elif os.path.exists("/usr/bin/sqlite3"):
        binaries["sqlite"] = "/usr/bin/sqlite3"

    # DuckDB
    duckdb_path = shutil.which("duckdb")
    if duckdb_path:
        binaries["duckdb"] = duckdb_path

    # MySQL client
    mysql_path = shutil.which("mysql")
    if mysql_path:
        binaries["mysql"] = mysql_path

    return binaries


def run_benchmarks(output_path: Optional[str] = None, num_runs: int = NUM_RUNS) -> FootprintResults:
    """Run all footprint benchmarks."""
    print("🔍 Footprint Benchmark")
    print("=" * 50)
    print(f"Platform: {platform.system()} {platform.release()}")
    print(f"Runs per database: {num_runs}")
    print()

    # Get repo root for WASM path lookup
    repo_root = Path(__file__).parent.parent

    # Find binaries
    print("📂 Locating database binaries...")
    binaries = find_binaries()

    for db, path in binaries.items():
        print(f"  {db}: {path}")

    print()
    print("📊 Running measurements...")

    results = []
    for db_name, binary_path in binaries.items():
        result = measure_database(db_name, binary_path, repo_root, num_runs)
        results.append(asdict(result))

    # Create final results object
    footprint_results = FootprintResults(
        benchmarks=results,
        datetime=datetime.now().isoformat(),
        machine_info=get_machine_info(),
        num_runs=num_runs
    )

    # Output results
    if output_path:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(asdict(footprint_results), f, indent=2)
        print()
        print(f"✅ Results saved to: {output_path}")
    else:
        print()
        print("📋 Results (JSON):")
        print(json.dumps(asdict(footprint_results), indent=2))

    return footprint_results


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Benchmark database binary footprint")
    parser.add_argument(
        "--output", "-o",
        help="Output JSON file path (default: print to stdout)",
        default=None
    )
    parser.add_argument(
        "--runs", "-n",
        type=int,
        default=NUM_RUNS,
        help=f"Number of runs for averaging (default: {NUM_RUNS})"
    )

    args = parser.parse_args()

    try:
        run_benchmarks(args.output, args.runs)
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
