
import pytest
import sqlite3
import duckdb
import vibesql
import json
import platform
import psutil
import sys
from pathlib import Path

# Ensure the benchmarks directory is in the Python path for utils imports
benchmarks_dir = Path(__file__).parent
if str(benchmarks_dir) not in sys.path:
    sys.path.insert(0, str(benchmarks_dir))

# pytest-benchmark configuration
def pytest_configure(config):
    """Configure pytest-benchmark for consistent measurements"""
    config.option.benchmark_min_rounds = 5
    config.option.benchmark_warmup = True
    config.option.benchmark_warmup_iterations = 2
    config.option.benchmark_disable_gc = True  # Disable GC during timing
    # config.option.benchmark_json = "benchmark_results.json"  # Disabled to avoid JSON saving issues

def pytest_addoption(parser):
    """Add custom command-line options"""
    parser.addoption(
        "--no-duckdb",
        action="store_true",
        default=False,
        help="Skip DuckDB benchmarks (faster iteration)"
    )

@pytest.fixture(scope="session")
def hardware_metadata():
    """Collect hardware and environment metadata"""
    return {
        "cpu": platform.processor() or platform.machine(),
        "cpu_count": psutil.cpu_count(logical=False),
        "memory_gb": round(psutil.virtual_memory().total / (1024**3), 2),
        "os": f"{platform.system()} {platform.release()}",
        "python_version": platform.python_version(),
        "timestamp": None  # Set at runtime
    }

@pytest.fixture
def sqlite_db():
    """Create SQLite in-memory database connection"""
    conn = sqlite3.connect(':memory:')
    yield conn
    conn.close()

@pytest.fixture
def vibesql_db():
    """Create vibesql in-memory database connection"""
    # Assuming PyO3 bindings follow DB-API 2.0 pattern
    conn = vibesql.connect()  # In-memory by default
    yield conn
    conn.close()

@pytest.fixture
def duckdb_db(request):
    """Create DuckDB in-memory database connection"""
    # Skip if --no-duckdb flag is set
    if request.config.getoption("--no-duckdb"):
        pytest.skip("DuckDB benchmarks disabled with --no-duckdb")

    conn = duckdb.connect(':memory:')
    yield conn
    conn.close()

@pytest.fixture
def both_databases(sqlite_db, vibesql_db):
    """Provide both databases for head-to-head comparison"""
    return {
        'sqlite': sqlite_db,
        'vibesql': vibesql_db
    }

@pytest.fixture
def all_databases(sqlite_db, vibesql_db, duckdb_db):
    """Provide all three databases for comprehensive comparison"""
    return {
        'sqlite': sqlite_db,
        'vibesql': vibesql_db,
        'duckdb': duckdb_db
    }

@pytest.fixture
def setup_test_table(both_databases):
    """Create identical test table in both databases"""
    schema = """
        CREATE TABLE test_users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50),
            age INTEGER,
            salary FLOAT,
            active BOOLEAN
        )
    """

    # Create table in SQLite
    sqlite_db = both_databases['sqlite']
    sqlite_db.execute(schema)

    # Create table in vibesql
    vibesql_db = both_databases['vibesql']
    cursor = vibesql_db.cursor()
    cursor.execute(schema)

    return both_databases

@pytest.fixture
def insert_test_data(setup_test_table, request):
    """Insert test data into both databases

    Usage: @pytest.mark.parametrize("row_count", [1000, 10000])
    """
    row_count = getattr(request, 'param', 1000)  # Default 1K rows

    databases = setup_test_table

    # Generate test data
    test_data = [
        (i, f"User{i}", 20 + (i % 50), 30000.0 + (i * 100), i % 2 == 0)
        for i in range(row_count)
    ]

    # Insert into SQLite
    sqlite_db = databases['sqlite']
    sqlite_db.executemany(
        "INSERT INTO test_users VALUES (?, ?, ?, ?, ?)",
        test_data
    )
    sqlite_db.commit()

    # Insert into vibesql
    vibesql_db = databases['vibesql']
    cursor = vibesql_db.cursor()
    for row in test_data:
        cursor.execute(
            "INSERT INTO test_users VALUES (?, ?, ?, ?, ?)",
            row
        )

    return databases, row_count
