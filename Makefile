# VibeSQL Makefile
# Convenience targets for common development tasks

.PHONY: all all-fg logs status build build-all build-wasm build-python test test-unit test-workspace test-ignored test-sqllogictest test-sqllogictest-halting benchmark benchmark-tpch benchmark-tpcc benchmark-tpcds benchmark-tpcds-all benchmark-sysbench clean help analyze-tests analyze-benchmarks analyze flamegraph-tpch flamegraph-tpcc flamegraph-sysbench flamegraph-select profile-query bench-storage bench-executor bench-types website mysql-start mysql-stop mysql-status

# Log file location for background runs
LOG_FILE := /tmp/vibesql-make-all.log
PID_FILE := /tmp/vibesql-make-all.pid

# Default target - runs in background since it takes a long time
# Use 'make all-fg' for foreground execution
all:
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  Starting 'make all' in background (build + test + benchmark)"
	@echo "══════════════════════════════════════════════════════════════════"
	@echo ""
	@echo "  Log file: $(LOG_FILE)"
	@echo ""
	@echo "  Monitor progress:"
	@echo "    make status    - Quick status check with last 10 lines"
	@echo "    make logs      - Follow full output (Ctrl+C to stop)"
	@echo "    tail -f $(LOG_FILE)"
	@echo ""
	@nohup $(MAKE) all-fg > $(LOG_FILE) 2>&1 & echo $$! > $(PID_FILE)
	@echo "  PID: $$(cat $(PID_FILE))"
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"

# Run all targets in foreground (build, test, benchmark)
# This is what 'make all' runs in the background
all-fg: build-all test benchmark

# Tail the background make output
logs:
	@if [ -f $(LOG_FILE) ]; then \
		tail -f $(LOG_FILE); \
	else \
		echo "No log file found. Run 'make all' first."; \
	fi

# Check status of background make
status:
	@if [ -f $(PID_FILE) ]; then \
		PID=$$(cat $(PID_FILE)); \
		if ps -p $$PID > /dev/null 2>&1; then \
			echo "✓ make all is running (PID: $$PID)"; \
			echo ""; \
			echo "Last 10 lines of output:"; \
			tail -10 $(LOG_FILE) 2>/dev/null || echo "(no output yet)"; \
		else \
			echo "✗ make all has finished"; \
			echo ""; \
			echo "Exit status (last 20 lines):"; \
			tail -20 $(LOG_FILE) 2>/dev/null; \
		fi; \
	else \
		echo "No background make running. Use 'make all' to start."; \
	fi

# Help target
help:
	@echo "VibeSQL Makefile - Common development tasks"
	@echo ""
	@echo "Build targets:"
	@echo "  make build              - Build Rust crates in release mode (excludes Python)"
	@echo "  make build-all          - Build everything including Python bindings"
	@echo "  make build-wasm         - Build WebAssembly bindings for web demo"
	@echo "  make build-python       - Build Python bindings wheel (via maturin)"
	@echo ""
	@echo "Test targets:"
	@echo "  make test               - Run all tests (workspace includes sqllogictest suite)"
	@echo "  make test-unit          - Run unit tests only (lib tests)"
	@echo "  make test-workspace     - Run all workspace tests (unit + integration + sqllogictest)"
	@echo "  make test-ignored       - Run only ignored/slow tests (disk-backed indexes, etc.)"
	@echo "  make test-sqllogictest  - Run SQLLogicTest standalone (with JSON output)"
	@echo "  make test-sqllogictest-halting - Run SQLLogicTest, stop on first failure"
	@echo ""
	@echo "Benchmark targets:"
	@echo "  make benchmark          - Run all benchmarks (TPC-H, TPC-C, TPC-DS, Sysbench)"
	@echo "  make benchmark-tpch     - Run TPC-H benchmark suite (30s timeout)"
	@echo "  make benchmark-tpcc     - Run TPC-C benchmark suite (60s duration)"
	@echo "  make benchmark-tpcds    - Run TPC-DS benchmark suite (isolated, memory-safe)"
	@echo "  make benchmark-tpcds-all - Run TPC-DS with all engines simultaneously (may OOM)"
	@echo "  make benchmark-sysbench - Run Sysbench OLTP benchmarks"
	@echo ""
	@echo "Profiling targets:"
	@echo "  make flamegraph-tpch    - Generate flamegraph for TPC-H queries"
	@echo "  make flamegraph-tpcc    - Generate flamegraph for TPC-C transactions"
	@echo "  make flamegraph-sysbench - Generate flamegraph for Sysbench OLTP"
	@echo "  make flamegraph-select  - Generate flamegraph for point SELECT"
	@echo "  make profile-query Q=X  - Profile specific TPC-H query (e.g., Q=Q6)"
	@echo ""
	@echo "Subsystem benchmarks:"
	@echo "  make bench-storage      - Run storage subsystem benchmarks (B-tree, page cache)"
	@echo "  make bench-executor     - Run executor benchmarks (expression eval, iterators)"
	@echo "  make bench-types        - Run type system benchmarks (SqlValue operations)"
	@echo ""
	@echo "Analysis targets:"
	@echo "  make analyze            - Show test and benchmark analysis"
	@echo "  make analyze-tests      - Show SQLLogicTest analysis from database"
	@echo "  make analyze-benchmarks - Show TPC-H benchmark analysis from database"
	@echo ""
	@echo "MySQL Docker targets:"
	@echo "  make mysql-start        - Start MySQL Docker container for benchmarks"
	@echo "  make mysql-stop         - Stop and remove MySQL Docker container"
	@echo "  make mysql-status       - Check MySQL Docker container status"
	@echo ""
	@echo "Utility targets:"
	@echo "  make clean              - Clean build artifacts"
	@echo "  make website            - Regenerate web dashboard data from benchmark database"
	@echo "  make all                - Build, test, benchmark (runs in BACKGROUND by default)"
	@echo "  make all-fg             - Run 'make all' in foreground (blocking)"
	@echo "  make logs               - Tail the background make output"
	@echo "  make status             - Check if background make is running and show recent output"
	@echo "  make help               - Show this help message"

#
# Build Targets
#

# Build all Rust crates in release mode (excludes Python bindings which require maturin)
build:
	@echo "Building VibeSQL (release mode)..."
	cargo build --release --workspace --exclude vibesql-python-bindings

# Build WebAssembly bindings for web demo
build-wasm:
	@echo "Building WebAssembly bindings..."
	./scripts/build-wasm.sh

# Build Python bindings wheel
build-python:
	@echo "Building Python bindings..."
	./scripts/build-python.sh

# Build everything including Python bindings
build-all: build build-python

#
# Test Targets
#

# Run all tests with analysis
# Note: workspace tests include sqllogictest_suite (623 files) internally,
# so we don't run test-sqllogictest separately to avoid duplicate work.
# Use 'make test-sqllogictest' directly if you want the standalone runner with JSON output.
test: test-workspace analyze-tests

# Run unit tests only (lib tests)
test-unit:
	@echo "Running unit tests..."
	@echo "This runs library tests across all workspace crates"
	cargo test --release --workspace --lib

# Run all workspace tests (unit + integration)
test-workspace:
	@echo "Running workspace tests (unit + integration)..."
	@echo "This includes 2,991 unit tests + 739 sqltest conformance tests"
	cargo test --release --workspace

# Run only ignored/slow tests (disk-backed indexes, unimplemented features, etc.)
test-ignored:
	@echo "Running ignored tests only..."
	@echo "These are slow tests that are skipped during normal test runs"
	cargo test --release --workspace -- --ignored

# Run SQLLogicTest suite (parallel mode recommended)
test-sqllogictest:
	@echo "Running SQLLogicTest suite (parallel, auto-detected workers)..."
	@echo "This runs ~5.9M tests across 628 test files"
	./scripts/sqllogictest run --parallel

# Run SQLLogicTest suite in fail-fast mode (stop on first failure)
# Useful for troubleshooting regressions - shows exactly where things break
test-sqllogictest-halting:
	@echo "Running SQLLogicTest suite (fail-fast mode)..."
	@echo "Will stop on first test file failure for easier debugging"
	./scripts/sqllogictest run --fail-fast

#
# Benchmark Targets
#

# Run all benchmarks with analysis (using unified CLI)
benchmark:
	@./scripts/bench --all

# Run TPC-H benchmarks with 30s timeout per query and store results in database
benchmark-tpch:
	@./scripts/bench --test=tpch --timeout=30

# Run TPC-C benchmarks (OLTP workload) with database tracking
# Automatically starts MySQL Docker container if Docker is available
benchmark-tpcc:
	@./scripts/bench --test=tpcc --engine=vibesql,mysql --duration=60

# Run TPC-DS benchmarks with database tracking
# Uses isolated execution (each database engine in separate process) to avoid memory pressure
benchmark-tpcds:
	@./scripts/bench --test=tpcds

# Run TPC-DS benchmarks with all engines simultaneously (may cause memory pressure)
benchmark-tpcds-all:
	@echo "⚠️  This target is deprecated. Use 'make benchmark-tpcds' instead."
	@echo "   Use '--engine=all' for comparison: ./scripts/bench --test=tpcds --engine=all"
	@./scripts/bench --test=tpcds --engine=all

# Run Sysbench OLTP benchmarks with database tracking
# Automatically starts MySQL Docker container if Docker is available
benchmark-sysbench:
	@./scripts/bench --test=sysbench --engine=vibesql,mysql

#
# Analysis Targets
#

# Show all analysis (tests + benchmarks)
analyze: analyze-tests analyze-benchmarks

# Show SQLLogicTest analysis from database
analyze-tests:
	@echo ""
	@echo "=========================================="
	@echo "SQLLogicTest Analysis"
	@echo "=========================================="
	@./scripts/sqllogictest analyze --top-fixes 2>/dev/null || echo "Run 'make test-sqllogictest' first to generate test data"
	@echo ""

# Show all benchmark analysis from database
analyze-benchmarks:
	@echo ""
	@echo "=========================================="
	@echo "TPC-H Benchmark Analysis"
	@echo "=========================================="
	@./scripts/query_benchmark_results.py --latest 2>/dev/null || echo "Run 'make benchmark-tpch' first to generate benchmark data"
	@echo ""
	@./scripts/query_benchmark_results.py --stats 2>/dev/null || true
	@echo ""
	@echo "=========================================="
	@echo "TPC-C Benchmark Analysis"
	@echo "=========================================="
	@./scripts/query_benchmark_results.py --tpcc 2>/dev/null || echo "Run 'make benchmark-tpcc' first to generate benchmark data"
	@echo ""
	@echo "=========================================="
	@echo "TPC-DS Benchmark Analysis"
	@echo "=========================================="
	@./scripts/query_benchmark_results.py --tpcds 2>/dev/null || echo "Run 'make benchmark-tpcds' first to generate benchmark data"
	@echo ""
	@echo "=========================================="
	@echo "Sysbench OLTP Analysis"
	@echo "=========================================="
	@./scripts/query_benchmark_results.py --sysbench 2>/dev/null || echo "Run 'make benchmark-sysbench' first to generate benchmark data"
	@echo ""

#
# Utility Targets
#

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	rm -rf web-demo/public/pkg
	rm -rf target/wheels
	rm -f target/sqllogictest_*.json
	rm -f /tmp/tpch_results.txt
	rm -f flamegraph*.svg

# Regenerate web dashboard data from benchmark database
website:
	@echo "Regenerating web dashboard data..."
	@./scripts/generate_web_dashboard.py 2>/dev/null || echo "Note: Run 'make benchmark' first to populate the database"
	@echo ""
	@echo "Output: web-demo/public/data/dashboard.json"
	@echo "Run 'cd web-demo && pnpm run build' to rebuild the site"

#
# Profiling Targets
#

# Generate flamegraph for TPC-H queries
flamegraph-tpch:
	@echo "Generating flamegraph for TPC-H queries..."
	@echo "Requires: cargo install flamegraph"
	./scripts/flamegraph.sh tpch

# Generate flamegraph for TPC-C transactions
flamegraph-tpcc:
	@echo "Generating flamegraph for TPC-C transactions..."
	./scripts/flamegraph.sh tpcc

# Generate flamegraph for Sysbench OLTP
flamegraph-sysbench:
	@echo "Generating flamegraph for Sysbench OLTP..."
	./scripts/flamegraph.sh sysbench

# Generate flamegraph for point SELECT operations
flamegraph-select:
	@echo "Generating flamegraph for SELECT operations..."
	./scripts/flamegraph.sh select

# Profile specific TPC-H query with detailed timing breakdown
# Usage: make profile-query Q=Q6
profile-query:
ifndef Q
	@echo "Usage: make profile-query Q=<query>"
	@echo "Example: make profile-query Q=Q6"
	@exit 1
endif
	@echo "Profiling TPC-H query: $(Q)"
	./scripts/profile-query.sh --tpch $(Q)

#
# Subsystem Benchmarks
#

# Run storage subsystem benchmarks (B-tree operations, page cache)
bench-storage:
	@echo "Running storage subsystem benchmarks..."
	cargo bench --package vibesql-storage --bench storage_bench 2>&1 | tee /tmp/storage_bench_results.txt || \
		echo "Note: Storage benchmarks not yet implemented. Create benches/storage_bench.rs"

# Run executor benchmarks (expression evaluation, iterator operations)
bench-executor:
	@echo "Running executor benchmarks..."
	cargo bench --package vibesql-executor --bench iterator_execution -- --noplot 2>&1 | tee /tmp/executor_bench_results.txt
	cargo bench --package vibesql-executor --bench columnar_execution -- --noplot 2>&1 | tee -a /tmp/executor_bench_results.txt

# Run type system benchmarks (SqlValue construction, comparison, conversion)
bench-types:
	@echo "Running type system benchmarks..."
	cargo bench --package vibesql-types --bench types_bench 2>&1 | tee /tmp/types_bench_results.txt || \
		echo "Note: Types benchmarks not yet implemented. Create benches/types_bench.rs"

#
# MySQL Docker Targets
#

# Start MySQL Docker container for benchmarks
mysql-start:
	@echo "Starting MySQL Docker container..."
	@./scripts/ensure-mysql-docker.sh || exit 1
	@echo ""
	@echo "MySQL is ready for benchmarks!"
	@echo "Run benchmarks with: make benchmark-tpcc  or  make benchmark-sysbench"

# Stop and remove MySQL Docker container
mysql-stop:
	@echo "Stopping MySQL Docker container..."
	@if docker ps -q -f name=vibesql-mysql-tpch 2>/dev/null | grep -q .; then \
		docker stop vibesql-mysql-tpch && echo "Container stopped"; \
	else \
		echo "Container not running"; \
	fi
	@if docker ps -aq -f name=vibesql-mysql-tpch 2>/dev/null | grep -q .; then \
		docker rm vibesql-mysql-tpch && echo "Container removed"; \
	fi

# Check MySQL Docker container status
mysql-status:
	@if docker ps -q -f name=vibesql-mysql-tpch 2>/dev/null | grep -q .; then \
		echo "MySQL container: RUNNING"; \
		echo "  Container: vibesql-mysql-tpch"; \
		echo "  Port: 3306"; \
		echo "  MYSQL_URL: mysql://root@127.0.0.1:3306/sysbench"; \
	elif docker ps -aq -f name=vibesql-mysql-tpch 2>/dev/null | grep -q .; then \
		echo "MySQL container: STOPPED (use 'make mysql-start' to start)"; \
	else \
		echo "MySQL container: NOT CREATED (use 'make mysql-start' to create)"; \
	fi
