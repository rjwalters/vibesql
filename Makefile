# VibeSQL Makefile
# Convenience targets for common development tasks

.PHONY: all all-fg logs status build build-all build-wasm build-python test test-unit test-workspace test-ignored test-sqllogictest test-sqllogictest-halting fuzz fuzz-parser fuzz-expr fuzz-type-convert fuzz-query fuzz-type-coercion fuzz-differential fuzz-list benchmark benchmark-quick benchmark-smoke benchmark-all benchmark-tpch benchmark-tpch-quick benchmark-tpch-profile benchmark-tpcc benchmark-tpcds benchmark-tpcds-all benchmark-sysbench clean help analyze-tests analyze-benchmarks analyze flamegraph-tpch flamegraph-tpcc flamegraph-sysbench flamegraph-select profile-query bench-storage bench-executor bench-types website mysql-start mysql-stop mysql-status strip-quarantine

# Default target: show help
.DEFAULT_GOAL := help

# Log file location for background runs
LOG_FILE := /tmp/vibesql-make-all.log
PID_FILE := /tmp/vibesql-make-all.pid

# Build everything in background since it takes a long time
# Use 'make all-fg' for foreground execution
all:
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  Starting 'make all' in background (build + test + benchmark-quick)"
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
all-fg: build-all test benchmark-quick

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
	@echo "Fuzzing targets:"
	@echo "  make fuzz               - Run all fuzz targets (5 min each)"
	@echo "  make fuzz-parser        - Fuzz SQL parser"
	@echo "  make fuzz-expr          - Fuzz expression evaluation contexts"
	@echo "  make fuzz-type-convert  - Fuzz CAST type conversion"
	@echo "  make fuzz-query         - Fuzz query execution (parser + executor)"
	@echo "  make fuzz-type-coercion - Fuzz type coercion and comparison"
	@echo "  make fuzz-differential  - Fuzz with SQLite differential testing"
	@echo "  make fuzz-list          - List available fuzz targets"
	@echo ""
	@echo "Benchmark targets:"
	@echo "  make benchmark           - Run all benchmarks, VibeSQL only (~2.5 hours)"
	@echo "  make benchmark-quick     - Quick CI run with reduced iterations (~25 min)"
	@echo "  make benchmark-smoke     - Smoke test for pipeline validation (~30s)"
	@echo "  make benchmark-all       - FULL matrix: all tests × all engines (~8+ hours)"
	@echo "  make benchmark-tpch      - Run TPC-H (all 4 engines: VibeSQL, SQLite, DuckDB, MySQL)"
	@echo "  make benchmark-tpch-quick - Run TPC-H (VibeSQL only - fast iteration)"
	@echo "  make benchmark-tpch-profile - Run TPC-H profiling (detailed timing breakdown)"
	@echo "  make benchmark-tpcc      - Run TPC-C benchmark suite (60s duration)"
	@echo "  make benchmark-tpcds     - Run TPC-DS benchmark suite (isolated, memory-safe)"
	@echo "  make benchmark-tpcds-all - Run TPC-DS with all engines simultaneously (may OOM)"
	@echo "  make benchmark-sysbench  - Run Sysbench OLTP benchmarks"
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
	@echo "  make all                - Build, test, benchmark-quick (runs in BACKGROUND by default)"
	@echo "  make all-fg             - Run 'make all' in foreground (blocking)"
	@echo "  make logs               - Tail the background make output"
	@echo "  make status             - Check if background make is running and show recent output"
	@echo "  make help               - Show this help message"

#
# Build Targets
#

# Strip macOS quarantine attribute from built binaries
# macOS quarantines downloaded/compiled binaries. This removes the attribute so they can run.
strip-quarantine:
	@if [ "$$(uname)" = "Darwin" ]; then \
		find target -type f -perm +111 -exec xattr -d com.apple.quarantine {} \; 2>/dev/null || true; \
	fi

# Build all Rust crates in release mode (excludes Python bindings which require maturin)
build:
	@echo "Building VibeSQL (release mode)..."
	cargo build --release --workspace --exclude vibesql-python-bindings
	@$(MAKE) strip-quarantine

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
	cargo test --release --workspace --lib --no-run
	@$(MAKE) strip-quarantine
	cargo test --release --workspace --lib

# Run all workspace tests (unit + integration)
test-workspace:
	@echo "Running workspace tests (unit + integration)..."
	@echo "This includes 2,991 unit tests + 739 sqltest conformance tests"
	cargo test --release --workspace --no-run
	@$(MAKE) strip-quarantine
	cargo test --release --workspace

# Run only ignored/slow tests (disk-backed indexes, unimplemented features, etc.)
test-ignored:
	@echo "Running ignored tests only..."
	@echo "These are slow tests that are skipped during normal test runs"
	cargo test --release --workspace --no-run
	@$(MAKE) strip-quarantine
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
# Fuzzing Targets
#
# Requires: rustup install nightly && cargo +nightly install cargo-fuzz
# See fuzz/README.md for detailed usage
#

# Default fuzz duration (in seconds)
FUZZ_DURATION ?= 300

# Run all fuzz targets
fuzz:
	@echo "Running all fuzz targets ($(FUZZ_DURATION)s each)..."
	@echo "Targets: sql_parser, expr_eval, type_convert, query_executor, type_coercion, differential_sqlite"
	@echo ""
	cd fuzz && cargo +nightly fuzz run sql_parser -- -dict=dictionaries/sql.dict -max_total_time=$(FUZZ_DURATION)
	cd fuzz && cargo +nightly fuzz run expr_eval -- -max_total_time=$(FUZZ_DURATION)
	cd fuzz && cargo +nightly fuzz run type_convert -- -max_total_time=$(FUZZ_DURATION)
	cd fuzz && cargo +nightly fuzz run query_executor -- -dict=dictionaries/sql.dict -max_total_time=$(FUZZ_DURATION)
	cd fuzz && cargo +nightly fuzz run type_coercion -- -max_total_time=$(FUZZ_DURATION)
	cd fuzz && cargo +nightly fuzz run differential_sqlite -- -dict=dictionaries/sql.dict -max_total_time=$(FUZZ_DURATION)

# Fuzz SQL parser
fuzz-parser:
	@echo "Fuzzing SQL parser..."
	cd fuzz && cargo +nightly fuzz run sql_parser -- -dict=dictionaries/sql.dict

# Fuzz expression evaluation
fuzz-expr:
	@echo "Fuzzing expression evaluation..."
	cd fuzz && cargo +nightly fuzz run expr_eval

# Fuzz CAST type conversion
fuzz-type-convert:
	@echo "Fuzzing CAST type conversion..."
	cd fuzz && cargo +nightly fuzz run type_convert

# Fuzz query execution (parser + planner + executor)
fuzz-query:
	@echo "Fuzzing query execution..."
	cd fuzz && cargo +nightly fuzz run query_executor -- -dict=dictionaries/sql.dict

# Fuzz type coercion and comparison
fuzz-type-coercion:
	@echo "Fuzzing type coercion..."
	cd fuzz && cargo +nightly fuzz run type_coercion

# Fuzz with SQLite differential testing
fuzz-differential:
	@echo "Fuzzing with SQLite differential testing..."
	cd fuzz && cargo +nightly fuzz run differential_sqlite -- -dict=dictionaries/sql.dict

# List available fuzz targets
fuzz-list:
	@echo "Available fuzz targets:"
	@echo ""
	@cd fuzz && cargo +nightly fuzz list 2>/dev/null || echo "  (install cargo-fuzz: cargo +nightly install cargo-fuzz)"

#
# Benchmark Targets
#
# Use ./scripts/bench directly for full flexibility:
#   ./scripts/bench --help              Show all options
#   ./scripts/bench --test=tpch         Run TPC-H only
#   ./scripts/bench --test=tpcc         Run TPC-C only
#   ./scripts/bench --engine=mysql      Compare against MySQL
#   ./scripts/bench --query=Q1,Q6       Run specific queries
#

# Run all benchmarks (TPC-H, TPC-C, TPC-DS, Sysbench)
benchmark:
	@./scripts/bench --all

# Quick benchmark subset for CI (fast, no comparisons)
benchmark-quick:
	@./scripts/bench --quick

# Smoke test benchmark for pipeline validation (~30s total)
# Purpose: Validate data collection, storage, and analysis pipeline
# Runs minimal queries with short durations to test the full flow
benchmark-smoke:
	@./scripts/bench --smoke --all

# Full benchmark matrix: all tests × all engines
# This is the most comprehensive benchmark (~8+ hours)
# Runs TPC-H, TPC-C, TPC-DS, Sysbench against VibeSQL, SQLite, DuckDB, MySQL
benchmark-all:
	@echo "Running FULL benchmark matrix (all tests × all engines)..."
	@echo "Expected time: 8+ hours"
	@./scripts/bench --test=all --engine=all

#
# Individual Benchmark Targets
#

# Run TPC-H benchmark (all 4 engines: VibeSQL, SQLite, DuckDB, MySQL)
benchmark-tpch:
	@echo "Running TPC-H benchmarks (all engines)..."
	@./scripts/bench --test=tpch --engine=all

# Run TPC-H benchmark (VibeSQL only - fast iteration)
benchmark-tpch-quick:
	@echo "Running TPC-H benchmarks (VibeSQL only)..."
	@./scripts/bench --test=tpch --engine=vibesql

# Run TPC-H profiling (detailed timing breakdown per phase)
benchmark-tpch-profile:
	@echo "Running TPC-H profiling (detailed timing)..."
	@./scripts/bench-tpch.sh --mode standard --timeout 30

# Run TPC-C benchmark (OLTP workload)
benchmark-tpcc:
	@./scripts/bench --test=tpcc

# Run TPC-DS benchmark (decision support queries)
benchmark-tpcds:
	@./scripts/bench --test=tpcds

# Run TPC-DS benchmark with all engines (may require significant memory)
benchmark-tpcds-all:
	@./scripts/bench --test=tpcds --engine=all

# Run Sysbench OLTP benchmark
benchmark-sysbench:
	@./scripts/bench --test=sysbench

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
