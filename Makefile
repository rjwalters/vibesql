# VibeSQL Makefile
# Convenience targets for common development tasks

.PHONY: all all-bg logs status build build-all build-wasm build-python test test-unit test-workspace test-sqllogictest test-sqllogictest-halting test-tcl test-tcl-all test-tcl-file test-tcl-status test-cluster fuzz fuzz-parser fuzz-expr fuzz-type-convert fuzz-query fuzz-type-coercion fuzz-differential fuzz-list benchmark benchmark-quick benchmark-smoke benchmark-all benchmark-all-bg benchmark-logs benchmark-status benchmark-embedded-all benchmark-server-all benchmark-tpch benchmark-tpch-quick benchmark-tpch-profile benchmark-tpch-server benchmark-tpcc benchmark-tpcc-server benchmark-tpcds benchmark-sysbench benchmark-sysbench-server benchmark-vibesql benchmark-sqlite benchmark-duckdb benchmark-cli benchmark-cli-prep benchmark-cli-quick fmt fmt-check clean help analyze-tests analyze-benchmarks analyze profile-tpch profile-tpcc profile-sysbench profile-select profile-query bench-storage bench-executor bench-types website mysql-start mysql-stop mysql-status strip-quarantine

# Default target: show help
.DEFAULT_GOAL := help

# Log file locations for background runs
LOG_FILE := /tmp/vibesql-make-all.log
PID_FILE := /tmp/vibesql-make-all.pid
BENCH_LOG_FILE := /tmp/vibesql-benchmark-all.log
BENCH_PID_FILE := /tmp/vibesql-benchmark-all.pid

# Run all targets in foreground (build, test) - DEFAULT
# Use 'make all-bg' to run in background
all:
	@./scripts/make-all

# Build everything in background since it takes a long time
all-bg:
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  Starting 'make all' in background (build + test)"
	@echo "══════════════════════════════════════════════════════════════════"
	@echo ""
	@echo "  Log file: $(LOG_FILE)"
	@echo ""
	@echo "  Monitor progress:"
	@echo "    make status    - Quick status check with last 10 lines"
	@echo "    make logs      - Follow full output (Ctrl+C to stop)"
	@echo "    tail -f $(LOG_FILE)"
	@echo ""
	@nohup $(MAKE) all > $(LOG_FILE) 2>&1 & echo $$! > $(PID_FILE)
	@echo "  PID: $$(cat $(PID_FILE))"
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"

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
			echo "✓ make all-bg is running (PID: $$PID)"; \
			echo ""; \
			echo "Last 10 lines of output:"; \
			tail -10 $(LOG_FILE) 2>/dev/null || echo "(no output yet)"; \
		else \
			echo "✗ make all-bg has finished"; \
			echo ""; \
			echo "Exit status (last 20 lines):"; \
			tail -20 $(LOG_FILE) 2>/dev/null; \
		fi; \
	else \
		echo "No background make running. Use 'make all-bg' to start."; \
	fi

# Tail the background benchmark output
benchmark-logs:
	@if [ -f $(BENCH_LOG_FILE) ]; then \
		tail -f $(BENCH_LOG_FILE); \
	else \
		echo "No benchmark log file found. Run 'make benchmark-all' first."; \
	fi

# Check status of background benchmark
benchmark-status:
	@if [ -f $(BENCH_PID_FILE) ]; then \
		PID=$$(cat $(BENCH_PID_FILE)); \
		if ps -p $$PID > /dev/null 2>&1; then \
			echo "✓ make benchmark-all-bg is running (PID: $$PID)"; \
			echo ""; \
			echo "Last 10 lines of output:"; \
			tail -10 $(BENCH_LOG_FILE) 2>/dev/null || echo "(no output yet)"; \
		else \
			echo "✗ make benchmark-all-bg has finished"; \
			echo ""; \
			echo "Exit status (last 30 lines):"; \
			tail -30 $(BENCH_LOG_FILE) 2>/dev/null; \
		fi; \
	else \
		echo "No background benchmark running. Use 'make benchmark-all-bg' to start."; \
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
	@echo "  make test-sqllogictest  - Run SQLLogicTest standalone (with JSON output)"
	@echo "  make test-sqllogictest-halting - Run SQLLogicTest, stop on first failure"
	@echo "  make test-tcl           - Run SQLite TCL tests (Priority 1 - core SQL)"
	@echo "  make test-tcl-all       - Run all SQLite TCL tests (1174 files)"
	@echo "  make test-tcl-file FILE=X - Run specific TCL test file"
	@echo "  make test-tcl-status    - Show TCL test status"
	@echo "  make test-cluster       - Run 3-node TCP consensus cluster smoke tests"
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
	@echo "  make benchmark-all       - FULL matrix: embedded + server + MySQL (requires Docker)"
	@echo "  make benchmark-all-bg    - Run 'make benchmark-all' in background"
	@echo "  make benchmark-logs      - Tail the background benchmark output"
	@echo "  make benchmark-status    - Check if background benchmark is running"
	@echo "  make start-mysql         - Start MySQL Docker container for benchmarks"
	@echo "  make stop-mysql          - Stop MySQL Docker container"
	@echo ""
	@echo "Embedded benchmarks (VibeSQL, SQLite, DuckDB - in-process databases):"
	@echo "  make benchmark-embedded-all  - All embedded benchmarks × all embedded engines"
	@echo "  make benchmark-tpch          - TPC-H (embedded engines)"
	@echo "  make benchmark-tpcc          - TPC-C OLTP benchmark (embedded engines)"
	@echo "  make benchmark-tpcds         - TPC-DS decision support (embedded engines)"
	@echo "  make benchmark-sysbench      - Sysbench OLTP (embedded engines)"
	@echo ""
	@echo "Server benchmarks (VibeSQL-server, MySQL - client-server, requires Docker for MySQL):"
	@echo "  make benchmark-server-all    - All server benchmarks × all server engines"
	@echo "  make benchmark-tpch-server   - TPC-H via client-server protocol"
	@echo "  make benchmark-tpcc-server   - TPC-C OLTP via client-server protocol"
	@echo "  make benchmark-sysbench-server - Sysbench via client-server protocol"
	@echo ""
	@echo "Per-engine benchmark targets:"
	@echo "  make benchmark-vibesql   - All embedded benchmarks for VibeSQL only"
	@echo "  make benchmark-sqlite    - All embedded benchmarks for SQLite only"
	@echo "  make benchmark-duckdb    - All embedded benchmarks for DuckDB only"
	@echo ""
	@echo "CLI benchmarks (apples-to-apples via CLI tools):"
	@echo "  make benchmark-cli       - Run CLI-based TPC-H benchmark (vibesql vs sqlite3)"
	@echo "  make benchmark-cli-quick - Quick CLI benchmark (Q1, Q6 only)"
	@echo "  make benchmark-cli-prep  - Prepare databases for CLI benchmarks"
	@echo ""
	@echo "Profiling targets (uses samply, no sudo required):"
	@echo "  make profile-tpch       - Profile TPC-H queries (opens Firefox Profiler)"
	@echo "  make profile-tpcc       - Profile TPC-C transactions"
	@echo "  make profile-sysbench   - Profile Sysbench OLTP"
	@echo "  make profile-select     - Profile point SELECT operations"
	@echo "  make profile-query Q=X  - Profile specific TPC-H query (e.g., Q=Q6)"
	@echo "  (Requires: cargo install samply)"
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
	@echo "  make fmt                - Format code using nightly rustfmt"
	@echo "  make fmt-check          - Check formatting without making changes"
	@echo "  make clean              - Clean build artifacts"
	@echo "  make website            - Regenerate web dashboard data from benchmark database"
	@echo "  make all                - Build, test, and run TCL tests (foreground)"
	@echo "  make all-bg             - Run 'make all' in background"
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

# Run all workspace tests (unit + integration, then compliance last)
test-workspace:
	@echo "Running workspace tests..."
	@echo "Phase 1: Unit tests (lib)"
	cargo test --release --workspace --lib --no-run
	@$(MAKE) strip-quarantine
	cargo test --release --workspace --lib
	@echo ""
	@echo "Phase 2: Doc tests"
	cargo test --release --workspace --doc
	@echo ""
	@echo "Phase 3: Integration tests (excluding compliance)"
	@./scripts/run-integration-tests.sh
	@echo ""
	@echo "Phase 4: Compliance tests (sqllogictest)"
	cargo test --release --workspace --test sqllogictest_suite --test sqllogictest_basic --test sqllogictest_benchmark --test sqllogictest_runner --test sqllogictest_sqlite --test sqltest_conformance --test pgsql_regress
	@# Process SQLLogicTest results into database for analysis
	@if [ -f target/sqllogictest_results.json ]; then \
		echo "Processing SQLLogicTest results into database..."; \
		python3 scripts/process_test_results.py --input target/sqllogictest_results.json 2>/dev/null || true; \
	fi

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

# Run SQLite TCL test suite (Priority 1 - core SQL tests)
# This is the canonical SQLite conformance test suite
# Uses errors-only mode by default. Add --verbose for full output.
test-tcl:
	@echo "Running SQLite TCL test suite (Priority 1 - core SQL)..."
	@echo "Tests: select, insert, update, delete, where, join, aggregate, func"
	./scripts/tcltest run --priority 1

# Run all SQLite TCL tests (all priorities)
# Uses errors-only mode by default. Add --verbose for full output.
test-tcl-all:
	@echo "Running full SQLite TCL test suite (all 1174 files)..."
	./scripts/tcltest run

# Run specific TCL test file
test-tcl-file:
	@if [ -z "$(FILE)" ]; then \
		echo "Usage: make test-tcl-file FILE=select1.test"; \
		exit 1; \
	fi
	./scripts/tcltest test $(FILE)

# Show TCL test status
test-tcl-status:
	./scripts/tcltest status

# Run the multi-node consensus cluster smoke tests (Raft Phase A3, #5197).
# Boots 3-voter clusters on localhost (ephemeral ports, durable Raft logs)
# wired by the TCP transport and exercises election, replication, leader
# kill -> re-election, restart -> catch-up, a 2-1 minority partition, and
# garbage frames on the wire. Exits nonzero on any failure.
test-cluster:
	cargo test --release -p vibesql-consensus --test tcp_cluster -- --nocapture

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

# Run all benchmarks (TPC-H, TPC-C, TPC-DS, Sysbench) - VibeSQL only
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

# Start MySQL Docker container for benchmarks
# This is automatically called by benchmark-all, but can be run manually
start-mysql:
	@./scripts/ensure-mysql-docker.sh

# Stop MySQL Docker container
stop-mysql:
	@docker stop vibesql-mysql-tpch 2>/dev/null || echo "MySQL container not running"

# Full benchmark matrix: embedded + server benchmarks
# Runs all benchmarks (TPC-H, TPC-C, TPC-DS, Sysbench) with timing report
# Note: Tests are NOT included - run 'make test' separately
# Note: Requires Docker for MySQL comparison benchmarks
benchmark-all:
	@./scripts/bench-all

# Run benchmark-all in background
benchmark-all-bg:
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  Starting 'make benchmark-all' in background"
	@echo "══════════════════════════════════════════════════════════════════"
	@echo ""
	@echo "  Log file: $(BENCH_LOG_FILE)"
	@echo ""
	@echo "  Monitor progress:"
	@echo "    make benchmark-status    - Quick status check with last 10 lines"
	@echo "    make benchmark-logs      - Follow full output (Ctrl+C to stop)"
	@echo "    tail -f $(BENCH_LOG_FILE)"
	@echo ""
	@nohup $(MAKE) benchmark-all > $(BENCH_LOG_FILE) 2>&1 & echo $$! > $(BENCH_PID_FILE)
	@echo "  PID: $$(cat $(BENCH_PID_FILE))"
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"

#
# Embedded Benchmarks (in-process databases: VibeSQL, SQLite, DuckDB)
#

# All embedded benchmarks × all embedded engines
benchmark-embedded-all:
	@echo "Running all EMBEDDED benchmarks (VibeSQL, SQLite, DuckDB)..."
	@echo "Tests: TPC-H, TPC-C, TPC-DS, Sysbench"
	@./scripts/bench --test=all --engine=vibesql,sqlite,duckdb

# Run TPC-H benchmark (embedded engines only)
benchmark-tpch:
	@echo "Running TPC-H benchmarks (embedded engines)..."
	@./scripts/bench --test=tpch --engine=vibesql,sqlite,duckdb

# Run TPC-H benchmark (VibeSQL only - fast iteration)
benchmark-tpch-quick:
	@echo "Running TPC-H benchmarks (VibeSQL only)..."
	@./scripts/bench --test=tpch --engine=vibesql

# Run TPC-H profiling (detailed timing breakdown per phase)
benchmark-tpch-profile:
	@echo "Running TPC-H profiling (detailed timing)..."
	@./scripts/bench-tpch.sh --mode standard --timeout 30

# Run TPC-C benchmark (embedded engines)
benchmark-tpcc:
	@echo "Running TPC-C benchmarks (embedded engines)..."
	@./scripts/bench --test=tpcc --engine=vibesql,sqlite,duckdb

# Run TPC-DS benchmark (embedded engines)
benchmark-tpcds:
	@echo "Running TPC-DS benchmarks (embedded engines)..."
	@./scripts/bench --test=tpcds --engine=vibesql,sqlite,duckdb

# Run Sysbench OLTP benchmark (embedded engines)
benchmark-sysbench:
	@echo "Running Sysbench benchmarks (embedded engines)..."
	@./scripts/bench --test=sysbench --engine=vibesql,sqlite,duckdb

#
# CLI Benchmarks (apples-to-apples comparison via CLI tools)
#

# Prepare TPC-H databases for CLI benchmarks
# Creates pre-built database files in /tmp/tpch_bench for fast CLI testing
benchmark-cli-prep:
	@echo "Preparing TPC-H databases for CLI benchmarks..."
	@cargo build --release -p vibesql-executor --bench prep_tpch_databases --features sqlite --quiet 2>/dev/null || cargo build --release -p vibesql-executor --bench prep_tpch_databases --features sqlite
	@find ./target/release/deps -name 'prep_tpch_databases-*' -type f -perm +111 -exec {} --sqlite \;

# Run CLI-based TPC-H benchmark (apples-to-apples via CLI tools)
# Uses vibesql and sqlite3 CLI tools for fair comparison
benchmark-cli: benchmark-cli-prep
	@echo "Running CLI benchmarks (vibesql vs sqlite3)..."
	@./scripts/bench-cli --db-dir /tmp/tpch_bench

# Run CLI benchmark with specific queries
benchmark-cli-quick: benchmark-cli-prep
	@echo "Running quick CLI benchmark (Q1, Q6)..."
	@./scripts/bench-cli --db-dir /tmp/tpch_bench --queries Q1,Q6 --iterations 3

#
# Server Benchmarks (client-server databases: VibeSQL-server, MySQL)
#

# All server benchmarks × all server engines
benchmark-server-all:
	@echo "Running all SERVER benchmarks (VibeSQL-server, MySQL)..."
	@echo "Tests: TPC-H (server), TPC-C (server), Sysbench (server)"
	@./scripts/bench --test=tpch-server --engine=vibesql-server,mysql
	@./scripts/bench --test=tpcc-server --engine=vibesql-server,mysql
	@./scripts/bench --test=sysbench-server --engine=vibesql-server,mysql

# Run TPC-H via client-server protocol
benchmark-tpch-server:
	@echo "Running TPC-H server benchmarks..."
	@./scripts/bench --test=tpch-server --engine=vibesql-server,mysql

# Run TPC-C via client-server protocol
benchmark-tpcc-server:
	@echo "Running TPC-C server benchmarks..."
	@./scripts/bench --test=tpcc-server --engine=vibesql-server,mysql

# Run Sysbench via client-server protocol
benchmark-sysbench-server:
	@echo "Running Sysbench server benchmarks..."
	@./scripts/bench --test=sysbench-server --engine=vibesql-server,mysql

#
# Per-Engine Benchmark Targets (embedded only)
#

# Run all embedded benchmarks for VibeSQL only (~1 hour)
benchmark-vibesql:
	@echo "Running all embedded benchmarks for VibeSQL only..."
	@./scripts/bench --test=all --engine=vibesql

# Run all embedded benchmarks for SQLite only (~1.5 hours)
benchmark-sqlite:
	@echo "Running all embedded benchmarks for SQLite only..."
	@./scripts/bench --test=all --engine=sqlite

# Run all embedded benchmarks for DuckDB only (~2 hours)
benchmark-duckdb:
	@echo "Running all embedded benchmarks for DuckDB only..."
	@./scripts/bench --test=all --engine=duckdb

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
	@if [ -f ~/.vibesql/test_results/sqllogictest_results.vbsql ]; then \
		python3 scripts/query_test_results.py --preset analyze-summary --database ~/.vibesql/test_results/sqllogictest_results.vbsql 2>/dev/null || true; \
		FAILS=$$(echo "SELECT COUNT(*) as cnt FROM test_results WHERE status IN ('FAIL', 'TIMEOUT');" | ./target/release/vibesql ~/.vibesql/test_results/sqllogictest_results.vbsql 2>/dev/null | sed -n '4p' | tr -d '| '); \
		if [ "$$FAILS" = "0" ] || [ -z "$$FAILS" ]; then \
			echo ""; \
			echo "✓ All SQLLogicTests passing - no failures to analyze"; \
		else \
			./scripts/sqllogictest analyze --top-fixes 2>/dev/null; \
		fi \
	else \
		echo "Run tests first to generate database:"; \
		echo "  ./scripts/sqllogictest run"; \
	fi
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

# Format code using nightly rustfmt (required for all config options)
fmt:
	@echo "Formatting code with nightly rustfmt..."
	cargo +nightly fmt

# Check formatting without making changes
fmt-check:
	@echo "Checking code formatting..."
	cargo +nightly fmt --check

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	rm -rf web-demo/public/pkg
	rm -rf target/wheels
	rm -f target/sqllogictest_*.json
	rm -f /tmp/tpch_results.txt
	rm -f profile-*.json.gz

# Regenerate web demo data from VibeSQL benchmark database (dogfooding)
# Single unified script exports all data in one database load
website:
	@python3 ./scripts/export_website_data.py || echo "Note: Run 'make benchmark-all' first to populate the database"
	@echo ""
	@echo "Run 'cd web-demo && pnpm run build' to rebuild the site"

#
# Profiling Targets
#
# Uses samply profiler (no sudo required, opens Firefox Profiler in browser).
# Install: cargo install samply
#

# Profile TPC-H queries
profile-tpch:
	@echo "Profiling TPC-H queries..."
	./scripts/flamegraph.sh tpch

# Profile TPC-C transactions
profile-tpcc:
	@echo "Profiling TPC-C transactions..."
	./scripts/flamegraph.sh tpcc

# Profile Sysbench OLTP
profile-sysbench:
	@echo "Profiling Sysbench OLTP..."
	./scripts/flamegraph.sh sysbench

# Profile point SELECT operations
profile-select:
	@echo "Profiling SELECT operations..."
	./scripts/flamegraph.sh select

# Profile specific TPC-H query
# Usage: make profile-query Q=Q6
profile-query:
ifndef Q
	@echo "Usage: make profile-query Q=<query>"
	@echo "Example: make profile-query Q=Q6"
	@exit 1
endif
	@echo "Profiling TPC-H query: $(Q)"
	./scripts/flamegraph.sh tpch $(Q)

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
