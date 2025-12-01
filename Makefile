# VibeSQL Makefile
# Convenience targets for common development tasks

.PHONY: all all-fg logs status build build-all build-wasm build-python test test-unit test-workspace test-sqllogictest benchmark benchmark-tpch benchmark-tpcc benchmark-tpcds benchmark-tpcds-all benchmark-sysbench clean help analyze-tests analyze-benchmarks analyze flamegraph-tpch flamegraph-tpcc flamegraph-sysbench flamegraph-select profile-query bench-storage bench-executor bench-types website

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
	@echo "  make test-sqllogictest  - Run SQLLogicTest standalone (with JSON output)"
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

# Run SQLLogicTest suite (parallel mode recommended)
test-sqllogictest:
	@echo "Running SQLLogicTest suite (parallel, auto-detected workers)..."
	@echo "This runs ~5.9M tests across 628 test files"
	./scripts/sqllogictest run --parallel

#
# Benchmark Targets
#

# Run all benchmarks with analysis
benchmark: benchmark-tpch benchmark-tpcc benchmark-tpcds benchmark-sysbench analyze-benchmarks

# Run TPC-H benchmarks with 30s timeout per query and store results in database
benchmark-tpch:
	@echo "Running TPC-H benchmarks..."
	./scripts/bench-tpch.sh 30
	@echo ""
	@echo "Processing benchmark results into database..."
	./scripts/process_benchmark_results.py --input /tmp/tpch_results.txt --timeout 30
	@echo ""
	@echo "Query results:"
	@echo "  ./scripts/query_benchmark_results.py --latest"
	@echo "  ./scripts/query_benchmark_results.py --trend"

# Run TPC-C benchmarks (OLTP workload) with database tracking
benchmark-tpcc:
	@echo "Running TPC-C benchmarks..."
	@echo "Building TPC-C benchmark..."
	cargo bench --package vibesql-executor --bench tpcc_benchmark --features benchmark-comparison --no-run
	@echo ""
	@echo "Running TPC-C benchmark (60s duration, 10s warmup)..."
	TPCC_DURATION_SECS=60 TPCC_WARMUP_SECS=10 TPCC_SCALE_FACTOR=1 \
		$$(find ./target/release/deps -maxdepth 1 -name "tpcc_benchmark-*" -type f ! -name "*.d" ! -name "*.o" -perm +111 | head -1) \
		2>&1 | tee /tmp/tpcc_results.txt
	@echo ""
	@echo "Processing TPC-C results into database..."
	./scripts/process_tpcc_results.py --input /tmp/tpcc_results.txt --scale-factor 1 --duration 60

# Run TPC-DS benchmarks with database tracking
# Uses isolated execution (each database engine in separate process) to avoid memory pressure
benchmark-tpcds:
	@echo "Running TPC-DS benchmarks (isolated mode)..."
	./scripts/bench-tpcds-isolated.sh /tmp/tpcds_results.txt
	@echo ""
	@echo "Processing TPC-DS results into database..."
	./scripts/process_tpcds_results.py --stdin < /tmp/tpcds_results.txt || \
		./scripts/process_tpcds_results.py --criterion-dir target/criterion

# Run TPC-DS benchmarks with all engines simultaneously (may cause memory pressure)
benchmark-tpcds-all:
	@echo "Running TPC-DS benchmarks (all engines simultaneously)..."
	@echo "WARNING: This may cause memory pressure. Use 'make benchmark-tpcds' for isolated execution."
	cargo bench --package vibesql-executor --bench tpcds_benchmark --features benchmark-comparison -- --noplot 2>&1 | tee /tmp/tpcds_results.txt
	@echo ""
	@echo "Processing TPC-DS results into database..."
	./scripts/process_tpcds_results.py --stdin < /tmp/tpcds_results.txt || \
		./scripts/process_tpcds_results.py --criterion-dir target/criterion

# Run Sysbench OLTP benchmarks with database tracking
benchmark-sysbench:
	@echo "Running Sysbench OLTP benchmarks..."
	cargo bench --package vibesql-executor --bench sysbench_oltp --features benchmark-comparison -- --noplot 2>&1 | tee /tmp/sysbench_results.txt
	@echo ""
	@echo "Processing Sysbench results into database..."
	./scripts/process_sysbench_results.py --criterion-dir target/criterion || \
		./scripts/process_sysbench_results.py --stdin < /tmp/sysbench_results.txt

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
