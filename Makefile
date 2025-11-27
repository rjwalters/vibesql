# VibeSQL Makefile
# Convenience targets for common development tasks

.PHONY: all build build-all build-wasm build-python test test-unit test-workspace test-sqllogictest benchmark benchmark-tpch benchmark-tpcc benchmark-tpcds benchmark-sysbench clean help analyze-tests analyze-benchmarks analyze

# Default target - fully qualify and update the state of the repo
# Runs build-all (including Python), all tests, benchmarks, and records results to database
all: build-all test benchmark

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
	@echo "  make test               - Run all tests (workspace + sqllogictest)"
	@echo "  make test-unit          - Run unit tests only (lib tests)"
	@echo "  make test-workspace     - Run all workspace tests (unit + integration)"
	@echo "  make test-sqllogictest  - Run SQLLogicTest suite (parallel, 8 workers)"
	@echo ""
	@echo "Benchmark targets:"
	@echo "  make benchmark          - Run all benchmarks (TPC-H, TPC-C, TPC-DS, Sysbench)"
	@echo "  make benchmark-tpch     - Run TPC-H benchmark suite (30s timeout)"
	@echo "  make benchmark-tpcc     - Run TPC-C benchmark suite (60s duration)"
	@echo "  make benchmark-tpcds    - Run TPC-DS benchmark suite (99 queries)"
	@echo "  make benchmark-sysbench - Run Sysbench OLTP benchmarks"
	@echo ""
	@echo "Analysis targets:"
	@echo "  make analyze            - Show test and benchmark analysis"
	@echo "  make analyze-tests      - Show SQLLogicTest analysis from database"
	@echo "  make analyze-benchmarks - Show TPC-H benchmark analysis from database"
	@echo ""
	@echo "Utility targets:"
	@echo "  make clean              - Clean build artifacts"
	@echo "  make all                - Build, test, benchmark, and record all results"
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

# Run all tests (workspace + sqllogictest) with analysis
test: test-workspace test-sqllogictest analyze-tests

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
		$$(find ./target/release/deps -maxdepth 1 -name "tpcc_benchmark-*" -type f ! -name "*.d" | head -1) \
		2>&1 | tee /tmp/tpcc_results.txt
	@echo ""
	@echo "Processing TPC-C results into database..."
	./scripts/process_tpcc_results.py --input /tmp/tpcc_results.txt --scale-factor 1 --duration 60

# Run TPC-DS benchmarks with database tracking
benchmark-tpcds:
	@echo "Running TPC-DS benchmarks..."
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
	./scripts/process_sysbench_results.py --stdin < /tmp/sysbench_results.txt || \
		./scripts/process_sysbench_results.py --criterion-dir target/criterion

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
