# VibeSQL Makefile
# Convenience targets for common development tasks

.PHONY: all build build-wasm build-python test test-unit test-workspace test-sqllogictest benchmark benchmark-tpch clean help analyze-tests analyze-benchmarks analyze

# Default target - fully qualify and update the state of the repo
all: build test

# Help target
help:
	@echo "VibeSQL Makefile - Common development tasks"
	@echo ""
	@echo "Build targets:"
	@echo "  make build              - Build all Rust crates in release mode"
	@echo "  make build-wasm         - Build WebAssembly bindings for web demo"
	@echo "  make build-python       - Build Python bindings wheel"
	@echo ""
	@echo "Test targets:"
	@echo "  make test               - Run all tests (workspace + sqllogictest)"
	@echo "  make test-unit          - Run unit tests only (lib tests)"
	@echo "  make test-workspace     - Run all workspace tests (unit + integration)"
	@echo "  make test-sqllogictest  - Run SQLLogicTest suite (parallel, 8 workers)"
	@echo ""
	@echo "Benchmark targets:"
	@echo "  make benchmark          - Run all benchmarks (TPC-H)"
	@echo "  make benchmark-tpch     - Run TPC-H benchmark suite (30s timeout)"
	@echo ""
	@echo "Analysis targets:"
	@echo "  make analyze            - Show test and benchmark analysis"
	@echo "  make analyze-tests      - Show SQLLogicTest analysis from database"
	@echo "  make analyze-benchmarks - Show TPC-H benchmark analysis from database"
	@echo ""
	@echo "Utility targets:"
	@echo "  make clean              - Clean build artifacts"
	@echo "  make all                - Build and test everything"
	@echo "  make help               - Show this help message"

#
# Build Targets
#

# Build all Rust crates in release mode
build:
	@echo "Building VibeSQL (release mode)..."
	cargo build --release --workspace

# Build WebAssembly bindings for web demo
build-wasm:
	@echo "Building WebAssembly bindings..."
	./scripts/build-wasm.sh

# Build Python bindings wheel
build-python:
	@echo "Building Python bindings..."
	./scripts/build-python.sh

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
	@echo "Running SQLLogicTest suite (parallel, 8 workers)..."
	@echo "This runs ~5.9M tests across 628 test files (10-20 minutes)"
	./scripts/sqllogictest run --parallel --workers 8

#
# Benchmark Targets
#

# Run all benchmarks with analysis
benchmark: benchmark-tpch analyze-benchmarks

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

# Show TPC-H benchmark analysis from database
analyze-benchmarks:
	@echo ""
	@echo "=========================================="
	@echo "TPC-H Benchmark Analysis"
	@echo "=========================================="
	@./scripts/query_benchmark_results.py --latest 2>/dev/null || echo "Run 'make benchmark-tpch' first to generate benchmark data"
	@echo ""
	@./scripts/query_benchmark_results.py --stats 2>/dev/null || true
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
