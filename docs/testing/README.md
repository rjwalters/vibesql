# Testing Documentation

This directory contains documentation for VibeSQL's testing infrastructure and conformance validation.

## Quick Links

| Document | Purpose |
|----------|---------|
| [TESTING_STRATEGY.md](TESTING_STRATEGY.md) | Overall testing approach, SQL:1999 and SQLLogicTest strategies |
| [SQL1999_CONFORMANCE.md](SQL1999_CONFORMANCE.md) | Current conformance status (auto-generated) |
| [sqllogictest/](sqllogictest/) | SQLLogicTest suite documentation |

## Test Suites

### SQL:1999 Conformance Tests

739 tests extracted from [sqltest](https://github.com/elliotchance/sqltest) covering Core SQL:1999 features.

```bash
# Run SQL:1999 conformance tests
cargo test --test sqltest_conformance --release

# Generate conformance report
./scripts/generate_compliance_report.sh
```

### SQLLogicTest Suite

Industry-standard test framework used by SQLite, DuckDB, and other databases.

```bash
# Run full suite with parallel workers
./scripts/sqllogictest run --parallel

# Run specific test file
./scripts/sqllogictest test random/select/slt_good_19.test

# Query results
./scripts/sqllogictest query --preset by-category
```

See [sqllogictest/QUICKSTART.md](sqllogictest/QUICKSTART.md) for detailed usage.

### SQLite TCL Test Suite

1,174 canonical SQLite tests for conformance validation.

```bash
# Run Priority 1 tests (core SQL)
make test-tcl

# Run all tests
make test-tcl-all

# Run specific test file
make test-tcl-file FILE=select1.test
```

### Code Coverage

```bash
# Generate HTML coverage report
cargo coverage

# Generate lcov.info for CI
cargo coverage-lcov
```

## Related Documentation

- [Benchmarking Guide](../development/BENCHMARKING.md) - Performance benchmarking
- [Performance Profiling](../performance/CPU_PROFILING.md) - Profiling tools decision tree
- [Feature Status](../reference/FEATURE_STATUS.md) - SQL feature implementation status
