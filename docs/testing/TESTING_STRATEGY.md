# Testing Strategy

## Overview

This document describes VibeSQL's comprehensive testing strategy, including SQL:1999 conformance, SQLLogicTest integration, and SQLite TCL test suite coverage.

## Current Status

| Test Suite | Status | Pass Rate |
|------------|--------|-----------|
| SQL:1999 Conformance | ✅ Complete | 100% (739/739) |
| SQLLogicTest | ✅ Integrated | See [SQL1999_CONFORMANCE.md](SQL1999_CONFORMANCE.md) |
| SQLite TCL Tests | ✅ Integrated | 1,174 test files |

## Test Suites

### SQL:1999 Conformance Tests

**Source**: [sqltest by Elliot Chance](https://github.com/elliotchance/sqltest)

- **Location**: `tests/sql1999/manifest.json`
- **Test Count**: 739 tests covering Core SQL:1999 features
- **Organization**: Feature-based by SQL standard codes (E011, E021, F031, etc.)
- **Pass Rate**: **100%** (739/739 tests passing)

**Test Runner**: `tests/sqltest_conformance.rs`

**Coverage includes**:
- **E011**: Numeric data types and arithmetic
- **E021**: Character string types (CHAR, VARCHAR)
- **E051**: Basic query specification
- **E061**: Predicates and search conditions
- **E091**: Set functions (aggregates)
- **E141**: Integrity constraints
- **E151**: Transaction support
- **F031**: Basic schema manipulation
- **F051**: Date/Time types and operations

See `tests/sql1999/README.md` for details.

### Execution Strategy

**Current**: Direct Rust API testing
- Tests instantiate `Database` struct directly
- Execute SQL via parser → executor pipeline
- No protocol overhead (ODBC/JDBC)
- Fast iteration during development
- Identifies parser and executor gaps

**Future**: ODBC/JDBC protocol testing (see "Planned Enhancements" below)

### Automated Testing & Reporting

**GitHub Actions Integration**: `.github/workflows/deploy-demo.yml`

The CI pipeline automatically runs conformance tests and generates compliance artifacts:

1. **Test Execution** (lines 71-73):
   ```yaml
   - name: Run sqltest conformance suite
     run: cargo test --test sqltest_conformance --release -- --nocapture
     continue-on-error: true
   ```

2. **Badge Generation** (lines 76-111):
   ```yaml
   - name: Generate badge JSON
     run: |
       mkdir -p badges
       PASS_RATE=$(jq -r '.pass_rate // "0"' target/sqltest_results.json)

       # Determine color: 80%+ green, 60%+ yellow, 40%+ orange, <40% red
       # Creates shields.io endpoint format badge
       cat > badges/sql1999-conformance.json <<JSON
       {
         "schemaVersion": 1,
         "label": "SQL:1999",
         "message": "${PASS_RATE}%",
         "color": "$COLOR"
       }
       JSON
   ```

3. **Deployment**: Badge and results deployed to GitHub Pages
   - **Badge Endpoint**: https://vibesql.org/badges/sql1999-conformance.json
   - **README Badge**: Displays live conformance percentage (README.md:7)

**Compliance Report Generation**: `scripts/generate_compliance_report.sh`

Automated script that parses test results and generates `docs/SQL1999_CONFORMANCE.md`:

```bash
# Run after tests complete
./scripts/generate_compliance_report.sh

# Generates report with:
# - Summary metrics (total, passed, failed, errors, pass rate)
# - Test coverage details
# - Known gaps (parser/executor)
# - Improvement roadmap
```

The report provides:
- Summary metrics table
- Feature coverage breakdown
- Known implementation gaps (parser and executor)
- Phased improvement roadmap
- Local testing instructions

## Planned Enhancements

This section describes future testing strategies that are not yet implemented but are being considered for comprehensive SQL:1999 validation.

### Phase 1: ODBC/JDBC Protocol Testing (Planned)

**Status**: Not yet implemented (awaiting ODBC/JDBC driver development)

**Rationale**: Per upstream requirements, conformance tests should execute correctly through both ODBC and JDBC protocols to validate protocol compliance.

**Implementation Plan**:

Once ODBC and JDBC drivers are implemented, adapt the test suite for dual-protocol execution:

```
Test Definition Layer (protocol-agnostic)
         ↓
Protocol Adapter Layer
    ↙         ↘
ODBC Driver    JDBC Driver
    ↓            ↓
Database Engine
```

**Requirements**:
- All 100+ tests must pass via ODBC connection
- All 100+ tests must pass via JDBC connection
- Both protocols produce identical correct results

**GitHub Actions Matrix** (planned):
```yaml
test:
  strategy:
    matrix:
      protocol: [odbc, jdbc]
  steps:
    - name: Run tests via ${{ matrix.protocol }}
      run: ./run_tests.sh --protocol=${{ matrix.protocol }}
```

### SQLLogicTest Suite

**Status**: ✅ Integrated

Industry-standard test framework used by SQLite, DuckDB, and other production databases.

**Details**:
- **Scale**: 600+ test files with comprehensive SQL coverage
- **Coverage**: Core SQL operations (SELECT, INSERT, UPDATE, DELETE, JOINs, aggregates, etc.)
- **Source**: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

**Running Tests**:
```bash
# Full suite with parallel workers
./scripts/sqllogictest run --parallel

# Query results
./scripts/sqllogictest query --preset by-category
```

**Results Storage**: VibeSQL stores its own test results in a VibeSQL database (dogfooding!)

See [sqllogictest/QUICKSTART.md](sqllogictest/QUICKSTART.md) for detailed usage.

### Phase 3: Expanded SQL:1999 Feature Coverage (Planned)

**Status**: Not yet implemented

**Rationale**: Extend beyond current 100-test baseline to cover all Core and Optional SQL:1999 features.

**Approach**:
1. Extract more tests from sqltest repository (currently 100, can expand to 200-500+)
2. Build custom tests based on ISO/IEC 9075:1999 specification
3. Cover all ~169 Core SQL:1999 features
4. Add tests for Optional features (triggers, stored procedures, recursive queries, etc.)

**Proposed Test Organization**:
```
tests/sql1999/
├── manifest.json (current: 100 tests)
├── core/ (planned: expanded coverage)
│   ├── e011_numeric_types/
│   ├── e021_character_types/
│   ├── f031_basic_schema/
│   ├── f041_basic_joins/
│   └── ...
└── optional/ (planned: new coverage)
    ├── t031_boolean_type/
    ├── t131_recursive_queries/
    ├── triggers/
    └── stored_procedures/
```

**Expansion Strategy**:
- Use `scripts/extract_sql1999_tests.py` to pull more tests from sqltest
- Supplement with hand-crafted tests for gaps
- Maintain JSON manifest format for consistency

### Phase 4: Additional Test Sources (Optional)

**NIST SQL-92 Tests**: If obtainable, provides historical baseline
**PostgreSQL Regression Tests**: Real-world SQL validation
**Apache Derby Tests**: JDBC-centric testing examples

## Test Result Tracking

### Current Tracking

**Automated via CI/CD**:
- Test results published to `target/sqltest_results.json`
- Compliance report generated in `docs/SQL1999_CONFORMANCE.md`
- Pass rate badge updated on GitHub Pages
- Metrics: total tests, passed, failed, errors, pass rate percentage

**Current Metrics** (739 tests):
- **100% pass rate** (739/739 passing)
- Full conformance across Core SQL:1999 features achieved November 2025

### Future Compliance Matrix (Planned)

When ODBC/JDBC testing is implemented, track feature-by-feature compliance:

```markdown
| Feature ID | Feature Name | Core/Optional | Direct | ODBC | JDBC | Notes |
|------------|--------------|---------------|--------|------|------|-------|
| E011 | Numeric types | Core | ✅ | ✅ | ✅ | - |
| F031 | Basic schema | Core | ✅ | ✅ | ✅ | - |
| T031 | BOOLEAN type | Optional | ❌ | ❌ | ❌ | Issue #123 |
```

**Future Tracking Goals**:
- Separate tracking for Core vs Optional features
- Per-protocol breakdown (Direct, ODBC, JDBC)
- Trend over time (improving/regressing)
- Feature-by-feature status updates

## Coverage Reporting

1. Install [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) if it is not already available:

   ```bash
   cargo install cargo-llvm-cov
   ```

2. Generate an HTML report for the entire workspace:

   ```bash
   cargo coverage
   ```

   This writes the report to `target/coverage/html/html/index.html`.

3. Create an `lcov.info` artifact that can be uploaded to services such as Codecov:

   ```bash
   cargo coverage-lcov
   ```

   The file is emitted at `target/coverage/lcov.info`.

4. Clean cached instrumentation before re-running coverage:

   ```bash
   cargo coverage-clean
   ```

Coverage commands are defined as Cargo aliases in `.cargo/config.toml`, so the invocations above work consistently for every contributor and in automation.

## Test Development Priorities

### ✅ Completed: SQL:1999 Core Compliance
- [x] 739 SQL:1999 tests extracted from sqltest
- [x] **100% pass rate achieved** (739/739 tests)
- [x] GitHub Actions integration with badge generation
- [x] Compliance report automation
- [x] Full parser and executor support for Core SQL:1999

### ✅ Completed: SQLLogicTest Integration
- [x] SQLLogicTest suite integrated
- [x] Parallel test execution
- [x] Results stored in VibeSQL database (dogfooding)
- [x] Query interface for analyzing results

### ✅ Completed: SQLite TCL Test Suite
- [x] 1,174 test files from SQLite's canonical test suite
- [x] TCL shim for VibeSQL compatibility
- [x] Priority-based test organization

### Future: Protocol Testing
- [ ] ODBC driver implementation and testing
- [ ] JDBC driver implementation and testing
- [ ] Protocol-level conformance validation

## Alternative Test Resources

### PostgreSQL Regression Tests
- **Source**: https://github.com/postgres/postgres/tree/master/src/test/regress
- **Coverage**: Extensive SQL tests including SQL:1999 features
- **License**: PostgreSQL License (permissive)
- **Value**: Real-world SQL from production database

### Apache Derby Tests
- **Source**: https://github.com/apache/derby
- **Coverage**: Java-based RDBMS with good SQL:1999 support
- **License**: Apache 2.0
- **Value**: JDBC-centric testing approach

### SQL Feature Comparison Sites
- **Modern-SQL.com**: Documents SQL standard features with examples
- **Use-The-Index-Luke.com**: SQL best practices and testing

## Deliverables

### Test Suite Components
1. **test/** directory with all tests
2. **scripts/** for test execution
3. **docs/COMPLIANCE.md** tracking feature status
4. **GitHub Actions** workflows for CI/CD
5. **Test results dashboard** (HTML report)

### Documentation
1. Test organization and structure
2. How to run tests locally
3. How to add new tests
4. Compliance report interpretation
5. Feature coverage matrix

## Success Criteria

### ✅ Achieved Goals

1. ✅ Basic test infrastructure established
2. ✅ Tests run automatically in GitHub Actions on every commit
3. ✅ Automated badge generation and compliance reporting
4. ✅ **100% pass rate achieved** (739/739 SQL:1999 tests)
5. ✅ SQLLogicTest suite integrated
6. ✅ SQLite TCL test suite integrated (1,174 tests)

### Future Goals

- [ ] ODBC and JDBC drivers implemented
- [ ] Protocol-level test execution
- [ ] Expanded optional feature coverage

## Risks and Mitigations

### Risk 1: No Official NIST SQL:1999 Test Suite
- **Mitigation**: Build custom suite based on specification
- **Mitigation**: Use sqllogictest for baseline coverage
- **Mitigation**: Validate against multiple reference implementations

### Risk 2: Ambiguity in Standard Interpretation
- **Mitigation**: Test against PostgreSQL, Oracle, SQL Server for comparison
- **Mitigation**: Use Mimer SQL validator for syntax checking
- **Mitigation**: Document interpretation decisions

### Risk 3: Incomplete Feature Coverage
- **Mitigation**: Systematic approach using feature taxonomy
- **Mitigation**: Track coverage percentage continuously
- **Mitigation**: Regular audit against specification

### Risk 4: ODBC/JDBC Protocol Complexity
- **Mitigation**: Start simple, iterate toward full protocol support
- **Mitigation**: Use existing ODBC/JDBC drivers as reference
- **Mitigation**: Test incrementally as protocols are implemented

## Running Tests

### SQL:1999 Conformance

```bash
cargo test --test sqltest_conformance --release
```

### SQLLogicTest Suite

```bash
# Full suite
./scripts/sqllogictest run --parallel

# Individual file
./scripts/sqllogictest test random/select/slt_good_19.test

# Query results
./scripts/sqllogictest query --preset by-category
```

### SQLite TCL Tests

```bash
# Priority 1 tests (core SQL)
make test-tcl

# All tests
make test-tcl-all

# Specific test
make test-tcl-file FILE=select1.test
```

### Code Coverage

```bash
cargo coverage        # HTML report
cargo coverage-lcov   # lcov.info for CI
```

## References

### Internal Files

| File | Purpose |
|------|---------|
| `tests/sql1999/manifest.json` | SQL:1999 test definitions |
| `tests/sqltest_conformance.rs` | SQL:1999 test runner |
| `scripts/sqllogictest` | SQLLogicTest CLI tool |
| `scripts/tcltest` | TCL test suite runner |
| `scripts/generate_compliance_report.sh` | Generate conformance report |

### External Resources

- [sqltest by Elliot Chance](https://github.com/elliotchance/sqltest) - SQL:1999 test source
- [SQLLogicTest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) - Test framework
- [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) - Rust implementation

### Related Documentation

- [SQL1999_CONFORMANCE.md](SQL1999_CONFORMANCE.md) - Current conformance status
- [sqllogictest/QUICKSTART.md](sqllogictest/QUICKSTART.md) - SQLLogicTest usage guide
- [Benchmarking Guide](../development/BENCHMARKING.md) - Performance testing
- [Feature Status](../reference/FEATURE_STATUS.md) - SQL feature implementation
