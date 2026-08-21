# vibesql-sqllogictest

**VibeSQL's vendored and modified version of the sqllogictest-rs library**

## What is this?

This crate is our fork of the [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) library, vendored into the VibeSQL repository for customization and optimization.

### Directory vs Package Name

- **Directory name**: `crates/vibesql-sqllogictest/` (makes ownership clear)
- **Package name**: `sqllogictest` (for compatibility with patch directive)
- **Purpose**: Run SQLLogicTest corpus against VibeSQL

This naming scheme eliminates confusion with:
- `third_party/sqllogictest/` - Official SQLLogicTest test corpus (third-party data)
- `tests/sqllogictest/` - Our integration test adapter code

## Why vendored?

We vendor this library to:
1. **Optimize performance** - Add VibeSQL-specific optimizations (e.g., rowsort improvements)
2. **Add features** - Result storage, parallel execution enhancements, regression detection
3. **Fix bugs** - Apply fixes without waiting for upstream
4. **Control versioning** - Pin exact version for reproducible builds

## Original License

This code is originally from sqllogictest-rs, licensed under MIT OR Apache-2.0.
See the original repository: https://github.com/risinglightdb/sqllogictest-rs

## Current Status

### What we've customized:
- (In progress) Performance optimizations for rowsort
- (Done) Enhanced parallel test execution (`src/executor/parallel.rs`, `run_parallel_async`)
- (Done) Built-in result storage and analysis (`src/result_updater.rs` + `scripts/process_test_results.py`)

### Upstream version:
Based on sqllogictest-rs v0.28.4

## Future Plans

See [MIGRATION_PLAN.md](./MIGRATION_PLAN.md) for our roadmap to:
1. Move Python scripts into this Rust crate
2. Add result storage using VibeSQL (dogfooding!)
3. Enhance parallel test runner
4. Create unified CLI tool

## Usage

This crate is used internally by VibeSQL's test suite. It's not intended for external use.

For testing VibeSQL:
```bash
# Run tests using the wrapper script
./scripts/sqllogictest test <file>
./scripts/sqllogictest run --parallel

# Or directly with cargo
SQLLOGICTEST_FILE=third_party/sqllogictest/test/select1.test \
  cargo test --test sqllogictest_suite
```

## Contributing

When modifying this library:
1. **Document changes** - Add comments explaining why we diverged from upstream
2. **Preserve correctness** - Run full test suite before committing
3. **Consider upstream** - Could this fix/feature benefit upstream project?
4. **Update version** - Bump patch version when making changes (0.28.4 → 0.28.5)

## Questions?

- What is this? - Our modified version of sqllogictest-rs
- Should I modify it? - Yes, that's the point of vendoring!
- Will we sync with upstream? - Eventually, but we maintain our own optimizations
- Can others use it? - It's published=false, for internal use only
