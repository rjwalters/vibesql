# Fuzzing vibesql

This directory contains fuzz testing infrastructure using [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) and [libFuzzer](https://llvm.org/docs/LibFuzzer.html).

## Fuzz Targets

| Target | Description | Priority |
|--------|-------------|----------|
| `sql_parser` | Fuzzes the SQL parser with arbitrary input strings | High |
| `expr_eval` | Fuzzes SQL expression parsing in various contexts | High |
| `type_convert` | Fuzzes CAST expressions with arbitrary values and types | Medium |
| `query_executor` | Fuzzes the full query execution pipeline (parser → optimizer → executor) | High |
| `type_coercion` | Fuzzes SqlValue type comparison and coercion at runtime | Medium |
| `differential_sqlite` | Compares VibeSQL results against SQLite to catch semantic bugs | High |

### Target Details

**sql_parser**: Tests parser robustness with arbitrary byte sequences converted to strings.

**expr_eval**: Wraps input in various SQL contexts (SELECT, WHERE, CASE, GROUP BY, ORDER BY) to exercise expression parsing paths.

**type_convert**: Uses structured fuzzing to generate type-aware CAST operations across all 13 SQL types.

**query_executor**: Creates an in-memory database with test tables and executes fuzzed SELECT queries to catch bugs in:
- Query planning and optimization
- Expression evaluation at runtime
- Type coercion during execution
- Memory management in execution pipelines

**type_coercion**: Tests SqlValue comparison operations (PartialEq, PartialOrd, Ord) with structured inputs, verifying invariants like reflexivity and antisymmetry.

**differential_sqlite**: Runs the same queries on both VibeSQL and SQLite, comparing results to catch semantic bugs that produce wrong results without crashing.

## Prerequisites

```bash
# Install cargo-fuzz (requires nightly Rust)
rustup install nightly
cargo +nightly install cargo-fuzz
```

## Running Fuzz Tests

### Using Make (Recommended)

```bash
# Run all fuzz targets (5 min each by default)
make fuzz

# Run all targets with custom duration
FUZZ_DURATION=600 make fuzz

# Run individual targets
make fuzz-parser        # SQL parser
make fuzz-expr          # Expression evaluation
make fuzz-type-convert  # CAST type conversion
make fuzz-query         # Query execution
make fuzz-type-coercion # Type coercion
make fuzz-differential  # SQLite differential testing

# List available targets
make fuzz-list
```

### Using cargo-fuzz Directly

```bash
# Change to fuzz directory
cd fuzz

# Run SQL parser fuzzer (with dictionary)
cargo +nightly fuzz run sql_parser -- -dict=dictionaries/sql.dict

# Run query executor fuzzer
cargo +nightly fuzz run query_executor -- -dict=dictionaries/sql.dict

# Run differential testing
cargo +nightly fuzz run differential_sqlite -- -dict=dictionaries/sql.dict

# Run for a specific duration (e.g., 5 minutes)
cargo +nightly fuzz run sql_parser -- -max_total_time=300

# Run with multiple jobs
cargo +nightly fuzz run sql_parser -- -jobs=4 -workers=4
```

## Corpus Management

The `corpus/` directory contains seed inputs for each fuzz target:

```
corpus/
├── sql_parser/          # SQL statement seeds
├── expr_eval/           # Expression seeds
├── type_convert/        # Type conversion seeds
├── query_executor/      # Query execution seeds
└── differential_sqlite/ # Differential testing seeds
```

To add new seeds:
```bash
echo -n "SELECT new_sql_here" > corpus/sql_parser/new_seed
```

## Dictionary

The `dictionaries/sql.dict` file contains SQL keywords, operators, and common patterns that help the fuzzer generate more meaningful inputs. This dictionary is comprehensive and includes:

- Core SQL keywords (SELECT, FROM, WHERE, etc.)
- All data types (INTEGER, VARCHAR, etc.)
- Operators and punctuation
- Common functions (COUNT, SUM, etc.)
- Window function patterns
- Subquery patterns
- Edge case values (NaN, Infinity, etc.)
- Comment patterns
- Escape sequences

## Reproducing Crashes

When a crash is found, it's saved to `artifacts/<target>/crash-<hash>`. To reproduce:

```bash
# Reproduce a specific crash
cargo +nightly fuzz run sql_parser artifacts/sql_parser/crash-xxxxx
```

## Minimizing Test Cases

To minimize a crashing input:

```bash
cargo +nightly fuzz tmin sql_parser artifacts/sql_parser/crash-xxxxx
```

## Coverage

To generate coverage reports:

```bash
cargo +nightly fuzz coverage sql_parser
# View coverage in target/coverage/
```

## Differential Testing

The `differential_sqlite` target compares VibeSQL query results against SQLite to catch semantic bugs - queries that produce wrong results without crashing.

Features:
- Creates identical schemas in both databases
- Normalizes results for comparison (float precision, NULL handling, etc.)
- Filters out queries with legitimate implementation differences (RANDOM, NOW, etc.)
- Reports mismatches in row count, column count, or values

Limitations:
- Only compares SELECT queries
- Some queries are intentionally skipped due to SQL dialect differences
- Type system differences may cause false positives

## CI Integration

Fuzzing runs nightly via GitHub Actions.

The workflow:
1. Runs each fuzz target for 5 minutes
2. Caches the corpus between runs for faster discovery
3. Uploads crash artifacts
4. Creates GitHub issues for any crashes found

## Adding New Fuzz Targets

1. Create a new file in `fuzz_targets/`:
```rust
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Your fuzzing logic here
});
```

2. Add the target to `Cargo.toml`:
```toml
[[bin]]
name = "my_target"
path = "fuzz_targets/my_target.rs"
test = false
doc = false
bench = false
```

3. Add seed corpus:
```bash
mkdir corpus/my_target
echo -n "seed input" > corpus/my_target/seed1
```

4. Update the Makefile with a new target (optional but recommended)

## References

- [cargo-fuzz book](https://rust-fuzz.github.io/book/)
- [libFuzzer documentation](https://llvm.org/docs/LibFuzzer.html)
- [Fuzzing best practices](https://github.com/google/fuzzing/blob/master/docs/good-fuzz-target.md)
