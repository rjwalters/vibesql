# Fuzzing vibesql

This directory contains fuzz testing infrastructure using [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) and [libFuzzer](https://llvm.org/docs/LibFuzzer.html).

## Fuzz Targets

| Target | Description |
|--------|-------------|
| `sql_parser` | Fuzzes the SQL parser with arbitrary input strings |
| `expr_eval` | Fuzzes SQL expression parsing in various contexts |
| `type_convert` | Fuzzes CAST expressions with arbitrary values and types |

## Prerequisites

```bash
# Install cargo-fuzz (requires nightly Rust)
rustup install nightly
cargo +nightly install cargo-fuzz
```

## Running Fuzz Tests

```bash
# Change to fuzz directory
cd fuzz

# Run SQL parser fuzzer (with dictionary)
cargo +nightly fuzz run sql_parser -- -dict=dictionaries/sql.dict

# Run expression evaluator fuzzer
cargo +nightly fuzz run expr_eval

# Run type conversion fuzzer
cargo +nightly fuzz run type_convert

# Run for a specific duration (e.g., 5 minutes)
cargo +nightly fuzz run sql_parser -- -max_total_time=300

# Run with multiple jobs
cargo +nightly fuzz run sql_parser -- -jobs=4 -workers=4
```

## Corpus Management

The `corpus/` directory contains seed inputs for each fuzz target:

```
corpus/
├── sql_parser/     # SQL statement seeds
├── expr_eval/      # Expression seeds
└── type_convert/   # Type conversion seeds
```

To add new seeds:
```bash
echo "SELECT new_sql_here" > corpus/sql_parser/new_seed
```

## Dictionary

The `dictionaries/sql.dict` file contains SQL keywords and tokens that help the fuzzer generate more meaningful inputs. The dictionary uses libFuzzer format.

## Reproducing Crashes

When a crash is found, it's saved to `artifacts/<target>/crash-<hash>`. To reproduce:

```bash
# Reproduce a specific crash
cargo +nightly fuzz run sql_parser artifacts/sql_parser/crash-xxxxx
```

## CI Integration

Fuzzing runs nightly via GitHub Actions. See `.github/workflows/fuzz.yml`.

The workflow:
1. Runs each fuzz target for 5 minutes
2. Caches the corpus between runs for faster discovery
3. Uploads crash artifacts
4. Creates GitHub issues for any crashes found

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
