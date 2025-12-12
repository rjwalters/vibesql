# CLI Benchmarks

Apples-to-apples TPC-H benchmark comparison using CLI tools. This provides a fair comparison
by running the same queries through the vibesql and sqlite3 (or duckdb) CLI tools.

## Why CLI Benchmarks?

The existing Rust-based benchmarks use internal APIs and in-process database connections.
While efficient for profiling, they don't measure what users actually experience when
running queries through the CLI tools.

CLI benchmarks provide:
- **Fair comparison**: All databases measured the same way (via their CLI)
- **Representative results**: Measures actual user experience
- **Extensible**: Easy to add more databases (just need CLI support)

## Quick Start

```bash
# Run quick benchmark (Q1, Q6 only)
make benchmark-cli-quick

# Run full TPC-H benchmark (all 22 queries)
make benchmark-cli
```

## Manual Usage

### Step 1: Prepare Databases

Pre-build the TPC-H databases (uses fast Rust data generators):

```bash
# Build and run the prep tool
cargo build --release -p vibesql-executor --bench prep_tpch_databases --features sqlite
./target/release/deps/prep_tpch_databases-* --sqlite

# Or with custom scale factor
./target/release/deps/prep_tpch_databases-* --scale 0.1 --sqlite --output /tmp/tpch_bench
```

### Step 2: Run Benchmarks

```bash
# Run with pre-built databases (fast)
./scripts/bench-cli --db-dir /tmp/tpch_bench

# Run specific queries
./scripts/bench-cli --db-dir /tmp/tpch_bench --queries Q1,Q6,Q14

# Customize iterations
./scripts/bench-cli --db-dir /tmp/tpch_bench --iterations 5 --warmup 2

# Output JSON results
./scripts/bench-cli --db-dir /tmp/tpch_bench --output results.json

# Show query output (verbose mode)
./scripts/bench-cli --db-dir /tmp/tpch_bench --queries Q1 --verbose
```

## Directory Structure

```
benchmarks/cli/
├── README.md           # This file
├── tpch/
│   ├── schema.sql      # TPC-H table definitions
│   ├── queries/        # TPC-H queries (q01.sql - q22.sql)
│   └── generate_data.py  # Python data generator (backup)
└── tools/              # (empty, prep tool is a Rust benchmark)
```

## Benchmark Options

| Option | Default | Description |
|--------|---------|-------------|
| `--scale` | 0.01 | TPC-H scale factor (0.01 = ~60K rows in lineitem) |
| `--engines` | vibesql,sqlite | Comma-separated list of engines |
| `--queries` | all | Comma-separated queries (e.g., Q1,Q6) |
| `--iterations` | 3 | Number of timed iterations per query |
| `--warmup` | 1 | Number of warmup iterations |
| `--output` | - | JSON output file path |
| `--db-dir` | - | Pre-built database directory |
| `--verbose` | false | Show query output |

## Supported Engines

- **vibesql**: Uses `./target/release/vibesql` CLI
- **sqlite**: Uses system `sqlite3` command
- **duckdb**: Uses system `duckdb` command (if installed)

## Notes

- Pre-built databases are stored in `/tmp/tpch_bench` by default
- VibeSQL databases use `.vbsql` extension (binary format)
- SQLite databases use `.sqlite` extension
- The prep tool uses the same data generators as Rust benchmarks (deterministic)
- Scale factor 0.01 creates ~86K total rows, suitable for quick tests
- For production benchmarks, use scale factor 1.0 (6M lineitem rows)
