//! Morsel Operation Benchmark - Targeted benchmarks for each parallel operation
//!
//! This benchmark provides isolated benchmarks for each operation that uses morsel
//! parallelism, with a synthetic data generator designed to probe specific behaviors.
//!
//! ## Philosophy
//!
//! Unlike TPC-H queries which mix multiple operations, these benchmarks isolate
//! each operation to clearly measure the effect of morsel size on that specific
//! operation. This enables:
//!
//! - Understanding which operations benefit most from smaller morsel sizes
//! - Identifying cache-sensitive operations
//! - Measuring work-stealing effectiveness per operation
//! - Tuning morsel sizes per operation type (future optimization)
//!
//! ## Operations Benchmarked
//!
//! ### Direct API Benchmarks (Row-Oriented)
//! These test the morsel-driven work-stealing functions directly:
//!
//! 1. **Morsel Filter** - `morsel_parallel_filter` (direct API, bypasses SQL/columnar)
//! 2. **Group By** - `morsel_parallel_group`
//! 3. **Hash Join Build** - `build_hash_table_parallel`
//! 4. **Hash Join Probe** - `morsel_parallel_probe_sqlvalue`
//! 5. **Semi Join** - `build_existence_hash_table_parallel`
//! 6. **Sort** - `par_sort_by`
//! 7. **Aggregate** - `morsel_parallel_reduce`
//!
//! ### SQL-Based Benchmarks (Columnar by Default)
//! These use SQL queries which may route through the columnar execution engine:
//!
//! - **Filter (WHERE)** - SQL queries with WHERE clauses (typically uses SIMD columnar filter)
//! - **Scan** - Table materialization via SQL
//!
//! **Note**: SQL filter benchmarks show columnar SIMD performance (~100x faster than row-based),
//! which has different parallelism characteristics. Use `morsel_filter` for direct API testing.
//!
//! ## Usage
//!
//! ```bash
//! # Build the benchmark
//! cargo bench --package vibesql-executor --bench morsel_operation_benchmark --no-run
//!
//! # Run all benchmarks
//! ./target/release/deps/morsel_operation_benchmark-*
//!
//! # Run specific operation
//! OPERATION_FILTER=morsel_filter ./target/release/deps/morsel_operation_benchmark-*  # Direct API
//! OPERATION_FILTER=filter ./target/release/deps/morsel_operation_benchmark-*         # SQL/columnar
//! OPERATION_FILTER=groupby ./target/release/deps/morsel_operation_benchmark-*
//! OPERATION_FILTER=join ./target/release/deps/morsel_operation_benchmark-*
//!
//! # Test specific morsel sizes
//! MORSEL_SIZES=2048,8192,50000 ./target/release/deps/morsel_operation_benchmark-*
//!
//! # Vary thread counts
//! MAX_THREADS=8 ./target/release/deps/morsel_operation_benchmark-*
//!
//! # Debug morsel execution
//! MORSEL_DEBUG=1 ./target/release/deps/morsel_operation_benchmark-*
//! ```
//!
//! ## Environment Variables
//!
//! - `OPERATION_FILTER` - Run only specific operation (morsel_filter, filter, scan, groupby, join, sort, agg)
//! - `MORSEL_SIZES` - Comma-separated list of morsel sizes to test (default: 1024,2048,4096,8192,16384,32768,50000)
//! - `ROW_COUNTS` - Comma-separated list of row counts to test (default: 100000,500000,1000000)
//! - `MAX_THREADS` - Maximum thread count to test (default: 16)
//! - `WARMUP_ITERATIONS` - Number of warmup runs (default: 2)
//! - `BENCHMARK_ITERATIONS` - Number of timed runs (default: 5)
//! - `MORSEL_DEBUG` - Enable detailed morsel execution logging

mod harness;

use std::{
    env,
    hint::black_box,
    time::{Duration, Instant},
};

use harness::{print_group_header, BenchConfig, BenchResult, Harness};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::select::morsel::{morsel_parallel_filter, MorselConfig};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

// =============================================================================
// Configuration
// =============================================================================

/// Default morsel sizes to test (covers SIMD-friendly to large)
const DEFAULT_MORSEL_SIZES: &[usize] = &[1024, 2048, 4096, 8192, 16384, 32768, 50000];

/// Default row counts to test
const DEFAULT_ROW_COUNTS: &[usize] = &[100_000, 500_000, 1_000_000];

/// Default thread counts to test
const DEFAULT_THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];

fn get_morsel_sizes() -> Vec<usize> {
    env::var("MORSEL_SIZES")
        .ok()
        .map(|s| s.split(',').filter_map(|v| v.trim().parse().ok()).collect())
        .unwrap_or_else(|| DEFAULT_MORSEL_SIZES.to_vec())
}

fn get_row_counts() -> Vec<usize> {
    env::var("ROW_COUNTS")
        .ok()
        .map(|s| s.split(',').filter_map(|v| v.trim().parse().ok()).collect())
        .unwrap_or_else(|| DEFAULT_ROW_COUNTS.to_vec())
}

fn get_thread_counts() -> Vec<usize> {
    let max_threads: usize =
        env::var("MAX_THREADS").ok().and_then(|s| s.parse().ok()).unwrap_or(16);
    DEFAULT_THREAD_COUNTS.iter().copied().filter(|&t| t <= max_threads).collect()
}

fn get_operation_filter() -> Option<String> {
    env::var("OPERATION_FILTER").ok().map(|s| s.to_lowercase())
}

// =============================================================================
// Synthetic Data Generator
// =============================================================================

/// Data generator for morsel benchmarks
///
/// Creates tables with specific characteristics to isolate operations:
/// - Uniform distribution (baseline)
/// - Skewed distribution (tests work-stealing)
/// - Varied selectivity (tests filter efficiency)
/// - Different cardinalities (tests group by, joins)
pub struct DataGenerator {
    row_count: usize,
}

impl DataGenerator {
    pub fn new(row_count: usize) -> Self {
        Self { row_count }
    }

    /// Create a database for filter benchmarks
    ///
    /// Table: FILTER_DATA (id INTEGER, category INTEGER, value BIGINT, flag INTEGER)
    ///
    /// Characteristics:
    /// - category: 0-99 (uniform distribution for varied selectivity)
    /// - value: random-ish values for range queries
    /// - flag: 0 or 1 (50% selectivity)
    pub fn create_filter_database(&self) -> Database {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "FILTER_DATA".to_string(),
            vec![
                ColumnSchema {
                    name: "ID".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "CATEGORY".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "VALUE".to_string(),
                    data_type: DataType::Bigint,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "FLAG".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
            ],
        );
        db.create_table(schema).unwrap();

        for i in 0..self.row_count {
            let row = Row::new(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Integer((i % 100) as i64),                  // 100 categories
                SqlValue::Bigint(((i * 17 + 42) % 1_000_000) as i64), // pseudo-random
                SqlValue::Integer((i % 2) as i64),                    // 50% selectivity
            ]);
            db.insert_row("FILTER_DATA", row).unwrap();
        }

        db
    }

    /// Create a database for GROUP BY benchmarks
    ///
    /// Table: GROUPBY_DATA (id INTEGER, group_key INTEGER, value BIGINT)
    ///
    /// Characteristics:
    /// - group_key variants: low cardinality (10), medium (1000), high (100000)
    /// - value: values to aggregate
    pub fn create_groupby_database(&self, num_groups: usize) -> Database {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "GROUPBY_DATA".to_string(),
            vec![
                ColumnSchema {
                    name: "ID".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "GROUP_KEY".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "VALUE".to_string(),
                    data_type: DataType::Bigint,
                    nullable: false,
                    default_value: None,
                },
            ],
        );
        db.create_table(schema).unwrap();

        for i in 0..self.row_count {
            let row = Row::new(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Integer((i % num_groups) as i64),
                SqlValue::Bigint(((i * 7 + 13) % 10000) as i64),
            ]);
            db.insert_row("GROUPBY_DATA", row).unwrap();
        }

        db
    }

    /// Create a database for hash join benchmarks
    ///
    /// Tables:
    /// - BUILD_TABLE (id INTEGER, value BIGINT) - smaller table for hash build
    /// - PROBE_TABLE (id INTEGER, build_id INTEGER, data BIGINT) - larger table for probe
    ///
    /// Characteristics:
    /// - Configurable build:probe ratio
    /// - Each build row matches multiple probe rows (fan-out)
    pub fn create_join_database(&self, build_size: usize) -> Database {
        let mut db = Database::new();

        // Build table (smaller)
        let build_schema = TableSchema::new(
            "BUILD_TABLE".to_string(),
            vec![
                ColumnSchema {
                    name: "ID".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "VALUE".to_string(),
                    data_type: DataType::Bigint,
                    nullable: false,
                    default_value: None,
                },
            ],
        );
        db.create_table(build_schema).unwrap();

        for i in 0..build_size {
            let row = Row::new(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Bigint((i * 100) as i64),
            ]);
            db.insert_row("BUILD_TABLE", row).unwrap();
        }

        // Probe table (larger)
        let probe_schema = TableSchema::new(
            "PROBE_TABLE".to_string(),
            vec![
                ColumnSchema {
                    name: "ID".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "BUILD_ID".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "DATA".to_string(),
                    data_type: DataType::Bigint,
                    nullable: false,
                    default_value: None,
                },
            ],
        );
        db.create_table(probe_schema).unwrap();

        for i in 0..self.row_count {
            let row = Row::new(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Integer((i % build_size) as i64), // foreign key to build table
                SqlValue::Bigint((i * 11) as i64),
            ]);
            db.insert_row("PROBE_TABLE", row).unwrap();
        }

        db
    }

    /// Create a database for sort benchmarks
    ///
    /// Table: SORT_DATA (id INTEGER, sort_key BIGINT, category VARCHAR)
    ///
    /// Characteristics:
    /// - sort_key: reverse sorted (worst case for some algorithms)
    /// - category: string column for multi-column sort
    pub fn create_sort_database(&self) -> Database {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "SORT_DATA".to_string(),
            vec![
                ColumnSchema {
                    name: "ID".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "SORT_KEY".to_string(),
                    data_type: DataType::Bigint,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "CATEGORY".to_string(),
                    data_type: DataType::Varchar { max_length: Some(20) },
                    nullable: false,
                    default_value: None,
                },
            ],
        );
        db.create_table(schema).unwrap();

        for i in 0..self.row_count {
            let row = Row::new(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Bigint((self.row_count - i) as i64), // reverse sorted
                SqlValue::Varchar(arcstr::ArcStr::from(format!("cat_{:05}", i % 1000))),
            ]);
            db.insert_row("SORT_DATA", row).unwrap();
        }

        db
    }

    /// Create a database for aggregate (no GROUP BY) benchmarks
    ///
    /// Table: AGG_DATA (id INTEGER, value BIGINT, amount DOUBLE)
    pub fn create_aggregate_database(&self) -> Database {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "AGG_DATA".to_string(),
            vec![
                ColumnSchema {
                    name: "ID".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "VALUE".to_string(),
                    data_type: DataType::Bigint,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "AMOUNT".to_string(),
                    data_type: DataType::DoublePrecision,
                    nullable: false,
                    default_value: None,
                },
            ],
        );
        db.create_table(schema).unwrap();

        for i in 0..self.row_count {
            let row = Row::new(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Bigint((i % 10000) as i64),
                SqlValue::Double((i as f64) * 1.5 + 0.5),
            ]);
            db.insert_row("AGG_DATA", row).unwrap();
        }

        db
    }

    /// Create a database for semi-join (EXISTS) benchmarks
    ///
    /// Tables:
    /// - OUTER_TABLE (id INTEGER, key INTEGER)
    /// - INNER_TABLE (key INTEGER) - subset of keys for EXISTS check
    pub fn create_semi_join_database(&self, match_ratio: f64) -> Database {
        let mut db = Database::new();

        // Outer table
        let outer_schema = TableSchema::new(
            "OUTER_TABLE".to_string(),
            vec![
                ColumnSchema {
                    name: "ID".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "KEY".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
            ],
        );
        db.create_table(outer_schema).unwrap();

        for i in 0..self.row_count {
            let row = Row::new(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Integer((i % 10000) as i64), // 10000 distinct keys
            ]);
            db.insert_row("OUTER_TABLE", row).unwrap();
        }

        // Inner table (subset of keys)
        let inner_schema = TableSchema::new(
            "INNER_TABLE".to_string(),
            vec![ColumnSchema {
                name: "KEY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            }],
        );
        db.create_table(inner_schema).unwrap();

        let inner_keys = (10000.0 * match_ratio) as usize;
        for i in 0..inner_keys {
            let row = Row::new(vec![SqlValue::Integer(i as i64)]);
            db.insert_row("INNER_TABLE", row).unwrap();
        }

        db
    }
}

// =============================================================================
// Query Execution Helper
// =============================================================================

fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

fn run_query(db: &Database, sql: &str) -> BenchResult {
    let stmt = parse_select(sql);
    let executor = SelectExecutor::new(db);
    let start = Instant::now();
    match executor.execute(&stmt) {
        Ok(rows) => {
            black_box(rows);
            BenchResult::Ok(start.elapsed())
        }
        Err(e) => BenchResult::Error(e.to_string()),
    }
}

// =============================================================================
// Filter Operation Benchmark
// =============================================================================

/// Benchmark filter operation with different selectivities
fn bench_filter_operation(harness: &Harness) {
    print_group_header("Filter Operation (WHERE clause)");

    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    // Queries with different selectivities
    let queries = [
        ("filter_50pct", "SELECT COUNT(*) FROM FILTER_DATA WHERE FLAG = 1"), // 50%
        ("filter_10pct", "SELECT COUNT(*) FROM FILTER_DATA WHERE CATEGORY < 10"), // 10%
        ("filter_1pct", "SELECT COUNT(*) FROM FILTER_DATA WHERE CATEGORY = 0"), // 1%
        (
            "filter_compound",
            "SELECT COUNT(*) FROM FILTER_DATA WHERE CATEGORY < 50 AND FLAG = 1",
        ), // ~25%
    ];

    for &row_count in &row_counts {
        eprintln!("\n--- {} rows ---", row_count);
        let gen = DataGenerator::new(row_count);
        let db = gen.create_filter_database();

        for (query_name, query_sql) in &queries {
            eprintln!("\n  Query: {}", query_name);

            // Test morsel size sensitivity
            print_morsel_size_header(&morsel_sizes);

            for &threads in &thread_counts {
                eprint!("  {:>2}T ", threads);

                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("Failed to create thread pool");

                for &morsel_size in &morsel_sizes {
                    env::set_var("MORSEL_SIZE", morsel_size.to_string());

                    let stats = pool.install(|| {
                        let name = format!("{}_{}t_{}m", query_name, threads, morsel_size);
                        harness.run(&name, || run_query(&db, query_sql))
                    });

                    eprint!(" {:>8.2?}", stats.mean);
                }
                eprintln!();
            }
        }
    }

    env::remove_var("MORSEL_SIZE");
}

// =============================================================================
// Direct Morsel Filter API Benchmark
// =============================================================================

/// Benchmark morsel_parallel_filter directly using the Rust API.
///
/// This bypasses SQL execution and columnar processing to directly measure
/// the morsel-driven work-stealing filter performance. This is the correct
/// way to benchmark morsel parallelism, as SQL queries may be routed through
/// the columnar execution engine which has different parallelism strategies.
fn bench_morsel_filter_direct(harness: &Harness) {
    print_group_header("Morsel Filter (Direct API - Row-Oriented)");

    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    // Create test data outside the timing loop
    for &row_count in &row_counts {
        eprintln!("\n--- {} rows ---", row_count);

        // Generate rows: each row has (id, category, value, flag)
        let rows: Vec<Row> = (0..row_count)
            .map(|i| {
                Row::new(vec![
                    SqlValue::Integer(i as i64),
                    SqlValue::Integer((i % 100) as i64),                  // category 0-99
                    SqlValue::Bigint(((i * 17 + 42) % 1_000_000) as i64), // pseudo-random value
                    SqlValue::Integer((i % 2) as i64),                    // flag 0 or 1
                ])
            })
            .collect();

        // Different filter predicates
        let predicates: Vec<(&str, Box<dyn Fn(&Row) -> bool + Send + Sync>)> = vec![
            // 50% selectivity: flag = 1
            (
                "50pct_flag",
                Box::new(|row: &Row| matches!(row.values[3], SqlValue::Integer(1))),
            ),
            // 10% selectivity: category < 10
            (
                "10pct_category",
                Box::new(|row: &Row| matches!(row.values[1], SqlValue::Integer(c) if c < 10)),
            ),
            // 1% selectivity: category = 0
            (
                "1pct_category",
                Box::new(|row: &Row| matches!(row.values[1], SqlValue::Integer(0))),
            ),
            // Complex: category < 50 AND flag = 1 (~25%)
            (
                "25pct_compound",
                Box::new(|row: &Row| {
                    matches!(row.values[1], SqlValue::Integer(c) if c < 50)
                        && matches!(row.values[3], SqlValue::Integer(1))
                }),
            ),
        ];

        for (pred_name, predicate) in &predicates {
            eprintln!("\n  Predicate: {}", pred_name);

            print_morsel_size_header(&morsel_sizes);

            for &threads in &thread_counts {
                eprint!("  {:>2}T ", threads);

                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("Failed to create thread pool");

                for &morsel_size in &morsel_sizes {
                    let config = MorselConfig::new(morsel_size);

                    let stats = pool.install(|| {
                        let name = format!("morsel_{}_{}t_{}m", pred_name, threads, morsel_size);
                        harness.run(&name, || {
                            let start = Instant::now();
                            let result = morsel_parallel_filter(&rows, &config, |row| predicate(row));
                            black_box(result);
                            BenchResult::Ok(start.elapsed())
                        })
                    });

                    eprint!(" {:>8.2?}", stats.mean);
                }
                eprintln!();
            }
        }
    }
}

// =============================================================================
// Group By Operation Benchmark
// =============================================================================

/// Benchmark GROUP BY operation with different cardinalities
fn bench_groupby_operation(harness: &Harness) {
    print_group_header("Group By Operation");

    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    // Different group cardinalities
    let cardinalities = [
        ("low_card", 10),     // 10 groups - small hash table
        ("med_card", 1000),   // 1000 groups - medium hash table
        ("high_card", 10000), // 10000 groups - large hash table
    ];

    for &row_count in &row_counts {
        eprintln!("\n--- {} rows ---", row_count);

        for (card_name, num_groups) in &cardinalities {
            eprintln!("\n  Cardinality: {} ({} groups)", card_name, num_groups);

            let gen = DataGenerator::new(row_count);
            let db = gen.create_groupby_database(*num_groups);

            // Query without ORDER BY to enable parallel GROUP BY
            let query = "SELECT GROUP_KEY, SUM(VALUE), COUNT(*), AVG(VALUE) FROM GROUPBY_DATA GROUP BY GROUP_KEY";

            print_morsel_size_header(&morsel_sizes);

            for &threads in &thread_counts {
                eprint!("  {:>2}T ", threads);

                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("Failed to create thread pool");

                for &morsel_size in &morsel_sizes {
                    env::set_var("MORSEL_SIZE", morsel_size.to_string());

                    let stats = pool.install(|| {
                        let name = format!("{}_{}t_{}m", card_name, threads, morsel_size);
                        harness.run(&name, || run_query(&db, query))
                    });

                    eprint!(" {:>8.2?}", stats.mean);
                }
                eprintln!();
            }
        }
    }

    env::remove_var("MORSEL_SIZE");
}

// =============================================================================
// Hash Join Operation Benchmark
// =============================================================================

/// Benchmark hash join build and probe phases
fn bench_join_operation(harness: &Harness) {
    print_group_header("Hash Join Operation (Build + Probe)");

    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    // Different build:probe ratios
    let ratios = [
        ("ratio_1_10", 10),  // 1:10 ratio (build is 10% of probe)
        ("ratio_1_100", 100), // 1:100 ratio (build is 1% of probe)
    ];

    for &row_count in &row_counts {
        eprintln!("\n--- {} probe rows ---", row_count);

        for (ratio_name, ratio) in &ratios {
            let build_size = row_count / ratio;
            eprintln!(
                "\n  Ratio: {} ({} build rows, {} probe rows)",
                ratio_name, build_size, row_count
            );

            let gen = DataGenerator::new(row_count);
            let db = gen.create_join_database(build_size);

            let query = "SELECT COUNT(*) FROM PROBE_TABLE p JOIN BUILD_TABLE b ON p.BUILD_ID = b.ID";

            print_morsel_size_header(&morsel_sizes);

            for &threads in &thread_counts {
                eprint!("  {:>2}T ", threads);

                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("Failed to create thread pool");

                for &morsel_size in &morsel_sizes {
                    env::set_var("MORSEL_SIZE", morsel_size.to_string());

                    let stats = pool.install(|| {
                        let name = format!("{}_{}t_{}m", ratio_name, threads, morsel_size);
                        harness.run(&name, || run_query(&db, query))
                    });

                    eprint!(" {:>8.2?}", stats.mean);
                }
                eprintln!();
            }
        }
    }

    env::remove_var("MORSEL_SIZE");
}

// =============================================================================
// Join with Filter Operation Benchmark
// =============================================================================

/// Benchmark join with post-join filter (tests filter on join output)
///
/// This tests work-stealing effectiveness when filtering join results.
fn bench_join_filter_operation(harness: &Harness) {
    print_group_header("Join with Filter Operation");

    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    // Different filter selectivities on join result
    let selectivities = [
        ("filter_low", 1000),     // p.ID < 1000 (1% at 100K)
        ("filter_mid", 50000),    // p.ID < 50000 (50%)
        ("filter_high", 90000),   // p.ID < 90000 (90%)
    ];

    for &row_count in &row_counts {
        eprintln!("\n--- {} probe rows ---", row_count);

        // Create join database with 1:10 ratio
        let build_size = row_count / 10;
        let gen = DataGenerator::new(row_count);
        let db = gen.create_join_database(build_size);

        for (select_name, threshold) in &selectivities {
            let pct = (*threshold as f64 / row_count as f64 * 100.0) as i32;
            eprintln!("\n  Filter: {} (~{}% of rows)", select_name, pct);

            // Simple filter on ID column
            let query = format!(
                "SELECT COUNT(*) FROM PROBE_TABLE p JOIN BUILD_TABLE b ON p.BUILD_ID = b.ID WHERE p.ID < {}",
                threshold
            );

            print_morsel_size_header(&morsel_sizes);

            for &threads in &thread_counts {
                eprint!("  {:>2}T ", threads);

                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("Failed to create thread pool");

                for &morsel_size in &morsel_sizes {
                    env::set_var("MORSEL_SIZE", morsel_size.to_string());

                    let stats = pool.install(|| {
                        let name = format!("{}_{}t_{}m", select_name, threads, morsel_size);
                        harness.run(&name, || run_query(&db, &query))
                    });

                    eprint!(" {:>8.2?}", stats.mean);
                }
                eprintln!();
            }
        }
    }

    env::remove_var("MORSEL_SIZE");
}

// =============================================================================
// Sort Operation Benchmark
// =============================================================================

/// Benchmark sort operation
fn bench_sort_operation(harness: &Harness) {
    print_group_header("Sort Operation (ORDER BY)");

    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    // Different sort types
    let sort_types = [
        ("sort_int", "SELECT * FROM SORT_DATA ORDER BY SORT_KEY"),
        ("sort_string", "SELECT * FROM SORT_DATA ORDER BY CATEGORY"),
        ("sort_multi", "SELECT * FROM SORT_DATA ORDER BY CATEGORY, SORT_KEY"),
    ];

    for &row_count in &row_counts {
        eprintln!("\n--- {} rows ---", row_count);

        let gen = DataGenerator::new(row_count);
        let db = gen.create_sort_database();

        for (sort_name, query) in &sort_types {
            eprintln!("\n  Sort type: {}", sort_name);

            print_morsel_size_header(&morsel_sizes);

            for &threads in &thread_counts {
                eprint!("  {:>2}T ", threads);

                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("Failed to create thread pool");

                for &morsel_size in &morsel_sizes {
                    env::set_var("MORSEL_SIZE", morsel_size.to_string());

                    let stats = pool.install(|| {
                        let name = format!("{}_{}t_{}m", sort_name, threads, morsel_size);
                        harness.run(&name, || run_query(&db, query))
                    });

                    eprint!(" {:>8.2?}", stats.mean);
                }
                eprintln!();
            }
        }
    }

    env::remove_var("MORSEL_SIZE");
}

// =============================================================================
// Aggregate Operation Benchmark (no GROUP BY)
// =============================================================================

/// Benchmark aggregate operations without GROUP BY
fn bench_aggregate_operation(harness: &Harness) {
    print_group_header("Aggregate Operation (no GROUP BY)");

    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    // Different aggregate types
    let agg_types = [
        ("agg_count", "SELECT COUNT(*) FROM AGG_DATA"),
        ("agg_sum", "SELECT SUM(VALUE) FROM AGG_DATA"),
        ("agg_multi", "SELECT COUNT(*), SUM(VALUE), AVG(VALUE), MIN(VALUE), MAX(VALUE) FROM AGG_DATA"),
        ("agg_filtered", "SELECT SUM(VALUE) FROM AGG_DATA WHERE VALUE > 5000"),
    ];

    for &row_count in &row_counts {
        eprintln!("\n--- {} rows ---", row_count);

        let gen = DataGenerator::new(row_count);
        let db = gen.create_aggregate_database();

        for (agg_name, query) in &agg_types {
            eprintln!("\n  Aggregate type: {}", agg_name);

            print_morsel_size_header(&morsel_sizes);

            for &threads in &thread_counts {
                eprint!("  {:>2}T ", threads);

                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("Failed to create thread pool");

                for &morsel_size in &morsel_sizes {
                    env::set_var("MORSEL_SIZE", morsel_size.to_string());

                    let stats = pool.install(|| {
                        let name = format!("{}_{}t_{}m", agg_name, threads, morsel_size);
                        harness.run(&name, || run_query(&db, query))
                    });

                    eprint!(" {:>8.2?}", stats.mean);
                }
                eprintln!();
            }
        }
    }

    env::remove_var("MORSEL_SIZE");
}

// =============================================================================
// Scan Operation Benchmark
// =============================================================================

/// Benchmark scan operation (table materialization)
fn bench_scan_operation(harness: &Harness) {
    print_group_header("Scan Operation (Table Materialization)");

    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    for &row_count in &row_counts {
        eprintln!("\n--- {} rows ---", row_count);

        let gen = DataGenerator::new(row_count);
        let db = gen.create_filter_database(); // Reuse filter table - has multiple columns

        // Simple scan with projection
        let query = "SELECT ID, CATEGORY, VALUE FROM FILTER_DATA";

        print_morsel_size_header(&morsel_sizes);

        for &threads in &thread_counts {
            eprint!("  {:>2}T ", threads);

            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("Failed to create thread pool");

            for &morsel_size in &morsel_sizes {
                env::set_var("MORSEL_SIZE", morsel_size.to_string());

                let stats = pool.install(|| {
                    let name = format!("scan_{}t_{}m", threads, morsel_size);
                    harness.run(&name, || run_query(&db, query))
                });

                eprint!(" {:>8.2?}", stats.mean);
            }
            eprintln!();
        }
    }

    env::remove_var("MORSEL_SIZE");
}

// =============================================================================
// Helper Functions
// =============================================================================

fn print_morsel_size_header(morsel_sizes: &[usize]) {
    eprint!("      ");
    for &size in morsel_sizes {
        if size >= 1000 {
            eprint!(" {:>7}K", size / 1000);
        } else {
            eprint!(" {:>8}", size);
        }
    }
    eprintln!();
    eprint!("      ");
    for _ in morsel_sizes {
        eprint!(" --------");
    }
    eprintln!();
}

/// Compute and print speedup analysis for a set of results
#[allow(dead_code)]
fn print_speedup_analysis(results: &[(usize, Duration)]) {
    if results.is_empty() {
        return;
    }

    let baseline = results[0].1;
    eprintln!("\n  Speedup Analysis (vs 1 thread):");
    for (threads, time) in results {
        let speedup = baseline.as_secs_f64() / time.as_secs_f64();
        let efficiency = speedup / *threads as f64 * 100.0;
        eprintln!("    {}T: {:.2}x speedup ({:.1}% efficiency)", threads, speedup, efficiency);
    }
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    eprintln!("\n========================================");
    eprintln!("  Morsel Operation Benchmark");
    eprintln!("========================================\n");

    // Configuration
    let filter = get_operation_filter();
    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    eprintln!("Configuration:");
    eprintln!("  Operation Filter: {:?}", filter);
    eprintln!("  Row Counts: {:?}", row_counts);
    eprintln!("  Morsel Sizes: {:?}", morsel_sizes);
    eprintln!("  Thread Counts: {:?}", thread_counts);
    eprintln!("  MORSEL_DEBUG: {}", env::var("MORSEL_DEBUG").unwrap_or_default());
    eprintln!();

    // Create harness
    let config = BenchConfig::new(
        env::var("WARMUP_ITERATIONS").ok().and_then(|s| s.parse().ok()).unwrap_or(2),
        env::var("BENCHMARK_ITERATIONS").ok().and_then(|s| s.parse().ok()).unwrap_or(5),
        120, // 2 minute timeout
    );
    let harness = Harness::with_config(config);

    // Run benchmarks based on filter
    match filter.as_deref() {
        Some("filter") | Some("where") => {
            bench_filter_operation(&harness);
        }
        Some("morsel_filter") | Some("morsel") => {
            // Direct morsel API benchmark - tests morsel_parallel_filter without SQL/columnar
            bench_morsel_filter_direct(&harness);
        }
        Some("groupby") | Some("group") => {
            bench_groupby_operation(&harness);
        }
        Some("join") | Some("hash_join") => {
            bench_join_operation(&harness);
        }
        Some("join_filter") | Some("joinfilter") => {
            bench_join_filter_operation(&harness);
        }
        Some("sort") | Some("order") => {
            bench_sort_operation(&harness);
        }
        Some("agg") | Some("aggregate") => {
            bench_aggregate_operation(&harness);
        }
        Some("scan") => {
            bench_scan_operation(&harness);
        }
        None => {
            // Run all benchmarks
            bench_morsel_filter_direct(&harness); // Direct API test first
            bench_filter_operation(&harness);     // Then SQL-based (columnar) test
            bench_groupby_operation(&harness);
            bench_join_operation(&harness);
            bench_join_filter_operation(&harness);
            bench_sort_operation(&harness);
            bench_aggregate_operation(&harness);
            bench_scan_operation(&harness);
        }
        Some(unknown) => {
            eprintln!("Unknown operation filter: {}", unknown);
            eprintln!("Valid filters: filter, morsel_filter, groupby, join, join_filter, sort, agg, scan");
            std::process::exit(1);
        }
    }

    eprintln!("\n========================================");
    eprintln!("  Benchmark Complete");
    eprintln!("========================================\n");

    eprintln!("Tips for analysis:");
    eprintln!("  - Look for the 'sweet spot' morsel size for each operation");
    eprintln!("  - Compare speedup/efficiency across thread counts");
    eprintln!("  - Smaller morsels often benefit cache-sensitive operations");
    eprintln!("  - Larger morsels reduce scheduling overhead");
    eprintln!();
    eprintln!("Cache size reference:");
    eprintln!("  - L1: ~32KB  (~300 rows at 100 bytes/row)");
    eprintln!("  - L2: ~256KB (~2,500 rows)");
    eprintln!("  - L3: ~8-32MB (~80,000-320,000 rows)");
}
