//! Arena-Based Prepared Statement Benchmarks
//!
//! These benchmarks measure the performance improvement from arena-based
//! prepared statement execution. This is a follow-on from #3258 (arena-throughout
//! pipeline for prepared statements).
//!
//! ## Benchmark Categories
//!
//! 1. **Micro-benchmarks**: Parsing and binding overhead
//!    - Arena vs owned parsing
//!    - Parameter binding (inline resolution vs AST cloning)
//!    - Expression evaluation
//!
//! 2. **Workload benchmarks**: Common OLTP patterns
//!    - Simple point queries: `SELECT * FROM t WHERE id = ?`
//!    - Multi-column filters: `SELECT * FROM t WHERE a = ? AND b = ?`
//!    - IN list queries: `SELECT * FROM t WHERE id IN (?, ?, ?)`
//!
//! 3. **TPC-C integration**: Real-world OLTP throughput
//!    - Measure improvement with arena path enabled
//!    - Compare against current owned path
//!
//! ## Running
//!
//! ```bash
//! # Build and run
//! cargo bench --bench arena_prepared_statement_benchmark
//!
//! # Run specific benchmark group
//! cargo bench --bench arena_prepared_statement_benchmark -- arena_parsing
//! cargo bench --bench arena_prepared_statement_benchmark -- workload
//! ```
//!
//! ## Expected Results
//!
//! Based on arena parser Phase 1 results (~10% parsing improvement), we expect:
//! - 10-30% improvement for simple prepared queries (parsing-bound)
//! - Larger improvements for high-throughput OLTP workloads
//! - Near-zero allocation for simple prepared queries

use bumpalo::Bump;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::sync::Arc;
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::{PreparedStatementCache, SelectExecutor, Session};
use vibesql_parser::arena_parser::ArenaParser;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

// ============================================================================
// Test Data Setup
// ============================================================================

/// Create a test database for OLTP-style workloads
fn create_oltp_database(row_count: usize) -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Users table with primary key
    let user_columns = vec![
        ColumnSchema::new("id".to_string(), DataType::Integer, false),
        ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ColumnSchema::new("email".to_string(), DataType::Varchar { max_length: Some(255) }, true),
        ColumnSchema::new("age".to_string(), DataType::Integer, true),
        ColumnSchema::new("status".to_string(), DataType::Varchar { max_length: Some(20) }, true),
    ];
    let user_schema =
        TableSchema::with_primary_key("users".to_string(), user_columns, vec!["id".to_string()]);
    db.create_table(user_schema).unwrap();

    // Orders table with composite key
    let order_columns = vec![
        ColumnSchema::new("order_id".to_string(), DataType::Integer, false),
        ColumnSchema::new("user_id".to_string(), DataType::Integer, false),
        ColumnSchema::new("product".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ColumnSchema::new("quantity".to_string(), DataType::Integer, true),
        ColumnSchema::new("price".to_string(), DataType::Decimal { precision: 10, scale: 2 }, true),
    ];
    let order_schema = TableSchema::with_primary_key(
        "orders".to_string(),
        order_columns,
        vec!["order_id".to_string()],
    );
    db.create_table(order_schema).unwrap();

    // Insert test data
    for i in 0..row_count {
        let user_row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(format!("User_{}", i))),
            SqlValue::Varchar(arcstr::ArcStr::from(format!("user{}@example.com", i))),
            SqlValue::Integer((20 + (i % 50)) as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(if i % 3 == 0 { "active" } else { "inactive" })),
        ]);
        db.insert_row("users", user_row).unwrap();

        // Multiple orders per user
        for j in 0..3 {
            let order_row = Row::new(vec![
                SqlValue::Integer((i * 3 + j) as i64),
                SqlValue::Integer(i as i64),
                SqlValue::Varchar(arcstr::ArcStr::from(format!("Product_{}", j))),
                SqlValue::Integer((1 + j % 10) as i64),
                SqlValue::Numeric((1000 + (i * 100)) as f64 / 100.0),
            ]);
            db.insert_row("orders", order_row).unwrap();
        }
    }

    db
}

// ============================================================================
// Micro-benchmarks: Parsing
// ============================================================================

/// Benchmark arena vs owned parsing for prepared statement queries
fn bench_arena_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("arena_parsing");

    // Queries typical for prepared statements
    let queries = [
        ("point_lookup", "SELECT * FROM users WHERE id = 1"),
        ("multi_column", "SELECT * FROM users WHERE id = 1 AND status = 'active'"),
        ("range_query", "SELECT * FROM users WHERE id >= 1 AND id < 100"),
        ("in_list_3", "SELECT * FROM users WHERE id IN (1, 2, 3)"),
        ("in_list_10", "SELECT * FROM users WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)"),
        ("projection", "SELECT id, name, email FROM users WHERE id = 1"),
        ("join_point", "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 1"),
    ];

    for (name, sql) in queries {
        group.throughput(Throughput::Bytes(sql.len() as u64));

        // Standard owned parser
        group.bench_with_input(BenchmarkId::new("owned", name), sql, |b, sql| {
            b.iter(|| black_box(Parser::parse_sql(black_box(sql)).unwrap()));
        });

        // Arena parser (fresh arena each time)
        group.bench_with_input(BenchmarkId::new("arena_fresh", name), sql, |b, sql| {
            b.iter(|| {
                let arena = Bump::new();
                let result = ArenaParser::parse_sql(black_box(sql), &arena).unwrap();
                black_box(&result);
                // arena drops at end of closure, deallocating result
            });
        });

        // Arena parser with reused arena (amortized allocation)
        group.bench_with_input(BenchmarkId::new("arena_reuse", name), sql, |b, sql| {
            let mut arena = Bump::with_capacity(4096);
            b.iter(|| {
                arena.reset();
                let result = ArenaParser::parse_sql(black_box(sql), &arena).unwrap();
                let _ = black_box(&result);
            });
        });
    }

    group.finish();
}

/// Benchmark parameter binding overhead
fn bench_parameter_binding(c: &mut Criterion) {
    let mut group = c.benchmark_group("parameter_binding");

    let db = create_oltp_database(100);
    let session = Session::new(&db);

    // Prepare statements with different placeholder counts
    let stmt_1param = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
    let stmt_2param = session.prepare("SELECT * FROM users WHERE id >= ? AND id < ?").unwrap();
    let stmt_3param =
        session.prepare("SELECT * FROM users WHERE id = ? AND name = ? AND status = ?").unwrap();
    let stmt_5param = session.prepare("SELECT * FROM users WHERE id IN (?, ?, ?, ?, ?)").unwrap();

    group.throughput(Throughput::Elements(100));

    // 1 parameter binding
    group.bench_function("1_param", |b| {
        b.iter(|| {
            for i in 0..100 {
                let result =
                    session.execute_prepared(&stmt_1param, &[SqlValue::Integer(i)]).unwrap();
                black_box(result);
            }
        });
    });

    // 2 parameter binding
    group.bench_function("2_params", |b| {
        b.iter(|| {
            for i in 0..100 {
                let start = i * 10;
                let result = session
                    .execute_prepared(
                        &stmt_2param,
                        &[SqlValue::Integer(start), SqlValue::Integer(start + 10)],
                    )
                    .unwrap();
                black_box(result);
            }
        });
    });

    // 3 parameter binding
    group.bench_function("3_params", |b| {
        b.iter(|| {
            for i in 0..100 {
                let result = session
                    .execute_prepared(
                        &stmt_3param,
                        &[
                            SqlValue::Integer(i),
                            SqlValue::Varchar(arcstr::ArcStr::from(format!("User_{}", i))),
                            SqlValue::Varchar(arcstr::ArcStr::from("active")),
                        ],
                    )
                    .unwrap();
                black_box(result);
            }
        });
    });

    // 5 parameter binding (IN list)
    group.bench_function("5_params_inlist", |b| {
        b.iter(|| {
            for i in 0..100 {
                let base = i * 5;
                let result = session
                    .execute_prepared(
                        &stmt_5param,
                        &[
                            SqlValue::Integer(base),
                            SqlValue::Integer(base + 1),
                            SqlValue::Integer(base + 2),
                            SqlValue::Integer(base + 3),
                            SqlValue::Integer(base + 4),
                        ],
                    )
                    .unwrap();
                black_box(result);
            }
        });
    });

    group.finish();
}

// ============================================================================
// Workload Benchmarks: Common OLTP Patterns
// ============================================================================

/// Benchmark workload: Simple point lookups (PK = ?)
fn bench_workload_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("workload_point_lookup");

    for row_count in [100, 1000, 10000] {
        let db = create_oltp_database(row_count);
        let session = Session::new(&db);
        let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();

        let iterations = 100;
        group.throughput(Throughput::Elements(iterations as u64));

        group.bench_with_input(
            BenchmarkId::new("prepared", row_count),
            &iterations,
            |b, &iterations| {
                b.iter(|| {
                    for i in 0..iterations {
                        let id = (i as i64) % (row_count as i64);
                        let result =
                            session.execute_prepared(&stmt, &[SqlValue::Integer(id)]).unwrap();
                        black_box(result);
                    }
                });
            },
        );

        // Raw SQL for comparison (parse each time)
        group.bench_with_input(
            BenchmarkId::new("raw_sql", row_count),
            &iterations,
            |b, &iterations| {
                b.iter(|| {
                    for i in 0..iterations {
                        let id = (i as i64) % (row_count as i64);
                        let sql = format!("SELECT * FROM users WHERE id = {}", id);
                        let parsed = Parser::parse_sql(&sql).unwrap();
                        if let vibesql_ast::Statement::Select(select_stmt) = parsed {
                            let executor = SelectExecutor::new(&db);
                            let result = executor.execute(&select_stmt).unwrap();
                            black_box(result);
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark workload: Multi-column filter queries
fn bench_workload_multi_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("workload_multi_filter");

    let db = create_oltp_database(1000);
    let session = Session::new(&db);
    let iterations = 100;
    group.throughput(Throughput::Elements(iterations as u64));

    // 2-column filter
    let stmt_2col = session.prepare("SELECT * FROM users WHERE age >= ? AND age < ?").unwrap();
    group.bench_function("2_columns", |b| {
        b.iter(|| {
            for i in 0..iterations {
                let age_start = 20 + (i % 30) as i64;
                let result = session
                    .execute_prepared(
                        &stmt_2col,
                        &[SqlValue::Integer(age_start), SqlValue::Integer(age_start + 10)],
                    )
                    .unwrap();
                black_box(result);
            }
        });
    });

    // 3-column filter
    let stmt_3col =
        session.prepare("SELECT * FROM users WHERE id >= ? AND id < ? AND status = ?").unwrap();
    group.bench_function("3_columns", |b| {
        b.iter(|| {
            for i in 0..iterations {
                let start = (i * 10) as i64;
                let result = session
                    .execute_prepared(
                        &stmt_3col,
                        &[
                            SqlValue::Integer(start),
                            SqlValue::Integer(start + 100),
                            SqlValue::Varchar(arcstr::ArcStr::from("active")),
                        ],
                    )
                    .unwrap();
                black_box(result);
            }
        });
    });

    group.finish();
}

/// Benchmark workload: IN list queries with varying sizes
fn bench_workload_in_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("workload_in_list");

    let db = create_oltp_database(1000);
    let session = Session::new(&db);
    let iterations = 50;
    group.throughput(Throughput::Elements(iterations as u64));

    // IN list with 3 values
    let stmt_in3 = session.prepare("SELECT * FROM users WHERE id IN (?, ?, ?)").unwrap();
    group.bench_function("in_list_3", |b| {
        b.iter(|| {
            for i in 0..iterations {
                let base = (i * 3) as i64;
                let result = session
                    .execute_prepared(
                        &stmt_in3,
                        &[
                            SqlValue::Integer(base),
                            SqlValue::Integer(base + 1),
                            SqlValue::Integer(base + 2),
                        ],
                    )
                    .unwrap();
                black_box(result);
            }
        });
    });

    // IN list with 5 values
    let stmt_in5 = session.prepare("SELECT * FROM users WHERE id IN (?, ?, ?, ?, ?)").unwrap();
    group.bench_function("in_list_5", |b| {
        b.iter(|| {
            for i in 0..iterations {
                let base = (i * 5) as i64;
                let result = session
                    .execute_prepared(
                        &stmt_in5,
                        &[
                            SqlValue::Integer(base),
                            SqlValue::Integer(base + 1),
                            SqlValue::Integer(base + 2),
                            SqlValue::Integer(base + 3),
                            SqlValue::Integer(base + 4),
                        ],
                    )
                    .unwrap();
                black_box(result);
            }
        });
    });

    // IN list with 10 values
    let stmt_in10 =
        session.prepare("SELECT * FROM users WHERE id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").unwrap();
    group.bench_function("in_list_10", |b| {
        b.iter(|| {
            for i in 0..iterations {
                let base = (i * 10) as i64;
                let result = session
                    .execute_prepared(
                        &stmt_in10,
                        &[
                            SqlValue::Integer(base),
                            SqlValue::Integer(base + 1),
                            SqlValue::Integer(base + 2),
                            SqlValue::Integer(base + 3),
                            SqlValue::Integer(base + 4),
                            SqlValue::Integer(base + 5),
                            SqlValue::Integer(base + 6),
                            SqlValue::Integer(base + 7),
                            SqlValue::Integer(base + 8),
                            SqlValue::Integer(base + 9),
                        ],
                    )
                    .unwrap();
                black_box(result);
            }
        });
    });

    group.finish();
}

// ============================================================================
// Cache Efficiency Benchmarks
// ============================================================================

/// Benchmark prepared statement cache effectiveness
fn bench_cache_efficiency(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_efficiency");

    let db = create_oltp_database(1000);
    let iterations = 1000;
    group.throughput(Throughput::Elements(iterations as u64));

    // Shared cache: all queries hit the same prepared statement
    group.bench_function("shared_cache_hot", |b| {
        let shared_cache = Arc::new(PreparedStatementCache::default_cache());
        // Pre-warm
        let warmup_session = Session::with_shared_cache(&db, Arc::clone(&shared_cache));
        let _ = warmup_session.prepare("SELECT * FROM users WHERE id = ?").unwrap();

        b.iter(|| {
            let session = Session::with_shared_cache(&db, Arc::clone(&shared_cache));
            let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
            for i in 0..iterations {
                let result =
                    session.execute_prepared(&stmt, &[SqlValue::Integer(i as i64 % 1000)]).unwrap();
                black_box(result);
            }
        });
    });

    // Cold cache: fresh cache each benchmark iteration
    group.bench_function("cold_cache", |b| {
        b.iter(|| {
            let session = Session::new(&db);
            let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
            for i in 0..iterations {
                let result =
                    session.execute_prepared(&stmt, &[SqlValue::Integer(i as i64 % 1000)]).unwrap();
                black_box(result);
            }
        });
    });

    // Mixed queries: simulate realistic workload with multiple query types
    group.bench_function("mixed_queries", |b| {
        let shared_cache = Arc::new(PreparedStatementCache::default_cache());

        b.iter(|| {
            let session = Session::with_shared_cache(&db, Arc::clone(&shared_cache));
            let stmt1 = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
            let stmt2 = session.prepare("SELECT * FROM users WHERE age = ?").unwrap();
            let stmt3 = session.prepare("SELECT * FROM orders WHERE user_id = ?").unwrap();

            for i in 0..iterations {
                let query_type = i % 3;
                let result = match query_type {
                    0 => session.execute_prepared(&stmt1, &[SqlValue::Integer(i as i64 % 1000)]),
                    1 => {
                        session.execute_prepared(&stmt2, &[SqlValue::Integer(20 + (i as i64 % 50))])
                    }
                    _ => session.execute_prepared(&stmt3, &[SqlValue::Integer(i as i64 % 1000)]),
                };
                black_box(result.unwrap());
            }
        });
    });

    group.finish();
}

// ============================================================================
// Throughput Benchmarks
// ============================================================================

/// Benchmark maximum throughput for different query patterns
fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    group.sample_size(50);

    let db = create_oltp_database(10000);
    let session = Session::new(&db);

    // High-throughput point lookup
    let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
    let iterations = 10000;
    group.throughput(Throughput::Elements(iterations as u64));

    group.bench_function("point_lookup_10k", |b| {
        b.iter(|| {
            for i in 0..iterations {
                let result = session
                    .execute_prepared(&stmt, &[SqlValue::Integer(i as i64 % 10000)])
                    .unwrap();
                black_box(result);
            }
        });
    });

    // High-throughput with projection
    let stmt_proj = session.prepare("SELECT id, name FROM users WHERE id = ?").unwrap();
    group.bench_function("projection_10k", |b| {
        b.iter(|| {
            for i in 0..iterations {
                let result = session
                    .execute_prepared(&stmt_proj, &[SqlValue::Integer(i as i64 % 10000)])
                    .unwrap();
                black_box(result);
            }
        });
    });

    group.finish();
}

// ============================================================================
// Latency Distribution Benchmarks
// ============================================================================

/// Measure latency percentiles for prepared statements
fn bench_latency_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency");

    let db = create_oltp_database(10000);
    let session = Session::new(&db);
    let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();

    // Single operation latency (for p50/p99/p999 measurement)
    group.bench_function("single_point_lookup", |b| {
        let mut i = 0i64;
        b.iter(|| {
            i = (i + 1) % 10000;
            let result = session.execute_prepared(&stmt, &[SqlValue::Integer(i)]).unwrap();
            black_box(result)
        });
    });

    // Batch of 10 operations
    group.bench_function("batch_10", |b| {
        let mut base = 0i64;
        b.iter(|| {
            for i in 0..10 {
                let result = session
                    .execute_prepared(&stmt, &[SqlValue::Integer((base + i) % 10000)])
                    .unwrap();
                black_box(&result);
            }
            base = (base + 10) % 10000;
        });
    });

    // Batch of 100 operations
    group.bench_function("batch_100", |b| {
        let mut base = 0i64;
        b.iter(|| {
            for i in 0..100 {
                let result = session
                    .execute_prepared(&stmt, &[SqlValue::Integer((base + i) % 10000)])
                    .unwrap();
                black_box(&result);
            }
            base = (base + 100) % 10000;
        });
    });

    group.finish();
}

// ============================================================================
// Arena Allocation Comparison (Future: Arena-Throughout Pipeline)
// ============================================================================

/// Benchmark comparing allocation patterns
///
/// This benchmark prepares infrastructure for measuring the arena-throughout
/// pipeline once #3258 is complete. Currently measures:
/// - Standard path (owned allocations)
/// - Arena parsing overhead
fn bench_allocation_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("allocation");

    // Test queries of varying complexity
    let queries = [
        ("simple", "SELECT * FROM users WHERE id = 1"),
        ("medium", "SELECT id, name, email FROM users WHERE id = 1 AND status = 'active'"),
        ("complex", "SELECT u.id, u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 1"),
    ];

    for (name, sql) in queries {
        // Owned path: standard parser
        group.bench_with_input(BenchmarkId::new("owned_parse", name), sql, |b, sql| {
            b.iter(|| {
                let stmt = Parser::parse_sql(black_box(sql)).unwrap();
                black_box(stmt)
            });
        });

        // Arena path: arena parser
        group.bench_with_input(BenchmarkId::new("arena_parse", name), sql, |b, sql| {
            b.iter(|| {
                let arena = Bump::with_capacity(4096);
                let stmt = ArenaParser::parse_sql(black_box(sql), &arena).unwrap();
                // Access the stmt before arena is dropped
                black_box(&stmt);
                // arena drops at end of closure, deallocating result
            });
        });

        // TODO: Once #3258 is complete, add:
        // - Arena binding (inline parameter resolution)
        // - Arena execution (zero-copy expression evaluation)
    }

    group.finish();
}

// ============================================================================
// Benchmark Groups
// ============================================================================

criterion_group!(micro_benchmarks, bench_arena_parsing, bench_parameter_binding,);

criterion_group!(
    workload_benchmarks,
    bench_workload_point_lookup,
    bench_workload_multi_filter,
    bench_workload_in_list,
);

criterion_group!(cache_benchmarks, bench_cache_efficiency,);

criterion_group!(
    performance_benchmarks,
    bench_throughput,
    bench_latency_distribution,
    bench_allocation_comparison,
);

criterion_main!(micro_benchmarks, workload_benchmarks, cache_benchmarks, performance_benchmarks,);
