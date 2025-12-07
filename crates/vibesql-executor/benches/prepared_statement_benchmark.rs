//! Benchmarks for Prepared Statement API performance
//!
//! These benchmarks demonstrate the performance benefits of prepared statements
//! over repeated SQL parsing + execution. For repeated queries with different
//! parameters, prepared statements avoid the parsing overhead by caching the AST
//! and performing parameter binding at the AST level.
//!
//! Run with: `cargo bench --bench prepared_statement_benchmark`

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::{SelectExecutor, Session};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Create a test database with users table
fn create_test_db(row_count: usize) -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create users table with primary key for efficient lookups
    let columns = vec![
        ColumnSchema::new("id".to_string(), DataType::Integer, false),
        ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ColumnSchema::new("email".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ColumnSchema::new("age".to_string(), DataType::Integer, true),
    ];
    let schema =
        TableSchema::with_primary_key("users".to_string(), columns, vec!["id".to_string()]);
    db.create_table(schema).unwrap();

    // Insert test data
    for i in 0..row_count {
        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(format!("User_{}", i))),
            SqlValue::Varchar(arcstr::ArcStr::from(format!("user{}@example.com", i))),
            SqlValue::Integer((20 + (i % 50)) as i64),
        ]);
        db.insert_row("users", row).unwrap();
    }

    db
}

/// Benchmark: Comparing prepared statement execution vs raw SQL parsing + execution
fn bench_prepared_vs_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("prepared_vs_raw");

    let row_count = 1000;
    let iterations = 100;
    let db = create_test_db(row_count);

    group.throughput(Throughput::Elements(iterations as u64));

    // Prepared statement path: Parse once, execute many times with different params
    group.bench_function(BenchmarkId::new("prepared_statement", iterations), |b| {
        let session = Session::new(&db);
        let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();

        b.iter(|| {
            for i in 0..iterations {
                let result =
                    session.execute_prepared(&stmt, &[SqlValue::Integer(i as i64)]).unwrap();
                black_box(result);
            }
        });
    });

    // Raw SQL path: Parse and execute each time with string formatting
    group.bench_function(BenchmarkId::new("raw_sql_parsing", iterations), |b| {
        b.iter(|| {
            for i in 0..iterations {
                let sql = format!("SELECT * FROM users WHERE id = {}", i);
                let parsed = Parser::parse_sql(&sql).unwrap();
                if let vibesql_ast::Statement::Select(select_stmt) = parsed {
                    let executor = SelectExecutor::new(&db);
                    let result = executor.execute(&select_stmt).unwrap();
                    black_box(result);
                }
            }
        });
    });

    group.finish();
}

/// Benchmark: Prepared statement with multiple parameters
fn bench_multi_param_prepared(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_param_prepared");

    let row_count = 1000;
    let iterations = 100;
    let db = create_test_db(row_count);

    group.throughput(Throughput::Elements(iterations as u64));

    // Prepared statement with range query (two parameters)
    group.bench_function(BenchmarkId::new("prepared_range_query", iterations), |b| {
        let session = Session::new(&db);
        let stmt = session.prepare("SELECT * FROM users WHERE id >= ? AND id < ?").unwrap();

        b.iter(|| {
            for i in 0..iterations {
                let start = (i * 10) as i64;
                let end = start + 10;
                let result = session
                    .execute_prepared(&stmt, &[SqlValue::Integer(start), SqlValue::Integer(end)])
                    .unwrap();
                black_box(result);
            }
        });
    });

    // Raw SQL path for comparison
    group.bench_function(BenchmarkId::new("raw_range_query", iterations), |b| {
        b.iter(|| {
            for i in 0..iterations {
                let start = i * 10;
                let end = start + 10;
                let sql = format!("SELECT * FROM users WHERE id >= {} AND id < {}", start, end);
                let parsed = Parser::parse_sql(&sql).unwrap();
                if let vibesql_ast::Statement::Select(select_stmt) = parsed {
                    let executor = SelectExecutor::new(&db);
                    let result = executor.execute(&select_stmt).unwrap();
                    black_box(result);
                }
            }
        });
    });

    group.finish();
}

/// Benchmark: Cache hit rate impact on performance
fn bench_cache_hit_rate(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_hit_rate");

    let row_count = 1000;
    let db = create_test_db(row_count);

    // Single statement cached and reused (100% hit rate after first call)
    group.bench_function("100_percent_cache_hit", |b| {
        let session = Session::new(&db);
        // Pre-warm the cache
        let _stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();

        b.iter(|| {
            // This should hit the cache every time
            let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
            let result = session.execute_prepared(&stmt, &[SqlValue::Integer(42)]).unwrap();
            black_box(result);
        });
    });

    // Fresh parse each time (simulates 0% cache - fresh session each time)
    group.bench_function("0_percent_cache_hit", |b| {
        b.iter(|| {
            // Create new session each time = no cache benefit
            let session = Session::new(&db);
            let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
            let result = session.execute_prepared(&stmt, &[SqlValue::Integer(42)]).unwrap();
            black_box(result);
        });
    });

    group.finish();
}

/// Benchmark: Query complexity impact
fn bench_query_complexity(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_complexity");

    let row_count = 1000;
    let db = create_test_db(row_count);
    let iterations = 50;

    group.throughput(Throughput::Elements(iterations as u64));

    // Simple point query
    group.bench_function("simple_point_query", |b| {
        let session = Session::new(&db);
        let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();

        b.iter(|| {
            for i in 0..iterations {
                let result =
                    session.execute_prepared(&stmt, &[SqlValue::Integer(i as i64)]).unwrap();
                black_box(result);
            }
        });
    });

    // Complex query with multiple conditions
    group.bench_function("complex_multi_condition", |b| {
        let session = Session::new(&db);
        let stmt = session
            .prepare("SELECT id, name, email, age FROM users WHERE id >= ? AND id < ? AND age > ?")
            .unwrap();

        b.iter(|| {
            for i in 0..iterations {
                let start = (i * 10) as i64;
                let result = session
                    .execute_prepared(
                        &stmt,
                        &[
                            SqlValue::Integer(start),
                            SqlValue::Integer(start + 100),
                            SqlValue::Integer(25),
                        ],
                    )
                    .unwrap();
                black_box(result);
            }
        });
    });

    group.finish();
}

/// Benchmark: Shared cache performance across sessions
fn bench_shared_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("shared_cache");

    let row_count = 1000;
    let db = create_test_db(row_count);

    // Shared cache: multiple "sessions" share the same cache
    group.bench_function("shared_cache_sessions", |b| {
        use std::sync::Arc;
        use vibesql_executor::PreparedStatementCache;

        let shared_cache = Arc::new(PreparedStatementCache::default_cache());

        // Pre-warm the cache
        let warmup_session = Session::with_shared_cache(&db, Arc::clone(&shared_cache));
        let _ = warmup_session.prepare("SELECT * FROM users WHERE id = ?").unwrap();

        b.iter(|| {
            // Simulate multiple sessions using the shared cache
            let session = Session::with_shared_cache(&db, Arc::clone(&shared_cache));
            let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
            let result = session.execute_prepared(&stmt, &[SqlValue::Integer(42)]).unwrap();
            black_box(result);
        });
    });

    // Separate caches: each "session" has its own cache (simulates non-sharing)
    group.bench_function("separate_cache_sessions", |b| {
        b.iter(|| {
            // Each iteration creates a new session with fresh cache
            let session = Session::new(&db);
            let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
            let result = session.execute_prepared(&stmt, &[SqlValue::Integer(42)]).unwrap();
            black_box(result);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_prepared_vs_raw,
    bench_multi_param_prepared,
    bench_cache_hit_rate,
    bench_query_complexity,
    bench_shared_cache,
);

criterion_main!(benches);
