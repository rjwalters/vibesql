//! DML parsing benchmarks for arena-allocated vs standard parser
//!
//! Measures performance improvement from arena allocation for INSERT, UPDATE,
//! and DELETE statement parsing.
//!
//! Part of Epic #3228 (Parser & Lexer Performance Improvements)

use bumpalo::Bump;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use vibesql_parser::arena_parser::ArenaParser;
use vibesql_parser::Parser;

// ============================================================================
// INSERT Statement Test Cases
// ============================================================================

/// Simple INSERT with VALUES
const INSERT_SIMPLE: &str =
    "INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com')";

/// INSERT with multiple rows (batch insert)
const INSERT_MULTI_ROW: &str = r#"INSERT INTO orders (id, customer_id, product_id, quantity, price) VALUES
    (1, 100, 500, 2, 29.99),
    (2, 101, 501, 1, 49.99),
    (3, 102, 502, 5, 9.99),
    (4, 103, 503, 3, 19.99),
    (5, 104, 504, 1, 99.99)"#;

/// INSERT with large batch (10 rows)
const INSERT_LARGE_BATCH: &str = r#"INSERT INTO events (id, event_time, event_type, user_id, data) VALUES
    (1, '2024-01-01 00:00:00', 'click', 100, 'home'),
    (2, '2024-01-01 00:00:01', 'view', 101, 'product'),
    (3, '2024-01-01 00:00:02', 'click', 102, 'cart'),
    (4, '2024-01-01 00:00:03', 'purchase', 103, 'checkout'),
    (5, '2024-01-01 00:00:04', 'click', 104, 'home'),
    (6, '2024-01-01 00:00:05', 'view', 105, 'search'),
    (7, '2024-01-01 00:00:06', 'click', 106, 'product'),
    (8, '2024-01-01 00:00:07', 'view', 107, 'home'),
    (9, '2024-01-01 00:00:08', 'purchase', 108, 'cart'),
    (10, '2024-01-01 00:00:09', 'click', 109, 'checkout')"#;

/// INSERT with SELECT subquery
const INSERT_SELECT: &str = r#"INSERT INTO archive_orders (id, customer_id, total, created_at)
    SELECT id, customer_id, total, created_at FROM orders
    WHERE created_at < '2024-01-01' AND status = 'completed'"#;

/// INSERT with ON DUPLICATE KEY UPDATE (MySQL-style)
const INSERT_ON_DUPLICATE: &str = r#"INSERT INTO counters (id, name, count, updated_at)
    VALUES (1, 'page_views', 1, NOW())
    ON DUPLICATE KEY UPDATE count = count + 1, updated_at = NOW()"#;

/// TPC-C style NEW_ORDER insert
const INSERT_TPCC_NEW_ORDER: &str = r#"INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
    VALUES (3001, 5, 1)"#;

/// TPC-C style ORDER_LINE insert (multiple lines)
const INSERT_TPCC_ORDER_LINE: &str = r#"INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES
    (3001, 5, 1, 1, 12345, 1, 5, 49.95, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
    (3001, 5, 1, 2, 23456, 1, 3, 29.97, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'),
    (3001, 5, 1, 3, 34567, 1, 2, 19.98, 'cccccccccccccccccccccccccccccccccc')"#;

// ============================================================================
// UPDATE Statement Test Cases
// ============================================================================

/// Simple UPDATE with single column
const UPDATE_SIMPLE: &str = "UPDATE users SET status = 'inactive' WHERE last_login < '2024-01-01'";

/// UPDATE with multiple columns
const UPDATE_MULTI_COLUMN: &str = r#"UPDATE products
    SET price = 29.99, discount = 0.10, updated_at = NOW(), stock = stock - 1
    WHERE id = 12345"#;

/// UPDATE with complex WHERE clause
const UPDATE_COMPLEX_WHERE: &str = r#"UPDATE orders
    SET status = 'shipped', shipped_at = NOW(), tracking_number = 'TRK123456'
    WHERE id IN (SELECT order_id FROM pending_shipments WHERE warehouse_id = 5)
    AND status = 'processing' AND created_at > '2024-01-01'"#;

/// UPDATE with arithmetic expressions
const UPDATE_ARITHMETIC: &str = r#"UPDATE inventory
    SET quantity = quantity - 10,
        reserved = reserved + 10,
        available = quantity - reserved - 10,
        last_updated = NOW()
    WHERE product_id = 500 AND warehouse_id = 1"#;

/// TPC-C style stock update
const UPDATE_TPCC_STOCK: &str = r#"UPDATE stock
    SET s_quantity = s_quantity - 5,
        s_ytd = s_ytd + 5,
        s_order_cnt = s_order_cnt + 1,
        s_remote_cnt = s_remote_cnt + 0
    WHERE s_i_id = 12345 AND s_w_id = 1"#;

/// TPC-C style district update
const UPDATE_TPCC_DISTRICT: &str = r#"UPDATE district
    SET d_next_o_id = d_next_o_id + 1
    WHERE d_id = 5 AND d_w_id = 1"#;

// ============================================================================
// DELETE Statement Test Cases
// ============================================================================

/// Simple DELETE
const DELETE_SIMPLE: &str = "DELETE FROM sessions WHERE expires_at < NOW()";

/// DELETE with complex WHERE clause
const DELETE_COMPLEX_WHERE: &str = r#"DELETE FROM logs
    WHERE created_at < '2024-01-01'
    AND severity NOT IN ('ERROR', 'CRITICAL')
    AND (source = 'debug' OR message LIKE '%test%')"#;

/// DELETE with subquery
const DELETE_SUBQUERY: &str = r#"DELETE FROM order_items
    WHERE order_id IN (
        SELECT id FROM orders WHERE status = 'cancelled' AND created_at < '2024-01-01'
    )"#;

/// TPC-C style NEW_ORDER delete
const DELETE_TPCC_NEW_ORDER: &str = r#"DELETE FROM new_order
    WHERE no_o_id = 2001 AND no_d_id = 5 AND no_w_id = 1"#;

// ============================================================================
// Benchmarks
// ============================================================================

/// Benchmark INSERT statement parsing: standard vs arena parser
fn bench_insert_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("dml_insert");

    let queries = [
        ("simple", INSERT_SIMPLE),
        ("multi_row", INSERT_MULTI_ROW),
        ("large_batch", INSERT_LARGE_BATCH),
        ("select", INSERT_SELECT),
        ("on_duplicate", INSERT_ON_DUPLICATE),
        ("tpcc_new_order", INSERT_TPCC_NEW_ORDER),
        ("tpcc_order_line", INSERT_TPCC_ORDER_LINE),
    ];

    for (name, sql) in queries {
        group.throughput(Throughput::Bytes(sql.len() as u64));

        // Standard parser
        group.bench_with_input(BenchmarkId::new("standard", name), sql, |b, sql| {
            b.iter(|| black_box(Parser::parse_sql(black_box(sql)).unwrap()));
        });

        // Arena parser (fresh arena each time)
        group.bench_with_input(BenchmarkId::new("arena", name), sql, |b, sql| {
            b.iter(|| {
                let arena = Bump::new();
                let result = ArenaParser::parse_insert(black_box(sql), &arena).unwrap();
                let _ = black_box(&result);
                drop(arena);
            });
        });

        // Arena parser with reused arena (amortized allocation cost)
        group.bench_with_input(BenchmarkId::new("arena_reuse", name), sql, |b, sql| {
            let mut arena = Bump::with_capacity(4096);
            b.iter(|| {
                arena.reset();
                let result = ArenaParser::parse_insert(black_box(sql), &arena).unwrap();
                let _ = black_box(&result);
            });
        });
    }

    group.finish();
}

/// Benchmark UPDATE statement parsing: standard vs arena parser
fn bench_update_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("dml_update");

    let queries = [
        ("simple", UPDATE_SIMPLE),
        ("multi_column", UPDATE_MULTI_COLUMN),
        ("complex_where", UPDATE_COMPLEX_WHERE),
        ("arithmetic", UPDATE_ARITHMETIC),
        ("tpcc_stock", UPDATE_TPCC_STOCK),
        ("tpcc_district", UPDATE_TPCC_DISTRICT),
    ];

    for (name, sql) in queries {
        group.throughput(Throughput::Bytes(sql.len() as u64));

        // Standard parser
        group.bench_with_input(BenchmarkId::new("standard", name), sql, |b, sql| {
            b.iter(|| black_box(Parser::parse_sql(black_box(sql)).unwrap()));
        });

        // Arena parser (fresh arena each time)
        group.bench_with_input(BenchmarkId::new("arena", name), sql, |b, sql| {
            b.iter(|| {
                let arena = Bump::new();
                let result = ArenaParser::parse_update(black_box(sql), &arena).unwrap();
                let _ = black_box(&result);
                drop(arena);
            });
        });

        // Arena parser with reused arena
        group.bench_with_input(BenchmarkId::new("arena_reuse", name), sql, |b, sql| {
            let mut arena = Bump::with_capacity(4096);
            b.iter(|| {
                arena.reset();
                let result = ArenaParser::parse_update(black_box(sql), &arena).unwrap();
                let _ = black_box(&result);
            });
        });
    }

    group.finish();
}

/// Benchmark DELETE statement parsing: standard vs arena parser
fn bench_delete_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("dml_delete");

    let queries = [
        ("simple", DELETE_SIMPLE),
        ("complex_where", DELETE_COMPLEX_WHERE),
        ("subquery", DELETE_SUBQUERY),
        ("tpcc_new_order", DELETE_TPCC_NEW_ORDER),
    ];

    for (name, sql) in queries {
        group.throughput(Throughput::Bytes(sql.len() as u64));

        // Standard parser
        group.bench_with_input(BenchmarkId::new("standard", name), sql, |b, sql| {
            b.iter(|| black_box(Parser::parse_sql(black_box(sql)).unwrap()));
        });

        // Arena parser (fresh arena each time)
        group.bench_with_input(BenchmarkId::new("arena", name), sql, |b, sql| {
            b.iter(|| {
                let arena = Bump::new();
                let result = ArenaParser::parse_delete(black_box(sql), &arena).unwrap();
                let _ = black_box(&result);
                drop(arena);
            });
        });

        // Arena parser with reused arena
        group.bench_with_input(BenchmarkId::new("arena_reuse", name), sql, |b, sql| {
            let mut arena = Bump::with_capacity(4096);
            b.iter(|| {
                arena.reset();
                let result = ArenaParser::parse_delete(black_box(sql), &arena).unwrap();
                let _ = black_box(&result);
            });
        });
    }

    group.finish();
}

/// Benchmark TPC-C style mixed DML workload
///
/// Simulates a realistic OLTP workload with a mix of INSERT, UPDATE, and DELETE
/// statements in proportions similar to TPC-C transactions.
fn bench_tpcc_dml_mix(c: &mut Criterion) {
    let mut group = c.benchmark_group("dml_tpcc_mix");

    // TPC-C inspired workload:
    // - NEW_ORDER: INSERT new_order, INSERT order_line (multiple)
    // - PAYMENT: UPDATE warehouse, UPDATE district, UPDATE customer
    // - DELIVERY: DELETE new_order, UPDATE orders, UPDATE customer
    let workload = [
        INSERT_TPCC_NEW_ORDER,
        INSERT_TPCC_ORDER_LINE,
        UPDATE_TPCC_STOCK,
        UPDATE_TPCC_DISTRICT,
        DELETE_TPCC_NEW_ORDER,
    ];

    let total_bytes: usize = workload.iter().map(|s| s.len()).sum();
    group.throughput(Throughput::Bytes(total_bytes as u64));

    // Standard parser - parse all statements
    group.bench_function("standard", |b| {
        b.iter(|| {
            for sql in &workload {
                black_box(Parser::parse_sql(black_box(sql)).unwrap());
            }
        });
    });

    // Arena parser - fresh arena for each iteration
    group.bench_function("arena", |b| {
        b.iter(|| {
            let arena = Bump::new();
            let _ = black_box(ArenaParser::parse_insert(black_box(workload[0]), &arena).unwrap());
            let _ = black_box(ArenaParser::parse_insert(black_box(workload[1]), &arena).unwrap());
            let _ = black_box(ArenaParser::parse_update(black_box(workload[2]), &arena).unwrap());
            let _ = black_box(ArenaParser::parse_update(black_box(workload[3]), &arena).unwrap());
            let _ = black_box(ArenaParser::parse_delete(black_box(workload[4]), &arena).unwrap());
            drop(arena);
        });
    });

    // Arena parser - reuse arena across all statements in workload
    group.bench_function("arena_reuse", |b| {
        let mut arena = Bump::with_capacity(8192);
        b.iter(|| {
            arena.reset();
            let _ = black_box(ArenaParser::parse_insert(black_box(workload[0]), &arena).unwrap());
            let _ = black_box(ArenaParser::parse_insert(black_box(workload[1]), &arena).unwrap());
            let _ = black_box(ArenaParser::parse_update(black_box(workload[2]), &arena).unwrap());
            let _ = black_box(ArenaParser::parse_update(black_box(workload[3]), &arena).unwrap());
            let _ = black_box(ArenaParser::parse_delete(black_box(workload[4]), &arena).unwrap());
        });
    });

    group.finish();
}

/// Benchmark scaling with batch insert size
fn bench_insert_batch_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("dml_insert_scaling");

    // Generate INSERT statements with varying row counts
    let row_counts = [1, 5, 10, 25, 50];

    for count in row_counts {
        let mut values = Vec::new();
        for i in 0..count {
            values.push(format!("({}, 'user{}', 'user{}@example.com')", i, i, i));
        }
        let sql = format!("INSERT INTO users (id, name, email) VALUES {}", values.join(", "));

        let name = format!("{}_rows", count);
        group.throughput(Throughput::Bytes(sql.len() as u64));

        // Standard parser
        group.bench_with_input(BenchmarkId::new("standard", &name), &sql, |b, sql| {
            b.iter(|| black_box(Parser::parse_sql(black_box(sql)).unwrap()));
        });

        // Arena parser with reuse
        group.bench_with_input(BenchmarkId::new("arena_reuse", &name), &sql, |b, sql| {
            let mut arena = Bump::with_capacity(4096);
            b.iter(|| {
                arena.reset();
                let result = ArenaParser::parse_insert(black_box(sql), &arena).unwrap();
                let _ = black_box(&result);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_insert_parsing,
    bench_update_parsing,
    bench_delete_parsing,
    bench_tpcc_dml_mix,
    bench_insert_batch_scaling,
);
criterion_main!(benches);
