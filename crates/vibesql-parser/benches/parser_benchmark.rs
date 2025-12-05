//! Parser performance benchmarks
//!
//! Measures parse times for various SQL query types to establish baselines
//! and track optimization improvements.

use bumpalo::Bump;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use vibesql_parser::arena_parser::{parse_select_to_owned, ArenaParser};
use vibesql_parser::{Lexer, Parser};

/// Simple SELECT query - baseline for minimal parsing overhead
const SIMPLE_SELECT: &str = "SELECT a FROM t";

/// Point lookup query - typical OLTP workload
const POINT_LOOKUP: &str = "SELECT * FROM users WHERE id = 1";

/// SELECT with multiple columns and conditions
const MULTI_COLUMN: &str =
    "SELECT id, name, email, created_at FROM users WHERE status = 'active' AND user_role = 'admin'";

/// INSERT with values
const INSERT_SINGLE: &str =
    "INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com')";

/// INSERT with multiple rows
const INSERT_MULTI: &str = r#"INSERT INTO users (id, name, email) VALUES
    (1, 'John', 'john@example.com'),
    (2, 'Jane', 'jane@example.com'),
    (3, 'Bob', 'bob@example.com')"#;

/// INSERT with SELECT subquery
const INSERT_SELECT: &str = r#"INSERT INTO archive_orders (id, customer_id, total, created_at)
    SELECT id, customer_id, total, created_at FROM orders WHERE status = 'completed'"#;

/// UPDATE with single assignment
const UPDATE_SIMPLE: &str = "UPDATE users SET status = 'active' WHERE id = 1";

/// UPDATE with multiple assignments
const UPDATE_MULTI: &str = r#"UPDATE orders SET
    status = 'shipped',
    shipped_at = '2024-01-15',
    tracking_number = 'TRK123456'
WHERE id = 42"#;

/// UPDATE with complex WHERE clause
const UPDATE_COMPLEX: &str = r#"UPDATE products SET
    price = price * 1.1,
    updated_at = '2024-01-15'
WHERE category_id IN (1, 2, 3) AND stock_quantity > 0 AND is_active = 1"#;

/// DELETE simple
const DELETE_SIMPLE: &str = "DELETE FROM sessions WHERE user_id = 1";

/// DELETE with complex WHERE clause
const DELETE_COMPLEX: &str = r#"DELETE FROM audit_logs
    WHERE created_at < '2023-01-01'
    AND log_level = 'debug'
    AND (user_id IS NULL OR user_id NOT IN (SELECT id FROM users WHERE is_admin = 1))"#;

/// TPC-H Q1 - Complex aggregation query
const TPCH_Q1: &str = r#"SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus"#;

/// Complex JOIN query
const COMPLEX_JOIN: &str = r#"SELECT
    c.name, o.order_date, p.product_name, oi.quantity
FROM customers c
INNER JOIN orders o ON c.id = o.customer_id
INNER JOIN order_items oi ON o.id = oi.order_id
INNER JOIN products p ON oi.product_id = p.id
WHERE o.order_date >= '2024-01-01'
ORDER BY o.order_date DESC
LIMIT 100"#;

/// CREATE TABLE with constraints
const CREATE_TABLE: &str = r#"CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('active', 'inactive', 'pending')),
    CONSTRAINT fk_department FOREIGN KEY (department_id) REFERENCES departments(id)
)"#;

/// Subquery with CTE
const CTE_QUERY: &str = r#"WITH active_users AS (
    SELECT id, name, email FROM users WHERE status = 'active'
),
recent_orders AS (
    SELECT user_id, COUNT(*) as order_count
    FROM orders
    WHERE created_at >= '2024-01-01'
    GROUP BY user_id
)
SELECT u.name, r.order_count
FROM active_users u
LEFT JOIN recent_orders r ON u.id = r.user_id
ORDER BY r.order_count DESC"#;

/// Benchmark lexer tokenization only
fn bench_lexer(c: &mut Criterion) {
    let mut group = c.benchmark_group("lexer");

    let queries = [
        ("simple_select", SIMPLE_SELECT),
        ("point_lookup", POINT_LOOKUP),
        ("multi_column", MULTI_COLUMN),
        ("insert_single", INSERT_SINGLE),
        ("insert_multi", INSERT_MULTI),
        ("tpch_q1", TPCH_Q1),
        ("complex_join", COMPLEX_JOIN),
        ("create_table", CREATE_TABLE),
        ("cte_query", CTE_QUERY),
    ];

    for (name, sql) in queries {
        group.throughput(Throughput::Bytes(sql.len() as u64));
        group.bench_with_input(BenchmarkId::new("tokenize", name), sql, |b, sql| {
            b.iter(|| {
                let mut lexer = Lexer::new(black_box(sql));
                black_box(lexer.tokenize().unwrap())
            });
        });
    }

    group.finish();
}

/// Benchmark full parse (lexer + parser)
fn bench_parser(c: &mut Criterion) {
    let mut group = c.benchmark_group("parser");

    let queries = [
        ("simple_select", SIMPLE_SELECT),
        ("point_lookup", POINT_LOOKUP),
        ("multi_column", MULTI_COLUMN),
        ("insert_single", INSERT_SINGLE),
        ("insert_multi", INSERT_MULTI),
        ("insert_select", INSERT_SELECT),
        ("update_simple", UPDATE_SIMPLE),
        ("update_multi", UPDATE_MULTI),
        ("update_complex", UPDATE_COMPLEX),
        ("delete_simple", DELETE_SIMPLE),
        ("delete_complex", DELETE_COMPLEX),
        ("tpch_q1", TPCH_Q1),
        ("complex_join", COMPLEX_JOIN),
        ("create_table", CREATE_TABLE),
        ("cte_query", CTE_QUERY),
    ];

    for (name, sql) in queries {
        group.throughput(Throughput::Bytes(sql.len() as u64));
        group.bench_with_input(BenchmarkId::new("parse", name), sql, |b, sql| {
            b.iter(|| black_box(Parser::parse_sql(black_box(sql)).unwrap()));
        });
    }

    group.finish();
}

/// Benchmark keyword-heavy queries (stress test keyword lookup)
fn bench_keywords(c: &mut Criterion) {
    let mut group = c.benchmark_group("keywords");

    // Query with many SQL keywords
    let keyword_heavy = r#"SELECT DISTINCT a FROM t
        LEFT OUTER JOIN u ON t.id = u.t_id
        WHERE NOT EXISTS (SELECT 1 FROM v WHERE v.x BETWEEN 1 AND 10)
        GROUP BY a HAVING COUNT(*) > 1
        ORDER BY a ASC NULLS FIRST
        LIMIT 10 OFFSET 5"#;

    group.throughput(Throughput::Bytes(keyword_heavy.len() as u64));
    group.bench_function("keyword_heavy", |b| {
        b.iter(|| black_box(Parser::parse_sql(black_box(keyword_heavy)).unwrap()));
    });

    group.finish();
}

/// Benchmark identifier-heavy queries (stress test identifier allocation)
fn bench_identifiers(c: &mut Criterion) {
    let mut group = c.benchmark_group("identifiers");

    // Query with many identifiers
    let ident_heavy = r#"SELECT
        table1.column1, table1.column2, table1.column3,
        table2.column4, table2.column5, table2.column6,
        table3.column7, table3.column8, table3.column9,
        table4.column10, table4.column11, table4.column12
    FROM schema1.table1
    JOIN schema2.table2 ON table1.id = table2.table1_id
    JOIN schema3.table3 ON table2.id = table3.table2_id
    JOIN schema4.table4 ON table3.id = table4.table3_id
    WHERE table1.status = 'active'"#;

    group.throughput(Throughput::Bytes(ident_heavy.len() as u64));
    group.bench_function("identifier_heavy", |b| {
        b.iter(|| black_box(Parser::parse_sql(black_box(ident_heavy)).unwrap()));
    });

    group.finish();
}

/// Benchmark arena-allocated parser (supports full AST as of Phase 2)
fn bench_arena_parser(c: &mut Criterion) {
    let mut group = c.benchmark_group("arena_parser");

    // Arena parser now supports DML and DDL statements
    let queries = [
        ("simple_select", SIMPLE_SELECT),
        ("point_lookup", POINT_LOOKUP),
        ("multi_column", MULTI_COLUMN),
        ("insert_single", INSERT_SINGLE),
        ("insert_multi", INSERT_MULTI),
        ("insert_select", INSERT_SELECT),
        ("update_simple", UPDATE_SIMPLE),
        ("update_multi", UPDATE_MULTI),
        ("delete_simple", DELETE_SIMPLE),
        ("tpch_q1", TPCH_Q1),
        ("complex_join", COMPLEX_JOIN),
        ("cte_query", CTE_QUERY),
    ];

    for (name, sql) in queries {
        group.throughput(Throughput::Bytes(sql.len() as u64));
        group.bench_with_input(BenchmarkId::new("parse", name), sql, |b, sql| {
            b.iter(|| {
                let arena = Bump::new();
                let result = ArenaParser::parse_sql(black_box(sql), &arena).unwrap();
                // Just verify parsing succeeded - result is tied to arena lifetime
                // Arena is dropped at end of scope, deallocating everything
                black_box(&result);
            });
        });
    }

    group.finish();
}

/// Compare standard parser vs arena parser
fn bench_parser_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("parser_comparison");

    // Phase 2: Arena parser now supports DML statements + DATE/INTERVAL literals (TPC-H Q1)
    let queries = [
        ("simple_select", SIMPLE_SELECT),
        ("point_lookup", POINT_LOOKUP),
        ("multi_column", MULTI_COLUMN),
        ("insert_single", INSERT_SINGLE),
        ("complex_join", COMPLEX_JOIN),
        ("tpch_q1", TPCH_Q1),
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
                let result = ArenaParser::parse_sql(black_box(sql), &arena).unwrap();
                // Arena is dropped at end of scope, deallocating everything
                black_box(&result);
            });
        });

        // Arena parser with reused arena (amortized allocation cost)
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

/// Benchmark arena parsing with conversion to owned types
/// This measures the end-to-end performance of:
/// 1. Arena parsing (fast)
/// 2. Conversion to standard AST (additional allocation)
fn bench_arena_with_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("arena_conversion");

    let queries = [
        ("simple_select", SIMPLE_SELECT),
        ("point_lookup", POINT_LOOKUP),
        ("multi_column", MULTI_COLUMN),
        ("complex_join", COMPLEX_JOIN),
    ];

    for (name, sql) in queries {
        group.throughput(Throughput::Bytes(sql.len() as u64));

        // Standard parser (baseline)
        group.bench_with_input(BenchmarkId::new("standard", name), sql, |b, sql| {
            b.iter(|| black_box(Parser::parse_sql(black_box(sql)).unwrap()));
        });

        // Arena parser with conversion to owned
        // This is the recommended approach for Phase 3 integration
        group.bench_with_input(BenchmarkId::new("arena_to_owned", name), sql, |b, sql| {
            b.iter(|| black_box(parse_select_to_owned(black_box(sql)).unwrap()));
        });
    }

    group.finish();
}

/// Dedicated DML statement comparison benchmark (INSERT, UPDATE, DELETE)
/// Compares standard parser vs arena parser for DML operations
fn bench_dml_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("dml_comparison");

    // INSERT statement variations
    let insert_queries = [
        ("insert_simple", INSERT_SINGLE),
        ("insert_multi_row", INSERT_MULTI),
        ("insert_select", INSERT_SELECT),
    ];

    for (name, sql) in insert_queries {
        group.throughput(Throughput::Bytes(sql.len() as u64));

        // Standard parser
        group.bench_with_input(BenchmarkId::new("standard", name), sql, |b, sql| {
            b.iter(|| black_box(Parser::parse_sql(black_box(sql)).unwrap()));
        });

        // Arena parser with fresh arena
        group.bench_with_input(BenchmarkId::new("arena", name), sql, |b, sql| {
            b.iter(|| {
                let arena = Bump::new();
                let result = ArenaParser::parse_sql(black_box(sql), &arena).unwrap();
                black_box(&result);
            });
        });

        // Arena parser with reused arena (amortized cost)
        group.bench_with_input(BenchmarkId::new("arena_reuse", name), sql, |b, sql| {
            let mut arena = Bump::with_capacity(4096);
            b.iter(|| {
                arena.reset();
                let result = ArenaParser::parse_sql(black_box(sql), &arena).unwrap();
                black_box(&result);
            });
        });
    }

    // UPDATE statement variations
    let update_queries = [
        ("update_simple", UPDATE_SIMPLE),
        ("update_multi_assign", UPDATE_MULTI),
        ("update_complex_where", UPDATE_COMPLEX),
    ];

    for (name, sql) in update_queries {
        group.throughput(Throughput::Bytes(sql.len() as u64));

        group.bench_with_input(BenchmarkId::new("standard", name), sql, |b, sql| {
            b.iter(|| black_box(Parser::parse_sql(black_box(sql)).unwrap()));
        });

        group.bench_with_input(BenchmarkId::new("arena", name), sql, |b, sql| {
            b.iter(|| {
                let arena = Bump::new();
                let result = ArenaParser::parse_sql(black_box(sql), &arena).unwrap();
                black_box(&result);
            });
        });

        group.bench_with_input(BenchmarkId::new("arena_reuse", name), sql, |b, sql| {
            let mut arena = Bump::with_capacity(4096);
            b.iter(|| {
                arena.reset();
                let result = ArenaParser::parse_sql(black_box(sql), &arena).unwrap();
                black_box(&result);
            });
        });
    }

    // DELETE statement variations
    let delete_queries =
        [("delete_simple", DELETE_SIMPLE), ("delete_complex_where", DELETE_COMPLEX)];

    for (name, sql) in delete_queries {
        group.throughput(Throughput::Bytes(sql.len() as u64));

        group.bench_with_input(BenchmarkId::new("standard", name), sql, |b, sql| {
            b.iter(|| black_box(Parser::parse_sql(black_box(sql)).unwrap()));
        });

        group.bench_with_input(BenchmarkId::new("arena", name), sql, |b, sql| {
            b.iter(|| {
                let arena = Bump::new();
                let result = ArenaParser::parse_sql(black_box(sql), &arena).unwrap();
                black_box(&result);
            });
        });

        group.bench_with_input(BenchmarkId::new("arena_reuse", name), sql, |b, sql| {
            let mut arena = Bump::with_capacity(4096);
            b.iter(|| {
                arena.reset();
                let result = ArenaParser::parse_sql(black_box(sql), &arena).unwrap();
                black_box(&result);
            });
        });
    }

    group.finish();
}

/// Mixed DML workload benchmark (TPC-C style mix)
/// Simulates a realistic OLTP workload with INSERT/UPDATE/DELETE ratio
fn bench_mixed_dml_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_dml_workload");

    // TPC-C style workload: 45% New Order, 43% Payment, 4% Order Status, 4% Delivery, 4% Stock Level
    // For parser benchmarks, we focus on the DML operations:
    // - New Order: INSERT (order) + UPDATE (stock)
    // - Payment: UPDATE (customer, district, warehouse)
    // - Delivery: UPDATE (order, customer) + DELETE (new_order)
    let tpcc_queries = [
        // New Order transaction queries
        ("new_order_insert", "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (1, 1, 1, 1, '2024-01-15 10:30:00', 5, 1)"),
        ("new_order_line_insert", "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (1, 1, 1, 1, 100, 1, 5, 50.00, 'dist_info_01')"),
        ("stock_update", "UPDATE stock SET s_quantity = s_quantity - 5, s_ytd = s_ytd + 5, s_order_cnt = s_order_cnt + 1 WHERE s_i_id = 100 AND s_w_id = 1"),
        // Payment transaction queries
        ("payment_warehouse_update", "UPDATE warehouse SET w_ytd = w_ytd + 100.00 WHERE w_id = 1"),
        ("payment_district_update", "UPDATE district SET d_ytd = d_ytd + 100.00 WHERE d_w_id = 1 AND d_id = 1"),
        ("payment_customer_update", "UPDATE customer SET c_balance = c_balance - 100.00, c_ytd_payment = c_ytd_payment + 100.00, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1"),
        // Delivery transaction queries
        ("delivery_new_order_delete", "DELETE FROM new_order WHERE no_o_id = 1 AND no_d_id = 1 AND no_w_id = 1"),
        ("delivery_order_update", "UPDATE orders SET o_carrier_id = 1 WHERE o_id = 1 AND o_d_id = 1 AND o_w_id = 1"),
    ];

    // Benchmark individual queries
    for (name, sql) in tpcc_queries {
        group.throughput(Throughput::Bytes(sql.len() as u64));

        group.bench_with_input(BenchmarkId::new("standard", name), &sql, |b, sql| {
            b.iter(|| black_box(Parser::parse_sql(black_box(sql)).unwrap()));
        });

        group.bench_with_input(BenchmarkId::new("arena_reuse", name), &sql, |b, sql| {
            let mut arena = Bump::with_capacity(4096);
            b.iter(|| {
                arena.reset();
                let result = ArenaParser::parse_sql(black_box(sql), &arena).unwrap();
                black_box(&result);
            });
        });
    }

    // Benchmark full transaction (multiple queries)
    let new_order_queries = [
        "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (1, 1, 1, 1, '2024-01-15', 5, 1)",
        "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (1, 1, 1, 1, 100, 1, 5, 50.00, 'dist_info')",
        "UPDATE stock SET s_quantity = s_quantity - 5 WHERE s_i_id = 100 AND s_w_id = 1",
    ];

    let total_bytes: u64 = new_order_queries.iter().map(|s| s.len() as u64).sum();
    group.throughput(Throughput::Bytes(total_bytes));

    group.bench_function("new_order_txn_standard", |b| {
        b.iter(|| {
            for sql in &new_order_queries {
                black_box(Parser::parse_sql(black_box(sql)).unwrap());
            }
        });
    });

    group.bench_function("new_order_txn_arena_reuse", |b| {
        let mut arena = Bump::with_capacity(8192);
        b.iter(|| {
            for sql in &new_order_queries {
                arena.reset();
                let result = ArenaParser::parse_sql(black_box(sql), &arena).unwrap();
                black_box(&result);
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_lexer,
    bench_parser,
    bench_keywords,
    bench_identifiers,
    bench_arena_parser,
    bench_parser_comparison,
    bench_arena_with_conversion,
    bench_dml_comparison,
    bench_mixed_dml_workload,
);
criterion_main!(benches);
