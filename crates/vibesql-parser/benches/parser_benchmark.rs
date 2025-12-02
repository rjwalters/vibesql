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
const MULTI_COLUMN: &str = "SELECT id, name, email, created_at FROM users WHERE status = 'active' AND user_role = 'admin'";

/// INSERT with values
const INSERT_SINGLE: &str = "INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com')";

/// INSERT with multiple rows
const INSERT_MULTI: &str = r#"INSERT INTO users (id, name, email) VALUES
    (1, 'John', 'john@example.com'),
    (2, 'Jane', 'jane@example.com'),
    (3, 'Bob', 'bob@example.com')"#;

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
        ("tpch_q1", TPCH_Q1),
        ("complex_join", COMPLEX_JOIN),
        ("create_table", CREATE_TABLE),
        ("cte_query", CTE_QUERY),
    ];

    for (name, sql) in queries {
        group.throughput(Throughput::Bytes(sql.len() as u64));
        group.bench_with_input(BenchmarkId::new("parse", name), sql, |b, sql| {
            b.iter(|| {
                black_box(Parser::parse_sql(black_box(sql)).unwrap())
            });
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
        b.iter(|| {
            black_box(Parser::parse_sql(black_box(keyword_heavy)).unwrap())
        });
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
        b.iter(|| {
            black_box(Parser::parse_sql(black_box(ident_heavy)).unwrap())
        });
    });

    group.finish();
}

/// Benchmark arena-allocated parser (SELECT statements only for Phase 1)
fn bench_arena_parser(c: &mut Criterion) {
    let mut group = c.benchmark_group("arena_parser");

    // Only SELECT queries are supported by the arena parser
    let queries = [
        ("simple_select", SIMPLE_SELECT),
        ("point_lookup", POINT_LOOKUP),
        ("multi_column", MULTI_COLUMN),
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
                let _ = black_box(&result);
                drop(arena);
            });
        });
    }

    group.finish();
}

/// Compare standard parser vs arena parser
fn bench_parser_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("parser_comparison");

    // Note: Only queries that work with Phase 1 arena parser (no DATE literals, etc.)
    let queries = [
        ("simple_select", SIMPLE_SELECT),
        ("point_lookup", POINT_LOOKUP),
        ("multi_column", MULTI_COLUMN),
        ("complex_join", COMPLEX_JOIN),
    ];

    for (name, sql) in queries {
        group.throughput(Throughput::Bytes(sql.len() as u64));

        // Standard parser
        group.bench_with_input(BenchmarkId::new("standard", name), sql, |b, sql| {
            b.iter(|| {
                black_box(Parser::parse_sql(black_box(sql)).unwrap())
            });
        });

        // Arena parser (fresh arena each time)
        group.bench_with_input(BenchmarkId::new("arena", name), sql, |b, sql| {
            b.iter(|| {
                let arena = Bump::new();
                let result = ArenaParser::parse_sql(black_box(sql), &arena).unwrap();
                let _ = black_box(&result);
                drop(arena);
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
            b.iter(|| {
                black_box(Parser::parse_sql(black_box(sql)).unwrap())
            });
        });

        // Arena parser with conversion to owned
        // This is the recommended approach for Phase 3 integration
        group.bench_with_input(BenchmarkId::new("arena_to_owned", name), sql, |b, sql| {
            b.iter(|| {
                black_box(parse_select_to_owned(black_box(sql)).unwrap())
            });
        });
    }

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
);
criterion_main!(benches);
