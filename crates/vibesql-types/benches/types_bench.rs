//! Type system benchmarks
//!
//! Benchmarks for SqlValue operations:
//! - Construction
//! - Comparison
//! - Clone/Copy
//! - Type checking
//! - Hashing (for hash tables)
//!
//! Run with:
//!   cargo bench --package vibesql-types --bench types_bench
//!
//! Or via Makefile:
//!   make bench-types

use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::HashMap;
use std::hint::black_box;
use vibesql_types::{Date, SqlValue, Time, Timestamp};

/// Benchmark SqlValue construction
fn bench_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("construction");

    group.bench_function("integer", |b| {
        b.iter(|| black_box(SqlValue::Integer(42)));
    });

    group.bench_function("varchar_short", |b| {
        b.iter(|| black_box(SqlValue::Varchar("hello".to_string())));
    });

    group.bench_function("varchar_long", |b| {
        let s = "a".repeat(100);
        b.iter(|| black_box(SqlValue::Varchar(s.clone())));
    });

    group.bench_function("double", |b| {
        b.iter(|| black_box(SqlValue::Double(123.456789)));
    });

    group.bench_function("boolean", |b| {
        b.iter(|| black_box(SqlValue::Boolean(true)));
    });

    group.bench_function("null", |b| {
        b.iter(|| black_box(SqlValue::Null));
    });

    group.bench_function("date", |b| {
        b.iter(|| black_box(SqlValue::Date(Date::new(2024, 11, 29).unwrap())));
    });

    group.bench_function("timestamp", |b| {
        b.iter(|| {
            black_box(SqlValue::Timestamp(Timestamp::new(
                Date::new(2024, 11, 29).unwrap(),
                Time::new(12, 30, 45, 0).unwrap(),
            )))
        });
    });

    group.finish();
}

/// Benchmark SqlValue clone operations
fn bench_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("clone");

    let int_val = SqlValue::Integer(42);
    group.bench_function("integer", |b| {
        b.iter(|| black_box(int_val.clone()));
    });

    let varchar_short = SqlValue::Varchar("hello".to_string());
    group.bench_function("varchar_short", |b| {
        b.iter(|| black_box(varchar_short.clone()));
    });

    let varchar_long = SqlValue::Varchar("a".repeat(100));
    group.bench_function("varchar_long", |b| {
        b.iter(|| black_box(varchar_long.clone()));
    });

    let double_val = SqlValue::Double(123.456789);
    group.bench_function("double", |b| {
        b.iter(|| black_box(double_val.clone()));
    });

    let date_val = SqlValue::Date(Date::new(2024, 11, 29).unwrap());
    group.bench_function("date", |b| {
        b.iter(|| black_box(date_val.clone()));
    });

    let timestamp_val = SqlValue::Timestamp(Timestamp::new(
        Date::new(2024, 11, 29).unwrap(),
        Time::new(12, 30, 45, 0).unwrap(),
    ));
    group.bench_function("timestamp", |b| {
        b.iter(|| black_box(timestamp_val.clone()));
    });

    let null_val = SqlValue::Null;
    group.bench_function("null", |b| {
        b.iter(|| black_box(null_val.clone()));
    });

    group.finish();
}

/// Benchmark SqlValue comparison operations
fn bench_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison");

    // Integer comparisons
    let int_a = SqlValue::Integer(42);
    let int_b = SqlValue::Integer(43);
    group.bench_function("integer_eq", |b| {
        b.iter(|| black_box(int_a == int_b));
    });
    group.bench_function("integer_ord", |b| {
        b.iter(|| black_box(int_a.partial_cmp(&int_b)));
    });

    // Varchar comparisons
    let varchar_a = SqlValue::Varchar("hello world".to_string());
    let varchar_b = SqlValue::Varchar("hello world!".to_string());
    group.bench_function("varchar_eq", |b| {
        b.iter(|| black_box(varchar_a == varchar_b));
    });
    group.bench_function("varchar_ord", |b| {
        b.iter(|| black_box(varchar_a.partial_cmp(&varchar_b)));
    });

    // Long varchar comparisons
    let varchar_long_a = SqlValue::Varchar("a".repeat(100));
    let varchar_long_b = SqlValue::Varchar("a".repeat(99) + "b");
    group.bench_function("varchar_long_eq", |b| {
        b.iter(|| black_box(varchar_long_a == varchar_long_b));
    });

    // Date comparisons
    let date_a = SqlValue::Date(Date::new(2024, 11, 29).unwrap());
    let date_b = SqlValue::Date(Date::new(2024, 11, 30).unwrap());
    group.bench_function("date_eq", |b| {
        b.iter(|| black_box(date_a == date_b));
    });
    group.bench_function("date_ord", |b| {
        b.iter(|| black_box(date_a.partial_cmp(&date_b)));
    });

    // Null comparisons
    let null = SqlValue::Null;
    group.bench_function("null_eq", |b| {
        b.iter(|| black_box(null == int_a));
    });

    group.finish();
}

/// Benchmark SqlValue hashing (important for hash joins)
fn bench_hashing(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashing");

    // Hash map insertion (tests hashing + equality)
    group.bench_function("hashmap_insert_integer", |b| {
        b.iter(|| {
            let mut map = HashMap::new();
            for i in 0..100 {
                map.insert(SqlValue::Integer(i), i);
            }
            black_box(map.len())
        });
    });

    group.bench_function("hashmap_insert_varchar", |b| {
        b.iter(|| {
            let mut map = HashMap::new();
            for i in 0..100 {
                map.insert(SqlValue::Varchar(format!("key_{}", i)), i);
            }
            black_box(map.len())
        });
    });

    // Hash map lookup
    group.bench_function("hashmap_lookup_integer", |b| {
        let mut map = HashMap::new();
        for i in 0..1000 {
            map.insert(SqlValue::Integer(i), i);
        }
        let mut j = 0i64;
        b.iter(|| {
            let key = SqlValue::Integer(j % 1000);
            black_box(map.get(&key));
            j += 1;
        });
    });

    group.bench_function("hashmap_lookup_varchar", |b| {
        let mut map = HashMap::new();
        for i in 0..1000 {
            map.insert(SqlValue::Varchar(format!("key_{}", i)), i);
        }
        let mut j = 0;
        b.iter(|| {
            let key = SqlValue::Varchar(format!("key_{}", j % 1000));
            black_box(map.get(&key));
            j += 1;
        });
    });

    group.finish();
}

/// Benchmark SqlValue type checking operations
fn bench_type_checking(c: &mut Criterion) {
    let mut group = c.benchmark_group("type_checking");

    let values = [
        SqlValue::Integer(42),
        SqlValue::Varchar("hello".to_string()),
        SqlValue::Double(99.99),
        SqlValue::Boolean(true),
        SqlValue::Null,
        SqlValue::Date(Date::new(2024, 11, 29).unwrap()),
    ];

    group.bench_function("is_null", |b| {
        let mut i = 0;
        b.iter(|| {
            black_box(values[i % values.len()].is_null());
            i += 1;
        });
    });

    group.bench_function("type_name", |b| {
        let mut i = 0;
        b.iter(|| {
            black_box(values[i % values.len()].type_name());
            i += 1;
        });
    });

    group.bench_function("get_type", |b| {
        let mut i = 0;
        b.iter(|| {
            black_box(values[i % values.len()].get_type());
            i += 1;
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_construction,
    bench_clone,
    bench_comparison,
    bench_hashing,
    bench_type_checking,
);

criterion_main!(benches);
