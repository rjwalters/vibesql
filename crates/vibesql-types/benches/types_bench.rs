//! Type system benchmarks
//!
//! Benchmarks for SqlValue operations:
//! - Construction
//! - Comparison
//! - Clone/Copy
//! - Type checking
//! - Hashing (for hash tables)
//! - SSO boundary (arcstr small string optimization)
//!
//! Run with:
//!   cargo bench --package vibesql-types --bench types_bench

use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::HashMap;
use std::hint::black_box;
use vibesql_types::{Date, SqlValue, StringValue, Time, Timestamp};

/// Helper to create StringValue from &str
fn string_value(s: &str) -> StringValue {
    StringValue::from(s)
}

/// Benchmark SqlValue construction
fn bench_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("construction");

    group.bench_function("integer", |b| {
        b.iter(|| black_box(SqlValue::Integer(42)));
    });

    // Tiny string: 2 bytes (well within SSO threshold)
    group.bench_function("varchar_tiny", |b| {
        b.iter(|| black_box(SqlValue::Varchar(string_value("id"))));
    });

    // Short string: 5 bytes (within SSO threshold)
    group.bench_function("varchar_short", |b| {
        b.iter(|| black_box(SqlValue::Varchar(string_value("hello"))));
    });

    // SSO boundary: 22 bytes (max SSO size for arcstr)
    group.bench_function("varchar_sso_boundary", |b| {
        let s = "a".repeat(22);
        b.iter(|| black_box(SqlValue::Varchar(string_value(&s))));
    });

    // Just over SSO: 23 bytes (requires heap allocation in arcstr)
    group.bench_function("varchar_just_over_sso", |b| {
        let s = "a".repeat(23);
        b.iter(|| black_box(SqlValue::Varchar(string_value(&s))));
    });

    // Medium string: 50 bytes
    group.bench_function("varchar_medium", |b| {
        let s = "a".repeat(50);
        b.iter(|| black_box(SqlValue::Varchar(string_value(&s))));
    });

    // Long string: 100 bytes
    group.bench_function("varchar_long", |b| {
        let s = "a".repeat(100);
        b.iter(|| black_box(SqlValue::Varchar(string_value(&s))));
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

    // Tiny string clone (should be very fast with SSO)
    let varchar_tiny = SqlValue::Varchar(string_value("id"));
    group.bench_function("varchar_tiny", |b| {
        b.iter(|| black_box(varchar_tiny.clone()));
    });

    // Short string clone
    let varchar_short = SqlValue::Varchar(string_value("hello"));
    group.bench_function("varchar_short", |b| {
        b.iter(|| black_box(varchar_short.clone()));
    });

    // SSO boundary clone - critical test for arcstr benefit
    let varchar_sso = SqlValue::Varchar(string_value(&"a".repeat(22)));
    group.bench_function("varchar_sso_boundary", |b| {
        b.iter(|| black_box(varchar_sso.clone()));
    });

    // Just over SSO - should show Arc-like behavior
    let varchar_over_sso = SqlValue::Varchar(string_value(&"a".repeat(23)));
    group.bench_function("varchar_just_over_sso", |b| {
        b.iter(|| black_box(varchar_over_sso.clone()));
    });

    // Long string clone
    let varchar_long = SqlValue::Varchar(string_value(&"a".repeat(100)));
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

    // Short varchar comparisons (within SSO)
    let varchar_a = SqlValue::Varchar(string_value("hello world"));
    let varchar_b = SqlValue::Varchar(string_value("hello world!"));
    group.bench_function("varchar_eq", |b| {
        b.iter(|| black_box(varchar_a == varchar_b));
    });
    group.bench_function("varchar_ord", |b| {
        b.iter(|| black_box(varchar_a.partial_cmp(&varchar_b)));
    });

    // SSO boundary comparison
    let varchar_sso_a = SqlValue::Varchar(string_value(&"a".repeat(22)));
    let varchar_sso_b = SqlValue::Varchar(string_value(&("a".repeat(21) + "b")));
    group.bench_function("varchar_sso_eq", |b| {
        b.iter(|| black_box(varchar_sso_a == varchar_sso_b));
    });

    // Long varchar comparisons
    let varchar_long_a = SqlValue::Varchar(string_value(&"a".repeat(100)));
    let varchar_long_b = SqlValue::Varchar(string_value(&("a".repeat(99) + "b")));
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

    // Short varchar insertion (within SSO)
    group.bench_function("hashmap_insert_varchar_short", |b| {
        b.iter(|| {
            let mut map = HashMap::new();
            for i in 0..100 {
                // key_0 to key_99 are all within SSO (max 6 chars)
                map.insert(SqlValue::Varchar(string_value(&format!("key_{}", i))), i);
            }
            black_box(map.len())
        });
    });

    // SSO boundary insertion
    group.bench_function("hashmap_insert_varchar_sso", |b| {
        b.iter(|| {
            let mut map = HashMap::new();
            for i in 0..100 {
                // 22-char keys at SSO boundary
                let key = format!("{:022}", i);
                map.insert(SqlValue::Varchar(string_value(&key)), i);
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

    group.bench_function("hashmap_lookup_varchar_short", |b| {
        let mut map = HashMap::new();
        for i in 0..1000 {
            map.insert(SqlValue::Varchar(string_value(&format!("key_{}", i))), i);
        }
        let mut j = 0;
        b.iter(|| {
            let key = SqlValue::Varchar(string_value(&format!("key_{}", j % 1000)));
            black_box(map.get(&key));
            j += 1;
        });
    });

    group.bench_function("hashmap_lookup_varchar_sso", |b| {
        let mut map = HashMap::new();
        for i in 0..1000 {
            let key = format!("{:022}", i);
            map.insert(SqlValue::Varchar(string_value(&key)), i);
        }
        let mut j = 0;
        b.iter(|| {
            let key = format!("{:022}", j % 1000);
            black_box(map.get(&SqlValue::Varchar(string_value(&key))));
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
        SqlValue::Varchar(string_value("hello")),
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

/// Benchmark realistic SQL column name patterns
/// These are typical short strings that would benefit from SSO
fn bench_realistic_column_names(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_columns");

    // Common column names in databases (all within SSO threshold)
    let column_names = [
        "id",       // 2 bytes
        "name",     // 4 bytes
        "status",   // 6 bytes
        "email",    // 5 bytes
        "created_at", // 10 bytes
        "updated_at", // 10 bytes
        "user_id",  // 7 bytes
        "order_id", // 8 bytes
        "price",    // 5 bytes
        "quantity", // 8 bytes
    ];

    // Benchmark creating many SqlValues with typical column names
    group.bench_function("create_10_columns", |b| {
        b.iter(|| {
            for name in &column_names {
                black_box(SqlValue::Varchar(string_value(name)));
            }
        });
    });

    // Benchmark cloning typical column values
    let values: Vec<SqlValue> = column_names
        .iter()
        .map(|s| SqlValue::Varchar(string_value(s)))
        .collect();

    group.bench_function("clone_10_columns", |b| {
        b.iter(|| {
            for v in &values {
                black_box(v.clone());
            }
        });
    });

    // Common short values (single char codes, status strings)
    let short_values = [
        "A",        // 1 byte
        "N",        // 1 byte
        "active",   // 6 bytes
        "pending",  // 7 bytes
        "shipped",  // 7 bytes
        "completed", // 9 bytes
    ];

    group.bench_function("create_short_values", |b| {
        b.iter(|| {
            for v in &short_values {
                black_box(SqlValue::Varchar(string_value(v)));
            }
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
    bench_realistic_column_names,
);

criterion_main!(benches);
