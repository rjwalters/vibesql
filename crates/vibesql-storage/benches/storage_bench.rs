//! Storage subsystem benchmarks
//!
//! Benchmarks for:
//! - B-tree operations (insert, lookup, range scan)
//! - Table operations (insert, update, scan)
//! - Buffer pool and page cache efficiency
//!
//! Run with:
//!   cargo bench --package vibesql-storage --bench storage_bench
//!
//! Or via Makefile:
//!   make bench-storage

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{Row, Table};
use vibesql_types::{DataType, SqlValue};

/// Create a test row with the given id
fn make_row(id: i64) -> Row {
    Row::new(vec![
        SqlValue::Integer(id),
        SqlValue::Varchar(arcstr::ArcStr::from(format!("name_{}", id))),
        SqlValue::Double((id as f64) * 100.0),
    ])
}

/// Create a table schema for benchmarking
fn make_schema(name: &str) -> TableSchema {
    TableSchema::with_primary_key(
        name.to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("amount".to_string(), DataType::DoublePrecision, false),
        ],
        vec!["id".to_string()],
    )
}

/// Benchmark table insert operations
fn bench_table_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_insert");

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let schema = make_schema("bench_table");
                let mut table = Table::new(schema);
                for i in 0..size {
                    table.insert(make_row(i as i64)).unwrap();
                }
                black_box(table.row_count())
            });
        });
    }

    group.finish();
}

/// Benchmark table scan operations
fn bench_table_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_scan");

    for size in [100, 1000, 10000].iter() {
        // Setup: create and populate table
        let schema = make_schema("bench_table");
        let mut table = Table::new(schema);
        for i in 0..*size {
            table.insert(make_row(i as i64)).unwrap();
        }

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &table, |b, table| {
            b.iter(|| {
                let mut count = 0;
                for row in table.scan().iter() {
                    black_box(row);
                    count += 1;
                }
                count
            });
        });
    }

    group.finish();
}

/// Benchmark primary key lookups via index
fn bench_pk_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("pk_lookup");

    for size in [1000, 10000, 100000].iter() {
        // Setup: create and populate table
        let schema = make_schema("bench_table");
        let mut table = Table::new(schema);
        for i in 0..*size {
            table.insert(make_row(i as i64)).unwrap();
        }

        group.bench_with_input(BenchmarkId::from_parameter(size), &table, |b, table| {
            let mut i: i64 = 0;
            b.iter(|| {
                // Lookup a random key
                let key = SqlValue::Integer(i % (*size as i64));
                if let Some(pk_index) = table.primary_key_index() {
                    black_box(pk_index.get(&vec![key]));
                }
                i += 1;
            });
        });
    }

    group.finish();
}

/// Benchmark table update operations
fn bench_table_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_update");

    for size in [100, 1000, 10000].iter() {
        // Setup: create and populate table
        let schema = make_schema("bench_table");
        let mut table = Table::new(schema);
        for i in 0..*size {
            table.insert(make_row(i as i64)).unwrap();
        }

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut table_copy = table.clone();
            b.iter(|| {
                // Update all rows
                for i in 0..size {
                    let new_row = Row::new(vec![
                        SqlValue::Integer(i as i64),
                        SqlValue::Varchar(arcstr::ArcStr::from(format!("updated_{}", i))),
                        SqlValue::Double((i as f64) * 200.0),
                    ]);
                    table_copy.update_row(i, new_row).unwrap();
                }
                black_box(table_copy.row_count())
            });
        });
    }

    group.finish();
}

/// Benchmark Row construction and access
fn bench_row_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_ops");

    // Benchmark row construction
    group.bench_function("construct_3_cols", |b| {
        b.iter(|| {
            black_box(Row::new(vec![
                SqlValue::Integer(42),
                SqlValue::Varchar(arcstr::ArcStr::from("hello world")),
                SqlValue::Double(123.45),
            ]))
        });
    });

    // Benchmark row field access
    let row = Row::new(vec![
        SqlValue::Integer(42),
        SqlValue::Varchar(arcstr::ArcStr::from("hello world")),
        SqlValue::Double(123.45),
    ]);

    group.bench_function("get_field", |b| {
        b.iter(|| {
            black_box(row.get(0));
            black_box(row.get(1));
            black_box(row.get(2));
        });
    });

    group.finish();
}

/// Benchmark SqlValue operations (comparison, clone)
fn bench_sql_value_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_value_ops");

    let int_val = SqlValue::Integer(42);
    let varchar_val = SqlValue::Varchar(arcstr::ArcStr::from("hello world benchmark string"));
    let double_val = SqlValue::Double(12345.6789);

    group.bench_function("clone_integer", |b| {
        b.iter(|| black_box(int_val.clone()));
    });

    group.bench_function("clone_varchar", |b| {
        b.iter(|| black_box(varchar_val.clone()));
    });

    group.bench_function("clone_double", |b| {
        b.iter(|| black_box(double_val.clone()));
    });

    // Comparison benchmarks
    let int_val2 = SqlValue::Integer(43);
    let varchar_val2 = SqlValue::Varchar(arcstr::ArcStr::from("hello world benchmark string!"));

    group.bench_function("compare_integer", |b| {
        b.iter(|| black_box(int_val.partial_cmp(&int_val2)));
    });

    group.bench_function("compare_varchar", |b| {
        b.iter(|| black_box(varchar_val.partial_cmp(&varchar_val2)));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_table_insert,
    bench_table_scan,
    bench_pk_lookup,
    bench_table_update,
    bench_row_ops,
    bench_sql_value_ops,
);

criterion_main!(benches);
