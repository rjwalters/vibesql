//! Benchmarks for SIMD-accelerated predicate filtering
//!
//! These benchmarks compare row-at-a-time vs vectorized predicate evaluation,
//! demonstrating the performance benefits of SIMD filtering.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use vibesql_executor::select::columnar::batch::ColumnarBatch;
use vibesql_executor::select::columnar::filter::ColumnPredicate;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

/// Create test data for filtering benchmarks
fn create_numeric_data(row_count: usize) -> Vec<Row> {
    (0..row_count)
        .map(|i| {
            Row::new(vec![
                SqlValue::Integer((i % 100) as i64), // col0: integers 0-99
                SqlValue::Double(100.0 + (i % 1000) as f64), // col1: floats 100-1099
                SqlValue::Integer((i % 50) as i64),  // col2: integers 0-49
            ])
        })
        .collect()
}

/// Row-at-a-time filter evaluation (baseline)
fn row_at_a_time_filter(rows: &[Row], column_idx: usize, threshold: i64) -> Vec<bool> {
    rows.iter()
        .map(|row| {
            if let Some(SqlValue::Integer(v)) = row.get(column_idx) {
                *v < threshold
            } else {
                false
            }
        })
        .collect()
}

/// Row-at-a-time filter with NotEqual
fn row_at_a_time_filter_ne(rows: &[Row], column_idx: usize, value: i64) -> Vec<bool> {
    rows.iter()
        .map(
            |row| {
                if let Some(SqlValue::Integer(v)) = row.get(column_idx) {
                    *v != value
                } else {
                    false
                }
            },
        )
        .collect()
}

/// Row-at-a-time BETWEEN evaluation
fn row_at_a_time_between(rows: &[Row], column_idx: usize, low: f64, high: f64) -> Vec<bool> {
    rows.iter()
        .map(|row| {
            if let Some(SqlValue::Double(v)) = row.get(column_idx) {
                *v >= low && *v <= high
            } else {
                false
            }
        })
        .collect()
}

/// Benchmark LessThan predicate: row-at-a-time vs vectorized
fn bench_less_than_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_filter_less_than");

    for row_count in [10_000, 100_000, 1_000_000] {
        let rows = create_numeric_data(row_count);
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        group.throughput(Throughput::Elements(row_count as u64));

        // Benchmark row-at-a-time
        group.bench_with_input(BenchmarkId::new("row_at_a_time", row_count), &row_count, |b, _| {
            b.iter(|| {
                let _result = black_box(row_at_a_time_filter(&rows, 0, 50));
            });
        });

        // Benchmark vectorized/SIMD
        let predicate = ColumnPredicate::LessThan { column_idx: 0, value: SqlValue::Integer(50) };
        group.bench_with_input(
            BenchmarkId::new("vectorized_simd", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let _result = black_box(
                        vibesql_executor::select::columnar::simd_filter::simd_filter_batch(
                            &batch,
                            std::slice::from_ref(&predicate),
                        ),
                    );
                });
            },
        );
    }

    group.finish();
}

/// Benchmark NotEqual predicate: row-at-a-time vs vectorized
fn bench_not_equal_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_filter_not_equal");

    for row_count in [10_000, 100_000, 1_000_000] {
        let rows = create_numeric_data(row_count);
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        group.throughput(Throughput::Elements(row_count as u64));

        // Benchmark row-at-a-time
        group.bench_with_input(BenchmarkId::new("row_at_a_time", row_count), &row_count, |b, _| {
            b.iter(|| {
                let _result = black_box(row_at_a_time_filter_ne(&rows, 0, 25));
            });
        });

        // Benchmark vectorized/SIMD
        let predicate = ColumnPredicate::NotEqual { column_idx: 0, value: SqlValue::Integer(25) };
        group.bench_with_input(
            BenchmarkId::new("vectorized_simd", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let _result = black_box(
                        vibesql_executor::select::columnar::simd_filter::simd_filter_batch(
                            &batch,
                            std::slice::from_ref(&predicate),
                        ),
                    );
                });
            },
        );
    }

    group.finish();
}

/// Benchmark BETWEEN predicate: row-at-a-time vs vectorized
fn bench_between_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_filter_between");

    for row_count in [10_000, 100_000, 1_000_000] {
        let rows = create_numeric_data(row_count);
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        group.throughput(Throughput::Elements(row_count as u64));

        // Benchmark row-at-a-time
        group.bench_with_input(BenchmarkId::new("row_at_a_time", row_count), &row_count, |b, _| {
            b.iter(|| {
                let _result = black_box(row_at_a_time_between(&rows, 1, 200.0, 500.0));
            });
        });

        // Benchmark vectorized/SIMD
        let predicate = ColumnPredicate::Between {
            column_idx: 1,
            low: SqlValue::Double(200.0),
            high: SqlValue::Double(500.0),
        };
        group.bench_with_input(
            BenchmarkId::new("vectorized_simd", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let _result = black_box(
                        vibesql_executor::select::columnar::simd_filter::simd_filter_batch(
                            &batch,
                            std::slice::from_ref(&predicate),
                        ),
                    );
                });
            },
        );
    }

    group.finish();
}

/// Benchmark compound predicate: multiple filters combined
fn bench_compound_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_filter_compound");

    for row_count in [10_000, 100_000, 1_000_000] {
        let rows = create_numeric_data(row_count);
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        group.throughput(Throughput::Elements(row_count as u64));

        // Benchmark row-at-a-time (multiple conditions)
        group.bench_with_input(BenchmarkId::new("row_at_a_time", row_count), &row_count, |b, _| {
            b.iter(|| {
                let _result: Vec<bool> = rows
                    .iter()
                    .map(|row| {
                        let cond1 = if let Some(SqlValue::Integer(v)) = row.get(0) {
                            *v < 50
                        } else {
                            false
                        };
                        let cond2 = if let Some(SqlValue::Double(v)) = row.get(1) {
                            *v >= 200.0 && *v <= 800.0
                        } else {
                            false
                        };
                        let cond3 = if let Some(SqlValue::Integer(v)) = row.get(2) {
                            *v != 25
                        } else {
                            false
                        };
                        cond1 && cond2 && cond3
                    })
                    .collect();
                black_box(_result)
            });
        });

        // Benchmark vectorized/SIMD with multiple predicates
        let predicates = vec![
            ColumnPredicate::LessThan { column_idx: 0, value: SqlValue::Integer(50) },
            ColumnPredicate::Between {
                column_idx: 1,
                low: SqlValue::Double(200.0),
                high: SqlValue::Double(800.0),
            },
            ColumnPredicate::NotEqual { column_idx: 2, value: SqlValue::Integer(25) },
        ];
        group.bench_with_input(
            BenchmarkId::new("vectorized_simd", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let _result = black_box(
                        vibesql_executor::select::columnar::simd_filter::simd_filter_batch(
                            &batch,
                            &predicates,
                        ),
                    );
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_less_than_filter,
    bench_not_equal_filter,
    bench_between_filter,
    bench_compound_filter
);
criterion_main!(benches);
