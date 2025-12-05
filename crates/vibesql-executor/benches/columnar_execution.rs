//! Benchmarks for columnar vs row-by-row execution
//!
//! These benchmarks demonstrate the performance benefits of columnar execution
//! for analytical queries with aggregation and filtering.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use vibesql_executor::select::columnar::{
    execute_columnar_aggregate, fast_aggregate_on_rows, AggregateOp, AggregateSource,
    AggregateSpec, ColumnPredicate,
};
use vibesql_storage::Row;
use vibesql_types::{Date, SqlValue};

/// Create test data similar to TPC-H lineitem table
fn create_lineitem_data(row_count: usize) -> Vec<Row> {
    (0..row_count)
        .map(|i| {
            Row::new(vec![
                SqlValue::Integer((i % 50) as i64),               // l_quantity
                SqlValue::Double(100.0 + (i % 1000) as f64),      // l_extendedprice
                SqlValue::Double(0.01 + (i % 10) as f64 / 100.0), // l_discount
                SqlValue::Date(Date::new(1994 + (i % 2) as i32, 1 + (i % 12) as u8, 1).unwrap()), // l_shipdate
            ])
        })
        .collect()
}

/// Row-by-row aggregate implementation for comparison
fn row_by_row_aggregate(
    rows: &[Row],
    predicates: &[ColumnPredicate],
    aggregates: &[(usize, AggregateOp)],
) -> Vec<SqlValue> {
    // Filter rows
    let filtered_rows: Vec<&Row> = rows
        .iter()
        .filter(|row| {
            predicates.iter().all(|pred| match pred {
                ColumnPredicate::LessThan { column_idx, value } => {
                    if let Some(cell_value) = row.get(*column_idx) {
                        compare_less_than(cell_value, value)
                    } else {
                        false
                    }
                }
                ColumnPredicate::Between { column_idx, low, high } => {
                    if let Some(cell_value) = row.get(*column_idx) {
                        !compare_less_than(cell_value, low) && !compare_less_than(high, cell_value)
                    } else {
                        false
                    }
                }
                _ => true,
            })
        })
        .collect();

    // Compute aggregates
    aggregates
        .iter()
        .map(|(column_idx, op)| match op {
            AggregateOp::Sum => {
                let sum: f64 = filtered_rows
                    .iter()
                    .filter_map(|row| row.get(*column_idx))
                    .filter_map(|v| match v {
                        SqlValue::Integer(i) => Some(*i as f64),
                        SqlValue::Double(d) => Some(*d),
                        _ => None,
                    })
                    .sum();
                SqlValue::Double(sum)
            }
            AggregateOp::Count => SqlValue::Integer(filtered_rows.len() as i64),
            AggregateOp::Avg => {
                let sum: f64 = filtered_rows
                    .iter()
                    .filter_map(|row| row.get(*column_idx))
                    .filter_map(|v| match v {
                        SqlValue::Integer(i) => Some(*i as f64),
                        SqlValue::Double(d) => Some(*d),
                        _ => None,
                    })
                    .sum();
                let count = filtered_rows.len();
                if count > 0 {
                    SqlValue::Double(sum / count as f64)
                } else {
                    SqlValue::Null
                }
            }
            _ => SqlValue::Null,
        })
        .collect()
}

fn compare_less_than(a: &SqlValue, b: &SqlValue) -> bool {
    match (a, b) {
        (SqlValue::Integer(a), SqlValue::Integer(b)) => a < b,
        (SqlValue::Double(a), SqlValue::Double(b)) => a < b,
        _ => false,
    }
}

/// Benchmark TPC-H Q6 style query with filtering and aggregation
fn bench_tpch_q6_style(c: &mut Criterion) {
    let mut group = c.benchmark_group("tpch_q6_aggregation");

    for row_count in [1_000, 10_000, 100_000] {
        let rows = create_lineitem_data(row_count);

        group.throughput(Throughput::Elements(row_count as u64));

        // Predicates: l_quantity < 24 AND l_discount BETWEEN 0.05 AND 0.07
        let predicates = vec![
            ColumnPredicate::LessThan { column_idx: 0, value: SqlValue::Integer(24) },
            ColumnPredicate::Between {
                column_idx: 2,
                low: SqlValue::Double(0.05),
                high: SqlValue::Double(0.07),
            },
        ];

        // Aggregates: SUM(l_extendedprice), COUNT(*)
        let aggregates_tuple = vec![(1, AggregateOp::Sum), (0, AggregateOp::Count)];
        let aggregates = vec![
            AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(1) },
            AggregateSpec { op: AggregateOp::Count, source: AggregateSource::CountStar },
        ];

        // Benchmark row-by-row execution
        group.bench_with_input(BenchmarkId::new("row_by_row", row_count), &row_count, |b, _| {
            b.iter(|| {
                let _result =
                    black_box(row_by_row_aggregate(&rows, &predicates, &aggregates_tuple));
            });
        });

        // Benchmark columnar execution (converts rows to batch first)
        group.bench_with_input(BenchmarkId::new("columnar", row_count), &row_count, |b, _| {
            b.iter(|| {
                let _result = black_box(
                    execute_columnar_aggregate(&rows, &predicates, &aggregates, None).unwrap(),
                );
            });
        });

        // Benchmark fast single-pass aggregate (no batch conversion)
        group.bench_with_input(
            BenchmarkId::new("fast_aggregate", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let _result =
                        black_box(fast_aggregate_on_rows(&rows, &predicates, &aggregates).unwrap());
                });
            },
        );
    }

    group.finish();
}

/// Benchmark simple aggregation without filtering
fn bench_simple_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_aggregation");

    for row_count in [1_000, 10_000, 100_000] {
        let rows = create_lineitem_data(row_count);

        group.throughput(Throughput::Elements(row_count as u64));

        // No predicates (full table scan)
        let predicates = vec![];

        // Aggregates: SUM(l_extendedprice), AVG(l_discount), COUNT(*)
        let aggregates_tuple =
            vec![(1, AggregateOp::Sum), (2, AggregateOp::Avg), (0, AggregateOp::Count)];
        let aggregates = vec![
            AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(1) },
            AggregateSpec { op: AggregateOp::Avg, source: AggregateSource::Column(2) },
            AggregateSpec { op: AggregateOp::Count, source: AggregateSource::CountStar },
        ];

        // Benchmark row-by-row execution
        group.bench_with_input(BenchmarkId::new("row_by_row", row_count), &row_count, |b, _| {
            b.iter(|| {
                let _result =
                    black_box(row_by_row_aggregate(&rows, &predicates, &aggregates_tuple));
            });
        });

        // Benchmark columnar execution
        group.bench_with_input(BenchmarkId::new("columnar", row_count), &row_count, |b, _| {
            b.iter(|| {
                let _result = black_box(
                    execute_columnar_aggregate(&rows, &predicates, &aggregates, None).unwrap(),
                );
            });
        });

        // Benchmark fast single-pass aggregate (no batch conversion)
        group.bench_with_input(
            BenchmarkId::new("fast_aggregate", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let _result =
                        black_box(fast_aggregate_on_rows(&rows, &predicates, &aggregates).unwrap());
                });
            },
        );
    }

    group.finish();
}

/// Benchmark highly selective filtering (only ~1% of rows match)
fn bench_selective_filtering(c: &mut Criterion) {
    let mut group = c.benchmark_group("selective_filtering");

    for row_count in [10_000, 100_000] {
        let rows = create_lineitem_data(row_count);

        group.throughput(Throughput::Elements(row_count as u64));

        // Highly selective predicate (only ~1% match)
        let predicates = vec![
            ColumnPredicate::LessThan {
                column_idx: 0,
                value: SqlValue::Integer(5), // l_quantity < 5 (only ~10% of values)
            },
            ColumnPredicate::Between {
                column_idx: 2,
                low: SqlValue::Double(0.05),
                high: SqlValue::Double(0.06), // Narrow range
            },
        ];

        let aggregates_tuple = vec![(1, AggregateOp::Sum)];
        let aggregates =
            vec![AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(1) }];

        // Benchmark row-by-row execution
        group.bench_with_input(BenchmarkId::new("row_by_row", row_count), &row_count, |b, _| {
            b.iter(|| {
                let _result =
                    black_box(row_by_row_aggregate(&rows, &predicates, &aggregates_tuple));
            });
        });

        // Benchmark columnar execution
        group.bench_with_input(BenchmarkId::new("columnar", row_count), &row_count, |b, _| {
            b.iter(|| {
                let _result = black_box(
                    execute_columnar_aggregate(&rows, &predicates, &aggregates, None).unwrap(),
                );
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_tpch_q6_style, bench_simple_aggregation, bench_selective_filtering);
criterion_main!(benches);
