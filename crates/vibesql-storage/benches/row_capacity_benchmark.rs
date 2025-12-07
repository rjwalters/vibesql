//! Benchmark for Row SmallVec inline capacity optimization
//!
//! This benchmark tests different ROW_INLINE_CAPACITY values to find the optimal
//! trade-off between inline storage and heap allocations.
//!
//! Run with:
//!   cargo bench --package vibesql-storage --bench row_capacity_benchmark
//!
//! Or run directly:
//!   cargo bench --package vibesql-storage --bench row_capacity_benchmark --no-run
//!   ./target/release/deps/row_capacity_benchmark-*
//!
//! Environment Variables:
//!   ROW_CAPACITY_ITERATIONS  Number of iterations per test (default: 100000)

use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};
use smallvec::SmallVec;
use std::hint::black_box;
use std::sync::Arc;
use vibesql_types::SqlValue;

/// Typical column counts for different query patterns
const COLUMN_COUNTS: [usize; 6] = [2, 4, 6, 8, 10, 16];

/// Type aliases for different capacities
type Row4 = SmallVec<[SqlValue; 4]>;
type Row6 = SmallVec<[SqlValue; 6]>;
type Row8 = SmallVec<[SqlValue; 8]>;
type Row10 = SmallVec<[SqlValue; 10]>;
type Row12 = SmallVec<[SqlValue; 12]>;

/// Generate a value for a given column index
fn make_value(i: usize) -> SqlValue {
    match i % 5 {
        0 => SqlValue::Integer(i as i64 * 1000),
        1 => SqlValue::Double(i as f64 * 3.14159),
        2 => SqlValue::Varchar(Arc::from(format!("value_{}", i))),
        3 => SqlValue::Boolean(i % 2 == 0),
        4 => SqlValue::Null,
        _ => unreachable!(),
    }
}

/// Create a row with capacity 4
fn create_row4(column_count: usize) -> Row4 {
    let mut row = SmallVec::new();
    for i in 0..column_count {
        row.push(make_value(i));
    }
    row
}

/// Create a row with capacity 6
fn create_row6(column_count: usize) -> Row6 {
    let mut row = SmallVec::new();
    for i in 0..column_count {
        row.push(make_value(i));
    }
    row
}

/// Create a row with capacity 8 (current default)
fn create_row8(column_count: usize) -> Row8 {
    let mut row = SmallVec::new();
    for i in 0..column_count {
        row.push(make_value(i));
    }
    row
}

/// Create a row with capacity 10
fn create_row10(column_count: usize) -> Row10 {
    let mut row = SmallVec::new();
    for i in 0..column_count {
        row.push(make_value(i));
    }
    row
}

/// Create a row with capacity 12
fn create_row12(column_count: usize) -> Row12 {
    let mut row = SmallVec::new();
    for i in 0..column_count {
        row.push(make_value(i));
    }
    row
}

/// Benchmark row creation with different capacities
fn bench_row_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_creation");

    for &col_count in &COLUMN_COUNTS {
        group.throughput(Throughput::Elements(col_count as u64));

        // Capacity 4
        group.bench_with_input(BenchmarkId::new("capacity_4", col_count), &col_count, |b, &cols| {
            b.iter(|| black_box(create_row4(cols)));
        });

        // Capacity 6
        group.bench_with_input(BenchmarkId::new("capacity_6", col_count), &col_count, |b, &cols| {
            b.iter(|| black_box(create_row6(cols)));
        });

        // Capacity 8 (current default)
        group.bench_with_input(BenchmarkId::new("capacity_8", col_count), &col_count, |b, &cols| {
            b.iter(|| black_box(create_row8(cols)));
        });

        // Capacity 10
        group.bench_with_input(
            BenchmarkId::new("capacity_10", col_count),
            &col_count,
            |b, &cols| {
                b.iter(|| black_box(create_row10(cols)));
            },
        );

        // Capacity 12
        group.bench_with_input(
            BenchmarkId::new("capacity_12", col_count),
            &col_count,
            |b, &cols| {
                b.iter(|| black_box(create_row12(cols)));
            },
        );
    }

    group.finish();
}

/// Benchmark batch row creation (simulates query result processing)
fn bench_batch_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_creation");
    const BATCH_SIZE: usize = 1000;

    for &col_count in &[4, 8, 12] {
        group.throughput(Throughput::Elements(BATCH_SIZE as u64));

        // Capacity 4
        group.bench_with_input(BenchmarkId::new("capacity_4", col_count), &col_count, |b, &cols| {
            b.iter(|| {
                let batch: Vec<Row4> = (0..BATCH_SIZE).map(|_| create_row4(cols)).collect();
                black_box(batch)
            });
        });

        // Capacity 8 (current default)
        group.bench_with_input(BenchmarkId::new("capacity_8", col_count), &col_count, |b, &cols| {
            b.iter(|| {
                let batch: Vec<Row8> = (0..BATCH_SIZE).map(|_| create_row8(cols)).collect();
                black_box(batch)
            });
        });

        // Capacity 12
        group.bench_with_input(
            BenchmarkId::new("capacity_12", col_count),
            &col_count,
            |b, &cols| {
                b.iter(|| {
                    let batch: Vec<Row12> = (0..BATCH_SIZE).map(|_| create_row12(cols)).collect();
                    black_box(batch)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark row cloning (important for query processing)
fn bench_row_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_clone");

    for &col_count in &COLUMN_COUNTS {
        // Pre-create rows to clone
        let row4 = create_row4(col_count);
        let row8 = create_row8(col_count);
        let row12 = create_row12(col_count);

        group.bench_with_input(BenchmarkId::new("capacity_4", col_count), &row4, |b, row| {
            b.iter(|| black_box(row.clone()));
        });

        group.bench_with_input(BenchmarkId::new("capacity_8", col_count), &row8, |b, row| {
            b.iter(|| black_box(row.clone()));
        });

        group.bench_with_input(BenchmarkId::new("capacity_12", col_count), &row12, |b, row| {
            b.iter(|| black_box(row.clone()));
        });
    }

    group.finish();
}

/// Benchmark row access patterns (simulates filtering)
fn bench_row_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_access");

    for &col_count in &[4, 8, 16] {
        // Pre-create rows
        let row4 = create_row4(col_count);
        let row8 = create_row8(col_count);
        let row12 = create_row12(col_count);

        group.bench_with_input(BenchmarkId::new("capacity_4", col_count), &row4, |b, row| {
            b.iter(|| {
                // Simulate accessing multiple columns (like in a filter)
                let mut sum = 0i64;
                for i in 0..row.len() {
                    if let SqlValue::Integer(v) = &row[i] {
                        sum += v;
                    }
                }
                black_box(sum)
            });
        });

        group.bench_with_input(BenchmarkId::new("capacity_8", col_count), &row8, |b, row| {
            b.iter(|| {
                let mut sum = 0i64;
                for i in 0..row.len() {
                    if let SqlValue::Integer(v) = &row[i] {
                        sum += v;
                    }
                }
                black_box(sum)
            });
        });

        group.bench_with_input(BenchmarkId::new("capacity_12", col_count), &row12, |b, row| {
            b.iter(|| {
                let mut sum = 0i64;
                for i in 0..row.len() {
                    if let SqlValue::Integer(v) = &row[i] {
                        sum += v;
                    }
                }
                black_box(sum)
            });
        });
    }

    group.finish();
}

/// Print size information for different capacities
fn print_size_info() {
    use std::mem::size_of;

    eprintln!("\n=== Size Information ===\n");
    eprintln!("SqlValue size: {} bytes", size_of::<SqlValue>());
    eprintln!();
    eprintln!("SmallVec inline sizes:");
    eprintln!("  Capacity 4:  {} bytes", size_of::<Row4>());
    eprintln!("  Capacity 6:  {} bytes", size_of::<Row6>());
    eprintln!("  Capacity 8:  {} bytes", size_of::<Row8>());
    eprintln!("  Capacity 10: {} bytes", size_of::<Row10>());
    eprintln!("  Capacity 12: {} bytes", size_of::<Row12>());
    eprintln!();

    // Show spill behavior
    eprintln!("Spill behavior (heap allocation when columns > capacity):");
    eprintln!("  Capacity 4:  spills at 5+ columns");
    eprintln!("  Capacity 6:  spills at 7+ columns");
    eprintln!("  Capacity 8:  spills at 9+ columns (current default)");
    eprintln!("  Capacity 10: spills at 11+ columns");
    eprintln!("  Capacity 12: spills at 13+ columns");
    eprintln!();
}

criterion_group!(benches, bench_row_creation, bench_batch_creation, bench_row_clone, bench_row_access);

fn main() {
    // Print size info before running benchmarks
    print_size_info();

    // Run criterion benchmarks
    benches();
    Criterion::default().configure_from_args().final_summary();
}
