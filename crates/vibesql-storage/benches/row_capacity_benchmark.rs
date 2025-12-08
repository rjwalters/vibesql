//! Benchmark for Row SmallVec inline capacity optimization
//!
//! This benchmark tests different ROW_INLINE_CAPACITY values to find the optimal
//! trade-off between inline storage and heap allocations.
//!
//! Migrated from Criterion to custom harness for deterministic timing.
//!
//! Run with:
//!   cargo bench --package vibesql-storage --bench row_capacity_benchmark
//!
//! Environment variables:
//!   WARMUP_ITERATIONS - Number of warmup runs (default: 3)
//!   BENCHMARK_ITERATIONS - Number of timed runs (default: 10)

mod harness;

use harness::{print_group_header, print_summary_table, Harness};
use smallvec::SmallVec;
use std::hint::black_box;
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
        1 => SqlValue::Double(i as f64 * 2.5),
        2 => SqlValue::Varchar(arcstr::ArcStr::from(format!("value_{}", i))),
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

/// Benchmark row creation with different capacities
fn bench_row_creation(harness: &Harness) -> Vec<harness::BenchStats> {
    print_group_header("Row Creation");

    let mut results = Vec::new();

    for &col_count in &COLUMN_COUNTS {
        // Capacity 4
        let name = format!("capacity_4/{}_cols", col_count);
        let stats = harness.run(&name, || create_row4(col_count));
        stats.print_compact();
        results.push(stats);

        // Capacity 6
        let name = format!("capacity_6/{}_cols", col_count);
        let stats = harness.run(&name, || create_row6(col_count));
        stats.print_compact();
        results.push(stats);

        // Capacity 8 (current default)
        let name = format!("capacity_8/{}_cols", col_count);
        let stats = harness.run(&name, || create_row8(col_count));
        stats.print_compact();
        results.push(stats);

        // Capacity 10
        let name = format!("capacity_10/{}_cols", col_count);
        let stats = harness.run(&name, || create_row10(col_count));
        stats.print_compact();
        results.push(stats);

        // Capacity 12
        let name = format!("capacity_12/{}_cols", col_count);
        let stats = harness.run(&name, || create_row12(col_count));
        stats.print_compact();
        results.push(stats);

        eprintln!(); // Blank line between column counts
    }

    results
}

/// Benchmark batch row creation (simulates query result processing)
fn bench_batch_creation(harness: &Harness) -> Vec<harness::BenchStats> {
    print_group_header("Batch Creation (1000 rows)");
    const BATCH_SIZE: usize = 1000;

    let mut results = Vec::new();

    for &col_count in &[4usize, 8, 12] {
        // Capacity 4
        let name = format!("capacity_4/{}_cols", col_count);
        let stats = harness.run(&name, || {
            let batch: Vec<Row4> = (0..BATCH_SIZE).map(|_| create_row4(col_count)).collect();
            black_box(batch)
        });
        stats.print_compact();
        results.push(stats);

        // Capacity 8 (current default)
        let name = format!("capacity_8/{}_cols", col_count);
        let stats = harness.run(&name, || {
            let batch: Vec<Row8> = (0..BATCH_SIZE).map(|_| create_row8(col_count)).collect();
            black_box(batch)
        });
        stats.print_compact();
        results.push(stats);

        // Capacity 12
        let name = format!("capacity_12/{}_cols", col_count);
        let stats = harness.run(&name, || {
            let batch: Vec<Row12> = (0..BATCH_SIZE).map(|_| create_row12(col_count)).collect();
            black_box(batch)
        });
        stats.print_compact();
        results.push(stats);

        eprintln!(); // Blank line between column counts
    }

    results
}

/// Benchmark row cloning (important for query processing)
fn bench_row_clone(harness: &Harness) -> Vec<harness::BenchStats> {
    print_group_header("Row Clone");

    let mut results = Vec::new();

    for &col_count in &COLUMN_COUNTS {
        // Pre-create rows to clone
        let row4 = create_row4(col_count);
        let row8 = create_row8(col_count);
        let row12 = create_row12(col_count);

        let name = format!("capacity_4/{}_cols", col_count);
        let stats = harness.run(&name, || row4.clone());
        stats.print_compact();
        results.push(stats);

        let name = format!("capacity_8/{}_cols", col_count);
        let stats = harness.run(&name, || row8.clone());
        stats.print_compact();
        results.push(stats);

        let name = format!("capacity_12/{}_cols", col_count);
        let stats = harness.run(&name, || row12.clone());
        stats.print_compact();
        results.push(stats);

        eprintln!(); // Blank line between column counts
    }

    results
}

/// Benchmark row access patterns (simulates filtering)
fn bench_row_access(harness: &Harness) -> Vec<harness::BenchStats> {
    print_group_header("Row Access");

    let mut results = Vec::new();

    for &col_count in &[4usize, 8, 16] {
        // Pre-create rows
        let row4 = create_row4(col_count);
        let row8 = create_row8(col_count);
        let row12 = create_row12(col_count);

        let name = format!("capacity_4/{}_cols", col_count);
        let stats = harness.run(&name, || {
            // Simulate accessing multiple columns (like in a filter)
            let mut sum = 0i64;
            for i in 0..row4.len() {
                if let SqlValue::Integer(v) = &row4[i] {
                    sum += v;
                }
            }
            black_box(sum)
        });
        stats.print_compact();
        results.push(stats);

        let name = format!("capacity_8/{}_cols", col_count);
        let stats = harness.run(&name, || {
            let mut sum = 0i64;
            for i in 0..row8.len() {
                if let SqlValue::Integer(v) = &row8[i] {
                    sum += v;
                }
            }
            black_box(sum)
        });
        stats.print_compact();
        results.push(stats);

        let name = format!("capacity_12/{}_cols", col_count);
        let stats = harness.run(&name, || {
            let mut sum = 0i64;
            for i in 0..row12.len() {
                if let SqlValue::Integer(v) = &row12[i] {
                    sum += v;
                }
            }
            black_box(sum)
        });
        stats.print_compact();
        results.push(stats);

        eprintln!(); // Blank line between column counts
    }

    results
}

fn main() {
    // Print size info before running benchmarks
    print_size_info();

    eprintln!("=== Row Capacity Benchmarks ===\n");

    let harness = Harness::new();

    let mut all_results = Vec::new();
    all_results.extend(bench_row_creation(&harness));
    all_results.extend(bench_batch_creation(&harness));
    all_results.extend(bench_row_clone(&harness));
    all_results.extend(bench_row_access(&harness));

    print_summary_table(&all_results);

    eprintln!("\n=== Benchmark Complete ===\n");
}
