//! Benchmarks for Skip-Scan optimization
//!
//! These benchmarks measure skip-scan performance across different:
//! - Prefix cardinalities (5, 10, 25, 50, 100, 500, 1000)
//! - Filter selectivities (0.001, 0.01, 0.1, 0.5)
//! - Table sizes (10K, 100K rows)
//!
//! Run with:
//!   cargo bench --package vibesql-storage --bench skip_scan_benchmark
//!
//! Or via Makefile:
//!   make bench-skip-scan
//!
//! # Cost Model Validation
//!
//! The benchmarks also validate the cost model from `statistics/cost.rs`:
//! - `estimate_skip_scan_cost()` - Cost estimation
//! - `should_use_skip_scan()` - Decision logic
//!
//! After running, compare estimated costs with actual execution times
//! to validate cost model accuracy.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use instant::SystemTime;
use std::collections::BTreeMap;
use std::hint::black_box;
use vibesql_storage::statistics::{ColumnStatistics, CostEstimator, TableStatistics};
use vibesql_storage::IndexData;
use vibesql_types::SqlValue;

// ============================================================================
// Test Data Generation
// ============================================================================

/// Create composite index data with controlled prefix cardinality
///
/// Creates an index on (prefix_column, filter_column) where:
/// - prefix_column cycles through 0..prefix_cardinality
/// - filter_column contains values 0..row_count
///
/// This simulates an index like (region, date) where region has low cardinality
/// and date has high cardinality.
fn create_composite_index_data(row_count: usize, prefix_cardinality: usize) -> IndexData {
    let mut data = BTreeMap::new();

    for i in 0..row_count {
        let prefix_value = (i % prefix_cardinality) as i64;
        let filter_value = i as i64;

        let key = vec![SqlValue::Integer(prefix_value), SqlValue::Integer(filter_value)];
        data.insert(key, vec![i]);
    }

    IndexData::InMemory {
        data,
        pending_deletions: Vec::new(),
    }
}

/// Create table statistics for cost estimation
fn create_table_stats(row_count: usize) -> TableStatistics {
    TableStatistics {
        row_count,
        columns: std::collections::HashMap::new(),
        last_updated: SystemTime::now(),
        is_stale: false,
        sample_metadata: None,
        avg_row_bytes: Some(32.0), // Estimate 32 bytes per row
    }
}

/// Create column statistics for the prefix column
fn create_prefix_column_stats(n_distinct: usize) -> ColumnStatistics {
    ColumnStatistics {
        n_distinct,
        null_count: 0,
        min_value: Some(SqlValue::Integer(0)),
        max_value: Some(SqlValue::Integer((n_distinct - 1) as i64)),
        most_common_values: vec![],
        histogram: None,
    }
}

// ============================================================================
// Benchmark: Skip-Scan vs Table Scan at Different Prefix Cardinalities
// ============================================================================

/// Benchmark skip-scan performance vs table scan across prefix cardinalities
///
/// This is the primary benchmark for understanding when skip-scan is beneficial.
/// Lower prefix cardinality = fewer seeks = skip-scan more likely to win.
fn bench_skip_scan_vs_table_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("skip_scan_vs_table_scan");
    let row_count = 100_000;
    let filter_value = SqlValue::Integer(500); // Filter matches ~1/cardinality rows per prefix

    for cardinality in [5, 10, 25, 50, 100, 500, 1000] {
        let index = create_composite_index_data(row_count, cardinality);

        group.throughput(Throughput::Elements(row_count as u64));

        // Benchmark skip-scan (filter on second column)
        group.bench_with_input(
            BenchmarkId::new("skip_scan", cardinality),
            &cardinality,
            |b, _| {
                b.iter(|| {
                    let results = index.skip_scan_equality(1, &filter_value);
                    black_box(results.len())
                });
            },
        );

        // Benchmark simulated table scan (iterate all entries)
        group.bench_with_input(
            BenchmarkId::new("table_scan_sim", cardinality),
            &cardinality,
            |b, _| {
                b.iter(|| {
                    // Simulate table scan: check every entry
                    let mut count = 0;
                    if let IndexData::InMemory { data, .. } = &index {
                        for (_key, row_indices) in data.iter() {
                            // In a real table scan, we'd check the predicate
                            // Here we just count to simulate the I/O
                            count += row_indices.len();
                        }
                    }
                    black_box(count)
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark: Skip-Scan at Different Filter Selectivities
// ============================================================================

/// Benchmark skip-scan performance at different filter selectivities
///
/// Lower selectivity (fewer matching rows) = skip-scan more efficient
/// because it scans fewer entries per prefix group.
fn bench_skip_scan_selectivity(c: &mut Criterion) {
    let mut group = c.benchmark_group("skip_scan_selectivity");
    let row_count = 100_000;
    let prefix_cardinality = 25; // Fixed moderate cardinality

    // Create index with controlled filter value distribution
    // Filter values 0..100 repeated, so selectivity = 1/100 for any single value
    let mut data = BTreeMap::new();
    for i in 0..row_count {
        let prefix_value = (i % prefix_cardinality) as i64;
        let filter_value = (i % 100) as i64; // Values 0-99

        let key = vec![SqlValue::Integer(prefix_value), SqlValue::Integer(filter_value)];
        // Multiple rows can have same key - use row index as value
        data.entry(key).or_insert_with(Vec::new).push(i);
    }
    let index = IndexData::InMemory {
        data,
        pending_deletions: Vec::new(),
    };

    group.throughput(Throughput::Elements(row_count as u64));

    // Different filter values to achieve different effective selectivities
    // Value 0 appears in ~1000 rows (1% selectivity)
    // Value 50 appears in ~1000 rows (1% selectivity)
    for (label, filter_val) in [
        ("selective_0.01", 0i64),
        ("moderate_0.01", 50),
        ("range_10pct", -1), // Special: use range scan for 10% selectivity
    ] {
        if filter_val >= 0 {
            let filter_value = SqlValue::Integer(filter_val);
            group.bench_with_input(BenchmarkId::new("equality", label), &label, |b, _| {
                b.iter(|| {
                    let results = index.skip_scan_equality(1, &filter_value);
                    black_box(results.len())
                });
            });
        } else {
            // Range scan: filter_value BETWEEN 0 AND 9 (10% of values)
            group.bench_with_input(BenchmarkId::new("range", label), &label, |b, _| {
                b.iter(|| {
                    let results = index.skip_scan_range(
                        1,
                        Some(&SqlValue::Integer(0)),
                        true,
                        Some(&SqlValue::Integer(9)),
                        true,
                    );
                    black_box(results.len())
                });
            });
        }
    }

    group.finish();
}

// ============================================================================
// Benchmark: Skip-Scan at Different Table Sizes
// ============================================================================

/// Benchmark skip-scan performance at different table sizes
///
/// Skip-scan cost scales with O(cardinality * log(n) + k) where:
/// - cardinality = prefix distinct values
/// - n = total rows
/// - k = matching rows
fn bench_skip_scan_table_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("skip_scan_table_size");
    let prefix_cardinality = 25;
    let filter_value = SqlValue::Integer(500);

    for row_count in [10_000, 50_000, 100_000] {
        let index = create_composite_index_data(row_count, prefix_cardinality);

        group.throughput(Throughput::Elements(row_count as u64));

        group.bench_with_input(
            BenchmarkId::new("skip_scan", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let results = index.skip_scan_equality(1, &filter_value);
                    black_box(results.len())
                });
            },
        );

        // Compare with get_distinct_first_column_values (prefix enumeration cost)
        group.bench_with_input(
            BenchmarkId::new("enumerate_prefixes", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let prefixes = index.get_distinct_first_column_values();
                    black_box(prefixes.len())
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark: Break-Even Analysis
// ============================================================================

/// Find the break-even point where skip-scan becomes more expensive than table scan
///
/// Sweeps through cardinalities to find where skip_scan_cost > table_scan_cost
fn bench_breakeven_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("skip_scan_breakeven");
    let row_count = 50_000;
    let filter_value = SqlValue::Integer(500);
    let cost_estimator = CostEstimator::default();
    let filter_selectivity = 0.01; // 1% selectivity

    println!("\n=== Skip-Scan Break-Even Analysis ===");
    println!("Row count: {}", row_count);
    println!("Filter selectivity: {:.1}%\n", filter_selectivity * 100.0);
    println!(
        "{:>12} {:>15} {:>15} {:>10}",
        "Cardinality", "Skip-Scan Cost", "Table Scan Cost", "Ratio"
    );
    println!("{}", "-".repeat(55));

    let table_stats = create_table_stats(row_count);
    let table_scan_cost = cost_estimator.estimate_table_scan(&table_stats);

    for cardinality in [1, 2, 5, 10, 25, 50, 100, 200, 500, 1000, 2000] {
        let index = create_composite_index_data(row_count, cardinality);
        let prefix_stats = create_prefix_column_stats(cardinality);

        let skip_scan_cost =
            cost_estimator.estimate_skip_scan_cost(&table_stats, &prefix_stats, filter_selectivity);

        let ratio = skip_scan_cost / table_scan_cost;
        let marker = if ratio < 1.0 { "<- skip-scan wins" } else { "" };

        println!(
            "{:>12} {:>15.2} {:>15.2} {:>10.2} {}",
            cardinality, skip_scan_cost, table_scan_cost, ratio, marker
        );

        // Benchmark actual execution time
        group.bench_with_input(
            BenchmarkId::new("skip_scan", cardinality),
            &cardinality,
            |b, _| {
                b.iter(|| {
                    let results = index.skip_scan_equality(1, &filter_value);
                    black_box(results.len())
                });
            },
        );
    }

    println!();
    group.finish();
}

// ============================================================================
// Benchmark: Cost Model Accuracy
// ============================================================================

/// Validate cost model by comparing estimated vs actual costs
///
/// For each (cardinality, selectivity) combination:
/// 1. Compute estimated cost
/// 2. Measure actual execution time
/// 3. Print comparison
fn bench_cost_model_accuracy(c: &mut Criterion) {
    let mut group = c.benchmark_group("cost_model_accuracy");
    let row_count = 50_000;
    let cost_estimator = CostEstimator::default();

    // Print header for cost model analysis
    println!("\n=== Cost Model Accuracy Analysis ===");
    println!(
        "{:>12} {:>12} {:>15} {:>12}",
        "Cardinality", "Selectivity", "Est Skip-Scan", "Should Use?"
    );
    println!("{}", "-".repeat(55));

    let table_stats = create_table_stats(row_count);

    for cardinality in [10, 50, 200] {
        let prefix_stats = create_prefix_column_stats(cardinality);

        for selectivity in [0.001, 0.01, 0.1] {
            let skip_scan_cost =
                cost_estimator.estimate_skip_scan_cost(&table_stats, &prefix_stats, selectivity);
            let should_use =
                cost_estimator.should_use_skip_scan(&table_stats, &prefix_stats, selectivity);

            println!(
                "{:>12} {:>12.3} {:>15.2} {:>12}",
                cardinality,
                selectivity,
                skip_scan_cost,
                if should_use { "YES" } else { "NO" }
            );

            // Benchmark the actual operation
            let index = create_composite_index_data(row_count, cardinality);
            // For selectivity simulation, use filter value that matches approximately right fraction
            let filter_value = SqlValue::Integer(0);

            let label = format!("card{}_sel{}", cardinality, selectivity);
            group.bench_with_input(BenchmarkId::new("skip_scan", &label), &label, |b, _| {
                b.iter(|| {
                    let results = index.skip_scan_equality(1, &filter_value);
                    black_box(results.len())
                });
            });
        }
    }

    println!();
    group.finish();
}

// ============================================================================
// Benchmark: Range Skip-Scan
// ============================================================================

/// Benchmark skip-scan with range predicates (BETWEEN, >, <, etc.)
fn bench_skip_scan_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("skip_scan_range");
    let row_count = 50_000;
    let prefix_cardinality = 25;

    let index = create_composite_index_data(row_count, prefix_cardinality);

    group.throughput(Throughput::Elements(row_count as u64));

    // Equality filter (baseline)
    let filter_value = SqlValue::Integer(500);
    group.bench_function("equality", |b| {
        b.iter(|| {
            let results = index.skip_scan_equality(1, &filter_value);
            black_box(results.len())
        });
    });

    // Range filter: value > 40000 (top 20%)
    group.bench_function("range_gt", |b| {
        b.iter(|| {
            let results = index.skip_scan_range(
                1,
                Some(&SqlValue::Integer(40000)),
                false, // exclusive
                None,
                false,
            );
            black_box(results.len())
        });
    });

    // Range filter: 20000 <= value <= 30000 (20% in middle)
    group.bench_function("range_between", |b| {
        b.iter(|| {
            let results = index.skip_scan_range(
                1,
                Some(&SqlValue::Integer(20000)),
                true,
                Some(&SqlValue::Integer(30000)),
                true,
            );
            black_box(results.len())
        });
    });

    // Range filter: value < 10000 (bottom 20%)
    group.bench_function("range_lt", |b| {
        b.iter(|| {
            let results = index.skip_scan_range(1, None, false, Some(&SqlValue::Integer(10000)), false);
            black_box(results.len())
        });
    });

    group.finish();
}

// ============================================================================
// Criterion Groups
// ============================================================================

criterion_group!(
    benches,
    bench_skip_scan_vs_table_scan,
    bench_skip_scan_selectivity,
    bench_skip_scan_table_size,
    bench_breakeven_analysis,
    bench_cost_model_accuracy,
    bench_skip_scan_range,
);

criterion_main!(benches);
