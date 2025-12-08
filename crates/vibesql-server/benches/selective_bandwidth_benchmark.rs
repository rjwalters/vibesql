//! Bandwidth benchmarks for selective column updates
//!
//! This benchmark measures the bandwidth savings from using selective column updates
//! (0xF7) versus full row updates (0xF2) for subscription notifications.
//!
//! The selective column update feature is designed to reduce bandwidth for wide tables
//! where only a few columns change. This benchmark quantifies the actual savings.
//!
//! Run with: cargo bench --bench selective_bandwidth_benchmark

use bytes::BytesMut;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use vibesql_server::protocol::messages::{BackendMessage, PartialRowUpdate, SubscriptionUpdateType};
use vibesql_server::subscription::{
    create_partial_row_update, SelectiveColumnConfig,
};

/// Create a test row with N columns, each containing a string value
fn create_row(num_columns: usize, row_id: usize) -> Vec<Option<Vec<u8>>> {
    (0..num_columns)
        .map(|col| {
            // Create realistic column values - mix of short and medium length
            let value = if col == 0 {
                // Primary key - short numeric string
                format!("{}", row_id)
            } else if col % 3 == 0 {
                // Every 3rd column is a longer text field
                format!("field_{}_row_{}_with_some_extra_text_to_simulate_realistic_data", col, row_id)
            } else {
                // Other columns are medium length
                format!("value_{}_{}", col, row_id)
            };
            Some(value.into_bytes())
        })
        .collect()
}

/// Create a modified row where specific columns have changed
fn create_modified_row(
    original: &[Option<Vec<u8>>],
    columns_to_change: &[usize],
    modification_suffix: &str,
) -> Vec<Option<Vec<u8>>> {
    original
        .iter()
        .enumerate()
        .map(|(idx, val)| {
            if columns_to_change.contains(&idx) {
                val.as_ref().map(|v| {
                    let mut new_val = v.clone();
                    new_val.extend_from_slice(modification_suffix.as_bytes());
                    new_val
                })
            } else {
                val.clone()
            }
        })
        .collect()
}

/// Measure the encoded size of a full row update message
fn measure_full_update_size(rows: &[Vec<Option<Vec<u8>>>], subscription_id: [u8; 16]) -> usize {
    let mut buf = BytesMut::new();
    let msg = BackendMessage::SubscriptionData {
        subscription_id,
        update_type: SubscriptionUpdateType::DeltaUpdate,
        rows: rows.to_vec(),
    };
    msg.encode(&mut buf);
    buf.len()
}

/// Measure the encoded size of a selective column update message
fn measure_selective_update_size(
    partial_rows: &[PartialRowUpdate],
    subscription_id: [u8; 16],
) -> usize {
    let mut buf = BytesMut::new();
    let msg = BackendMessage::SubscriptionPartialData {
        subscription_id,
        rows: partial_rows.to_vec(),
    };
    msg.encode(&mut buf);
    buf.len()
}

/// Configuration for a bandwidth test scenario
struct BandwidthScenario {
    name: &'static str,
    num_columns: usize,
    columns_to_change: Vec<usize>,
}

/// Results from a bandwidth comparison
#[derive(Debug)]
#[allow(dead_code)]
struct BandwidthResult {
    scenario_name: String,
    num_columns: usize,
    columns_changed: usize,
    full_update_bytes: usize,
    selective_update_bytes: usize,
    savings_bytes: i64,
    savings_percent: f64,
    selective_used: bool,
}

/// Run bandwidth comparison for a given scenario
fn compare_bandwidth(scenario: &BandwidthScenario, config: &SelectiveColumnConfig) -> BandwidthResult {
    let subscription_id = [0u8; 16];
    let pk_columns = vec![0usize]; // Column 0 is primary key

    // Create original and modified rows
    let original_row = create_row(scenario.num_columns, 1);
    let modified_row = create_modified_row(&original_row, &scenario.columns_to_change, "_modified");

    // Measure full update size
    let full_update_bytes = measure_full_update_size(std::slice::from_ref(&modified_row), subscription_id);

    // Try to create selective update
    let (selective_update_bytes, selective_used) =
        if let Some(partial) = create_partial_row_update(&original_row, &modified_row, &pk_columns, config) {
            (measure_selective_update_size(&[partial], subscription_id), true)
        } else {
            (full_update_bytes, false)
        };

    let savings_bytes = full_update_bytes as i64 - selective_update_bytes as i64;
    let savings_percent = if full_update_bytes > 0 {
        (savings_bytes as f64 / full_update_bytes as f64) * 100.0
    } else {
        0.0
    };

    BandwidthResult {
        scenario_name: scenario.name.to_string(),
        num_columns: scenario.num_columns,
        columns_changed: scenario.columns_to_change.len(),
        full_update_bytes,
        selective_update_bytes,
        savings_bytes,
        savings_percent,
        selective_used,
    }
}

/// Print a formatted bandwidth comparison report
fn print_bandwidth_report(results: &[BandwidthResult]) {
    println!("\n╔════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                       SELECTIVE COLUMN UPDATE BANDWIDTH BENCHMARK                                   ║");
    println!("╠════════════════════════════════════════════════════════════════════════════════════════════════════╣");
    println!("║ {:30} │ {:5} │ {:7} │ {:10} │ {:10} │ {:8} │ {:8} ║",
             "Scenario", "Cols", "Changed", "Full (B)", "Select (B)", "Savings", "Used?");
    println!("╠════════════════════════════════════════════════════════════════════════════════════════════════════╣");

    for result in results {
        println!("║ {:30} │ {:5} │ {:7} │ {:10} │ {:10} │ {:7.1}% │ {:8} ║",
                 result.scenario_name,
                 result.num_columns,
                 result.columns_changed,
                 result.full_update_bytes,
                 result.selective_update_bytes,
                 result.savings_percent,
                 if result.selective_used { "Yes" } else { "No (fallback)" });
    }

    println!("╚════════════════════════════════════════════════════════════════════════════════════════════════════╝");

    // Summary statistics
    let total_full: usize = results.iter().map(|r| r.full_update_bytes).sum();
    let total_selective: usize = results.iter().map(|r| r.selective_update_bytes).sum();
    let selective_scenarios: usize = results.iter().filter(|r| r.selective_used).count();

    println!("\nSummary:");
    println!("  Total scenarios: {}", results.len());
    println!("  Scenarios using selective updates: {}", selective_scenarios);
    println!("  Total bandwidth (full updates): {} bytes", total_full);
    println!("  Total bandwidth (with selective): {} bytes", total_selective);
    if total_full > 0 {
        let overall_savings = ((total_full - total_selective) as f64 / total_full as f64) * 100.0;
        println!("  Overall bandwidth savings: {:.1}%", overall_savings);
    }
}

/// Main benchmark that runs all scenarios and prints results
fn bandwidth_benchmark(c: &mut Criterion) {
    let config = SelectiveColumnConfig::default();

    // Define test scenarios
    let scenarios = vec![
        // 10-column table scenarios
        BandwidthScenario {
            name: "10 cols, update 1 col (10%)",
            num_columns: 10,
            columns_to_change: vec![1],
        },
        BandwidthScenario {
            name: "10 cols, update 2 cols (20%)",
            num_columns: 10,
            columns_to_change: vec![1, 2],
        },
        BandwidthScenario {
            name: "10 cols, update 5 cols (50%)",
            num_columns: 10,
            columns_to_change: vec![1, 2, 3, 4, 5],
        },
        BandwidthScenario {
            name: "10 cols, update 7 cols (70%)",
            num_columns: 10,
            columns_to_change: vec![1, 2, 3, 4, 5, 6, 7],
        },
        BandwidthScenario {
            name: "10 cols, update all (100%)",
            num_columns: 10,
            columns_to_change: (1..10).collect(),
        },

        // 20-column table scenarios
        BandwidthScenario {
            name: "20 cols, update 1 col (5%)",
            num_columns: 20,
            columns_to_change: vec![1],
        },
        BandwidthScenario {
            name: "20 cols, update 5 cols (25%)",
            num_columns: 20,
            columns_to_change: vec![1, 2, 3, 4, 5],
        },
        BandwidthScenario {
            name: "20 cols, update 10 cols (50%)",
            num_columns: 20,
            columns_to_change: (1..11).collect(),
        },
        BandwidthScenario {
            name: "20 cols, update 15 cols (75%)",
            num_columns: 20,
            columns_to_change: (1..16).collect(),
        },

        // 50-column table scenarios (wide table)
        BandwidthScenario {
            name: "50 cols, update 1 col (2%)",
            num_columns: 50,
            columns_to_change: vec![1],
        },
        BandwidthScenario {
            name: "50 cols, update 5 cols (10%)",
            num_columns: 50,
            columns_to_change: vec![1, 2, 3, 4, 5],
        },
        BandwidthScenario {
            name: "50 cols, update 12 cols (24%)",
            num_columns: 50,
            columns_to_change: (1..13).collect(),
        },
        BandwidthScenario {
            name: "50 cols, update 25 cols (50%)",
            num_columns: 50,
            columns_to_change: (1..26).collect(),
        },
        BandwidthScenario {
            name: "50 cols, update 37 cols (74%)",
            num_columns: 50,
            columns_to_change: (1..38).collect(),
        },
    ];

    // Run all scenarios and collect results
    let results: Vec<BandwidthResult> = scenarios
        .iter()
        .map(|s| compare_bandwidth(s, &config))
        .collect();

    // Print the report
    print_bandwidth_report(&results);

    // Now run actual benchmarks for encoding performance
    let mut group = c.benchmark_group("subscription_update_encoding");

    for num_cols in [10, 20, 50] {
        // Benchmark full row encoding
        let row = create_row(num_cols, 1);
        let subscription_id = [0u8; 16];

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("full_row_encode", num_cols),
            &row,
            |b, row| {
                b.iter(|| {
                    let mut buf = BytesMut::new();
                    let msg = BackendMessage::SubscriptionData {
                        subscription_id,
                        update_type: SubscriptionUpdateType::DeltaUpdate,
                        rows: vec![black_box(row.clone())],
                    };
                    msg.encode(&mut buf);
                    buf
                });
            },
        );

        // Benchmark selective update encoding (updating 1 column)
        let modified_row = create_modified_row(&row, &[1], "_mod");
        let pk_columns = vec![0usize];

        if let Some(partial) = create_partial_row_update(&row, &modified_row, &pk_columns, &config) {
            group.bench_with_input(
                BenchmarkId::new("selective_1col_encode", num_cols),
                &partial,
                |b, partial| {
                    b.iter(|| {
                        let mut buf = BytesMut::new();
                        let msg = BackendMessage::SubscriptionPartialData {
                            subscription_id,
                            rows: vec![black_box(partial.clone())],
                        };
                        msg.encode(&mut buf);
                        buf
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark the column diff computation
fn column_diff_benchmark(c: &mut Criterion) {
    let config = SelectiveColumnConfig::default();
    let pk_columns = vec![0usize];

    let mut group = c.benchmark_group("column_diff_computation");

    for num_cols in [10, 20, 50, 100] {
        let original = create_row(num_cols, 1);

        // Benchmark diff with 1 column changed
        let modified_1 = create_modified_row(&original, &[1], "_mod");
        group.bench_with_input(
            BenchmarkId::new("diff_1_col", num_cols),
            &(&original, &modified_1),
            |b, (orig, modif)| {
                b.iter(|| {
                    create_partial_row_update(
                        black_box(orig),
                        black_box(modif),
                        &pk_columns,
                        &config,
                    )
                });
            },
        );

        // Benchmark diff with 25% columns changed
        let quarter_cols: Vec<usize> = (1..=num_cols/4).collect();
        let modified_quarter = create_modified_row(&original, &quarter_cols, "_mod");
        group.bench_with_input(
            BenchmarkId::new("diff_25pct_cols", num_cols),
            &(&original, &modified_quarter),
            |b, (orig, modif)| {
                b.iter(|| {
                    create_partial_row_update(
                        black_box(orig),
                        black_box(modif),
                        &pk_columns,
                        &config,
                    )
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bandwidth_benchmark, column_diff_benchmark);
criterion_main!(benches);
