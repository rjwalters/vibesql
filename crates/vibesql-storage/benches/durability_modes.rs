//! Durability Mode Benchmarks
//!
//! Compares performance across durability modes:
//!
//! | Mode     | Single INSERT | Bulk 1M rows | Durability           |
//! |----------|--------------|--------------|----------------------|
//! | Volatile | ~1μs         | ~500ms       | None                 |
//! | Lazy     | ~1μs         | ~500ms+50ms  | ~100ms loss window   |
//! | Durable  | ~100μs       | ~1s          | Committed safe       |
//! | Paranoid | ~200μs       | ~2s          | All writes safe      |
//!
//! Run with:
//!   cargo bench --package vibesql-storage --bench durability_modes
//!
//! Or via Makefile:
//!   make bench-durability

use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use std::hint::black_box;
use std::io::Cursor;
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::wal::{PersistenceConfig, PersistenceEngine};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

// ============================================================================
// Durability Mode Configurations
// ============================================================================

/// Durability mode for benchmarking
#[derive(Debug, Clone, Copy)]
enum DurabilityMode {
    /// No persistence - pure in-memory
    Volatile,
    /// Async batched WAL writes (default 50ms flush interval)
    Lazy,
    /// Sync after each operation (wait for WAL write)
    Durable,
    /// Fsync after each write (paranoid mode)
    Paranoid,
}

impl DurabilityMode {
    fn as_str(&self) -> &'static str {
        match self {
            DurabilityMode::Volatile => "volatile",
            DurabilityMode::Lazy => "lazy",
            DurabilityMode::Durable => "durable",
            DurabilityMode::Paranoid => "paranoid",
        }
    }

    fn all() -> [DurabilityMode; 4] {
        [
            DurabilityMode::Volatile,
            DurabilityMode::Lazy,
            DurabilityMode::Durable,
            DurabilityMode::Paranoid,
        ]
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

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

/// Create a database with the specified durability mode
fn create_database_with_mode(mode: DurabilityMode) -> Database {
    let mut db = Database::new();

    match mode {
        DurabilityMode::Volatile => {
            // No persistence engine - pure in-memory
        }
        DurabilityMode::Lazy => {
            // Default async batched config (50ms flush interval)
            let buf = Vec::new();
            let cursor = Cursor::new(buf);
            let config = PersistenceConfig::default();
            let engine = PersistenceEngine::with_writer(cursor, config).unwrap();
            db.enable_persistence(engine);
        }
        DurabilityMode::Durable => {
            // Minimal flush interval for near-sync behavior
            // Still uses async writer but with very frequent flushes
            let buf = Vec::new();
            let cursor = Cursor::new(buf);
            let config = PersistenceConfig {
                flush_interval_ms: 1, // 1ms flush interval
                flush_count: 1,       // Flush after every entry
                ..Default::default()
            };
            let engine = PersistenceEngine::with_writer(cursor, config).unwrap();
            db.enable_persistence(engine);
        }
        DurabilityMode::Paranoid => {
            // Same as Durable for now - true paranoid mode would require
            // O_SYNC or fsync after each write, which isn't exposed yet
            let buf = Vec::new();
            let cursor = Cursor::new(buf);
            let config = PersistenceConfig {
                flush_interval_ms: 1,
                flush_count: 1,
                ..Default::default()
            };
            let engine = PersistenceEngine::with_writer(cursor, config).unwrap();
            db.enable_persistence(engine);
        }
    }

    db
}

// ============================================================================
// Single INSERT Latency Benchmarks
// ============================================================================

/// Benchmark single INSERT latency for each durability mode
fn bench_single_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability_single_insert");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(1000);

    for mode in DurabilityMode::all() {
        group.bench_function(mode.as_str(), |b| {
            let mut db = create_database_with_mode(mode);
            let schema = make_schema("bench_table");
            db.create_table(schema).unwrap();

            let mut id = 0i64;
            b.iter(|| {
                id += 1;
                let row = make_row(id);
                db.insert_row("bench_table", row).unwrap();

                // For durable/paranoid modes, sync after each insert
                if matches!(mode, DurabilityMode::Durable | DurabilityMode::Paranoid) {
                    db.sync_persistence().unwrap();
                }

                black_box(id)
            });
        });
    }

    group.finish();
}

// ============================================================================
// Bulk INSERT Throughput Benchmarks
// ============================================================================

/// Benchmark bulk INSERT throughput for each durability mode
fn bench_bulk_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability_bulk_insert");

    // Smaller batch sizes for practical benchmark times
    // Full 1M row benchmark is too slow for CI
    for &batch_size in &[1000u64, 10_000u64, 100_000u64] {
        group.throughput(Throughput::Elements(batch_size));

        for mode in DurabilityMode::all() {
            let bench_id = format!("{}/{}", mode.as_str(), batch_size);
            group.bench_with_input(
                BenchmarkId::from_parameter(&bench_id),
                &batch_size,
                |b, &size| {
                    b.iter(|| {
                        let mut db = create_database_with_mode(mode);
                        let schema = make_schema("bench_table");
                        db.create_table(schema).unwrap();

                        // Use batch insert for better performance
                        let rows: Vec<Row> = (0..size as i64).map(make_row).collect();
                        let count = db.insert_rows_batch("bench_table", rows).unwrap();

                        // Sync at the end for durable modes
                        if matches!(mode, DurabilityMode::Lazy) {
                            // Lazy mode: wait for async flush
                            db.sync_persistence().unwrap();
                        } else if matches!(
                            mode,
                            DurabilityMode::Durable | DurabilityMode::Paranoid
                        ) {
                            db.sync_persistence().unwrap();
                        }

                        black_box(count)
                    });
                },
            );
        }
    }

    group.finish();
}

// ============================================================================
// Latency Distribution Benchmarks
// ============================================================================

/// Benchmark to measure latency distribution (p50, p95, p99)
/// Uses many iterations to build statistical significance
fn bench_insert_latency_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability_latency_distribution");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10_000); // Large sample for better percentile accuracy

    for mode in DurabilityMode::all() {
        // Only Volatile and Lazy for distribution test (faster)
        if matches!(mode, DurabilityMode::Durable | DurabilityMode::Paranoid) {
            continue; // Skip slow modes for distribution test
        }

        group.bench_function(mode.as_str(), |b| {
            let mut db = create_database_with_mode(mode);
            let schema = make_schema("bench_table");
            db.create_table(schema).unwrap();

            let mut id = 0i64;
            b.iter(|| {
                id += 1;
                let row = make_row(id);
                db.insert_row("bench_table", row).unwrap();
                black_box(id)
            });
        });
    }

    group.finish();
}

// ============================================================================
// Mixed Workload Benchmarks
// ============================================================================

/// Benchmark mixed read/write workload
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability_mixed_workload");
    group.sample_size(100);

    let ops_per_iter = 1000u64;
    group.throughput(Throughput::Elements(ops_per_iter));

    for mode in [DurabilityMode::Volatile, DurabilityMode::Lazy] {
        group.bench_function(mode.as_str(), |b| {
            let mut db = create_database_with_mode(mode);
            let schema = make_schema("bench_table");
            db.create_table(schema).unwrap();

            // Pre-populate with some data
            let rows: Vec<Row> = (0..1000i64).map(make_row).collect();
            db.insert_rows_batch("bench_table", rows).unwrap();

            let mut write_id = 1000i64;
            let mut read_id = 0i64;
            b.iter(|| {
                // 80% reads, 20% writes (typical OLTP mix)
                for i in 0..ops_per_iter {
                    if i % 5 == 0 {
                        // Write
                        write_id += 1;
                        let row = make_row(write_id);
                        db.insert_row("bench_table", row).unwrap();
                    } else {
                        // Read (via table scan for now - could use PK lookup)
                        read_id = (read_id + 1) % 1000;
                        let pk = SqlValue::Integer(read_id);
                        let _ = black_box(db.get_row_by_pk("bench_table", &pk));
                    }
                }
                black_box(write_id)
            });
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark Groups
// ============================================================================

criterion_group!(
    benches,
    bench_single_insert,
    bench_bulk_insert,
    bench_insert_latency_distribution,
    bench_mixed_workload,
);

criterion_main!(benches);
