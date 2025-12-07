//! Change event broadcasting benchmarks
//!
//! Benchmarks to verify change event overhead meets performance targets:
//! - Single event send: < 100ns
//! - INSERT overhead: < 1µs additional
//! - Fanout to 100 receivers: < 10µs
//!
//! Run with:
//!   cargo bench --package vibesql-storage --bench change_event_bench
//!
//! Part of Phase 1.1 of Real-Time Reactive Query Subscriptions (#3421)
//! Issue: #3447

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::change_events::{self, ChangeEvent, ChangeEventReceiver};
use vibesql_storage::{Database, Row};
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

// ============================================================================
// Benchmark: Single Event Send
// Target: < 100ns
// ============================================================================

/// Measure time to send a single change event through the broadcast channel
fn bench_change_event_send(c: &mut Criterion) {
    let mut group = c.benchmark_group("change_event_send");

    // Benchmark with no receivers (baseline)
    group.bench_function("no_receivers", |b| {
        let (sender, _receiver) = change_events::channel(1024);
        // Drop the receiver so there are no active receivers
        b.iter(|| {
            sender.send(ChangeEvent::Insert {
                table_name: "users".to_string(),
                row_index: black_box(0),
            })
        });
    });

    // Benchmark with one receiver (typical case)
    group.bench_function("one_receiver", |b| {
        let (sender, _receiver) = change_events::channel(1024);
        b.iter(|| {
            sender.send(ChangeEvent::Insert {
                table_name: "users".to_string(),
                row_index: black_box(0),
            })
        });
    });

    // Benchmark with pre-allocated table name (avoid String allocation overhead)
    group.bench_function("one_receiver_preallocated", |b| {
        let (sender, _receiver) = change_events::channel(1024);
        let table_name = "users".to_string();
        b.iter(|| {
            sender.send(ChangeEvent::Insert {
                table_name: table_name.clone(),
                row_index: black_box(0),
            })
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: INSERT Overhead
// Target: < 1µs additional overhead
// ============================================================================

/// Compare INSERT performance with and without change event broadcasting
fn bench_insert_with_change_event(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_overhead");

    // Benchmark: INSERT without change events (baseline)
    group.bench_function("without_change_events", |b| {
        b.iter_batched(
            || {
                let mut db = Database::new();
                db.create_table(make_schema("bench_table")).unwrap();
                db
            },
            |mut db| {
                for i in 0..100 {
                    db.insert_row("bench_table", make_row(i)).unwrap();
                }
                black_box(())
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Benchmark: INSERT with change events enabled (no active receiver)
    group.bench_function("with_change_events_no_receiver", |b| {
        b.iter_batched(
            || {
                let mut db = Database::new();
                db.create_table(make_schema("bench_table")).unwrap();
                let _rx = db.enable_change_events(1024);
                // Receiver dropped here, but change events still enabled
                db
            },
            |mut db| {
                for i in 0..100 {
                    db.insert_row("bench_table", make_row(i)).unwrap();
                }
                black_box(())
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Benchmark: INSERT with change events enabled (active receiver, not consuming)
    group.bench_function("with_change_events_active_receiver", |b| {
        b.iter_batched(
            || {
                let mut db = Database::new();
                db.create_table(make_schema("bench_table")).unwrap();
                let rx = db.enable_change_events(1024);
                (db, rx)
            },
            |(mut db, _rx)| {
                for i in 0..100 {
                    db.insert_row("bench_table", make_row(i)).unwrap();
                }
                black_box(())
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Benchmark: INSERT with change events + consuming receiver
    group.bench_function("with_change_events_consuming", |b| {
        b.iter_batched(
            || {
                let mut db = Database::new();
                db.create_table(make_schema("bench_table")).unwrap();
                let rx = db.enable_change_events(1024);
                (db, rx)
            },
            |(mut db, mut rx)| {
                for i in 0..100 {
                    db.insert_row("bench_table", make_row(i)).unwrap();
                }
                // Consume all events
                let events = rx.recv_all();
                black_box(events.len())
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark single INSERT overhead more precisely
fn bench_single_insert_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_insert_overhead");

    // Single INSERT without change events
    group.bench_function("without_change_events", |b| {
        let mut db = Database::new();
        db.create_table(make_schema("bench_table")).unwrap();
        let mut i = 0i64;

        b.iter(|| {
            // Use a unique ID each iteration to avoid PK conflicts
            i += 1;
            db.insert_row("bench_table", make_row(i)).unwrap();
        });
    });

    // Single INSERT with change events enabled
    group.bench_function("with_change_events", |b| {
        let mut db = Database::new();
        db.create_table(make_schema("bench_table")).unwrap();
        let _rx = db.enable_change_events(1024);
        let mut i = 0i64;

        b.iter(|| {
            i += 1;
            db.insert_row("bench_table", make_row(i)).unwrap();
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: Fanout to N Receivers
// Target: < 10µs for 100 receivers
// ============================================================================

/// Measure time to broadcast to N receivers
fn bench_change_event_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("change_event_fanout");

    for receiver_count in [1, 10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*receiver_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(receiver_count),
            receiver_count,
            |b, &count| {
                let (sender, first_receiver) = change_events::channel(1024);

                // Create additional receivers
                let mut receivers: Vec<ChangeEventReceiver> = vec![first_receiver];
                for _ in 1..count {
                    receivers.push(sender.subscribe());
                }

                b.iter(|| {
                    // Send a single event that fans out to all receivers
                    let num_receivers = sender.send(ChangeEvent::Insert {
                        table_name: "users".to_string(),
                        row_index: black_box(0),
                    });
                    black_box(num_receivers)
                });
            },
        );
    }

    group.finish();
}

/// Measure fanout + receive latency
fn bench_change_event_fanout_and_receive(c: &mut Criterion) {
    let mut group = c.benchmark_group("change_event_fanout_and_receive");

    for receiver_count in [1, 10, 100].iter() {
        group.throughput(Throughput::Elements(*receiver_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(receiver_count),
            receiver_count,
            |b, &count| {
                let (sender, first_receiver) = change_events::channel(1024);

                // Create receivers
                let mut receivers: Vec<ChangeEventReceiver> = vec![first_receiver];
                for _ in 1..count {
                    receivers.push(sender.subscribe());
                }

                b.iter(|| {
                    // Send event
                    sender.send(ChangeEvent::Insert {
                        table_name: "users".to_string(),
                        row_index: 0,
                    });

                    // All receivers consume the event
                    let mut total = 0;
                    for rx in receivers.iter_mut() {
                        if let Ok(_event) = rx.try_recv() {
                            total += 1;
                        }
                    }
                    black_box(total)
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark: Receive Operations
// ============================================================================

/// Measure receive performance
fn bench_change_event_receive(c: &mut Criterion) {
    let mut group = c.benchmark_group("change_event_receive");

    // try_recv when event available
    group.bench_function("try_recv_available", |b| {
        let (sender, mut receiver) = change_events::channel(1024);

        b.iter(|| {
            // Send an event
            sender.send(ChangeEvent::Insert { table_name: "users".to_string(), row_index: 0 });

            // Receive it
            let event = receiver.try_recv();
            black_box(event)
        });
    });

    // try_recv when no event (Empty)
    group.bench_function("try_recv_empty", |b| {
        let (_sender, mut receiver) = change_events::channel(1024);

        b.iter(|| {
            let result = receiver.try_recv();
            black_box(result)
        });
    });

    // recv_all with multiple events
    group.bench_function("recv_all_10_events", |b| {
        let (sender, mut receiver) = change_events::channel(1024);

        b.iter(|| {
            // Send 10 events
            for i in 0..10 {
                sender.send(ChangeEvent::Insert { table_name: "users".to_string(), row_index: i });
            }

            // Receive all
            let events = receiver.recv_all();
            black_box(events.len())
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: Batch Insert with Change Events
// ============================================================================

/// Benchmark batch inserts to measure amortized overhead
fn bench_batch_insert_change_events(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_insert_overhead");

    for batch_size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));

        // Without change events
        group.bench_with_input(
            BenchmarkId::new("without_change_events", batch_size),
            batch_size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut db = Database::new();
                        db.create_table(make_schema("bench_table")).unwrap();
                        let rows: Vec<Row> = (0..size).map(|i| make_row(i as i64)).collect();
                        (db, rows)
                    },
                    |(mut db, rows)| {
                        db.insert_rows_batch("bench_table", rows).unwrap();
                        black_box(())
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        // With change events
        group.bench_with_input(
            BenchmarkId::new("with_change_events", batch_size),
            batch_size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut db = Database::new();
                        db.create_table(make_schema("bench_table")).unwrap();
                        let rx = db.enable_change_events(size + 100);
                        let rows: Vec<Row> = (0..size).map(|i| make_row(i as i64)).collect();
                        (db, rows, rx)
                    },
                    |(mut db, rows, _rx)| {
                        db.insert_rows_batch("bench_table", rows).unwrap();
                        black_box(())
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_change_event_send,
    bench_insert_with_change_event,
    bench_single_insert_overhead,
    bench_change_event_fanout,
    bench_change_event_fanout_and_receive,
    bench_change_event_receive,
    bench_batch_insert_change_events,
);

criterion_main!(benches);
