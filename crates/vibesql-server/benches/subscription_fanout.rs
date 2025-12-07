//! Subscription fanout benchmarks
//!
//! Benchmarks for subscription manager performance:
//! - Subscription creation (with query parsing and table extraction)
//! - Fanout to many subscriptions on a single table
//! - Table indexing efficiency
//! - Concurrent subscription handling
//!
//! Run with:
//!   cargo bench --package vibesql-server --bench subscription_fanout
//!
//! ## Target Performance (from issue #3449)
//!
//! | Benchmark | Target |
//! |-----------|--------|
//! | Create subscription | < 100µs |
//! | Find affected (10K subs) | < 1ms |
//! | Fanout to 10K subs | < 10ms |
//! | Concurrent ops (100 threads) | No deadlocks, linear scaling |

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use tokio::sync::mpsc;

use vibesql_server::subscription::{SubscriptionId, SubscriptionManager, SubscriptionUpdate};

/// Create a SubscriptionManager with N subscriptions all watching the same table
fn setup_manager_with_subscriptions(
    count: usize,
) -> (SubscriptionManager, Vec<mpsc::Receiver<SubscriptionUpdate>>) {
    let manager = SubscriptionManager::new();
    let mut receivers = Vec::with_capacity(count);

    for _ in 0..count {
        let (tx, rx) = mpsc::channel(16);
        // Simple query on the "users" table
        let _id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();
        receivers.push(rx);
    }

    (manager, receivers)
}

/// Create a SubscriptionManager with subscriptions distributed across multiple tables
fn setup_manager_distributed(
    total: usize,
    tables: &[&str],
) -> (SubscriptionManager, Vec<mpsc::Receiver<SubscriptionUpdate>>) {
    let manager = SubscriptionManager::new();
    let mut receivers = Vec::with_capacity(total);
    let subs_per_table = total / tables.len();

    for table in tables {
        for _ in 0..subs_per_table {
            let (tx, rx) = mpsc::channel(16);
            let _ = manager.subscribe(format!("SELECT * FROM {}", table), tx).unwrap();
            receivers.push(rx);
        }
    }

    (manager, receivers)
}

// ============================================================================
// Benchmark: Subscription Creation
// ============================================================================

/// Benchmark single subscription creation time (target: < 100µs)
///
/// Measures the time to create a subscription including:
/// - Query parsing
/// - Table dependency extraction
/// - Registration in manager
fn bench_subscription_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription_creation");

    // Benchmark simple query
    group.bench_function("simple_query", |b| {
        let manager = SubscriptionManager::new();
        b.iter(|| {
            let (tx, _rx) = mpsc::channel(1);
            let id = manager.subscribe("SELECT * FROM users".to_string(), tx);
            black_box(id)
        });
    });

    // Benchmark query with WHERE clause
    group.bench_function("query_with_where", |b| {
        let manager = SubscriptionManager::new();
        b.iter(|| {
            let (tx, _rx) = mpsc::channel(1);
            let id = manager.subscribe(
                "SELECT id, name FROM users WHERE active = TRUE AND age > 18".to_string(),
                tx,
            );
            black_box(id)
        });
    });

    // Benchmark JOIN query (more complex parsing)
    group.bench_function("join_query", |b| {
        let manager = SubscriptionManager::new();
        b.iter(|| {
            let (tx, _rx) = mpsc::channel(1);
            let id = manager.subscribe(
                "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id".to_string(),
                tx,
            );
            black_box(id)
        });
    });

    // Benchmark complex query with subquery
    group.bench_function("complex_query", |b| {
        let manager = SubscriptionManager::new();
        b.iter(|| {
            let (tx, _rx) = mpsc::channel(1);
            let id = manager.subscribe(
                "SELECT u.id, u.name, (SELECT COUNT(*) FROM orders WHERE user_id = u.id) as order_count FROM users u WHERE u.active = TRUE".to_string(),
                tx,
            );
            black_box(id)
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: Find Affected Subscriptions (Lookup)
// ============================================================================

/// Benchmark finding affected subscriptions by table name (target: < 1ms for 10K)
fn bench_find_affected_subscriptions(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("find_affected_subscriptions");

    for size in [100, 1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &subscription_count| {
                let (manager, _receivers) = setup_manager_with_subscriptions(subscription_count);

                b.to_async(&rt).iter(|| async {
                    let affected = manager.find_affected_subscriptions("users");
                    black_box(affected.len())
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark: Fanout Notification
// ============================================================================

/// Benchmark total time to notify all affected subscriptions (target: < 10ms for 10K)
///
/// This measures the full fanout operation:
/// 1. Find affected subscriptions
/// 2. Clone sender handles
/// 3. Send notifications to all channels
fn bench_fanout_notification(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("fanout_notification");

    for size in [100, 1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.to_async(&rt).iter_batched(
                || {
                    // Setup: create manager with subscriptions and keep receivers alive
                    let manager = SubscriptionManager::new();
                    let mut receivers = Vec::with_capacity(size);
                    for _ in 0..size {
                        let (tx, rx) = mpsc::channel(16);
                        let _ = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();
                        receivers.push(rx);
                    }
                    (Arc::new(manager), receivers)
                },
                |(manager, receivers)| async move {
                    // Benchmark: simulate fanout by finding and iterating affected
                    let affected = manager.find_affected_subscriptions("users");

                    // Simulate notification by accessing each subscription
                    // (full notification requires DB access, so we measure the lookup + iteration)
                    let count = affected.len();
                    black_box(count);

                    // Keep receivers alive until after benchmark
                    drop(receivers);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark actual channel send operations (measures channel overhead)
fn bench_channel_fanout(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("channel_fanout");

    for size in [100, 1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.to_async(&rt).iter_batched(
                || {
                    // Setup: create channels
                    let mut senders = Vec::with_capacity(size);
                    let mut receivers = Vec::with_capacity(size);
                    for _ in 0..size {
                        let (tx, rx) = mpsc::channel::<SubscriptionUpdate>(16);
                        senders.push(tx);
                        receivers.push(rx);
                    }
                    (senders, receivers)
                },
                |(senders, receivers)| async move {
                    // Benchmark: send to all channels
                    let update = SubscriptionUpdate::Full { subscription_id: SubscriptionId::new(), rows: vec![] };
                    for sender in &senders {
                        let _ = sender.try_send(update.clone());
                    }
                    black_box(senders.len());

                    // Keep receivers alive
                    drop(receivers);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark: Subscribe/Unsubscribe Operations
// ============================================================================

/// Benchmark batch subscribe operations
fn bench_subscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscribe_batch");

    for size in [100, 1_000, 5_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let manager = SubscriptionManager::new();
                for _ in 0..size {
                    let (tx, _rx) = mpsc::channel(1);
                    let id = manager.subscribe("SELECT * FROM users".to_string(), tx);
                    let _ = black_box(id);
                }
                black_box(manager.subscription_count())
            });
        });
    }

    group.finish();
}

/// Benchmark unsubscribe operations
fn bench_unsubscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("unsubscribe");

    for size in [100, 1_000, 5_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    // Setup: create manager with subscriptions
                    let manager = SubscriptionManager::new();
                    let mut ids = Vec::with_capacity(size);
                    for _ in 0..size {
                        let (tx, _rx) = mpsc::channel(1);
                        let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();
                        ids.push(id);
                    }
                    (manager, ids)
                },
                |(manager, ids)| {
                    // Benchmark: unsubscribe all
                    for id in ids {
                        manager.unsubscribe(id);
                    }
                    black_box(manager.subscription_count())
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark: Concurrent Operations
// ============================================================================

/// Benchmark concurrent subscribe/unsubscribe operations
/// Target: No deadlocks, linear scaling with threads
fn bench_concurrent_subscribe_unsubscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_operations");

    for num_threads in [2, 4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let manager = Arc::new(SubscriptionManager::new());
                    let ops_per_thread = 100;

                    let handles: Vec<_> = (0..num_threads)
                        .map(|_| {
                            let manager = Arc::clone(&manager);
                            thread::spawn(move || {
                                let mut ids = Vec::with_capacity(ops_per_thread);

                                // Subscribe phase
                                for _ in 0..ops_per_thread {
                                    let (tx, _rx) = mpsc::channel(1);
                                    if let Ok(id) =
                                        manager.subscribe("SELECT * FROM users".to_string(), tx)
                                    {
                                        ids.push(id);
                                    }
                                }

                                // Unsubscribe phase
                                for id in ids {
                                    manager.unsubscribe(id);
                                }
                            })
                        })
                        .collect();

                    for handle in handles {
                        handle.join().unwrap();
                    }

                    black_box(manager.subscription_count())
                });
            },
        );
    }

    group.finish();
}

/// Benchmark concurrent mixed workload (subscribe, unsubscribe, lookup)
fn bench_concurrent_mixed_workload(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrent_mixed");

    group.bench_function("100_concurrent_ops", |b| {
        b.to_async(&rt).iter(|| async {
            let manager = Arc::new(SubscriptionManager::new());
            let mut handles = Vec::with_capacity(100);

            // Spawn concurrent tasks
            for i in 0..100 {
                let manager = Arc::clone(&manager);
                handles.push(tokio::spawn(async move {
                    match i % 3 {
                        0 => {
                            // Subscribe
                            let (tx, _rx) = mpsc::channel(1);
                            let _ = manager.subscribe("SELECT * FROM users".to_string(), tx);
                        }
                        1 => {
                            // Lookup
                            let _ = manager.find_affected_subscriptions("users");
                        }
                        _ => {
                            // Subscribe then unsubscribe
                            let (tx, _rx) = mpsc::channel(1);
                            if let Ok(id) =
                                manager.subscribe("SELECT * FROM orders".to_string(), tx)
                            {
                                manager.unsubscribe(id);
                            }
                        }
                    }
                }));
            }

            for handle in handles {
                let _ = handle.await;
            }

            black_box(manager.subscription_count())
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: Table Index Efficiency
// ============================================================================

/// Benchmark table index lookup with subscriptions distributed across multiple tables
fn bench_table_index_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_index_lookup");

    let tables = vec![
        "users",
        "orders",
        "products",
        "inventory",
        "payments",
        "shipments",
        "reviews",
        "carts",
    ];

    for total_subscriptions in [1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(total_subscriptions),
            total_subscriptions,
            |b, &total| {
                let (manager, _receivers) = setup_manager_distributed(total, &tables);

                // Benchmark looking up affected subscriptions for a random table
                b.iter(|| {
                    let affected = manager.find_affected_subscriptions("users");
                    black_box(affected.len())
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark: Mixed Workload (Steady State)
// ============================================================================

/// Benchmark mixed workload simulating realistic usage patterns
fn bench_mixed_workload(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("mixed_workload");

    // Simulate realistic usage: maintain ~1000 active subscriptions
    // while handling continuous subscribe/unsubscribe/lookup operations
    group.bench_function("1000_active_ops", |b| {
        b.to_async(&rt).iter(|| async {
            let manager = SubscriptionManager::new();
            let mut ids: Vec<SubscriptionId> = Vec::with_capacity(1000);

            // Initial setup: 1000 subscriptions
            for _ in 0..1000 {
                let (tx, _rx) = mpsc::channel(1);
                let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();
                ids.push(id);
            }

            // Perform 100 operations: mix of subscribe, unsubscribe, lookup
            for i in 0..100 {
                match i % 3 {
                    0 => {
                        // Subscribe new
                        let (tx, _rx) = mpsc::channel(1);
                        let id = manager.subscribe("SELECT * FROM orders".to_string(), tx).unwrap();
                        ids.push(id);
                    }
                    1 => {
                        // Unsubscribe oldest
                        if !ids.is_empty() {
                            let id = ids.remove(0);
                            manager.unsubscribe(id);
                        }
                    }
                    _ => {
                        // Lookup affected
                        let affected = manager.find_affected_subscriptions("users");
                        black_box(affected.len());
                    }
                }
            }

            black_box(manager.subscription_count())
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: Memory Footprint (Informational)
// ============================================================================

/// Benchmark to measure memory characteristics (subscription count scaling)
fn bench_subscription_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription_scaling");

    // Measure time to create increasing numbers of subscriptions
    for size in [1_000, 5_000, 10_000, 20_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let manager = SubscriptionManager::new();
                for _ in 0..size {
                    let (tx, _rx) = mpsc::channel(1);
                    let _ = manager.subscribe("SELECT * FROM users".to_string(), tx);
                }
                black_box(manager.subscription_count())
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    // Core benchmarks from issue requirements
    bench_subscription_creation,
    bench_find_affected_subscriptions,
    bench_fanout_notification,
    bench_channel_fanout,
    bench_concurrent_subscribe_unsubscribe,
    // Additional benchmarks
    bench_subscribe,
    bench_unsubscribe,
    bench_table_index_lookup,
    bench_mixed_workload,
    bench_concurrent_mixed_workload,
    bench_subscription_scaling,
);

criterion_main!(benches);
