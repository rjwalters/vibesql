//! Subscription fanout benchmarks
//!
//! Benchmarks for subscription manager performance:
//! - Fanout to many subscriptions on a single table
//! - Table indexing efficiency
//! - Concurrent subscription handling
//!
//! Run with:
//!   cargo bench --package vibesql-server --bench subscription_fanout
//!
//! Target: < 10ms fanout for 10,000 subscriptions

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use tokio::sync::mpsc;

use vibesql_server::subscription::{SubscriptionId, SubscriptionManager, SubscriptionUpdate};

/// Create a SubscriptionManager with N subscriptions all watching the same table
fn setup_manager_with_subscriptions(
    count: usize,
) -> (SubscriptionManager, Vec<mpsc::Receiver<SubscriptionUpdate>>) {
    let manager = SubscriptionManager::new(None);
    let mut receivers = Vec::with_capacity(count);

    for _ in 0..count {
        let (tx, rx) = mpsc::channel(16);
        // Simple query on the "users" table
        let _id = manager
            .subscribe("SELECT * FROM users".to_string(), tx)
            .unwrap();
        receivers.push(rx);
    }

    (manager, receivers)
}

/// Benchmark finding affected subscriptions by table name
fn bench_find_affected_subscriptions(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("find_affected_subscriptions");

    for size in [100, 1000, 10_000].iter() {
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

/// Benchmark subscribe operation
fn bench_subscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscribe");

    for size in [100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let manager = SubscriptionManager::new(None);
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

/// Benchmark unsubscribe operation
fn bench_unsubscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("unsubscribe");

    for size in [100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    // Setup: create manager with subscriptions
                    let manager = SubscriptionManager::new(None);
                    let mut ids = Vec::with_capacity(size);
                    for _ in 0..size {
                        let (tx, _rx) = mpsc::channel(1);
                        let id = manager
                            .subscribe("SELECT * FROM users".to_string(), tx)
                            .unwrap();
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

/// Benchmark table index lookup (simulates the lookup done during change handling)
fn bench_table_index_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_index_lookup");

    // Create subscriptions across multiple tables to simulate realistic workload
    let tables = vec![
        "users", "orders", "products", "inventory", "payments", "shipments", "reviews", "carts",
    ];

    for total_subscriptions in [1000, 10_000].iter() {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(total_subscriptions),
            total_subscriptions,
            |b, &total| {
                let manager = SubscriptionManager::new(None);
                let subs_per_table = total / tables.len();

                // Distribute subscriptions across tables
                for table in &tables {
                    for _ in 0..subs_per_table {
                        let (tx, _rx) = mpsc::channel(1);
                        let _ = manager.subscribe(format!("SELECT * FROM {}", table), tx);
                    }
                }

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

/// Benchmark mixed workload (subscribe, unsubscribe, lookup)
fn bench_mixed_workload(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("mixed_workload");

    // Simulate realistic usage: maintain ~1000 active subscriptions
    // while handling continuous subscribe/unsubscribe/lookup operations
    group.bench_function("1000_active_ops", |b| {
        b.to_async(&rt).iter(|| async {
            let manager = SubscriptionManager::new(None);
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

criterion_group!(
    benches,
    bench_find_affected_subscriptions,
    bench_subscribe,
    bench_unsubscribe,
    bench_table_index_lookup,
    bench_mixed_workload,
);

criterion_main!(benches);
