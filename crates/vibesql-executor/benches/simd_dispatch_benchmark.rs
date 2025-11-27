//! Benchmark for SIMD dispatch overhead and performance
//!
//! This benchmark measures:
//! 1. Runtime dispatch overhead (should be negligible)
//! 2. Performance of different SIMD implementations
//! 3. Comparison between direct and dispatched calls

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;

#[cfg(feature = "simd")]
use vibesql_executor::simd::{dispatched, simd_add_f64, CpuFeatures};

#[cfg(feature = "simd")]
fn benchmark_dispatch_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_dispatch_overhead");

    let sizes = vec![100, 1000, 10000];

    for size in sizes {
        let a: Vec<f64> = (0..size).map(|i| i as f64).collect();
        let b: Vec<f64> = (0..size).map(|i| (i * 2) as f64).collect();

        group.throughput(Throughput::Elements(size as u64));

        // Direct call (no dispatch)
        group.bench_with_input(BenchmarkId::new("direct_add", size), &size, |bench, _| {
            bench.iter(|| {
                let result = simd_add_f64(black_box(&a), black_box(&b));
                black_box(result);
            });
        });

        // Dispatched call (with runtime selection)
        group.bench_with_input(
            BenchmarkId::new("dispatched_add", size),
            &size,
            |bench, _| {
                bench.iter(|| {
                    let result = dispatched::add_f64(black_box(&a), black_box(&b));
                    black_box(result);
                });
            },
        );
    }

    group.finish();
}

#[cfg(feature = "simd")]
fn benchmark_operations(c: &mut Criterion) {
    let features = CpuFeatures::get();
    let level = features.best_simd_level();

    let mut group = c.benchmark_group(format!("simd_operations_{}", level.name()));

    let size = 10000;
    let a: Vec<f64> = (0..size).map(|i| i as f64 + 1.0).collect();
    let b: Vec<f64> = (0..size).map(|i| (i * 2) as f64 + 1.0).collect();

    group.throughput(Throughput::Elements(size as u64));

    group.bench_function("add_f64", |bench| {
        bench.iter(|| {
            let result = dispatched::add_f64(black_box(&a), black_box(&b));
            black_box(result);
        });
    });

    group.bench_function("mul_f64", |bench| {
        bench.iter(|| {
            let result = dispatched::mul_f64(black_box(&a), black_box(&b));
            black_box(result);
        });
    });

    group.bench_function("sub_f64", |bench| {
        bench.iter(|| {
            let result = dispatched::sub_f64(black_box(&a), black_box(&b));
            black_box(result);
        });
    });

    group.bench_function("div_f64", |bench| {
        bench.iter(|| {
            let result = dispatched::div_f64(black_box(&a), black_box(&b));
            black_box(result);
        });
    });

    let a_i64: Vec<i64> = (0..size).map(|i| i as i64 + 1).collect();
    let b_i64: Vec<i64> = (0..size).map(|i| (i * 2) as i64 + 1).collect();

    group.bench_function("add_i64", |bench| {
        bench.iter(|| {
            let result = dispatched::add_i64(black_box(&a_i64), black_box(&b_i64));
            black_box(result);
        });
    });

    group.bench_function("mul_i64", |bench| {
        bench.iter(|| {
            let result = dispatched::mul_i64(black_box(&a_i64), black_box(&b_i64));
            black_box(result);
        });
    });

    group.finish();
}

#[cfg(feature = "simd")]
fn benchmark_scalar_vs_simd(c: &mut Criterion) {
    let mut group = c.benchmark_group("scalar_vs_simd");

    let size = 10000;
    let a: Vec<f64> = (0..size).map(|i| i as f64 + 1.0).collect();
    let b: Vec<f64> = (0..size).map(|i| (i * 2) as f64 + 1.0).collect();

    group.throughput(Throughput::Elements(size as u64));

    // Scalar baseline
    group.bench_function("scalar_add", |bench| {
        bench.iter(|| {
            let result: Vec<f64> = a
                .iter()
                .zip(b.iter())
                .map(|(x, y)| x + y)
                .collect();
            black_box(result);
        });
    });

    // SIMD with dispatch
    group.bench_function("simd_add", |bench| {
        bench.iter(|| {
            let result = dispatched::add_f64(black_box(&a), black_box(&b));
            black_box(result);
        });
    });

    group.finish();
}

#[cfg(feature = "simd")]
criterion_group!(
    benches,
    benchmark_dispatch_overhead,
    benchmark_operations,
    benchmark_scalar_vs_simd
);

#[cfg(not(feature = "simd"))]
criterion_group!(benches,);

criterion_main!(benches);
