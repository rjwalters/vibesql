//! Demonstration of runtime SIMD dispatch
//!
//! This example shows how CPU features are detected at runtime
//! and how operations are dispatched to the best available implementation.
//!
//! Run with: cargo run --example simd_dispatch_demo --features simd

#[cfg(feature = "simd")]
use vibesql_executor::simd::{dispatched, CpuFeatures};

fn main() {
    env_logger::init();

    #[cfg(feature = "simd")]
    {
        println!("=== Runtime SIMD Dispatch Demo ===\n");

        // Detect CPU features
        let features = CpuFeatures::get();
        let level = features.best_simd_level();

        println!("CPU Features:");
        println!("  SSE4.2:    {}", features.has_sse42);
        println!("  AVX2:      {}", features.has_avx2);
        println!("  AVX-512F:  {}", features.has_avx512f);
        println!("  AVX-512DQ: {}", features.has_avx512dq);
        println!("  NEON:      {}", features.has_neon);
        println!("  SVE:       {}", features.has_sve);
        println!();

        println!("Selected SIMD Level: {}", level.name());
        println!("  Vector width: {} bits", level.vector_width_bits());
        println!("  f64 lanes:    {}", level.f64_lanes());
        println!("  i64 lanes:    {}", level.i64_lanes());
        println!();

        // Test data
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let b = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0];

        // Perform operations using runtime dispatch
        println!("=== Operations using {} ===", level.name());
        println!();

        let result = dispatched::add_f64(&a, &b);
        println!("Addition:       {:?} + {:?} = {:?}", a, b, result);

        let result = dispatched::mul_f64(&a, &b);
        println!("Multiplication: {:?} * {:?} = {:?}", a, b, result);

        println!();
        println!("=== Performance Characteristics ===");
        println!();
        println!("Theoretical speedup vs scalar:");
        match level {
            vibesql_executor::simd::SimdLevel::Scalar => {
                println!("  1x (baseline - no SIMD)")
            }
            vibesql_executor::simd::SimdLevel::Sse42 => {
                println!("  ~2x (2 doubles per instruction)")
            }
            vibesql_executor::simd::SimdLevel::Avx2 => {
                println!("  ~4x (4 doubles per instruction)")
            }
            vibesql_executor::simd::SimdLevel::Avx512 => {
                println!("  ~8x (8 doubles per instruction)")
            }
            vibesql_executor::simd::SimdLevel::Neon => {
                println!("  ~2x (2 doubles per instruction)")
            }
            vibesql_executor::simd::SimdLevel::Sve => {
                println!("  ~4x (variable width, typically 256-bit)")
            }
        }

        println!();
        println!("Note: Actual speedup is typically 50-80% of theoretical");
        println!("due to memory bandwidth and other bottlenecks.");
    }

    #[cfg(not(feature = "simd"))]
    {
        println!("SIMD feature is not enabled!");
        println!("Run with: cargo run --example simd_dispatch_demo --features simd");
    }
}
