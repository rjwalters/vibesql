//! Vectorized execution module with Apache Arrow SIMD support
//!
//! This module provides both:
//! 1. Chunk-based predicate evaluation for cache optimization (existing)
//! 2. Apache Arrow RecordBatch-based SIMD execution (Phase 3 - NEW)
//!
//! ## SIMD Vectorization (Phase 3)
//!
//! The Arrow-based approach provides true SIMD vectorization:
//! - **RecordBatch adapter**: Converts row-based data to columnar Arrow format
//! - **SIMD filter kernel**: Uses Arrow compute for 4-8 values per CPU instruction
//! - **SIMD aggregation**: Vectorized SUM/AVG/MIN/MAX operations
//! - **Target**: 5-10x performance improvement for analytical queries
//!
//! See `PHASE3_VECTORIZATION.md` in the crate root for complete design documentation.

pub mod aggregate;
pub mod arithmetic;
pub mod batch;
pub mod bitmap;
pub mod compiled_predicate;
pub mod filter;
pub mod predicate;

pub use arithmetic::evaluate_arithmetic_simd;
pub use batch::{record_batch_to_rows, rows_to_record_batch};
pub use filter::filter_record_batch_simd;
pub use predicate::apply_where_filter_vectorized;

/// Default chunk size for vectorized operations
/// Tuned for L1 cache utilization (typical 32-64KB)
#[allow(dead_code)]
pub const DEFAULT_CHUNK_SIZE: usize = 256;

/// Threshold for using vectorized evaluation
/// Below this, row-by-row is more efficient due to lower setup overhead
pub const VECTORIZE_THRESHOLD: usize = 100;
