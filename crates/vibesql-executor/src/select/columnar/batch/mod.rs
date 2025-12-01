//! Columnar batch structure for high-performance query execution
//!
//! This module implements true columnar storage with type-specialized column arrays
//! optimized for SIMD operations and cache locality.
//!
//! ## Zero-Copy Design
//!
//! ColumnArray uses `Arc<Vec<T>>` for column data, enabling:
//! - Zero-copy sharing with storage layer (via `from_storage_columnar`)
//! - O(1) clone operations (reference count bump instead of data copy)
//! - Cache-friendly data that can be shared across query executions
//!
//! Note: Index-based iteration is used in some places for performance (better vectorization).
//!
//! ## Module Structure
//!
//! - `types`: Core data structures (`ColumnarBatch`, `ColumnArray`)
//! - `builder`: Batch construction methods (`new`, `from_rows`, `from_columns`)
//! - `operations`: Accessor and manipulation methods
//! - `arrow`: Arrow RecordBatch conversion
//! - `storage`: Storage layer ColumnarTable conversion

mod arrow;
mod builder;
mod operations;
mod storage;
mod types;

// Re-export public types
pub use types::{ColumnArray, ColumnarBatch};
