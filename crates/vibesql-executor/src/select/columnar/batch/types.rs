//! Core batch data structures and type definitions
//!
//! This module contains the fundamental types for columnar batch storage:
//! - `ColumnarBatch`: The main container for columnar data
//! - `ColumnArray`: Type-specialized column storage
//! - `ColumnType`: Internal type inference helper

use std::sync::Arc;
use vibesql_types::SqlValue;

/// A columnar batch stores data in column-oriented format for efficient SIMD processing
///
/// Unlike row-oriented storage (Vec<Row>), columnar batches store each column
/// in a contiguous array, enabling:
/// - SIMD vectorization (process 4-8 values per instruction)
/// - Better cache locality (columns accessed together are stored together)
/// - Type-specialized code paths (no SqlValue enum matching)
/// - Efficient NULL handling with separate bitmasks
///
/// # Example
///
/// ```text
/// // Convert rows to columnar batch
/// let batch = ColumnarBatch::from_rows(&rows, &schema)?;
///
/// // Access columns with zero-copy
/// if let ColumnArray::Int64(values, nulls) = &batch.columns[0] {
///     // Process with SIMD operations
///     let sum = simd_sum_i64(values);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ColumnarBatch {
    /// Number of rows in this batch
    pub(crate) row_count: usize,

    /// Column arrays (one per column)
    pub(crate) columns: Vec<ColumnArray>,

    /// Optional column names for debugging
    pub(crate) column_names: Option<Vec<String>>,
}

/// Type-specialized column storage
///
/// Each variant stores values in a native array for maximum SIMD efficiency.
/// NULL values are tracked separately in a boolean bitmap.
///
/// ## Zero-Copy Design
///
/// All column data is wrapped in `Arc<Vec<T>>`, enabling:
/// - Zero-copy sharing with storage layer
/// - O(1) clone operations for query execution
/// - Direct SIMD access via `as_slice()` on Arc contents
#[derive(Debug, Clone)]
pub enum ColumnArray {
    /// 64-bit integers (INT, BIGINT)
    Int64(Arc<Vec<i64>>, Option<Arc<Vec<bool>>>),

    /// 32-bit integers (INT, SMALLINT)
    Int32(Arc<Vec<i32>>, Option<Arc<Vec<bool>>>),

    /// 64-bit floats (DOUBLE PRECISION, FLOAT)
    Float64(Arc<Vec<f64>>, Option<Arc<Vec<bool>>>),

    /// 32-bit floats (REAL)
    Float32(Arc<Vec<f32>>, Option<Arc<Vec<bool>>>),

    /// Variable-length strings (VARCHAR, TEXT)
    String(Arc<Vec<Arc<str>>>, Option<Arc<Vec<bool>>>),

    /// Fixed-length strings (CHAR)
    FixedString(Arc<Vec<Arc<str>>>, Option<Arc<Vec<bool>>>),

    /// Dates (stored as i32 days since epoch)
    Date(Arc<Vec<i32>>, Option<Arc<Vec<bool>>>),

    /// Timestamps (stored as i64 microseconds since epoch)
    Timestamp(Arc<Vec<i64>>, Option<Arc<Vec<bool>>>),

    /// Booleans (stored as bytes for SIMD compatibility)
    Boolean(Arc<Vec<u8>>, Option<Arc<Vec<bool>>>),

    /// Mixed-type column (fallback for complex types)
    Mixed(Arc<Vec<SqlValue>>),
}

/// Internal column type inference
#[derive(Debug, Clone, Copy)]
pub(crate) enum ColumnType {
    Int64,
    Float64,
    String,
    Date,
    Boolean,
    Mixed,
}
