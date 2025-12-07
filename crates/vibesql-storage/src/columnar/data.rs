//! Column data storage types.
//!
//! This module provides the `ColumnData` enum for storing typed column data
//! with NULL bitmap.
//!
//! ## Zero-Copy Design
//!
//! Column data uses `Arc<Vec<T>>` for all arrays, enabling:
//! - Zero-copy sharing between storage and executor layers
//! - O(1) clone operations (reference count bump instead of data copy)
//! - Cache-friendly columnar data that can be shared across query executions

use std::sync::Arc;
use vibesql_types::{Date, Interval, SqlValue, Time, Timestamp};

/// Typed column data with NULL bitmap
///
/// Each variant stores a vector of non-NULL values and a separate bitmap
/// indicating which positions are NULL. This design:
/// - Avoids Option<T> overhead (16 bytes vs 8 bytes for f64)
/// - Enables direct SIMD operations on value vectors
/// - Provides O(1) NULL checks via bitmap
/// - Uses Arc for zero-copy sharing with executor layer
/// - String columns use Arc<str> for O(1) cloning
#[derive(Debug, Clone)]
pub enum ColumnData {
    /// 64-bit signed integers
    Int64 { values: Arc<Vec<i64>>, nulls: Arc<Vec<bool>> },
    /// 64-bit floating point
    Float64 { values: Arc<Vec<f64>>, nulls: Arc<Vec<bool>> },
    /// Variable-length strings (using Arc<str> for O(1) cloning)
    String { values: Arc<Vec<Arc<str>>>, nulls: Arc<Vec<bool>> },
    /// Boolean values
    Bool { values: Arc<Vec<bool>>, nulls: Arc<Vec<bool>> },
    /// Date values
    Date { values: Arc<Vec<Date>>, nulls: Arc<Vec<bool>> },
    /// Time values
    Time { values: Arc<Vec<Time>>, nulls: Arc<Vec<bool>> },
    /// Timestamp values
    Timestamp { values: Arc<Vec<Timestamp>>, nulls: Arc<Vec<bool>> },
    /// Interval values
    Interval { values: Arc<Vec<Interval>>, nulls: Arc<Vec<bool>> },
    /// Vector values (for AI/ML workloads)
    Vector { values: Arc<Vec<Vec<f32>>>, nulls: Arc<Vec<bool>> },
}

#[allow(clippy::type_complexity)]
impl ColumnData {
    /// Get the number of values in this column (including NULLs)
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Int64 { nulls, .. } => nulls.len(),
            ColumnData::Float64 { nulls, .. } => nulls.len(),
            ColumnData::String { nulls, .. } => nulls.len(),
            ColumnData::Bool { nulls, .. } => nulls.len(),
            ColumnData::Date { nulls, .. } => nulls.len(),
            ColumnData::Time { nulls, .. } => nulls.len(),
            ColumnData::Timestamp { nulls, .. } => nulls.len(),
            ColumnData::Interval { nulls, .. } => nulls.len(),
            ColumnData::Vector { nulls, .. } => nulls.len(),
        }
    }

    /// Check if the column is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Estimate the memory size of this column in bytes
    ///
    /// This is used for memory budgeting in the columnar cache.
    /// The estimate includes:
    /// - Value storage (type-specific size * element count)
    /// - NULL bitmap (1 byte per element, not packed)
    /// - Vec overhead (capacity, length, pointer)
    pub fn size_in_bytes(&self) -> usize {
        const VEC_OVERHEAD: usize = 3 * std::mem::size_of::<usize>(); // ptr, len, cap

        match self {
            ColumnData::Int64 { values, nulls } => {
                VEC_OVERHEAD * 2
                    + values.capacity() * std::mem::size_of::<i64>()
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Float64 { values, nulls } => {
                VEC_OVERHEAD * 2
                    + values.capacity() * std::mem::size_of::<f64>()
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::String { values, nulls } => {
                // For Arc<str>, we need to account for the Arc overhead
                // plus the actual string data on the heap
                let arc_overhead = std::mem::size_of::<Arc<str>>(); // ptr + refcount
                let string_data: usize = values.iter().map(|s| s.len()).sum();
                VEC_OVERHEAD * 2
                    + values.capacity() * arc_overhead
                    + string_data
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Bool { values, nulls } => {
                VEC_OVERHEAD * 2
                    + values.capacity() * std::mem::size_of::<bool>()
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Date { values, nulls } => {
                VEC_OVERHEAD * 2
                    + values.capacity() * std::mem::size_of::<Date>()
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Time { values, nulls } => {
                VEC_OVERHEAD * 2
                    + values.capacity() * std::mem::size_of::<Time>()
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Timestamp { values, nulls } => {
                VEC_OVERHEAD * 2
                    + values.capacity() * std::mem::size_of::<Timestamp>()
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Interval { values, nulls } => {
                // Interval contains a String, so we need to account for that
                let interval_overhead = std::mem::size_of::<Interval>();
                let string_data: usize = values.iter().map(|i| i.value.capacity()).sum();
                VEC_OVERHEAD * 2
                    + values.capacity() * interval_overhead
                    + string_data
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Vector { values, nulls } => {
                // Vector contains Vec<f32>, so we need to account for each inner vector
                let vec_overhead = std::mem::size_of::<Vec<f32>>();
                let vector_data: usize =
                    values.iter().map(|v| v.capacity() * std::mem::size_of::<f32>()).sum();
                VEC_OVERHEAD * 2
                    + values.capacity() * vec_overhead
                    + vector_data
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
        }
    }

    /// Check if the value at the given index is NULL
    pub fn is_null(&self, index: usize) -> bool {
        match self {
            ColumnData::Int64 { nulls, .. } => nulls[index],
            ColumnData::Float64 { nulls, .. } => nulls[index],
            ColumnData::String { nulls, .. } => nulls[index],
            ColumnData::Bool { nulls, .. } => nulls[index],
            ColumnData::Date { nulls, .. } => nulls[index],
            ColumnData::Time { nulls, .. } => nulls[index],
            ColumnData::Timestamp { nulls, .. } => nulls[index],
            ColumnData::Interval { nulls, .. } => nulls[index],
            ColumnData::Vector { nulls, .. } => nulls[index],
        }
    }

    /// Get the SQL value at the given index (converts back to SqlValue)
    pub fn get(&self, index: usize) -> SqlValue {
        if self.is_null(index) {
            return SqlValue::Null;
        }

        match self {
            ColumnData::Int64 { values, .. } => SqlValue::Integer(values[index]),
            ColumnData::Float64 { values, .. } => SqlValue::Double(values[index]),
            ColumnData::String { values, .. } => SqlValue::Varchar(arcstr::ArcStr::from(values[index].as_ref())),
            ColumnData::Bool { values, .. } => SqlValue::Boolean(values[index]),
            ColumnData::Date { values, .. } => SqlValue::Date(values[index]),
            ColumnData::Time { values, .. } => SqlValue::Time(values[index]),
            ColumnData::Timestamp { values, .. } => SqlValue::Timestamp(values[index]),
            ColumnData::Interval { values, .. } => SqlValue::Interval(values[index].clone()),
            ColumnData::Vector { values, .. } => SqlValue::Vector(values[index].clone()),
        }
    }

    /// Get the underlying Arc for i64 values (zero-copy sharing with executor)
    pub fn as_i64_arc(&self) -> Option<(&Arc<Vec<i64>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::Int64 { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Get the underlying Arc for f64 values (zero-copy sharing with executor)
    pub fn as_f64_arc(&self) -> Option<(&Arc<Vec<f64>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::Float64 { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Get the underlying Arc for string values (zero-copy sharing with executor)
    pub fn as_string_arc(&self) -> Option<(&Arc<Vec<Arc<str>>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::String { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Get the underlying Arc for bool values (zero-copy sharing with executor)
    pub fn as_bool_arc(&self) -> Option<(&Arc<Vec<bool>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::Bool { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Get the underlying Arc for date values (zero-copy sharing with executor)
    pub fn as_date_arc(&self) -> Option<(&Arc<Vec<Date>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::Date { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Get the underlying Arc for timestamp values (zero-copy sharing with executor)
    pub fn as_timestamp_arc(&self) -> Option<(&Arc<Vec<Timestamp>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::Timestamp { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Get the underlying Arc for time values (zero-copy sharing with executor)
    pub fn as_time_arc(&self) -> Option<(&Arc<Vec<Time>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::Time { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Get the underlying Arc for interval values (zero-copy sharing with executor)
    pub fn as_interval_arc(&self) -> Option<(&Arc<Vec<Interval>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::Interval { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }
}
