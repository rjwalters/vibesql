//! Column data storage types.
//!
//! This module provides the `ColumnData` enum for storing typed column data
//! with NULL bitmap.

use vibesql_types::{Date, Interval, SqlValue, Time, Timestamp};

/// Typed column data with NULL bitmap
///
/// Each variant stores a vector of non-NULL values and a separate bitmap
/// indicating which positions are NULL. This design:
/// - Avoids Option<T> overhead (16 bytes vs 8 bytes for f64)
/// - Enables direct SIMD operations on value vectors
/// - Provides O(1) NULL checks via bitmap
#[derive(Debug, Clone)]
pub enum ColumnData {
    /// 64-bit signed integers
    Int64 { values: Vec<i64>, nulls: Vec<bool> },
    /// 64-bit floating point
    Float64 { values: Vec<f64>, nulls: Vec<bool> },
    /// Variable-length strings
    String { values: Vec<String>, nulls: Vec<bool> },
    /// Boolean values
    Bool { values: Vec<bool>, nulls: Vec<bool> },
    /// Date values
    Date { values: Vec<Date>, nulls: Vec<bool> },
    /// Time values
    Time { values: Vec<Time>, nulls: Vec<bool> },
    /// Timestamp values
    Timestamp { values: Vec<Timestamp>, nulls: Vec<bool> },
    /// Interval values
    Interval { values: Vec<Interval>, nulls: Vec<bool> },
}

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
                // For strings, we need to account for the String struct overhead
                // plus the actual string data on the heap
                let string_overhead = std::mem::size_of::<String>(); // ptr, len, cap
                let string_data: usize = values.iter().map(|s| s.capacity()).sum();
                VEC_OVERHEAD * 2
                    + values.capacity() * string_overhead
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
            ColumnData::String { values, .. } => SqlValue::Varchar(values[index].clone()),
            ColumnData::Bool { values, .. } => SqlValue::Boolean(values[index]),
            ColumnData::Date { values, .. } => SqlValue::Date(values[index]),
            ColumnData::Time { values, .. } => SqlValue::Time(values[index]),
            ColumnData::Timestamp { values, .. } => SqlValue::Timestamp(values[index]),
            ColumnData::Interval { values, .. } => SqlValue::Interval(values[index].clone()),
        }
    }
}
