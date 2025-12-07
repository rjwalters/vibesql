//! SQL Value runtime representation

mod comparison;
mod display;
mod hash;

use crate::{
    temporal::{Date, Interval, IntervalField, Time, Timestamp},
    DataType,
};

/// String type for SQL values using ArcStr with small string optimization (SSO).
/// Strings ≤22 bytes are stored inline without heap allocation.
pub type StringValue = arcstr::ArcStr;

/// SQL Values - runtime representation of data
///
/// Represents actual values in SQL, including NULL.
///
/// String types use `StringValue` (ArcStr) which provides O(1) cloning.
/// Strings ≤22 bytes are stored inline (small string optimization) avoiding heap allocation.
#[derive(Debug, Clone)]
pub enum SqlValue {
    Integer(i64),
    Smallint(i16),
    Bigint(i64),
    Unsigned(u64), // 64-bit unsigned integer (MySQL compatibility)
    Numeric(f64),  // f64 for performance (was: String)

    Float(f32),
    Real(f32),
    Double(f64),

    Character(StringValue),
    Varchar(StringValue),

    Boolean(bool),

    // Date/Time types with proper structured representation
    Date(Date),
    Time(Time),
    Timestamp(Timestamp),

    // Interval type
    Interval(Interval),

    // Vector type (for AI/ML workloads)
    Vector(Vec<f32>),

    Null,
}

impl SqlValue {
    /// Check if this value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, SqlValue::Null)
    }

    /// Get the type name as a string (for error messages)
    pub fn type_name(&self) -> &'static str {
        match self {
            SqlValue::Integer(_) => "INTEGER",
            SqlValue::Smallint(_) => "SMALLINT",
            SqlValue::Bigint(_) => "BIGINT",
            SqlValue::Unsigned(_) => "UNSIGNED",
            SqlValue::Numeric(_) => "NUMERIC",
            SqlValue::Float(_) => "FLOAT",
            SqlValue::Real(_) => "REAL",
            SqlValue::Double(_) => "DOUBLE PRECISION",
            SqlValue::Character(_) => "CHAR",
            SqlValue::Varchar(_) => "VARCHAR",
            SqlValue::Boolean(_) => "BOOLEAN",
            SqlValue::Date(_) => "DATE",
            SqlValue::Time(_) => "TIME",
            SqlValue::Timestamp(_) => "TIMESTAMP",
            SqlValue::Interval(_) => "INTERVAL",
            SqlValue::Vector(_) => "VECTOR",
            SqlValue::Null => "NULL",
        }
    }

    /// Get the data type of this value
    pub fn get_type(&self) -> DataType {
        match self {
            SqlValue::Integer(_) => DataType::Integer,
            SqlValue::Smallint(_) => DataType::Smallint,
            SqlValue::Bigint(_) => DataType::Bigint,
            SqlValue::Unsigned(_) => DataType::Unsigned,
            SqlValue::Numeric(_) => DataType::Numeric { precision: 38, scale: 0 }, // Default
            SqlValue::Float(_) => DataType::Float { precision: 53 }, // Default to double precision
            SqlValue::Real(_) => DataType::Real,
            SqlValue::Double(_) => DataType::DoublePrecision,
            SqlValue::Character(s) => DataType::Character { length: s.len() },  // Arc<str> has len()
            SqlValue::Varchar(_) => DataType::Varchar { max_length: None }, /* Unknown/unlimited */
            // length
            SqlValue::Boolean(_) => DataType::Boolean,
            SqlValue::Date(_) => DataType::Date,
            SqlValue::Time(_) => DataType::Time { with_timezone: false },
            SqlValue::Timestamp(_) => DataType::Timestamp { with_timezone: false },
            SqlValue::Interval(_) => DataType::Interval {
                start_field: IntervalField::Day, /* Default - actual type lost in string
                                                  * representation */
                end_field: None,
            },
            SqlValue::Vector(v) => DataType::Vector { dimensions: v.len() as u32 },
            SqlValue::Null => DataType::Null,
        }
    }

    /// Estimate the memory size of this value in bytes
    ///
    /// Used for memory limit tracking during query execution.
    /// Provides a reasonable approximation including heap allocations.
    pub fn estimated_size_bytes(&self) -> usize {
        use std::mem::size_of;

        // Base size of the enum (24 bytes for largest variant discriminant + data)
        let base_size = size_of::<SqlValue>();

        // Add heap allocation size for variable-length types
        match self {
            SqlValue::Character(s) | SqlValue::Varchar(s) => {
// StringValue: base + string length
                // Note: ArcStr strings ≤22 bytes use SSO (no heap)
                // but we still count the string length for accounting purposes
                base_size + s.len()
            }
            SqlValue::Interval(i) => {
                // Interval stores a String internally
                base_size + i.to_string().len()
            }
            SqlValue::Vector(v) => {
                // Vector: base + heap capacity (each f32 is 4 bytes)
                base_size + (v.capacity() * std::mem::size_of::<f32>())
            }
            // Fixed-size types: just the enum size
            SqlValue::Integer(_)
            | SqlValue::Smallint(_)
            | SqlValue::Bigint(_)
            | SqlValue::Unsigned(_)
            | SqlValue::Numeric(_)
            | SqlValue::Float(_)
            | SqlValue::Real(_)
            | SqlValue::Double(_)
            | SqlValue::Boolean(_)
            | SqlValue::Date(_)
            | SqlValue::Time(_)
            | SqlValue::Timestamp(_)
            | SqlValue::Null => base_size,
        }
    }
}
