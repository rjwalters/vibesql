//! Column type classification for columnar storage.
//!
//! This module provides type classification for efficient pre-allocation
//! of column storage vectors.

use vibesql_types::SqlValue;

/// Column type classification for fast pre-allocation
///
/// This enum categorizes SQL types into columnar storage types,
/// enabling efficient pre-allocation of typed vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnTypeClass {
    /// 64-bit signed integers (Integer, Bigint, Smallint)
    Int64,
    /// 64-bit floating point (Float, Double, Real, Numeric, Unsigned)
    Float64,
    /// Variable-length strings (Varchar, Character)
    String,
    /// Boolean values
    Bool,
    /// Date values
    Date,
    /// Time values
    Time,
    /// Timestamp values
    Timestamp,
    /// Interval values
    Interval,
    /// Vector values (for AI/ML workloads)
    Vector,
    /// NULL (used when column type cannot be inferred)
    Null,
}

impl ColumnTypeClass {
    /// Classify a SqlValue into a column type for pre-allocation
    ///
    /// # Arguments
    /// * `value` - The SQL value to classify
    ///
    /// # Returns
    /// The appropriate column type classification
    pub fn from_sql_value(value: &SqlValue) -> Self {
        match value {
            SqlValue::Integer(_) | SqlValue::Bigint(_) | SqlValue::Smallint(_) => {
                ColumnTypeClass::Int64
            }
            SqlValue::Float(_)
            | SqlValue::Double(_)
            | SqlValue::Real(_)
            | SqlValue::Numeric(_)
            | SqlValue::Unsigned(_) => ColumnTypeClass::Float64,
            SqlValue::Varchar(_) | SqlValue::Character(_) => ColumnTypeClass::String,
            SqlValue::Boolean(_) => ColumnTypeClass::Bool,
            SqlValue::Date(_) => ColumnTypeClass::Date,
            SqlValue::Time(_) => ColumnTypeClass::Time,
            SqlValue::Timestamp(_) => ColumnTypeClass::Timestamp,
            SqlValue::Interval(_) => ColumnTypeClass::Interval,
            SqlValue::Vector(_) => ColumnTypeClass::Vector,
            SqlValue::Null => ColumnTypeClass::Null,
        }
    }
}
