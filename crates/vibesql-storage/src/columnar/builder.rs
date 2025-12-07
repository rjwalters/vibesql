//! Column builder for constructing typed column data.
//!
//! This module provides the `ColumnBuilder` struct for efficiently building
//! column data with pre-allocated capacity.
//!
//! ## String Interning
//!
//! For string columns, the builder uses a `StringInterner` to deduplicate
//! low-cardinality string values. This provides significant memory savings
//! and enables faster equality comparisons for enum-like columns.

use std::sync::Arc;
use vibesql_types::{Date, Interval, SqlValue, Time, Timestamp};

use super::data::ColumnData;
use super::interner::StringInterner;
use super::types::ColumnTypeClass;

/// Builder for constructing column data with pre-allocated capacity
///
/// The builder pre-allocates storage based on the expected column type,
/// avoiding reallocation during row processing.
///
/// For string columns, uses string interning to deduplicate values when
/// the number of distinct strings is below a threshold (default: 32).
pub(crate) struct ColumnBuilder {
    type_class: ColumnTypeClass,
    int64_values: Vec<i64>,
    float64_values: Vec<f64>,
    string_values: Vec<Arc<str>>,
    bool_values: Vec<bool>,
    date_values: Vec<Date>,
    time_values: Vec<Time>,
    timestamp_values: Vec<Timestamp>,
    interval_values: Vec<Interval>,
    nulls: Vec<bool>,
    /// String interner for deduplicating low-cardinality string columns
    string_interner: StringInterner,
}

impl ColumnBuilder {
    /// Create a new column builder with pre-allocated capacity
    ///
    /// # Arguments
    /// * `type_class` - The column type to build
    /// * `capacity` - Expected number of rows
    pub fn new(type_class: ColumnTypeClass, capacity: usize) -> Self {
        let mut builder = ColumnBuilder {
            type_class,
            int64_values: Vec::new(),
            float64_values: Vec::new(),
            string_values: Vec::new(),
            bool_values: Vec::new(),
            date_values: Vec::new(),
            time_values: Vec::new(),
            timestamp_values: Vec::new(),
            interval_values: Vec::new(),
            nulls: Vec::with_capacity(capacity),
            string_interner: StringInterner::default(),
        };

        // Pre-allocate the appropriate vector based on type
        match type_class {
            ColumnTypeClass::Int64 | ColumnTypeClass::Null => {
                builder.int64_values = Vec::with_capacity(capacity);
            }
            ColumnTypeClass::Float64 => {
                builder.float64_values = Vec::with_capacity(capacity);
            }
            ColumnTypeClass::String => {
                builder.string_values = Vec::with_capacity(capacity);
            }
            ColumnTypeClass::Bool => {
                builder.bool_values = Vec::with_capacity(capacity);
            }
            ColumnTypeClass::Date => {
                builder.date_values = Vec::with_capacity(capacity);
            }
            ColumnTypeClass::Time => {
                builder.time_values = Vec::with_capacity(capacity);
            }
            ColumnTypeClass::Timestamp => {
                builder.timestamp_values = Vec::with_capacity(capacity);
            }
            ColumnTypeClass::Interval => {
                builder.interval_values = Vec::with_capacity(capacity);
            }
            ColumnTypeClass::Vector => {
                // Vector storage is not yet implemented in columnar format
                // Future phase will add specialized vector storage
            }
        }

        builder
    }

    /// Push a value into the column builder
    ///
    /// # Arguments
    /// * `value` - The SQL value to push
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(String)` if the value type doesn't match the column type
    pub fn push(&mut self, value: &SqlValue) -> Result<(), String> {
        match (self.type_class, value) {
            // Int64 handling
            (ColumnTypeClass::Int64 | ColumnTypeClass::Null, SqlValue::Integer(v)) => {
                self.int64_values.push(*v);
                self.nulls.push(false);
            }
            (ColumnTypeClass::Int64 | ColumnTypeClass::Null, SqlValue::Bigint(v)) => {
                self.int64_values.push(*v);
                self.nulls.push(false);
            }
            (ColumnTypeClass::Int64 | ColumnTypeClass::Null, SqlValue::Smallint(v)) => {
                self.int64_values.push(*v as i64);
                self.nulls.push(false);
            }
            (ColumnTypeClass::Int64 | ColumnTypeClass::Null, SqlValue::Null) => {
                self.int64_values.push(0);
                self.nulls.push(true);
            }

            // Float64 handling
            (ColumnTypeClass::Float64, SqlValue::Float(v)) => {
                self.float64_values.push(*v as f64);
                self.nulls.push(false);
            }
            (ColumnTypeClass::Float64, SqlValue::Double(v)) => {
                self.float64_values.push(*v);
                self.nulls.push(false);
            }
            (ColumnTypeClass::Float64, SqlValue::Real(v)) => {
                self.float64_values.push(*v as f64);
                self.nulls.push(false);
            }
            (ColumnTypeClass::Float64, SqlValue::Numeric(v)) => {
                self.float64_values.push(*v);
                self.nulls.push(false);
            }
            (ColumnTypeClass::Float64, SqlValue::Unsigned(v)) => {
                self.float64_values.push(*v as f64);
                self.nulls.push(false);
            }
            (ColumnTypeClass::Float64, SqlValue::Null) => {
                self.float64_values.push(0.0);
                self.nulls.push(true);
            }

            // String handling - use interner for deduplication
            (ColumnTypeClass::String, SqlValue::Varchar(v)) => {
                // Use intern_arc to potentially deduplicate the string
                // Convert ArcStr to Arc<str> for the interner
                let interned = self.string_interner.intern_arc(Arc::from(v.as_str()));
                self.string_values.push(interned);
                self.nulls.push(false);
            }
            (ColumnTypeClass::String, SqlValue::Character(v)) => {
                // Use intern_arc to potentially deduplicate the string
                // Convert ArcStr to Arc<str> for the interner
                let interned = self.string_interner.intern_arc(Arc::from(v.as_str()));
                self.string_values.push(interned);
                self.nulls.push(false);
            }
            (ColumnTypeClass::String, SqlValue::Null) => {
                self.string_values.push(Arc::from(""));
                self.nulls.push(true);
            }

            // Bool handling
            (ColumnTypeClass::Bool, SqlValue::Boolean(v)) => {
                self.bool_values.push(*v);
                self.nulls.push(false);
            }
            (ColumnTypeClass::Bool, SqlValue::Null) => {
                self.bool_values.push(false);
                self.nulls.push(true);
            }

            // Date handling
            (ColumnTypeClass::Date, SqlValue::Date(v)) => {
                self.date_values.push(*v);
                self.nulls.push(false);
            }
            (ColumnTypeClass::Date, SqlValue::Null) => {
                self.date_values.push(Date::new(1970, 1, 1).unwrap());
                self.nulls.push(true);
            }

            // Time handling
            (ColumnTypeClass::Time, SqlValue::Time(v)) => {
                self.time_values.push(*v);
                self.nulls.push(false);
            }
            (ColumnTypeClass::Time, SqlValue::Null) => {
                self.time_values.push(Time::new(0, 0, 0, 0).unwrap());
                self.nulls.push(true);
            }

            // Timestamp handling
            (ColumnTypeClass::Timestamp, SqlValue::Timestamp(v)) => {
                self.timestamp_values.push(*v);
                self.nulls.push(false);
            }
            (ColumnTypeClass::Timestamp, SqlValue::Null) => {
                let date = Date::new(1970, 1, 1).unwrap();
                let time = Time::new(0, 0, 0, 0).unwrap();
                self.timestamp_values.push(Timestamp::new(date, time));
                self.nulls.push(true);
            }

            // Interval handling
            (ColumnTypeClass::Interval, SqlValue::Interval(v)) => {
                self.interval_values.push(v.clone());
                self.nulls.push(false);
            }
            (ColumnTypeClass::Interval, SqlValue::Null) => {
                self.interval_values.push(Interval::new("0".to_string()));
                self.nulls.push(true);
            }

            // Type mismatch
            (expected, got) => {
                return Err(format!(
                    "Column has mixed types: expected {:?}, got {}",
                    expected,
                    got.type_name()
                ));
            }
        }
        Ok(())
    }

    /// Build the final column data from accumulated values
    ///
    /// Consumes the builder and returns the typed column data wrapped in Arc
    /// for zero-copy sharing with the executor layer.
    pub fn build(self) -> ColumnData {
        match self.type_class {
            ColumnTypeClass::Int64 | ColumnTypeClass::Null => ColumnData::Int64 {
                values: Arc::new(self.int64_values),
                nulls: Arc::new(self.nulls),
            },
            ColumnTypeClass::Float64 => ColumnData::Float64 {
                values: Arc::new(self.float64_values),
                nulls: Arc::new(self.nulls),
            },
            ColumnTypeClass::String => ColumnData::String {
                values: Arc::new(self.string_values),
                nulls: Arc::new(self.nulls),
            },
            ColumnTypeClass::Bool => {
                ColumnData::Bool { values: Arc::new(self.bool_values), nulls: Arc::new(self.nulls) }
            }
            ColumnTypeClass::Date => {
                ColumnData::Date { values: Arc::new(self.date_values), nulls: Arc::new(self.nulls) }
            }
            ColumnTypeClass::Time => {
                ColumnData::Time { values: Arc::new(self.time_values), nulls: Arc::new(self.nulls) }
            }
            ColumnTypeClass::Timestamp => ColumnData::Timestamp {
                values: Arc::new(self.timestamp_values),
                nulls: Arc::new(self.nulls),
            },
            ColumnTypeClass::Interval => ColumnData::Interval {
                values: Arc::new(self.interval_values),
                nulls: Arc::new(self.nulls),
            },
            ColumnTypeClass::Vector => {
                // Vector values are stored as Vec<Vec<f32>>
                ColumnData::Vector { values: Arc::new(Vec::new()), nulls: Arc::new(self.nulls) }
            }
        }
    }
}
