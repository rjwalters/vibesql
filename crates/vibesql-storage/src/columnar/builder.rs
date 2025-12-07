//! Column builder for constructing typed column data.
//!
//! This module provides the `ColumnBuilder` struct for efficiently building
//! column data with pre-allocated capacity.
//!
//! ## String Interning
//!
//! String columns can optionally use string interning to reduce memory usage
//! when columns have many repeated values (enum-like data, status codes, etc.).
//! Enable with `ColumnBuilder::with_string_interning()`.

use std::sync::Arc;
use vibesql_types::{Date, Interval, SqlValue, Time, Timestamp};

use super::data::ColumnData;
use super::string_interner::StringInterner;
use super::types::ColumnTypeClass;

/// Builder for constructing column data with pre-allocated capacity
///
/// The builder pre-allocates storage based on the expected column type,
/// avoiding reallocation during row processing.
///
/// Supports optional string interning for columns with many repeated values.
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
    /// Optional string interner for low-cardinality string columns
    string_interner: Option<StringInterner>,
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
            string_interner: None,
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

    /// Enable string interning for this string column
    ///
    /// String interning caches repeated string values, deduplicating storage.
    /// This is beneficial for columns with limited distinct values.
    ///
    /// # Arguments
    /// * `capacity` - Expected number of unique strings in the column
    ///
    /// # Example
    /// ```ignore
    /// let mut builder = ColumnBuilder::new(ColumnTypeClass::String, 10000);
    /// builder.with_string_interning(500); // Expect ~500 distinct values
    /// ```
    pub fn with_string_interning(mut self, capacity: usize) -> Self {
        self.string_interner = Some(StringInterner::with_capacity(capacity));
        self
    }

    /// Check if string interning is enabled
    pub fn has_string_interning(&self) -> bool {
        self.string_interner.is_some()
    }

    /// Get statistics from string interning (if enabled)
    ///
    /// Returns None if string interning is not enabled.
    pub fn string_interner_stats(&self) -> Option<super::string_interner::InternerStats> {
        self.string_interner.as_ref().map(|interner| interner.stats())
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

            // String handling
            (ColumnTypeClass::String, SqlValue::Varchar(v)) => {
                let interned = if let Some(interner) = &mut self.string_interner {
                    interner.intern(v)
                } else {
                    v.clone()
                };
                self.string_values.push(interned);
                self.nulls.push(false);
            }
            (ColumnTypeClass::String, SqlValue::Character(v)) => {
                let interned = if let Some(interner) = &mut self.string_interner {
                    interner.intern(v)
                } else {
                    v.clone()
                };
                self.string_values.push(interned);
                self.nulls.push(false);
            }
            (ColumnTypeClass::String, SqlValue::Null) => {
                let empty_arc = Arc::from("");
                let interned = if let Some(interner) = &mut self.string_interner {
                    interner.intern("")
                } else {
                    empty_arc
                };
                self.string_values.push(interned);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_interning_basic() {
        let mut builder = ColumnBuilder::new(ColumnTypeClass::String, 100)
            .with_string_interning(10);

        builder.push(&SqlValue::Varchar(Arc::from("status"))).unwrap();
        builder.push(&SqlValue::Varchar(Arc::from("status"))).unwrap();
        builder.push(&SqlValue::Varchar(Arc::from("pending"))).unwrap();
        builder.push(&SqlValue::Varchar(Arc::from("status"))).unwrap();

        let stats = builder.string_interner_stats().unwrap();
        assert_eq!(stats.total_interned, 4);
        assert_eq!(stats.cache_hits, 2); // Second and fourth "status"
        assert_eq!(stats.unique_strings, 2); // "status" and "pending"
    }

    #[test]
    fn test_string_interning_deduplication() {
        let mut builder = ColumnBuilder::new(ColumnTypeClass::String, 100)
            .with_string_interning(5);

        // Push 6 values, 3 unique
        builder.push(&SqlValue::Varchar(Arc::from("active"))).unwrap();
        builder.push(&SqlValue::Varchar(Arc::from("pending"))).unwrap();
        builder.push(&SqlValue::Varchar(Arc::from("active"))).unwrap();
        builder.push(&SqlValue::Varchar(Arc::from("completed"))).unwrap();
        builder.push(&SqlValue::Varchar(Arc::from("pending"))).unwrap();
        builder.push(&SqlValue::Varchar(Arc::from("active"))).unwrap();

        let stats = builder.string_interner_stats().unwrap();
        assert_eq!(stats.unique_strings, 3);
        // Hit rate should be > 0.5 (3 cache hits out of 6 total)
        let hit_rate = stats.cache_hits as f64 / stats.total_interned as f64;
        assert!(hit_rate > 0.4);

        let column = builder.build();
        assert_eq!(column.len(), 6);
    }

    #[test]
    fn test_without_string_interning() {
        let mut builder = ColumnBuilder::new(ColumnTypeClass::String, 100);

        assert!(!builder.has_string_interning());

        builder.push(&SqlValue::Varchar(Arc::from("test"))).unwrap();
        builder.push(&SqlValue::Varchar(Arc::from("test"))).unwrap();

        assert!(builder.string_interner_stats().is_none());

        let column = builder.build();
        assert_eq!(column.len(), 2);
    }

    #[test]
    fn test_string_interning_with_null() {
        let mut builder = ColumnBuilder::new(ColumnTypeClass::String, 100)
            .with_string_interning(5);

        builder.push(&SqlValue::Varchar(Arc::from("active"))).unwrap();
        builder.push(&SqlValue::Null).unwrap();
        builder.push(&SqlValue::Varchar(Arc::from("active"))).unwrap();
        builder.push(&SqlValue::Null).unwrap();

        let stats = builder.string_interner_stats().unwrap();
        // The empty string "" for NULL values should also be interned
        assert!(stats.unique_strings > 0);

        let column = builder.build();
        assert_eq!(column.len(), 4);
        assert!(column.is_null(1));
        assert!(column.is_null(3));
    }

    #[test]
    fn test_enum_like_column() {
        // Simulate a status column with 4 possible values and 1000 rows
        let mut builder = ColumnBuilder::new(ColumnTypeClass::String, 1000)
            .with_string_interning(10);

        let statuses = vec!["pending", "active", "completed", "cancelled"];

        for i in 0..1000 {
            let status = statuses[i % 4];
            builder.push(&SqlValue::Varchar(Arc::from(status))).unwrap();
        }

        let stats = builder.string_interner_stats().unwrap();
        assert_eq!(stats.unique_strings, 4);
        assert_eq!(stats.total_interned, 1000);
        // Should have ~750 cache hits (all but first occurrence of each status)
        assert_eq!(stats.cache_hits, 996); // 1000 - 4 initial insertions

        let column = builder.build();
        assert_eq!(column.len(), 1000);
    }

    #[test]
    fn test_character_type_with_interning() {
        let mut builder = ColumnBuilder::new(ColumnTypeClass::String, 100)
            .with_string_interning(5);

        builder.push(&SqlValue::Character(Arc::from("A"))).unwrap();
        builder.push(&SqlValue::Character(Arc::from("B"))).unwrap();
        builder.push(&SqlValue::Character(Arc::from("A"))).unwrap();

        let stats = builder.string_interner_stats().unwrap();
        assert_eq!(stats.unique_strings, 2);
        assert_eq!(stats.cache_hits, 1);
    }
}
