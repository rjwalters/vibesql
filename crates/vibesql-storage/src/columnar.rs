//! Columnar Storage Format
//!
//! This module provides columnar storage representation for analytical query performance.
//! Unlike row-oriented storage (Vec<Row>), columnar storage groups values by column,
//! enabling SIMD operations and better cache efficiency for analytical workloads.
//!
//! ## Architecture
//!
//! ```text
//! Row-Oriented (current):        Columnar (new):
//! ┌────┬────┬────┐              ┌──────────────┐
//! │id=1│age=25│   │              │ id: [1,2,3]  │
//! ├────┼────┼────┤              ├──────────────┤
//! │id=2│age=30│   │    ──────>  │ age: [25,30,42]
//! ├────┼────┼────┤              ├──────────────┤
//! │id=3│age=42│   │              │ NULL: [F,F,F]│
//! └────┴────┴────┘              └──────────────┘
//! ```
//!
//! ## Performance Benefits
//!
//! - **SIMD**: Process 4-8 values per instruction (vs 1 in row-oriented)
//! - **Cache**: Contiguous memory access (vs jumping between rows)
//! - **Compression**: Column values often have similar patterns
//! - **Projection**: Only load needed columns (vs entire rows)
//!
//! ## Conversion Overhead
//!
//! Conversion between row/columnar has O(n) cost. Use columnar storage for:
//! - Analytical queries (GROUP BY, aggregations, arithmetic)
//! - Large table scans with filtering
//! - Few columns projected (SELECT a, b vs SELECT *)
//!
//! Avoid for:
//! - Point lookups (WHERE id = 123)
//! - Small result sets (<1000 rows)
//! - Many columns projected (SELECT *)

use std::collections::HashMap;
use vibesql_types::{Date, Interval, SqlValue, Time, Timestamp};

use crate::Row;

/// Column type classification for fast pre-allocation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnTypeClass {
    Int64,
    Float64,
    String,
    Bool,
    Date,
    Time,
    Timestamp,
    Interval,
    Null,
}

/// Builder for constructing column data with pre-allocated capacity
struct ColumnBuilder {
    type_class: ColumnTypeClass,
    int64_values: Vec<i64>,
    float64_values: Vec<f64>,
    string_values: Vec<String>,
    bool_values: Vec<bool>,
    date_values: Vec<Date>,
    time_values: Vec<Time>,
    timestamp_values: Vec<Timestamp>,
    interval_values: Vec<Interval>,
    nulls: Vec<bool>,
}

impl ColumnBuilder {
    fn new(type_class: ColumnTypeClass, capacity: usize) -> Self {
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
        }

        builder
    }

    fn push(&mut self, value: &SqlValue) -> Result<(), String> {
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
                self.string_values.push(v.clone());
                self.nulls.push(false);
            }
            (ColumnTypeClass::String, SqlValue::Character(v)) => {
                self.string_values.push(v.clone());
                self.nulls.push(false);
            }
            (ColumnTypeClass::String, SqlValue::Null) => {
                self.string_values.push(String::new());
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

            // Type mismatch - use same error message format as original for compatibility
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

    fn build(self) -> ColumnData {
        match self.type_class {
            ColumnTypeClass::Int64 | ColumnTypeClass::Null => {
                ColumnData::Int64 { values: self.int64_values, nulls: self.nulls }
            }
            ColumnTypeClass::Float64 => {
                ColumnData::Float64 { values: self.float64_values, nulls: self.nulls }
            }
            ColumnTypeClass::String => {
                ColumnData::String { values: self.string_values, nulls: self.nulls }
            }
            ColumnTypeClass::Bool => {
                ColumnData::Bool { values: self.bool_values, nulls: self.nulls }
            }
            ColumnTypeClass::Date => {
                ColumnData::Date { values: self.date_values, nulls: self.nulls }
            }
            ColumnTypeClass::Time => {
                ColumnData::Time { values: self.time_values, nulls: self.nulls }
            }
            ColumnTypeClass::Timestamp => {
                ColumnData::Timestamp { values: self.timestamp_values, nulls: self.nulls }
            }
            ColumnTypeClass::Interval => {
                ColumnData::Interval { values: self.interval_values, nulls: self.nulls }
            }
        }
    }
}

/// Typed column data with NULL bitmap
///
/// Each variant stores a vector of non-NULL values and a separate bitmap
/// indicating which positions are NULL. This design:
/// - Avoids Option<T> overhead (16 bytes vs 8 bytes for f64)
/// - Enables direct SIMD operations on value vectors
/// - Provides O(1) NULL checks via bitmap
#[derive(Debug, Clone)]
pub enum ColumnData {
    Int64 { values: Vec<i64>, nulls: Vec<bool> },
    Float64 { values: Vec<f64>, nulls: Vec<bool> },
    String { values: Vec<String>, nulls: Vec<bool> },
    Bool { values: Vec<bool>, nulls: Vec<bool> },
    Date { values: Vec<Date>, nulls: Vec<bool> },
    Time { values: Vec<Time>, nulls: Vec<bool> },
    Timestamp { values: Vec<Timestamp>, nulls: Vec<bool> },
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

/// Columnar table storage
///
/// Stores data in column-oriented format for analytical query performance.
/// Each column is stored as a typed vector with a separate NULL bitmap.
///
/// # Example
///
/// ```rust,ignore
/// use vibesql_storage::{Row, ColumnarTable};
/// use vibesql_types::SqlValue;
///
/// // Create rows
/// let rows = vec![
///     Row::new(vec![SqlValue::Integer(1), SqlValue::Double(3.14)]),
///     Row::new(vec![SqlValue::Integer(2), SqlValue::Double(2.71)]),
/// ];
///
/// // Convert to columnar
/// let column_names = vec!["id".to_string(), "value".to_string()];
/// let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();
///
/// // Access column data
/// assert_eq!(columnar.row_count(), 2);
/// assert_eq!(columnar.column_count(), 2);
/// ```
#[derive(Debug, Clone)]
pub struct ColumnarTable {
    /// Column data indexed by column name
    columns: HashMap<String, ColumnData>,
    /// Column names in order (for iteration)
    column_names: Vec<String>,
    /// Number of rows
    row_count: usize,
}

impl ColumnarTable {
    /// Create a new empty columnar table
    pub fn new() -> Self {
        ColumnarTable { columns: HashMap::new(), column_names: Vec::new(), row_count: 0 }
    }

    /// Convert row-oriented data to columnar format (optimized single-pass)
    ///
    /// # Arguments
    /// * `rows` - Vector of rows to convert
    /// * `column_names` - Names of columns in order
    ///
    /// # Returns
    /// * `Ok(ColumnarTable)` on success
    /// * `Err(String)` if column count mismatch or incompatible types
    ///
    /// # Performance
    /// O(n * m) single pass through all data
    pub fn from_rows(rows: &[Row], column_names: &[String]) -> Result<Self, String> {
        Self::from_rows_fast(rows, column_names)
    }

    /// Fast single-pass row-to-columnar conversion
    ///
    /// This optimized implementation:
    /// 1. Infers column types from first row (O(m))
    /// 2. Pre-allocates all column vectors (O(m))
    /// 3. Single pass through rows distributing values (O(n*m))
    ///
    /// Total complexity: O(n*m) with better cache locality than multi-pass
    fn from_rows_fast(rows: &[Row], column_names: &[String]) -> Result<Self, String> {
        if rows.is_empty() {
            return Ok(ColumnarTable {
                columns: HashMap::new(),
                column_names: column_names.to_vec(),
                row_count: 0,
            });
        }

        let row_count = rows.len();
        let col_count = column_names.len();

        // Validate first row column count
        if rows[0].len() != col_count {
            return Err(format!(
                "Row 0 has {} columns, expected {}",
                rows[0].len(),
                col_count
            ));
        }

        // Infer column types from first row (or first non-null value)
        let col_types: Vec<_> = (0..col_count)
            .map(|col_idx| {
                rows.iter()
                    .filter_map(|row| row.get(col_idx))
                    .find(|v| !v.is_null())
                    .map(Self::classify_type)
                    .unwrap_or(ColumnTypeClass::Null)
            })
            .collect();

        // Pre-allocate column storage based on inferred types
        let mut column_builders: Vec<ColumnBuilder> = col_types
            .iter()
            .map(|t| ColumnBuilder::new(*t, row_count))
            .collect();

        // Single pass through rows - distribute values to columns
        for (row_idx, row) in rows.iter().enumerate() {
            if row.len() != col_count {
                return Err(format!(
                    "Row {} has {} columns, expected {}",
                    row_idx,
                    row.len(),
                    col_count
                ));
            }

            for (col_idx, value) in row.values.iter().enumerate() {
                column_builders[col_idx].push(value)?;
            }
        }

        // Build final columns HashMap - consume builders
        let mut columns = HashMap::with_capacity(col_count);
        for (col_name, builder) in column_names.iter().zip(column_builders.into_iter()) {
            columns.insert(col_name.clone(), builder.build());
        }

        Ok(ColumnarTable { columns, column_names: column_names.to_vec(), row_count })
    }

    /// Classify SqlValue into a column type for pre-allocation
    fn classify_type(value: &SqlValue) -> ColumnTypeClass {
        match value {
            SqlValue::Integer(_) | SqlValue::Bigint(_) | SqlValue::Smallint(_) => ColumnTypeClass::Int64,
            SqlValue::Float(_) | SqlValue::Double(_) | SqlValue::Real(_)
            | SqlValue::Numeric(_) | SqlValue::Unsigned(_) => ColumnTypeClass::Float64,
            SqlValue::Varchar(_) | SqlValue::Character(_) => ColumnTypeClass::String,
            SqlValue::Boolean(_) => ColumnTypeClass::Bool,
            SqlValue::Date(_) => ColumnTypeClass::Date,
            SqlValue::Time(_) => ColumnTypeClass::Time,
            SqlValue::Timestamp(_) => ColumnTypeClass::Timestamp,
            SqlValue::Interval(_) => ColumnTypeClass::Interval,
            SqlValue::Null => ColumnTypeClass::Null,
        }
    }

    /// Original multi-pass row-to-columnar conversion (kept for reference)
    #[allow(dead_code)]
    fn from_rows_original(rows: &[Row], column_names: &[String]) -> Result<Self, String> {
        if rows.is_empty() {
            return Ok(ColumnarTable {
                columns: HashMap::new(),
                column_names: column_names.to_vec(),
                row_count: 0,
            });
        }

        // Validate column count
        let expected_cols = column_names.len();
        for (i, row) in rows.iter().enumerate() {
            if row.len() != expected_cols {
                return Err(format!(
                    "Row {} has {} columns, expected {}",
                    i,
                    row.len(),
                    expected_cols
                ));
            }
        }

        let row_count = rows.len();
        let mut columns: HashMap<String, ColumnData> = HashMap::new();

        // Process each column
        for (col_idx, col_name) in column_names.iter().enumerate() {
            // Infer column type from first non-NULL value
            let col_type = rows
                .iter()
                .filter_map(|row| {
                    let val = row.get(col_idx)?;
                    if val.is_null() {
                        None
                    } else {
                        Some(val)
                    }
                })
                .next();

            let column_data = match col_type {
                Some(SqlValue::Integer(_))
                | Some(SqlValue::Bigint(_))
                | Some(SqlValue::Smallint(_)) => {
                    let mut values = Vec::with_capacity(row_count);
                    let mut nulls = Vec::with_capacity(row_count);

                    for row in rows {
                        match row.get(col_idx) {
                            Some(SqlValue::Integer(v)) => {
                                values.push(*v);
                                nulls.push(false);
                            }
                            Some(SqlValue::Bigint(v)) => {
                                values.push(*v);
                                nulls.push(false);
                            }
                            Some(SqlValue::Smallint(v)) => {
                                values.push(*v as i64);
                                nulls.push(false);
                            }
                            Some(SqlValue::Null) | None => {
                                values.push(0); // Placeholder for NULL
                                nulls.push(true);
                            }
                            Some(other) => {
                                return Err(format!(
                                    "Column '{}' has mixed types: expected integer, got {}",
                                    col_name,
                                    other.type_name()
                                ));
                            }
                        }
                    }

                    ColumnData::Int64 { values, nulls }
                }

                Some(SqlValue::Float(_))
                | Some(SqlValue::Double(_))
                | Some(SqlValue::Real(_))
                | Some(SqlValue::Numeric(_))
                | Some(SqlValue::Unsigned(_)) => {
                    let mut values = Vec::with_capacity(row_count);
                    let mut nulls = Vec::with_capacity(row_count);

                    for row in rows {
                        match row.get(col_idx) {
                            Some(SqlValue::Float(v)) => {
                                values.push(*v as f64);
                                nulls.push(false);
                            }
                            Some(SqlValue::Double(v)) => {
                                values.push(*v);
                                nulls.push(false);
                            }
                            Some(SqlValue::Real(v)) => {
                                values.push(*v as f64);
                                nulls.push(false);
                            }
                            Some(SqlValue::Numeric(v)) => {
                                values.push(*v);
                                nulls.push(false);
                            }
                            Some(SqlValue::Unsigned(v)) => {
                                values.push(*v as f64);
                                nulls.push(false);
                            }
                            Some(SqlValue::Null) | None => {
                                values.push(0.0); // Placeholder for NULL
                                nulls.push(true);
                            }
                            Some(other) => {
                                return Err(format!(
                                    "Column '{}' has mixed types: expected float, got {}",
                                    col_name,
                                    other.type_name()
                                ));
                            }
                        }
                    }

                    ColumnData::Float64 { values, nulls }
                }

                Some(SqlValue::Varchar(_)) | Some(SqlValue::Character(_)) => {
                    let mut values = Vec::with_capacity(row_count);
                    let mut nulls = Vec::with_capacity(row_count);

                    for row in rows {
                        match row.get(col_idx) {
                            Some(SqlValue::Varchar(v)) => {
                                values.push(v.clone());
                                nulls.push(false);
                            }
                            Some(SqlValue::Character(v)) => {
                                values.push(v.clone());
                                nulls.push(false);
                            }
                            Some(SqlValue::Null) | None => {
                                values.push(String::new()); // Placeholder for NULL
                                nulls.push(true);
                            }
                            Some(other) => {
                                return Err(format!(
                                    "Column '{}' has mixed types: expected string, got {}",
                                    col_name,
                                    other.type_name()
                                ));
                            }
                        }
                    }

                    ColumnData::String { values, nulls }
                }

                Some(SqlValue::Boolean(_)) => {
                    let mut values = Vec::with_capacity(row_count);
                    let mut nulls = Vec::with_capacity(row_count);

                    for row in rows {
                        match row.get(col_idx) {
                            Some(SqlValue::Boolean(v)) => {
                                values.push(*v);
                                nulls.push(false);
                            }
                            Some(SqlValue::Null) | None => {
                                values.push(false); // Placeholder for NULL
                                nulls.push(true);
                            }
                            Some(other) => {
                                return Err(format!(
                                    "Column '{}' has mixed types: expected boolean, got {}",
                                    col_name,
                                    other.type_name()
                                ));
                            }
                        }
                    }

                    ColumnData::Bool { values, nulls }
                }

                Some(SqlValue::Date(_)) => {
                    let mut values = Vec::with_capacity(row_count);
                    let mut nulls = Vec::with_capacity(row_count);

                    for row in rows {
                        match row.get(col_idx) {
                            Some(SqlValue::Date(v)) => {
                                values.push(*v);
                                nulls.push(false);
                            }
                            Some(SqlValue::Null) | None => {
                                values.push(Date::new(1970, 1, 1).unwrap());
                                nulls.push(true);
                            }
                            Some(other) => {
                                return Err(format!(
                                    "Column '{}' has mixed types: expected date, got {}",
                                    col_name,
                                    other.type_name()
                                ));
                            }
                        }
                    }

                    ColumnData::Date { values, nulls }
                }

                Some(SqlValue::Time(_)) => {
                    let mut values = Vec::with_capacity(row_count);
                    let mut nulls = Vec::with_capacity(row_count);

                    for row in rows {
                        match row.get(col_idx) {
                            Some(SqlValue::Time(v)) => {
                                values.push(*v);
                                nulls.push(false);
                            }
                            Some(SqlValue::Null) | None => {
                                values.push(Time::new(0, 0, 0, 0).unwrap());
                                nulls.push(true);
                            }
                            Some(other) => {
                                return Err(format!(
                                    "Column '{}' has mixed types: expected time, got {}",
                                    col_name,
                                    other.type_name()
                                ));
                            }
                        }
                    }

                    ColumnData::Time { values, nulls }
                }

                Some(SqlValue::Timestamp(_)) => {
                    let mut values = Vec::with_capacity(row_count);
                    let mut nulls = Vec::with_capacity(row_count);

                    for row in rows {
                        match row.get(col_idx) {
                            Some(SqlValue::Timestamp(v)) => {
                                values.push(*v);
                                nulls.push(false);
                            }
                            Some(SqlValue::Null) | None => {
                                let date = Date::new(1970, 1, 1).unwrap();
                                let time = Time::new(0, 0, 0, 0).unwrap();
                                values.push(Timestamp::new(date, time));
                                nulls.push(true);
                            }
                            Some(other) => {
                                return Err(format!(
                                    "Column '{}' has mixed types: expected timestamp, got {}",
                                    col_name,
                                    other.type_name()
                                ));
                            }
                        }
                    }

                    ColumnData::Timestamp { values, nulls }
                }

                Some(SqlValue::Interval(_)) => {
                    let mut values = Vec::with_capacity(row_count);
                    let mut nulls = Vec::with_capacity(row_count);

                    for row in rows {
                        match row.get(col_idx) {
                            Some(SqlValue::Interval(v)) => {
                                values.push(v.clone());
                                nulls.push(false);
                            }
                            Some(SqlValue::Null) | None => {
                                values.push(Interval::new("0".to_string()));
                                nulls.push(true);
                            }
                            Some(other) => {
                                return Err(format!(
                                    "Column '{}' has mixed types: expected interval, got {}",
                                    col_name,
                                    other.type_name()
                                ));
                            }
                        }
                    }

                    ColumnData::Interval { values, nulls }
                }

                None | Some(SqlValue::Null) => {
                    // All NULL column - default to Int64
                    ColumnData::Int64 {
                        values: vec![0; row_count],
                        nulls: vec![true; row_count],
                    }
                }
            };

            columns.insert(col_name.clone(), column_data);
        }

        Ok(ColumnarTable { columns, column_names: column_names.to_vec(), row_count })
    }

    /// Convert columnar data back to row-oriented format
    ///
    /// # Returns
    /// Vector of rows reconstructed from columnar data
    ///
    /// # Performance
    /// O(n * m) where n = rows, m = columns
    pub fn to_rows(&self) -> Vec<Row> {
        let mut rows = Vec::with_capacity(self.row_count);

        for row_idx in 0..self.row_count {
            let mut values = Vec::with_capacity(self.column_names.len());

            for col_name in &self.column_names {
                if let Some(column) = self.columns.get(col_name) {
                    values.push(column.get(row_idx));
                } else {
                    values.push(SqlValue::Null);
                }
            }

            rows.push(Row::new(values));
        }

        rows
    }

    /// Get the number of rows
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.column_names.len()
    }

    /// Get column data by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnData> {
        self.columns.get(name)
    }

    /// Get all column names
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }
}

impl Default for ColumnarTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_table() {
        let rows: Vec<Row> = vec![];
        let column_names = vec!["id".to_string(), "name".to_string()];
        let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();

        assert_eq!(columnar.row_count(), 0);
        assert_eq!(columnar.column_count(), 2);
    }

    #[test]
    fn test_int64_column() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Null]),
        ];

        let column_names = vec!["id".to_string(), "value".to_string()];
        let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();

        assert_eq!(columnar.row_count(), 3);
        assert_eq!(columnar.column_count(), 2);

        // Check id column
        let id_col = columnar.get_column("id").unwrap();
        assert_eq!(id_col.len(), 3);
        assert!(!id_col.is_null(0));
        assert!(!id_col.is_null(1));
        assert!(!id_col.is_null(2));

        // Check value column
        let value_col = columnar.get_column("value").unwrap();
        assert!(!value_col.is_null(0));
        assert!(!value_col.is_null(1));
        assert!(value_col.is_null(2)); // NULL value
    }

    #[test]
    fn test_float64_column() {
        let rows = vec![
            Row::new(vec![SqlValue::Double(3.14), SqlValue::Float(1.5)]),
            Row::new(vec![SqlValue::Double(2.71), SqlValue::Null]),
        ];

        let column_names = vec!["pi".to_string(), "value".to_string()];
        let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();

        assert_eq!(columnar.row_count(), 2);

        let pi_col = columnar.get_column("pi").unwrap();
        assert_eq!(pi_col.len(), 2);
        assert!(!pi_col.is_null(0));
        assert!(!pi_col.is_null(1));

        let value_col = columnar.get_column("value").unwrap();
        assert!(!value_col.is_null(0));
        assert!(value_col.is_null(1));
    }

    #[test]
    fn test_string_column() {
        let rows = vec![
            Row::new(vec![SqlValue::Varchar("Alice".to_string())]),
            Row::new(vec![SqlValue::Varchar("Bob".to_string())]),
            Row::new(vec![SqlValue::Null]),
        ];

        let column_names = vec!["name".to_string()];
        let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();

        assert_eq!(columnar.row_count(), 3);

        let name_col = columnar.get_column("name").unwrap();
        assert!(!name_col.is_null(0));
        assert!(!name_col.is_null(1));
        assert!(name_col.is_null(2));
    }

    #[test]
    fn test_bool_column() {
        let rows = vec![
            Row::new(vec![SqlValue::Boolean(true)]),
            Row::new(vec![SqlValue::Boolean(false)]),
            Row::new(vec![SqlValue::Null]),
        ];

        let column_names = vec!["flag".to_string()];
        let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();

        assert_eq!(columnar.row_count(), 3);

        let flag_col = columnar.get_column("flag").unwrap();
        assert!(!flag_col.is_null(0));
        assert!(!flag_col.is_null(1));
        assert!(flag_col.is_null(2));
    }

    #[test]
    fn test_to_rows_round_trip() {
        let original_rows = vec![
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Double(3.14),
                SqlValue::Varchar("Alice".to_string()),
            ]),
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Null,
                SqlValue::Varchar("Bob".to_string()),
            ]),
            Row::new(vec![
                SqlValue::Integer(3),
                SqlValue::Double(2.71),
                SqlValue::Null,
            ]),
        ];

        let column_names = vec!["id".to_string(), "value".to_string(), "name".to_string()];

        // Convert to columnar
        let columnar = ColumnarTable::from_rows(&original_rows, &column_names).unwrap();

        // Convert back to rows
        let reconstructed = columnar.to_rows();

        // Verify round trip
        assert_eq!(reconstructed.len(), original_rows.len());
        for (orig, recon) in original_rows.iter().zip(reconstructed.iter()) {
            assert_eq!(orig.len(), recon.len());
            for i in 0..orig.len() {
                match (orig.get(i), recon.get(i)) {
                    (Some(SqlValue::Integer(a)), Some(SqlValue::Integer(b))) => {
                        assert_eq!(a, b);
                    }
                    (Some(SqlValue::Double(a)), Some(SqlValue::Double(b))) => {
                        assert!((a - b).abs() < 1e-10);
                    }
                    (Some(SqlValue::Varchar(a)), Some(SqlValue::Varchar(b))) => {
                        assert_eq!(a, b);
                    }
                    (Some(SqlValue::Null), Some(SqlValue::Null)) => {}
                    (a, b) => {
                        panic!("Mismatch at column {}: {:?} vs {:?}", i, a, b);
                    }
                }
            }
        }
    }

    #[test]
    fn test_mixed_types_error() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Double(2.5)]), // Type mismatch
        ];

        let column_names = vec!["value".to_string()];
        let result = ColumnarTable::from_rows(&rows, &column_names);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("mixed types"));
    }

    #[test]
    fn test_column_count_mismatch() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(2)]), // Missing column
        ];

        let column_names = vec!["id".to_string(), "value".to_string()];
        let result = ColumnarTable::from_rows(&rows, &column_names);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("has 1 columns, expected 2"));
    }
}
