//! Apache Arrow RecordBatch adapter for SIMD vectorized execution
//!
//! This module provides conversion between vibesql's row-based format and Arrow's
//! columnar RecordBatch format, enabling SIMD acceleration through Arrow compute kernels.
//!
//! Note: This module is experimental/research code.

#![allow(dead_code)]

use crate::errors::ExecutorError;
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Float64Array, Int64Array,
    PrimitiveBuilder, StringArray, StringBuilder, TimestampMicrosecondArray,
};
use arrow::datatypes::{
    DataType, Date32Type, Field, Float64Type, Int64Type, Schema, TimeUnit, TimestampMicrosecondType,
};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use vibesql_storage::Row;
use vibesql_types::{Date, SqlValue, Time, Timestamp};

/// Default batch size for vectorized operations
/// Tuned for balance between memory usage and SIMD efficiency
pub const DEFAULT_BATCH_SIZE: usize = 1024;

/// Batch size optimized for pure table scans (larger = better SIMD utilization)
pub const SCAN_BATCH_SIZE: usize = 4096;

/// Batch size optimized for joins (smaller = less memory pressure)
pub const JOIN_BATCH_SIZE: usize = 512;

/// Batch size optimized for L1 cache (32KB typical)
/// Assumes ~8 bytes per value, 4 columns: 32KB / (8 * 4) = 1024 rows
pub const L1_CACHE_BATCH_SIZE: usize = 1024;

/// Batch size optimized for L2 cache (256KB typical)
/// Allows larger batches for better SIMD throughput
pub const L2_CACHE_BATCH_SIZE: usize = 2048;

/// Query context for adaptive batch sizing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryContext {
    /// Pure table scan with filter
    Scan,
    /// Join operation (reduce memory pressure)
    Join,
    /// Aggregation with GROUP BY
    GroupBy,
    /// Default/unknown context
    Default,
}

impl QueryContext {
    /// Get recommended batch size for this query context
    pub fn recommended_batch_size(&self) -> usize {
        match self {
            QueryContext::Scan => SCAN_BATCH_SIZE,
            QueryContext::Join => JOIN_BATCH_SIZE,
            QueryContext::GroupBy => L1_CACHE_BATCH_SIZE,
            QueryContext::Default => DEFAULT_BATCH_SIZE,
        }
    }
}

/// Convert a batch of rows to an Arrow RecordBatch
///
/// This performs the key transformation from row-oriented to column-oriented layout.
pub fn rows_to_record_batch(
    rows: &[Row],
    column_names: &[String],
) -> Result<RecordBatch, ExecutorError> {
    rows_to_record_batch_with_columns(rows, column_names, None)
}

/// Convert a batch of rows to an Arrow RecordBatch with column pruning
///
/// This optimized version only converts the columns specified in `column_indices`,
/// reducing conversion overhead and memory usage for queries that don't reference
/// all columns.
///
/// # Arguments
/// * `rows` - The rows to convert
/// * `column_names` - Names for all columns (full schema)
/// * `column_indices` - Optional set of column indices to include. If None, includes all columns.
///
/// # Performance Benefits
/// - Reduces row→columnar conversion time by ~N where N is the pruning ratio
/// - Smaller RecordBatch footprint improves cache utilization
/// - Less memory allocation overhead
pub fn rows_to_record_batch_with_columns(
    rows: &[Row],
    column_names: &[String],
    column_indices: Option<&[usize]>,
) -> Result<RecordBatch, ExecutorError> {
    if rows.is_empty() {
        return Err(ExecutorError::ColumnarLengthMismatch {
            expected: 1,
            actual: 0,
            context: "Cannot create RecordBatch from empty rows".to_string(),
        });
    }

    // Determine which columns to convert
    let indices: Vec<usize> = match column_indices {
        Some(cols) => cols.to_vec(),
        None => (0..rows[0].values.len()).collect(),
    };

    if indices.is_empty() {
        return Err(ExecutorError::ColumnarColumnNotFound { column_index: 0, batch_columns: 0 });
    }

    // Build schema for selected columns only
    let mut schema_fields = Vec::with_capacity(indices.len());
    for &col_idx in &indices {
        let name = column_names.get(col_idx).cloned().unwrap_or_else(|| format!("col_{}", col_idx));

        let data_type = match &rows[0].values.get(col_idx) {
            Some(SqlValue::Integer(_))
            | Some(SqlValue::Bigint(_))
            | Some(SqlValue::Smallint(_)) => DataType::Int64,
            Some(SqlValue::Float(_))
            | Some(SqlValue::Real(_))
            | Some(SqlValue::Double(_))
            | Some(SqlValue::Numeric(_)) => DataType::Float64,
            Some(SqlValue::Character(_)) | Some(SqlValue::Varchar(_)) => DataType::Utf8,
            Some(SqlValue::Boolean(_)) => DataType::Boolean,
            Some(SqlValue::Date(_)) => DataType::Date32,
            Some(SqlValue::Timestamp(_)) => DataType::Timestamp(TimeUnit::Microsecond, None),
            Some(SqlValue::Null) => DataType::Int64,
            _ => {
                return Err(ExecutorError::UnsupportedArrayType {
                    operation: format!("SIMD conversion at column {}", col_idx),
                    array_type: format!("{:?}", rows[0].values.get(col_idx)),
                })
            }
        };

        schema_fields.push(Field::new(name, data_type, true));
    }
    let schema = Schema::new(schema_fields);

    // Build columns for selected indices only
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(indices.len());
    for (field_idx, &col_idx) in indices.iter().enumerate() {
        let field = schema.field(field_idx);
        let array = build_column_array(rows, col_idx, field.data_type())?;
        columns.push(array);
    }

    RecordBatch::try_new(Arc::new(schema), columns).map_err(|e| {
        ExecutorError::SimdOperationFailed {
            operation: "RecordBatch creation".to_string(),
            reason: e.to_string(),
        }
    })
}

/// Convert RecordBatch back to row format
pub fn record_batch_to_rows(batch: &RecordBatch) -> Result<Vec<Row>, ExecutorError> {
    let num_rows = batch.num_rows();
    let num_columns = batch.num_columns();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut values = Vec::with_capacity(num_columns);

        for col_idx in 0..num_columns {
            let array = batch.column(col_idx);
            let value = arrow_value_to_sql(array, row_idx)?;
            values.push(value);
        }

        rows.push(Row::from_vec(values));
    }

    Ok(rows)
}

/// Convert Date to days since Unix epoch (1970-01-01)
pub(super) fn date_to_days_since_epoch(date: &Date) -> i32 {
    // Simple calculation: days since Unix epoch
    // Note: This is a simplified calculation and doesn't account for all leap years perfectly
    let year_days = (date.year - 1970) * 365;
    let leap_years =
        ((date.year - 1969) / 4) - ((date.year - 1901) / 100) + ((date.year - 1601) / 400);
    let month_days: i32 = match date.month {
        1 => 0,
        2 => 31,
        3 => 59,
        4 => 90,
        5 => 120,
        6 => 151,
        7 => 181,
        8 => 212,
        9 => 243,
        10 => 273,
        11 => 304,
        12 => 334,
        _ => 0,
    };

    // Add leap day if after February in a leap year
    let is_leap = date.year % 4 == 0 && (date.year % 100 != 0 || date.year % 400 == 0);
    let leap_adjustment = if is_leap && date.month > 2 { 1 } else { 0 };

    year_days + leap_years + month_days + leap_adjustment + (date.day as i32) - 1
}

/// Convert days since Unix epoch to Date
pub(super) fn days_since_epoch_to_date(days: i32) -> Date {
    // Simplified conversion: start from 1970-01-01 and count forward
    let mut year = 1970;
    let mut remaining_days = days;

    // Handle years
    loop {
        let year_days =
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { 366 } else { 365 };
        if remaining_days < year_days {
            break;
        }
        remaining_days -= year_days;
        year += 1;
    }

    // Handle months
    let is_leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_lengths = if is_leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1;
    for &days_in_month in &month_lengths {
        if remaining_days < days_in_month {
            break;
        }
        remaining_days -= days_in_month;
        month += 1;
    }

    let day = remaining_days + 1;

    Date::new(year, month as u8, day as u8).unwrap_or_else(|_| Date::new(1970, 1, 1).unwrap())
}

/// Convert Timestamp to microseconds since Unix epoch
pub(super) fn timestamp_to_microseconds(ts: &Timestamp) -> i64 {
    let days = date_to_days_since_epoch(&ts.date);
    let day_micros = days as i64 * 86_400_000_000; // 24 * 60 * 60 * 1_000_000
    let time_micros = (ts.time.hour as i64) * 3_600_000_000
        + (ts.time.minute as i64) * 60_000_000
        + (ts.time.second as i64) * 1_000_000
        + (ts.time.nanosecond as i64) / 1_000; // Convert nanoseconds to microseconds
    day_micros + time_micros
}

/// Convert microseconds since Unix epoch to Timestamp
pub(super) fn microseconds_to_timestamp(micros: i64) -> Timestamp {
    let days = (micros / 86_400_000_000) as i32;
    let remaining_micros = micros % 86_400_000_000;

    let date = days_since_epoch_to_date(days);

    let hours = (remaining_micros / 3_600_000_000) as u8;
    let remaining_micros = remaining_micros % 3_600_000_000;
    let minutes = (remaining_micros / 60_000_000) as u8;
    let remaining_micros = remaining_micros % 60_000_000;
    let seconds = (remaining_micros / 1_000_000) as u8;
    let nanoseconds = ((remaining_micros % 1_000_000) * 1_000) as u32; // Convert microseconds to nanoseconds

    let time = Time::new(hours, minutes, seconds, nanoseconds)
        .unwrap_or_else(|_| Time::new(0, 0, 0, 0).unwrap());

    Timestamp::new(date, time)
}

/// Infer Arrow schema from vibesql Row
fn infer_schema_from_row(row: &Row, column_names: &[String]) -> Result<Schema, ExecutorError> {
    let fields: Result<Vec<_>, _> = row
        .values
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            let name = column_names.get(idx).cloned().unwrap_or_else(|| format!("col_{}", idx));

            let data_type = match value {
                SqlValue::Integer(_) | SqlValue::Bigint(_) | SqlValue::Smallint(_) => {
                    DataType::Int64
                }
                SqlValue::Float(_)
                | SqlValue::Real(_)
                | SqlValue::Double(_)
                | SqlValue::Numeric(_) => DataType::Float64,
                SqlValue::Character(_) | SqlValue::Varchar(_) => DataType::Utf8,
                SqlValue::Boolean(_) => DataType::Boolean,
                SqlValue::Date(_) => DataType::Date32,
                SqlValue::Timestamp(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
                SqlValue::Null => DataType::Int64, // Default to Int64 for nulls
                _ => {
                    return Err(ExecutorError::UnsupportedArrayType {
                        operation: "SIMD schema inference".to_string(),
                        array_type: format!("{:?}", value),
                    })
                }
            };

            Ok(Field::new(name, data_type, true))
        })
        .collect();

    Ok(Schema::new(fields?))
}

/// Build a columnar array from row data
fn build_column_array(
    rows: &[Row],
    col_idx: usize,
    data_type: &DataType,
) -> Result<ArrayRef, ExecutorError> {
    match data_type {
        DataType::Int64 => {
            let mut builder = PrimitiveBuilder::<Int64Type>::new();
            for row in rows {
                match &row.values[col_idx] {
                    SqlValue::Integer(v) => builder.append_value(*v),
                    SqlValue::Bigint(v) => builder.append_value(*v),
                    SqlValue::Smallint(v) => builder.append_value(*v as i64),
                    SqlValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = PrimitiveBuilder::<Float64Type>::new();
            for row in rows {
                match &row.values[col_idx] {
                    SqlValue::Float(v) => builder.append_value(*v as f64),
                    SqlValue::Real(v) => builder.append_value(*v as f64),
                    SqlValue::Double(v) => builder.append_value(*v),
                    SqlValue::Numeric(v) => builder.append_value(*v),
                    SqlValue::Integer(v) => builder.append_value(*v as f64),
                    SqlValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for row in rows {
                match &row.values[col_idx] {
                    SqlValue::Character(s) | SqlValue::Varchar(s) => builder.append_value(s),
                    SqlValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for row in rows {
                match &row.values[col_idx] {
                    SqlValue::Boolean(b) => builder.append_value(*b),
                    SqlValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let mut builder = PrimitiveBuilder::<Date32Type>::new();
            for row in rows {
                match &row.values[col_idx] {
                    SqlValue::Date(d) => builder.append_value(date_to_days_since_epoch(d)),
                    SqlValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let mut builder = PrimitiveBuilder::<TimestampMicrosecondType>::new();
            for row in rows {
                match &row.values[col_idx] {
                    SqlValue::Timestamp(ts) => builder.append_value(timestamp_to_microseconds(ts)),
                    SqlValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(ExecutorError::UnsupportedArrayType {
            operation: "build_column_array".to_string(),
            array_type: format!("{:?}", data_type),
        }),
    }
}

/// Convert Arrow array value to SqlValue
fn arrow_value_to_sql(array: &dyn Array, idx: usize) -> Result<SqlValue, ExecutorError> {
    if array.is_null(idx) {
        return Ok(SqlValue::Null);
    }

    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                ExecutorError::ArrowDowncastError {
                    expected_type: "Int64Array".to_string(),
                    context: "arrow_value_to_sql".to_string(),
                }
            })?;
            Ok(SqlValue::Integer(arr.value(idx)))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                ExecutorError::ArrowDowncastError {
                    expected_type: "Float64Array".to_string(),
                    context: "arrow_value_to_sql".to_string(),
                }
            })?;
            Ok(SqlValue::Double(arr.value(idx)))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                ExecutorError::ArrowDowncastError {
                    expected_type: "StringArray".to_string(),
                    context: "arrow_value_to_sql".to_string(),
                }
            })?;
            Ok(SqlValue::Varchar(arr.value(idx).into()))
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                ExecutorError::ArrowDowncastError {
                    expected_type: "BooleanArray".to_string(),
                    context: "arrow_value_to_sql".to_string(),
                }
            })?;
            Ok(SqlValue::Boolean(arr.value(idx)))
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().ok_or_else(|| {
                ExecutorError::ArrowDowncastError {
                    expected_type: "Date32Array".to_string(),
                    context: "arrow_value_to_sql".to_string(),
                }
            })?;
            let days = arr.value(idx);
            Ok(SqlValue::Date(days_since_epoch_to_date(days)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let arr =
                array.as_any().downcast_ref::<TimestampMicrosecondArray>().ok_or_else(|| {
                    ExecutorError::ArrowDowncastError {
                        expected_type: "TimestampMicrosecondArray".to_string(),
                        context: "arrow_value_to_sql".to_string(),
                    }
                })?;
            let micros = arr.value(idx);
            Ok(SqlValue::Timestamp(microseconds_to_timestamp(micros)))
        }
        _ => Err(ExecutorError::UnsupportedArrayType {
            operation: "arrow_value_to_sql conversion".to_string(),
            array_type: format!("{:?}", array.data_type()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rows_to_record_batch_basic() {
        let rows = vec![
            Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("hello"))]),
            Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("world"))]),
        ];

        let column_names = vec!["id".to_string(), "name".to_string()];
        let batch = rows_to_record_batch(&rows, &column_names).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_round_trip_conversion() {
        let original_rows = vec![
            Row::from_vec(vec![
                    SqlValue::Integer(1),
                    SqlValue::Double(3.5),
                    SqlValue::Varchar(arcstr::ArcStr::from("test")),
                    SqlValue::Boolean(true),
                ]),
            Row::from_vec(vec![
                    SqlValue::Integer(2),
                    SqlValue::Double(2.5),
                    SqlValue::Varchar(arcstr::ArcStr::from("data")),
                    SqlValue::Boolean(false),
                ]),
        ];

        let column_names =
            vec!["id".to_string(), "value".to_string(), "name".to_string(), "flag".to_string()];

        let batch = rows_to_record_batch(&original_rows, &column_names).unwrap();
        let converted_rows = record_batch_to_rows(&batch).unwrap();

        assert_eq!(original_rows.len(), converted_rows.len());
        assert_eq!(original_rows[0].values.len(), converted_rows[0].values.len());
    }

    #[test]
    fn test_column_pruning() {
        // Test that column pruning only converts specified columns
        let rows = vec![
            Row::from_vec(vec![
                    SqlValue::Integer(1),
                    SqlValue::Double(3.5),
                    SqlValue::Varchar(arcstr::ArcStr::from("test")),
                    SqlValue::Boolean(true),
                ]),
            Row::from_vec(vec![
                    SqlValue::Integer(2),
                    SqlValue::Double(2.5),
                    SqlValue::Varchar(arcstr::ArcStr::from("data")),
                    SqlValue::Boolean(false),
                ]),
        ];

        let column_names =
            vec!["id".to_string(), "value".to_string(), "name".to_string(), "flag".to_string()];

        // Only convert columns 0 and 2 (id and name)
        let column_indices = vec![0, 2];
        let batch =
            rows_to_record_batch_with_columns(&rows, &column_names, Some(&column_indices)).unwrap();

        // Batch should only have 2 columns
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 2);

        // Verify schema only contains selected columns
        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_adaptive_batch_sizing() {
        // Test that different query contexts recommend different batch sizes
        assert_eq!(QueryContext::Scan.recommended_batch_size(), SCAN_BATCH_SIZE);
        assert_eq!(QueryContext::Join.recommended_batch_size(), JOIN_BATCH_SIZE);
        assert_eq!(QueryContext::GroupBy.recommended_batch_size(), L1_CACHE_BATCH_SIZE);
        assert_eq!(QueryContext::Default.recommended_batch_size(), DEFAULT_BATCH_SIZE);

        // Verify scan batch size is larger (better SIMD utilization)
        assert!(SCAN_BATCH_SIZE > DEFAULT_BATCH_SIZE);

        // Verify join batch size is smaller (less memory pressure)
        assert!(JOIN_BATCH_SIZE < DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn test_date_conversion() {
        use vibesql_types::Date;

        let date1 = Date::new(2024, 1, 15).unwrap();
        let date2 = Date::new(2024, 12, 25).unwrap();

        let rows = vec![
            Row::from_vec(vec![SqlValue::Date(date1), SqlValue::Integer(1)]),
            Row::from_vec(vec![SqlValue::Date(date2), SqlValue::Integer(2)]),
        ];

        let column_names = vec!["date_col".to_string(), "id".to_string()];
        let batch = rows_to_record_batch(&rows, &column_names).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        // Test round-trip conversion
        let converted_rows = record_batch_to_rows(&batch).unwrap();
        assert_eq!(rows.len(), converted_rows.len());
    }

    #[test]
    fn test_timestamp_conversion() {
        use vibesql_types::{Date, Time, Timestamp};

        let date = Date::new(2024, 6, 15).unwrap();
        let time = Time::new(14, 30, 45, 123456000).unwrap(); // 123.456 milliseconds in nanoseconds
        let ts = Timestamp::new(date, time);

        let rows = vec![Row::from_vec(vec![SqlValue::Timestamp(ts), SqlValue::Integer(1)])];

        let column_names = vec!["ts_col".to_string(), "id".to_string()];
        let batch = rows_to_record_batch(&rows, &column_names).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);

        // Test round-trip conversion
        let converted_rows = record_batch_to_rows(&batch).unwrap();
        assert_eq!(rows.len(), converted_rows.len());
    }

    #[test]
    fn test_date_timestamp_round_trip() {
        use vibesql_types::{Date, Time, Timestamp};

        let date1 = Date::new(2020, 1, 1).unwrap();
        let date2 = Date::new(2025, 12, 31).unwrap();
        let ts_date = Date::new(2024, 7, 4).unwrap();
        let ts_time = Time::new(10, 20, 30, 500000000).unwrap(); // 500 milliseconds in nanoseconds
        let ts = Timestamp::new(ts_date, ts_time);

        let original_rows = vec![
            Row::from_vec(vec![
                    SqlValue::Date(date1),
                    SqlValue::Timestamp(ts),
                    SqlValue::Integer(100),
                ]),
            Row::from_vec(vec![
                    SqlValue::Date(date2),
                    SqlValue::Timestamp(ts),
                    SqlValue::Integer(200),
                ]),
        ];

        let column_names = vec!["date_col".to_string(), "ts_col".to_string(), "id".to_string()];

        let batch = rows_to_record_batch(&original_rows, &column_names).unwrap();
        let converted_rows = record_batch_to_rows(&batch).unwrap();

        assert_eq!(original_rows.len(), converted_rows.len());
        assert_eq!(original_rows[0].values.len(), converted_rows[0].values.len());
    }

    #[test]
    fn test_null_values_in_numeric_columns() {
        // Test that NULL values are preserved in Int64 and Float64 columns
        let original_rows = vec![
            Row::from_vec(vec![SqlValue::Integer(100), SqlValue::Double(3.5)]),
            Row::from_vec(vec![
                    SqlValue::Null, // NULL integer
                    SqlValue::Double(2.5),
                ]),
            Row::from_vec(vec![
                    SqlValue::Integer(200),
                    SqlValue::Null, // NULL float
                ]),
            Row::from_vec(vec![
                    SqlValue::Null, // NULL integer
                    SqlValue::Null, // NULL float
                ]),
        ];

        let column_names = vec!["int_col".to_string(), "float_col".to_string()];
        let batch = rows_to_record_batch(&original_rows, &column_names).unwrap();

        // Verify batch structure
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 2);

        // Verify NULL values are tracked
        let int_array = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(!int_array.is_null(0)); // First row is not NULL
        assert!(int_array.is_null(1)); // Second row is NULL
        assert!(!int_array.is_null(2)); // Third row is not NULL
        assert!(int_array.is_null(3)); // Fourth row is NULL

        let float_array = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(!float_array.is_null(0)); // First row is not NULL
        assert!(!float_array.is_null(1)); // Second row is not NULL
        assert!(float_array.is_null(2)); // Third row is NULL
        assert!(float_array.is_null(3)); // Fourth row is NULL

        // Test round-trip conversion preserves NULLs
        let converted_rows = record_batch_to_rows(&batch).unwrap();
        assert_eq!(original_rows.len(), converted_rows.len());

        // Verify NULLs are preserved
        assert!(matches!(converted_rows[1].values[0], SqlValue::Null));
        assert!(matches!(converted_rows[2].values[1], SqlValue::Null));
        assert!(matches!(converted_rows[3].values[0], SqlValue::Null));
        assert!(matches!(converted_rows[3].values[1], SqlValue::Null));
    }

    #[test]
    fn test_null_values_in_all_column_types() {
        // Test NULL handling across different column types
        let original_rows = vec![
            Row::from_vec(vec![
                    SqlValue::Integer(1),
                    SqlValue::Double(1.1),
                    SqlValue::Varchar(arcstr::ArcStr::from("test")),
                    SqlValue::Boolean(true),
                ]),
            Row::from_vec(vec![SqlValue::Null, SqlValue::Null, SqlValue::Null, SqlValue::Null]),
            Row::from_vec(vec![
                    SqlValue::Integer(3),
                    SqlValue::Double(3.3),
                    SqlValue::Varchar(arcstr::ArcStr::from("data")),
                    SqlValue::Boolean(false),
                ]),
        ];

        let column_names = vec![
            "int_col".to_string(),
            "float_col".to_string(),
            "str_col".to_string(),
            "bool_col".to_string(),
        ];

        let batch = rows_to_record_batch(&original_rows, &column_names).unwrap();

        // Verify NULLs in each column
        assert!(!batch.column(0).is_null(0));
        assert!(batch.column(0).is_null(1)); // NULL integer
        assert!(!batch.column(0).is_null(2));

        assert!(!batch.column(1).is_null(0));
        assert!(batch.column(1).is_null(1)); // NULL float
        assert!(!batch.column(1).is_null(2));

        assert!(!batch.column(2).is_null(0));
        assert!(batch.column(2).is_null(1)); // NULL string
        assert!(!batch.column(2).is_null(2));

        assert!(!batch.column(3).is_null(0));
        assert!(batch.column(3).is_null(1)); // NULL boolean
        assert!(!batch.column(3).is_null(2));

        // Round-trip conversion
        let converted_rows = record_batch_to_rows(&batch).unwrap();
        assert_eq!(original_rows.len(), converted_rows.len());

        // Verify second row (all NULLs) is preserved
        assert!(matches!(converted_rows[1].values[0], SqlValue::Null));
        assert!(matches!(converted_rows[1].values[1], SqlValue::Null));
        assert!(matches!(converted_rows[1].values[2], SqlValue::Null));
        assert!(matches!(converted_rows[1].values[3], SqlValue::Null));
    }

    #[test]
    fn test_mixed_null_non_null_numeric() {
        // Test realistic scenario with mixed NULL/non-NULL numeric values
        // Simulates a query like: SELECT quantity * discount FROM orders
        // where discount can be NULL for some rows
        let original_rows = vec![
            Row::from_vec(vec![SqlValue::Integer(10), SqlValue::Double(0.1)]),
            Row::from_vec(vec![
                    SqlValue::Integer(20),
                    SqlValue::Null, // No discount applied
                ]),
            Row::from_vec(vec![SqlValue::Integer(30), SqlValue::Double(0.2)]),
            Row::from_vec(vec![
                    SqlValue::Integer(40),
                    SqlValue::Null, // No discount applied
                ]),
        ];

        let column_names = vec!["quantity".to_string(), "discount".to_string()];
        let batch = rows_to_record_batch(&original_rows, &column_names).unwrap();

        // Verify NULL pattern in discount column
        let discount_array = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(!discount_array.is_null(0));
        assert!(discount_array.is_null(1));
        assert!(!discount_array.is_null(2));
        assert!(discount_array.is_null(3));

        // Round-trip preserves NULL pattern
        let converted_rows = record_batch_to_rows(&batch).unwrap();
        assert!(matches!(converted_rows[1].values[1], SqlValue::Null));
        assert!(matches!(converted_rows[3].values[1], SqlValue::Null));

        // Non-NULL values are preserved
        match &converted_rows[0].values[1] {
            SqlValue::Double(v) => assert!((v - 0.1).abs() < 0.001),
            _ => panic!("Expected Double value"),
        }
        match &converted_rows[2].values[1] {
            SqlValue::Double(v) => assert!((v - 0.2).abs() < 0.001),
            _ => panic!("Expected Double value"),
        }
    }
}
