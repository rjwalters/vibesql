use crate::errors::ExecutorError;

/// Determine target column indices and types for an INSERT statement
pub fn resolve_target_columns(
    schema: &vibesql_catalog::TableSchema,
    table_name: &str,
    specified_columns: &[String],
) -> Result<Vec<(usize, vibesql_types::DataType)>, ExecutorError> {
    if specified_columns.is_empty() {
        // No columns specified: INSERT INTO t VALUES (...)
        // Use all columns in schema order
        Ok(schema
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col)| (idx, col.data_type.clone()))
            .collect())
    } else {
        // Columns specified: INSERT INTO t (col1, col2) VALUES (...)
        // Validate and resolve columns
        specified_columns
            .iter()
            .map(|col_name| {
                schema
                    .get_column_index(col_name)
                    .map(|idx| {
                        let col = &schema.columns[idx];
                        (idx, col.data_type.clone())
                    })
                    .ok_or_else(|| ExecutorError::ColumnNotFound {
                        column_name: col_name.clone(),
                        table_name: table_name.to_string(),
                        searched_tables: vec![table_name.to_string()],
                        available_columns: schema.columns.iter().map(|c| c.name.clone()).collect(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()
    }
}

/// Validate that each row has the correct number of values
pub fn validate_row_column_counts(
    rows: &[Vec<vibesql_ast::Expression>],
    expected_count: usize,
) -> Result<(), ExecutorError> {
    for (row_idx, value_exprs) in rows.iter().enumerate() {
        if value_exprs.len() != expected_count {
            return Err(ExecutorError::UnsupportedExpression(format!(
                "INSERT row {} column count mismatch: expected {}, got {}",
                row_idx + 1,
                expected_count,
                value_exprs.len()
            )));
        }
    }
    Ok(())
}

/// Coerce a value to match the expected column type
/// Performs automatic type conversions where appropriate
pub fn coerce_value(
    value: vibesql_types::SqlValue,
    expected_type: &vibesql_types::DataType,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    use vibesql_types::{DataType, SqlValue};

    // NULL is valid for any type (NOT NULL constraint checked separately)
    if matches!(value, SqlValue::Null) {
        return Ok(value);
    }

    // Check type compatibility with automatic coercion
    match (&value, expected_type) {
        // Exact matches - no coercion needed
        (SqlValue::Integer(_), DataType::Integer) => Ok(value),
        (SqlValue::Varchar(_), DataType::Varchar { .. }) => Ok(value),
        (SqlValue::Character(_), DataType::Character { .. }) => Ok(value),
        (SqlValue::Boolean(_), DataType::Boolean) => Ok(value),
        (SqlValue::Float(_), DataType::Float { .. }) => Ok(value),
        (SqlValue::Real(_), DataType::Real) => Ok(value),
        (SqlValue::Double(_), DataType::DoublePrecision) => Ok(value),
        (SqlValue::Date(_), DataType::Date) => Ok(value),
        (SqlValue::Time(_), DataType::Time { .. }) => Ok(value),
        (SqlValue::Timestamp(_), DataType::Timestamp { .. }) => Ok(value),
        (SqlValue::Interval(_), DataType::Interval { .. }) => Ok(value),

        // VARCHAR/CHARACTER → DATE/TIME/TIMESTAMP conversions (implicit casting)
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Date) => {
            s.parse::<vibesql_types::Date>().map(SqlValue::Date).map_err(|e| {
                ExecutorError::UnsupportedExpression(format!("Cannot parse '{}' as DATE: {}", s, e))
            })
        }
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Time { .. }) => {
            s.parse::<vibesql_types::Time>().map(SqlValue::Time).map_err(|e| {
                ExecutorError::UnsupportedExpression(format!("Cannot parse '{}' as TIME: {}", s, e))
            })
        }
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Timestamp { .. }) => {
            s.parse::<vibesql_types::Timestamp>().map(SqlValue::Timestamp).map_err(|e| {
                ExecutorError::UnsupportedExpression(format!(
                    "Cannot parse '{}' as TIMESTAMP: {}",
                    s, e
                ))
            })
        }
        (SqlValue::Smallint(_), DataType::Smallint) => Ok(value),
        (SqlValue::Bigint(_), DataType::Bigint) => Ok(value),
        (SqlValue::Numeric(_), DataType::Numeric { .. }) => Ok(value),
        (SqlValue::Numeric(_), DataType::Decimal { .. }) => Ok(value),

        // Numeric literal → Float/Real/Double
        (SqlValue::Numeric(f), DataType::Float { .. }) => Ok(SqlValue::Float(*f as f32)),
        (SqlValue::Numeric(f), DataType::Real) => Ok(SqlValue::Real(*f as f32)),
        (SqlValue::Numeric(f), DataType::DoublePrecision) => Ok(SqlValue::Double(*f)),

        // Numeric literal → Integer types
        (SqlValue::Numeric(f), DataType::Integer) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(SqlValue::Integer(*f as i64))
            } else {
                Err(ExecutorError::UnsupportedExpression(format!(
                    "Cannot convert numeric '{}' to Integer (must be whole number in range)",
                    f
                )))
            }
        }
        (SqlValue::Numeric(f), DataType::Smallint) => {
            if f.fract() == 0.0 && *f >= i16::MIN as f64 && *f <= i16::MAX as f64 {
                Ok(SqlValue::Smallint(*f as i16))
            } else {
                Err(ExecutorError::UnsupportedExpression(format!(
                    "Cannot convert numeric '{}' to Smallint (must be whole number in range)",
                    f
                )))
            }
        }
        (SqlValue::Numeric(f), DataType::Bigint) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(SqlValue::Bigint(*f as i64))
            } else {
                Err(ExecutorError::UnsupportedExpression(format!(
                    "Cannot convert numeric '{}' to Bigint (must be whole number in range)",
                    f
                )))
            }
        }

        // Integer → Float types (safe widening conversion)
        (SqlValue::Integer(i), DataType::Float { .. }) => Ok(SqlValue::Float(*i as f32)),
        (SqlValue::Integer(i), DataType::Real) => Ok(SqlValue::Real(*i as f32)),
        (SqlValue::Integer(i), DataType::DoublePrecision) => Ok(SqlValue::Double(*i as f64)),
        (SqlValue::Smallint(i), DataType::Float { .. }) => Ok(SqlValue::Float(*i as f32)),
        (SqlValue::Smallint(i), DataType::Real) => Ok(SqlValue::Real(*i as f32)),
        (SqlValue::Smallint(i), DataType::DoublePrecision) => Ok(SqlValue::Double(*i as f64)),
        (SqlValue::Bigint(i), DataType::Float { .. }) => Ok(SqlValue::Float(*i as f32)),
        (SqlValue::Bigint(i), DataType::Real) => Ok(SqlValue::Real(*i as f32)),
        (SqlValue::Bigint(i), DataType::DoublePrecision) => Ok(SqlValue::Double(*i as f64)),

        // Integer widening conversions
        (SqlValue::Smallint(i), DataType::Integer) => Ok(SqlValue::Integer(*i as i64)),
        (SqlValue::Smallint(i), DataType::Bigint) => Ok(SqlValue::Bigint(*i as i64)),
        (SqlValue::Integer(i), DataType::Bigint) => Ok(SqlValue::Bigint(*i)),

        // Varchar ↔ Character conversions
        (SqlValue::Varchar(s), DataType::Character { length }) => {
            let s = if s.len() > *length {
                s[..*length].to_string() // Truncate
            } else {
                format!("{:width$}", s, width = length) // Pad with spaces
            };
            Ok(SqlValue::Character(arcstr::ArcStr::from(s)))
        }
        (SqlValue::Character(s), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(s.trim_end()))) // Remove trailing spaces
        }

        // Type mismatch
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Type mismatch: expected {:?}, got {:?}",
            expected_type, value
        ))),
    }
}
