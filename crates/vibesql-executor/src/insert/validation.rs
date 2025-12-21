use crate::errors::ExecutorError;

/// Result of resolving INSERT column targets
pub struct ResolvedInsertColumns {
    /// Column indices and types for regular columns (schema columns)
    pub columns: Vec<(usize, vibesql_types::DataType)>,
    /// Position of rowid pseudo-column in the input column list, if specified
    /// This allows extracting the rowid value from VALUES for explicit rowid inserts
    pub rowid_position: Option<usize>,
}

/// Check if a column name is a ROWID pseudo-column (SQLite compatibility)
/// Returns true for "rowid", "_rowid_", "oid" (case-insensitive)
fn is_rowid_pseudo_column(col_name: &str) -> bool {
    let lower = col_name.to_lowercase();
    lower == "rowid" || lower == "_rowid_" || lower == "oid"
}

/// Determine target column indices and types for an INSERT statement,
/// including support for the ROWID pseudo-column (SQLite compatibility)
pub fn resolve_target_columns_with_rowid(
    schema: &vibesql_catalog::TableSchema,
    table_name: &str,
    specified_columns: &[String],
) -> Result<ResolvedInsertColumns, ExecutorError> {
    if specified_columns.is_empty() {
        // No columns specified: INSERT INTO t VALUES (...)
        // Use all columns in schema order
        Ok(ResolvedInsertColumns {
            columns: schema
                .columns
                .iter()
                .enumerate()
                .map(|(idx, col)| (idx, col.data_type.clone()))
                .collect(),
            rowid_position: None,
        })
    } else {
        // Columns specified: INSERT INTO t (col1, col2) VALUES (...)
        // Validate and resolve columns, handling rowid pseudo-column specially
        let mut columns = Vec::new();
        let mut rowid_position = None;

        for (input_idx, col_name) in specified_columns.iter().enumerate() {
            // First check if there's a real column with this name
            if let Some(schema_idx) = schema.get_column_index(col_name) {
                let col = &schema.columns[schema_idx];
                columns.push((schema_idx, col.data_type.clone()));
            } else if is_rowid_pseudo_column(col_name) {
                // It's a rowid pseudo-column - record its position but don't add to columns
                if rowid_position.is_some() {
                    return Err(ExecutorError::UnsupportedExpression(
                        "Multiple rowid columns specified in INSERT".to_string(),
                    ));
                }
                rowid_position = Some(input_idx);
            } else {
                // Column not found and not a pseudo-column
                return Err(ExecutorError::ColumnNotFound {
                    column_name: col_name.to_string(),
                    table_name: table_name.to_string(),
                    searched_tables: vec![table_name.to_string()],
                    available_columns: schema.columns.iter().map(|c| c.name.clone()).collect(),
                });
            }
        }

        Ok(ResolvedInsertColumns { columns, rowid_position })
    }
}

/// Validate that each row has the correct number of values
pub fn validate_row_column_counts(
    rows: &[Vec<vibesql_ast::Expression>],
    expected_count: usize,
    table_name: &str,
) -> Result<(), ExecutorError> {
    for value_exprs in rows.iter() {
        if value_exprs.len() != expected_count {
            // Match SQLite's error message format exactly
            return Err(ExecutorError::InsertColumnCountMismatch {
                table_name: table_name.to_string(),
                expected: expected_count,
                provided: value_exprs.len(),
            });
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
        // SQLite type affinity: try to convert to integer if possible,
        // otherwise keep as numeric (SQLite stores values with actual type, not column affinity)
        (SqlValue::Numeric(f), DataType::Integer) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(SqlValue::Integer(*f as i64))
            } else {
                // SQLite affinity: non-integer values stay as REAL in INTEGER column
                Ok(SqlValue::Numeric(*f))
            }
        }
        (SqlValue::Numeric(f), DataType::Smallint) => {
            if f.fract() == 0.0 && *f >= i16::MIN as f64 && *f <= i16::MAX as f64 {
                Ok(SqlValue::Smallint(*f as i16))
            } else {
                // SQLite affinity: non-integer values stay as REAL in SMALLINT column
                Ok(SqlValue::Numeric(*f))
            }
        }
        (SqlValue::Numeric(f), DataType::Bigint) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(SqlValue::Bigint(*f as i64))
            } else {
                // SQLite affinity: non-integer values stay as REAL in BIGINT column
                Ok(SqlValue::Numeric(*f))
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

        // BinaryLargeObject (BLOB) accepts any value type
        // This implements SQLite's type affinity behavior where untyped columns
        // (which default to BLOB affinity) can store values of any type.
        // The value is stored as-is without conversion.
        (_, DataType::BinaryLargeObject) => Ok(value),

        // SQLite type affinity: any value can be stored in TEXT columns by converting to string
        // This implements SQLite's behavior where TEXT affinity columns accept any value type
        (SqlValue::Integer(i), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(i.to_string())))
        }
        (SqlValue::Smallint(i), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(i.to_string())))
        }
        (SqlValue::Bigint(i), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(i.to_string())))
        }
        (SqlValue::Numeric(f), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(f.to_string())))
        }
        (SqlValue::Float(f), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(f.to_string())))
        }
        (SqlValue::Real(f), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(f.to_string())))
        }
        (SqlValue::Double(f), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(f.to_string())))
        }
        (SqlValue::Boolean(b), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(if *b { "1" } else { "0" })))
        }

        // Type mismatch
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Type mismatch: expected {:?}, got {:?}",
            expected_type, value
        ))),
    }
}
