use crate::errors::ExecutorError;

/// Result of resolving INSERT column targets
pub struct ResolvedInsertColumns {
    /// Column indices and types for regular columns (schema columns)
    pub columns: Vec<(usize, vibesql_types::DataType)>,
    /// Position of rowid pseudo-column in the input column list, if specified
    /// This allows extracting the rowid value from VALUES for explicit rowid inserts
    pub rowid_position: Option<usize>,
    /// True if rowid_position refers to a separate pseudo-column (rowid, _rowid_, oid)
    /// False if rowid_position refers to an INTEGER PRIMARY KEY column
    /// This affects value count validation - pseudo-columns add 1 to the expected count
    pub rowid_is_pseudo_column: bool,
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
        // Use all non-generated columns in schema order
        // Generated columns (those with generated_expr) are excluded per SQLite behavior
        let columns: Vec<_> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.generated_expr.is_none())
            .map(|(idx, col)| (idx, col.data_type.clone()))
            .collect();

        // Detect INTEGER PRIMARY KEY column for rowid aliasing
        // When a single-column INTEGER PRIMARY KEY exists, that column's value
        // becomes the explicit rowid. Find its position in the filtered column list.
        let rowid_position = if let Some(ref pk_cols) = schema.primary_key {
            if pk_cols.len() == 1 {
                // Check if the PK column is INTEGER type (rowid alias)
                if let Some(pk_idx) = schema.get_column_index(&pk_cols[0]) {
                    let pk_col = &schema.columns[pk_idx];
                    // INTEGER PRIMARY KEY is a rowid alias if is_exact_integer_type is true
                    if pk_col.is_exact_integer_type {
                        // Find this column's position in the filtered column list
                        columns.iter().position(|(idx, _)| *idx == pk_idx)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(ResolvedInsertColumns {
            columns,
            rowid_position,
            rowid_is_pseudo_column: false, // INTEGER PRIMARY KEY, not a pseudo-column
        })
    } else {
        // Columns specified: INSERT INTO t (col1, col2) VALUES (...)
        // Validate and resolve columns, handling rowid pseudo-column specially
        let mut columns = Vec::new();
        let mut rowid_position = None;
        let mut rowid_is_pseudo_column = false;

        // Get INTEGER PRIMARY KEY column info if it exists (single-column INTEGER PK)
        let integer_pk_col_name = if let Some(ref pk_cols) = schema.primary_key {
            if pk_cols.len() == 1 {
                if let Some(pk_idx) = schema.get_column_index(&pk_cols[0]) {
                    let pk_col = &schema.columns[pk_idx];
                    if pk_col.is_exact_integer_type {
                        Some(pk_cols[0].to_lowercase())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        for (input_idx, col_name) in specified_columns.iter().enumerate() {
            // First check if there's a real column with this name
            if let Some(schema_idx) = schema.get_column_index(col_name) {
                let col = &schema.columns[schema_idx];
                // Check if this is a generated column - cannot INSERT into generated columns
                if col.generated_expr.is_some() {
                    return Err(ExecutorError::CannotInsertIntoGeneratedColumn {
                        column_name: col_name.to_string(),
                    });
                }
                columns.push((schema_idx, col.data_type.clone()));

                // Check if this is the INTEGER PRIMARY KEY column (rowid alias)
                if let Some(ref pk_name) = integer_pk_col_name {
                    if col_name.to_lowercase() == *pk_name {
                        // This column's value in VALUES will be the explicit rowid
                        rowid_position = Some(input_idx);
                        // Not a pseudo-column - it's a real column that aliases rowid
                        rowid_is_pseudo_column = false;
                    }
                }
            } else if is_rowid_pseudo_column(col_name) {
                // It's a rowid pseudo-column - record its position but don't add to columns
                if rowid_position.is_some() {
                    return Err(ExecutorError::UnsupportedExpression(
                        "Multiple rowid columns specified in INSERT".to_string(),
                    ));
                }
                rowid_position = Some(input_idx);
                rowid_is_pseudo_column = true;
            } else {
                // Column not found and not a pseudo-column
                // Use SQLite-compatible error: "table T has no column named C"
                return Err(ExecutorError::InsertNoSuchColumn {
                    table_name: table_name.to_string(),
                    column_name: col_name.to_string(),
                });
            }
        }

        Ok(ResolvedInsertColumns {
            columns,
            rowid_position,
            rowid_is_pseudo_column,
        })
    }
}

/// Validate that each row has the correct number of values
///
/// SQLite checks in this order:
/// 1. All VALUES rows must have the same number of terms
/// 2. The number of terms must match the table column count
pub fn validate_row_column_counts(
    rows: &[Vec<vibesql_ast::Expression>],
    expected_count: usize,
    table_name: &str,
    has_explicit_columns: bool,
) -> Result<(), ExecutorError> {
    // First check: all VALUES rows must have the same number of terms
    if let Some(first_row) = rows.first() {
        let first_len = first_row.len();
        for value_exprs in rows.iter().skip(1) {
            if value_exprs.len() != first_len {
                return Err(ExecutorError::ValuesRowCountMismatch);
            }
        }
    }

    // Second check: number of terms must match expected column count
    for value_exprs in rows.iter() {
        if value_exprs.len() != expected_count {
            return Err(ExecutorError::InsertColumnCountMismatch {
                table_name: table_name.to_string(),
                expected: expected_count,
                provided: value_exprs.len(),
                has_explicit_columns,
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
        // NUMERIC affinity: if the value is a whole number, store as integer
        (SqlValue::Numeric(f), DataType::Numeric { .. } | DataType::Decimal { .. }) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(SqlValue::Integer(*f as i64))
            } else {
                Ok(value)
            }
        }

        // Numeric literal → Float/Real/Double
        (SqlValue::Numeric(f), DataType::Float { .. }) => Ok(SqlValue::Float(*f as f32)),
        (SqlValue::Numeric(f), DataType::Real) => Ok(SqlValue::Real(*f)),  // Real is now f64
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

        // VARCHAR/CHARACTER → Integer types (SQLite type affinity)
        // Try to convert text to integer, otherwise keep as text
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Integer) => {
            let trimmed = s.trim();
            if let Ok(i) = trimmed.parse::<i64>() {
                Ok(SqlValue::Integer(i))
            } else if let Ok(f) = trimmed.parse::<f64>() {
                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    Ok(SqlValue::Integer(f as i64))
                } else {
                    // Non-integer float stored as REAL
                    Ok(SqlValue::Double(f))
                }
            } else {
                // Can't convert - keep as text (SQLite behavior)
                Ok(value)
            }
        }
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Smallint) => {
            let trimmed = s.trim();
            if let Ok(i) = trimmed.parse::<i16>() {
                Ok(SqlValue::Smallint(i))
            } else if let Ok(f) = trimmed.parse::<f64>() {
                if f.fract() == 0.0 && f >= i16::MIN as f64 && f <= i16::MAX as f64 {
                    Ok(SqlValue::Smallint(f as i16))
                } else {
                    Ok(SqlValue::Double(f))
                }
            } else {
                Ok(value)
            }
        }
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Bigint) => {
            let trimmed = s.trim();
            if let Ok(i) = trimmed.parse::<i64>() {
                Ok(SqlValue::Bigint(i))
            } else if let Ok(f) = trimmed.parse::<f64>() {
                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    Ok(SqlValue::Bigint(f as i64))
                } else {
                    Ok(SqlValue::Double(f))
                }
            } else {
                Ok(value)
            }
        }

        // VARCHAR/CHARACTER → Float types (SQLite type affinity)
        // Try to convert text to float, otherwise keep as text
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Float { .. }) => {
            let trimmed = s.trim();
            if let Ok(f) = trimmed.parse::<f32>() {
                Ok(SqlValue::Float(f))
            } else {
                Ok(value)
            }
        }
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Real) => {
            // Real is now f64 (SQLite REAL is 8-byte IEEE float)
            let trimmed = s.trim();
            if let Ok(f) = trimmed.parse::<f64>() {
                Ok(SqlValue::Real(f))
            } else {
                Ok(value)
            }
        }
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::DoublePrecision) => {
            let trimmed = s.trim();
            if let Ok(f) = trimmed.parse::<f64>() {
                Ok(SqlValue::Double(f))
            } else {
                Ok(value)
            }
        }

        // VARCHAR/CHARACTER → Numeric types (SQLite type affinity)
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Numeric { .. }) => {
            let trimmed = s.trim();
            if let Ok(i) = trimmed.parse::<i64>() {
                Ok(SqlValue::Integer(i))
            } else if let Ok(f) = trimmed.parse::<f64>() {
                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    Ok(SqlValue::Integer(f as i64))
                } else {
                    Ok(SqlValue::Double(f))
                }
            } else {
                Ok(value)
            }
        }

        // Integer → Float types (safe widening conversion)
        (SqlValue::Integer(i), DataType::Float { .. }) => Ok(SqlValue::Float(*i as f32)),
        (SqlValue::Integer(i), DataType::Real) => Ok(SqlValue::Real(*i as f64)),  // Real is now f64
        (SqlValue::Integer(i), DataType::DoublePrecision) => Ok(SqlValue::Double(*i as f64)),
        (SqlValue::Smallint(i), DataType::Float { .. }) => Ok(SqlValue::Float(*i as f32)),
        (SqlValue::Smallint(i), DataType::Real) => Ok(SqlValue::Real(*i as f64)),  // Real is now f64
        (SqlValue::Smallint(i), DataType::DoublePrecision) => Ok(SqlValue::Double(*i as f64)),
        (SqlValue::Bigint(i), DataType::Float { .. }) => Ok(SqlValue::Float(*i as f32)),
        (SqlValue::Bigint(i), DataType::Real) => Ok(SqlValue::Real(*i as f64)),  // Real is now f64
        (SqlValue::Bigint(i), DataType::DoublePrecision) => Ok(SqlValue::Double(*i as f64)),

        // Integer → Numeric/Decimal (SQLite type affinity - integers can be stored in NUMERIC columns)
        (SqlValue::Integer(i), DataType::Numeric { .. }) => Ok(SqlValue::Integer(*i)),
        (SqlValue::Integer(i), DataType::Decimal { .. }) => Ok(SqlValue::Integer(*i)),
        (SqlValue::Smallint(i), DataType::Numeric { .. }) => Ok(SqlValue::Integer(*i as i64)),
        (SqlValue::Smallint(i), DataType::Decimal { .. }) => Ok(SqlValue::Integer(*i as i64)),
        (SqlValue::Bigint(i), DataType::Numeric { .. }) => Ok(SqlValue::Bigint(*i)),
        (SqlValue::Bigint(i), DataType::Decimal { .. }) => Ok(SqlValue::Bigint(*i)),

        // Float/Real/Double → Numeric/Decimal (SQLite type affinity)
        // SQLite NUMERIC affinity: if the value is a whole number, store as integer
        (SqlValue::Float(f), DataType::Numeric { .. } | DataType::Decimal { .. }) => {
            let f64_val = *f as f64;
            if f64_val.fract() == 0.0 && f64_val >= i64::MIN as f64 && f64_val <= i64::MAX as f64 {
                Ok(SqlValue::Integer(f64_val as i64))
            } else {
                Ok(SqlValue::Double(f64_val))
            }
        }
        (SqlValue::Real(f), DataType::Numeric { .. } | DataType::Decimal { .. }) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(SqlValue::Integer(*f as i64))
            } else {
                Ok(SqlValue::Double(*f))
            }
        }
        (SqlValue::Double(f), DataType::Numeric { .. } | DataType::Decimal { .. }) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(SqlValue::Integer(*f as i64))
            } else {
                Ok(SqlValue::Double(*f))
            }
        }

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

        // UserDefined types: Apply SQLite's type affinity rules based on the type name
        // See https://www.sqlite.org/datatype3.html#determination_of_column_affinity
        //
        // Type affinity is determined by checking substrings in the type name:
        // 1. Contains "INT" → INTEGER affinity
        // 2. Contains "CHAR", "CLOB", "TEXT" → TEXT affinity
        // 3. Contains "BLOB" or no type specified → BLOB affinity (accept any)
        // 4. Contains "REAL", "FLOA", "DOUB" → REAL affinity
        // 5. Otherwise → NUMERIC affinity (try to convert text to number)
        (_, DataType::UserDefined { type_name }) => {
            let type_upper = type_name.to_uppercase();
            let affinity = if type_upper.contains("INT") {
                "INTEGER"
            } else if type_upper.contains("CHAR")
                || type_upper.contains("CLOB")
                || type_upper.contains("TEXT")
            {
                "TEXT"
            } else if type_upper.contains("BLOB") || type_upper.is_empty() {
                "BLOB"
            } else if type_upper.contains("REAL")
                || type_upper.contains("FLOA")
                || type_upper.contains("DOUB")
            {
                "REAL"
            } else {
                // Default: NUMERIC affinity (including NUM, NUMBER, etc.)
                "NUMERIC"
            };

            match affinity {
                "BLOB" => {
                    // BLOB affinity: accept any value as-is
                    Ok(value)
                }
                "TEXT" => {
                    // TEXT affinity: convert to text representation
                    match &value {
                        SqlValue::Varchar(_) | SqlValue::Character(_) => Ok(value),
                        SqlValue::Integer(i) => {
                            Ok(SqlValue::Varchar(arcstr::ArcStr::from(i.to_string())))
                        }
                        SqlValue::Bigint(i) => {
                            Ok(SqlValue::Varchar(arcstr::ArcStr::from(i.to_string())))
                        }
                        SqlValue::Smallint(i) => {
                            Ok(SqlValue::Varchar(arcstr::ArcStr::from(i.to_string())))
                        }
                        SqlValue::Numeric(f) => {
                            Ok(SqlValue::Varchar(arcstr::ArcStr::from(f.to_string())))
                        }
                        SqlValue::Double(f) => {
                            Ok(SqlValue::Varchar(arcstr::ArcStr::from(f.to_string())))
                        }
                        SqlValue::Real(f) => {
                            Ok(SqlValue::Varchar(arcstr::ArcStr::from(f.to_string())))
                        }
                        SqlValue::Float(f) => {
                            Ok(SqlValue::Varchar(arcstr::ArcStr::from(f.to_string())))
                        }
                        _ => Ok(value), // Other types pass through
                    }
                }
                "INTEGER" => {
                    // INTEGER affinity: try to convert to integer
                    match &value {
                        SqlValue::Integer(_)
                        | SqlValue::Bigint(_)
                        | SqlValue::Smallint(_)
                        | SqlValue::Unsigned(_) => Ok(value),
                        SqlValue::Numeric(f) => {
                            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                                Ok(SqlValue::Integer(*f as i64))
                            } else {
                                // Non-integer stays as-is (SQLite stores actual type)
                                Ok(value)
                            }
                        }
                        SqlValue::Double(f) => {
                            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                                Ok(SqlValue::Integer(*f as i64))
                            } else {
                                Ok(value)
                            }
                        }
                        SqlValue::Real(f) => {
                            let f64_val = *f as f64;
                            if f64_val.fract() == 0.0
                                && f64_val >= i64::MIN as f64
                                && f64_val <= i64::MAX as f64
                            {
                                Ok(SqlValue::Integer(f64_val as i64))
                            } else {
                                Ok(value)
                            }
                        }
                        SqlValue::Varchar(s) | SqlValue::Character(s) => {
                            // Try to parse as integer
                            if let Ok(i) = s.trim().parse::<i64>() {
                                Ok(SqlValue::Integer(i))
                            } else if let Ok(f) = s.trim().parse::<f64>() {
                                // Try as float, convert to int if whole number
                                if f.fract() == 0.0
                                    && f >= i64::MIN as f64
                                    && f <= i64::MAX as f64
                                {
                                    Ok(SqlValue::Integer(f as i64))
                                } else {
                                    // Store as text if can't convert to integer
                                    Ok(value)
                                }
                            } else {
                                // Non-numeric text stays as text
                                Ok(value)
                            }
                        }
                        _ => Ok(value), // Other types pass through
                    }
                }
                "REAL" => {
                    // REAL affinity: convert to floating point
                    match &value {
                        SqlValue::Real(_) | SqlValue::Double(_) | SqlValue::Float(_) => Ok(value),
                        SqlValue::Integer(i) => Ok(SqlValue::Double(*i as f64)),
                        SqlValue::Bigint(i) => Ok(SqlValue::Double(*i as f64)),
                        SqlValue::Smallint(i) => Ok(SqlValue::Double(*i as f64)),
                        SqlValue::Numeric(f) => Ok(SqlValue::Double(*f)),
                        SqlValue::Varchar(s) | SqlValue::Character(s) => {
                            // Try to parse as float
                            if let Ok(f) = s.trim().parse::<f64>() {
                                Ok(SqlValue::Double(f))
                            } else {
                                // Non-numeric text stays as text
                                Ok(value)
                            }
                        }
                        _ => Ok(value),
                    }
                }
                "NUMERIC" | _ => {
                    // NUMERIC affinity: try integer first, then real, else keep as text
                    // This is SQLite's behavior for types like NUM, NUMBER, etc.
                    match &value {
                        // Numeric types pass through
                        SqlValue::Integer(_)
                        | SqlValue::Bigint(_)
                        | SqlValue::Smallint(_)
                        | SqlValue::Numeric(_)
                        | SqlValue::Double(_)
                        | SqlValue::Real(_)
                        | SqlValue::Float(_)
                        | SqlValue::Unsigned(_) => Ok(value),
                        // Text: try to convert to number
                        SqlValue::Varchar(s) | SqlValue::Character(s) => {
                            let trimmed = s.trim();
                            // Try integer first
                            if let Ok(i) = trimmed.parse::<i64>() {
                                return Ok(SqlValue::Integer(i));
                            }
                            // Try float next
                            if let Ok(f) = trimmed.parse::<f64>() {
                                // SQLite converts to integer if it's a whole number
                                if f.fract() == 0.0
                                    && f >= i64::MIN as f64
                                    && f <= i64::MAX as f64
                                {
                                    return Ok(SqlValue::Integer(f as i64));
                                }
                                return Ok(SqlValue::Double(f));
                            }
                            // Can't convert - keep as text (SQLite behavior)
                            Ok(value)
                        }
                        // Other types pass through
                        _ => Ok(value),
                    }
                }
            }
        }

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

        // Blob → Any column type (SQLite stores blobs as-is regardless of column affinity)
        // In SQLite, blobs can be stored in any column and retain their blob type
        (SqlValue::Blob(_), _) => Ok(value),

        // Type mismatch
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Type mismatch: expected {:?}, got {:?}",
            expected_type, value
        ))),
    }
}
